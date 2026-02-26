import os
import logging
import sys
import time
import json
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pika
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR", "/data/processed")

POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB       = os.getenv("POSTGRES_DB", "nyc_taxi")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "nyc")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "nyc")

RABBITMQ_HOST      = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT      = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER      = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD  = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_IN_QUEUE  = os.getenv("RABBITMQ_T_QUEUE", "etl.transformed")   # listen
RABBITMQ_L_EXCHANGE = os.getenv("RABBITMQ_L_EXCHANGE", "etl.loaded")    # publish

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091")

TAXI_TYPES = ["yellow"]
# TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

# ── Dataset → table configuration ─────────────────────────────────────────
# Maps dataset folder name to (table_name, primary_key_columns)
DATASETS: dict[str, tuple[str, list[str]]] = {
    "zone_hourly": (
        "zone_hourly",
        ["pickup_date", "pickup_hour", "pickup_location_id", "taxi_type"],
    ),
    "daily_stats": (
        "daily_stats",
        ["pickup_date", "taxi_type"],
    ),
    "zone_time_buckets": (
        "zone_time_buckets",
        ["pickup_location_id", "time_bucket", "taxi_type"],
    ),
    "zone_anomaly_stats": (
        "zone_anomaly_stats",
        ["pickup_location_id", "taxi_type"],
    ),
}

# ── PostgreSQL helpers ─────────────────────────────────────────────────────

def get_pg_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def ensure_schema(conn: psycopg2.extensions.connection) -> None:
    """Create all tables if they do not already exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS zone_hourly (
        pickup_year          INT,
        pickup_month         INT,
        pickup_date          DATE,
        pickup_dow           INT,
        pickup_hour          INT,
        pickup_location_id   BIGINT,
        taxi_type            VARCHAR(10),
        trip_count           BIGINT,
        avg_duration_minutes DOUBLE PRECISION,
        avg_distance         DOUBLE PRECISION,
        avg_fare             DOUBLE PRECISION,
        avg_total            DOUBLE PRECISION,
        PRIMARY KEY (pickup_date, pickup_hour, pickup_location_id, taxi_type)
    );

    CREATE TABLE IF NOT EXISTS daily_stats (
        pickup_date          DATE,
        taxi_type            VARCHAR(10),
        trip_count           BIGINT,
        avg_duration_minutes DOUBLE PRECISION,
        total_passengers     BIGINT,
        avg_fare             DOUBLE PRECISION,
        total_revenue        DOUBLE PRECISION,
        PRIMARY KEY (pickup_date, taxi_type)
    );

    CREATE TABLE IF NOT EXISTS zone_time_buckets (
        pickup_location_id BIGINT,
        time_bucket        VARCHAR(20),
        taxi_type          VARCHAR(10),
        trip_count         BIGINT,
        PRIMARY KEY (pickup_location_id, time_bucket, taxi_type)
    );

    CREATE TABLE IF NOT EXISTS zone_anomaly_stats (
        pickup_location_id          BIGINT,
        taxi_type                   VARCHAR(10),
        trip_count                  BIGINT,
        avg_fare_amount             DOUBLE PRECISION,
        stddev_fare_amount          DOUBLE PRECISION,
        avg_trip_duration_minutes   DOUBLE PRECISION,
        stddev_trip_duration_minutes DOUBLE PRECISION,
        avg_trip_distance           DOUBLE PRECISION,
        stddev_trip_distance        DOUBLE PRECISION,
        PRIMARY KEY (pickup_location_id, taxi_type)
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logger.info("Schema ensured.")


def upsert_dataframe(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    table: str,
    pk_cols: list[str],
) -> int:
    """
    Bulk-upsert a DataFrame into a PostgreSQL table.
    Returns the number of rows upserted.
    """
    if df.empty:
        return 0

    # Replace NaN with None so psycopg2 inserts NULL
    df = df.where(pd.notnull(df), None)

    cols = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]

    update_cols = [c for c in cols if c not in pk_cols]
    if update_cols:
        update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        conflict_action = f"DO UPDATE SET {update_clause}"
    else:
        conflict_action = "DO NOTHING"

    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({', '.join(pk_cols)}) {conflict_action}"
    )

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    return len(values)


# ── Dataset reader ─────────────────────────────────────────────────────────

def read_parquet_dir(path: Path) -> pd.DataFrame | None:
    """
    Read all parquet part-files from a Spark output directory.
    Returns None if the directory does not exist or contains no parquet files.
    """
    if not path.exists():
        return None
    files = list(path.rglob("*.parquet"))
    if not files:
        return None
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


# ── Prometheus helper ──────────────────────────────────────────────────────

def _push_metrics(registry: CollectorRegistry) -> None:
    try:
        push_to_gateway(PUSHGATEWAY_URL, job="etl_load", registry=registry)
        logger.info(f"Metrics pushed to Pushgateway ({PUSHGATEWAY_URL}) for job 'etl_load'.")
    except Exception as e:
        logger.warning(f"Failed to push metrics to Pushgateway: {e}")


def _push_noop_metrics() -> None:
    """Heartbeat push when load is skipped (no-op)."""
    registry        = CollectorRegistry()
    last_success_ts = Gauge(
        "etl_load_last_success_timestamp",
        "Unix timestamp of the last successful load run",
        registry=registry,
    )
    last_success_ts.set(time.time())
    _push_metrics(registry)


# ── Core load logic ────────────────────────────────────────────────────────

def run_load(processed_dir: str) -> None:
    """Read all aggregated Parquet datasets and write them to PostgreSQL."""
    run_start = time.time()
    logger.info("=== Starting load ===")

    # ── Prometheus metrics ─────────────────────────────────────────────────
    registry        = CollectorRegistry()
    rows_loaded     = Gauge(
        "etl_load_rows_loaded_total",
        "Number of rows upserted per dataset and taxi type",
        ["dataset", "taxi_type"],
        registry=registry,
    )
    load_duration   = Gauge(
        "etl_load_duration_seconds",
        "Load duration per dataset and taxi type in seconds",
        ["dataset", "taxi_type"],
        registry=registry,
    )
    total_duration  = Gauge(
        "etl_load_total_duration_seconds",
        "Total load run duration in seconds",
        registry=registry,
    )
    failures        = Gauge(
        "etl_load_failures_total",
        "Number of dataset/taxi_type combinations that failed to load",
        registry=registry,
    )
    last_success_ts = Gauge(
        "etl_load_last_success_timestamp",
        "Unix timestamp of the last successful load run",
        registry=registry,
    )
    # ──────────────────────────────────────────────────────────────────────

    conn = None
    failure_count = 0
    try:
        conn = get_pg_conn()
        ensure_schema(conn)

        for dataset, (table, pk_cols) in DATASETS.items():
            for taxi_type in TAXI_TYPES:
                src = Path(processed_dir) / dataset / taxi_type
                t0  = time.time()
                try:
                    df = read_parquet_dir(src)
                    if df is None:
                        logger.info(f"No data at {src}, skipping.")
                        continue

                    n = upsert_dataframe(conn, df, table, pk_cols)
                    elapsed = time.time() - t0
                    rows_loaded.labels(dataset=dataset, taxi_type=taxi_type).set(n)
                    load_duration.labels(dataset=dataset, taxi_type=taxi_type).set(elapsed)
                    logger.info(f"Loaded {n:,} rows → {table} [{taxi_type}] ({elapsed:.1f}s)")

                except Exception as e:
                    failure_count += 1
                    logger.error(f"Failed to load {dataset}/{taxi_type}: {e}", exc_info=True)
                    if conn:
                        conn.rollback()

    except Exception as e:
        logger.error(f"Load aborted: {e}", exc_info=True)
        failure_count += 1
    finally:
        if conn:
            conn.close()

    failures.set(failure_count)
    total_duration.set(time.time() - run_start)
    last_success_ts.set(time.time())
    _push_metrics(registry)

    logger.info("=== Load complete ===")


# ── RabbitMQ publisher (fanout exchange) ───────────────────────────────────

def publish_loaded(payload: dict) -> None:
    """Publish to the etl.loaded fanout exchange to trigger all model services."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials
    )
    for attempt in range(1, 6):
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            # Fanout exchange — model services each bind their own queue to it
            channel.exchange_declare(
                exchange=RABBITMQ_L_EXCHANGE,
                exchange_type="fanout",
                durable=True,
            )
            channel.basic_publish(
                exchange=RABBITMQ_L_EXCHANGE,
                routing_key="",          # ignored by fanout
                body=json.dumps(payload),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            connection.close()
            logger.info(f"Published to exchange '{RABBITMQ_L_EXCHANGE}': {payload}")
            return
        except Exception as e:
            logger.warning(f"RabbitMQ publish attempt {attempt}/5 failed: {e}")
            time.sleep(5)
    logger.error("Could not publish to RabbitMQ after 5 attempts.")


# ── RabbitMQ consumer ──────────────────────────────────────────────────────

def on_message(ch, method, properties, body) -> None:
    """Callback triggered when a message arrives on etl.transformed."""
    try:
        payload = json.loads(body)
        logger.info(f"Received message: {payload}")

        if payload.get("action") == "no-op":
            logger.info("No new data — skipping load, updating Pushgateway.")
            _push_noop_metrics()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        processed_dir = payload.get("path", PROCESSED_DATA_DIR)
        run_load(processed_dir)

        publish_loaded({
            "event": "load_complete",
            "path": processed_dir,
            "action": "model_update",
        })

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def start_consumer() -> None:
    """Connect to RabbitMQ and start consuming messages."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
    )

    for attempt in range(1, 11):
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=RABBITMQ_IN_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=RABBITMQ_IN_QUEUE,
                on_message_callback=on_message,
            )
            logger.info(f"Listening on queue: {RABBITMQ_IN_QUEUE}")
            channel.start_consuming()
            return
        except Exception as e:
            logger.warning(f"Connection attempt {attempt}/10 failed: {e}")
            time.sleep(10)

    logger.error("Could not connect to RabbitMQ after 10 attempts. Exiting.")
    sys.exit(1)


# ── Startup catch-up ──────────────────────────────────────────────────────

def has_processed_data() -> bool:
    """
    Return True if any processed parquet dataset is present on disk and
    ready to be loaded into PostgreSQL.  Used on startup to catch up after
    a mid-pipeline crash where transform finished but load did not.
    """
    for dataset in DATASETS:
        for taxi_type in TAXI_TYPES:
            src = Path(PROCESSED_DATA_DIR) / dataset / taxi_type
            if src.exists() and list(src.rglob("*.parquet")):
                logger.info(f"Catch-up needed: unloaded processed data at {src}")
                return True
    return False


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Catch-up: if processed data exists but load never ran (e.g. after a
    # container crash), upsert it into PostgreSQL before entering the consumer.
    if has_processed_data():
        logger.info("=== Startup catch-up: unloaded processed data detected ===")
        try:
            run_load(PROCESSED_DATA_DIR)
            publish_loaded({
                "event": "load_complete",
                "path": PROCESSED_DATA_DIR,
                "action": "model_update",
            })
        except Exception as e:
            logger.error(f"Startup catch-up load failed: {e}", exc_info=True)
    else:
        logger.info("Startup catch-up: no unloaded processed data found, skipping.")

    start_consumer()
