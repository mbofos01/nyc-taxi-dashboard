import os
import logging
import sys
import time
import json
import threading
from pathlib import Path
from datetime import datetime, timezone
import redis
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pika
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
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
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT"))
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_IN_QUEUE = os.getenv("RABBITMQ_T_QUEUE")              # listen
RABBITMQ_L_EXCHANGE = os.getenv("RABBITMQ_L_EXCHANGE")          # publish
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_TRACKING_ROOT = os.getenv("REDIS_TRACKING_ROOT")
REDIS_LOADED_DIRS_HASH = os.getenv("REDIS_LOADED_DIRS_HASH")
REDIS_LOADED_FLAG = os.getenv("REDIS_LOADED_FLAG")
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")

LOAD_CRON_HOUR = int(os.getenv("LOAD_CRON_HOUR"))
LOAD_CRON_MINUTE = int(os.getenv("LOAD_CRON_MINUTE"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Taxi types are discovered dynamically from the processed data directory.

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
    """Return a new psycopg2 connection using credentials from environment variables."""
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

    # Deduplicate on PK columns: ON CONFLICT DO UPDATE cannot affect the same
    # row twice within a single command (happens when Spark writes duplicate
    # rows across part-files for the same partition key).
    before = len(df)
    df = df.drop_duplicates(subset=pk_cols, keep="last")
    dropped = before - len(df)
    if dropped:
        logger.warning(
            f"[{table}] Dropped {dropped} duplicate PK row(s) before upsert."
        )

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


# ── Loaded-dirs tracking (Redis hash) ────────────────────────────────────

def _loaded_dirs_key() -> str:
    return f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_DIRS_HASH}"


def _get_dir_mtime(path: Path) -> float:
    """Return the most-recent mtime among all parquet files in *path*."""
    files = list(path.rglob("*.parquet"))
    if not files:
        return 0.0
    return max(f.stat().st_mtime for f in files)


def _is_dir_pending(dataset: str, taxi_type: str, path: Path) -> bool:
    """Return True if *path* has been modified since it was last loaded."""
    field = f"{dataset}/{taxi_type}"
    stored = r.hget(_loaded_dirs_key(), field)
    if stored is None:
        return True  # never loaded
    return _get_dir_mtime(path) > float(stored)


def _mark_dir_loaded(dataset: str, taxi_type: str, path: Path) -> None:
    """Record the current mtime of *path* as the last-loaded timestamp."""
    field = f"{dataset}/{taxi_type}"
    r.hset(_loaded_dirs_key(), field, str(_get_dir_mtime(path)))


def find_pending_dirs(processed_dir: str) -> list[tuple[str, str]]:
    """
    Return (dataset, taxi_type) pairs whose processed directory has been
    modified since the last successful load (or has never been loaded).
    """
    pending = []
    for dataset in DATASETS:
        dataset_dir = Path(processed_dir) / dataset
        if not dataset_dir.exists():
            continue
        for taxi_dir in sorted(dataset_dir.iterdir()):
            if not taxi_dir.is_dir():
                continue
            if _is_dir_pending(dataset, taxi_dir.name, taxi_dir):
                pending.append((dataset, taxi_dir.name))
    return pending


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
    """Push *registry* metrics to Prometheus Pushgateway; logs and swallows any connection error."""
    try:
        push_to_gateway(PUSHGATEWAY_URL, job="etl_load", registry=registry)
        logger.info(
            f"Metrics pushed to Pushgateway ({PUSHGATEWAY_URL}) for job 'etl_load'.")
    except Exception as e:
        logger.warning(f"Failed to push metrics to Pushgateway: {e}")


def _push_noop_metrics() -> None:
    """Heartbeat push when load is skipped (no-op)."""
    registry = CollectorRegistry()
    last_success_ts = Gauge(
        "etl_load_last_success_timestamp",
        "Unix timestamp of the last successful load run",
        registry=registry,
    )
    last_success_ts.set(time.time())
    _push_metrics(registry)


# ── Core load logic ────────────────────────────────────────────────────────

def run_load(processed_dir: str, only: list[tuple[str, str]] | None = None) -> None:
    """
    Read aggregated Parquet datasets and write them to PostgreSQL.
    If *only* is given, restrict to those (dataset, taxi_type) pairs.
    After each successful upsert the dir mtime is recorded in Redis so it
    won't be re-loaded by the cron unless the data changes again.
    """
    run_start = time.time()
    logger.info("=== Starting load ===")

    # ── Prometheus metrics ─────────────────────────────────────────────────
    registry = CollectorRegistry()
    rows_loaded = Gauge(
        "etl_load_rows_loaded_total",
        "Number of rows upserted per dataset and taxi type",
        ["dataset", "taxi_type"],
        registry=registry,
    )
    load_duration = Gauge(
        "etl_load_duration_seconds",
        "Load duration per dataset and taxi type in seconds",
        ["dataset", "taxi_type"],
        registry=registry,
    )
    total_duration = Gauge(
        "etl_load_total_duration_seconds",
        "Total load run duration in seconds",
        registry=registry,
    )
    failures = Gauge(
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

        only_set = set(only) if only is not None else None

        for dataset, (table, pk_cols) in DATASETS.items():
            dataset_dir = Path(processed_dir) / dataset
            taxi_types = sorted([d.name for d in dataset_dir.iterdir(
            ) if d.is_dir()]) if dataset_dir.exists() else []
            if not taxi_types:
                logger.info(
                    f"No taxi type subdirs found under {dataset_dir}, skipping.")
                continue
            for taxi_type in taxi_types:
                if only_set is not None and (dataset, taxi_type) not in only_set:
                    logger.debug(
                        f"Skipping {dataset}/{taxi_type} (not pending).")
                    continue
                src = Path(processed_dir) / dataset / taxi_type
                t0 = time.time()
                try:
                    df = read_parquet_dir(src)
                    if df is None:
                        logger.info(f"No data at {src}, skipping.")
                        continue

                    n = upsert_dataframe(conn, df, table, pk_cols)
                    elapsed = time.time() - t0
                    rows_loaded.labels(
                        dataset=dataset, taxi_type=taxi_type).set(n)
                    load_duration.labels(
                        dataset=dataset, taxi_type=taxi_type).set(elapsed)
                    logger.info(
                        f"Loaded {n:,} rows → {table} [{taxi_type}] ({elapsed:.1f}s)")
                    _mark_dir_loaded(dataset, taxi_type, src)

                except Exception as e:
                    failure_count += 1
                    logger.error(
                        f"Failed to load {dataset}/{taxi_type}: {e}", exc_info=True)
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
            logger.info(
                f"Published to exchange '{RABBITMQ_L_EXCHANGE}': {payload}")
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

        _timestamp_ = payload.get("timestamp", "N/A")
        _event_ = payload.get("event", "N/A")
        _summary_ = payload.get("summary", {})
        logger.info(
            f" ({_timestamp_}) - Processing event: {_event_} with {_summary_}.")

        if r.get(f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_FLAG}") == "1":
            run_load(PROCESSED_DATA_DIR)
            r.set(f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_FLAG}", "0")
            publish_loaded({
                "timestamp":    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "event":        "data_loaded",
                "triggered_by": "Load stage",
                "summary":      "Data loaded successfully to PostgreSQL",
            })
        else:
            logger.info("Redis flag not set, skipping load.")
            _push_noop_metrics()

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


# ── Scheduled load job ────────────────────────────────────────────────────

def scheduled_load() -> None:
    """Cron-triggered load: only loads dataset/taxi_type dirs modified since last load."""
    logger.info("[CRON] Scheduled load triggered — checking for pending dirs.")
    try:
        pending = find_pending_dirs(PROCESSED_DATA_DIR)
        if not pending:
            logger.info("[CRON] All dirs already up-to-date, nothing to load.")
            _push_noop_metrics()
            return
        logger.info(f"[CRON] {len(pending)} pending dir(s): {pending}")
        run_load(PROCESSED_DATA_DIR, only=pending)
        r.set(f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_FLAG}", "0")
        publish_loaded({
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event": "data_loaded",
            "summary": f"Loaded {len(pending)} pending dataset(s) to PostgreSQL (scheduled)",
        })
    except Exception as e:
        logger.error(f"[CRON] Scheduled load failed: {e}", exc_info=True)


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # On startup: ensure the loaded_dirs key is a hash (not a leftover string).
    _dirs_key = f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_DIRS_HASH}"
    try:
        key_type = r.type(_dirs_key)
        if key_type not in ("hash", "none"):
            logger.warning(
                f"Redis key '{_dirs_key}' has wrong type '{key_type}', deleting it."
            )
            r.delete(_dirs_key)
    except Exception as e:
        logger.error(f"Error checking Redis key type for '{_dirs_key}': {e}", exc_info=True)

    # On startup: check Redis in case we missed the RabbitMQ message while down.
    try:
        if r.get(f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_FLAG}") == "1":
            logger.info(
                "Transformed data ready flag found in Redis on startup, running load.")
            run_load(PROCESSED_DATA_DIR)
            r.set(f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_FLAG}", "0")
    except Exception as e:
        logger.error(
            f"Error checking Redis for loaded flag: {e}", exc_info=True)

    # RabbitMQ consumer runs in a daemon thread so the BlockingScheduler
    # can own the main thread.
    consumer_thread = threading.Thread(
        target=start_consumer, daemon=True, name="rabbitmq-consumer"
    )
    consumer_thread.start()

    # Daily cron: load all processed data into PostgreSQL.
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        scheduled_load,
        trigger=CronTrigger(hour=LOAD_CRON_HOUR, minute=LOAD_CRON_MINUTE),
        name="daily_load",
        misfire_grace_time=3600,
    )
    logger.info(
        f"Scheduler started — daily load at {LOAD_CRON_HOUR}:{LOAD_CRON_MINUTE:02d} UTC."
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
