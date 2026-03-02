import os
import re
import logging
import sys
import time
import json
from pathlib import Path
from typing import List
import redis
from datetime import datetime, timezone
import shutil
import threading

import pika
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")
LOG_DIR = os.getenv("LOG_DIR")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_IN_QUEUE = os.getenv("RABBITMQ_E_QUEUE")  # listen
RABBITMQ_OUT_QUEUE = os.getenv("RABBITMQ_T_QUEUE")  # publish
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_TRACKING_ROOT = os.getenv("REDIS_TRACKING_ROOT")
REDIS_PROCESSED_SET = os.getenv("REDIS_PROCESSED_SET")
REDIS_LOADED_FLAG = os.getenv("REDIS_LOADED_FLAG")
# daily morning pending check (UTC)
CRON_HOUR = int(os.getenv("TRANSFORM_CRON_HOUR"))
CRON_MINUTE = int(os.getenv("TRANSFORM_CRON_MINUTE"))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


# ── Column name normalisation ──────────────────────────────────────────────
# Raw pickup-datetime column per taxi type (used for per-file date validation)
DATETIME_COLS = {
    "yellow": "tpep_pickup_datetime",
    "green": "lpep_pickup_datetime",
    "fhv": "pickup_datetime",
    "fhvhv": "pickup_datetime",
}

COLUMN_MAPS = {
    "yellow": {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "fare_amount": "fare_amount",
        "tip_amount": "tip_amount",
        "total_amount": "total_amount",
        "payment_type": "payment_type",
        "RatecodeID": "rate_code",
    },
    "green": {
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "fare_amount": "fare_amount",
        "tip_amount": "tip_amount",
        "total_amount": "total_amount",
        "payment_type": "payment_type",
        "RatecodeID": "rate_code",
    },
    "fhv": {
        "pickup_datetime": "pickup_datetime",
        "dropOff_datetime": "dropoff_datetime",
        "PUlocationID": "pickup_location_id",
        "DOlocationID": "dropoff_location_id",
    },
    "fhvhv": {
        "pickup_datetime": "pickup_datetime",
        "dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "trip_miles": "trip_distance",
        "base_passenger_fare": "fare_amount",
        "tips": "tip_amount",
        "driver_pay": "total_amount",
    },
}


# ── Spark ──────────────────────────────────────────────────────────────────


def get_spark() -> SparkSession:
    """Create (or reuse) the SparkSession connected to the standalone master."""
    spark = (
        SparkSession.builder.master(SPARK_MASTER_URL)
        .appName("nyc-taxi-transform")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "3g")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        # The driver runs inside the 'transform' container. Workers need to
        # reach it by container name over the Docker bridge network.
        .config("spark.driver.host", "transform")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Transformation helpers ─────────────────────────────────────────────────


def _is_valid_parquet(path: str) -> bool:
    """
    Quick sanity-check: valid Parquet files start AND end with the 4-byte
    magic number b'PAR1'.  Catches partially-downloaded or truncated files
    before they reach Spark and abort the entire job.
    """
    MAGIC = b"PAR1"
    try:
        with open(path, "rb") as fh:
            header = fh.read(4)
            fh.seek(-4, 2)  # 4 bytes from end
            footer = fh.read(4)
        return header == MAGIC and footer == MAGIC
    except Exception as e:
        logger.warning(f"Could not validate parquet magic for {path}: {e}")
        return False


def load_and_validate_file(
    spark: SparkSession, file: str, taxi_type: str
) -> DataFrame | None:
    """
    Read a single Parquet file and immediately discard any rows whose
    pickup timestamp does not match the year-month encoded in the filename
    (e.g. ``yellow_tripdata_2022-01.parquet`` → keep only rows where
    pickup year == 2022 and pickup month == 01).

    Returns None if the file cannot be read or contains no valid rows.
    """
    if not _is_valid_parquet(file):
        logger.warning(f"Skipping corrupt/incomplete parquet (bad magic bytes): {file}")
        return None

    try:
        df = spark.read.parquet(file)
    except Exception as e:
        logger.warning(f"Skipping unreadable file {file}: {e}")
        return None

    dt_col = DATETIME_COLS.get(taxi_type)
    m = re.search(r"_(\d{4})-(\d{2})\.parquet$", Path(file).name)

    if m and dt_col and dt_col in df.columns:
        exp_year = int(m.group(1))
        exp_month = int(m.group(2))
        df = df.filter((F.year(dt_col) == exp_year) & (F.month(dt_col) == exp_month))
        logger.info(f"Read & date-validated ({exp_year}-{exp_month:02d}): {file}")
    else:
        logger.info(f"Read (no date validation possible): {file}")

    return df


def normalise(df: DataFrame, taxi_type: str) -> DataFrame:
    """Rename raw columns to the unified schema and drop unused ones."""
    col_map = COLUMN_MAPS.get(taxi_type, {})
    available = [c for c in col_map if c in df.columns]
    df = df.select(available)
    for old, new in col_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df.withColumn("taxi_type", F.lit(taxi_type))


def clean(df: DataFrame) -> DataFrame:
    """Filter out bad rows and derive time-feature columns."""
    # ── Null guards ────────────────────────────────────────────────────────
    df = df.filter(
        F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & F.col("pickup_location_id").isNotNull()
        & F.col("dropoff_location_id").isNotNull()
    )

    # ── Secondary date sanity check ────────────────────────────────────────
    # Catches files with no date in their name and any edge-case timezone
    # artefacts that survived the per-file filename validation above.
    df = df.filter(
        (F.year("pickup_datetime") >= 2019)
        & (F.year("pickup_datetime") <= F.year(F.current_date()))
    )

    # ── Trip duration ──────────────────────────────────────────────────────
    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"))
        / 60.0,
    )
    df = df.filter(
        (F.col("trip_duration_minutes") > 0) & (F.col("trip_duration_minutes") < 300)
    )

    # ── Range checks on optional numeric columns ───────────────────────────
    if "trip_distance" in df.columns:
        df = df.filter((F.col("trip_distance") >= 0) & (F.col("trip_distance") < 200))
    if "fare_amount" in df.columns:
        df = df.filter((F.col("fare_amount") >= 0) & (F.col("fare_amount") < 1000))

    # ── Time features ──────────────────────────────────────────────────────
    df = (
        df.withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_dow", F.dayofweek("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        .withColumn("pickup_year", F.year("pickup_datetime"))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("is_weekend", F.dayofweek("pickup_datetime").isin(1, 7))
        .withColumn(
            "time_bucket",
            F.when(F.col("pickup_hour").between(6, 11), "morning")
            .when(F.col("pickup_hour").between(12, 17), "afternoon")
            .when(F.col("pickup_hour").between(18, 22), "evening")
            .otherwise("night"),
        )
    )

    return df


# ── Aggregations ───────────────────────────────────────────────────────────


def agg_zone_hourly(df: DataFrame) -> DataFrame:
    """Trip count + averages per zone per hour — feeds map & demand forecast."""
    aggs = [F.count("*").alias("trip_count")]
    if "trip_duration_minutes" in df.columns:
        aggs.append(F.avg("trip_duration_minutes").alias("avg_duration_minutes"))
    if "trip_distance" in df.columns:
        aggs.append(F.avg("trip_distance").alias("avg_distance"))
    if "fare_amount" in df.columns:
        aggs.append(F.avg("fare_amount").alias("avg_fare"))
    if "total_amount" in df.columns:
        aggs.append(F.avg("total_amount").alias("avg_total"))
    return df.groupBy(
        "pickup_year",
        "pickup_month",
        "pickup_date",
        "pickup_dow",
        "pickup_hour",
        "pickup_location_id",
        "taxi_type",
    ).agg(*aggs)


def agg_daily_stats(df: DataFrame) -> DataFrame:
    """Daily summary per taxi type — feeds KPI cards."""
    aggs = [F.count("*").alias("trip_count")]
    if "trip_duration_minutes" in df.columns:
        aggs.append(F.avg("trip_duration_minutes").alias("avg_duration_minutes"))
    if "passenger_count" in df.columns:
        aggs.append(F.sum("passenger_count").alias("total_passengers"))
    if "fare_amount" in df.columns:
        aggs += [
            F.avg("fare_amount").alias("avg_fare"),
            F.sum("fare_amount").alias("total_revenue"),
        ]
    return df.groupBy("pickup_date", "taxi_type").agg(*aggs)


def agg_zone_time_buckets(df: DataFrame) -> DataFrame:
    """Trip count per zone per time bucket — feeds hotspot clustering."""
    return df.groupBy(
        "pickup_location_id",
        "time_bucket",
        "taxi_type",
    ).agg(F.count("*").alias("trip_count"))


def agg_zone_anomaly_stats(df: DataFrame) -> DataFrame:
    """Mean + stddev per zone — feeds anomaly detection."""
    aggs = [F.count("*").alias("trip_count")]
    for col in ["fare_amount", "trip_duration_minutes", "trip_distance"]:
        if col in df.columns:
            aggs += [
                F.avg(col).alias(f"avg_{col}"),
                F.stddev(col).alias(f"stddev_{col}"),
            ]
    return df.groupBy("pickup_location_id", "taxi_type").agg(*aggs)


# ── Write helper ───────────────────────────────────────────────────────────


def write_parquet(df: DataFrame, name: str, merge: bool = True) -> None:
    """
    Write an aggregated DataFrame to PROCESSED_DATA_DIR/{name}.
    If merge=True and output already exists, union with the existing data
    before writing so previous months are not overwritten (incremental append).
    If merge=False, overwrite directly (used after a detected crash/partial run).

    Uses a write-to-tmp-then-swap pattern to avoid a read/write conflict:
    mode("overwrite") would delete the source files mid-execution if we wrote
    directly to the same path we're reading from.
    """
    out_path = Path(PROCESSED_DATA_DIR) / name
    tmp_path = Path(PROCESSED_DATA_DIR) / (name + "_tmp")
    out = str(out_path)
    tmp = str(tmp_path)

    if merge and out_path.exists():
        try:
            existing = df.sparkSession.read.parquet(out)
            df = existing.unionByName(df, allowMissingColumns=True)
            logger.info(f"Merging with existing data: {out}")
        except Exception as e:
            logger.warning(f"Could not read existing data at {out}, overwriting: {e}")

    # Write to a temp path first — this keeps the original intact while Spark
    # reads it (if merging), avoiding a FileNotFound mid-job.
    if tmp_path.exists():
        shutil.rmtree(tmp)
    df.write.mode("overwrite").parquet(tmp)

    # Swap: remove old output and promote the temp dir.
    if out_path.exists():
        shutil.rmtree(out)
    tmp_path.rename(out_path)
    logger.info(f"Written: {out}")


def find_pending_files() -> List[Path]:
    """
    Return the list of raw parquet files that have not yet been processed.

    Scans ``RAW_DATA_DIR`` for ``*.parquet`` files and filters out any whose
    filename is already recorded in the Redis processed-files set
    (``{REDIS_TRACKING_ROOT}:{REDIS_PROCESSED_SET}``).  The full pending list
    is also written to ``<LOG_DIR>/pending_files.log`` for auditing.

    Returns:
        List of ``Path`` objects for files that still need to be transformed.
    """
    logger.info(f"Scanning for pending files in {PROCESSED_DATA_DIR}...")
    raw_files = set(Path(RAW_DATA_DIR).glob("*.parquet"))
    out = Path(LOG_DIR) / "pending_files.log"
    out.parent.mkdir(parents=True, exist_ok=True)
    pending_files = []

    with out.open("w") as f:
        f.write(f"{'===' * 20} \n")
        f.write(f"Pending files as of {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        for rf in raw_files:
            if r.sismember(f"{REDIS_TRACKING_ROOT}:{REDIS_PROCESSED_SET}", rf.name):
                logger.debug(f"Already processed (Redis): {rf.name}")
            else:
                logger.info(f"New pending file: {rf.name}")
                f.write(f"Pending: {rf.name}\n")
                pending_files.append(rf)
        f.write(f"{'===' * 20} \n")

    return pending_files


def process_files(file_list: List[Path]) -> None:
    """
    Run the Spark transform pipeline on the given pending files,
    grouped by taxi type. Marks each file as processed in Redis on success.

    Args:
        file_list (List[Path]): Raw parquet files to process.
    """
    if not file_list:
        logger.info("No pending files to process.")
        return

    run_start = time.time()

    # ── Prometheus metrics ─────────────────────────────────────────────────
    registry = CollectorRegistry()
    g_processed = Gauge(
        "etl_transform_files_processed_total",
        "Number of valid Parquet files processed per taxi type",
        ["taxi_type"],
        registry=registry,
    )
    g_corrupt = Gauge(
        "transform_files_corrupt_total",
        "Files skipped due to corrupt or unreadable parquet",
        ["taxi_type"],
        registry=registry,
    )
    g_rows_before = Gauge(
        "etl_transform_rows_before_cleaning",
        "Row count after normalisation, before cleaning, per taxi type",
        ["taxi_type"],
        registry=registry,
    )
    g_rows_after = Gauge(
        "etl_transform_rows_after_cleaning",
        "Row count after cleaning per taxi type",
        ["taxi_type"],
        registry=registry,
    )
    g_taxi_duration = Gauge(
        "etl_transform_processing_duration_seconds",
        "Processing duration per taxi type in seconds",
        ["taxi_type"],
        registry=registry,
    )
    g_duration = Gauge(
        "transform_total_duration_seconds",
        "Total wall-clock time of the transform run",
        registry=registry,
    )
    g_last_run = Gauge(
        "etl_transform_last_success_timestamp",
        "Unix timestamp of the last successful transform run",
        registry=registry,
    )
    # ──────────────────────────────────────────────────────────────────────

    # ── Group files by taxi type (derived from filename prefix) ───────────
    grouped: dict[str, List[Path]] = {}
    for file_path in file_list:
        taxi_type = next((t for t in COLUMN_MAPS if file_path.name.startswith(t)), None)
        if taxi_type is None:
            logger.warning(
                f"Cannot determine taxi type for {file_path.name}, skipping."
            )
            continue
        grouped.setdefault(taxi_type, []).append(file_path)

    out = Path(LOG_DIR) / "action.log"
    out.parent.mkdir(parents=True, exist_ok=True)
    spark = get_spark()

    with out.open("w") as f:
        f.write(f"{'===' * 20}\n")
        f.write(f"Transform run: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")

        for taxi_type, files in grouped.items():
            t0 = time.time()
            logger.info(f"[{taxi_type}] Processing {len(files)} file(s)...")
            f.write(f"\n[{taxi_type}]\n")

            dfs = []
            corrupt_count = 0
            for file_path in files:
                df = load_and_validate_file(spark, str(file_path), taxi_type)
                if df is not None:
                    dfs.append((file_path, df))
                else:
                    corrupt_count += 1
                    f.write(f"  SKIP (unreadable): {file_path.name}\n")

            g_corrupt.labels(taxi_type=taxi_type).set(corrupt_count)

            if not dfs:
                logger.warning(f"[{taxi_type}] No valid DataFrames, skipping.")
                g_taxi_duration.labels(taxi_type=taxi_type).set(time.time() - t0)
                continue

            # Union all valid DataFrames for this taxi type
            combined = dfs[0][1]
            for _, df in dfs[1:]:
                combined = combined.unionByName(df, allowMissingColumns=True)

            try:
                # ── Crash-safety: guard against partial output ─────────────
                # Read the flag BEFORE marking this run as dirty.
                # "1" means the previous run completed cleanly → safe to merge.
                # "0" or missing means a crash left partial output → overwrite.
                output_flag_key = f"{REDIS_TRACKING_ROOT}:{taxi_type}"
                previous_clean = r.get(output_flag_key) == "1"
                r.set(output_flag_key, "0")  # mark dirty for this run

                normalised = normalise(combined, taxi_type)
                clean_df = clean(normalised)
                clean_df.cache()
                rows_after = clean_df.count()  # triggers the cache
                rows_before = (
                    normalised.count()
                )  # extra pass; acceptable for monitoring

                # Merge with existing output only if the previous run completed
                # cleanly. If dirty or missing, overwrite to avoid double-counting.
                should_merge = previous_clean
                if not should_merge:
                    logger.info(
                        f"[{taxi_type}] Dirty/missing flag — overwriting existing output."
                    )

                write_parquet(
                    agg_zone_hourly(clean_df),
                    f"zone_hourly/{taxi_type}",
                    merge=should_merge,
                )
                write_parquet(
                    agg_daily_stats(clean_df),
                    f"daily_stats/{taxi_type}",
                    merge=should_merge,
                )
                write_parquet(
                    agg_zone_time_buckets(clean_df),
                    f"zone_time_buckets/{taxi_type}",
                    merge=should_merge,
                )
                write_parquet(
                    agg_zone_anomaly_stats(clean_df),
                    f"zone_anomaly_stats/{taxi_type}",
                    merge=should_merge,
                )

                # All 4 writes succeeded — mark output as clean and files as done
                r.set(output_flag_key, "1")
                for file_path, _ in dfs:
                    r.sadd(
                        f"{REDIS_TRACKING_ROOT}:{REDIS_PROCESSED_SET}", file_path.name
                    )
                    f.write(f"  DONE: {file_path.name}\n")
                    logger.info(f"Marked as processed in Redis: {file_path.name}")

                clean_df.unpersist()
                g_processed.labels(taxi_type=taxi_type).set(len(dfs))
                g_rows_before.labels(taxi_type=taxi_type).set(rows_before)
                g_rows_after.labels(taxi_type=taxi_type).set(rows_after)
                g_taxi_duration.labels(taxi_type=taxi_type).set(time.time() - t0)

            except Exception as e:
                logger.error(f"[{taxi_type}] Spark pipeline failed: {e}", exc_info=True)
                f.write(f"  ERROR: {e}\n")

        f.write(f"\n{'===' * 20}\n")

    spark.stop()

    # signal that transformed data is ready for loading
    r.set(f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_FLAG}", "1")
    publish(
        {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event": "transform_completed",
            "triggered_by": "Transform stage",
            "summary": f"Processed {len(file_list)} files across {len(grouped)} taxi types.",
        }
    )

    g_duration.set(time.time() - run_start)
    g_last_run.set(time.time())
    try:
        push_to_gateway(PUSHGATEWAY_URL, job="etl_transform", registry=registry)
        logger.info(f"Metrics pushed to Pushgateway ({PUSHGATEWAY_URL}).")
    except Exception as e:
        logger.warning(f"Failed to push metrics: {e}")


# ── RabbitMQ consumer ──────────────────────────────────────────────────────


def _push_noop_metrics() -> None:
    """Heartbeat push when transform is skipped (no pending files)."""
    registry = CollectorRegistry()
    Gauge(
        "etl_transform_last_run_timestamp",
        "Unix timestamp of the last transform run",
        registry=registry,
    ).set(time.time())
    try:
        push_to_gateway(PUSHGATEWAY_URL, job="etl_transform", registry=registry)
    except Exception as e:
        logger.warning(f"Failed to push noop metrics: {e}")


def publish(payload: dict) -> None:
    """Publish *payload* as a durable JSON message to the configured RabbitMQ queue, retrying up to 5 times."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
    )
    for attempt in range(1, 6):
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=RABBITMQ_OUT_QUEUE, durable=True)
            channel.basic_publish(
                exchange="",
                routing_key=RABBITMQ_OUT_QUEUE,
                body=json.dumps(payload),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            connection.close()
            logger.info(f"Published to {RABBITMQ_OUT_QUEUE}: {payload}")
            return
        except Exception as e:
            logger.warning(f"RabbitMQ publish attempt {attempt}/5 failed: {e}")
            time.sleep(5)
    logger.error("Could not publish to RabbitMQ after 5 attempts.")


def on_message(ch, method, properties, body) -> None:
    """Callback triggered when a message arrives on etl.extracted."""
    try:
        payload = json.loads(body)
        logger.info(f"Received message: {payload}")
        _timestamp_ = payload.get("timestamp", time.time())
        _event_ = payload.get("event", "unknown")
        _summary_ = payload.get("summary", "")

        logger.info(f" ({_timestamp_}) - Processing event: {_event_} with {_summary_}.")

        pending_files = find_pending_files()
        if not pending_files:
            logger.info("No pending files to process, skipping.")
            _push_noop_metrics()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        process_files(pending_files)

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

    # Retry connection on startup
    for attempt in range(1, 11):
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=RABBITMQ_IN_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)  # One job at a time
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


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Start the RabbitMQ consumer in a daemon thread so the BlockingScheduler
    # can own the main thread and survive gracefully.
    consumer_thread = threading.Thread(
        target=start_consumer, daemon=True, name="rabbitmq-consumer"
    )
    consumer_thread.start()

    # Daily cron: scan for any pending files and process them each morning
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        lambda: process_files(find_pending_files()),
        trigger=CronTrigger(hour=CRON_HOUR, minute=CRON_MINUTE),
        name="daily_pending_check",
        misfire_grace_time=3600,
    )
    logger.info(
        f"Scheduler started — daily pending check at {CRON_HOUR}:{CRON_MINUTE:02d} UTC."
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
