import os
import logging
import sys
import time
import json
from pathlib import Path

import pika
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
RAW_DATA_DIR       = os.getenv("RAW_DATA_DIR", "/data/raw")
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR", "/data/processed")

RABBITMQ_HOST      = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT      = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER      = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD  = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_IN_QUEUE  = os.getenv("RABBITMQ_E_QUEUE", "etl.extracted")    # listen
RABBITMQ_OUT_QUEUE = os.getenv("RABBITMQ_T_QUEUE", "etl.transformed")  # publish

# ── Column name normalisation ──────────────────────────────────────────────
# Each taxi type uses different column names — we map them to a unified schema

COLUMN_MAPS = {
    "yellow": {
        "tpep_pickup_datetime":  "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID":          "pickup_location_id",
        "DOLocationID":          "dropoff_location_id",
        "passenger_count":       "passenger_count",
        "trip_distance":         "trip_distance",
        "fare_amount":           "fare_amount",
        "tip_amount":            "tip_amount",
        "total_amount":          "total_amount",
        "payment_type":          "payment_type",
        "RatecodeID":            "rate_code",
    },
    "green": {
        "lpep_pickup_datetime":  "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID":          "pickup_location_id",
        "DOLocationID":          "dropoff_location_id",
        "passenger_count":       "passenger_count",
        "trip_distance":         "trip_distance",
        "fare_amount":           "fare_amount",
        "tip_amount":            "tip_amount",
        "total_amount":          "total_amount",
        "payment_type":          "payment_type",
        "RatecodeID":            "rate_code",
    },
    "fhv": {
        "pickup_datetime":  "pickup_datetime",
        "dropOff_datetime": "dropoff_datetime",
        "PUlocationID":     "pickup_location_id",
        "DOlocationID":     "dropoff_location_id",
    },
    "fhvhv": {
        "pickup_datetime":   "pickup_datetime",
        "dropoff_datetime":  "dropoff_datetime",
        "PULocationID":      "pickup_location_id",
        "DOLocationID":      "dropoff_location_id",
        "trip_miles":        "trip_distance",
        "base_passenger_fare": "fare_amount",
        "tips":              "tip_amount",
        "driver_pay":        "total_amount",
    },
}

# ── Spark ──────────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("nyc-taxi-transform")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


# ── Transformation helpers ─────────────────────────────────────────────────

def normalise(df: DataFrame, taxi_type: str) -> DataFrame:
    """Rename raw columns to unified schema and drop unused ones."""
    col_map = COLUMN_MAPS.get(taxi_type, {})
    available = [c for c in col_map if c in df.columns]
    df = df.select(available)
    for old, new in col_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df.withColumn("taxi_type", F.lit(taxi_type))


def clean(df: DataFrame) -> DataFrame:
    """Filter out bad rows and add time features."""
    df = df.filter(
        F.col("pickup_datetime").isNotNull() &
        F.col("dropoff_datetime").isNotNull() &
        F.col("pickup_location_id").isNotNull() &
        F.col("dropoff_location_id").isNotNull()
    )

    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0
    )

    df = df.filter(
        (F.col("trip_duration_minutes") > 0) &
        (F.col("trip_duration_minutes") < 300)
    )

    if "trip_distance" in df.columns:
        df = df.filter(
            (F.col("trip_distance") >= 0) &
            (F.col("trip_distance") < 200)
        )

    if "fare_amount" in df.columns:
        df = df.filter(
            (F.col("fare_amount") >= 0) &
            (F.col("fare_amount") < 1000)
        )

    # Time features
    df = (
        df
        .withColumn("pickup_hour",  F.hour("pickup_datetime"))
        .withColumn("pickup_dow",   F.dayofweek("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        .withColumn("pickup_year",  F.year("pickup_datetime"))
        .withColumn("pickup_date",  F.to_date("pickup_datetime"))
        .withColumn("is_weekend",   F.dayofweek("pickup_datetime").isin(1, 7))
        .withColumn(
            "time_bucket",
            F.when(F.col("pickup_hour").between(6,  11), "morning")
             .when(F.col("pickup_hour").between(12, 17), "afternoon")
             .when(F.col("pickup_hour").between(18, 22), "evening")
             .otherwise("night")
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
        "pickup_year", "pickup_month", "pickup_date",
        "pickup_hour", "pickup_location_id", "taxi_type"
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
    """Trip count per zone per time bucket — feeds clustering/hotspots."""
    return df.groupBy(
        "pickup_location_id", "time_bucket", "taxi_type"
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

def write_parquet(df: DataFrame, name: str) -> None:
    """Write aggregated DataFrame to /data/processed/{name}."""
    out = str(Path(PROCESSED_DATA_DIR) / name)
    df.write.mode("overwrite").parquet(out)
    logger.info(f"Written: {out}")


# ── Core transform logic ───────────────────────────────────────────────────

def run_transform(raw_dir: str) -> None:
    """Read all raw Parquet files, transform, and write aggregations."""
    logger.info("=== Starting transform ===")
    spark = get_spark()

    for taxi_type in COLUMN_MAPS.keys():
        type_dir = Path(raw_dir) / taxi_type
        if not type_dir.exists():
            logger.warning(f"No data found for {taxi_type}, skipping.")
            continue

        logger.info(f"Processing {taxi_type}...")
        try:
            parquet_files = [str(p) for p in type_dir.rglob("*.parquet")]
            if not parquet_files:
                logger.warning(f"No parquet files found for {taxi_type}, skipping.")
                continue

            # Read each file individually to skip corrupted ones
            dfs = []
            for file in parquet_files:
                try:
                    df = spark.read.parquet(file)
                    dfs.append(df)
                    logger.info(f"Read: {file}")
                except Exception as e:
                    logger.warning(f"Skipping corrupted file {file}: {e}")

            if not dfs:
                logger.warning(f"No valid files for {taxi_type}, skipping.")
                continue

            raw_df = dfs[0]
            for df in dfs[1:]:
                raw_df = raw_df.union(df)

            norm_df  = normalise(raw_df, taxi_type)
            clean_df = clean(norm_df)

            write_parquet(agg_zone_hourly(clean_df),      f"zone_hourly/{taxi_type}")
            write_parquet(agg_daily_stats(clean_df),      f"daily_stats/{taxi_type}")
            write_parquet(agg_zone_time_buckets(clean_df),f"zone_time_buckets/{taxi_type}")
            write_parquet(agg_zone_anomaly_stats(clean_df),f"zone_anomaly_stats/{taxi_type}")

        except Exception as e:
            logger.error(f"Failed to process {taxi_type}: {e}", exc_info=True)

    spark.stop()
    logger.info("=== Transform complete ===")


# ── RabbitMQ consumer ──────────────────────────────────────────────────────

def publish(payload: dict) -> None:
    """Publish transform-complete event to RabbitMQ."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials
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
            logger.info(f"Published to RabbitMQ: {payload}")
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

        # Skip if extraction was a no-op
        # if payload.get("action") == "no-op":
        #     logger.info("No new files — skipping transform.")
        #     ch.basic_ack(delivery_tag=method.delivery_tag)
        #     return

        raw_dir = payload.get("path", RAW_DATA_DIR)
        run_transform(raw_dir)

        publish({
            "event": "transform_complete",
            "path": PROCESSED_DATA_DIR,
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
    start_consumer()
