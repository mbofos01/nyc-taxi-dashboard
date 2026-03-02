import os
import logging
import sys
import time
import json
from pathlib import Path

import pika
import redis as redis_client
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

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
MODELS_DIR = os.getenv("MODELS_DIR")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_L_EXCHANGE")
RABBITMQ_QUEUE = "model.fare.train"  # unique queue bound to the fanout

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_MODEL_ROOT = os.getenv("REDIS_MODEL_ROOT")
REDIS_FARE_MODEL_KEY = os.getenv("REDIS_FARE_MODEL_KEY")
REDIS_KEY = f"{REDIS_MODEL_ROOT}:{REDIS_FARE_MODEL_KEY}:status"

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")

# Taxi types that have fare data
FARE_TAXI_TYPES = ["yellow", "green"]  # add "fhvhv" later

# ── Spark ──────────────────────────────────────────────────────────────────


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder.master(SPARK_MASTER_URL)
        .appName("nyc-taxi-fare-trainer")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "3g")
        .config("spark.driver.host", "fare-trainer")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Redis ──────────────────────────────────────────────────────────────────


def set_model_status(status: str) -> None:
    """Set model status in Redis: training | ready | failed."""
    try:
        r = redis_client.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.set(REDIS_KEY, status)
        logger.info(f"Redis: {REDIS_KEY} = {status}")
    except Exception as e:
        logger.warning(f"Failed to set Redis key: {e}")


# ── Training ───────────────────────────────────────────────────────────────


def run_training() -> None:
    logger.info("=== Starting fare model training ===")
    set_model_status("training")

    train_start = time.time()
    spark = get_spark()

    # ── Prometheus metrics ─────────────────────────────────────────────────
    registry = CollectorRegistry()
    training_duration = Gauge(
        "model_fare_training_duration_seconds",
        "Total training duration in seconds",
        registry=registry,
    )
    rmse_gauge = Gauge(
        "model_fare_rmse",
        "Root Mean Squared Error on test set",
        registry=registry,
    )
    mae_gauge = Gauge(
        "model_fare_mae",
        "Mean Absolute Error on test set",
        registry=registry,
    )
    training_rows = Gauge(
        "model_fare_training_rows",
        "Number of rows used for training",
        registry=registry,
    )
    last_trained_ts = Gauge(
        "model_fare_last_trained_timestamp",
        "Unix timestamp of last successful training run",
        registry=registry,
    )
    # ──────────────────────────────────────────────────────────────────────

    try:
        # ── Load processed data ────────────────────────────────────────────
        dfs = []
        for taxi_type in FARE_TAXI_TYPES:
            data_path = Path(PROCESSED_DATA_DIR) / "zone_hourly" / taxi_type
            if not data_path.exists():
                logger.warning(f"No processed data for {taxi_type}, skipping.")
                continue
            files = list(data_path.rglob("*.parquet"))
            if not files:
                logger.warning(f"No parquet files for {taxi_type}, skipping.")
                continue
            df = spark.read.parquet(*[str(f) for f in files])
            dfs.append(df)
            logger.info(f"Loaded {taxi_type} data: {data_path}")

        if not dfs:
            raise ValueError("No data available for fare model training.")

        df = dfs[0]
        for d in dfs[1:]:
            df = df.unionByName(d, allowMissingColumns=True)

        # ── Feature selection & filtering ──────────────────────────────────
        df = df.filter(
            F.col("avg_fare").isNotNull()
            & (F.col("avg_fare") > 0)
            & (F.col("avg_fare") < 200)
        )

        # Encode taxi_type as numeric index
        type_indexer = StringIndexer(
            inputCol="taxi_type", outputCol="taxi_type_idx", handleInvalid="keep"
        )

        feature_cols = [
            "pickup_location_id",
            "pickup_hour",
            "pickup_dow",
            "pickup_month",
            "taxi_type_idx",
        ]

        assembler = VectorAssembler(
            inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip"
        )

        scaler = StandardScaler(
            inputCol="features_raw", outputCol="features", withMean=True, withStd=True
        )

        lr = LinearRegression(
            featuresCol="features",
            labelCol="avg_fare",
            maxIter=100,
            regParam=0.1,
            elasticNetParam=0.5,
        )

        pipeline = Pipeline(stages=[type_indexer, assembler, scaler, lr])

        # ── Train / test split ─────────────────────────────────────────────
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        row_count = train_df.count()
        training_rows.set(row_count)
        logger.info(f"Training rows: {row_count:,}")

        # ── Train ──────────────────────────────────────────────────────────
        logger.info("Fitting LinearRegression model...")
        model = pipeline.fit(train_df)

        # ── Evaluate ───────────────────────────────────────────────────────
        predictions = model.transform(test_df)

        rmse_eval = RegressionEvaluator(
            labelCol="avg_fare", predictionCol="prediction", metricName="rmse"
        )
        mae_eval = RegressionEvaluator(
            labelCol="avg_fare", predictionCol="prediction", metricName="mae"
        )

        rmse = rmse_eval.evaluate(predictions)
        mae = mae_eval.evaluate(predictions)

        rmse_gauge.set(rmse)
        mae_gauge.set(mae)
        logger.info(f"RMSE: {rmse:.4f} | MAE: {mae:.4f}")

        # ── Save model ─────────────────────────────────────────────────────
        model_path = str(Path(MODELS_DIR) / "fare")
        model.write().overwrite().save(model_path)
        logger.info(f"Model saved to: {model_path}")

        # ── Finalise metrics ───────────────────────────────────────────────
        duration = time.time() - train_start
        training_duration.set(duration)
        last_trained_ts.set(time.time())

        set_model_status("ready")
        logger.info(f"=== Fare model training complete ({duration:.1f}s) ===")

    except Exception as e:
        logger.error(f"Training failed: {e}", exc_info=True)
        set_model_status("failed")

    finally:
        try:
            push_to_gateway(PUSHGATEWAY_URL, job="model_fare", registry=registry)
            logger.info("Metrics pushed to Pushgateway.")
        except Exception as e:
            logger.warning(f"Failed to push metrics: {e}")
        spark.stop()


# ── RabbitMQ consumer ──────────────────────────────────────────────────────


def on_message(ch, method, properties, body) -> None:
    try:
        payload = json.loads(body)
        logger.info(f"Received message: {payload}")
        _timestamp_ = payload.get("timestamp", "N/A")
        _event_ = payload.get("event", "N/A")

        logger.info(f" ({_timestamp_}) - Processing event: {_event_} ...")
        # run_training()
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def start_consumer() -> None:
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

            # Declare fanout exchange and bind a dedicated queue for this service
            channel.exchange_declare(
                exchange=RABBITMQ_EXCHANGE, exchange_type="fanout", durable=True
            )
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            channel.queue_bind(queue=RABBITMQ_QUEUE, exchange=RABBITMQ_EXCHANGE)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=RABBITMQ_QUEUE,
                on_message_callback=on_message,
            )
            logger.info(
                f"Listening on exchange: {RABBITMQ_EXCHANGE} / queue: {RABBITMQ_QUEUE}"
            )
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
