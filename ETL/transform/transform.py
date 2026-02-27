import os
import re
import logging
import sys
import time
import json
from pathlib import Path
from typing import List

import shutil

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
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")
LOAD_BUCKET_DIR = os.getenv("LOAD_BUCKET_DIR")
LOG_DIR = os.getenv("LOG_DIR")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_IN_QUEUE = os.getenv("RABBITMQ_E_QUEUE")    # listen
RABBITMQ_OUT_QUEUE = os.getenv("RABBITMQ_T_QUEUE")  # publish
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")


def find_pending_files():
    # list the files in the raw data directory
    logger.info(f"Scanning for pending files in {PROCESSED_DATA_DIR}...")
    raw_files = set(Path(RAW_DATA_DIR).glob("*.parquet"))
    processed_files = set(Path(PROCESSED_DATA_DIR).glob("*.parquet"))
    processed_names = {p.name for p in processed_files}
    pending_files = [f for f in raw_files if f.name not in processed_names]

    out = Path(LOG_DIR) / "pending_files.log"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        f.write(f"Pending files ({len(pending_files)}) — {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        for p in sorted(pending_files):
            f.write(f"{p}\n")
    
    logger.info(f"Pending file list written to {out} - {len(pending_files)} files pending.")
    return pending_files

def process_file(file_list:List[Path]) -> None:
    # copy a file from raw to processed
    out = Path(LOG_DIR) / "action.log"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        for file_path in file_list:
            try:
                f.write(f"Processing file: {file_path} - {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                output_path = Path(PROCESSED_DATA_DIR) / file_path.name
                shutil.copy2(file_path, output_path)

                # symlink to LOAD_BUCKET_DIR
                bucket_path = Path(LOAD_BUCKET_DIR) / file_path.name
                Path(LOAD_BUCKET_DIR).mkdir(parents=True, exist_ok=True)
                if bucket_path.exists() or bucket_path.is_symlink():
                    bucket_path.unlink()
                bucket_path.symlink_to(output_path)
                f.write(f"\t Symlink created: {bucket_path} -> {output_path}\n")
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}", exc_info=True)

def on_message(ch, method, properties, body) -> None:
    """Callback triggered when a message arrives on etl.extracted."""
    try:
        payload = json.loads(body)
        logger.info(f"Received message: {payload}")
        _timestamp_ = payload.get("timestamp", time.time())
        _event_ = payload.get("event", "unknown")
        _new_files_ = int(payload.get("new_files", 0))
        _summary_ = payload.get("summary", "")

        logger.info(
            f" - ({_timestamp_}) - Processing event: {_event_} with {_summary_}.")
        logger.info(f"Extracted files: {_new_files_}")

        if _new_files_ == 0:
            logger.info(
                "No new files to process. Testing if pending files exist.")
            pending_files = find_pending_files()
            process_file(pending_files)

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
