import os
import logging
import sys
import time
import json
from datetime import date
from pathlib import Path

import requests
import pika
from dateutil.relativedelta import relativedelta
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
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "/data/raw")
# START_DATE = date(2019, 1, 1)
START_DATE = date(2023, 1, 1)

# TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]
TAXI_TYPES = ["yellow", "green"]

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_E_QUEUE", "etl.extracted")

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091")

# ── Helpers ────────────────────────────────────────────────────────────────


def build_url(taxi_type: str, year: int, month: int) -> str:
    """
    This function constructs the URL for the Parquet file based on the taxi type, year, and month.

    URL format: https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet

    Args:
        taxi_type (str): The type of taxi (e.g., "yellow", "green", "fhv", "fhvhv").
        year (int): The year of the trip data (e.g., 2019).
        month (int): The month of the trip data (1-12).

    Returns:
        str: The constructed URL for the Parquet file.
    """
    return f"{TLC_BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def dest_path(taxi_type: str, year: int, month: int) -> Path:
    """
    This function generates the local file path where the downloaded Parquet file should be saved.
    The path is structured as: /data/raw/{taxi_type}/{year}/{taxi

    Args:
        taxi_type (str): The type of taxi (e.g., "yellow", "green", "fhv", "fhvhv").
        year (int): The year of the trip data (e.g., 2019).
        month (int): The month of the trip data (1-12).

    Returns:
        Path: The local file path for the downloaded Parquet file.
    """
    path = Path(RAW_DATA_DIR) / taxi_type / str(year)
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{taxi_type}_{year}-{month:02d}.parquet"


def download_file(url: str, dest: Path) -> bool:
    """
    This function downloads a file from the specified URL and saves it to the given destination path.
    If the file already exists at the destination, it skips the download.

    Args:
        url (str): The URL of the file to download.
        dest (Path): The local file path where the downloaded file should be saved.

    Returns:
        bool: True if the file was downloaded or already exists, False if the download failed (e.g., 404 or network error).
    """
    if dest.exists():
        logger.info(f"Already exists, skipping: {dest.name}")
        return True

    logger.info(f"Downloading: {url}")
    try:
        response = requests.get(url, stream=True, timeout=60)
        if response.status_code == 404:
            logger.warning(f"Not found (404): {url}")
            return False
        response.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Saved: {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
        return True

    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        if dest.exists():
            dest.unlink()  # Remove partial file
        return False


def publish(payload: dict) -> None:
    """
    This function publishes a message to a RabbitMQ queue.
    It establishes a connection to the RabbitMQ server using the provided credentials and connection parameters. 
    The message is published to the specified queue in JSON format.

    Args:
        payload (dict): The message payload to be published to RabbitMQ.

    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
    )

    # Retry in case RabbitMQ is briefly unavailable
    for attempt in range(1, 6):
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            channel.basic_publish(
                exchange="",
                routing_key=RABBITMQ_QUEUE,
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


# ── Core ETL logic ─────────────────────────────────────────────────────────

def _push_metrics(registry: CollectorRegistry, job: str) -> None:
    """Push collected metrics to Prometheus Pushgateway. Logs a warning on failure."""
    try:
        push_to_gateway(PUSHGATEWAY_URL, job=job, registry=registry)
        logger.info(f"Metrics pushed to Pushgateway ({PUSHGATEWAY_URL}) for job '{job}'.")
    except Exception as e:
        logger.warning(f"Failed to push metrics to Pushgateway: {e}")


def get_expected_files() -> set[str]:
    """
    This function generates a set of expected filenames for the taxi trip data Parquet files based on the defined taxi types and a date range starting from January 2019 up to two months before the current date. 
    The filenames follow the format: {taxi_type}_{year}-{month:02d}.parquet.
    The function calculates the cutoff date as the first day of the current month minus three months to ensure that it only includes files that should be available for download, accounting for any potential delays in data availability.
    """

    expected = set()
    today = date.today()
    cutoff = date(today.year, today.month, 1) - relativedelta(months=3)
    cutoff = date(2023, 4, 1)  # For testing with fixed cutoff

    for taxi_type in TAXI_TYPES:
        current = START_DATE
        while current <= cutoff:
            expected.add(
                f"{taxi_type}_{current.year}-{current.month:02d}.parquet")
            current += relativedelta(months=1)

    return expected


def get_local_files() -> set[str]:
    """
    This function scans the local directory specified by RAW_DATA_DIR for any existing Parquet files and returns a set of their filenames.
    """
    local = set()
    for path in Path(RAW_DATA_DIR).rglob("*.parquet"):
        local.add(path.name)
    return local


def run_extraction() -> None:
    """
    This function performs the extraction process for the taxi trip data.
    It identifies the expected files, checks for missing files, downloads them if necessary,
    publishes the extraction status to RabbitMQ, and pushes Prometheus metrics.
    """
    run_start = time.time()
    logger.info("=== Starting extraction ===")

    # ── Prometheus metrics ─────────────────────────────────────────────────
    registry          = CollectorRegistry()
    files_downloaded  = Gauge(
        "etl_extract_files_downloaded_total",
        "Number of files downloaded in this run",
        registry=registry,
    )
    files_skipped     = Gauge(
        "etl_extract_files_skipped_total",
        "Number of files skipped because they were already present on disk",
        registry=registry,
    )
    files_failed      = Gauge(
        "etl_extract_files_failed_total",
        "Number of files that failed to download",
        registry=registry,
    )
    download_duration = Gauge(
        "etl_extract_download_duration_seconds",
        "Download duration per file in seconds",
        ["filename"],
        registry=registry,
    )
    total_duration    = Gauge(
        "etl_extract_total_duration_seconds",
        "Total extraction run duration in seconds",
        registry=registry,
    )
    last_success_ts   = Gauge(
        "etl_extract_last_success_timestamp",
        "Unix timestamp of the last successful extraction run",
        registry=registry,
    )
    # ──────────────────────────────────────────────────────────────────────

    expected = get_expected_files()
    local    = get_local_files()
    missing  = expected - local

    already_present = len(expected) - len(missing)  # files we can skip entirely

    if not missing:
        logger.info("All files already up to date — nothing to download.")
        files_downloaded.set(0)
        files_skipped.set(already_present)
        files_failed.set(0)
        total_duration.set(time.time() - run_start)
        last_success_ts.set(time.time())
        _push_metrics(registry, "etl_extract")
        publish({"event": "extraction_complete", "action": "no-op"})
        return

    logger.info(f"Found {len(missing)} new file(s) to download.")

    downloaded     = []
    downloaded_cnt = 0
    skipped_cnt    = already_present
    failures_cnt   = 0

    for filename in sorted(missing):
        # Parse taxi_type, year, month back from filename
        # e.g. "yellow_2024-01.parquet" → yellow, 2024, 1
        parts     = filename.replace(".parquet", "").rsplit("_", 1)
        taxi_type = parts[0]
        year, month = map(int, parts[1].split("-"))

        url  = build_url(taxi_type, year, month)
        dest = dest_path(taxi_type, year, month)

        # File may have appeared since we scanned (race condition) — count as skip
        if dest.exists():
            skipped_cnt += 1
            time.sleep(1)
            continue

        t0      = time.time()
        success = download_file(url, dest)
        elapsed = time.time() - t0

        if success:
            downloaded_cnt += 1
            download_duration.labels(filename=filename).set(elapsed)
            downloaded.append({
                "taxi_type": taxi_type,
                "year": year,
                "month": month,
                "path": str(dest),
            })
        else:
            failures_cnt += 1

        # Add a delay to avoid rate limiting
        time.sleep(1)

    files_downloaded.set(downloaded_cnt)
    files_skipped.set(skipped_cnt)
    files_failed.set(failures_cnt)
    total_duration.set(time.time() - run_start)
    last_success_ts.set(time.time())
    _push_metrics(registry, "etl_extract")

    logger.info(
        f"Extraction complete. {downloaded_cnt} new file(s) downloaded.")
    publish({
        "event": "extraction_complete",
        "path": RAW_DATA_DIR,
        "action": "downloaded",
    })


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Run immediately on startup (catches up on any missing files)
    run_extraction()

    # Then schedule monthly on the 15th at 02:00 UTC
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_extraction,
        trigger=CronTrigger(day=15, hour=2),
        name="monthly_extraction",
        misfire_grace_time=3600,
    )
    logger.info(
        "Scheduler started — next run on the 15th of each month at 02:00 UTC.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
