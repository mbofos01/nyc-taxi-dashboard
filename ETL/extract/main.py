import glob
import os
import logging
import sys
import time
import json
from datetime import date, datetime, timezone
from enum import Enum, auto
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
TLC_BASE_URL = os.getenv("TLC_BASE_URL")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
LOG_DIR = os.getenv("LOG_DIR")
SERVER_TIMEOUT = int(os.getenv("SERVER_TIMEOUT", 15))  # seconds
START_YEAR = int(os.getenv("START_YEAR", 2019))  # 2019
START_MONTH = int(os.getenv("START_MONTH", 1))
START_DAY = int(os.getenv("START_DAY", 1))
# START_DATE = date(2019, 1, 1)
START_DATE = date(START_YEAR, START_MONTH, START_DAY)
# TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]
TAXI_TYPES = ["yellow", "green"]

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_E_QUEUE")
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")

CRON_DAY = os.getenv("EXTRACT_CRON_DAY",  "15")   # day-of-month to run
CRON_HOUR = os.getenv("EXTRACT_CRON_HOUR", "2")    # UTC hour to run


# ── Download result ────────────────────────────────────────────────────────

class DownloadResult(Enum):
    ALREADY_EXISTS = auto()  # file was present on disk, skipped
    DOWNLOADED = auto()  # file fetched successfully
    NOT_FOUND = auto()  # HTTP 404 — data not released yet
    ACCESS_DENIED = auto()  # HTTP 403 — permission / CDN issue
    UNAVAILABLE = auto()  # other non-OK HTTP status
    CHECK_FAILED = auto()  # network error during HEAD check
    DOWNLOAD_FAILED = auto()  # network / IO error during GET


def _file_path_builder(taxi_type: str, year: int, month: int, create_full_path: bool = False, raw_file_dir: str = RAW_DATA_DIR, create_url: bool = False, base_url: str = TLC_BASE_URL) -> Path | str:
    """
    Builds the file path for a given taxi type, year, and month. Can either return a full local file path or a URL to the TLC data.

    Example: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet

    Args:
        taxi_type (str): The type of taxi (e.g., "yellow", "green").
        year (int): The year of the data.
        month (int): The month of the data.

    Returns:
        Path or str: The file path or URL for the specified taxi data.
    """
    global TAXI_TYPES
    assert taxi_type in TAXI_TYPES, f"Invalid taxi type: {taxi_type}"
    assert 1 <= month <= 12, f"Invalid month: {month}"
    assert not (
        create_full_path and create_url), "Cannot create both full path and URL at the same time"

    if create_url:
        return f"{base_url}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"

    if create_full_path:
        full_path = Path(raw_file_dir) / \
            f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        return full_path

    return f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def _is_valid_parquet(path: Path) -> bool:
    """
    Validate a parquet file by checking the PAR1 magic bytes at the
    start and end of the file. A partially downloaded file will have a
    valid header but a corrupt/missing tail.
    """
    MAGIC = b"PAR1"
    try:
        with open(path, "rb") as fh:
            header = fh.read(4)
            fh.seek(-4, 2)
            footer = fh.read(4)
        return header == MAGIC and footer == MAGIC
    except Exception as e:
        logger.warning(f"Could not validate parquet magic for {path}: {e}")
        return False


def _download_file(url: str, dest: Path) -> DownloadResult:
    """
    Downloads a file from the specified URL and saves it to dest.
    Performs a HEAD check first to confirm availability before streaming.

    Args:
        url (str): The URL of the file to download.
        dest (Path): The local file path where the downloaded file should be saved.

    Returns:
        DownloadResult: outcome of the operation.
    """
    if dest.exists():
        # theoretically should never hit this since we check before adding to the queue, but good to be safe
        logger.info(f"Already exists, skipping: {dest.name}")
        return DownloadResult.ALREADY_EXISTS

    logger.info(f"Checking availability: {url}")
    try:
        head = requests.head(url, timeout=15, allow_redirects=True)
        if head.status_code == 404:
            logger.warning(f"Not found (404): {url}")
            return DownloadResult.NOT_FOUND
        if head.status_code == 403:
            logger.warning(f"Access denied (403): {url}")
            return DownloadResult.ACCESS_DENIED
        if not head.ok:
            logger.warning(
                f"Resource unavailable (HTTP {head.status_code}): {url}")
            return DownloadResult.UNAVAILABLE
        logger.info(f"Resource available, downloading: {url}")
    except Exception as e:
        logger.error(f"Availability check failed for {url}: {e}")
        return DownloadResult.CHECK_FAILED

    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Saved: {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")

        if not _is_valid_parquet(dest):
            logger.warning(
                f"Downloaded file is corrupt, retrying once: {dest.name}")
            dest.unlink()
            # ── Single retry ───────────────────────────────────────────────
            try:
                response2 = requests.get(url, stream=True, timeout=60)
                response2.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in response2.iter_content(chunk_size=8192):
                        f.write(chunk)
                if not _is_valid_parquet(dest):
                    logger.error(
                        f"Retry also produced corrupt file, marking as failed: {dest.name}")
                    dest.unlink()
                    return DownloadResult.DOWNLOAD_FAILED
                logger.info(
                    f"Retry succeeded: {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
            except Exception as e:
                logger.error(f"Retry download failed for {url}: {e}")
                if dest.exists():
                    dest.unlink()
                return DownloadResult.DOWNLOAD_FAILED

        return DownloadResult.DOWNLOADED

    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        if dest.exists():
            dest.unlink()  # Remove partial file
        return DownloadResult.DOWNLOAD_FAILED


def _calculate_end_date():
    """
    Due to the way that TLC releases data, we want to set the end date to 3 months ago (the last month that should be fully available).

    Returns:
        date: The calculated end date.
    """
    today = date.today()
    return date(today.year, today.month, 1) - relativedelta(months=3)


def _check_existing_files(start_date: date, end_date: date, taxi_types: list, raw_data_dir: str = RAW_DATA_DIR) -> tuple[list[Path], int]:
    missing_urls = []
    existing_files = 0
    out = Path(LOG_DIR) / "check.log"
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w") as f:
        f.write(f"{'===' * 20} \n")
        f.write(
            f"Checking for existing files between {start_date} and {end_date} \n")
        for taxi_type in taxi_types:
            current = start_date
            while current <= end_date:
                expected_file = _file_path_builder(
                    taxi_type, current.year, current.month, create_full_path=True)
                if not expected_file.exists():
                    logger.warning(f"Missing file: {expected_file}")
                    target_url = _file_path_builder(
                        taxi_type, current.year, current.month, create_full_path=False, create_url=True)
                    target_path = _file_path_builder(
                        taxi_type, current.year, current.month, create_full_path=True)
                    missing_urls.append((target_url, target_path))
                elif not _is_valid_parquet(expected_file):
                    logger.warning(
                        f"Corrupt parquet file detected, deleting for re-download: {expected_file.name}")
                    f.write(
                        f"Corrupt (deleted, queued for re-download): {expected_file} \n")
                    expected_file.unlink()
                    target_url = _file_path_builder(
                        taxi_type, current.year, current.month, create_full_path=False, create_url=True)
                    target_path = _file_path_builder(
                        taxi_type, current.year, current.month, create_full_path=True)
                    missing_urls.append((target_url, target_path))
                else:
                    f.write(f"File exists: {expected_file} \n")
                    existing_files += 1
                current += relativedelta(months=1)
        f.write(f"{'===' * 20} ")

    return missing_urls, existing_files


def _results_summary(attempts: list[tuple[str, Path, DownloadResult]]) -> str:
    summary = {}
    for _, _, outcome in attempts:
        summary[outcome] = summary.get(outcome, 0) + 1
    return ", ".join(f"{outcome.name}: {count}" for outcome, count in summary.items())


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
            logger.info(f"Published to {RABBITMQ_QUEUE}: {payload}")
            return
        except Exception as e:
            logger.warning(f"RabbitMQ publish attempt {attempt}/5 failed: {e}")
            time.sleep(5)

    logger.error("Could not publish to RabbitMQ after 5 attempts.")


def run_extraction():
    run_start = time.time()
    targets, existing_files = _check_existing_files(
        START_DATE, _calculate_end_date(), TAXI_TYPES)

    # ── Prometheus metrics ─────────────────────────────────────────────────
    registry = CollectorRegistry()
    g_downloaded = Gauge("extract_files_downloaded_total",
                         "Files successfully downloaded this run",
                         registry=registry)
    g_skipped = Gauge("extract_files_skipped_total",
                      "Files skipped because they already existed on disk",
                      registry=registry)
    g_not_found = Gauge("extract_files_not_found_total",
                        "Files that returned HTTP 404 (not yet released)",
                        registry=registry)
    g_access_denied = Gauge("extract_files_access_denied_total",
                            "Files that returned HTTP 403",
                            registry=registry)
    g_unavailable = Gauge("extract_files_unavailable_total",
                          "Files that returned another non-OK HTTP status",
                          registry=registry)
    g_check_failed = Gauge("extract_files_check_failed_total",
                           "Files where the HEAD availability check failed",
                           registry=registry)
    g_download_failed = Gauge("extract_files_download_failed_total",
                              "Files where the GET download failed",
                              registry=registry)
    g_duration = Gauge("extract_total_duration_seconds",
                       "Total wall-clock time of the extraction run",
                       registry=registry)
    g_last_run = Gauge("extract_last_run_timestamp",
                       "Unix timestamp of the last extraction run",
                       registry=registry)
    g_file_size = Gauge("extract_file_size_bytes",
                        "Size of each raw parquet file on disk in bytes",
                        ["taxi_type", "filename"],
                        registry=registry)
    g_total_size = Gauge("extract_total_disk_bytes",
                         "Total bytes of all raw parquet files on disk",
                         registry=registry)
    g_outcome_duration = Gauge("extract_outcome_duration_seconds",
                               "Total seconds spent on files with each outcome",
                               ["outcome"],
                               registry=registry)
    # ──────────────────────────────────────────────────────────────────────

    counters = {r: 0 for r in DownloadResult}
    outcome_durations = {r: 0.0 for r in DownloadResult}
    attempts = []
    for url, path in targets:
        t0 = time.time()
        outcome = _download_file(url, path)
        outcome_durations[outcome] += time.time() - t0
        attempts.append((url, path, outcome))
        counters[outcome] += 1
        time.sleep(SERVER_TIMEOUT)  # be kind to the server

    g_downloaded.set(counters[DownloadResult.DOWNLOADED])
    g_skipped.set(counters[DownloadResult.ALREADY_EXISTS]+existing_files)
    g_not_found.set(counters[DownloadResult.NOT_FOUND])
    g_access_denied.set(counters[DownloadResult.ACCESS_DENIED])
    g_unavailable.set(counters[DownloadResult.UNAVAILABLE])
    g_check_failed.set(counters[DownloadResult.CHECK_FAILED])
    g_download_failed.set(counters[DownloadResult.DOWNLOAD_FAILED])
    for result, dur in outcome_durations.items():
        g_outcome_duration.labels(outcome=result.name.lower()).set(dur)
    g_duration.set(time.time() - run_start)
    g_last_run.set(time.time())

    # ── File sizes (all files currently on disk, including pre-existing) ──
    total_bytes = 0
    raw_dir = Path(RAW_DATA_DIR)
    for taxi_type in TAXI_TYPES:
        for f in raw_dir.glob(f"{taxi_type}_tripdata_*.parquet"):
            size = f.stat().st_size
            g_file_size.labels(taxi_type=taxi_type, filename=f.name).set(size)
            total_bytes += size
    g_total_size.set(total_bytes)

    try:
        push_to_gateway(PUSHGATEWAY_URL, job="etl_extract", registry=registry)
        logger.info(f"Metrics pushed to Pushgateway ({PUSHGATEWAY_URL}).")
    except Exception as e:
        logger.warning(f"Failed to push metrics: {e}")

    logger.info(f"Summary: {_results_summary(attempts)}")
    publish({
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "event": "extraction_completed",
        "summary": f"{counters[DownloadResult.DOWNLOADED]} new files downloaded",
        "new_files": counters[DownloadResult.DOWNLOADED],
    })


    # ── Entry point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Run immediately on startup (catches up on any missing files)
    run_extraction()

    # Then schedule monthly using configurable cron settings
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_extraction,
        trigger=CronTrigger(day=CRON_DAY, hour=CRON_HOUR),
        name="monthly_extraction",
        misfire_grace_time=3600,
    )
    logger.info(
        f"Scheduler started — next run on day={CRON_DAY} of each month at {CRON_HOUR}:00 UTC.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
