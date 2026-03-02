import os
import json
import logging
import sys
from datetime import datetime, timezone

import pika
import redis as redis_client
from fastapi import FastAPI, HTTPException
from fastapi.openapi.docs import get_redoc_html
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import shutil

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")

RABBITMQ_CMD_EXTRACT = os.getenv("RABBITMQ_CMD_EXTRACT")
RABBITMQ_E_QUEUE = os.getenv("RABBITMQ_E_QUEUE")
RABBITMQ_T_QUEUE = os.getenv("RABBITMQ_T_QUEUE")
RABBITMQ_L_EXCHANGE = os.getenv("RABBITMQ_L_EXCHANGE")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_TRACKING_ROOT = os.getenv("REDIS_TRACKING_ROOT")
REDIS_PROCESSED_SET = os.getenv("REDIS_PROCESSED_SET")
REDIS_LOADED_FLAG = os.getenv("REDIS_LOADED_FLAG")
REDIS_LOADED_DIRS_HASH = os.getenv("REDIS_LOADED_DIRS_HASH")

PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")


r = redis_client.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# ── App ────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="NYC Taxi ETL Control API",
    description=(
        "Trigger ETL pipeline stages on demand and manage Redis and filesystem state.\n\n"
        "## Triggers\n"
        "Publish messages to RabbitMQ to start extract, transform, or load. "
        "Use `/etl/models` to broadcast directly to the fanout exchange and trigger all model services.\n\n"
        "## Invalidation\n"
        "Force individual pipeline stages or the entire pipeline to re-run by clearing Redis tracking keys "
        "and optionally wiping raw or processed data directories."
    ),
    version="1.0.0",
    openapi_tags=[
        {"name": "health", "description": "Service health"},
        {"name": "triggers", "description": "Trigger ETL pipeline stages via RabbitMQ"},
        {
            "name": "invalidation",
            "description": "Force pipeline stages to re-run by clearing Redis state and/or deleting data files",
        },
    ],
    redoc_url=None,  # disable default, overridden below
)


@app.get("/redoc", include_in_schema=False)
def redoc() -> HTMLResponse:
    return get_redoc_html(
        openapi_url="/openapi.json",
        title="NYC Taxi ETL Control API — ReDoc",
        redoc_js_url="https://unpkg.com/redoc@latest/bundles/redoc.standalone.js",
    )


# ── Pydantic models ────────────────────────────────────────────────────────
class TriggerResponse(BaseModel):
    status: str
    service: str
    queue: str
    timestamp: str


class InvalidateResponse(BaseModel):
    status: str
    keys_deleted: list[str]
    timestamp: str


# ── RabbitMQ helper ────────────────────────────────────────────────────────
def _publish(queue: str, payload: dict) -> None:
    """Publish a message to the given queue."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2,
    )
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        connection.close()
        logger.info(f"Published to '{queue}': {payload}")
    except Exception as e:
        logger.error(f"Failed to publish to '{queue}': {e}")
        raise HTTPException(status_code=503, detail=f"RabbitMQ unavailable: {e}")


def _publish_fanout(exchange: str, payload: dict) -> None:
    """Publish a message to a fanout exchange."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2,
    )
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(
            exchange=exchange,
            exchange_type="fanout",
            durable=True,
        )
        channel.basic_publish(
            exchange=exchange,
            routing_key="",  # ignored by fanout
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        connection.close()
        logger.info(f"Published to fanout exchange '{exchange}': {payload}")
    except Exception as e:
        logger.error(f"Failed to publish to fanout exchange '{exchange}': {e}")
        raise HTTPException(status_code=503, detail=f"RabbitMQ unavailable: {e}")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _clear_dir(path: str) -> None:
    """Delete all contents of *path* without removing the directory itself."""
    if not os.path.exists(path):
        return
    for entry in os.scandir(path):
        if entry.is_dir():
            shutil.rmtree(entry.path)
        else:
            os.remove(entry.path)
    logger.info(f"Cleared contents of {path}")


# ── Routes: health ─────────────────────────────────────────────────────────
@app.get("/health", summary="Health check", tags=["health"])
def health():
    return {"status": "ok"}


# ── Routes: ETL triggers ───────────────────────────────────────────────────
@app.post(
    "/etl/extract",
    response_model=TriggerResponse,
    tags=["triggers"],
    summary="Trigger the extract service",
    description="Publishes a run command to the extract command queue.",
)
def trigger_extract():
    _publish(
        RABBITMQ_CMD_EXTRACT,
        {
            "command": "run",
            "service": "extract",
            "triggered_by": "api",
            "timestamp": _now(),
        },
    )
    return TriggerResponse(
        status="triggered",
        service="extract",
        queue=RABBITMQ_CMD_EXTRACT,
        timestamp=_now(),
    )


@app.post(
    "/etl/transform",
    response_model=TriggerResponse,
    tags=["triggers"],
    summary="Trigger the transform service",
    description=(
        "Publishes a pipeline event to etl.extracted. "
        "Transform picks it up and processes any pending files via Redis-tracked state."
    ),
)
def trigger_transform():
    _publish(
        RABBITMQ_E_QUEUE,
        {
            "event": "extraction_completed",
            "triggered_by": "api",
            "summary": "Triggered via API",
            "timestamp": _now(),
        },
    )
    return TriggerResponse(
        status="triggered",
        service="transform",
        queue=RABBITMQ_E_QUEUE,
        timestamp=_now(),
    )


@app.post(
    "/etl/load",
    response_model=TriggerResponse,
    tags=["triggers"],
    summary="Trigger the load service",
    description=(
        "Publishes a pipeline event to etl.transformed. "
        "Load picks it up and loads all pending data via Redis-tracked state."
    ),
)
def trigger_load():
    _publish(
        RABBITMQ_T_QUEUE,
        {
            "event": "transform_completed",
            "triggered_by": "api",
            "summary": "Triggered via API",
            "timestamp": _now(),
        },
    )
    return TriggerResponse(
        status="triggered", service="load", queue=RABBITMQ_T_QUEUE, timestamp=_now()
    )


@app.post(
    "/etl/models",
    response_model=TriggerResponse,
    tags=["triggers"],
    summary="Broadcast to the loaded fanout exchange",
    description=(
        f"Publishes directly to the `{RABBITMQ_L_EXCHANGE}` fanout exchange. "
        "Every model service bound to that exchange (e.g. fare model) will receive the message and start training. "
    ),
)
def trigger_loaded():
    _publish_fanout(
        RABBITMQ_L_EXCHANGE,
        {
            "event": "load_completed",
            "triggered_by": "api",
            "summary": "Triggered via API",
            "timestamp": _now(),
        },
    )
    return TriggerResponse(
        status="triggered",
        service="loaded-fanout",
        queue=RABBITMQ_L_EXCHANGE,
        timestamp=_now(),
    )


# ── Routes: ETL Invalidation ─────────────────────────────────────────────
@app.delete(
    "/invalidate/extract",
    response_model=InvalidateResponse,
    tags=["invalidation"],
    summary="Invalidate Extract step",
    description=(
        f"Deletes all files under `{RAW_DATA_DIR}` and clears the "
        f"Redis processed-files set (`{REDIS_TRACKING_ROOT}:{REDIS_PROCESSED_SET}`). "
        "The next extract run will re-download and the next transform will re-process everything."
    ),
)
def invalidate_extract():
    redis_key = f"{REDIS_TRACKING_ROOT}:{REDIS_PROCESSED_SET}"
    try:
        r.delete(redis_key)
        _clear_dir(RAW_DATA_DIR)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to delete raw data: {e}")
    return InvalidateResponse(
        status="deleted", keys_deleted=[redis_key], timestamp=_now()
    )


@app.delete(
    "/invalidate/transform",
    response_model=InvalidateResponse,
    tags=["invalidation"],
    summary="Invalidate Transform step",
    description=(
        f"Deletes the Redis processed-files set (`{REDIS_TRACKING_ROOT}:{REDIS_PROCESSED_SET}`). "
        "On the next run, transform will treat ALL raw files as pending and re-transform everything."
    ),
)
def invalidate_transform():
    key = f"{REDIS_TRACKING_ROOT}:{REDIS_PROCESSED_SET}"
    try:
        r.delete(key)
        logger.info(f"Invalidated Redis key: {key}")
        _clear_dir(PROCESSED_DATA_DIR)

    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {e}")

    return InvalidateResponse(
        status="invalidated", keys_deleted=[key], timestamp=_now()
    )


@app.delete(
    "/invalidate/load",
    response_model=InvalidateResponse,
    tags=["invalidation"],
    summary="Invalidate Load step",
    description=(
        f"Deletes the loaded-dirs hash (`{REDIS_TRACKING_ROOT}:{REDIS_LOADED_DIRS_HASH}`) so the cron will "
        "treat every processed dir as pending, and sets "
        f"`{REDIS_TRACKING_ROOT}:{REDIS_LOADED_FLAG}` to `1` so the message-triggered path will also run. "
        "Both load paths will re-load all data on their next execution."
    ),
)
def invalidate_load():
    hash_key = f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_DIRS_HASH}"
    flag_key = f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_FLAG}"
    try:
        r.delete(hash_key)
        r.set(flag_key, "1")
        logger.info(f"Invalidated Redis keys: {hash_key}, set {flag_key}=1")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {e}")
    return InvalidateResponse(
        status="invalidated", keys_deleted=[hash_key, flag_key], timestamp=_now()
    )


@app.delete(
    "/invalidate/pipeline",
    response_model=InvalidateResponse,
    tags=["invalidation"],
    summary="Invalidate entire pipeline",
    description=(
        "Nuclear reset: clears the `processed_files` set and `loaded_dirs` hash from Redis, "
        f"sets `{REDIS_TRACKING_ROOT}:{REDIS_LOADED_FLAG}` to `1`, "
        f"and wipes both `{PROCESSED_DATA_DIR}` and `{RAW_DATA_DIR}`. "
        "Every stage — extract, transform, and load — will fully re-run from scratch."
    ),
)
def invalidate_pipeline():
    keys = [
        f"{REDIS_TRACKING_ROOT}:{REDIS_PROCESSED_SET}",
        f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_DIRS_HASH}",
    ]
    flag_key = f"{REDIS_TRACKING_ROOT}:{REDIS_LOADED_FLAG}"
    try:
        r.delete(*keys)
        r.set(flag_key, "1")
        logger.info(f"Invalidated Redis keys: {keys}, set {flag_key}=1")
        _clear_dir(PROCESSED_DATA_DIR)
        _clear_dir(RAW_DATA_DIR)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {e}")
    return InvalidateResponse(
        status="invalidated", keys_deleted=keys + [flag_key], timestamp=_now()
    )
