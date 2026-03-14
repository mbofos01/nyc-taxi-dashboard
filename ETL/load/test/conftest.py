import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    # Cleanup
    import shutil

    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_env():
    """Mock environment variables for testing"""
    env_vars = {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "nyc_taxi",
        "POSTGRES_USER": "nyc",
        "POSTGRES_PASSWORD": "nyc",
        "PROCESSED_DATA_DIR": "/data/processed",
        "RABBITMQ_HOST": "rabbitmq",
        "RABBITMQ_PORT": "5672",
        "RABBITMQ_USER": "guest",
        "RABBITMQ_PASSWORD": "guest",
        "RABBITMQ_T_QUEUE": "etl.transformed",
        "RABBITMQ_L_EXCHANGE": "etl.loaded",
        "PUSHGATEWAY_URL": "http://pushgateway:9091",
        "REDIS_HOST": "redis",
        "REDIS_PORT": "6379",
        "REDIS_TRACKING_ROOT": "etl:tracking",
        "REDIS_LOADED_FLAG": "loaded_flag",
        "REDIS_LOADED_DIRS_HASH": "loaded_dirs",
        "LOAD_CRON_HOUR": "3",
        "LOAD_CRON_MINUTE": "30",
    }

    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def mock_db_connection():
    """Mock PostgreSQL connection"""
    with patch("src.main.psycopg2.connect") as mock_connect:
        mock_conn = mock_connect.return_value
        yield mock_conn


@pytest.fixture
def mock_redis():
    """Mock Redis client"""
    with patch("src.main.r") as mock_redis:
        yield mock_redis


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame for testing"""
    import pandas as pd

    return pd.DataFrame(
        {
            "pickup_location_id": [1, 2, 3],
            "pickup_hour": [10, 11, 12],
            "trip_count": [100, 150, 200],
            "avg_fare": [15.5, 12.3, 18.7],
        }
    )


@pytest.fixture
def mock_rabbitmq():
    """Mock RabbitMQ connection"""
    with patch("src.main.pika") as mock_pika:
        mock_connection = mock_pika.BlockingConnection.return_value
        mock_channel = mock_connection.channel.return_value
        yield mock_channel
