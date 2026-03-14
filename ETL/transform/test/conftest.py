import pytest
import os
from unittest.mock import patch


@pytest.fixture
def mock_env():
    """Mock environment variables for testing"""
    env_vars = {
        "RAW_DATA_DIR": "/data/raw",
        "PROCESSED_DATA_DIR": "/data/processed",
        "LOG_DIR": "/app/logs",
        "RABBITMQ_HOST": "rabbitmq",
        "RABBITMQ_PORT": "5672",
        "RABBITMQ_USER": "guest",
        "RABBITMQ_PASSWORD": "guest",
        "RABBITMQ_IN_QUEUE": "etl.extracted",
        "RABBITMQ_OUT_QUEUE": "etl.transformed",
        "PUSHGATEWAY_URL": "http://pushgateway:9091",
        "SPARK_MASTER_URL": "spark://spark-master:7077",
        "REDIS_HOST": "redis",
        "REDIS_PORT": "6379",
        "REDIS_TRACKING_ROOT": "etl:tracking",
        "REDIS_PROCESSED_SET": "processed_files",
        "REDIS_LOADED_FLAG": "loaded_flag",
        "CRON_HOUR": "2",
        "CRON_MINUTE": "10",
    }

    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def mock_spark_session():
    """Mock Spark session for testing"""
    with patch("src.main.SparkSession") as mock_session:
        mock_spark = mock_session.builder.getOrCreate.return_value
        yield mock_spark


@pytest.fixture
def mock_redis():
    """Mock Redis client for testing"""
    with patch("src.main.r") as mock_redis:
        yield mock_redis


@pytest.fixture
def sample_dataframe():
    """Create a mock DataFrame for testing"""
    mock_df = patch("pyspark.sql.DataFrame").return_value
    return mock_df
