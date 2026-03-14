import pytest
import os
from unittest.mock import patch


@pytest.fixture
def mock_env():
    """Mock environment variables for testing"""
    env_vars = {
        "RABBITMQ_HOST": "localhost",
        "RABBITMQ_PORT": "5672",
        "RABBITMQ_USER": "guest",
        "RABBITMQ_PASSWORD": "guest",
        "RABBITMQ_CMD_EXTRACT": "etl.cmd.extract",
        "RABBITMQ_E_QUEUE": "etl.extracted",
        "RABBITMQ_T_QUEUE": "etl.transformed",
        "RABBITMQ_L_EXCHANGE": "etl.loaded",
        "REDIS_HOST": "localhost",
        "REDIS_PORT": "6379",
        "REDIS_TRACKING_ROOT": "etl:tracking",
        "REDIS_PROCESSED_SET": "processed_files",
        "REDIS_LOADED_FLAG": "loaded_flag",
        "REDIS_LOADED_DIRS_HASH": "loaded_dirs",
        "PROCESSED_DATA_DIR": "/data/processed",
        "RAW_DATA_DIR": "/data/raw",
    }

    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def mock_redis():
    """Mock Redis client"""
    with patch("src.main.r") as mock_redis:
        yield mock_redis
