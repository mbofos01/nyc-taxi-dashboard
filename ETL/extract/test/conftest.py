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
        "TLC_BASE_URL": "https://d37ci6vzurychx.cloudfront.net/trip-data",
        "RAW_DATA_DIR": "/data/raw",
        "LOG_DIR": "/app/logs",
        "SERVER_TIMEOUT": "15",
        "START_YEAR": "2025",
        "START_MONTH": "9",
        "START_DAY": "1",
        "RABBITMQ_HOST": "rabbitmq",
        "RABBITMQ_PORT": "5672",
        "RABBITMQ_USER": "guest",
        "RABBITMQ_PASSWORD": "guest",
        "RABBITMQ_QUEUE": "etl.extracted",
        "RABBITMQ_CMD_QUEUE": "etl.cmd.extract",
        "PUSHGATEWAY_URL": "http://pushgateway:9091",
        "EXTRACT_CRON_DAY": "15",
        "EXTRACT_CRON_HOUR": "3",
    }

    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def sample_parquet_content():
    """Create sample parquet file content with valid magic bytes"""
    return b"PAR1" + b"x" * 100 + b"PAR1"
