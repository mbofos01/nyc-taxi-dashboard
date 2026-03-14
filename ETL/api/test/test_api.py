import pytest
import os
import json
from unittest.mock import Mock, patch, MagicMock, ANY
from fastapi.testclient import TestClient
from datetime import datetime, timezone

# Import the FastAPI app from the src directory
from src.main import app, _publish, _publish_fanout, _clear_dir, _now


class TestAPIEndpoints:
    """Test cases for FastAPI endpoints"""

    def setup_method(self):
        """Set up test client and mock environment variables"""
        self.client = TestClient(app)

        # Mock environment variables
        self.env_patcher = patch.dict(
            os.environ,
            {
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
            },
        )
        self.env_patcher.start()

    def teardown_method(self):
        """Clean up patches"""
        self.env_patcher.stop()

    @patch("src.main.r")
    def test_health_endpoint(self, mock_redis):
        """Test health check endpoint"""
        response = self.client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    @patch("src.main._publish")
    def test_trigger_extract_endpoint(self, mock_publish):
        """Test extract trigger endpoint"""
        response = self.client.post("/etl/extract")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "triggered"
        assert data["service"] == "extract"
        assert data["queue"] == "etl.cmd.extract"
        assert "timestamp" in data

        # Verify _publish was called with correct arguments
        mock_publish.assert_called_once()
        call_args = mock_publish.call_args[0]
        assert call_args[0] == "etl.cmd.extract"
        assert call_args[1]["command"] == "run"
        assert call_args[1]["service"] == "extract"
        assert call_args[1]["triggered_by"] == "api"

    @patch("src.main._publish")
    def test_trigger_transform_endpoint(self, mock_publish):
        """Test transform trigger endpoint"""
        response = self.client.post("/etl/transform")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "triggered"
        assert data["service"] == "transform"
        assert data["queue"] == "etl.extracted"

        mock_publish.assert_called_once_with(
            "etl.extracted",
            {
                "event": "extraction_completed",
                "triggered_by": "api",
                "summary": "Triggered via API",
                "timestamp": ANY,
            },
        )

    @patch("src.main._publish")
    def test_trigger_load_endpoint(self, mock_publish):
        """Test load trigger endpoint"""
        response = self.client.post("/etl/load")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "triggered"
        assert data["service"] == "load"
        assert data["queue"] == "etl.transformed"

    @patch("src.main._publish_fanout")
    def test_trigger_models_endpoint(self, mock_publish_fanout):
        """Test models trigger endpoint"""
        response = self.client.post("/etl/models")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "triggered"
        assert data["service"] == "loaded-fanout"
        assert data["queue"] == "etl.loaded"

        mock_publish_fanout.assert_called_once_with(
            "etl.loaded",
            {
                "event": "load_completed",
                "triggered_by": "api",
                "summary": "Triggered via API",
                "timestamp": ANY,
            },
        )


class TestRabbitMQFunctions:
    """Test cases for RabbitMQ helper functions"""

    @patch("pika.BlockingConnection")
    def test_publish_success(self, mock_connection):
        """Test successful message publishing"""
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel

        payload = {"test": "data"}
        _publish("test_queue", payload)

        mock_connection.assert_called_once()
        mock_channel.queue_declare.assert_called_once_with(
            queue="test_queue", durable=True
        )
        mock_channel.basic_publish.assert_called_once()

    @patch("pika.BlockingConnection")
    def test_publish_failure(self, mock_connection):
        """Test message publishing failure"""
        mock_connection.side_effect = Exception("Connection failed")

        with pytest.raises(Exception):  # Should raise HTTPException
            _publish("test_queue", {"test": "data"})

    @patch("pika.BlockingConnection")
    def test_publish_fanout_success(self, mock_connection):
        """Test successful fanout publishing"""
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel

        payload = {"test": "data"}
        _publish_fanout("test_exchange", payload)

        mock_connection.assert_called_once()
        mock_channel.exchange_declare.assert_called_once_with(
            exchange="test_exchange", exchange_type="fanout", durable=True
        )
        mock_channel.basic_publish.assert_called_once()


class TestUtilityFunctions:
    """Test cases for utility functions"""

    @patch("os.path.exists")
    @patch("os.scandir")
    @patch("os.remove")
    @patch("shutil.rmtree")
    def test_clear_dir_with_files_and_dirs(
        self, mock_rmtree, mock_remove, mock_scandir, mock_exists
    ):
        """Test clearing directory with mixed content"""
        mock_exists.return_value = True

        # Mock directory entry
        mock_dir_entry = MagicMock()
        mock_dir_entry.is_dir.return_value = True
        mock_dir_entry.path = "/test/dir"

        # Mock file entry
        mock_file_entry = MagicMock()
        mock_file_entry.is_dir.return_value = False
        mock_file_entry.path = "/test/file.txt"

        mock_scandir.return_value = [mock_dir_entry, mock_file_entry]

        _clear_dir("/test/path")

        mock_rmtree.assert_called_once_with("/test/dir")
        mock_remove.assert_called_once_with("/test/file.txt")

    @patch("os.path.exists")
    def test_clear_dir_nonexistent(self, mock_exists):
        """Test clearing nonexistent directory"""
        mock_exists.return_value = False

        # Should not raise exception
        _clear_dir("/nonexistent/path")

    def test_now_function(self):
        """Test timestamp generation"""
        timestamp = _now()

        # Should be in expected format
        assert "UTC" in timestamp
        assert len(timestamp) > 10

        # Should be parseable
        datetime.strptime(timestamp.replace(" UTC", ""), "%Y-%m-%d %H:%M:%S")


class TestInvalidationEndpoints:
    """Test cases for invalidation endpoints"""

    def setup_method(self):
        """Set up test client"""
        self.client = TestClient(app)

        self.env_patcher = patch.dict(
            os.environ,
            {
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
            },
        )
        self.env_patcher.start()

    def teardown_method(self):
        """Clean up patches"""
        self.env_patcher.stop()

    @patch("src.main.r")
    @patch("src.main._clear_dir")
    def test_invalidate_extract(self, mock_clear_dir, mock_redis):
        """Test extract invalidation"""
        mock_redis.delete.return_value = 1

        response = self.client.delete("/invalidate/extract")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "deleted"
        assert "spark:processed_files" in data["keys_deleted"]

        mock_redis.delete.assert_called_once_with("spark:processed_files")
        mock_clear_dir.assert_called_once_with("/data/raw")

    @patch("src.main.r")
    @patch("src.main._clear_dir")
    def test_invalidate_transform(self, mock_clear_dir, mock_redis):
        """Test transform invalidation"""
        mock_redis.delete.return_value = 1

        response = self.client.delete("/invalidate/transform")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "invalidated"
        assert "spark:processed_files" in data["keys_deleted"]

        mock_redis.delete.assert_called_once_with("spark:processed_files")
        mock_clear_dir.assert_called_once_with("/data/processed")

    @patch("src.main.r")
    def test_invalidate_load(self, mock_redis):
        """Test load invalidation"""
        mock_redis.delete.return_value = 1
        mock_redis.set.return_value = True

        response = self.client.delete("/invalidate/load")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "invalidated"
        assert "spark:loaded_dirs" in data["keys_deleted"]
        assert "spark:loaded_flag" in data["keys_deleted"]

        mock_redis.delete.assert_called_once_with("spark:loaded_dirs")
        mock_redis.set.assert_called_once_with("spark:loaded_flag", "1")

    @patch("src.main.r")
    @patch("src.main._clear_dir")
    def test_invalidate_pipeline(self, mock_clear_dir, mock_redis):
        """Test full pipeline invalidation"""
        mock_redis.delete.return_value = 2  # Two keys deleted
        mock_redis.set.return_value = True

        response = self.client.delete("/invalidate/pipeline")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "invalidated"
        assert len(data["keys_deleted"]) == 3  # Two deletes + one set

        # Verify Redis operations
        assert mock_redis.delete.call_count == 1
        assert mock_redis.set.call_count == 1

        # Verify directory clearing
        assert mock_clear_dir.call_count == 2
        mock_clear_dir.assert_any_call("/data/processed")
        mock_clear_dir.assert_any_call("/data/raw")
