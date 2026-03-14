import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open
import pandas as pd
import psycopg2

# Import functions from the src directory
from src.main import (
    get_pg_conn,
    ensure_schema,
    upsert_dataframe,
    _loaded_dirs_key,
    _get_dir_mtime,
    _is_dir_pending,
    _mark_dir_loaded,
    find_pending_dirs,
    read_parquet_dir,
    run_load,
    publish_loaded,
)


class TestDatabaseConnection:
    """Test cases for database connection functions"""

    @patch("src.main.psycopg2.connect")
    def test_get_pg_conn_success(self, mock_connect):
        """Test successful PostgreSQL connection"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with patch.dict(
            os.environ,
            {
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "test_db",
                "POSTGRES_USER": "test_user",
                "POSTGRES_PASSWORD": "test_pass",
            },
        ):
            result = get_pg_conn()

            assert result == mock_conn
            mock_connect.assert_called_once()

    @patch("src.main.psycopg2.connect")
    def test_get_pg_conn_failure(self, mock_connect):
        """Test database connection failure"""
        mock_connect.side_effect = psycopg2.Error("Connection failed")

        with patch.dict(
            os.environ,
            {
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "test_db",
                "POSTGRES_USER": "test_user",
                "POSTGRES_PASSWORD": "test_pass",
            },
        ):
            with pytest.raises(psycopg2.Error):
                get_pg_conn()


class TestSchemaManagement:
    """Test cases for database schema operations"""

    def test_ensure_schema_creates_tables(self):
        """Test schema creation"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        ensure_schema(mock_conn)

        # Verify cursor was created and executed
        mock_conn.cursor.assert_called_once()
        # The cursor is used in a context manager, so we need to check the __enter__ result
        mock_cursor.__enter__.assert_called_once()
        # execute is called on the context manager result
        mock_cursor.__enter__().execute.assert_called_once()
        mock_cursor.__exit__.assert_called_once()
        mock_conn.commit.assert_called_once()


class TestDataUpsert:
    """Test cases for DataFrame upsert operations"""

    def test_upsert_dataframe_success(self):
        """Test successful DataFrame upsert"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock the cursor's connection for psycopg2 execute_values
        mock_cursor.connection.encoding = "UTF8"

        # Create sample DataFrame with required columns
        df = pd.DataFrame(
            {
                "pickup_date": ["2023-01-01", "2023-01-01", "2023-01-01"],
                "pickup_hour": [10, 11, 12],
                "pickup_location_id": [1, 2, 3],
                "taxi_type": ["yellow", "yellow", "yellow"],
                "trip_count": [100, 150, 200],
            }
        )

        upsert_dataframe(
            mock_conn,
            df,
            "zone_hourly",
            ["pickup_date", "pickup_hour", "pickup_location_id", "taxi_type"],
        )

        # Verify database operations
        mock_conn.cursor.assert_called_once()
        assert mock_cursor.executemany.called
        mock_cursor.close.assert_called_once()
        mock_conn.commit.assert_called_once()

    def test_upsert_dataframe_empty(self):
        """Test upsert with empty DataFrame"""
        mock_conn = MagicMock()

        df = pd.DataFrame()

        # Should not raise exception
        upsert_dataframe(mock_conn, df, "zone_hourly", "yellow")

        # Should not create cursor or execute
        mock_conn.cursor.assert_not_called()


class TestDirectoryTracking:
    """Test cases for directory tracking functions"""

    def test_loaded_dirs_key(self):
        """Test Redis key generation"""
        key = _loaded_dirs_key()
        assert key == "spark:loaded_dirs"

    def test_get_dir_mtime(self):
        """Test directory modification time retrieval"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            mtime = _get_dir_mtime(temp_path)

            assert isinstance(mtime, float)
            # Empty directory should return 0.0
            assert mtime == 0.0

    @patch("src.main.r")
    def test_is_dir_pending_new_directory(self, mock_redis):
        """Test pending check for new directory"""
        mock_redis.hget.return_value = None

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            result = _is_dir_pending("zone_hourly", "yellow", temp_path)

            assert result == True
            mock_redis.hget.assert_called_once()

    @patch("src.main.r")
    def test_is_dir_pending_modified_directory(self, mock_redis):
        """Test pending check for modified directory"""
        # Mock stored mtime as older
        mock_redis.hget.return_value = "-1"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            result = _is_dir_pending("zone_hourly", "yellow", temp_path)

            assert result == True

    @patch("src.main.r")
    def test_is_dir_pending_unchanged_directory(self, mock_redis):
        """Test pending check for unchanged directory"""
        # Mock stored mtime as current
        current_mtime = str(Path(tempfile.mkdtemp()).stat().st_mtime)
        mock_redis.hget.return_value = current_mtime

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            result = _is_dir_pending("zone_hourly", "yellow", temp_path)

            assert result == False

    @patch("src.main.r")
    def test_mark_dir_loaded(self, mock_redis):
        """Test marking directory as loaded"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            _mark_dir_loaded("zone_hourly", "yellow", temp_path)

            mock_redis.hset.assert_called_once()


class TestDirectoryDiscovery:
    """Test cases for directory discovery functions"""

    @patch("src.main._is_dir_pending")
    def test_find_pending_dirs_with_pending(self, mock_is_pending):
        """Test finding pending directories"""
        mock_is_pending.return_value = True

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create directory structure
            processed_dir = Path(temp_dir) / "processed"
            processed_dir.mkdir()

            zone_hourly_dir = processed_dir / "zone_hourly"
            zone_hourly_dir.mkdir()

            yellow_dir = zone_hourly_dir / "yellow"
            yellow_dir.mkdir()

            result = find_pending_dirs(str(processed_dir))

            assert len(result) > 0
            mock_is_pending.assert_called()

    @patch("src.main._is_dir_pending")
    def test_find_pending_dirs_no_pending(self, mock_is_pending):
        """Test when no directories are pending"""
        mock_is_pending.return_value = False

        with tempfile.TemporaryDirectory() as temp_dir:
            processed_dir = Path(temp_dir) / "processed"
            processed_dir.mkdir()

            result = find_pending_dirs(str(processed_dir))

            assert result == []


class TestParquetReading:
    """Test cases for parquet file reading"""

    @patch("src.main.pd.read_parquet")
    def test_read_parquet_dir_success(self, mock_read_parquet):
        """Test successful parquet directory reading"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mock_read_parquet.return_value = mock_df

        # Create a mock path with rglob returning files
        mock_path = MagicMock()
        mock_path.exists.return_value = True
        mock_file = MagicMock()
        mock_path.rglob.return_value = [mock_file]

        result = read_parquet_dir(mock_path)

        assert result is not None
        mock_read_parquet.assert_called_once_with(mock_file)

    @patch("src.main.pd.read_parquet")
    def test_read_parquet_dir_empty(self, mock_read_parquet):
        """Test reading empty parquet directory"""
        mock_read_parquet.side_effect = Exception("No files found")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            result = read_parquet_dir(temp_path)

            assert result is None

    @patch("src.main.pd.read_parquet")
    def test_read_parquet_dir_exception(self, mock_read_parquet):
        """Test exception handling during parquet reading"""
        mock_read_parquet.side_effect = Exception("Read error")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            result = read_parquet_dir(temp_path)

            assert result is None


class TestLoadOperations:
    """Test cases for load operations"""

    @patch("src.main.read_parquet_dir")
    @patch("src.main.upsert_dataframe")
    @patch("src.main._mark_dir_loaded")
    @patch("src.main.get_pg_conn")
    @patch("src.main.publish_loaded")
    def test_run_load_success(
        self,
        mock_publish,
        mock_conn,
        mock_mark_loaded,
        mock_upsert,
        mock_read,
    ):
        """Test successful load operation"""
        # Mock database connection
        mock_db_conn = MagicMock()
        mock_conn.return_value = mock_db_conn

        # Mock reading parquet data
        mock_df = pd.DataFrame(
            {
                "pickup_location_id": [1, 2],
                "pickup_hour": [10, 11],
                "trip_count": [100, 150],
            }
        )
        mock_read.return_value = mock_df

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create directory structure for zone_hourly/yellow
            dataset_dir = Path(temp_dir) / "zone_hourly"
            taxi_dir = dataset_dir / "yellow"
            taxi_dir.mkdir(parents=True)

            run_load(temp_dir)

            # Verify operations were called
            mock_read.assert_called_once()
            mock_upsert.assert_called_once()
            mock_mark_loaded.assert_called_once()
            mock_publish.assert_called_once()
            mock_db_conn.close.assert_called_once()

    @patch("src.main.find_pending_dirs")
    def test_run_load_no_pending(self, mock_find):
        """Test load operation when no directories are pending"""
        mock_find.return_value = []

        with tempfile.TemporaryDirectory() as temp_dir:
            # Should not raise exception
            run_load(temp_dir)

    @patch("src.main.find_pending_dirs")
    @patch("src.main.read_parquet_dir")
    def test_run_load_read_failure(self, mock_read, mock_find):
        """Test load operation when parquet reading fails"""
        mock_find.return_value = [("zone_hourly", "yellow")]
        mock_read.return_value = None

        with tempfile.TemporaryDirectory() as temp_dir:
            # Should not raise exception
            run_load(temp_dir)


class TestMessageHandling:
    """Test cases for RabbitMQ message handling"""

    @patch("src.main.pika")
    @patch("src.main.run_load")
    @patch("src.main.r")
    def test_on_message_load_trigger(self, mock_redis, mock_run_load, mock_pika):
        """Test message handler for load trigger"""
        from src.main import on_message

        mock_ch = MagicMock()
        mock_method = MagicMock()

        # Mock Redis flag check - flag is 1, should run load
        mock_redis.get.return_value = "1"

        message_body = '{"event": "transform_completed"}'

        with patch.dict(os.environ, {"PROCESSED_DATA_DIR": "/data/processed"}):
            on_message(mock_ch, mock_method, None, message_body)

            mock_run_load.assert_called_once_with("/data/processed")
            mock_redis.set.assert_called_once_with("spark:loaded_flag", "0")
            mock_ch.basic_ack.assert_called_once()

    @patch("src.main.pika")
    @patch("src.main.run_load")
    @patch("src.main.r")
    def test_on_message_load_skip(self, mock_redis, mock_run_load, mock_pika):
        """Test message handler when load should be skipped"""
        from src.main import on_message

        mock_ch = MagicMock()
        mock_method = MagicMock()

        # Mock Redis flag check - flag is 0, should skip
        mock_redis.get.return_value = "0"

        message_body = '{"event": "transform_completed"}'

        on_message(mock_ch, mock_method, None, message_body)

        # Should not run load
        mock_run_load.assert_not_called()
        mock_ch.basic_ack.assert_called_once()


class TestPublishing:
    """Test cases for message publishing"""

    @patch("src.main.pika")
    def test_publish_loaded_success(self, mock_pika):
        """Test successful load completion publishing"""
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        mock_pika.BlockingConnection.return_value = mock_connection

        payload = {"event": "data_loaded", "summary": "Test load"}

        publish_loaded(payload)

        mock_pika.BlockingConnection.assert_called_once()
        mock_channel.basic_publish.assert_called_once()

    @patch("src.main.pika")
    def test_publish_loaded_failure(self, mock_pika):
        """Test publishing failure"""
        mock_pika.BlockingConnection.side_effect = Exception("Connection failed")

        payload = {"event": "data_loaded"}

        # Function handles exceptions internally, doesn't re-raise
        publish_loaded(payload)


class TestScheduledOperations:
    """Test cases for scheduled load operations"""

    @patch("src.main.find_pending_dirs")
    @patch("src.main.run_load")
    def test_scheduled_load(self, mock_run_load, mock_find_pending):
        """Test scheduled load execution"""
        from src.main import scheduled_load

        # Mock find_pending_dirs to return some pending dirs
        mock_find_pending.return_value = [("zone_hourly", "yellow")]

        with patch.dict(os.environ, {"PROCESSED_DATA_DIR": "/data/processed"}):
            scheduled_load()

            mock_run_load.assert_called_once_with(
                "/data/processed", only=[("zone_hourly", "yellow")]
            )
