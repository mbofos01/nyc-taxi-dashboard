import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

# Import functions from the src directory
from src.main import (
    _is_valid_parquet,
    load_and_validate_file,
    normalise,
    clean,
    agg_zone_hourly,
    agg_daily_stats,
    agg_zone_time_buckets,
    agg_zone_anomaly_stats,
    find_pending_files,
    process_files,
)


class TestParquetValidation:
    """Test cases for parquet validation functions"""

    def test_is_valid_parquet_valid_file(self):
        """Test validation of valid parquet file"""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temp_file:
            # Write PAR1 magic bytes
            temp_file.write(b"PAR1")
            temp_file.write(b"x" * 100)
            temp_file.write(b"PAR1")
            temp_path = temp_file.name

        try:
            assert _is_valid_parquet(temp_path) == True
        finally:
            os.unlink(temp_path)

    def test_is_valid_parquet_invalid_magic(self):
        """Test validation of file with invalid magic bytes"""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temp_file:
            temp_file.write(b"INVALID")
            temp_file.write(b"x" * 100)
            temp_file.write(b"INVALID")
            temp_path = temp_file.name

        try:
            assert _is_valid_parquet(temp_path) == False
        finally:
            os.unlink(temp_path)

    def test_is_valid_parquet_nonexistent_file(self):
        """Test validation of nonexistent file"""
        assert _is_valid_parquet("/nonexistent/file.parquet") == False


class TestDataLoading:
    """Test cases for data loading and validation"""

    @patch("src.main._is_valid_parquet")
    @patch("src.main.SparkSession")
    def test_load_and_validate_file_success(self, mock_spark_session, mock_validate):
        """Test successful file loading and validation"""
        mock_validate.return_value = True

        # Mock Spark session and DataFrame
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.parquet.return_value = mock_df
        mock_spark_session.builder.getOrCreate.return_value = mock_spark

        result = load_and_validate_file("test.parquet", "yellow")

        assert result is not None
        mock_validate.assert_called_once_with("test.parquet")
        mock_spark.read.parquet.assert_called_once_with("test.parquet")

    @patch("src.main._is_valid_parquet")
    def test_load_and_validate_file_invalid_parquet(self, mock_validate):
        """Test loading of invalid parquet file"""
        mock_validate.return_value = False

        result = load_and_validate_file("invalid.parquet", "yellow")

        assert result is None
        mock_validate.assert_called_once_with("invalid.parquet")


class TestDataTransformation:
    """Test cases for data transformation functions"""

    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame for testing"""
        # This would need actual Spark session in real tests
        # For now, we'll mock the DataFrame operations
        pass

    @patch("src.main.SparkSession")
    def test_normalise_yellow_taxi(self, mock_spark_session):
        """Test normalisation for yellow taxi data"""
        # Mock DataFrame
        mock_df = MagicMock()
        mock_df.withColumnRenamed.return_value = mock_df

        result = normalise(mock_df, "yellow")

        # Verify column renames were called
        assert mock_df.withColumnRenamed.called

    @patch("src.main.SparkSession")
    def test_normalise_green_taxi(self, mock_spark_session):
        """Test normalisation for green taxi data"""
        mock_df = MagicMock()
        mock_df.withColumnRenamed.return_value = mock_df

        result = normalise(mock_df, "green")

        assert mock_df.withColumnRenamed.called

    @patch("src.main.F")
    def test_clean_data_filters(self, mock_F):
        """Test data cleaning filters"""
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df

        # Mock F functions
        mock_F.col.return_value = MagicMock()

        result = clean(mock_df)

        # Verify filters were applied
        assert mock_df.filter.called

    @patch("src.main.F")
    def test_agg_zone_hourly(self, mock_F):
        """Test zone hourly aggregation"""
        mock_df = MagicMock()
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df

        result = agg_zone_hourly(mock_df)

        # Verify groupBy and agg were called
        assert mock_df.groupBy.called
        assert mock_df.agg.called

    @patch("src.main.F")
    def test_agg_daily_stats(self, mock_F):
        """Test daily stats aggregation"""
        mock_df = MagicMock()
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df

        result = agg_daily_stats(mock_df)

        assert mock_df.groupBy.called
        assert mock_df.agg.called

    @patch("src.main.F")
    def test_agg_zone_time_buckets(self, mock_F):
        """Test zone time buckets aggregation"""
        mock_df = MagicMock()
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df

        result = agg_zone_time_buckets(mock_df)

        assert mock_df.groupBy.called
        assert mock_df.agg.called

    @patch("src.main.F")
    def test_agg_zone_anomaly_stats(self, mock_F):
        """Test zone anomaly stats aggregation"""
        mock_df = MagicMock()
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df

        result = agg_zone_anomaly_stats(mock_df)

        assert mock_df.groupBy.called
        assert mock_df.agg.called


class TestFileProcessing:
    """Test cases for file discovery and processing"""

    @patch("src.main.Path")
    @patch("src.main.r")
    def test_find_pending_files(self, mock_redis, mock_path):
        """Test finding pending files"""
        # Mock Redis
        mock_redis_client = MagicMock()
        mock_redis_client.smembers.return_value = {
            "processed1.parquet",
            "processed2.parquet",
        }
        # This is a bit complex to mock properly, but shows the structure

        # Mock Path operations
        mock_raw_dir = MagicMock()
        mock_file1 = MagicMock()
        mock_file1.name = "file1.parquet"
        mock_file2 = MagicMock()
        mock_file2.name = "file2.parquet"
        mock_raw_dir.glob.return_value = [mock_file1, mock_file2]

        # This would need more detailed mocking for full test
        # For now, just test that the function can be called
        pass

    @patch("src.main.load_and_validate_file")
    @patch("src.main.normalise")
    @patch("src.main.clean")
    @patch("src.main.agg_zone_hourly")
    @patch("src.main.write_parquet")
    @patch("src.main.r")
    def test_process_files_success(
        self, mock_redis, mock_write, mock_agg, mock_clean, mock_normalise, mock_load
    ):
        """Test successful file processing"""
        # Mock successful loading
        mock_df = MagicMock()
        mock_load.return_value = mock_df

        # Mock transformation chain
        mock_normalise.return_value = mock_df
        mock_clean.return_value = mock_df
        mock_agg.return_value = mock_df

        # Mock file paths
        mock_file = MagicMock()
        mock_file.name = "yellow_tripdata_2024-01.parquet"

        process_files([mock_file])

        # Verify the processing chain was called
        mock_load.assert_called_once()
        mock_normalise.assert_called_once()
        mock_clean.assert_called_once()
        mock_agg.assert_called_once()
        mock_write.assert_called()

    @patch("src.main.load_and_validate_file")
    def test_process_files_load_failure(self, mock_load):
        """Test file processing when loading fails"""
        mock_load.return_value = None

        mock_file = MagicMock()
        mock_file.name = "test.parquet"

        # Should not raise exception when loading fails
        process_files([mock_file])

        mock_load.assert_called_once()


class TestRedisIntegration:
    """Test cases for Redis operations"""

    @patch("src.main.r")
    def test_redis_tracking_operations(self, mock_redis):
        """Test Redis set operations for file tracking"""
        mock_redis_client = MagicMock()
        # Mock Redis operations that would be used in the transform process

        # This would test sadd, sismember operations
        pass


class TestSparkIntegration:
    """Test cases for Spark operations"""

    @patch("src.main.SparkSession")
    def test_spark_session_creation(self, mock_spark_session):
        """Test Spark session creation"""
        from src.main import get_spark

        mock_spark = MagicMock()
        mock_spark_session.builder.getOrCreate.return_value = mock_spark

        result = get_spark()

        assert result is not None
        mock_spark_session.builder.getOrCreate.assert_called_once()

    @patch("src.main.SparkSession")
    def test_write_parquet_operations(self, mock_spark_session):
        """Test parquet writing operations"""
        from src.main import write_parquet

        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_df.write.mode.return_value = mock_writer
        mock_writer.parquet.return_value = None

        write_parquet(mock_df, "test_output", merge=True)

        # Verify write operations
        mock_df.write.mode.assert_called_with("append")
        mock_writer.parquet.assert_called_once()


class TestMessageHandling:
    """Test cases for RabbitMQ message handling"""

    @patch("src.main.pika")
    @patch("src.main.process_files")
    @patch("src.main.find_pending_files")
    def test_on_message_processing(self, mock_find_files, mock_process, mock_pika):
        """Test message handler"""
        from src.main import on_message

        # Mock channel and method
        mock_ch = MagicMock()
        mock_method = MagicMock()

        # Mock message body
        import json

        message_body = json.dumps(
            {"event": "extraction_completed", "summary": "Test message"}
        )

        # Mock find_pending_files to return files
        mock_file = MagicMock()
        mock_find_files.return_value = [mock_file]

        on_message(mock_ch, mock_method, None, message_body)

        # Verify processing was triggered
        mock_find_files.assert_called_once()
        mock_process.assert_called_once_with([mock_file])
        mock_ch.basic_ack.assert_called_once_with(mock_method.delivery_tag)

    @patch("src.main.pika")
    @patch("src.main.find_pending_files")
    def test_on_message_no_pending_files(self, mock_find_files, mock_pika):
        """Test message handler when no files are pending"""
        from src.main import on_message

        mock_ch = MagicMock()
        mock_method = MagicMock()

        message_body = '{"event": "extraction_completed"}'
        mock_find_files.return_value = []

        on_message(mock_ch, mock_method, None, message_body)

        # Should ack but not process
        mock_find_files.assert_called_once()
        mock_ch.basic_ack.assert_called_once_with(mock_method.delivery_tag)
        # process_files should not be called
