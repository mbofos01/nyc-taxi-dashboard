import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from enum import Enum

# Import functions from the src directory
from src.main import (
    _file_path_builder,
    _is_valid_parquet,
    _download_file,
    DownloadResult,
    TAXI_TYPES,
)


class TestFilePathBuilder:
    """Test cases for _file_path_builder function"""

    def test_build_filename_only(self):
        """Test building just the filename"""
        result = _file_path_builder("yellow", 2024, 10)
        assert result == "yellow_tripdata_2024-10.parquet"

    def test_build_url(self):
        """Test building URL"""
        result = _file_path_builder(
            "yellow", 2024, 10, create_url=True, base_url="https://example.com"
        )
        assert result == "https://example.com/yellow_tripdata_2024-10.parquet"

    def test_build_full_path(self):
        """Test building full file path"""
        result = _file_path_builder(
            "yellow", 2024, 10, create_full_path=True, raw_file_dir="/data/raw"
        )
        expected = Path("/data/raw/yellow_tripdata_2024-10.parquet")
        assert result == expected

    def test_invalid_taxi_type(self):
        """Test invalid taxi type raises assertion error"""
        with pytest.raises(AssertionError, match="Invalid taxi type"):
            _file_path_builder("invalid", 2024, 10)

    def test_invalid_month_low(self):
        """Test invalid month (too low) raises assertion error"""
        with pytest.raises(AssertionError, match="Invalid month"):
            _file_path_builder("yellow", 2024, 0)

    def test_invalid_month_high(self):
        """Test invalid month (too high) raises assertion error"""
        with pytest.raises(AssertionError, match="Invalid month"):
            _file_path_builder("yellow", 2024, 13)

    def test_mutually_exclusive_flags(self):
        """Test that create_full_path and create_url are mutually exclusive"""
        with pytest.raises(AssertionError, match="Cannot create both"):
            _file_path_builder(
                "yellow", 2024, 10, create_full_path=True, create_url=True
            )

    @pytest.mark.parametrize("taxi_type", TAXI_TYPES)
    def test_all_valid_taxi_types(self, taxi_type):
        """Test all valid taxi types"""
        result = _file_path_builder(taxi_type, 2024, 10)
        assert taxi_type in result
        assert "2024-10" in result


class TestParquetValidation:
    """Test cases for _is_valid_parquet function"""

    def test_valid_parquet_file(self):
        """Test validation of a valid parquet file"""
        # Create a temporary file with PAR1 magic bytes
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"PAR1")  # Header
            temp_file.write(b"x" * 100)  # Some content
            temp_file.write(b"PAR1")  # Footer
            temp_path = Path(temp_file.name)

        try:
            assert _is_valid_parquet(temp_path) == True
        finally:
            temp_path.unlink()

    def test_invalid_header(self):
        """Test file with invalid header"""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"INVALID")  # Wrong header
            temp_file.write(b"x" * 100)
            temp_file.write(b"PAR1")  # Correct footer
            temp_path = Path(temp_file.name)

        try:
            assert _is_valid_parquet(temp_path) == False
        finally:
            temp_path.unlink()

    def test_invalid_footer(self):
        """Test file with invalid footer"""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"PAR1")  # Correct header
            temp_file.write(b"x" * 100)
            temp_file.write(b"INVALID")  # Wrong footer
            temp_path = Path(temp_file.name)

        try:
            assert _is_valid_parquet(temp_path) == False
        finally:
            temp_path.unlink()

    def test_nonexistent_file(self):
        """Test validation of nonexistent file"""
        nonexistent_path = Path("/nonexistent/file.parquet")
        assert _is_valid_parquet(nonexistent_path) == False

    def test_empty_file(self):
        """Test validation of empty file"""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = Path(temp_file.name)

        try:
            assert _is_valid_parquet(temp_path) == False
        finally:
            temp_path.unlink()

    def test_corrupt_file_exception(self):
        """Test handling of file read exceptions"""
        # Create a directory instead of a file to cause an exception
        with tempfile.TemporaryDirectory() as temp_dir:
            dir_path = Path(temp_dir)
            assert _is_valid_parquet(dir_path) == False


class TestDownloadFile:
    """Test cases for _download_file function"""

    def setup_method(self):
        """Set up test fixtures"""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.test_url = "http://nonexistent-domain-12345.com/test.parquet"
        self.dest_path = self.temp_dir / "test.parquet"

    def teardown_method(self):
        """Clean up test fixtures"""
        # Remove temp directory and contents
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch("requests.head")
    def test_file_already_exists(self, mock_head):
        """Test when destination file already exists"""
        # Create the file first
        self.dest_path.touch()

        result = _download_file(self.test_url, self.dest_path)

        assert result == DownloadResult.ALREADY_EXISTS
        mock_head.assert_not_called()  # Should not check availability

    @patch("requests.head")
    def test_head_request_404(self, mock_head):
        """Test 404 response from HEAD request"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_head.return_value = mock_response

        result = _download_file(self.test_url, self.dest_path)

        assert result == DownloadResult.NOT_FOUND
        mock_head.assert_called_once_with(
            self.test_url, timeout=15, allow_redirects=True
        )

    @patch("requests.head")
    def test_head_request_403(self, mock_head):
        """Test 403 response from HEAD request"""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_head.return_value = mock_response

        result = _download_file(self.test_url, self.dest_path)

        assert result == DownloadResult.ACCESS_DENIED

    @patch("src.main.requests.head")
    def test_head_request_other_error(self, mock_head):
        """Test non-OK status code from HEAD request"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.ok = False
        mock_head.return_value = mock_response

        result = _download_file(self.test_url, self.dest_path)

        assert result == DownloadResult.UNAVAILABLE

    @patch("requests.head")
    def test_head_request_exception(self, mock_head):
        """Test exception during HEAD request"""
        mock_head.side_effect = Exception("Network error")

        result = _download_file(self.test_url, self.dest_path)

        assert result == DownloadResult.CHECK_FAILED

    @patch("src.main.requests.head")
    @patch("src.main.requests.get")
    @patch("src.main._is_valid_parquet")
    def test_successful_download(self, mock_validate, mock_get, mock_head):
        """Test successful file download"""
        # Mock HEAD response
        head_response = Mock()
        head_response.status_code = 200
        head_response.ok = True
        mock_head.return_value = head_response

        # Mock GET response
        get_response = Mock()
        get_response.status_code = 200
        get_response.iter_content.return_value = [b"PAR1", b"data", b"PAR1"]
        mock_get.return_value = get_response

        # Mock validation
        mock_validate.return_value = True

        result = _download_file(self.test_url, self.dest_path)

        assert result == DownloadResult.DOWNLOADED
        assert self.dest_path.exists()

        mock_get.assert_called_once_with(self.test_url, stream=True, timeout=60)
        mock_validate.assert_called_once_with(self.dest_path)

    @patch("src.main.requests.head")
    @patch("src.main.requests.get")
    @patch("src.main._is_valid_parquet")
    def test_download_corrupt_retry_success(self, mock_validate, mock_get, mock_head):
        """Test download with corrupt file that succeeds on retry"""
        # Mock HEAD response
        head_response = Mock()
        head_response.status_code = 200
        head_response.ok = True
        mock_head.return_value = head_response

        # Mock GET responses - first corrupt, second valid
        get_response1 = Mock()
        get_response1.status_code = 200
        get_response1.iter_content.return_value = [b"PAR1", b"corrupt", b"INVALID"]

        get_response2 = Mock()
        get_response2.status_code = 200
        get_response2.iter_content.return_value = [b"PAR1", b"valid", b"PAR1"]

        mock_get.side_effect = [get_response1, get_response2]

        # Mock validation - first fails, second succeeds
        mock_validate.side_effect = [False, True]

        result = _download_file(self.test_url, self.dest_path)

        assert result == DownloadResult.DOWNLOADED
        assert mock_get.call_count == 2  # Should retry
        assert mock_validate.call_count == 2  # Should validate twice

    @patch("src.main.requests.head")
    @patch("src.main.requests.get")
    @patch("src.main._is_valid_parquet")
    def test_download_retry_also_corrupt(self, mock_validate, mock_get, mock_head):
        """Test download with corrupt file that fails even on retry"""
        # Mock HEAD response
        head_response = Mock()
        head_response.status_code = 200
        head_response.ok = True
        mock_head.return_value = head_response

        # Mock GET responses - both corrupt
        get_response = Mock()
        get_response.status_code = 200
        get_response.iter_content.return_value = [b"PAR1", b"corrupt", b"INVALID"]
        mock_get.return_value = get_response

        # Mock validation - always fails
        mock_validate.return_value = False

        result = _download_file(self.test_url, self.dest_path)

        assert result == DownloadResult.DOWNLOAD_FAILED
        assert not self.dest_path.exists()  # File should be removed
        assert mock_get.call_count == 2  # Should retry once

    @patch("src.main.requests.head")
    @patch("src.main.requests.get")
    def test_download_get_request_failure(self, mock_get, mock_head):
        """Test failure during GET request"""
        # Mock HEAD response
        head_response = Mock()
        head_response.status_code = 200
        head_response.ok = True
        mock_head.return_value = head_response

        # Mock GET to raise exception
        mock_get.side_effect = Exception("Download failed")

        result = _download_file(self.test_url, self.dest_path)

        assert result == DownloadResult.DOWNLOAD_FAILED
        assert not self.dest_path.exists()  # File should be removed


class TestDownloadResultEnum:
    """Test cases for DownloadResult enum"""

    def test_enum_values(self):
        """Test that all expected enum values exist"""
        assert DownloadResult.ALREADY_EXISTS
        assert DownloadResult.DOWNLOADED
        assert DownloadResult.NOT_FOUND
        assert DownloadResult.ACCESS_DENIED
        assert DownloadResult.UNAVAILABLE
        assert DownloadResult.CHECK_FAILED
        assert DownloadResult.DOWNLOAD_FAILED

    def test_enum_uniqueness(self):
        """Test that enum values are unique"""
        values = [member.value for member in DownloadResult]
        assert len(values) == len(set(values))
