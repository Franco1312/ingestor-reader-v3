"""Tests for VersionManager."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from src.infrastructure.versioning.version_manager import VersionManager


class TestVersionManager:
    """Tests for VersionManager class."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        return Mock()

    @pytest.fixture
    def version_manager(self, mock_s3_client):
        """Create a VersionManager instance with mocked S3 client."""
        return VersionManager(bucket="test-bucket", s3_client=mock_s3_client)

    def test_create_new_version_generates_timestamp_based_version_id(self, version_manager):
        """Test that create_new_version generates a timestamp-based version ID."""
        with patch("src.infrastructure.versioning.version_manager.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 15, 14, 30, 22, 123456)
            mock_datetime.strftime = datetime.strftime

            version_id = version_manager.create_new_version()

            assert version_id.startswith("v")
            assert "20240115" in version_id
            assert "143022" in version_id
            assert "123456" in version_id  # microseconds

    def test_create_new_version_returns_unique_version_ids(self, version_manager):
        """Test that create_new_version returns unique version IDs on each call."""
        version_id1 = version_manager.create_new_version()
        version_id2 = version_manager.create_new_version()

        # With microseconds, even rapid calls should be unique
        assert version_id1 != version_id2

    def test_get_current_version_returns_none_when_no_index_exists(
        self, version_manager, mock_s3_client
    ):
        """Test that get_current_version returns None when index file doesn't exist."""
        error = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        mock_s3_client.get_object.side_effect = error

        result = version_manager.get_current_version("test_dataset")

        assert result is None

    def test_get_current_version_returns_version_from_index(self, version_manager, mock_s3_client):
        """Test that get_current_version reads version from index file."""
        mock_body = Mock()
        mock_body.read.return_value = b"v20240115_143022"
        mock_body.__enter__ = Mock(return_value=mock_body)
        mock_body.__exit__ = Mock(return_value=None)
        mock_response = {"Body": mock_body}
        mock_s3_client.get_object.return_value = mock_response

        result = version_manager.get_current_version("test_dataset")

        assert result == "v20240115_143022"
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="datasets/test_dataset/index/current_version.txt"
        )

    def test_set_current_version_writes_to_index(self, version_manager, mock_s3_client):
        """Test that set_current_version writes version to index file."""
        version_manager.set_current_version("test_dataset", "v20240115_143022")

        mock_s3_client.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="datasets/test_dataset/index/current_version.txt",
            Body=b"v20240115_143022",
        )

    def test_list_versions_returns_all_versions(self, version_manager, mock_s3_client):
        """Test that list_versions returns all version IDs from S3."""
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "datasets/test_dataset/versions/v20240115_143022/manifest.json"},
                {"Key": "datasets/test_dataset/versions/v20240114_120000/manifest.json"},
                {"Key": "datasets/test_dataset/versions/v20240113_100000/manifest.json"},
            ]
        }

        versions = version_manager.list_versions("test_dataset")

        assert len(versions) == 3
        assert "v20240115_143022" in versions
        assert "v20240114_120000" in versions
        assert "v20240113_100000" in versions
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", Prefix="datasets/test_dataset/versions/"
        )

    def test_list_versions_returns_empty_list_when_no_versions_exist(
        self, version_manager, mock_s3_client
    ):
        """Test that list_versions returns empty list when no versions exist."""
        mock_s3_client.list_objects_v2.return_value = {"Contents": []}

        versions = version_manager.list_versions("test_dataset")

        assert versions == []

    def test_list_versions_handles_missing_prefix(self, version_manager, mock_s3_client):
        """Test that list_versions handles case when prefix doesn't exist."""
        error = ClientError({"Error": {"Code": "NoSuchKey"}}, "ListObjectsV2")
        mock_s3_client.list_objects_v2.side_effect = error

        versions = version_manager.list_versions("test_dataset")

        assert versions == []

    def test_get_current_version_raises_error_on_other_client_errors(
        self, version_manager, mock_s3_client
    ):
        """Test that get_current_version raises error on non-NoSuchKey ClientError."""
        error = ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
        mock_s3_client.get_object.side_effect = error

        with pytest.raises(ClientError):
            version_manager.get_current_version("test_dataset")

    def test_list_versions_returns_empty_when_no_contents(self, version_manager, mock_s3_client):
        """Test that list_versions returns empty list when response has no Contents."""
        mock_s3_client.list_objects_v2.return_value = {}  # No "Contents" key

        versions = version_manager.list_versions("test_dataset")

        assert versions == []

    def test_list_versions_raises_error_on_other_client_errors(
        self, version_manager, mock_s3_client
    ):
        """Test that list_versions raises error on non-NoSuchKey ClientError."""
        error = ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectsV2")
        mock_s3_client.list_objects_v2.side_effect = error

        with pytest.raises(ClientError):
            version_manager.list_versions("test_dataset")
