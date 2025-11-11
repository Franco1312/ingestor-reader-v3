"""Tests for ManifestManager."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from src.infrastructure.versioning.manifest_manager import ManifestManager
from tests.builders import DataPointBuilder


class TestManifestManager:
    """Tests for ManifestManager class."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        return Mock()

    @pytest.fixture
    def manifest_manager(self, mock_s3_client):
        """Create ManifestManager instance."""
        return ManifestManager(bucket="test-bucket", s3_client=mock_s3_client)

    def test_create_manifest_generates_complete_manifest(self, manifest_manager):
        """Test that create_manifest generates a complete manifest with all required fields."""
        data = [
            DataPointBuilder()
            .with_series_code("SERIES_1")
            .with_obs_time(datetime(2024, 1, 15, 12, 0, 0))
            .with_value(100.0)
            .with_unit("USD")
            .with_frequency("D")
            .build(),
            DataPointBuilder()
            .with_series_code("SERIES_1")
            .with_obs_time(datetime(2024, 1, 16, 12, 0, 0))
            .with_value(101.0)
            .with_unit("USD")
            .with_frequency("D")
            .build(),
            DataPointBuilder()
            .with_series_code("SERIES_2")
            .with_obs_time(datetime(2024, 2, 1, 12, 0, 0))
            .with_value(200.0)
            .with_unit("EUR")
            .with_frequency("M")
            .build(),
        ]

        # Add collection_date to all data points
        for dp in data:
            dp["collection_date"] = datetime(2024, 1, 15, 14, 25, 0)

        parquet_files = [
            "data/SERIES_1/year=2024/month=01/data.parquet",
            "data/SERIES_2/year=2024/month=02/data.parquet",
        ]
        partitions = [
            "SERIES_1/year=2024/month=01",
            "SERIES_2/year=2024/month=02",
        ]
        partition_strategy = "series_year_month"

        with patch("src.infrastructure.versioning.manifest_manager.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(
                2024, 1, 15, 14, 30, 22, tzinfo=datetime.now().astimezone().tzinfo
            )
            mock_datetime.strftime = datetime.strftime

            manifest = manifest_manager.create_manifest(
                version_id="v20240115_143022",
                dataset_id="test_dataset",
                data=data,
                parquet_files=parquet_files,
                partitions=partitions,
                partition_strategy=partition_strategy,
            )

            assert manifest["version_id"] == "v20240115_143022"
            assert manifest["dataset_id"] == "test_dataset"
            assert manifest["created_at"] == "2024-01-15T14:30:22Z"
            assert manifest["collection_date"] == "2024-01-15T14:25:00Z"
            assert manifest["data_points_count"] == 3
            assert manifest["series_count"] == 2
            assert set(manifest["series_codes"]) == {"SERIES_1", "SERIES_2"}
            assert manifest["date_range"]["min_obs_time"] == "2024-01-15T12:00:00Z"
            assert manifest["date_range"]["max_obs_time"] == "2024-02-01T12:00:00Z"
            assert manifest["parquet_files"] == parquet_files
            assert manifest["partitions"] == partitions
            assert manifest["partition_strategy"] == partition_strategy

    def test_create_manifest_handles_empty_data(self, manifest_manager):
        """Test that create_manifest handles empty data gracefully."""
        with patch("src.infrastructure.versioning.manifest_manager.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(
                2024, 1, 15, 14, 30, 22, tzinfo=datetime.now().astimezone().tzinfo
            )
            mock_datetime.strftime = datetime.strftime

            manifest = manifest_manager.create_manifest(
                version_id="v20240115_143022",
                dataset_id="test_dataset",
                data=[],
                parquet_files=[],
                partitions=[],
                partition_strategy="series_year_month",
            )

            assert manifest["data_points_count"] == 0
            assert manifest["series_count"] == 0
            assert manifest["series_codes"] == []
            assert manifest["date_range"]["min_obs_time"] is None
            assert manifest["date_range"]["max_obs_time"] is None
            assert manifest["collection_date"] is None

    def test_create_manifest_handles_missing_collection_date(self, manifest_manager):
        """Test that create_manifest handles missing collection_date."""
        data = [
            DataPointBuilder()
            .with_series_code("SERIES_1")
            .with_obs_time(datetime(2024, 1, 15, 12, 0, 0))
            .with_value(100.0)
            .build(),
        ]

        with patch("src.infrastructure.versioning.manifest_manager.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(
                2024, 1, 15, 14, 30, 22, tzinfo=datetime.now().astimezone().tzinfo
            )
            mock_datetime.strftime = datetime.strftime

            manifest = manifest_manager.create_manifest(
                version_id="v20240115_143022",
                dataset_id="test_dataset",
                data=data,
                parquet_files=[],
                partitions=[],
                partition_strategy="series_year_month",
            )

            assert manifest["collection_date"] is None

    def test_save_manifest_writes_to_s3(self, manifest_manager, mock_s3_client):
        """Test that save_manifest writes manifest to S3."""
        manifest = {
            "version_id": "v20240115_143022",
            "dataset_id": "test_dataset",
        }

        manifest_manager.save_manifest(
            dataset_id="test_dataset",
            version_id="v20240115_143022",
            manifest=manifest,
        )

        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        assert call_args.kwargs["Bucket"] == "test-bucket"
        assert (
            call_args.kwargs["Key"]
            == "datasets/test_dataset/versions/v20240115_143022/manifest.json"
        )
        assert "Body" in call_args.kwargs
        assert call_args.kwargs["ContentType"] == "application/json"

    def test_load_manifest_reads_from_s3(self, manifest_manager, mock_s3_client):
        """Test that load_manifest reads manifest from S3."""
        manifest_content = '{"version_id": "v20240115_143022", "dataset_id": "test_dataset"}'
        mock_body = Mock()
        mock_body.read.return_value = manifest_content.encode("utf-8")
        mock_body.__enter__ = Mock(return_value=mock_body)
        mock_body.__exit__ = Mock(return_value=None)
        mock_response = {"Body": mock_body}
        mock_s3_client.get_object.return_value = mock_response

        manifest = manifest_manager.load_manifest(
            dataset_id="test_dataset",
            version_id="v20240115_143022",
        )

        assert manifest["version_id"] == "v20240115_143022"
        assert manifest["dataset_id"] == "test_dataset"
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="datasets/test_dataset/versions/v20240115_143022/manifest.json",
        )

    def test_load_manifest_returns_none_when_not_found(self, manifest_manager, mock_s3_client):
        """Test that load_manifest returns None when manifest doesn't exist."""
        error = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
            "GetObject",
        )
        mock_s3_client.get_object.side_effect = error

        manifest = manifest_manager.load_manifest(
            dataset_id="test_dataset",
            version_id="v20240115_143022",
        )

        assert manifest is None

    def test_load_manifest_raises_error_on_other_errors(self, manifest_manager, mock_s3_client):
        """Test that load_manifest raises error on non-NoSuchKey errors."""
        error = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "GetObject",
        )
        mock_s3_client.get_object.side_effect = error

        with pytest.raises(ClientError):
            manifest_manager.load_manifest(
                dataset_id="test_dataset",
                version_id="v20240115_143022",
            )
