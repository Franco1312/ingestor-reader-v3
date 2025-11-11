"""Tests for S3VersionedLoader."""

# Access to protected members is necessary for testing internal components
# pylint: disable=protected-access

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from src.infrastructure.plugins.loaders.s3_versioned_loader import S3VersionedLoader
from tests.builders import DataPointBuilder


class TestS3VersionedLoader:
    """Tests for S3VersionedLoader class."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        return Mock()

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return {
            "dataset_id": "test_dataset",
            "load": {
                "plugin": "s3_versioned",
                "bucket": "test-bucket",
                "partition_strategy": "series_year_month",
                "compression": "snappy",
                "aws_region": "us-east-1",
            },
        }

    @pytest.fixture
    def loader(self, mock_s3_client, config):
        """Create S3VersionedLoader instance."""
        return S3VersionedLoader(s3_client=mock_s3_client, config=config)

    @pytest.fixture
    def sample_data(self):
        """Create sample data for tests."""
        data = [
            DataPointBuilder()
            .with_series_code("SERIES_1")
            .with_obs_time(datetime(2024, 1, 15, 12, 0, 0))
            .with_value(100.0)
            .build(),
        ]
        for dp in data:
            dp["collection_date"] = datetime(2024, 1, 15, 14, 25, 0)
        return data

    def test_load_creates_new_version(self, loader, config, sample_data):
        """Test that load creates a new version."""
        with (
            patch.object(loader._version_manager, "create_new_version") as mock_create_version,
            patch.object(loader._parquet_writer, "write_to_parquet") as mock_write,
            patch.object(loader._manifest_manager, "create_manifest"),
            patch.object(loader._manifest_manager, "save_manifest"),
            patch.object(loader._s3_client, "upload_file"),
        ):
            mock_create_version.return_value = "v20240115_143022"
            mock_write.return_value = ["SERIES_1/year=2024/month=01/data.parquet"]

            loader.load(sample_data, config)

            assert mock_create_version.called

    def test_load_writes_parquet_files(self, loader, config, sample_data):
        """Test that load writes parquet files."""
        with (
            patch.object(loader._version_manager, "create_new_version") as mock_create_version,
            patch.object(loader._parquet_writer, "write_to_parquet") as mock_write,
            patch.object(loader._manifest_manager, "create_manifest"),
            patch.object(loader._manifest_manager, "save_manifest"),
            patch.object(loader._s3_client, "upload_file"),
        ):
            mock_create_version.return_value = "v20240115_143022"
            mock_write.return_value = ["SERIES_1/year=2024/month=01/data.parquet"]

            loader.load(sample_data, config)

            assert mock_write.called

    def test_load_uploads_parquet_files_to_s3(self, loader, mock_s3_client, config, sample_data):
        """Test that load uploads parquet files to S3."""
        with (
            patch.object(loader._version_manager, "create_new_version") as mock_create_version,  # type: ignore[attr-defined]
            patch.object(loader._parquet_writer, "write_to_parquet") as mock_write,  # type: ignore[attr-defined]
            patch.object(loader._manifest_manager, "create_manifest"),  # type: ignore[attr-defined]
            patch.object(loader._manifest_manager, "save_manifest"),  # type: ignore[attr-defined]
        ):
            mock_create_version.return_value = "v20240115_143022"
            mock_write.return_value = ["SERIES_1/year=2024/month=01/data.parquet"]

            loader.load(sample_data, config)

            assert mock_s3_client.upload_file.called

    def test_load_creates_and_saves_manifest(self, loader, config, sample_data):
        """Test that load creates and saves manifest."""
        with (
            patch.object(loader._version_manager, "create_new_version") as mock_create_version,
            patch.object(loader._parquet_writer, "write_to_parquet") as mock_write,
            patch.object(loader._manifest_manager, "create_manifest") as mock_create_manifest,
            patch.object(loader._manifest_manager, "save_manifest") as mock_save_manifest,
            patch.object(loader._s3_client, "upload_file"),
        ):
            mock_create_version.return_value = "v20240115_143022"
            mock_write.return_value = ["SERIES_1/year=2024/month=01/data.parquet"]

            loader.load(sample_data, config)

            assert mock_create_manifest.called
            assert mock_save_manifest.called

    def test_load_updates_current_version(self, loader, config, sample_data):
        """Test that load updates current version pointer."""
        with (
            patch.object(loader._version_manager, "create_new_version") as mock_create_version,
            patch.object(loader._version_manager, "set_current_version") as mock_set_version,
            patch.object(loader._parquet_writer, "write_to_parquet") as mock_write,
            patch.object(loader._manifest_manager, "create_manifest"),
            patch.object(loader._manifest_manager, "save_manifest"),
            patch.object(loader._s3_client, "upload_file"),
        ):
            mock_create_version.return_value = "v20240115_143022"
            mock_write.return_value = ["SERIES_1/year=2024/month=01/data.parquet"]

            loader.load(sample_data, config)

            mock_set_version.assert_called_once_with("test_dataset", "v20240115_143022")

    def test_load_handles_empty_data(self, loader, config):
        """Test that load handles empty data gracefully."""
        with (
            patch.object(loader._version_manager, "create_new_version") as mock_create_version,  # type: ignore[attr-defined]
            patch.object(loader._parquet_writer, "write_to_parquet") as mock_write,  # type: ignore[attr-defined]
            patch.object(loader._manifest_manager, "create_manifest") as mock_create_manifest,  # type: ignore[attr-defined]
            patch.object(loader._manifest_manager, "save_manifest"),  # type: ignore[attr-defined]
            patch.object(loader._s3_client, "upload_file"),  # type: ignore[attr-defined]
        ):
            mock_create_version.return_value = "v20240115_143022"
            mock_write.return_value = []

            loader.load([], config)

            assert mock_create_version.called
            assert mock_create_manifest.called

    def test_init_requires_config(self):
        """Test that __init__ requires configuration."""
        with pytest.raises(ValueError, match="requires configuration"):
            S3VersionedLoader(config=None)

    def test_init_requires_dataset_id(self):
        """Test that __init__ requires dataset_id in config."""
        config = {"load": {"bucket": "test-bucket"}}
        with pytest.raises(ValueError, match="must include 'dataset_id'"):
            S3VersionedLoader(config=config)

    def test_init_requires_bucket(self):
        """Test that __init__ requires bucket in config."""
        config = {"dataset_id": "test_dataset", "load": {}}
        with pytest.raises(ValueError, match="must include 'load.bucket'"):
            S3VersionedLoader(config=config)
