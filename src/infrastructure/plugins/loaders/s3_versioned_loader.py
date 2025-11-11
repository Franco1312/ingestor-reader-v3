"""S3 versioned loader plugin."""

import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

import boto3

from src.domain.interfaces import Loader
from src.infrastructure.partitioning import PartitionStrategyFactory
from src.infrastructure.storage.parquet import ParquetWriter
from src.infrastructure.versioning import ManifestManager, VersionManager

logger = logging.getLogger(__name__)


class S3VersionedLoader(Loader):
    """Loader that persists data to S3 with versioning."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, s3_client: Any = None):
        """Initialize S3VersionedLoader.

        Args:
            config: Configuration dictionary. Must contain:
                - dataset_id: Dataset identifier
                - load.bucket: S3 bucket name
                - load.partition_strategy: Partition strategy (optional, default: "series_year_month")
                - load.compression: Compression codec (optional, default: "snappy")
                - load.aws_region: AWS region (optional, default: "us-east-1")
            s3_client: Boto3 S3 client (optional, for testing).
        """
        self._validate_config(config)
        self._config = config
        self._dataset_id: str = config["dataset_id"]  # type: ignore[index]
        self._bucket: str = config["load"]["bucket"]  # type: ignore[index]

        load_config = config.get("load", {})  # type: ignore[union-attr]
        aws_region = load_config.get("aws_region", "us-east-1")
        compression = load_config.get("compression", "snappy")

        self._s3_client = s3_client or boto3.client("s3", region_name=aws_region)

        # Initialize components
        self._partition_strategy = PartitionStrategyFactory.create(config)
        self._parquet_writer = ParquetWriter(
            partition_strategy=self._partition_strategy, compression=compression
        )
        self._version_manager = VersionManager(
            bucket=self._bucket, s3_client=self._s3_client, aws_region=aws_region
        )
        self._manifest_manager = ManifestManager(
            bucket=self._bucket, s3_client=self._s3_client, aws_region=aws_region
        )

    def load(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> None:
        """Load data to S3 with versioning.

        Args:
            data: List of transformed data point dictionaries.
            config: Configuration dictionary (same as passed to __init__).
        """
        logger.info("Loading %d data points to S3 with versioning", len(data))
        logger.info("Target bucket: %s, Dataset: %s", self._bucket, self._dataset_id)

        logger.info("Creating new version")
        version_id = self._version_manager.create_new_version()
        logger.info("Created version: %s", version_id)

        logger.info("Writing parquet files and uploading to S3")
        parquet_files = self._write_and_upload_parquet_files(data, version_id)
        logger.info("Uploaded %d parquet file(s) to S3", len(parquet_files))

        logger.info("Creating and saving manifest")
        self._create_and_save_manifest(data, parquet_files, version_id)
        logger.info("Manifest saved successfully")

        logger.info("Updating current version pointer to: %s", version_id)
        self._version_manager.set_current_version(self._dataset_id, version_id)
        logger.info("Version %s is now active", version_id)

    def _validate_config(self, config: Optional[Dict[str, Any]]) -> None:
        """Validate that required configuration is present.

        Args:
            config: Configuration dictionary to validate.

        Raises:
            ValueError: If required configuration is missing.
        """
        if not config:
            raise ValueError("S3VersionedLoader requires configuration")

        if not config.get("dataset_id"):
            raise ValueError("Configuration must include 'dataset_id'")

        load_config = config.get("load", {})
        if not load_config.get("bucket"):
            raise ValueError("Configuration must include 'load.bucket'")

    def _write_and_upload_parquet_files(
        self, data: List[Dict[str, Any]], version_id: str
    ) -> List[str]:
        """Write parquet files to temporary directory and upload to S3.

        Args:
            data: List of data point dictionaries.
            version_id: Version identifier.

        Returns:
            List of relative parquet file paths.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = os.path.join(tmpdir, "data")
            parquet_files = self._parquet_writer.write_to_parquet(data, base_path)
            logger.info("Generated %d parquet file(s)", len(parquet_files))

            for idx, rel_path in enumerate(parquet_files, 1):
                local_path = os.path.join(base_path, rel_path)
                s3_key = self._build_s3_key(rel_path, version_id)
                logger.info("Uploading file %d/%d: %s", idx, len(parquet_files), s3_key)
                self._s3_client.upload_file(local_path, self._bucket, s3_key)

            return parquet_files

    def _build_s3_key(self, rel_path: str, version_id: str) -> str:
        """Build S3 key for a parquet file.

        Args:
            rel_path: Relative path of the parquet file.
            version_id: Version identifier.

        Returns:
            S3 key string.
        """
        return f"datasets/{self._dataset_id}/versions/{version_id}/data/{rel_path}"

    def _create_and_save_manifest(
        self, data: List[Dict[str, Any]], parquet_files: List[str], version_id: str
    ) -> None:
        """Create and save manifest to S3.

        Args:
            data: List of data point dictionaries.
            parquet_files: List of relative parquet file paths.
            version_id: Version identifier.
        """
        partition_strategy_name = self._get_partition_strategy_name()
        partitions = self._extract_partitions_from_paths(parquet_files)

        manifest = self._manifest_manager.create_manifest(
            version_id=version_id,
            dataset_id=self._dataset_id,
            data=data,
            parquet_files=parquet_files,
            partitions=partitions,
            partition_strategy=partition_strategy_name,
        )

        self._manifest_manager.save_manifest(
            dataset_id=self._dataset_id, version_id=version_id, manifest=manifest
        )

    def _get_partition_strategy_name(self) -> str:
        """Get partition strategy name from configuration.

        Returns:
            Partition strategy name.
        """
        load_config = self._config.get("load", {})  # type: ignore[union-attr]
        return load_config.get("partition_strategy", "series_year_month")

    def _extract_partitions_from_paths(self, parquet_files: List[str]) -> List[str]:
        """Extract partition paths from parquet file paths.

        Args:
            parquet_files: List of relative parquet file paths.

        Returns:
            List of partition paths, sorted.
        """
        return sorted(self._partition_strategy.get_all_partitions_from_paths(parquet_files))
