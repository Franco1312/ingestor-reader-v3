"""Staging manager for projection operations."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List, Set

from botocore.exceptions import ClientError

from src.infrastructure.utils.aws_utils import create_s3_client

logger = logging.getLogger(__name__)


class StagingManager:
    """Manages the staging area for projection operations."""

    def __init__(
        self,
        bucket: str,
        s3_client: Any = None,
        aws_region: str = "us-east-1",
        copy_workers: int = 1,
    ):
        """Initialize StagingManager.

        Args:
            bucket: S3 bucket name.
            s3_client: Boto3 S3 client (optional, for testing).
            aws_region: AWS region (default: us-east-1).
            copy_workers: Number of parallel workers for copying files (default: 1, sequential).
        """
        self._bucket = bucket
        self._s3_client = create_s3_client(aws_region=aws_region, s3_client=s3_client)
        self._copy_workers = copy_workers

    def copy_from_version(
        self, version_id: str, dataset_id: str, json_files: List[str]
    ) -> List[str]:
        """Copy JSON files from a version to staging.

        Args:
            version_id: Version identifier.
            dataset_id: Dataset identifier.
            json_files: List of relative JSON file paths.

        Returns:
            List of staging paths where files were copied.
        """
        if not json_files:
            logger.info("No files to copy for version %s", version_id)
            return []

        self._log_copy_start(version_id, dataset_id, len(json_files))
        staging_paths = self._copy_all_files(version_id, dataset_id, json_files)
        self._log_copy_complete(len(staging_paths))
        return staging_paths

    def _copy_all_files(
        self, version_id: str, dataset_id: str, json_files: List[str]
    ) -> List[str]:
        """Copy all files from version to staging.

        Args:
            version_id: Version identifier.
            dataset_id: Dataset identifier.
            json_files: List of relative JSON file paths.

        Returns:
            List of staging paths where files were copied.
        """
        return self._copy_all_files_parallel(version_id, dataset_id, json_files)

    def _copy_all_files_parallel(
        self, version_id: str, dataset_id: str, json_files: List[str]
    ) -> List[str]:
        """Copy all files in parallel using ThreadPoolExecutor."""
        total_files = len(json_files)
        logger.info("Copying %d files in parallel with %d workers", total_files, self._copy_workers)

        staging_paths = []
        completed_count = 0

        def copy_single_file(json_file: str) -> str:
            """Copy a single file and return destination key."""
            source_key = self._build_version_file_path(dataset_id, version_id, json_file)
            dest_key = self._build_staging_file_path(dataset_id, json_file)
            self._log_file_copy(source_key, dest_key)
            self._copy_s3_object(source_key, dest_key)
            return dest_key

        with ThreadPoolExecutor(max_workers=self._copy_workers) as executor:
            future_to_file = {
                executor.submit(copy_single_file, json_file): json_file
                for json_file in json_files
            }

            for future in as_completed(future_to_file):
                completed_count += 1
                self._log_copy_progress(completed_count, total_files)
                try:
                    dest_key = future.result()
                    staging_paths.append(dest_key)
                except Exception as e:
                    json_file = future_to_file[future]
                    logger.error("Failed to copy file %s: %s", json_file, e)
                    raise

        return staging_paths

    def _log_copy_start(self, version_id: str, dataset_id: str, file_count: int) -> None:
        """Log the start of copy operation."""
        logger.info(
            "Copying %d file(s) from version %s to staging for dataset %s",
            file_count,
            version_id,
            dataset_id,
        )
        logger.info("Starting copy of %d files (sequential, one by one)", file_count)

    def _log_copy_progress(self, current: int, total: int) -> None:
        """Log copy progress at intervals."""
        if current % 100 == 0 or current == total:
            percentage = (current / total) * 100
            logger.info("Progress: %d/%d files copied (%.1f%%)", current, total, percentage)

    def _log_file_copy(self, source_key: str, dest_key: str) -> None:
        """Log individual file copy operation."""
        logger.debug("Copying: %s -> %s", source_key, dest_key)

    def _log_copy_complete(self, file_count: int) -> None:
        """Log completion of copy operation."""
        logger.info("Successfully copied %d file(s) to staging", file_count)

    def list_staging_partitions(self, dataset_id: str) -> List[str]:
        """List all partitions in staging.

        Args:
            dataset_id: Dataset identifier.

        Returns:
            List of unique partition paths.
        """
        logger.debug("Listing staging partitions for dataset %s", dataset_id)

        staging_prefix = self._build_staging_prefix(dataset_id)
        s3_keys = self._list_s3_keys(staging_prefix)

        if not s3_keys:
            logger.debug("No files found in staging for dataset %s", dataset_id)
            return []

        partitions = self._extract_partitions_from_keys(s3_keys, staging_prefix)
        logger.debug("Found %d unique partition(s) in staging", len(partitions))
        return sorted(partitions)

    def clear_staging(self, dataset_id: str) -> None:
        """Clear all files from staging.

        Args:
            dataset_id: Dataset identifier.
        """
        logger.info("Clearing staging area for dataset %s", dataset_id)

        staging_prefix = self._build_staging_prefix(dataset_id)
        s3_keys = self._list_s3_keys(staging_prefix)

        if not s3_keys:
            logger.info("Staging area is already empty for dataset %s", dataset_id)
            return

        logger.info("Found %d file(s) to delete from staging", len(s3_keys))
        self._delete_all_objects(s3_keys)
        logger.info("Cleared %d file(s) from staging", len(s3_keys))

    def _delete_all_objects(self, s3_keys: List[str]) -> None:
        """Delete all S3 objects.

        Args:
            s3_keys: List of S3 object keys to delete.
        """
        total_files = len(s3_keys)
        for idx, key in enumerate(s3_keys, start=1):
            if idx % 100 == 0 or idx == total_files:
                logger.info("Deleting progress: %d/%d files (%.1f%%)", idx, total_files, (idx / total_files) * 100)
            logger.debug("Deleting: %s", key)
            self._delete_s3_object(key)

    def _build_version_file_path(self, dataset_id: str, version_id: str, json_file: str) -> str:
        """Build S3 key path for a file in a version.

        Args:
            dataset_id: Dataset identifier.
            version_id: Version identifier.
            json_file: Relative JSON file path.

        Returns:
            Full S3 key path.
        """
        return f"datasets/{dataset_id}/versions/{version_id}/data/{json_file}"

    def _build_staging_file_path(self, dataset_id: str, json_file: str) -> str:
        """Build S3 key path for a file in staging.

        Args:
            dataset_id: Dataset identifier.
            json_file: Relative JSON file path.

        Returns:
            Full S3 key path.
        """
        return f"datasets/{dataset_id}/staging/{json_file}"

    def _build_staging_prefix(self, dataset_id: str) -> str:
        """Build S3 prefix for staging area.

        Args:
            dataset_id: Dataset identifier.

        Returns:
            S3 prefix string.
        """
        return f"datasets/{dataset_id}/staging/"

    def _copy_s3_object(self, source_key: str, dest_key: str) -> None:
        """Copy an S3 object within the same bucket.

        Args:
            source_key: Source S3 key.
            dest_key: Destination S3 key.
        """
        self._s3_client.copy_object(
            CopySource={"Bucket": self._bucket, "Key": source_key},
            Bucket=self._bucket,
            Key=dest_key,
        )

    def _list_s3_keys(self, prefix: str) -> List[str]:
        """List all S3 object keys with the given prefix, handling pagination.

        Args:
            prefix: S3 key prefix.

        Returns:
            List of S3 object keys.
        """
        all_keys = []
        continuation_token = None

        try:
            while True:
                params = {"Bucket": self._bucket, "Prefix": prefix}
                if continuation_token:
                    params["ContinuationToken"] = continuation_token

                response = self._s3_client.list_objects_v2(**params)
                keys = self._extract_keys_from_response(response)
                all_keys.extend(keys)

                if not response.get("IsTruncated", False):
                    break

                continuation_token = response.get("NextContinuationToken")

            return all_keys
        except ClientError as e:
            if self._is_nosuchkey_error(e):
                return []
            raise

    def _extract_keys_from_response(self, response: dict) -> List[str]:
        """Extract keys from S3 list_objects_v2 response.

        Args:
            response: S3 list_objects_v2 response.

        Returns:
            List of S3 object keys.
        """
        if "Contents" not in response:
            return []
        return [obj["Key"] for obj in response["Contents"]]

    def _is_nosuchkey_error(self, error: ClientError) -> bool:
        """Check if ClientError is a NoSuchKey error.

        Args:
            error: ClientError to check.

        Returns:
            True if error is NoSuchKey, False otherwise.
        """
        return error.response["Error"]["Code"] == "NoSuchKey"

    def _extract_partitions_from_keys(self, s3_keys: List[str], prefix: str) -> Set[str]:
        """Extract unique partition paths from S3 keys.

        Args:
            s3_keys: List of full S3 keys.
            prefix: Prefix to remove from keys.

        Returns:
            Set of unique partition paths.

        Example:
            Key: datasets/test/staging/SERIES_1/year=2024/month=01/data.json
            Prefix: datasets/test/staging/
            Result: SERIES_1/year=2024/month=01
        """
        partitions = set()
        for key in s3_keys:
            partition_path = self._extract_partition_from_key(key, prefix)
            if partition_path:
                partitions.add(partition_path)
        return partitions

    def _extract_partition_from_key(self, s3_key: str, prefix: str) -> str:
        """Extract partition path from a single S3 key.

        Args:
            s3_key: Full S3 key.
            prefix: Prefix to remove.

        Returns:
            Partition path (empty string if extraction fails).
        """
        if not s3_key.startswith(prefix):
            return ""

        relative_path = s3_key[len(prefix) :]
        return self._get_partition_path_from_relative(relative_path)

    def _get_partition_path_from_relative(self, relative_path: str) -> str:
        """Get partition path from relative file path.

        Args:
            relative_path: Relative path (e.g., "SERIES_1/year=2024/month=01/data.json").

        Returns:
            Partition path without filename (e.g., "SERIES_1/year=2024/month=01").
        """
        path_parts = relative_path.split("/")
        partition_parts = path_parts[:-1]  # Remove filename (last part)
        return "/".join(partition_parts)

    def _delete_s3_object(self, key: str) -> None:
        """Delete an S3 object.

        Args:
            key: S3 object key to delete.
        """
        self._s3_client.delete_object(Bucket=self._bucket, Key=key)
