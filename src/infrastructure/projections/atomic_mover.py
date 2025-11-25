"""Atomic mover for staging to projections."""

import logging
from typing import Any, List

from botocore.exceptions import ClientError

from src.infrastructure.utils.aws_utils import create_s3_client

logger = logging.getLogger(__name__)


class AtomicProjectionMover:
    """Moves staging data to projections atomically."""

    def __init__(self, bucket: str, s3_client: Any = None, aws_region: str = "us-east-1"):
        """Initialize AtomicProjectionMover.

        Args:
            bucket: S3 bucket name.
            s3_client: Boto3 S3 client (optional, for testing).
            aws_region: AWS region (default: us-east-1).
        """
        self._bucket = bucket
        self._s3_client = create_s3_client(aws_region=aws_region, s3_client=s3_client)

    def move_staging_to_projections(self, dataset_id: str) -> None:
        """Move all files from staging to projections atomically.

        Uses copy-then-delete strategy:
        1. Copy all files from staging to projections
        2. If all copies succeed, delete staging files
        3. If any copy fails, rollback (delete copied files) and keep staging

        Args:
            dataset_id: Dataset identifier.
        """
        logger.info("Moving staging to projections for dataset %s", dataset_id)

        staging_files = self._get_staging_files(dataset_id)
        if not staging_files:
            logger.info("No files in staging for dataset %s", dataset_id)
            return

        logger.info("Found %d file(s) in staging to move", len(staging_files))

        copied_files = self._copy_all_to_projections(dataset_id, staging_files)
        self._delete_staging_after_successful_copy(staging_files, len(copied_files))

    def _get_staging_files(self, dataset_id: str) -> List[str]:
        """Get all staging files for a dataset."""
        staging_prefix = f"datasets/{dataset_id}/staging/"
        return self._list_s3_files(staging_prefix)

    def _list_s3_files(self, prefix: str) -> List[str]:
        """List all S3 files with given prefix, handling pagination."""
        all_keys = []
        continuation_token = None

        try:
            while True:
                params = {"Bucket": self._bucket, "Prefix": prefix}
                if continuation_token:
                    params["ContinuationToken"] = continuation_token

                response = self._s3_client.list_objects_v2(**params)

                if "Contents" in response:
                    all_keys.extend([obj["Key"] for obj in response["Contents"]])

                if not response.get("IsTruncated", False):
                    break

                continuation_token = response.get("NextContinuationToken")

            return all_keys
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return []
            raise

    def _copy_all_to_projections(self, dataset_id: str, staging_files: List[str]) -> List[str]:
        """Copy all staging files to projections with automatic rollback on failure."""
        staging_prefix = f"datasets/{dataset_id}/staging/"
        projections_prefix = f"datasets/{dataset_id}/projections/"

        copied_files = []
        try:
            for staging_key in staging_files:
                projections_key = self._convert_to_projections_key(
                    staging_key, staging_prefix, projections_prefix
                )
                self._copy_s3_file(staging_key, projections_key)
                copied_files.append(projections_key)
            return copied_files
        except Exception:  # noqa: BLE001
            logger.error("Copy failed, rolling back %d copied file(s)", len(copied_files))
            self._delete_files(copied_files)
            raise

    def _convert_to_projections_key(
        self, staging_key: str, staging_prefix: str, projections_prefix: str
    ) -> str:
        """Convert staging key to projections key."""
        relative_path = staging_key[len(staging_prefix) :]
        return projections_prefix + relative_path

    def _copy_s3_file(self, source_key: str, destination_key: str) -> None:
        """Copy a single S3 file."""
        logger.debug("Copying %s to %s", source_key, destination_key)
        self._s3_client.copy_object(
            CopySource={"Bucket": self._bucket, "Key": source_key},
            Bucket=self._bucket,
            Key=destination_key,
        )

    def _delete_staging_after_successful_copy(
        self, staging_files: List[str], copied_count: int
    ) -> None:
        """Delete staging files after successful copy, handling errors gracefully."""
        try:
            self._delete_files(staging_files)
            logger.info("Successfully moved %d file(s) from staging to projections", copied_count)
        except Exception as e:  # pylint: disable=broad-except  # noqa: BLE001
            logger.warning(
                "Failed to delete staging files after successful copy: %s. "
                "Data is already in projections. Manual cleanup may be required.",
                e,
            )

    def _delete_files(self, keys: List[str]) -> None:
        """Delete multiple S3 files, continuing even if individual deletes fail."""
        for key in keys:
            self._delete_single_file(key)

    def _delete_single_file(self, key: str) -> None:
        """Delete a single S3 file, logging errors but not raising."""
        try:
            logger.debug("Deleting file %s", key)
            self._s3_client.delete_object(Bucket=self._bucket, Key=key)
        except Exception as e:  # pylint: disable=broad-except  # noqa: BLE001
            logger.error("Failed to delete file %s: %s", key, e)
