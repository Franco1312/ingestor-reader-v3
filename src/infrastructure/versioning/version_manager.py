"""Version manager for handling dataset versions in S3."""

import logging
from datetime import datetime
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class VersionManager:
    """Manages dataset versions and the current version pointer in S3."""

    def __init__(self, bucket: str, s3_client=None, aws_region: str = "us-east-1"):
        """Initialize VersionManager.

        Args:
            bucket: S3 bucket name.
            s3_client: Optional boto3 S3 client (for testing). If None, creates a new client.
            aws_region: AWS region (default: us-east-1).
        """
        self._bucket = bucket
        self._s3_client = s3_client or boto3.client("s3", region_name=aws_region)

    def create_new_version(self) -> str:
        """Create a new version ID (timestamp-based).

        Returns:
            Version ID string (e.g., "v20240115_143022" or "v20240115_143022_123456" with microseconds).
        """
        now = datetime.now()
        # Include microseconds to ensure uniqueness even for rapid calls
        version_id = f"v{now.strftime('%Y%m%d_%H%M%S_%f')}"
        return version_id

    def get_current_version(self, dataset_id: str) -> Optional[str]:
        """Get the current version ID from the index pointer.

        Args:
            dataset_id: Dataset identifier.

        Returns:
            Current version ID or None if no version exists.
        """
        key = f"datasets/{dataset_id}/index/current_version.txt"

        try:
            response = self._s3_client.get_object(Bucket=self._bucket, Key=key)
            with response["Body"] as body:
                version_id = body.read().decode("utf-8").strip()
                return version_id if version_id else None
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def set_current_version(self, dataset_id: str, version_id: str) -> None:
        """Set the current version pointer (atomic operation).

        Args:
            dataset_id: Dataset identifier.
            version_id: Version ID to set as current.
        """
        key = f"datasets/{dataset_id}/index/current_version.txt"
        self._s3_client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=version_id.encode("utf-8"),
        )

    def list_versions(self, dataset_id: str) -> List[str]:
        """List all version IDs for a dataset.

        Args:
            dataset_id: Dataset identifier.

        Returns:
            List of version IDs (sorted, most recent first).
        """
        prefix = f"datasets/{dataset_id}/versions/"

        try:
            response = self._s3_client.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
            if "Contents" not in response:
                return []

            versions = set()
            for obj in response["Contents"]:
                key = obj["Key"]
                # Extract version ID from path like: datasets/{dataset_id}/versions/{version_id}/manifest.json
                parts = key.split("/")
                if len(parts) >= 4 and parts[-1] == "manifest.json":
                    version_id = parts[-2]
                    if version_id.startswith("v"):
                        versions.add(version_id)

            return sorted(versions, reverse=True)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return []
            raise
