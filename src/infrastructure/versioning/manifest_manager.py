"""Manifest manager for version metadata."""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

# Import datetime type for isinstance checks (not affected by mocking)
DatetimeType = datetime


class ManifestManager:
    """Manages manifest creation and persistence in S3."""

    def __init__(self, bucket: str, s3_client: Any = None, aws_region: str = "us-east-1"):
        """Initialize ManifestManager.

        Args:
            bucket: S3 bucket name.
            s3_client: Boto3 S3 client (optional, for testing).
            aws_region: AWS region (default: us-east-1).
        """
        self._bucket = bucket
        self._s3_client = s3_client or boto3.client("s3", region_name=aws_region)

    def create_manifest(
        self,
        version_id: str,
        dataset_id: str,
        data: List[Dict[str, Any]],
        parquet_files: List[str],
        partitions: List[str],
        partition_strategy: str,
    ) -> Dict[str, Any]:
        """Create a manifest from data and metadata.

        Args:
            version_id: Version identifier.
            dataset_id: Dataset identifier.
            data: List of data point dictionaries.
            parquet_files: List of relative paths to parquet files.
            partitions: List of partition paths.
            partition_strategy: Partition strategy name used.

        Returns:
            Manifest dictionary.
        """
        now = datetime.now(timezone.utc)

        # Extract metadata from data
        data_points_count = len(data)
        series_codes = sorted(
            {code for dp in data if (code := dp.get("internal_series_code")) is not None}
        )

        # Find date range
        obs_times: List[datetime] = [
            obs_time
            for dp in data
            if (obs_time := dp.get("obs_time")) is not None and isinstance(obs_time, DatetimeType)
        ]
        min_obs_time = min(obs_times) if obs_times else None
        max_obs_time = max(obs_times) if obs_times else None
        collection_date = data[0].get("collection_date") if data else None

        manifest = {
            "version_id": version_id,
            "dataset_id": dataset_id,
            "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "collection_date": (
                collection_date.strftime("%Y-%m-%dT%H:%M:%SZ") if collection_date else None
            ),
            "data_points_count": data_points_count,
            "series_count": len(series_codes),
            "series_codes": series_codes,
            "date_range": {
                "min_obs_time": (
                    min_obs_time.strftime("%Y-%m-%dT%H:%M:%SZ") if min_obs_time else None
                ),
                "max_obs_time": (
                    max_obs_time.strftime("%Y-%m-%dT%H:%M:%SZ") if max_obs_time else None
                ),
            },
            "parquet_files": parquet_files,
            "partitions": partitions,
            "partition_strategy": partition_strategy,
        }

        return manifest

    def save_manifest(self, dataset_id: str, version_id: str, manifest: Dict[str, Any]) -> None:
        """Save manifest to S3.

        Args:
            dataset_id: Dataset identifier.
            version_id: Version identifier.
            manifest: Manifest dictionary to save.
        """
        key = f"datasets/{dataset_id}/versions/{version_id}/manifest.json"
        manifest_json = json.dumps(manifest, indent=2, default=str)

        self._s3_client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=manifest_json.encode("utf-8"),
            ContentType="application/json",
        )

    def load_manifest(self, dataset_id: str, version_id: str) -> Optional[Dict[str, Any]]:
        """Load manifest from S3.

        Args:
            dataset_id: Dataset identifier.
            version_id: Version identifier.

        Returns:
            Manifest dictionary or None if not found.
        """
        key = f"datasets/{dataset_id}/versions/{version_id}/manifest.json"

        try:
            response = self._s3_client.get_object(Bucket=self._bucket, Key=key)
            with response["Body"] as body:
                manifest_json = body.read().decode("utf-8")
                return json.loads(manifest_json)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise
