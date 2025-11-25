"""Projection merger for merging staging data with projections."""

import json
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from src.infrastructure.projections.staging_manager import StagingManager
from src.infrastructure.utils.aws_utils import create_s3_client

logger = logging.getLogger(__name__)


class ProjectionMerger:
    """Merges staging data with existing projections."""

    def __init__(
        self,
        bucket: str,
        s3_client: Any = None,
        aws_region: str = "us-east-1",
        compression: str = "snappy",
        merge_workers: int = 1,
    ):
        """Initialize ProjectionMerger.

        Args:
            bucket: S3 bucket name.
            s3_client: Boto3 S3 client (optional, for testing).
            aws_region: AWS region (default: us-east-1).
            compression: Compression codec (default: "snappy").
            merge_workers: Number of parallel workers for merging partitions (default: 1, sequential).
        """
        self._bucket = bucket
        self._s3_client = create_s3_client(aws_region=aws_region, s3_client=s3_client)
        self._compression = compression
        self._merge_workers = merge_workers

    def merge_partition(self, dataset_id: str, partition_path: str) -> None:
        """Merge a single partition from staging with projections.

        Args:
            dataset_id: Dataset identifier.
            partition_path: Partition path (e.g., "SERIES_1/year=2024/month=01").
        """
        logger.info("Merging partition %s for dataset %s", partition_path, dataset_id)

        staging_key = self._build_staging_file_key(dataset_id, partition_path)
        projections_key = self._build_projections_file_key(dataset_id, partition_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Download files if they exist
            staging_data = self._download_and_read_json(staging_key, tmpdir)
            projections_data = self._download_and_read_json(projections_key, tmpdir)

            # Merge data
            merged_data = self._merge_json_data(projections_data, staging_data)

            # Write merged data back to staging
            if merged_data:
                output_file = Path(tmpdir) / "merged.json"
                self._write_json(merged_data, output_file)
                self._upload_to_staging(staging_key, output_file)

        logger.info("Successfully merged partition %s", partition_path)

    def merge_all_partitions(self, dataset_id: str) -> None:
        """Merge all partitions in staging with projections.

        Args:
            dataset_id: Dataset identifier.
        """
        staging_manager = StagingManager(bucket=self._bucket, s3_client=self._s3_client)
        partitions = staging_manager.list_staging_partitions(dataset_id)

        logger.info("Merging %d partition(s) for dataset %s", len(partitions), dataset_id)

        self._merge_all_partitions_parallel(dataset_id, partitions)

        logger.info("Successfully merged all partitions")

    def _merge_all_partitions_parallel(self, dataset_id: str, partitions: List[str]) -> None:
        """Merge all partitions in parallel using ThreadPoolExecutor."""
        logger.info("Merging %d partitions in parallel with %d workers", len(partitions), self._merge_workers)

        completed_count = 0
        total_partitions = len(partitions)

        with ThreadPoolExecutor(max_workers=self._merge_workers) as executor:
            future_to_partition = {
                executor.submit(self.merge_partition, dataset_id, partition_path): partition_path
                for partition_path in partitions
            }

            for future in as_completed(future_to_partition):
                completed_count += 1
                partition_path = future_to_partition[future]
                logger.debug("Completed merging partition %d/%d: %s", completed_count, total_partitions, partition_path)
                try:
                    future.result()
                except Exception as e:
                    logger.error("Failed to merge partition %s: %s", partition_path, e)
                    raise

    def _build_staging_file_key(self, dataset_id: str, partition_path: str) -> str:
        """Build S3 key for staging file.

        Args:
            dataset_id: Dataset identifier.
            partition_path: Partition path.

        Returns:
            S3 key string.
        """
        return f"datasets/{dataset_id}/staging/{partition_path}/data.json"

    def _build_projections_file_key(self, dataset_id: str, partition_path: str) -> str:
        """Build S3 key for projections file.

        Args:
            dataset_id: Dataset identifier.
            partition_path: Partition path.

        Returns:
            S3 key string.
        """
        return f"datasets/{dataset_id}/projections/{partition_path}/data.json"

    def _download_and_read_json(self, s3_key: str, tmpdir: str) -> Optional[List[Dict[str, Any]]]:
        """Download JSON file from S3 and read it.

        Args:
            s3_key: S3 object key.
            tmpdir: Temporary directory path.

        Returns:
            List of data dictionaries, or None if file doesn't exist.
        """
        if not self._s3_object_exists(s3_key):
            return None

        local_file = Path(tmpdir) / Path(s3_key).name
        self._s3_client.download_file(self._bucket, s3_key, str(local_file))

        with open(local_file, encoding="utf-8") as f:
            data = json.load(f)
            # Handle both list and dict formats
            if isinstance(data, dict):
                # If it's a dict, assume it has a 'data' key or convert to list
                return data.get("data", [data])
            return data if isinstance(data, list) else []

    def _s3_object_exists(self, s3_key: str) -> bool:
        """Check if S3 object exists.

        Args:
            s3_key: S3 object key.

        Returns:
            True if object exists, False otherwise.
        """
        try:
            self._s3_client.head_object(Bucket=self._bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise

    def _merge_json_data(
        self, projections_data: Optional[List[Dict[str, Any]]], staging_data: Optional[List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """Merge projections and staging data, removing duplicates.

        Args:
            projections_data: Existing projections data (can be None).
            staging_data: New staging data (can be None).

        Returns:
            Merged list of data dictionaries.
        """
        if projections_data is None and staging_data is None:
            return []

        if projections_data is None:
            return staging_data or []

        if staging_data is None:
            return projections_data

        # Combine both lists
        combined = projections_data + staging_data

        # Remove duplicates based on (obs_time, internal_series_code)
        # Keep first occurrence (projections data takes precedence since it comes first)
        seen = set()
        merged = []
        for item in combined:
            obs_time = item.get("obs_time")
            internal_series_code = item.get("internal_series_code")
            
            # Convert obs_time to string for comparison if it's a datetime
            if isinstance(obs_time, datetime):
                obs_time_key = obs_time.isoformat()
            else:
                obs_time_key = str(obs_time) if obs_time else None
            
            key = (obs_time_key, str(internal_series_code) if internal_series_code else None)
            if key not in seen:
                seen.add(key)
                merged.append(item)

        return merged

    def _write_json(self, data: List[Dict[str, Any]], output_file: Path) -> None:
        """Write data to JSON file.

        Args:
            data: List of data dictionaries to write.
            output_file: Output file path.
        """
        # Convert datetime objects to ISO format strings for JSON serialization
        json_data = self._serialize_datetimes(data)
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)

    def _serialize_datetimes(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Serialize datetime objects to ISO format strings for JSON.

        Args:
            data: List of data dictionaries.

        Returns:
            List with datetime objects converted to ISO strings.
        """
        serialized = []
        for item in data:
            serialized_item = {}
            for key, value in item.items():
                if isinstance(value, datetime):
                    serialized_item[key] = value.isoformat()
                else:
                    serialized_item[key] = value
            serialized.append(serialized_item)
        return serialized

    def _upload_to_staging(self, s3_key: str, local_file: Path) -> None:
        """Upload file to staging in S3.

        Args:
            s3_key: S3 destination key.
            local_file: Local file path.
        """
        self._s3_client.upload_file(str(local_file), self._bucket, s3_key)
