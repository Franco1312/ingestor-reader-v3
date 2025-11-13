"""Projection manager for orchestrating the projection process."""

import logging
from typing import Any, Dict, List, Optional

import boto3

from src.infrastructure.projections.atomic_mover import AtomicProjectionMover
from src.infrastructure.projections.projection_merger import ProjectionMerger
from src.infrastructure.projections.projection_manifest_manager import (
    ProjectionManifestManager,
)
from src.infrastructure.projections.staging_manager import StagingManager
from src.infrastructure.versioning.manifest_manager import ManifestManager

logger = logging.getLogger(__name__)


class ProjectionManager:
    """Orchestrates the complete projection process."""

    def __init__(
        self,
        bucket: str,
        s3_client: Any = None,
        aws_region: str = "us-east-1",
        copy_workers: int = 1,
        merge_workers: int = 1,
    ):
        """Initialize ProjectionManager.

        Args:
            bucket: S3 bucket name.
            s3_client: Boto3 S3 client (optional, for testing).
            aws_region: AWS region (default: us-east-1).
            copy_workers: Number of parallel workers for copying files (default: 1).
            merge_workers: Number of parallel workers for merging partitions (default: 1).
        """
        self._bucket = bucket
        self._s3_client = s3_client or boto3.client("s3", region_name=aws_region)
        self._copy_workers = copy_workers
        self._merge_workers = merge_workers

    def project_version(self, version_id: str, dataset_id: str) -> bool:
        """Project a version to projections.

        Executes the complete flow:
        1. Check if version already projected (skip if yes)
        2. Cleanup staging (ensure clean start)
        3. Copy version data to staging
        4. Merge staging with existing projections
        5. Move staging to projections atomically (includes cleanup of staging)
        6. Record version as projected

        Args:
            version_id: Version identifier.
            dataset_id: Dataset identifier.

        Returns:
            True if new data was written to projections, False otherwise.

        Raises:
            ValueError: If manifest is not found.
        """
        logger.info("Starting projection for version %s, dataset %s", version_id, dataset_id)

        if self._is_version_already_projected(version_id, dataset_id):
            logger.info(
                "Version %s already projected for dataset %s, skipping", version_id, dataset_id
            )
            return False

        manifest = self._load_manifest(version_id, dataset_id)
        if manifest is None:
            raise ValueError(f"Manifest not found for version {version_id}, dataset {dataset_id}")

        parquet_files = manifest.get("parquet_files", [])
        if not parquet_files:
            logger.warning("No parquet files in manifest for version %s", version_id)
            return False

        self._cleanup_staging(dataset_id)
        self._copy_version_to_staging(version_id, dataset_id, parquet_files)
        self._merge_staging_with_projections(dataset_id)
        self._atomic_move_to_projections(dataset_id)
        self._record_projected_version(version_id, dataset_id)

        logger.info("Successfully projected version %s for dataset %s", version_id, dataset_id)
        return True

    def _load_manifest(self, version_id: str, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Load manifest for a version.

        Args:
            version_id: Version identifier.
            dataset_id: Dataset identifier.

        Returns:
            Manifest dictionary or None if not found.
        """
        manifest_manager = ManifestManager(
            bucket=self._bucket, s3_client=self._s3_client
        )
        return manifest_manager.load_manifest(dataset_id, version_id)

    def _copy_version_to_staging(
        self, version_id: str, dataset_id: str, parquet_files: List[str]
    ) -> None:
        """Copy version files to staging.

        Args:
            version_id: Version identifier.
            dataset_id: Dataset identifier.
            parquet_files: List of parquet file paths.
        """
        logger.info("Copying version %s to staging", version_id)
        staging_manager = StagingManager(
            bucket=self._bucket, s3_client=self._s3_client, copy_workers=self._copy_workers
        )
        staging_manager.copy_from_version(version_id, dataset_id, parquet_files)

    def _merge_staging_with_projections(self, dataset_id: str) -> None:
        """Merge staging data with existing projections.

        Args:
            dataset_id: Dataset identifier.
        """
        logger.info("Merging staging with projections for dataset %s", dataset_id)
        merger = ProjectionMerger(
            bucket=self._bucket, s3_client=self._s3_client, merge_workers=self._merge_workers
        )
        merger.merge_all_partitions(dataset_id)

    def _atomic_move_to_projections(self, dataset_id: str) -> None:
        """Move staging to projections atomically.

        Args:
            dataset_id: Dataset identifier.
        """
        logger.info("Moving staging to projections for dataset %s", dataset_id)
        mover = AtomicProjectionMover(bucket=self._bucket, s3_client=self._s3_client)
        mover.move_staging_to_projections(dataset_id)

    def _cleanup_staging(self, dataset_id: str) -> None:
        """Cleanup staging area.

        Args:
            dataset_id: Dataset identifier.
        """
        logger.info("Cleaning up staging for dataset %s", dataset_id)
        staging_manager = StagingManager(bucket=self._bucket, s3_client=self._s3_client)
        staging_manager.clear_staging(dataset_id)

    def _is_version_already_projected(self, version_id: str, dataset_id: str) -> bool:
        """Check if version has already been projected.

        Args:
            version_id: Version identifier.
            dataset_id: Dataset identifier.

        Returns:
            True if version is already projected, False otherwise.
        """
        manifest_manager = ProjectionManifestManager(
            bucket=self._bucket, s3_client=self._s3_client
        )
        return manifest_manager.is_version_projected(dataset_id, version_id)

    def _record_projected_version(self, version_id: str, dataset_id: str) -> None:
        """Record that a version has been successfully projected.

        Args:
            version_id: Version identifier.
            dataset_id: Dataset identifier.
        """
        manifest_manager = ProjectionManifestManager(
            bucket=self._bucket, s3_client=self._s3_client
        )
        manifest_manager.add_projected_version(dataset_id, version_id)

