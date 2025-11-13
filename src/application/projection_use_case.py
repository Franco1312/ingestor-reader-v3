"""Projection use case for executing projections."""

import logging
from typing import Optional

from src.infrastructure.notifications.projection_notification_service import (
    ProjectionNotificationService,
)
from src.infrastructure.projections.projection_manager import ProjectionManager

logger = logging.getLogger(__name__)


class ProjectionUseCase:
    """High-level use case for executing projections."""

    def __init__(
        self,
        projection_manager: ProjectionManager,
        notification_service: Optional[ProjectionNotificationService] = None,
        bucket: Optional[str] = None,
    ):
        """Initialize ProjectionUseCase.

        Args:
            projection_manager: ProjectionManager instance.
            notification_service: Optional notification service for SNS.
            bucket: S3 bucket name (required if notification_service is provided).
        """
        self._projection_manager = projection_manager
        self._notification_service = notification_service
        self._bucket = bucket

    def execute_projection(self, version_id: str, dataset_id: str) -> None:
        """Execute projection for a version.

        Args:
            version_id: Version identifier.
            dataset_id: Dataset identifier.

        Raises:
            ValueError: If manifest is not found or other projection errors occur.
        """
        logger.info(
            "Starting projection for version %s, dataset %s", version_id, dataset_id
        )

        try:
            was_new_projection = self._projection_manager.project_version(version_id, dataset_id)
            logger.info(
                "Successfully completed projection for version %s, dataset %s",
                version_id,
                dataset_id,
            )

            if was_new_projection:
                self._notify_if_configured(version_id, dataset_id)
        except Exception as e:
            logger.error(
                "Failed to project version %s for dataset %s: %s",
                version_id,
                dataset_id,
                e,
            )
            raise

    def _notify_if_configured(self, version_id: str, dataset_id: str) -> None:
        """Notify that a projection was updated if notification service is configured.

        Args:
            version_id: Version identifier.
            dataset_id: Dataset identifier.
        """
        if not self._notification_service or not self._bucket:
            return

        self._notification_service.notify_projection_update(
            dataset_id=dataset_id,
            bucket=self._bucket,
            version_manifest_path=f"datasets/{dataset_id}/versions/{version_id}/manifest.json",
            projections_path=f"datasets/{dataset_id}/projections/",
        )

