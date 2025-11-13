"""Projection notification service for SNS."""

import json
import logging
from typing import Any

import boto3

logger = logging.getLogger(__name__)


class ProjectionNotificationService:
    """Service for publishing projection update notifications to SNS."""

    def __init__(
        self, topic_arn: str, sns_client: Any = None, aws_region: str = "us-east-1"
    ):
        """Initialize ProjectionNotificationService.

        Args:
            topic_arn: SNS topic ARN where notifications will be published.
            sns_client: Boto3 SNS client (optional, for testing).
            aws_region: AWS region (default: us-east-1).
        """
        self._topic_arn = topic_arn
        self._sns_client = sns_client or boto3.client("sns", region_name=aws_region)

    def notify_projection_update(
        self,
        dataset_id: str,
        bucket: str,
        version_manifest_path: str,
        projections_path: str,
    ) -> None:
        """Publish a projection update notification to SNS.

        Args:
            dataset_id: Dataset identifier.
            bucket: S3 bucket name.
            version_manifest_path: Path to the version manifest in S3.
            projections_path: Base path to projections in S3.
        """
        event = {
            "event": "projection_update",
            "dataset_id": dataset_id,
            "bucket": bucket,
            "version_manifest_path": version_manifest_path,
            "projections_path": projections_path,
        }

        try:
            self._sns_client.publish(
                TopicArn=self._topic_arn,
                Message=json.dumps(event),
                Subject=f"projection_update:{dataset_id}",
            )
            logger.info(
                "Published projection update notification for dataset %s",
                dataset_id,
            )
        except Exception as e:  # noqa: BLE001
            logger.error(
                "Failed to publish projection update notification: %s",
                e,
                exc_info=True,
            )
            # Don't raise - notification failure shouldn't break the projection

