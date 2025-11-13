"""Tests for projection notification service."""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.infrastructure.notifications.projection_notification_service import (
    ProjectionNotificationService,
)


class TestProjectionNotificationService:
    """Tests for ProjectionNotificationService."""

    def test_notify_projection_update_publishes_correct_message(self):
        """Test that notify_projection_update publishes the correct message."""
        # Arrange
        topic_arn = "arn:aws:sns:us-east-1:123456789012:projection-updates"
        mock_sns_client = MagicMock()
        service = ProjectionNotificationService(
            topic_arn=topic_arn, sns_client=mock_sns_client
        )

        dataset_id = "test_dataset"
        bucket = "test-bucket"
        version_manifest_path = "datasets/test_dataset/versions/v1/manifest.json"
        projections_path = "datasets/test_dataset/projections/"

        # Act
        service.notify_projection_update(
            dataset_id=dataset_id,
            bucket=bucket,
            version_manifest_path=version_manifest_path,
            projections_path=projections_path,
        )

        # Assert
        mock_sns_client.publish.assert_called_once()
        call_args = mock_sns_client.publish.call_args

        assert call_args.kwargs["TopicArn"] == topic_arn
        assert call_args.kwargs["Subject"] == f"projection_update:{dataset_id}"

        message = json.loads(call_args.kwargs["Message"])
        assert message["event"] == "projection_update"
        assert message["dataset_id"] == dataset_id
        assert message["bucket"] == bucket
        assert message["version_manifest_path"] == version_manifest_path
        assert message["projections_path"] == projections_path

    def test_notify_projection_update_handles_sns_error_gracefully(self):
        """Test that notify_projection_update handles SNS errors gracefully."""
        # Arrange
        topic_arn = "arn:aws:sns:us-east-1:123456789012:projection-updates"
        mock_sns_client = MagicMock()
        mock_sns_client.publish.side_effect = Exception("SNS error")
        service = ProjectionNotificationService(
            topic_arn=topic_arn, sns_client=mock_sns_client
        )

        # Act & Assert - should not raise
        service.notify_projection_update(
            dataset_id="test_dataset",
            bucket="test-bucket",
            version_manifest_path="datasets/test_dataset/versions/v1/manifest.json",
            projections_path="datasets/test_dataset/projections/",
        )

        # Should have attempted to publish
        mock_sns_client.publish.assert_called_once()

    def test_notify_projection_update_creates_sns_client_if_not_provided(self):
        """Test that notify_projection_update creates SNS client if not provided."""
        # Arrange
        topic_arn = "arn:aws:sns:us-east-1:123456789012:projection-updates"

        with patch("boto3.client") as mock_boto3_client:
            mock_sns = MagicMock()
            mock_boto3_client.return_value = mock_sns

            service = ProjectionNotificationService(topic_arn=topic_arn)

            # Act
            service.notify_projection_update(
                dataset_id="test_dataset",
                bucket="test-bucket",
                version_manifest_path="datasets/test_dataset/versions/v1/manifest.json",
                projections_path="datasets/test_dataset/projections/",
            )

            # Assert
            mock_boto3_client.assert_called_once_with("sns", region_name="us-east-1")
            mock_sns.publish.assert_called_once()

