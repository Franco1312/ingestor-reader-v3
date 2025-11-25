"""Projection manifest manager for tracking projected versions."""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from botocore.exceptions import ClientError

from src.infrastructure.utils.aws_utils import create_s3_client

logger = logging.getLogger(__name__)


class ProjectionManifestManager:
    """Manages projection manifest to track projected versions."""

    def __init__(self, bucket: str, s3_client: Any = None, aws_region: str = "us-east-1"):
        """Initialize ProjectionManifestManager.

        Args:
            bucket: S3 bucket name.
            s3_client: Boto3 S3 client (optional, for testing).
            aws_region: AWS region (default: us-east-1).
        """
        self._bucket = bucket
        self._s3_client = create_s3_client(aws_region=aws_region, s3_client=s3_client)

    def is_version_projected(self, dataset_id: str, version_id: str) -> bool:
        """Check if a version has already been projected.

        Args:
            dataset_id: Dataset identifier.
            version_id: Version identifier.

        Returns:
            True if version is already projected, False otherwise.
        """
        manifest = self._load_manifest(dataset_id)
        if manifest is None:
            return False

        projected_versions = manifest.get("projected_versions", [])
        return version_id in projected_versions

    def add_projected_version(self, dataset_id: str, version_id: str) -> None:
        """Add a version to the list of projected versions.

        Args:
            dataset_id: Dataset identifier.
            version_id: Version identifier.
        """
        manifest = self._load_manifest(dataset_id) or self._create_empty_manifest()

        projected_versions = manifest.get("projected_versions", [])
        if version_id not in projected_versions:
            projected_versions.append(version_id)
            manifest["projected_versions"] = projected_versions

        now = datetime.now(timezone.utc)
        manifest["last_projection_date"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        manifest["last_projected_version"] = version_id

        self._save_manifest(dataset_id, manifest)

    def _load_manifest(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Load projection manifest from S3.

        Args:
            dataset_id: Dataset identifier.

        Returns:
            Manifest dictionary or None if not found.
        """
        key = f"datasets/{dataset_id}/projections/manifest.json"

        try:
            response = self._s3_client.get_object(Bucket=self._bucket, Key=key)
            with response["Body"] as body:
                manifest_json = body.read().decode("utf-8")
                return json.loads(manifest_json)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def _save_manifest(self, dataset_id: str, manifest: Dict[str, Any]) -> None:
        """Save projection manifest to S3.

        Args:
            dataset_id: Dataset identifier.
            manifest: Manifest dictionary.
        """
        key = f"datasets/{dataset_id}/projections/manifest.json"
        manifest_json = json.dumps(manifest, indent=2)

        self._s3_client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=manifest_json.encode("utf-8"),
            ContentType="application/json",
        )

    def _create_empty_manifest(self) -> Dict[str, Any]:
        """Create an empty manifest structure.

        Returns:
            Empty manifest dictionary.
        """
        return {
            "projected_versions": [],
            "last_projection_date": None,
            "last_projected_version": None,
        }

