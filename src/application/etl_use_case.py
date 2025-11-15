"""ETL use case."""

import logging
from typing import Optional

from ..domain.interfaces import (
    Extractor,
    Loader,
    LockManager,
    Normalizer,
    Parser,
    StateManager,
    Transformer,
)
from .projection_use_case import ProjectionUseCase

logger = logging.getLogger(__name__)


class ETLUseCase:
    """Orchestrates the ETL process."""

    def __init__(
        self,
        extractor: Extractor,
        parser: Optional[Parser] = None,
        normalizer: Optional[Normalizer] = None,
        transformer: Optional[Transformer] = None,
        loader: Optional[Loader] = None,
        state_manager: Optional[StateManager] = None,
        lock_manager: Optional[LockManager] = None,
        projection_use_case: Optional[ProjectionUseCase] = None,
    ):
        """Initialize ETL use case with dependencies.

        Args:
            extractor: Extractor instance.
            parser: Parser instance (optional).
            normalizer: Normalizer instance (optional).
            transformer: Transformer instance (optional).
            loader: Loader instance (optional).
            state_manager: StateManager instance for incremental updates (optional).
            lock_manager: LockManager instance for distributed locking (optional).
            projection_use_case: ProjectionUseCase instance for executing projections (optional).
        """
        self._extractor = extractor
        self._parser = parser
        self._normalizer = normalizer
        self._transformer = transformer
        self._loader = loader
        self._state_manager = state_manager
        self._lock_manager = lock_manager
        self._projection_use_case = projection_use_case

    @property
    def extractor(self):
        """Get extractor instance (for testing)."""
        return self._extractor

    @property
    def parser(self):
        """Get parser instance (for testing)."""
        return self._parser

    @property
    def normalizer(self):
        """Get normalizer instance (for testing)."""
        return self._normalizer

    @property
    def transformer(self):
        """Get transformer instance (for testing)."""
        return self._transformer

    @property
    def loader(self):
        """Get loader instance (for testing)."""
        return self._loader

    @property
    def state_manager(self):
        """Get state manager instance (for testing)."""
        return self._state_manager

    @property
    def lock_manager(self):
        """Get lock manager instance (for testing)."""
        return self._lock_manager

    def execute(self, config: Optional[dict] = None):
        """Execute the complete ETL process.

        Args:
            config: Configuration dictionary. May contain 'lock' config with:
                - 'key': Lock key (defaults to dataset_id)
                - 'timeout_seconds': Lock timeout (default: 300)

        Returns:
            Processed data.

        Raises:
            RuntimeError: If lock cannot be acquired.
        """
        config = config or {}
        dataset_id = config.get("dataset_id", "default")
        lock_key = None

        logger.info("Starting ETL pipeline for dataset: %s", dataset_id)

        if self._lock_manager:
            lock_config = config.get("lock", {})
            lock_key = lock_config.get("key", f"etl:{dataset_id}")
            timeout_seconds = lock_config.get("timeout_seconds", 300)
            logger.info("Attempting to acquire lock: %s (timeout: %ds)", lock_key, timeout_seconds)

            if not self._lock_manager.acquire(lock_key, timeout_seconds):
                logger.error("Failed to acquire lock: %s", lock_key)
                raise RuntimeError(
                    f"Could not acquire lock for '{lock_key}'. Another process may be running."
                )
            logger.info("Lock acquired successfully: %s", lock_key)

        try:
            return self._execute_etl(config)
        finally:
            if self._lock_manager and lock_key:
                logger.info("Releasing lock: %s", lock_key)
                self._lock_manager.release(lock_key)

    def _execute_etl(self, config: dict):
        """Execute ETL steps without lock management."""
        total_steps = self._calculate_total_steps()
        step_number = 1

        raw_data = self._execute_extract(step_number, total_steps)
        step_number += 1

        series_last_dates = self._load_state(config)
        data = self._execute_parse(raw_data, config, series_last_dates, step_number, total_steps)
        step_number += 1

        data = self._execute_normalize(data, config, step_number, total_steps)
        step_number += 1

        data = self._execute_transform(data, config, step_number, total_steps)
        step_number += 1

        self._execute_load(data, config, step_number, total_steps)

        logger.info("ETL pipeline completed. Total data points processed: %d", len(data))
        return data

    def _calculate_total_steps(self) -> int:
        """Calculate total number of ETL steps."""
        steps = 5  # Extract, Parse, Normalize, Transform, Load
        if self._projection_use_case:
            steps += 1
        return steps

    def _execute_extract(self, step_number: int, total_steps: int) -> bytes:
        """Execute extract step."""
        logger.info("Step %d/%d: Extract - Retrieving raw data from source", step_number, total_steps)
        raw_data = self._extractor.extract()
        logger.info("Extracted %d bytes of raw data", len(raw_data))
        return raw_data

    def _load_state(self, config: dict) -> Optional[dict]:
        """Load state for incremental processing."""
        if not self._state_manager:
            return None

        logger.info("Loading state for incremental processing")
        try:
            series_last_dates = self._state_manager.get_series_last_dates(config)
            if series_last_dates:
                logger.info("Found state for %d series", len(series_last_dates))
            else:
                logger.info("No previous state found, processing all data")
            return series_last_dates
        except Exception as e:
            logger.error("Error loading state: %s", e, exc_info=True)
            logger.warning("Continuing without state (will process all data)")
            return None

    def _execute_parse(self, raw_data: bytes, config: dict, series_last_dates: Optional[dict], step_number: int, total_steps: int) -> list:
        """Execute parse step."""
        logger.info("Step %d/%d: Parse - Converting raw data to structured format", step_number, total_steps)
        data = self._parser.parse(raw_data, config, series_last_dates) if self._parser else []
        logger.info("Parsed %d data points", len(data))
        return data

    def _execute_normalize(self, data: list, config: dict, step_number: int, total_steps: int) -> list:
        """Execute normalize step."""
        if self._normalizer:
            logger.info("Step %d/%d: Normalize - Standardizing data structure", step_number, total_steps)
            data = self._normalizer.normalize(data, config)
            logger.info("Normalized %d data points", len(data))
            if self._state_manager:
                logger.info("Saving state after normalization")
                self._state_manager.save_dates_from_data(data)
        else:
            logger.info("Step %d/%d: Normalize - Skipped (no normalizer configured)", step_number, total_steps)
        return data

    def _execute_transform(self, data: list, config: dict, step_number: int, total_steps: int) -> list:
        """Execute transform step."""
        if self._transformer:
            logger.info("Step %d/%d: Transform - Enriching data with metadata", step_number, total_steps)
            data = self._transformer.transform(data, config)
            logger.info("Transformed %d data points", len(data))
        else:
            logger.info("Step %d/%d: Transform - Skipped (no transformer configured)", step_number, total_steps)
        return data

    def _execute_load(self, data: list, config: dict, step_number: int, total_steps: int) -> None:
        """Execute load step and projection if configured."""
        if self._loader:
            logger.info("Step %d/%d: Load - Persisting data to destination", step_number, total_steps)
            self._loader.load(data, config)
            logger.info("Data loaded successfully")

            if self._projection_use_case:
                projection_step = step_number + 1
                self._execute_projection(config, projection_step, total_steps)
        else:
            logger.info("Step %d/%d: Load - Skipped (no loader configured)", step_number, total_steps)

    def _execute_projection(self, config: dict, step_number: int, total_steps: int) -> None:
        """Execute projection after successful load.

        Args:
            config: Configuration dictionary containing dataset_id and bucket.
            step_number: Current step number for logging.
            total_steps: Total number of steps for logging.
        """
        dataset_id = config.get("dataset_id", "default")
        load_config = config.get("load", {})
        bucket = load_config.get("bucket")
        aws_region = load_config.get("aws_region", "us-east-1")

        if not bucket:
            logger.warning("Cannot execute projection: bucket not found in config")
            return

        logger.info("Step %d/%d: Project - Executing projection for dataset %s", step_number, total_steps, dataset_id)

        try:
            version_id = self._get_current_version_id(bucket, aws_region, dataset_id)
            if not version_id:
                return

            if not self._projection_use_case:
                logger.warning("Projection use case not configured")
                return

            logger.info("Executing projection for version %s", version_id)
            self._projection_use_case.execute_projection(version_id, dataset_id)
            logger.info("Projection completed successfully")
        except Exception as e:
            logger.error("Failed to execute projection: %s", e)
            raise

    def _get_current_version_id(self, bucket: str, aws_region: str, dataset_id: str) -> Optional[str]:
        """Get current version ID from VersionManager.

        Args:
            bucket: S3 bucket name.
            aws_region: AWS region.
            dataset_id: Dataset identifier.

        Returns:
            Version ID or None if not found.
        """
        from src.infrastructure.versioning import VersionManager

        s3_client = getattr(self._loader, "_s3_client", None)
        version_manager = VersionManager(
            bucket=bucket, s3_client=s3_client, aws_region=aws_region
        )
        version_id = version_manager.get_current_version(dataset_id)

        if not version_id:
            logger.warning(
                "Cannot execute projection: no current version found for dataset %s",
                dataset_id,
            )
            return None

        return version_id
