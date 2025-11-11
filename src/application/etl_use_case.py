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
        """
        self._extractor = extractor
        self._parser = parser
        self._normalizer = normalizer
        self._transformer = transformer
        self._loader = loader
        self._state_manager = state_manager
        self._lock_manager = lock_manager

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
        logger.info("Step 1/5: Extract - Retrieving raw data from source")
        raw_data = self._extractor.extract()
        logger.info("Extracted %d bytes of raw data", len(raw_data))

        series_last_dates = None
        if self._state_manager:
            logger.info("Loading state for incremental processing")
            series_last_dates = self._state_manager.get_series_last_dates(config)
            if series_last_dates:
                logger.info("Found state for %d series", len(series_last_dates))
            else:
                logger.info("No previous state found, processing all data")

        logger.info("Step 2/5: Parse - Converting raw data to structured format")
        data = self._parser.parse(raw_data, config, series_last_dates) if self._parser else []
        logger.info("Parsed %d data points", len(data))

        if self._normalizer:
            logger.info("Step 3/5: Normalize - Standardizing data structure")
            data = self._normalizer.normalize(data, config)
            logger.info("Normalized %d data points", len(data))
            if self._state_manager:
                logger.info("Saving state after normalization")
                self._state_manager.save_dates_from_data(data)
        else:
            logger.info("Step 3/5: Normalize - Skipped (no normalizer configured)")

        if self._transformer:
            logger.info("Step 4/5: Transform - Enriching data with metadata")
            data = self._transformer.transform(data, config)
            logger.info("Transformed %d data points", len(data))
        else:
            logger.info("Step 4/5: Transform - Skipped (no transformer configured)")

        if self._loader:
            logger.info("Step 5/5: Load - Persisting data to destination")
            self._loader.load(data, config)
            logger.info("Data loaded successfully")
        else:
            logger.info("Step 5/5: Load - Skipped (no loader configured)")

        logger.info("ETL pipeline completed. Total data points processed: %d", len(data))
        return data
