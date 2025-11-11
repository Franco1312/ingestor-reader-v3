"""Domain interfaces (ports)."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional


class Extractor(ABC):
    """Interface for data extraction."""

    @abstractmethod
    def extract(self) -> bytes:
        """Extract data from source.

        Returns:
            bytes: Raw data extracted from the source.
        """


class Parser(ABC):
    """Interface for data parsing (plugin-based)."""

    @abstractmethod
    def parse(
        self,
        raw_data: bytes,
        config: Dict[str, Any],
        series_last_dates: Optional[Dict[str, datetime]] = None,
    ) -> Any:
        """Parse raw data according to config.

        Args:
            raw_data: Raw data to parse.
            config: Configuration dictionary for parsing.
            series_last_dates: Dictionary mapping series_code to last processed date (for incremental updates).

        Returns:
            Parsed data structure.
        """


class Normalizer(ABC):
    """Interface for data normalization (plugin-based)."""

    @abstractmethod
    def normalize(self, data: Any, config: Dict[str, Any]) -> Any:
        """Normalize data according to config.

        Args:
            data: Data to normalize.
            config: Configuration dictionary for normalization.

        Returns:
            Normalized data structure.
        """


class Transformer(ABC):
    """Interface for data transformation."""

    @abstractmethod
    def transform(self, data: Any, config: Dict[str, Any]) -> Any:
        """Transform data.

        Args:
            data: Data to transform.
            config: Configuration dictionary for transformation.

        Returns:
            Transformed data structure.
        """


class Loader(ABC):
    """Interface for data loading."""

    @abstractmethod
    def load(self, data: Any, config: Dict[str, Any]) -> None:
        """Load data to destination.

        Args:
            data: Data to load.
            config: Configuration dictionary for loading.
        """


class StateManager(ABC):
    """Interface for state management (incremental updates)."""

    @abstractmethod
    def get_series_last_dates(self, config: Dict[str, Any]) -> Dict[str, datetime]:
        """Get last processed date for each series in config.

        Args:
            config: Configuration dictionary containing parse_config.

        Returns:
            Dictionary mapping series_code to last processed date.
        """

    @abstractmethod
    def save_dates_from_data(self, data: List[Dict[str, Any]]) -> None:
        """Save max date for each series from normalized data.

        Args:
            data: List of normalized data points with obs_time and internal_series_code.
        """

    @abstractmethod
    def get_last_date(self, series_code: str) -> Optional[datetime]:
        """Get last processed date for a series.

        Args:
            series_code: Internal series code.

        Returns:
            Last processed date (naive datetime) or None if not found.
        """


class ConfigLoader(ABC):
    """Interface for configuration loading."""

    @abstractmethod
    def load_dataset_config(self, dataset_id: str) -> Dict[str, Any]:
        """Load configuration for a dataset.

        Args:
            dataset_id: Identifier of the dataset configuration to load.

        Returns:
            Dictionary containing the dataset configuration.
        """


class LockManager(ABC):
    """Interface for distributed locking."""

    @abstractmethod
    def acquire(self, lock_key: str, timeout_seconds: int = 300) -> bool:
        """Acquire a lock for the given key.

        Args:
            lock_key: Unique identifier for the lock.
            timeout_seconds: Lock expiration time in seconds (default: 300).

        Returns:
            True if lock was acquired, False if already locked.
        """

    @abstractmethod
    def release(self, lock_key: str) -> None:
        """Release a lock for the given key.

        Args:
            lock_key: Unique identifier for the lock.
        """

    def __enter__(self):
        """Context manager entry - returns self."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - no-op, use explicit release."""
        return False
