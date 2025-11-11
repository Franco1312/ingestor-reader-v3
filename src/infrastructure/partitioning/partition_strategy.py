"""Partition strategy interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Set


class PartitionStrategy(ABC):
    """Abstract base class for partition strategies."""

    @abstractmethod
    def get_partition_path(self, data_point: Dict[str, Any]) -> str:
        """Get partition path for a data point.

        Args:
            data_point: Data point dictionary with obs_time and internal_series_code.

        Returns:
            Partition path string (e.g., "SERIES_CODE/year=2024/month=01/").
        """
        raise NotImplementedError

    @abstractmethod
    def group_by_partition(self, data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group data points by partition path.

        Args:
            data: List of data point dictionaries.

        Returns:
            Dictionary mapping partition paths to lists of data points.
        """
        raise NotImplementedError

    @abstractmethod
    def parse_partition_path(self, partition_path: str) -> Dict[str, str]:
        """Parse a partition path to extract components.

        Args:
            partition_path: Partition path string.

        Returns:
            Dictionary with parsed components (e.g., {"internal_series_code": "...", "year": "2024", "month": "01"}).
        """
        raise NotImplementedError

    @abstractmethod
    def get_all_partitions_from_paths(self, paths: List[str]) -> Set[str]:
        """Extract all unique partitions from a list of S3 paths.

        Args:
            paths: List of S3 object paths.

        Returns:
            Set of unique partition paths.
        """
        raise NotImplementedError
