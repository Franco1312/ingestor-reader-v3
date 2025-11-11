"""Series-year-month partition strategy implementation."""

import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Set

from src.infrastructure.partitioning.partition_strategy import PartitionStrategy


class SeriesYearMonthPartitionStrategy(PartitionStrategy):
    """Partition strategy: {internal_series_code}/year={YYYY}/month={MM}/"""

    # Regex patterns for parsing partition paths
    PARTITION_PATH_PATTERN = re.compile(r"([^/]+)/year=(\d+)/month=(\d+)/?")
    PARTITION_IN_PATH_PATTERN = re.compile(r"([^/]+)/year=(\d+)/month=(\d+)/")

    def get_partition_path(self, data_point: Dict[str, Any]) -> str:
        """Get partition path for a data point.

        Args:
            data_point: Data point dictionary with obs_time and internal_series_code.

        Returns:
            Partition path string (e.g., "SERIES_CODE/year=2024/month=01/").
        """
        series_code = str(data_point.get("internal_series_code", ""))
        obs_time = data_point.get("obs_time")

        if not series_code or not obs_time:
            raise ValueError("data_point must have 'internal_series_code' and 'obs_time'")

        if not isinstance(obs_time, datetime):
            raise ValueError("obs_time must be a datetime object")

        return f"{series_code}/year={obs_time.year}/month={obs_time.month:02d}/"

    def group_by_partition(self, data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group data points by partition path.

        Args:
            data: List of data point dictionaries.

        Returns:
            Dictionary mapping partition paths to lists of data points.
        """
        grouped = defaultdict(list)
        for data_point in data:
            grouped[self.get_partition_path(data_point)].append(data_point)
        return dict(grouped)

    def parse_partition_path(self, partition_path: str) -> Dict[str, str]:
        """Parse a partition path to extract components.

        Args:
            partition_path: Partition path string (e.g., "SERIES_CODE/year=2024/month=01/").

        Returns:
            Dictionary with parsed components.
        """
        match = self.PARTITION_PATH_PATTERN.match(partition_path)
        if not match:
            raise ValueError(f"Invalid partition path format: {partition_path}")

        return {
            "internal_series_code": match.group(1),
            "year": match.group(2),
            "month": match.group(3),
        }

    def get_all_partitions_from_paths(self, paths: List[str]) -> Set[str]:
        """Extract all unique partitions from a list of S3 paths.

        Args:
            paths: List of S3 object paths (e.g., ["data/SERIES/year=2024/month=01/file.parquet"]).

        Returns:
            Set of unique partition paths (e.g., {"SERIES/year=2024/month=01/"}).
        """
        partitions = set()
        for path in paths:
            match = self.PARTITION_IN_PATH_PATTERN.search(path)
            if match:
                partitions.add(f"{match.group(1)}/year={match.group(2)}/month={match.group(3)}/")

        return partitions
