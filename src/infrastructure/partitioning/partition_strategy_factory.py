"""Factory for creating PartitionStrategy instances."""

from typing import Any, Dict, Optional

from src.infrastructure.partitioning.partition_strategy import PartitionStrategy
from src.infrastructure.partitioning.strategies.series_year_month import (
    SeriesYearMonthPartitionStrategy,
)


class PartitionStrategyFactory:
    """Factory for creating PartitionStrategy instances based on configuration."""

    DEFAULT_STRATEGY = "series_year_month"

    @staticmethod
    def create(config: Optional[Dict[str, Any]] = None) -> PartitionStrategy:
        """Create PartitionStrategy instance from configuration.

        Args:
            config: Configuration dictionary. Reads 'load.partition_strategy' or uses default.
                Examples:
                - {"load": {"partition_strategy": "series_year_month"}}
                - {"load": {}}  # defaults to series_year_month
                - {}  # defaults to series_year_month
                - None  # defaults to series_year_month

        Returns:
            PartitionStrategy instance (defaults to SeriesYearMonthPartitionStrategy).

        Raises:
            ValueError: If partition_strategy is unknown.
        """
        if config is None:
            config = {}

        load_config = config.get("load", {})
        strategy_name = load_config.get(
            "partition_strategy", PartitionStrategyFactory.DEFAULT_STRATEGY
        )

        if strategy_name == "series_year_month":
            return SeriesYearMonthPartitionStrategy()

        raise ValueError(f"Unknown partition strategy: {strategy_name}")
