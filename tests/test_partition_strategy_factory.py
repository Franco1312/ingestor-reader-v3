"""Tests for PartitionStrategyFactory."""

import pytest

from src.infrastructure.partitioning import (
    PartitionStrategyFactory,
    SeriesYearMonthPartitionStrategy,
)


class TestPartitionStrategyFactory:
    """Tests for PartitionStrategyFactory class."""

    def test_create_with_series_year_month_strategy(self):
        """Test creating SeriesYearMonthPartitionStrategy explicitly."""
        config = {
            "load": {
                "plugin": "s3_versioned",
                "partition_strategy": "series_year_month",
            }
        }
        result = PartitionStrategyFactory.create(config)

        assert isinstance(result, SeriesYearMonthPartitionStrategy)

    def test_create_without_partition_strategy_defaults_to_series_year_month(self):
        """Test that missing partition_strategy defaults to 'series_year_month'."""
        config = {
            "load": {
                "plugin": "s3_versioned",
            }
        }
        result = PartitionStrategyFactory.create(config)

        assert isinstance(result, SeriesYearMonthPartitionStrategy)

    def test_create_with_empty_load_config_defaults_to_series_year_month(self):
        """Test that empty load config defaults to 'series_year_month'."""
        config = {"load": {}}
        result = PartitionStrategyFactory.create(config)

        assert isinstance(result, SeriesYearMonthPartitionStrategy)

    def test_create_without_load_config_defaults_to_series_year_month(self):
        """Test that missing load config defaults to 'series_year_month'."""
        config = {}
        result = PartitionStrategyFactory.create(config)

        assert isinstance(result, SeriesYearMonthPartitionStrategy)

    def test_create_with_none_config_defaults_to_series_year_month(self):
        """Test that None config defaults to 'series_year_month'."""
        result = PartitionStrategyFactory.create(None)

        assert isinstance(result, SeriesYearMonthPartitionStrategy)

    def test_create_with_unknown_strategy_raises_error(self):
        """Test that unknown strategy raises ValueError."""
        config = {
            "load": {
                "partition_strategy": "unknown_strategy",
            }
        }

        with pytest.raises(ValueError, match="Unknown partition strategy: unknown_strategy"):
            PartitionStrategyFactory.create(config)

    def test_create_returns_strategy_with_required_methods(self):
        """Test that created strategy has all required methods."""
        config = {"load": {"partition_strategy": "series_year_month"}}
        result = PartitionStrategyFactory.create(config)

        assert hasattr(result, "get_partition_path")
        assert hasattr(result, "group_by_partition")
        assert hasattr(result, "parse_partition_path")
        assert hasattr(result, "get_all_partitions_from_paths")
