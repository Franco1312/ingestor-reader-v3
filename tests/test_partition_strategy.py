"""Tests for partition strategy."""

from datetime import datetime

import pytest
import pytz

from src.infrastructure.partitioning import SeriesYearMonthPartitionStrategy
from tests.builders import DataPointBuilder


class TestPartitionStrategy:
    """Tests for PartitionStrategy interface and implementations."""

    def test_get_partition_path_returns_series_year_month_format(self):
        """Test that partition path follows the pattern: {series_code}/year={YYYY}/month={MM}/"""

        strategy = SeriesYearMonthPartitionStrategy()

        # Create a data point with known values
        data_point = (
            DataPointBuilder()
            .with_series_code("TEST_SERIES")
            .with_obs_time(datetime(2024, 1, 15, 10, 30, 0))
            .with_value(100.0)
            .build()
        )

        partition_path = strategy.get_partition_path(data_point)

        assert partition_path == "TEST_SERIES/year=2024/month=01/"

    def test_get_partition_path_handles_timezone_aware_datetime(self):
        """Test that partition path works with timezone-aware datetimes."""
        strategy = SeriesYearMonthPartitionStrategy()

        # Create timezone-aware datetime
        tz = pytz.timezone("America/Argentina/Buenos_Aires")
        obs_time = tz.localize(datetime(2024, 3, 20, 14, 30, 0))

        data_point = (
            DataPointBuilder()
            .with_series_code("BCRA_SERIES")
            .with_obs_time(obs_time)
            .with_value(200.0)
            .build()
        )

        partition_path = strategy.get_partition_path(data_point)

        assert partition_path == "BCRA_SERIES/year=2024/month=03/"

    def test_group_by_partition_groups_data_points_by_partition(self):
        """Test that group_by_partition groups data points correctly."""
        strategy = SeriesYearMonthPartitionStrategy()

        # Create data points that should be grouped into different partitions
        data_points = [
            DataPointBuilder()
            .with_series_code("SERIES_A")
            .with_obs_time(datetime(2024, 1, 15))
            .with_value(100.0)
            .build(),
            DataPointBuilder()
            .with_series_code("SERIES_A")
            .with_obs_time(datetime(2024, 1, 20))
            .with_value(110.0)
            .build(),
            DataPointBuilder()
            .with_series_code("SERIES_A")
            .with_obs_time(datetime(2024, 2, 10))
            .with_value(120.0)
            .build(),
            DataPointBuilder()
            .with_series_code("SERIES_B")
            .with_obs_time(datetime(2024, 1, 15))
            .with_value(200.0)
            .build(),
        ]

        grouped = strategy.group_by_partition(data_points)

        # Should have 3 partitions:
        # - SERIES_A/year=2024/month=01/ (2 data points)
        # - SERIES_A/year=2024/month=02/ (1 data point)
        # - SERIES_B/year=2024/month=01/ (1 data point)
        assert len(grouped) == 3
        assert len(grouped["SERIES_A/year=2024/month=01/"]) == 2
        assert len(grouped["SERIES_A/year=2024/month=02/"]) == 1
        assert len(grouped["SERIES_B/year=2024/month=01/"]) == 1

    def test_get_partition_path_raises_error_on_missing_series_code(self):
        """Test that get_partition_path raises ValueError when series_code is missing."""
        strategy = SeriesYearMonthPartitionStrategy()

        data_point = (
            DataPointBuilder().with_obs_time(datetime(2024, 1, 15)).with_value(100.0).build()
        )

        with pytest.raises(
            ValueError, match="data_point must have 'internal_series_code' and 'obs_time'"
        ):
            strategy.get_partition_path(data_point)

    def test_get_partition_path_raises_error_on_missing_obs_time(self):
        """Test that get_partition_path raises ValueError when obs_time is missing."""
        strategy = SeriesYearMonthPartitionStrategy()

        data_point = DataPointBuilder().with_series_code("TEST_SERIES").with_value(100.0).build()

        with pytest.raises(
            ValueError, match="data_point must have 'internal_series_code' and 'obs_time'"
        ):
            strategy.get_partition_path(data_point)

    def test_get_partition_path_raises_error_on_invalid_obs_time_type(self):
        """Test that get_partition_path raises ValueError when obs_time is not a datetime."""
        strategy = SeriesYearMonthPartitionStrategy()

        # Create data point with invalid obs_time type (string instead of datetime)
        data_point = {
            "internal_series_code": "TEST_SERIES",
            "obs_time": "2024-01-15",  # String instead of datetime
            "value": 100.0,
        }

        with pytest.raises(ValueError, match="obs_time must be a datetime object"):
            strategy.get_partition_path(data_point)

    def test_parse_partition_path_extracts_components(self):
        """Test that parse_partition_path correctly extracts components from partition path."""
        strategy = SeriesYearMonthPartitionStrategy()

        partition_path = "TEST_SERIES/year=2024/month=01/"
        result = strategy.parse_partition_path(partition_path)

        assert result == {
            "internal_series_code": "TEST_SERIES",
            "year": "2024",
            "month": "01",
        }

    def test_parse_partition_path_handles_path_without_trailing_slash(self):
        """Test that parse_partition_path works with or without trailing slash."""
        strategy = SeriesYearMonthPartitionStrategy()

        partition_path = "TEST_SERIES/year=2024/month=01"
        result = strategy.parse_partition_path(partition_path)

        assert result == {
            "internal_series_code": "TEST_SERIES",
            "year": "2024",
            "month": "01",
        }

    def test_parse_partition_path_raises_error_on_invalid_format(self):
        """Test that parse_partition_path raises ValueError on invalid format."""
        strategy = SeriesYearMonthPartitionStrategy()

        with pytest.raises(ValueError, match="Invalid partition path format"):
            strategy.parse_partition_path("invalid/path")

    def test_get_all_partitions_from_paths_extracts_unique_partitions(self):
        """Test that get_all_partitions_from_paths extracts unique partitions from S3 paths."""
        strategy = SeriesYearMonthPartitionStrategy()

        paths = [
            "data/TEST_SERIES/year=2024/month=01/part-00000.parquet",
            "data/TEST_SERIES/year=2024/month=01/part-00001.parquet",
            "data/TEST_SERIES/year=2024/month=02/part-00000.parquet",
            "data/ANOTHER_SERIES/year=2024/month=01/part-00000.parquet",
            "some/other/path/file.txt",  # Should be ignored
        ]

        partitions = strategy.get_all_partitions_from_paths(paths)

        assert partitions == {
            "TEST_SERIES/year=2024/month=01/",
            "TEST_SERIES/year=2024/month=02/",
            "ANOTHER_SERIES/year=2024/month=01/",
        }

    def test_get_all_partitions_from_paths_handles_empty_list(self):
        """Test that get_all_partitions_from_paths returns empty set for empty list."""
        strategy = SeriesYearMonthPartitionStrategy()

        partitions = strategy.get_all_partitions_from_paths([])

        assert partitions == set()
