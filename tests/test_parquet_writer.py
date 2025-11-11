"""Tests for ParquetWriter."""

from datetime import datetime
from pathlib import Path

import pytest

from src.infrastructure.partitioning import SeriesYearMonthPartitionStrategy
from src.infrastructure.storage.parquet.parquet_writer import ParquetWriter
from tests.builders import DataPointBuilder


class TestParquetWriter:
    """Tests for ParquetWriter class."""

    @pytest.fixture
    def partition_strategy(self):
        """Create a partition strategy instance."""
        return SeriesYearMonthPartitionStrategy()

    @pytest.fixture
    def parquet_writer(self, partition_strategy):
        """Create a ParquetWriter instance."""
        return ParquetWriter(partition_strategy=partition_strategy, compression="snappy")

    def test_write_to_parquet_creates_files_for_each_partition(self, parquet_writer, tmp_path):
        """Test that write_to_parquet creates one file per partition."""
        data_points = [
            DataPointBuilder()
            .with_series_code("SERIES_A")
            .with_obs_time(datetime(2024, 1, 15))
            .with_value(100.0)
            .with_unit("test_unit")
            .with_frequency("D")
            .build(),
            DataPointBuilder()
            .with_series_code("SERIES_A")
            .with_obs_time(datetime(2024, 1, 20))
            .with_value(110.0)
            .with_unit("test_unit")
            .with_frequency("D")
            .build(),
            DataPointBuilder()
            .with_series_code("SERIES_B")
            .with_obs_time(datetime(2024, 1, 15))
            .with_value(200.0)
            .with_unit("test_unit")
            .with_frequency("D")
            .build(),
        ]

        output_path = str(tmp_path / "output")
        file_paths = parquet_writer.write_to_parquet(data_points, output_path)

        # Should create files in partitions: SERIES_A/year=2024/month=01/ and SERIES_B/year=2024/month=01/
        assert len(file_paths) == 2
        assert any("SERIES_A/year=2024/month=01" in path for path in file_paths)
        assert any("SERIES_B/year=2024/month=01" in path for path in file_paths)

    def test_write_to_parquet_creates_valid_parquet_files(self, parquet_writer, tmp_path):
        """Test that write_to_parquet creates valid parquet files."""
        data_point = (
            DataPointBuilder()
            .with_series_code("TEST_SERIES")
            .with_obs_time(datetime(2024, 1, 15, 10, 30, 0))
            .with_value(100.5)
            .with_unit("test_unit")
            .with_frequency("D")
            .build()
        )
        # Add collection_date as it's expected in transformed data
        data_point["collection_date"] = datetime(2024, 1, 15, 12, 0, 0)

        output_path = str(tmp_path / "output")
        file_paths = parquet_writer.write_to_parquet([data_point], output_path)

        assert len(file_paths) == 1
        full_path = tmp_path / "output" / file_paths[0]
        assert full_path.exists()
        assert full_path.suffix == ".parquet"
        assert full_path.name == "data.parquet"

    def test_write_to_parquet_handles_empty_data(self, parquet_writer, tmp_path):
        """Test that write_to_parquet handles empty data gracefully."""
        output_path = str(tmp_path / "output")
        file_paths = parquet_writer.write_to_parquet([], output_path)

        assert file_paths == []

    def test_write_to_parquet_returns_relative_paths(self, parquet_writer, tmp_path):
        """Test that write_to_parquet returns relative paths from base_output_path."""
        data_point = (
            DataPointBuilder()
            .with_series_code("TEST_SERIES")
            .with_obs_time(datetime(2024, 1, 15))
            .with_value(100.0)
            .with_unit("test_unit")
            .with_frequency("D")
            .build()
        )
        data_point["collection_date"] = datetime(2024, 1, 15, 12, 0, 0)

        output_path = str(tmp_path / "output")
        file_paths = parquet_writer.write_to_parquet([data_point], output_path)

        assert len(file_paths) == 1
        # Path should be relative, not absolute
        assert not Path(file_paths[0]).is_absolute()
        assert "TEST_SERIES/year=2024/month=01" in file_paths[0]
