"""Parquet writer for writing partitioned data."""

import os
from pathlib import Path
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

from src.infrastructure.partitioning.partition_strategy import PartitionStrategy


class ParquetWriter:
    """Writes data to Parquet format with partitioning."""

    def __init__(self, partition_strategy: PartitionStrategy, compression: str = "snappy"):
        """Initialize ParquetWriter.

        Args:
            partition_strategy: Partition strategy to use for grouping data.
            compression: Compression codec (default: "snappy").
        """
        self._partition_strategy = partition_strategy
        self._compression = compression

    def write_to_parquet(self, data: List[Dict[str, Any]], base_output_path: str) -> List[str]:
        """Write data to Parquet files partitioned by partition strategy.

        Args:
            data: List of data point dictionaries.
            base_output_path: Base directory path for output files.

        Returns:
            List of relative file paths (from base_output_path) of created parquet files.
        """
        if not data:
            return []

        # Group data by partition
        grouped = self._partition_strategy.group_by_partition(data)

        file_paths = []
        base_path = Path(base_output_path)

        for partition_path, partition_data in grouped.items():
            # Create partition directory
            partition_dir = base_path / partition_path
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Convert to PyArrow table
            table = self._data_to_table(partition_data)

            # Generate unique filename for this partition
            parquet_file = self._generate_parquet_filename(partition_dir)

            pq.write_table(
                table,
                str(parquet_file),
                compression=self._compression,
            )

            # Return relative path from base_output_path
            relative_path = os.path.relpath(str(parquet_file), base_output_path)
            file_paths.append(relative_path)

        return file_paths

    def _generate_parquet_filename(self, partition_dir: Path) -> Path:
        """Generate a unique filename for parquet file in partition.

        Args:
            partition_dir: Directory path for the partition.

        Returns:
            Path to the parquet file.
        """
        # Simple approach: use "data.parquet" if single file per partition
        # Future: could split into multiple parts if needed
        return partition_dir / "data.parquet"

    def _data_to_table(self, data: List[Dict[str, Any]]) -> pa.Table:
        """Convert list of dictionaries to PyArrow Table.

        Args:
            data: List of data point dictionaries.

        Returns:
            PyArrow Table.
        """
        schema = self.get_schema()

        # Extract columns
        arrays = {
            "obs_time": [],
            "internal_series_code": [],
            "value": [],
            "unit": [],
            "frequency": [],
            "collection_date": [],
        }

        for data_point in data:
            arrays["obs_time"].append(data_point.get("obs_time"))
            arrays["internal_series_code"].append(data_point.get("internal_series_code"))
            arrays["value"].append(data_point.get("value"))
            arrays["unit"].append(data_point.get("unit"))
            arrays["frequency"].append(data_point.get("frequency"))
            arrays["collection_date"].append(data_point.get("collection_date"))

        # Convert to PyArrow arrays
        pa_arrays = [
            pa.array(arrays["obs_time"], type=schema.field("obs_time").type),
            pa.array(
                arrays["internal_series_code"], type=schema.field("internal_series_code").type
            ),
            pa.array(arrays["value"], type=schema.field("value").type),
            pa.array(arrays["unit"], type=schema.field("unit").type),
            pa.array(arrays["frequency"], type=schema.field("frequency").type),
            pa.array(arrays["collection_date"], type=schema.field("collection_date").type),
        ]

        return pa.Table.from_arrays(pa_arrays, schema=schema)

    @staticmethod
    def get_schema() -> pa.Schema:
        """Get the PyArrow schema for data points.

        Returns:
            PyArrow Schema.
        """
        return pa.schema(
            [
                pa.field("obs_time", pa.timestamp("ns")),
                pa.field("internal_series_code", pa.string()),
                pa.field("value", pa.float64()),
                pa.field("unit", pa.string()),
                pa.field("frequency", pa.string()),
                pa.field("collection_date", pa.timestamp("ns")),
            ]
        )
