"""INDEC IPC transformer plugin."""

from datetime import UTC, datetime
from typing import Any, Dict, List

from src.domain.interfaces import Transformer


class IndecIpcTransformer(Transformer):
    """Transformer for INDEC IPC normalized data.

    This transformer:
    - Ensures unit and frequency come from configuration (not from parser)
    - Adds collection_date (when the ETL was executed)
    """

    def transform(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform normalized data by adding metadata from config.

        Args:
            data: List of normalized data points.
            config: Configuration dictionary containing parse_config.

        Returns:
            List of transformed data points with unit, frequency, and collection_date.
        """
        # Get series map from config to lookup unit and frequency
        parse_config = config.get("parse_config", {})
        series_map = parse_config.get("series_map", [])

        # Create lookup dict: series_code -> {unit, frequency}
        series_metadata = {}
        for series_config in series_map:
            series_code = str(series_config.get("internal_series_code", ""))
            if series_code:
                series_metadata[series_code] = {
                    "unit": series_config.get("unit"),
                    "frequency": series_config.get("frequency"),
                }

        # Get collection date (when ETL is executed)
        collection_date = datetime.now(UTC)

        # Transform each data point
        transformed = []
        for data_point in data:
            series_code = data_point.get("internal_series_code")

            # Get unit and frequency from config (not from parser)
            metadata = series_metadata.get(series_code, {})
            unit = metadata.get("unit")
            frequency = metadata.get("frequency")

            # Create transformed data point
            transformed_point = {
                "obs_time": data_point["obs_time"],
                "internal_series_code": series_code,
                "value": data_point["value"],
                "unit": unit,  # From config
                "frequency": frequency,  # From config
                "collection_date": collection_date,  # When ETL was executed
            }

            transformed.append(transformed_point)

        return transformed

