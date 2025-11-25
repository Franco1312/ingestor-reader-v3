"""INDEC IPC parser plugin."""

import io
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd

from src.domain.interfaces import Parser
from src.infrastructure.utils.date_utils import to_naive
from src.infrastructure.utils.excel_utils import excel_column_to_index

CellValue = Union[datetime, str, float, int, None]
SeriesDataPoint = Dict[str, Union[str, CellValue]]


class IndecIpcParser(Parser):
    """Parser for INDEC IPC Excel files.
    
    This parser handles Excel files where dates are in a horizontal row
    and values are in another horizontal row below it.
    """

    def parse(
        self, 
        raw_data: bytes, 
        config: Dict[str, Union[str, int, Dict, List]],
        series_last_dates: Optional[Dict[str, datetime]] = None
    ) -> List[SeriesDataPoint]:
        """Parse raw Excel data according to INDEC IPC configuration.
        
        Args:
            raw_data: Raw bytes of the Excel file.
            config: Configuration dictionary containing parse_config.
            series_last_dates: Dictionary mapping series_code to last processed date.
            
        Returns:
            List of dictionaries containing parsed series data.
        """
        parse_config = config.get("parse_config", {})
        series_map = parse_config.get("series_map", [])
        
        parsed_data: List[SeriesDataPoint] = []
        
        for series_config in series_map:
            series_code = str(series_config.get("internal_series_code", ""))
            last_date = series_last_dates.get(series_code) if series_last_dates else None
            series_data = self._extract_series(raw_data, series_config, last_date)
            parsed_data.extend(series_data)
        
        return parsed_data
    
    def _extract_series(
        self, 
        raw_data: bytes,
        series_config: Dict[str, Union[str, int, bool]],
        last_date: Optional[datetime] = None
    ) -> List[SeriesDataPoint]:
        """Extract data for a single series from the Excel file.
        
        Args:
            raw_data: Raw bytes of the Excel file.
            series_config: Configuration for the series to extract.
            last_date: Last processed date for this series (filters dates <= last_date).
            
        Returns:
            List of dictionaries containing series data points.
        """
        sheet_name = series_config["sheet"]
        fecha_row = series_config["fecha_row"]  # Row number (1-indexed)
        fecha_start_col = series_config["fecha_start_col"]  # Column letter (e.g., "B")
        valor_row = series_config["valor_row"]  # Row number (1-indexed)
        valor_start_col = series_config["valor_start_col"]  # Column letter (e.g., "B")
        drop_na = series_config.get("drop_na", False)
        
        # Convert Excel column letters to 0-based indices
        fecha_start_col_idx = excel_column_to_index(fecha_start_col)
        valor_start_col_idx = excel_column_to_index(valor_start_col)
        
        # Load Excel file using pandas with xlrd engine for .xls files
        excel_file = io.BytesIO(raw_data)
        try:
            df = pd.read_excel(
                excel_file, 
                sheet_name=sheet_name, 
                engine='xlrd', 
                header=None
            )
        except Exception:
            # Try with openpyxl if xlrd fails
            excel_file.seek(0)
            df = pd.read_excel(
                excel_file, 
                sheet_name=sheet_name, 
                engine='openpyxl', 
                header=None
            )
        
        # Convert to 0-based row indices
        fecha_row_idx = fecha_row - 1
        valor_row_idx = valor_row - 1
        
        # Extract dates from the fecha row
        fecha_row_data = df.iloc[fecha_row_idx, fecha_start_col_idx:]
        valor_row_data = df.iloc[valor_row_idx, valor_start_col_idx:]
        
        # Build series data points
        series_data: List[SeriesDataPoint] = []
        
        for fecha_col_idx, (fecha_val, valor_val) in enumerate(
            zip(fecha_row_data, valor_row_data)
        ):
            # Skip if fecha is None or not a datetime
            if pd.isna(fecha_val) or not isinstance(fecha_val, datetime):
                # Try to parse as string if it's a string
                if isinstance(fecha_val, str):
                    try:
                        fecha_val = pd.to_datetime(fecha_val)
                    except (ValueError, TypeError):
                        break  # No more valid dates
                else:
                    break  # No more valid dates
            
            # Convert pandas Timestamp to datetime if needed
            if isinstance(fecha_val, pd.Timestamp):
                fecha_val = fecha_val.to_pydatetime()
            
            # Apply timezone naive conversion if needed
            fecha_val = to_naive(fecha_val)
            
            # Filter by last_date if provided
            if last_date and fecha_val <= to_naive(last_date):
                continue
            
            # Skip if value is None and drop_na is True
            if drop_na and (pd.isna(valor_val) or valor_val is None):
                continue
            
            # Convert value to float if possible
            if pd.isna(valor_val) or valor_val is None:
                valor_val = None
            elif isinstance(valor_val, (int, float)):
                valor_val = float(valor_val)
            elif isinstance(valor_val, str):
                try:
                    valor_val = float(valor_val.replace(",", ".").strip())
                except (ValueError, AttributeError):
                    valor_val = None
            
            # Skip if value is still None and drop_na is True
            if drop_na and valor_val is None:
                continue
            
            series_data.append({
                "internal_series_code": str(series_config["internal_series_code"]),
                "unit": series_config.get("unit"),
                "frequency": series_config.get("frequency"),
                "obs_time": fecha_val,
                "value": valor_val,
            })
        
        return series_data

