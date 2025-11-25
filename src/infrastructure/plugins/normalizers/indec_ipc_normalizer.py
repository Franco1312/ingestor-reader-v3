"""INDEC IPC normalizer plugin."""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz

from src.domain.interfaces import Normalizer


class IndecIpcNormalizer(Normalizer):
    """Normalizer for INDEC IPC parsed data."""

    def normalize(
        self, 
        data: List[Dict[str, Any]], 
        config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Normalize parsed data according to configuration.
        
        Args:
            data: List of parsed data points from parser.
            config: Normalization configuration.
            
        Returns:
            List of normalized data points.
        """
        normalize_config = config.get("normalize", {})
        timezone_str = normalize_config.get("timezone", "UTC")
        primary_keys = normalize_config.get("primary_keys", [])
        
        timezone = pytz.timezone(timezone_str)
        normalized = []
        seen = set()
        
        for data_point in data:
            internal_series_code = data_point.get("internal_series_code")
            if not internal_series_code:
                continue
            
            obs_time = self._parse_datetime(data_point.get("obs_time"), timezone)
            if obs_time is None:
                continue
            
            value = self._normalize_value(data_point.get("value"))
            if value is None:
                continue
            
            dedup_key = (obs_time, internal_series_code) if primary_keys else None
            if dedup_key and dedup_key in seen:
                continue
            if dedup_key:
                seen.add(dedup_key)
            
            normalized.append({
                "obs_time": obs_time,
                "internal_series_code": str(internal_series_code),
                "value": value,
                "unit": data_point.get("unit"),
                "frequency": data_point.get("frequency"),
            })
        
        return normalized
    
    def _parse_datetime(
        self, 
        value: Any, 
        timezone: pytz.BaseTzInfo
    ) -> Optional[datetime]:
        """Parse datetime value and apply timezone.
        
        Args:
            value: Date value (datetime, string, or None).
            timezone: Timezone to apply.
            
        Returns:
            Datetime with timezone or None if invalid.
        """
        if not isinstance(value, datetime):
            return None
        
        if value.tzinfo is None:
            return timezone.localize(value)
        return value.astimezone(timezone)
    
    def _normalize_value(self, value: Any) -> Optional[float]:
        """Normalize value to float.
        
        Args:
            value: Value to normalize (float, int, string, or None).
            
        Returns:
            Float value or None if invalid.
        """
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            cleaned = value.replace(",", ".").replace(" ", "").strip()
            if not cleaned:
                return None
            try:
                return float(cleaned)
            except ValueError:
                return None
        
        return None

