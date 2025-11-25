"""INDEC IPC-specific HTTP extractor with dynamic file naming."""

from datetime import datetime
from typing import Any, Dict

import pytz
import requests

from src.domain.interfaces import Extractor


class IndecIpcHttpExtractor(Extractor):
    """Extractor for INDEC IPC dataset that builds URL based on current date."""

    DEFAULT_TIMEOUT = 30
    DEFAULT_TIMEZONE = "America/Argentina/Buenos_Aires"

    def __init__(self, source_config: Dict[str, Any]) -> None:
        """Initialize extractor.

        Args:
            source_config: Configuration containing:
                - url_template: Template with placeholders {MM} and {YY}
                - timezone (optional): Timezone for date calculation
                - timeout (optional): HTTP timeout
                - verify_ssl (optional): SSL verification flag
        """
        url_template = source_config.get("url_template")
        if not url_template:
            raise ValueError("source_config must contain 'url_template' key")

        timezone_str = source_config.get("timezone", self.DEFAULT_TIMEZONE)
        timezone = pytz.timezone(timezone_str)
        now = datetime.now(timezone)

        month = f"{now.month:02d}"
        year_short = f"{now.year % 100:02d}"

        self._url = url_template.format(MM=month, YY=year_short)
        self._timeout = source_config.get("timeout", self.DEFAULT_TIMEOUT)
        self._verify_ssl = source_config.get("verify_ssl", False)

    def extract(self) -> bytes:
        """Download the file."""
        response = requests.get(
            self._url,
            timeout=self._timeout,
            verify=self._verify_ssl,
        )
        response.raise_for_status()
        return response.content

