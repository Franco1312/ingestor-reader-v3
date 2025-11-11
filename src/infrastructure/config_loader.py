"""Concrete implementation of config loader."""

from pathlib import Path
from typing import Any, Dict

import yaml

from ..domain.interfaces import ConfigLoader


class YamlConfigLoader(ConfigLoader):
    """Loads configurations from YAML files."""

    def __init__(self, config_dir: str = "config/datasets") -> None:
        """Initialize YAML config loader.

        Args:
            config_dir: Directory path containing YAML configuration files.
        """
        self._config_dir = Path(config_dir)

    def load_dataset_config(self, dataset_id: str) -> Dict[str, Any]:
        """Load configuration for a dataset from YAML.

        Args:
            dataset_id: Identifier of the dataset configuration to load.

        Returns:
            Dictionary containing the dataset configuration.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
        """
        config_file = self._config_dir / f"{dataset_id}.yml"
        if not config_file.exists():
            config_file = self._config_dir / f"{dataset_id}.yaml"

        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found for dataset: {dataset_id}")

        with open(config_file, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
