"""Extractor plugins."""

from src.application.plugin_registry import PluginRegistry
from src.infrastructure.plugins.extractors.http_extractor import HttpExtractor
from src.infrastructure.plugins.extractors.indec_ipc_http_extractor import (
    IndecIpcHttpExtractor,
)


def register_extractors(registry: PluginRegistry) -> None:
    """Register all extractor plugins.

    Args:
        registry: PluginRegistry instance to register plugins in.
    """
    registry.register_extractor("http", HttpExtractor)
    registry.register_extractor("indec_ipc_http", IndecIpcHttpExtractor)
