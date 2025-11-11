"""Loader plugins."""

from src.application.plugin_registry import PluginRegistry
from src.infrastructure.plugins.loaders.s3_versioned_loader import S3VersionedLoader


def register_loaders(registry: PluginRegistry) -> None:
    """Register all loader plugins.

    Args:
        registry: PluginRegistry instance to register plugins in.
    """
    registry.register_loader("s3_versioned", S3VersionedLoader)
