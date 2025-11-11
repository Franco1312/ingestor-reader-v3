"""Plugin implementations and initialization."""

from src.application.plugin_registry import PluginRegistry
from src.infrastructure.plugins.extractors import register_extractors
from src.infrastructure.plugins.loaders import register_loaders
from src.infrastructure.plugins.normalizers import register_normalizers
from src.infrastructure.plugins.parsers import register_parsers
from src.infrastructure.plugins.transformers import register_transformers


def initialize_plugins(registry: PluginRegistry) -> None:
    """Initialize and register all available plugins in the registry.

    This function delegates registration to each plugin category module.
    New plugins should be registered in their respective category __init__.py files.

    Args:
        registry: PluginRegistry instance to register plugins in.
    """
    register_extractors(registry)
    register_parsers(registry)
    register_normalizers(registry)
    register_transformers(registry)
    register_loaders(registry)


def create_plugin_registry() -> PluginRegistry:
    """Create and initialize a PluginRegistry with all available plugins.

    Returns:
        PluginRegistry instance with all plugins registered.
    """
    registry = PluginRegistry()
    initialize_plugins(registry)
    return registry
