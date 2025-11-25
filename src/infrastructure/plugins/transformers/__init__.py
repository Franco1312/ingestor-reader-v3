"""Transformer plugins."""

from src.application.plugin_registry import PluginRegistry
from src.infrastructure.plugins.transformers.bcra_infomondia_transformer import (
    BcraInfomondiaTransformer,
)
from src.infrastructure.plugins.transformers.indec_ipc_transformer import (
    IndecIpcTransformer,
)


def register_transformers(registry: PluginRegistry) -> None:
    """Register all transformer plugins.

    Args:
        registry: PluginRegistry instance to register transformers in.
    """
    registry.register_transformer("bcra_infomondia", BcraInfomondiaTransformer)
    registry.register_transformer("indec_ipc", IndecIpcTransformer)
