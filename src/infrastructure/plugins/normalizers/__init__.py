"""Normalizer plugins."""

from src.application.plugin_registry import PluginRegistry
from src.infrastructure.plugins.normalizers.bcra_infomondia_normalizer import (
    BcraInfomondiaNormalizer,
)
from src.infrastructure.plugins.normalizers.indec_ipc_normalizer import (
    IndecIpcNormalizer,
)


def register_normalizers(registry: PluginRegistry) -> None:
    """Register all normalizer plugins.

    Args:
        registry: PluginRegistry instance to register plugins in.
    """
    registry.register_normalizer("bcra_infomondia", BcraInfomondiaNormalizer)
    registry.register_normalizer("indec_ipc", IndecIpcNormalizer)
