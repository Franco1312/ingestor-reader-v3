"""Plugin registry for managing plugins."""

from typing import Any, Dict, Optional, Type

from ..domain.interfaces import Extractor, Loader, Normalizer, Parser, Transformer


class PluginRegistry:
    """Registry for managing and retrieving plugins."""

    def __init__(self):
        """Initialize plugin registry."""
        self._extractors: Dict[str, Type[Extractor]] = {}
        self._parsers: Dict[str, Type[Parser]] = {}
        self._normalizers: Dict[str, Type[Normalizer]] = {}
        self._transformers: Dict[str, Type[Transformer]] = {}
        self._loaders: Dict[str, Type[Loader]] = {}

    def register_extractor(self, name: str, plugin_class: Type[Extractor]) -> None:
        """Register an extractor plugin.

        Args:
            name: Plugin name identifier.
            plugin_class: Extractor class to register.
        """
        self._extractors[name] = plugin_class

    def register_parser(self, name: str, plugin_class: Type[Parser]) -> None:
        """Register a parser plugin.

        Args:
            name: Plugin name identifier.
            plugin_class: Parser class to register.
        """
        self._parsers[name] = plugin_class

    def register_normalizer(self, name: str, plugin_class: Type[Normalizer]) -> None:
        """Register a normalizer plugin.

        Args:
            name: Plugin name identifier.
            plugin_class: Normalizer class to register.
        """
        self._normalizers[name] = plugin_class

    def register_transformer(self, name: str, plugin_class: Type[Transformer]) -> None:
        """Register a transformer plugin.

        Args:
            name: Plugin name identifier.
            plugin_class: Transformer class to register.
        """
        self._transformers[name] = plugin_class

    def register_loader(self, name: str, plugin_class: Type[Loader]) -> None:
        """Register a loader plugin.

        Args:
            name: Plugin name identifier.
            plugin_class: Loader class to register.
        """
        self._loaders[name] = plugin_class

    def get_extractor(self, name: str, config: Optional[Dict[str, Any]] = None) -> Extractor:
        """Get an extractor plugin by name.

        Args:
            name: Plugin name identifier.
            config: Optional configuration for plugin initialization.

        Returns:
            Extractor instance.

        Raises:
            ValueError: If plugin not found or config is required but not provided.
        """
        if name not in self._extractors:
            raise ValueError(f"Extractor plugin '{name}' not found")
        plugin_class = self._extractors[name]
        if config is not None:
            return plugin_class(config)  # type: ignore[call-arg]
        # Try to instantiate without config, but this may fail for extractors that require config
        try:
            return plugin_class()  # type: ignore[call-arg]
        except TypeError:
            raise ValueError(
                f"Extractor plugin '{name}' requires configuration but none was provided"
            ) from None

    def get_parser(self, name: str) -> Parser:
        """Get a parser plugin by name.

        Args:
            name: Plugin name identifier.

        Returns:
            Parser instance.

        Raises:
            ValueError: If plugin not found.
        """
        if name not in self._parsers:
            raise ValueError(f"Parser plugin '{name}' not found")
        return self._parsers[name]()

    def get_normalizer(self, name: str) -> Normalizer:
        """Get a normalizer plugin by name.

        Args:
            name: Plugin name identifier.

        Returns:
            Normalizer instance.

        Raises:
            ValueError: If plugin not found.
        """
        if name not in self._normalizers:
            raise ValueError(f"Normalizer plugin '{name}' not found")
        return self._normalizers[name]()

    def get_transformer(self, name: str) -> Transformer:
        """Get a transformer plugin by name.

        Args:
            name: Plugin name identifier.

        Returns:
            Transformer instance.

        Raises:
            ValueError: If plugin not found.
        """
        if name not in self._transformers:
            raise ValueError(f"Transformer plugin '{name}' not found")
        return self._transformers[name]()

    def get_loader(self, name: str, config: Optional[Dict[str, Any]] = None) -> Loader:
        """Get a loader plugin by name.

        Args:
            name: Plugin name identifier.
            config: Configuration dictionary for plugin initialization.

        Returns:
            Loader instance.

        Raises:
            ValueError: If plugin not found.
        """
        if name not in self._loaders:
            raise ValueError(f"Loader plugin '{name}' not found")
        plugin_class = self._loaders[name]
        return plugin_class(config=config)  # type: ignore[call-arg]
