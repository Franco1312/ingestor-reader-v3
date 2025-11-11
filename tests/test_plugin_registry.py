"""Tests for PluginRegistry."""

import pytest

from src.application.plugin_registry import PluginRegistry
from src.infrastructure.plugins.extractors.http_extractor import HttpExtractor
from src.infrastructure.plugins.normalizers.bcra_infomondia_normalizer import (
    BcraInfomondiaNormalizer,
)
from src.infrastructure.plugins.parsers.bcra_infomondia_parser import BcraInfomondiaParser


class TestPluginRegistry:
    """Tests for PluginRegistry class."""

    def test_register_and_get_extractor(self):
        """Test registering and getting an extractor plugin."""
        registry = PluginRegistry()
        registry.register_extractor("http", HttpExtractor)

        source_config = {"url": "https://example.com"}
        extractor = registry.get_extractor("http", source_config)
        assert isinstance(extractor, HttpExtractor)

    def test_register_and_get_parser(self):
        """Test registering and getting a parser plugin."""
        registry = PluginRegistry()
        registry.register_parser("bcra_infomondia", BcraInfomondiaParser)

        parser = registry.get_parser("bcra_infomondia")
        assert isinstance(parser, BcraInfomondiaParser)

    def test_register_and_get_normalizer(self):
        """Test registering and getting a normalizer plugin."""
        registry = PluginRegistry()
        registry.register_normalizer("bcra_infomondia", BcraInfomondiaNormalizer)

        normalizer = registry.get_normalizer("bcra_infomondia")
        assert isinstance(normalizer, BcraInfomondiaNormalizer)

    def test_get_extractor_not_found_raises_error(self):
        """Test that getting non-existent extractor raises ValueError."""
        registry = PluginRegistry()

        with pytest.raises(ValueError) as exc_info:
            registry.get_extractor("nonexistent", {"url": "https://example.com"})

        assert "Extractor plugin 'nonexistent' not found" in str(exc_info.value)

    def test_get_parser_not_found_raises_error(self):
        """Test that getting non-existent parser raises ValueError."""
        registry = PluginRegistry()

        with pytest.raises(ValueError) as exc_info:
            registry.get_parser("nonexistent")

        assert "Parser plugin 'nonexistent' not found" in str(exc_info.value)

    def test_get_normalizer_not_found_raises_error(self):
        """Test that getting non-existent normalizer raises ValueError."""
        registry = PluginRegistry()

        with pytest.raises(ValueError) as exc_info:
            registry.get_normalizer("nonexistent")

        assert "Normalizer plugin 'nonexistent' not found" in str(exc_info.value)

    def test_get_transformer_not_found_raises_error(self):
        """Test that getting non-existent transformer raises ValueError."""
        registry = PluginRegistry()

        with pytest.raises(ValueError) as exc_info:
            registry.get_transformer("nonexistent")

        assert "Transformer plugin 'nonexistent' not found" in str(exc_info.value)

    def test_get_loader_not_found_raises_error(self):
        """Test that getting non-existent loader raises ValueError."""
        registry = PluginRegistry()

        with pytest.raises(ValueError) as exc_info:
            registry.get_loader("nonexistent")

        assert "Loader plugin 'nonexistent' not found" in str(exc_info.value)

    def test_register_multiple_extractors(self):
        """Test registering multiple extractors."""
        registry = PluginRegistry()
        registry.register_extractor("http", HttpExtractor)
        registry.register_extractor("http2", HttpExtractor)

        source_config = {"url": "https://example.com"}
        extractor1 = registry.get_extractor("http", source_config)
        extractor2 = registry.get_extractor("http2", source_config)

        assert isinstance(extractor1, HttpExtractor)
        assert isinstance(extractor2, HttpExtractor)
        assert extractor1 is not extractor2  # Different instances

    def test_register_overwrites_existing(self):
        """Test that registering with same name overwrites previous."""
        registry = PluginRegistry()

        source_config = {"url": "https://example.com"}
        # Register first time
        registry.register_extractor("http", HttpExtractor)
        extractor1 = registry.get_extractor("http", source_config)

        # Register again (should overwrite)
        registry.register_extractor("http", HttpExtractor)
        extractor2 = registry.get_extractor("http", source_config)

        # Both should work, but be different instances
        assert isinstance(extractor1, HttpExtractor)
        assert isinstance(extractor2, HttpExtractor)

    def test_get_extractor_without_config_raises_error(self):
        """Test that extractor without config raises ValueError."""
        registry = PluginRegistry()
        registry.register_extractor("http", HttpExtractor)

        with pytest.raises(ValueError) as exc_info:
            registry.get_extractor("http", None)

        assert "requires configuration" in str(exc_info.value)

    def test_register_transformer(self):
        """Test registering a transformer plugin."""
        from unittest.mock import Mock
        from src.domain.interfaces import Transformer

        registry = PluginRegistry()
        # Create a mock class that can be instantiated
        mock_transformer_class = Mock()
        mock_transformer_instance = Mock(spec=Transformer)
        mock_transformer_class.return_value = mock_transformer_instance

        registry.register_transformer("test_transformer", mock_transformer_class)  # type: ignore[arg-type]

        transformer = registry.get_transformer("test_transformer")
        assert transformer is not None

    def test_register_loader(self):
        """Test registering a loader plugin."""
        from unittest.mock import Mock
        from src.domain.interfaces import Loader

        registry = PluginRegistry()
        # Create a mock class that can be instantiated
        mock_loader_class = Mock()
        mock_loader_instance = Mock(spec=Loader)
        mock_loader_class.return_value = mock_loader_instance

        registry.register_loader("test_loader", mock_loader_class)  # type: ignore[arg-type]

        config = {"dataset_id": "test", "load": {"bucket": "test-bucket"}}
        loader = registry.get_loader("test_loader", config=config)
        assert loader is not None
        mock_loader_class.assert_called_once_with(config=config)
