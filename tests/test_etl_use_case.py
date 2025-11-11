"""Tests for ETL use case."""

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from tests.builders import ConfigBuilder, ETLUseCaseBuilder, StateManagerBuilder

if TYPE_CHECKING:
    from src.application.etl_use_case import ETLUseCase


class TestETLUseCase:
    """Tests for ETLUseCase class."""

    @pytest.fixture
    def state_manager(self, tmp_path):
        """Create a StateManager with temp file."""
        state_file = tmp_path / "test_state.json"
        return StateManagerBuilder().with_file(str(state_file)).build()

    def test_execute_extract_only(self):
        """Test execution with only extractor."""
        etl = ETLUseCaseBuilder().with_extractor().build()
        result = etl.execute()

        etl.extractor.extract.assert_called_once()  # type: ignore[attr-defined]
        assert result == []

    def test_execute_extract_and_parse(self):
        """Test execution with extractor and parser."""
        config = {"test": "config"}
        etl = ETLUseCaseBuilder().with_extractor().with_parser().build()
        result = etl.execute(config)

        etl.extractor.extract.assert_called_once()  # type: ignore[attr-defined]
        assert etl.parser is not None
        etl.parser.parse.assert_called_once()  # type: ignore[attr-defined]
        assert len(result) == 1

    def test_execute_full_pipeline(self):
        """Test execution of full ETL pipeline."""
        config = {"test": "config"}
        etl = (
            ETLUseCaseBuilder()
            .with_extractor()
            .with_parser()
            .with_normalizer()
            .with_transformer()
            .with_loader()
            .build()
        )
        result = etl.execute(config)

        etl.extractor.extract.assert_called_once()  # type: ignore[attr-defined]
        assert etl.parser is not None
        etl.parser.parse.assert_called_once()  # type: ignore[attr-defined]
        assert etl.normalizer is not None
        etl.normalizer.normalize.assert_called_once()  # type: ignore[attr-defined]
        assert etl.transformer is not None
        etl.transformer.transform.assert_called_once()  # type: ignore[attr-defined]
        assert etl.loader is not None
        etl.loader.load.assert_called_once()  # type: ignore[attr-defined]
        etl.loader.load.assert_called_with([{"transformed": True}], config)  # type: ignore[attr-defined]
        assert result == [{"transformed": True}]

    def test_execute_with_state_manager(self, state_manager):
        """Test execution with state manager for incremental updates."""
        config = ConfigBuilder().with_series("TEST_SERIES").with_normalize_config("UTC", []).build()

        etl = (
            ETLUseCaseBuilder()
            .with_extractor()
            .with_parser()
            .with_normalizer()
            .with_state_manager(state_manager)
            .build()
        )

        # First run
        result1 = etl.execute(config)
        assert len(result1) == 1

        # Verify series_last_dates was passed to parser
        assert etl.parser is not None
        call_args = etl.parser.parse.call_args  # type: ignore[attr-defined]
        assert call_args[0][2] == {}  # series_last_dates should be empty dict on first run

        # Second run - should have series_last_dates with dates
        etl.execute(config)
        call_args = etl.parser.parse.call_args  # type: ignore[attr-defined]
        assert call_args[0][2] != {}  # series_last_dates should have dates

    def test_execute_with_state_manager_saves_dates(self, state_manager):
        """Test that state manager saves dates after normalization."""
        config = ConfigBuilder().with_series("TEST_SERIES").with_normalize_config("UTC", []).build()

        etl = (
            ETLUseCaseBuilder()
            .with_extractor()
            .with_parser()
            .with_normalizer()
            .with_state_manager(state_manager)
            .build()
        )

        etl.execute(config)

        # Verify date was saved
        saved_date = state_manager.get_last_date("TEST_SERIES")
        assert saved_date == datetime(2025, 1, 15)

    def test_execute_without_config(self):
        """Test execution without config."""
        etl = ETLUseCaseBuilder().with_extractor().with_parser().build()
        result = etl.execute()

        assert etl.parser is not None
        etl.parser.parse.assert_called_once()  # type: ignore[attr-defined]
        assert len(result) == 1

    def test_execute_parser_returns_empty(self):
        """Test execution when parser returns empty data."""
        parser = ETLUseCaseBuilder.create_mock_parser()
        parser.parse.return_value = []

        normalizer = ETLUseCaseBuilder.create_mock_normalizer()
        normalizer.normalize.return_value = []

        config = ConfigBuilder().with_normalize_config("UTC").build()

        etl = (
            ETLUseCaseBuilder()
            .with_extractor()
            .with_parser(parser)
            .with_normalizer(normalizer)
            .build()
        )
        result = etl.execute(config)

        assert result == []

    def test_execute_without_lock_manager(self):
        """Test execution without lock manager (should work normally)."""
        etl = ETLUseCaseBuilder().with_extractor().with_parser().build()
        result = etl.execute()

        etl.extractor.extract.assert_called_once()  # type: ignore[attr-defined]
        assert etl.parser is not None
        etl.parser.parse.assert_called_once()  # type: ignore[attr-defined]
        assert len(result) == 1

    def test_execute_with_lock_manager_acquires_and_releases(self):
        """Test that lock is acquired and released when lock_manager is configured."""
        lock_manager = ETLUseCaseBuilder.create_mock_lock_manager()
        etl = (
            ETLUseCaseBuilder()
            .with_extractor()
            .with_parser()
            .with_lock_manager(lock_manager)
            .build()
        )

        config = {"dataset_id": "test_dataset"}
        result = etl.execute(config)

        # Verify lock was acquired
        lock_manager.acquire.assert_called_once_with("etl:test_dataset", 300)

        # Verify lock was released
        lock_manager.release.assert_called_once_with("etl:test_dataset")

        # Verify ETL executed normally
        assert len(result) == 1

    def test_execute_with_lock_manager_custom_key(self):
        """Test lock with custom key from config."""
        lock_manager = ETLUseCaseBuilder.create_mock_lock_manager()
        etl = (
            ETLUseCaseBuilder()
            .with_extractor()
            .with_parser()
            .with_lock_manager(lock_manager)
            .build()
        )

        config = {"dataset_id": "test_dataset", "lock": {"key": "custom:lock:key"}}
        etl.execute(config)

        lock_manager.acquire.assert_called_once_with("custom:lock:key", 300)
        lock_manager.release.assert_called_once_with("custom:lock:key")

    def test_execute_with_lock_manager_custom_timeout(self):
        """Test lock with custom timeout from config."""
        lock_manager = ETLUseCaseBuilder.create_mock_lock_manager()
        etl = (
            ETLUseCaseBuilder()
            .with_extractor()
            .with_parser()
            .with_lock_manager(lock_manager)
            .build()
        )

        config = {"dataset_id": "test_dataset", "lock": {"timeout_seconds": 600}}
        etl.execute(config)

        lock_manager.acquire.assert_called_once_with("etl:test_dataset", 600)

    def test_execute_with_lock_manager_default_key_when_no_dataset_id(self):
        """Test lock uses default key when dataset_id is not in config."""
        lock_manager = ETLUseCaseBuilder.create_mock_lock_manager()
        etl = (
            ETLUseCaseBuilder()
            .with_extractor()
            .with_parser()
            .with_lock_manager(lock_manager)
            .build()
        )

        config = {}
        etl.execute(config)

        lock_manager.acquire.assert_called_once_with("etl:default", 300)

    def test_execute_with_lock_manager_fails_to_acquire(self):
        """Test that RuntimeError is raised when lock cannot be acquired."""
        lock_manager = ETLUseCaseBuilder.create_mock_lock_manager()
        lock_manager.acquire.return_value = False

        etl = (
            ETLUseCaseBuilder()
            .with_extractor()
            .with_parser()
            .with_lock_manager(lock_manager)
            .build()
        )

        config = {"dataset_id": "test_dataset"}

        with pytest.raises(RuntimeError) as exc_info:
            etl.execute(config)

        assert "Could not acquire lock" in str(exc_info.value)
        assert "etl:test_dataset" in str(exc_info.value)

        # Verify extractor was not called (lock failed before ETL)
        etl.extractor.extract.assert_not_called()  # type: ignore[attr-defined]

        # Verify release was not called (lock was never acquired)
        lock_manager.release.assert_not_called()

    def test_execute_with_lock_manager_releases_on_exception(self):
        """Test that lock is released even when ETL raises an exception."""
        lock_manager = ETLUseCaseBuilder.create_mock_lock_manager()
        extractor = ETLUseCaseBuilder.create_mock_extractor()
        extractor.extract.side_effect = ValueError("Test error")

        etl = (
            ETLUseCaseBuilder()
            .with_extractor(extractor)
            .with_parser()
            .with_lock_manager(lock_manager)
            .build()
        )

        config = {"dataset_id": "test_dataset"}

        with pytest.raises(ValueError):
            etl.execute(config)

        # Verify lock was acquired
        lock_manager.acquire.assert_called_once()

        # Verify lock was released even after exception
        lock_manager.release.assert_called_once_with("etl:test_dataset")

    def test_execute_with_lock_manager_and_state_manager(self):
        """Test execution with both lock_manager and state_manager."""
        lock_manager = ETLUseCaseBuilder.create_mock_lock_manager()
        state_manager = StateManagerBuilder().with_file("test_state.json").build()

        config = ConfigBuilder().with_series("TEST_SERIES").with_normalize_config("UTC", []).build()

        etl = (
            ETLUseCaseBuilder()
            .with_extractor()
            .with_parser()
            .with_normalizer()
            .with_state_manager(state_manager)
            .with_lock_manager(lock_manager)
            .build()
        )

        result = etl.execute(config)

        # Verify lock was acquired and released
        lock_manager.acquire.assert_called_once()
        lock_manager.release.assert_called_once()

        # Verify ETL executed normally
        assert len(result) == 1

    def test_properties_access(self):
        """Test that properties return correct instances."""
        lock_manager = ETLUseCaseBuilder.create_mock_lock_manager()
        state_manager = StateManagerBuilder().with_file("test_state.json").build()

        etl = (
            ETLUseCaseBuilder()
            .with_extractor()
            .with_state_manager(state_manager)
            .with_lock_manager(lock_manager)
            .build()
        )

        # Verify properties return correct instances
        assert etl.extractor is not None
        assert etl.state_manager is state_manager
        assert etl.lock_manager is lock_manager
