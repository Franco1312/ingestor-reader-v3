"""Command-line interface for running ETL pipelines."""

import argparse
import logging
import os
import sys

import requests
import yaml

from typing import Any, Dict, Optional

from src.application.etl_use_case import ETLUseCase
from src.application.plugin_registry import PluginRegistry
from src.application.projection_use_case import ProjectionUseCase
from src.domain.interfaces import (
    Extractor,
    Loader,
    LockManager,
    Normalizer,
    Parser,
    StateManager,
    Transformer,
)
from src.infrastructure.config_loader import YamlConfigLoader
from src.infrastructure.lock_managers.lock_manager_factory import LockManagerFactory
from src.infrastructure.notifications.projection_notification_service import (
    ProjectionNotificationService,
)
from src.infrastructure.plugins import create_plugin_registry
from src.infrastructure.projections.projection_manager import ProjectionManager
from src.infrastructure.state_managers.state_manager_factory import StateManagerFactory


def _get_extractor(registry: PluginRegistry, config: Dict[str, Any]) -> Extractor:
    """Get extractor from registry based on config.

    Args:
        registry: Plugin registry instance.
        config: Configuration dictionary.

    Returns:
        Extractor instance.
    """
    source_config = config.get("source", {})
    extractor_kind = source_config.get("kind", "http")
    return registry.get_extractor(extractor_kind, source_config)


def _get_parser(registry: PluginRegistry, config: Dict[str, Any]) -> Optional[Parser]:
    """Get parser from registry based on config.

    Args:
        registry: Plugin registry instance.
        config: Configuration dictionary.

    Returns:
        Parser instance or None if not configured.
    """
    parse_config = config.get("parse", {})
    if parse_config.get("plugin"):
        return registry.get_parser(parse_config["plugin"])
    return None


def _get_normalizer(registry: PluginRegistry, config: Dict[str, Any]) -> Optional[Normalizer]:
    """Get normalizer from registry based on config.

    Args:
        registry: Plugin registry instance.
        config: Configuration dictionary.

    Returns:
        Normalizer instance or None if not configured.
    """
    normalize_config = config.get("normalize", {})
    if normalize_config.get("plugin"):
        return registry.get_normalizer(normalize_config["plugin"])
    return None


def _get_transformer(registry: PluginRegistry, config: Dict[str, Any]) -> Optional[Transformer]:
    """Get transformer from registry based on config.

    Args:
        registry: Plugin registry instance.
        config: Configuration dictionary.

    Returns:
        Transformer instance or None if not configured.
    """
    transform_config = config.get("transform", {})
    if transform_config.get("plugin"):
        return registry.get_transformer(transform_config["plugin"])
    return None


def _get_loader(registry: PluginRegistry, config: Dict[str, Any]) -> Optional[Loader]:
    """Get loader from registry based on config.

    Args:
        registry: Plugin registry instance.
        config: Configuration dictionary.

    Returns:
        Loader instance or None if not configured.
    """
    load_config = config.get("load", {})
    if load_config.get("plugin"):
        return registry.get_loader(load_config["plugin"], config=config)
    return None


def _get_config(dataset_id: str) -> Dict[str, Any]:
    """Load configuration for a dataset.

    Args:
        dataset_id: Dataset identifier.

    Returns:
        Configuration dictionary.
    """
    config_loader = YamlConfigLoader()
    return config_loader.load_dataset_config(dataset_id)


def _get_state_manager(config: Dict[str, Any]) -> Optional[StateManager]:
    """Get state manager from factory based on config.

    Args:
        config: Configuration dictionary.

    Returns:
        StateManager instance or None if not configured.
    """
    state_config = config.get("state")
    return StateManagerFactory.create(state_config)


def _create_notification_service(aws_region: str) -> Optional[ProjectionNotificationService]:
    """Create notification service if SNS topic ARN is configured.

    Args:
        aws_region: AWS region.

    Returns:
        ProjectionNotificationService instance or None if not configured.
    """
    topic_arn = os.environ.get("SNS_PROJECTION_TOPIC_ARN")
    if not topic_arn:
        return None

    return ProjectionNotificationService(
        topic_arn=topic_arn,
        sns_client=None,
        aws_region=aws_region,
    )


def _get_lock_manager(config: Dict[str, Any]) -> Optional[LockManager]:
    """Get lock manager from factory based on config.

    Args:
        config: Configuration dictionary.

    Returns:
        LockManager instance or None if not configured.
    """
    lock_config = config.get("lock")
    return LockManagerFactory.create(lock_config)


def _get_projection_use_case(
    config: Dict[str, Any], loader: Optional[Loader]
) -> Optional[ProjectionUseCase]:
    """Get projection use case if loader is configured.

    Args:
        config: Configuration dictionary.
        loader: Loader instance (optional).

    Returns:
        ProjectionUseCase instance or None if loader is not configured.
    """
    if not loader:
        return None

    load_config = config.get("load", {})
    bucket = load_config.get("bucket")
    aws_region = load_config.get("aws_region", "us-east-1")

    if not bucket:
        return None

    projection_config = load_config.get("projection", {})
    copy_workers = projection_config.get("copy_workers", 1)
    merge_workers = projection_config.get("merge_workers", 1)

    try:
        s3_client = getattr(loader, "_s3_client", None)
        projection_manager = ProjectionManager(
            bucket=bucket,
            s3_client=s3_client,
            aws_region=aws_region,
            copy_workers=copy_workers,
            merge_workers=merge_workers,
        )

        notification_service = _create_notification_service(aws_region)

        return ProjectionUseCase(
            projection_manager=projection_manager,
            notification_service=notification_service,
            bucket=bucket,
        )
    except AttributeError:
        return None


def _execute_etl_pipeline(dataset_id: str) -> int:
    """Execute ETL pipeline for a dataset without error handling.

    Args:
        dataset_id: Dataset identifier.

    Returns:
        Exit code (0 for success).

    Raises:
        FileNotFoundError: If configuration file not found.
        yaml.YAMLError: If YAML parsing fails.
        ValueError: If configuration is invalid.
        RuntimeError: If ETL execution fails.
        requests.RequestException: If HTTP extraction fails.
        OSError: If I/O operations fail.
        TypeError: If type errors occur.
    """
    # Initialize plugin registry with all available plugins
    registry = create_plugin_registry()

    # Load configuration
    config = _get_config(dataset_id)

    # Create components from registry
    extractor = _get_extractor(registry, config)
    parser = _get_parser(registry, config)
    normalizer = _get_normalizer(registry, config)
    transformer = _get_transformer(registry, config)
    loader = _get_loader(registry, config)

    # Create managers from factories
    state_manager = _get_state_manager(config)
    lock_manager = _get_lock_manager(config)

    # Create projection use case if loader is configured
    projection_use_case = _get_projection_use_case(config, loader)

    # Create ETL use case
    etl = ETLUseCase(
        extractor=extractor,
        parser=parser,
        normalizer=normalizer,
        transformer=transformer,
        loader=loader,
        state_manager=state_manager,
        lock_manager=lock_manager,
        projection_use_case=projection_use_case,
    )

    data = etl.execute(config)

    print(f"✓ ETL completed successfully. Processed {len(data)} data points.")
    return 0


def _handle_error(error: BaseException) -> int:
    """Handle errors and return appropriate exit code.

    Args:
        error: Exception or BaseException that was raised.

    Returns:
        Exit code (1 for errors, 130 for KeyboardInterrupt).
    """
    if isinstance(error, FileNotFoundError):
        print(f"✗ Configuration file not found: {error}", file=sys.stderr)
        return 1
    if isinstance(error, yaml.YAMLError):
        print(f"✗ Invalid YAML configuration: {error}", file=sys.stderr)
        return 1
    if isinstance(error, ValueError):
        print(f"✗ Configuration error: {error}", file=sys.stderr)
        return 1
    if isinstance(error, RuntimeError):
        print(f"✗ Runtime error: {error}", file=sys.stderr)
        return 1
    if isinstance(error, requests.RequestException):
        print(f"✗ Network error during extraction: {error}", file=sys.stderr)
        return 1
    if isinstance(error, OSError):
        print(f"✗ I/O error: {error}", file=sys.stderr)
        return 1
    if isinstance(error, TypeError):
        print(f"✗ Type error: {error}", file=sys.stderr)
        return 1
    if isinstance(error, KeyboardInterrupt):
        print("\n✗ Operation cancelled by user", file=sys.stderr)
        return 130  # Standard exit code for SIGINT

    # Fallback for unexpected errors
    print(f"✗ Unexpected error: {error}", file=sys.stderr)
    return 1


def run_etl(dataset_id: str) -> int:
    """Run ETL pipeline for a dataset with error handling.

    Args:
        dataset_id: Dataset identifier.

    Returns:
        Exit code (0 for success, 1 for error, 130 for KeyboardInterrupt).
    """
    try:
        return _execute_etl_pipeline(dataset_id)
    except KeyboardInterrupt as error:
        return _handle_error(error)
    except (
        FileNotFoundError,
        yaml.YAMLError,
        ValueError,
        RuntimeError,
        requests.RequestException,
        OSError,
        TypeError,
    ) as error:
        return _handle_error(error)


def main():
    """Main entry point for CLI."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Suppress noisy third-party logs
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("s3transfer").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(
        description="Run ETL pipeline for a dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "dataset_id",
        type=str,
        help="Dataset identifier (e.g., 'bcra_infomondia_series')",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging (DEBUG level)",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info("Verbose logging enabled")

    exit_code = run_etl(args.dataset_id)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
