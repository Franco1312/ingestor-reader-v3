"""Lambda handler for ETL pipeline execution."""

import json
import logging
import os
import sys
from typing import Any, Dict, Optional

# Add src to path to allow imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from src.cli import run_etl

# Configure logging on module import
_logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Configure logging for Lambda environment."""
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


# Initialize logging
_configure_logging()
logger = _logger


# ============================================================================
# Event Parsing Functions
# ============================================================================

def _extract_from_direct_invocation(event: Dict[str, Any]) -> Optional[str]:
    """Extract dataset_id from direct Lambda invocation.
    
    Args:
        event: Lambda event dictionary.
        
    Returns:
        Dataset identifier if found, None otherwise.
    """
    return event.get("dataset_id")


def _extract_from_eventbridge(event: Dict[str, Any]) -> Optional[str]:
    """Extract dataset_id from EventBridge event.
    
    Args:
        event: Lambda event dictionary.
        
    Returns:
        Dataset identifier if found, None otherwise.
    """
    detail = event.get("detail")
    if isinstance(detail, dict):
        return detail.get("dataset_id")
    return None


def _extract_from_sqs_record(record: Dict[str, Any]) -> Optional[str]:
    """Extract dataset_id from SQS record body.
    
    Args:
        record: SQS record dictionary.
        
    Returns:
        Dataset identifier if found, None otherwise.
    """
    body = record.get("body")
    if not body:
        return None
    
    try:
        # Try parsing body as JSON
        parsed_body = json.loads(body)
        
        # If it's a dict, check for dataset_id
        if isinstance(parsed_body, dict):
            return parsed_body.get("dataset_id")
        
        # If it's a string, try parsing again (double-encoded case)
        if isinstance(parsed_body, str):
            double_parsed = json.loads(parsed_body)
            if isinstance(double_parsed, dict):
                return double_parsed.get("dataset_id")
    
    except (json.JSONDecodeError, TypeError):
        pass
    
    return None


def _extract_from_sqs(event: Dict[str, Any]) -> Optional[str]:
    """Extract dataset_id from SQS event.
    
    Args:
        event: Lambda event dictionary.
        
    Returns:
        Dataset identifier if found, None otherwise.
    """
    records = event.get("Records")
    if not isinstance(records, list) or len(records) == 0:
        return None
    
    return _extract_from_sqs_record(records[0])


def _extract_from_environment() -> Optional[str]:
    """Extract dataset_id from environment variable.
    
    Returns:
        Dataset identifier if found, None otherwise.
    """
    return os.environ.get("DATASET_ID")


def extract_dataset_id(event: Dict[str, Any]) -> str:
    """Extract dataset_id from Lambda event.
    
    Supports multiple event sources in order of precedence:
    1. Direct invocation: {"dataset_id": "..."}
    2. EventBridge: {"detail": {"dataset_id": "..."}}
    3. SQS: {"Records": [{"body": "{\"dataset_id\": \"...\"}"}]}
    4. Environment variable: DATASET_ID
    
    Args:
        event: Lambda event dictionary.
        
    Returns:
        Dataset identifier.
        
    Raises:
        ValueError: If dataset_id cannot be extracted from event.
    """
    # Try direct invocation
    dataset_id = _extract_from_direct_invocation(event)
    if dataset_id:
        return dataset_id
    
    # Try EventBridge
    dataset_id = _extract_from_eventbridge(event)
    if dataset_id:
        return dataset_id
    
    # Try SQS
    dataset_id = _extract_from_sqs(event)
    if dataset_id:
        return dataset_id
    
    # Try environment variable
    dataset_id = _extract_from_environment()
    if dataset_id:
        return dataset_id
    
    raise ValueError(
        "dataset_id not found in event. Expected one of: "
        "event['dataset_id'], event['detail']['dataset_id'], "
        "event['Records'][0]['body']['dataset_id'], or DATASET_ID env var"
    )


# ============================================================================
# Response Builder Functions
# ============================================================================

def _create_response(
    status_code: int,
    message: str,
    dataset_id: Optional[str] = None,
    error: Optional[str] = None,
    **extra_fields: Any,
) -> Dict[str, Any]:
    """Create a standardized Lambda response.
    
    Args:
        status_code: HTTP status code.
        message: Response message.
        dataset_id: Dataset identifier (optional).
        error: Error type (optional).
        **extra_fields: Additional fields to include in response body.
        
    Returns:
        Lambda response dictionary.
    """
    body: Dict[str, Any] = {"message": message}
    
    if dataset_id:
        body["dataset_id"] = dataset_id
    
    if error:
        body["error"] = error
    
    body.update(extra_fields)
    
    return {
        "statusCode": status_code,
        "body": json.dumps(body),
    }


def _create_success_response(dataset_id: str) -> Dict[str, Any]:
    """Create a success response for successful ETL execution.
    
    Args:
        dataset_id: Dataset identifier.
        
    Returns:
        Lambda response dictionary with status 200.
    """
    return _create_response(
        status_code=200,
        message=f"ETL completed successfully for dataset: {dataset_id}",
        dataset_id=dataset_id,
    )


def _create_failure_response(dataset_id: str, exit_code: int) -> Dict[str, Any]:
    """Create a failure response for failed ETL execution.
    
    Args:
        dataset_id: Dataset identifier.
        exit_code: ETL exit code.
        
    Returns:
        Lambda response dictionary with status 500.
    """
    return _create_response(
        status_code=500,
        message=f"ETL failed for dataset: {dataset_id}",
        dataset_id=dataset_id,
        exit_code=exit_code,
    )


def _create_bad_request_response(error_message: str) -> Dict[str, Any]:
    """Create a bad request response for invalid events.
    
    Args:
        error_message: Error message.
        
    Returns:
        Lambda response dictionary with status 400.
    """
    return _create_response(
        status_code=400,
        message=f"Invalid event: {error_message}",
        error="BadRequest",
    )


def _create_internal_error_response(error_message: str) -> Dict[str, Any]:
    """Create an internal server error response.
    
    Args:
        error_message: Error message.
        
    Returns:
        Lambda response dictionary with status 500.
    """
    return _create_response(
        status_code=500,
        message=f"Internal server error: {error_message}",
        error="InternalServerError",
    )


# ============================================================================
# ETL Processing Functions
# ============================================================================

def _process_etl_pipeline(dataset_id: str) -> int:
    """Execute ETL pipeline for a dataset.
    
    Args:
        dataset_id: Dataset identifier.
        
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    logger.info("Processing dataset: %s", dataset_id)
    exit_code = run_etl(dataset_id)
    
    if exit_code == 0:
        logger.info("ETL completed successfully for dataset: %s", dataset_id)
    else:
        logger.error("ETL failed with exit code %d for dataset: %s", exit_code, dataset_id)
    
    return exit_code


# ============================================================================
# Lambda Handler
# ============================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler entry point.
    
    Args:
        event: Lambda event dictionary.
        context: Lambda context object (unused but required by Lambda interface).
        
    Returns:
        Response dictionary with statusCode and body.
    """
    try:
        logger.info("Received event: %s", json.dumps(event))
        
        # Extract dataset_id from event
        dataset_id = extract_dataset_id(event)
        
        # Execute ETL pipeline
        exit_code = _process_etl_pipeline(dataset_id)
        
        # Return appropriate response based on exit code
        if exit_code == 0:
            return _create_success_response(dataset_id)
        else:
            return _create_failure_response(dataset_id, exit_code)
    
    except ValueError as e:
        logger.error("Invalid event: %s", e)
        return _create_bad_request_response(str(e))
    
    except (RuntimeError, OSError, TypeError) as e:
        logger.exception("Error processing event: %s", e)
        return _create_internal_error_response(str(e))
    
    except Exception as e:  # noqa: BLE001
        # Catch-all for any other unexpected errors
        logger.exception("Unexpected error processing event: %s", e)
        return _create_internal_error_response(str(e))
