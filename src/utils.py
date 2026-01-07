import hashlib
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any, Callable, cast

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError, ReadTimeoutError
from tenacity import (
    after_log,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from urllib3.exceptions import MaxRetryError

from src.constants import MAX_DELAY, MAX_RETRIES, MIN_DELAY


def setup_logging(
    *,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
    log_file: str = "logs.log",
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """Configure root logging once and return the application logger.

    Handlers are attached to the root logger so that any module using
    `logging.getLogger(__name__)` inherits the same configuration.
    The console and file levels can be tuned independently.
    Subsequent calls are idempotent (won't add duplicate handlers).
    """
    root_logger = logging.getLogger()

    if not getattr(root_logger, "_app_logging_configured", False):
        formatter = logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        root_logger.setLevel(min(console_level, file_level))

        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_handler.setFormatter(formatter)

        file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)

        # Clear pre-existing basicConfig handlers (if any) to avoid duplicates
        if root_logger.handlers:
            root_logger.handlers.clear()

        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

        # Quiet noisy third-party libraries
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("s3transfer").setLevel(logging.WARNING)

        # Mark as configured to prevent duplicate handler additions
        setattr(root_logger, "_app_logging_configured", True)

    # Return the root logger so callers may do `logger = setup_logging()` safely
    return logging.getLogger()


def to_timestamp(date_str: str) -> pd.Timestamp:
    """Convert date string to Timestamp, validated and type-narrowed."""
    try:
        ts = pd.Timestamp(date_str)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid date format or value: {date_str!r}") from e
    if ts is pd.NaT:
        raise ValueError(f"Invalid date: {date_str!r} (parsed as NaT)")
    return cast(pd.Timestamp, ts)


def with_retry(
    func: Callable[..., Any],
    logger: logging.Logger | None = None,
    max_retries: int = MAX_RETRIES,
    min_delay: int = MIN_DELAY,
    max_delay: int = MAX_DELAY,
) -> Callable[..., Any]:
    """Wrap a callable with retry logic and list-materialization for iterables.

    Parameters mirror sensible defaults used in the app. Pass a module logger
    (e.g., logging.getLogger(__name__)) so tenacity can log retries.
    """

    effective_logger = logger or logging.getLogger(getattr(func, "__module__", __name__))

    @retry(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(min=min_delay, max=max_delay),
        retry=retry_if_exception_type((ReadTimeoutError, MaxRetryError, ConnectionError)),
        before_sleep=before_sleep_log(effective_logger, logging.WARNING),
        after=after_log(effective_logger, logging.DEBUG),
    )
    def wrapper(*args: Any, **kwargs: Any):
        result = func(*args, **kwargs)
        if hasattr(result, "__iter__") and not isinstance(result, (str, bytes, pd.DataFrame, dict)):
            return list(result)
        return result

    return wrapper


def get_s3_client(
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
):
    """Create and return an S3 client."""
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials are missing!")

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    return session.client(
        "s3",
        endpoint_url="https://files.massive.com",
        config=Config(signature_version="s3v4"),
    )


def get_file_from_s3(
    object_key: str,
    bucket_name: str,
    logger: logging.Logger,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    if_none_match: str | None = None,
) -> tuple[bytes | None, str | None, bool]:
    """
    Get a minute aggregates flat file from S3 as raw bytes:
    - Stocks: https://massive.com/docs/flat-files/stocks/minute-aggregates
    - Options: https://massive.com/docs/flat-files/options/minute-aggregates

    Args:
        if_none_match: ETag to use for conditional GET. If provided and file unchanged,
                      returns None bytes.

    Returns:
        Tuple of (bytes or None, ETag, file_exists).
        - If file unchanged (304): (None, None, True)
        - If file doesn't exist (404): (None, None, False)
        - If file downloaded: (bytes, ETag, True)
    """
    s3 = get_s3_client(aws_access_key_id, aws_secret_access_key)

    logger.debug(f"Downloading '{object_key}' from '{bucket_name}'...")
    try:
        kwargs = {"Bucket": bucket_name, "Key": object_key}
        if if_none_match:
            kwargs["IfNoneMatch"] = if_none_match

        response = s3.get_object(**kwargs)

        # Get ETag from response
        etag = response.get("ETag", "").strip('"')

        # Read raw bytes
        file_bytes = response["Body"].read()

        logger.debug(f"Successfully downloaded '{object_key}' ({len(file_bytes)} bytes)")
        return file_bytes, etag, True

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        http_status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        # Handle 304 Not Modified (file unchanged)
        if http_status == 304 or error_code == "NotModified":
            logger.debug(f"File unchanged: '{object_key}'")
            return None, None, True

        if error_code == "NoSuchKey" or http_status == 404:
            logger.debug(f"File not found: '{object_key}'")
            return None, None, False
        if error_code == "403" or http_status == 403:
            raise RuntimeError(
                f"Access denied for '{object_key}': your plan may not include this date range"
            ) from e
        raise RuntimeError(f"Failed to download '{object_key}': {e}") from e


def get_flat_file_path(files_dir: str, current_day: pd.Timestamp) -> str:
    """Get the path for the cached flat file."""
    date_str = current_day.strftime("%Y-%m-%d")
    return f"{files_dir}/{date_str}.csv.gz"


def save_flat_file_bytes(file_bytes: bytes, file_path: str, logger: logging.Logger) -> None:
    """Save the raw file bytes to local cache."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(file_bytes)
    logger.debug(f"Saved flat file: {file_path}")


def create_empty_marker(file_path: str, logger: logging.Logger) -> None:
    """Create an empty marker file."""
    marker_path = f"{file_path}.empty"
    os.makedirs(os.path.dirname(marker_path), exist_ok=True)
    open(marker_path, "a").close()
    logger.debug(f"Created empty marker: {marker_path}")


def compute_file_md5(file_path: str) -> str | None:
    """Compute MD5 hash of a local file. Returns None if file doesn't exist."""
    if not os.path.exists(file_path):
        return None

    hash_md5 = hashlib.md5(usedforsecurity=False)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
