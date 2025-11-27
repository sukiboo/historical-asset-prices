import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any, Callable, cast

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError
from tenacity import (
    after_log,
    before_sleep_log,
    retry,
    retry_if_exception,
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
        retry=retry_if_exception(
            lambda exc: isinstance(exc, MaxRetryError) and "too many 429" in str(exc).lower()
        ),
        before_sleep=before_sleep_log(effective_logger, logging.DEBUG),
        after=after_log(effective_logger, logging.DEBUG),
    )
    def wrapper(*args: Any, **kwargs: Any):
        result = func(*args, **kwargs)
        if hasattr(result, "__iter__") and not isinstance(result, (str, bytes, pd.DataFrame)):
            return list(result)
        return result

    return wrapper


def save_daily_prices(df: pd.DataFrame, file_path: str) -> None:
    dir_path = os.path.dirname(file_path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    # assume index is the timestamp
    df.to_parquet(file_path, index=True)


def aggs_to_df(
    func: Callable[..., Any],
    logger: logging.Logger,
) -> Callable[..., pd.DataFrame]:
    """Wrapper that converts list_aggs result to DataFrame with logging.

    Args:
        func: The function to wrap (e.g., client.list_aggs)
        logger: Logger instance to use for logging

    Returns:
        Wrapped function that returns DataFrame or None
    """

    def wrapper(*args: Any, **kwargs: Any) -> pd.DataFrame:
        ticker = kwargs.get("ticker", args[0] if args else "unknown")
        from_date = kwargs.get("from_", "")
        to_date = kwargs.get("to", "")

        logger.debug(f"Retrieving {ticker} records from {from_date} to {to_date}...")
        aggs = func(*args, **kwargs)

        if not aggs:
            logger.debug(f"No records returned for {ticker} from {from_date} to {to_date}")
            return pd.DataFrame()

        df = pd.DataFrame([agg.__dict__ for agg in aggs])

        if df.empty:
            logger.debug(f"DataFrame is empty for {ticker} from {from_date} to {to_date}")
            return pd.DataFrame()

        logger.debug(f"Retrieved {len(df)} records for {ticker} from {from_date} to {to_date}")
        return df

    return wrapper


def get_file_from_s3(
    object_key: str,
    bucket_name: str,
    logger: logging.Logger,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
) -> pd.DataFrame:
    """
    Get a minute aggregates flat file from S3:
    - Stocks: https://massive.com/docs/flat-files/stocks/minute-aggregates
    - Options: https://massive.com/docs/flat-files/options/minute-aggregates

    Returns empty DataFrame if file doesn't exist (weekends/holidays).
    """
    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("AWS credentials are missing!")

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    s3 = session.client(
        "s3",
        endpoint_url="https://files.massive.com",
        config=Config(signature_version="s3v4"),
    )

    logger.debug(f"Downloading '{object_key}' from '{bucket_name}'...")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        result = pd.read_csv(
            response["Body"], compression="gzip" if object_key.endswith(".gz") else None
        )

        if not isinstance(result, pd.DataFrame):
            raise ValueError(f"Expected DataFrame but got {type(result)}")
        elif result.empty:
            logger.debug(f"Data from '{object_key}' is empty")
            return pd.DataFrame()
        else:
            logger.debug(f"Successfully downloaded '{object_key}' ({len(result)} rows)")
            return result

    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "NoSuchKey":
            logger.debug(f"File not found: '{object_key}'")
            return pd.DataFrame()
        raise RuntimeError(f"Failed to download '{object_key}': {e}") from e
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        logger.debug(f"Failed to parse '{object_key}': {e}")
        return pd.DataFrame()
