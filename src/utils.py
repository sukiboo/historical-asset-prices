from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Callable, cast

import pandas as pd
from polygon.exceptions import BadResponse
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
    level: int = logging.INFO,
    log_file: str = "logs.log",
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """Configure root logging once and return the application logger.

    This sets handlers on the root logger so that any module using
    `logging.getLogger(__name__)` inherits the same configuration.
    Subsequent calls are idempotent (won't add duplicate handlers).
    """
    root_logger = logging.getLogger()

    if not getattr(root_logger, "_app_logging_configured", False):
        root_logger.setLevel(level)

        formatter = logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)

        file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)

        # Clear pre-existing basicConfig handlers (if any) to avoid duplicates
        if root_logger.handlers:
            root_logger.handlers.clear()

        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

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
        retry=retry_if_exception_type((BadResponse, MaxRetryError)),
        before_sleep=before_sleep_log(effective_logger, logging.DEBUG),
        after=after_log(effective_logger, logging.DEBUG),
    )
    def wrapper(*args: Any, **kwargs: Any):
        result = func(*args, **kwargs)
        if hasattr(result, "__iter__") and not isinstance(result, (str, bytes)):
            return list(result)
        return result

    return wrapper
