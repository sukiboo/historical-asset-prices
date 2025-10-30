from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler


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
