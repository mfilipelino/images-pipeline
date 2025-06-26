"""Centralized logging configuration for the images pipeline."""

import os
import sys
import logging
from typing import Optional


def setup_logger(
    name: str = "images-pipeline",
    level: Optional[str] = None,
    format_type: str = "structured",
) -> logging.Logger:
    """
    Setup centralized logging with environment variable configuration.

    Args:
        name: Logger name (defaults to "images-pipeline")
        level: Log level override (defaults to env var or INFO)
        format_type: Logging format ("structured" or "simple")

    Returns:
        Configured logger instance

    Environment Variables:
        LOG_LEVEL: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        LOG_FORMAT: Set format type ("structured" or "simple")
    """
    logger = logging.getLogger(name)

    # Determine log level from parameter, env var, or default
    if level:
        log_level = getattr(logging, level.upper(), logging.INFO)
    else:
        env_level = os.getenv("LOG_LEVEL", "INFO").upper()
        log_level = getattr(logging, env_level, logging.INFO)

    logger.setLevel(log_level)

    # Avoid duplicate handlers
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)

        # Determine format from parameter or env var
        env_format = os.getenv("LOG_FORMAT", format_type).lower()

        if env_format == "structured":
            # Structured logging format with more context
            formatter = logging.Formatter(
                "%(asctime)s | %(name)s | %(levelname)-8s | "
                "%(filename)s:%(lineno)d | %(funcName)s() | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        else:
            # Simple format for basic use cases
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Prevent duplicate log messages
    logger.propagate = False
    return logger


def get_logger(name: str = "images-pipeline") -> logging.Logger:
    """
    Get a logger instance with consistent configuration.

    Args:
        name: Logger name

    Returns:
        Configured logger instance
    """
    return setup_logger(name)


def configure_multiprocessing_logging() -> None:
    """
    Configure logging for multiprocessing to avoid log interleaving.
    Call this in multiprocessing worker functions.
    """
    # In multiprocessing contexts, we want to ensure each process
    # has its own logger configuration
    import multiprocessing

    process_name = multiprocessing.current_process().name
    logger_name = f"images-pipeline.{process_name}"

    # Setup logger for this process
    setup_logger(logger_name)


# Create default logger instance
logger = setup_logger()
