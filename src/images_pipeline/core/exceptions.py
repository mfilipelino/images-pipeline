"""Custom exceptions and error handling utilities for the images pipeline."""

from __future__ import annotations

from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, TypeVar

from .logging_config import get_logger


class ImagesPipelineError(Exception):
    """Base exception for all images pipeline errors."""


class S3Error(ImagesPipelineError):
    """Error raised for S3 related failures."""


class ConfigurationError(ImagesPipelineError):
    """Error raised for invalid configuration options."""


class ImageProcessingError(ImagesPipelineError):
    """Error raised when processing a single image fails."""


F = TypeVar("F", bound=Callable[..., Any])


def with_error_handling(func: F) -> F:
    """Wrap a function with standardized error handling."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        logger = get_logger("processor")
        try:
            return func(*args, **kwargs)
        except ImagesPipelineError:
            logger.error("Pipeline error", exc_info=True)
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Unhandled error in {func.__name__}: {exc}", exc_info=True)
            raise ImageProcessingError(str(exc)) from exc

    return wrapper  # type: ignore[return-value]


@contextmanager
def batch_error_handler() -> Any:
    """Context manager to wrap batch operations with error handling."""
    try:
        yield
    except ImagesPipelineError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ImageProcessingError(str(exc)) from exc
