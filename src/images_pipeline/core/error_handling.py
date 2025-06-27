# src/images_pipeline/core/error_handling.py

import functools
import logging
import time
from .exceptions import ImageProcessingError, S3Error, ConfigurationError

# Attempt to import botocore.exceptions.ClientError for specific S3 error handling
try:
    from botocore.exceptions import ClientError as BotocoreClientError
    RETRYABLE_S3_ERROR_CODES = ('ProvisionedThroughputExceededException', 'ThrottlingException', 'SlowDown')
except ImportError:
    BotocoreClientError = None
    RETRYABLE_S3_ERROR_CODES = ()

# Attempt to import PIL.UnidentifiedImageError for specific image error handling
try:
    from PIL import UnidentifiedImageError as PILUnidentifiedImageError
except ImportError:
    PILUnidentifiedImageError = None


def with_error_handling(func):
    """
    A decorator to wrap functions with standardized error handling.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__ + '.' + func.__name__)
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"Error in '{func.__name__}': {e}",
                exc_info=True
            )
            if BotocoreClientError and isinstance(e, BotocoreClientError):
                raise S3Error(f"S3 operation failed in {func.__name__}: {e}") from e
            if PILUnidentifiedImageError and isinstance(e, PILUnidentifiedImageError):
                raise ImageProcessingError(f"Failed to identify image in {func.__name__}: {e}") from e
            if isinstance(e, ValueError) and func.__name__ == 'apply_transformation':
                 raise ImageProcessingError(f"Image transformation error in {func.__name__}: {e}") from e
            if isinstance(e, ImportError) and func.__name__ == 'sklearn_kmeans_quantize':
                raise ConfigurationError(f"Configuration error (missing dependency) in {func.__name__}: {e}") from e
            raise
    return wrapper


def retry_s3_operation(max_attempts=3, initial_delay=1, backoff_factor=2):
    """
    Decorator to retry S3 operations with exponential backoff.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__ + '.' + func.__name__)
            attempts = 0
            delay = initial_delay
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except S3Error as e:
                    attempts += 1
                    is_retryable = False
                    if BotocoreClientError and isinstance(e.__cause__, BotocoreClientError):
                        error_code = e.__cause__.response.get('Error', {}).get('Code')
                        if error_code in RETRYABLE_S3_ERROR_CODES:
                            is_retryable = True

                    if not is_retryable and attempts < max_attempts:
                         logger.warning(
                             f"S3 operation '{func.__name__}' failed with a non-standard retryable S3Error. "
                             f"Attempt {attempts}/{max_attempts}. Retrying in {delay:.2f}s. Error: {e}"
                         )
                    elif not is_retryable:
                        logger.error(f"S3 operation '{func.__name__}' failed with non-retryable S3Error: {e}")
                        raise e

                    if attempts >= max_attempts:
                        logger.error(
                            f"S3 operation '{func.__name__}' failed after {max_attempts} attempts. Error: {e}"
                        )
                        raise e

                    logger.info(
                        f"S3 operation '{func.__name__}' failed. Attempt {attempts}/{max_attempts}. "
                        f"Retrying in {delay:.2f}s. Error: {e}"
                    )
                    time.sleep(delay)
                    delay *= backoff_factor
                except Exception as e:
                    logger.error(f"Unexpected error during S3 operation '{func.__name__}': {e}", exc_info=True)
                    raise
            if attempts >= max_attempts:
                 logger.error(f"S3 operation '{func.__name__}' ultimately failed after {max_attempts} attempts.")
                 raise S3Error(f"S3 operation '{func.__name__}' failed after {max_attempts} attempts")
        return wrapper
    return decorator


class BatchOperationContextManager:
    """
    Context manager for batch operations to collect and summarize errors.
    """
    def __init__(self, operation_name="Batch Operation"):
        self.operation_name = operation_name
        self.errors = []
        self.logger = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)

    def __enter__(self):
        self.logger.info(f"Starting {self.operation_name}.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.errors:
            self.logger.warning(
                f"{self.operation_name} completed with {len(self.errors)} error(s)."
            )
            for i, error_detail in enumerate(self.errors):
                item_identifier = error_detail.get('item', 'Unknown item')
                error_message = error_detail.get('error', 'Unknown error')
                self.logger.error(
                    f"  Error {i+1}/{len(self.errors)} for item '{item_identifier}': {error_message}"
                )
            # Optionally, raise an aggregated error here if needed in the future
            # e.g., raise BatchProcessingError(f"{self.operation_name} failed with {len(self.errors)} errors.", self.errors)
        elif exc_type:
            # An exception occurred outside of explicit error reporting to the context manager
            self.logger.error(
                f"{self.operation_name} failed due to an unhandled exception: {exc_val}",
                exc_info=(exc_type, exc_val, exc_tb)
            )
        else:
            self.logger.info(f"{self.operation_name} completed successfully.")

        # Return False to propagate any exceptions that were not handled by add_error
        # If an exception was passed to __exit__ (exc_type is not None),
        # returning False will re-raise it. If it was handled (e.g. by add_error and we
        # choose to suppress it), we could return True.
        # For now, we log and let it propagate if it wasn't from add_error.
        return False

    def add_error(self, error_message: str, item_identifier: str = "Unknown item"):
        """
        Call this method within the 'with' block to report an error for a specific item.

        Args:
            error_message (str): The error message or exception string.
            item_identifier (str): A string identifying the item that failed (e.g., filename, key).
        """
        self.errors.append({"item": item_identifier, "error": str(error_message)})
        self.logger.debug(f"Error added for item '{item_identifier}' in {self.operation_name}: {error_message}")
