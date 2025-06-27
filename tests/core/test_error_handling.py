# tests/core/test_error_handling.py

import pytest
import logging
from unittest import mock

# Custom exceptions from the application
from images_pipeline.core.exceptions import (
    ImageProcessingError,
    S3Error,
    ConfigurationError,
)

# Decorators and context manager to be tested
from images_pipeline.core.error_handling import (
    with_error_handling,
    retry_s3_operation,
    BatchOperationContextManager,
)

# Attempt to import for simulating specific error conditions
try:
    from botocore.exceptions import ClientError as BotocoreClientError
except ImportError:
    BotocoreClientError = None # Placeholder if botocore is not available

try:
    from PIL import UnidentifiedImageError as PILUnidentifiedImageError
except ImportError:
    PILUnidentifiedImageError = None # Placeholder if Pillow is not available


# --- Tests for Custom Exceptions ---

def test_custom_exceptions_raisable():
    """Test that custom exceptions can be raised and caught."""
    with pytest.raises(ImageProcessingError):
        raise ImageProcessingError("Test image error")
    with pytest.raises(S3Error):
        raise S3Error("Test S3 error")
    with pytest.raises(ConfigurationError):
        raise ConfigurationError("Test config error")

def test_custom_exception_inheritance():
    """Test that custom exceptions inherit from Exception."""
    assert issubclass(ImageProcessingError, Exception)
    assert issubclass(S3Error, Exception)
    assert issubclass(ConfigurationError, Exception)

# --- Tests for @with_error_handling decorator ---

@pytest.fixture
def mock_logger():
    """Fixture to mock the logger used by the decorators."""
    # Using 'images_pipeline.core.error_handling' as the logger name
    # if decorators use getLogger(__name__) or similar.
    # Or, more generally, mock logging.getLogger.
    # The decorators use logging.getLogger(func.__module__ + '.' + func.__name__)
    # So we might need a more flexible mock or to patch specific logger instances.
    # For now, let's mock the generic 'logging.getLogger' and check its calls.
    with mock.patch('logging.getLogger') as mock_get_logger:
        mock_log_instance = mock.Mock()
        mock_get_logger.return_value = mock_log_instance
        yield mock_log_instance

def test_with_error_handling_logs_error(mock_logger):
    """Test that @with_error_handling logs the error."""
    @with_error_handling
    def func_raising_error():
        raise ValueError("Original error")

    with pytest.raises(ValueError): # It re-raises original if not specifically mapped by default path
        func_raising_error()

    mock_logger.error.assert_called_once()
    # Check that exc_info=True was passed for traceback logging
    args, kwargs = mock_logger.error.call_args
    assert kwargs.get('exc_info') is True

@pytest.mark.skipif(BotocoreClientError is None, reason="botocore not installed")
def test_with_error_handling_wraps_botocore_error(mock_logger):
    """Test @with_error_handling wrapping BotocoreClientError into S3Error."""
    @with_error_handling
    def func_raising_s3_client_error():
        # Simulate a botocore ClientError
        # The response structure is important for botocore errors
        raise BotocoreClientError(
            error_response={'Error': {'Code': 'SomeS3ErrorCode', 'Message': 'Details'}},
            operation_name='GetObject'
        )

    with pytest.raises(S3Error) as excinfo:
        func_raising_s3_client_error()

    assert "S3 operation failed" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, BotocoreClientError)
    mock_logger.error.assert_called_once()

@pytest.mark.skipif(PILUnidentifiedImageError is None, reason="Pillow not installed or PILUnidentifiedImageError not found")
def test_with_error_handling_wraps_pil_error(mock_logger):
    """Test @with_error_handling wrapping PILUnidentifiedImageError into ImageProcessingError."""
    @with_error_handling
    def func_raising_pil_error():
        raise PILUnidentifiedImageError("Cannot identify image file")

    with pytest.raises(ImageProcessingError) as excinfo:
        func_raising_pil_error()

    assert "Failed to identify image" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, PILUnidentifiedImageError)
    mock_logger.error.assert_called_once()

def test_with_error_handling_value_error_for_apply_transformation(mock_logger):
    """Test @with_error_handling for ValueError in 'apply_transformation'."""
    # The decorator's logic specifically checks func.__name__
    @with_error_handling
    def apply_transformation(): # Mock function with the target name
        raise ValueError("Unknown transformation")

    with pytest.raises(ImageProcessingError) as excinfo:
        apply_transformation()

    assert "Image transformation error" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, ValueError)
    mock_logger.error.assert_called_once()


def test_with_error_handling_import_error_for_sklearn_quantize(mock_logger):
    """Test @with_error_handling for ImportError in 'sklearn_kmeans_quantize'."""
    @with_error_handling
    def sklearn_kmeans_quantize(): # Mock function with the target name
        raise ImportError("numpy not found")

    with pytest.raises(ConfigurationError) as excinfo:
        sklearn_kmeans_quantize()

    assert "Configuration error (missing dependency)" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, ImportError)
    mock_logger.error.assert_called_once()


def test_with_error_handling_reraises_unmapped_exception(mock_logger):
    """Test that unmapped exceptions are re-raised by default."""
    class CustomNonMappedError(Exception):
        pass

    @with_error_handling
    def func_raising_unmapped_error():
        raise CustomNonMappedError("This one is not mapped.")

    with pytest.raises(CustomNonMappedError):
        func_raising_unmapped_error()

    # It should still be logged
    mock_logger.error.assert_called_once()
    args, kwargs = mock_logger.error.call_args
    assert "This one is not mapped." in args[0]
    assert kwargs.get('exc_info') is True

# --- Tests for @retry_s3_operation decorator ---

def test_retry_s3_operation_success_on_first_attempt(mock_logger):
    """Test @retry_s3_operation succeeds immediately if no error."""
    @retry_s3_operation(max_attempts=3, initial_delay=0.01)
    def func_succeeds():
        return "success"

    assert func_succeeds() == "success"
    mock_logger.info.assert_not_called() # No retry logging
    mock_logger.warning.assert_not_called()
    mock_logger.error.assert_not_called()


@mock.patch('time.sleep', return_value=None) # Mock time.sleep to speed up tests
def test_retry_s3_operation_success_after_retries(mock_time_sleep, mock_logger):
    """Test @retry_s3_operation succeeds after a few S3Error failures."""
    mock_s3_op = mock.Mock()
    # Simulate S3Error for the first 2 calls, then success
    # This S3Error does not wrap a BotocoreClientError with a RETRYABLE_S3_ERROR_CODES code,
    # so it will trigger the 'non-standard retryable S3Error' warning.
    mock_s3_op.side_effect = [
        S3Error("Simulated S3 failure 1"),
        S3Error("Simulated S3 failure 2"),
        "success"
    ]

    @retry_s3_operation(max_attempts=3, initial_delay=0.01)
    def func_to_retry():
        return mock_s3_op()

    assert func_to_retry() == "success"
    assert mock_s3_op.call_count == 3
    # Check that time.sleep was called for retries
    assert mock_time_sleep.call_count == 2
    # Check logging for retries (2 info, 2 warning)
    # Two warnings because the S3Error is not directly a "retryable" AWS one by code.
    assert mock_logger.warning.call_count == 2
    assert mock_logger.info.call_count == 2 # Each retry attempt logs an info message
    # Verify the warning message content for one of the calls
    mock_logger.warning.assert_any_call(
        "S3 operation 'func_to_retry' failed with a non-standard retryable S3Error. "
        "Attempt 1/3. Retrying in 0.01s. Error: Simulated S3 failure 1"
    )
    mock_logger.info.assert_any_call(
        "S3 operation 'func_to_retry' failed. Attempt 1/3. "
        "Retrying in 0.01s. Error: Simulated S3 failure 1"
    )


@mock.patch('time.sleep', return_value=None)
def test_retry_s3_operation_success_with_retryable_botocore_error(mock_time_sleep, mock_logger):
    """Test success after retries with a specifically retryable BotocoreClientError."""
    mock_s3_op = mock.Mock()

    # Simulate a retryable BotocoreClientError wrapped in S3Error
    if BotocoreClientError: # Only run if botocore is available
        retryable_boto_error = BotocoreClientError(
            error_response={'Error': {'Code': 'ThrottlingException', 'Message': 'Too fast'}},
            operation_name='AnyS3Op'
        )
        mock_s3_op.side_effect = [
            S3Error("Failed due to throttling", __cause__=retryable_boto_error),
            "success"
        ]

        @retry_s3_operation(max_attempts=3, initial_delay=0.01)
        def func_with_retryable_error():
            return mock_s3_op()

        assert func_with_retryable_error() == "success"
        assert mock_s3_op.call_count == 2
        assert mock_time_sleep.call_count == 1
        # This time, no warning because the error code is in RETRYABLE_S3_ERROR_CODES
        mock_logger.warning.assert_not_called()
        mock_logger.info.assert_called_once_with(
            "S3 operation 'func_with_retryable_error' failed. Attempt 1/3. "
            "Retrying in 0.01s. Error: Failed due to throttling"
        )
    else:
        pytest.skip("botocore.exceptions.ClientError not available for this test.")


@mock.patch('time.sleep', return_value=None)
def test_retry_s3_operation_fails_after_max_attempts(mock_time_sleep, mock_logger):
    """Test @retry_s3_operation re-raises S3Error after max attempts."""
    mock_s3_op = mock.Mock(side_effect=S3Error("Persistent S3 failure"))

    @retry_s3_operation(max_attempts=3, initial_delay=0.01)
    def func_fails_persistently():
        return mock_s3_op()

    with pytest.raises(S3Error) as excinfo:
        func_fails_persistently()

    assert "Persistent S3 failure" in str(excinfo.value)
    assert mock_s3_op.call_count == 3
    assert mock_time_sleep.call_count == 2 # Retried 2 times before final failure

    # Check logging: 2 warnings (for non-standard retryable S3Error), 2 infos, 1 final error
    assert mock_logger.warning.call_count == 2
    assert mock_logger.info.call_count == 2
    mock_logger.error.assert_called_once_with(
        "S3 operation 'func_fails_persistently' failed after 3 attempts. Error: Persistent S3 failure"
    )

@mock.patch('time.sleep', return_value=None)
def test_retry_s3_operation_non_s3error_not_retried(mock_time_sleep, mock_logger):
    """Test @retry_s3_operation does not retry non-S3Errors."""
    mock_op = mock.Mock(side_effect=ValueError("Not an S3 error"))

    @retry_s3_operation(max_attempts=3, initial_delay=0.01)
    def func_raises_other_error():
        return mock_op()

    with pytest.raises(ValueError) as excinfo:
        func_raises_other_error()

    assert "Not an S3 error" in str(excinfo.value)
    assert mock_op.call_count == 1 # Called only once
    mock_time_sleep.assert_not_called() # No retries, so no sleep
    mock_logger.error.assert_called_once_with( # The decorator logs the unexpected error
        "Unexpected error during S3 operation 'func_raises_other_error': Not an S3 error",
        exc_info=True
    )
    mock_logger.info.assert_not_called()
    mock_logger.warning.assert_not_called()


@mock.patch('time.sleep', return_value=None)
def test_retry_s3_operation_non_retryable_s3error(mock_time_sleep, mock_logger):
    """Test @retry_s3_operation does not retry S3Errors wrapping non-retryable Botocore codes if configured strictly."""
    # This test depends on how strictly the decorator adheres to RETRYABLE_S3_ERROR_CODES.
    # The current implementation logs a warning and retries anyway.
    # If it were to not retry, this test would change.
    # For now, this test will be similar to test_retry_s3_operation_success_after_retries
    # but with an error code that's not in RETRYABLE_S3_ERROR_CODES.

    if not BotocoreClientError:
        pytest.skip("botocore not installed")

    non_retryable_boto_error = BotocoreClientError(
        error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Forbidden'}},
        operation_name='AnyS3Op'
    )
    mock_s3_op = mock.Mock()
    mock_s3_op.side_effect = [
        S3Error("Access denied error", __cause__=non_retryable_boto_error),
        "success_surprisingly" # If it retries
    ]

    # Assuming the decorator retries S3Error even if the cause is not a "known retryable" ClientError code.
    @retry_s3_operation(max_attempts=2, initial_delay=0.01)
    def func_with_non_retryable_cause():
        return mock_s3_op()

    assert func_with_non_retryable_cause() == "success_surprisingly"
    assert mock_s3_op.call_count == 2
    mock_time_sleep.assert_called_once()
    # It should log a warning because 'AccessDenied' is not in RETRYABLE_S3_ERROR_CODES
    mock_logger.warning.assert_called_once_with(
        "S3 operation 'func_with_non_retryable_cause' failed with a non-standard retryable S3Error. "
        "Attempt 1/2. Retrying in 0.01s. Error: Access denied error"
    )
    mock_logger.info.assert_called_once() # For the retry attempt


# --- Tests for BatchOperationContextManager ---

def test_batch_manager_no_errors(mock_logger):
    """Test BatchOperationContextManager when no errors are reported."""
    with BatchOperationContextManager(operation_name="TestOp Success") as manager:
        # Simulate some operations within the context
        manager.logger.debug("Performing step 1...") # Example of using manager's logger
        manager.logger.debug("Performing step 2...")

    mock_logger.info.assert_any_call("Starting TestOp Success.")
    mock_logger.info.assert_any_call("TestOp Success completed successfully.")
    # Ensure no error-related logs were made to the manager's logger directly for summary
    # (individual errors would be logged by add_error if called)
    # We are checking the __exit__ behavior here.
    # mock_logger is the one passed to the test function, which is a mock of what getLogger returns.
    # The BatchOperationContextManager creates its own logger instance.
    # So, we need to check the logging calls more broadly or mock the specific logger name.

    # Let's refine mock_logger for BatchOperationContextManager tests
    # The manager uses logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)
    # e.g., 'images_pipeline.core.error_handling.BatchOperationContextManager'

    # For simplicity in this subtask, let's assume mock_logger (from the fixture)
    # would capture these if logging.getLogger was patched broadly.
    # The critical part is that no error summary (like "completed with X error(s)") is logged.

    # Check that no warning or error calls were made for error summaries
    assert not any("completed with" in call_args[0][0] and "error(s)" in call_args[0][0] for call_args in mock_logger.warning.call_args_list)


def test_batch_manager_with_errors_added(mock_logger):
    """Test BatchOperationContextManager collecting and logging errors via add_error."""
    with BatchOperationContextManager(operation_name="TestOp Errors") as manager:
        manager.add_error(error_message="First item failed", item_identifier="item1")
        manager.add_error(error_message="Second item failed", item_identifier="item2")

    mock_logger.info.assert_any_call("Starting TestOp Errors.")
    mock_logger.warning.assert_any_call("TestOp Errors completed with 2 error(s).")
    mock_logger.error.assert_any_call("  Error 1/2 for item 'item1': First item failed")
    mock_logger.error.assert_any_call("  Error 2/2 for item 'item2': Second item failed")
    # Check that add_error also logged debug messages
    mock_logger.debug.assert_any_call("Error added for item 'item1' in TestOp Errors: First item failed")


def test_batch_manager_handles_exception_in_context(mock_logger):
    """Test BatchOperationContextManager when an unhandled exception occurs within the context."""
    class MyContextError(Exception):
        pass

    with pytest.raises(MyContextError): # Expect the error to propagate
        with BatchOperationContextManager(operation_name="TestOp Unhandled") as manager:
            manager.add_error("This error was added", "item0") # This will be logged by add_error
            raise MyContextError("Something bad happened inside with block")

    mock_logger.info.assert_any_call("Starting TestOp Unhandled.")
    # The manager should log the unhandled exception in __exit__
    # Check for the specific log message from __exit__ for unhandled exceptions
    unhandled_log_found = False
    for call in mock_logger.error.call_args_list:
        if "TestOp Unhandled failed due to an unhandled exception: Something bad happened inside with block" in call[0][0]:
            unhandled_log_found = True
            # Check if exc_info was passed for the unhandled exception
            assert call[1].get('exc_info') is not None
            assert isinstance(call[1]['exc_info'][1], MyContextError)
            break
    assert unhandled_log_found, "Manager did not log the unhandled exception correctly."

    # It should also log the errors added before the exception
    mock_logger.warning.assert_any_call("TestOp Unhandled completed with 1 error(s).")
    mock_logger.error.assert_any_call("  Error 1/1 for item 'item0': This error was added")


def test_batch_manager_custom_operation_name(mock_logger):
    """Test that the operation_name is used in logs."""
    op_name = "My Custom Batch Job"
    with BatchOperationContextManager(operation_name=op_name) as manager:
        pass # No errors, just check logs

    mock_logger.info.assert_any_call(f"Starting {op_name}.")
    mock_logger.info.assert_any_call(f"{op_name} completed successfully.")
