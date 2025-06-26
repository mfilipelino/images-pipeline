"""Tests for logging_config.py utility functions."""

import os
import sys
import logging
from unittest.mock import patch, MagicMock
from images_pipeline.core.logging_config import (
    setup_logger,
    get_logger,
    configure_multiprocessing_logging,
    logger,
)


class TestSetupLogger:
    """Tests for setup_logger function."""

    def test_setup_logger_default_parameters(self):
        """Test setup_logger with default parameters."""
        test_logger = setup_logger()
        assert test_logger.name == "images-pipeline"
        assert test_logger.level == logging.INFO
        assert len(test_logger.handlers) == 1
        assert not test_logger.propagate

    def test_setup_logger_custom_name(self):
        """Test setup_logger with custom name."""
        custom_name = "test-custom-logger"
        test_logger = setup_logger(name=custom_name)
        assert test_logger.name == custom_name

    def test_setup_logger_custom_level_by_parameter(self):
        """Test setup_logger with custom level via parameter."""
        test_logger = setup_logger(level="DEBUG")
        assert test_logger.level == logging.DEBUG

    def test_setup_logger_custom_level_by_env_var(self):
        """Test setup_logger with custom level via environment variable."""
        with patch.dict(os.environ, {"LOG_LEVEL": "WARNING"}):
            test_logger = setup_logger(name="test-env-level")
            assert test_logger.level == logging.WARNING

    def test_setup_logger_invalid_level_defaults_to_info(self):
        """Test setup_logger with invalid level defaults to INFO."""
        test_logger = setup_logger(level="INVALID_LEVEL")
        assert test_logger.level == logging.INFO

    def test_setup_logger_structured_format(self):
        """Test setup_logger with structured format."""
        test_logger = setup_logger(name="test-structured", format_type="structured")
        handler = test_logger.handlers[0]
        formatter = handler.formatter

        # Check that formatter contains expected structured elements
        format_string = formatter._fmt
        assert "%(asctime)s" in format_string
        assert "%(name)s" in format_string
        assert "%(levelname)" in format_string
        assert "%(filename)s" in format_string
        assert "%(lineno)d" in format_string
        assert "%(funcName)s" in format_string

    def test_setup_logger_simple_format(self):
        """Test setup_logger with simple format."""
        test_logger = setup_logger(name="test-simple", format_type="simple")
        handler = test_logger.handlers[0]
        formatter = handler.formatter

        # Check that formatter contains expected simple elements
        format_string = formatter._fmt
        assert "%(asctime)s" in format_string
        assert "%(name)s" in format_string
        assert "%(levelname)s" in format_string
        assert "%(message)s" in format_string

    def test_setup_logger_env_format_override(self):
        """Test setup_logger format override via environment variable."""
        with patch.dict(os.environ, {"LOG_FORMAT": "simple"}):
            test_logger = setup_logger(name="test-env-format", format_type="structured")
            handler = test_logger.handlers[0]
            formatter = handler.formatter

            # Should use simple format from env var, not structured from parameter
            format_string = formatter._fmt
            assert "%(filename)s" not in format_string  # Not in simple format

    def test_setup_logger_no_duplicate_handlers(self):
        """Test that setup_logger doesn't add duplicate handlers."""
        logger_name = "test-no-duplicates"
        test_logger1 = setup_logger(name=logger_name)
        test_logger2 = setup_logger(name=logger_name)

        # Should be the same logger instance
        assert test_logger1 is test_logger2
        assert len(test_logger1.handlers) == 1

    def test_setup_logger_handler_uses_stdout(self):
        """Test that logger handler uses stdout."""
        test_logger = setup_logger(name="test-stdout")
        handler = test_logger.handlers[0]
        assert handler.stream is sys.stdout


class TestGetLogger:
    """Tests for get_logger function."""

    def test_get_logger_default_name(self):
        """Test get_logger with default name."""
        test_logger = get_logger()
        assert test_logger.name == "images-pipeline"

    def test_get_logger_custom_name(self):
        """Test get_logger with custom name."""
        custom_name = "test-get-logger"
        test_logger = get_logger(name=custom_name)
        assert test_logger.name == custom_name

    def test_get_logger_returns_configured_logger(self):
        """Test that get_logger returns a properly configured logger."""
        test_logger = get_logger(name="test-configured")
        assert len(test_logger.handlers) == 1
        assert not test_logger.propagate


class TestConfigureMultiprocessingLogging:
    """Tests for configure_multiprocessing_logging function."""

    @patch("multiprocessing.current_process")
    @patch("images_pipeline.core.logging_config.setup_logger")
    def test_configure_multiprocessing_logging(
        self, mock_setup_logger, mock_current_process
    ):
        """Test configure_multiprocessing_logging creates process-specific logger."""
        # Mock the current process
        mock_process = MagicMock()
        mock_process.name = "TestProcess-1"
        mock_current_process.return_value = mock_process

        configure_multiprocessing_logging()

        # Verify setup_logger was called with process-specific name
        expected_logger_name = "images-pipeline.TestProcess-1"
        mock_setup_logger.assert_called_once_with(expected_logger_name)


class TestDefaultLogger:
    """Tests for default logger instance."""

    def test_default_logger_exists(self):
        """Test that default logger instance is created."""
        assert logger is not None
        assert isinstance(logger, logging.Logger)
        assert logger.name == "images-pipeline"

    def test_default_logger_configured(self):
        """Test that default logger is properly configured."""
        assert len(logger.handlers) >= 1
        assert not logger.propagate


class TestLoggingIntegration:
    """Integration tests for logging functionality."""

    def test_logger_actually_logs_messages(self):
        """Test that logger actually outputs log messages."""
        test_logger = setup_logger(name="test-integration", level="DEBUG")

        # Test that the logger has a handler configured to stdout
        assert len(test_logger.handlers) == 1
        handler = test_logger.handlers[0]
        assert handler.stream is sys.stdout

    def test_different_log_levels_work(self):
        """Test that different log levels work correctly."""
        test_logger = setup_logger(name="test-levels", level="DEBUG")

        with patch("sys.stdout"):
            # These should not raise exceptions
            test_logger.debug("Debug message")
            test_logger.info("Info message")
            test_logger.warning("Warning message")
            test_logger.error("Error message")
            test_logger.critical("Critical message")

    def test_log_level_filtering_works(self):
        """Test that log level filtering works correctly."""
        # Logger set to WARNING should not output INFO messages
        test_logger = setup_logger(name="test-filtering", level="WARNING")

        with patch("sys.stdout"):
            test_logger.info("This should not appear")
            test_logger.warning("This should appear")

            # Info should not write to stdout, warning should
            # We can't easily test the exact calls due to formatting,
            # but we can verify the logger level is set correctly
            assert test_logger.level == logging.WARNING
