"""Factory classes for creating configured service instances."""

import logging
from typing import Dict, Any, Optional
import boto3

from .protocols import S3ClientProtocol, LoggerProtocol
from .services import (
    ImageProcessorService,
    S3FileDiscoveryService,
    ImageProcessingService,
    SerialBatchProcessor,
    ProcessingOrchestrator,
)


class LoggerAdapter:
    """Adapter to make standard logger compatible with LoggerProtocol."""

    def __init__(self, logger: logging.Logger):
        self._logger = logger

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self._logger.debug(message)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self._logger.info(message)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self._logger.warning(message)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        self._logger.error(message)


class LoggerFactory:
    """Factory for creating logger instances."""

    @staticmethod
    def create_logger(name: str, level: int = logging.INFO) -> LoggerProtocol:
        """Create a configured logger instance."""
        logger = logging.getLogger(name)
        logger.setLevel(level)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return LoggerAdapter(logger)


class S3ClientFactory:
    """Factory for creating S3 client instances."""

    @staticmethod
    def create_s3_client(**kwargs: Any) -> S3ClientProtocol:
        """Create S3 client with optional configuration."""
        session = boto3.Session()
        return session.client("s3", **kwargs)  # type: ignore


class ProcessingPipelineFactory:
    """Factory for creating the complete processing pipeline."""

    @staticmethod
    def create_pipeline(
        s3_client: Optional[S3ClientProtocol] = None,
        logger: Optional[LoggerProtocol] = None,
        config_overrides: Optional[Dict[str, Any]] = None,
    ) -> ProcessingOrchestrator:
        """Create a fully configured processing pipeline."""

        # Create default dependencies if not provided
        if s3_client is None:
            s3_client = S3ClientFactory.create_s3_client()

        if logger is None:
            logger = LoggerFactory.create_logger("images_pipeline")

        # Create services
        image_processor = ImageProcessorService()
        file_discovery = S3FileDiscoveryService(s3_client, logger)
        processing_service = ImageProcessingService(s3_client, image_processor, logger)
        batch_processor = SerialBatchProcessor(processing_service, logger)

        # Create orchestrator
        orchestrator = ProcessingOrchestrator(
            file_discovery=file_discovery,
            batch_processor=batch_processor,
            logger=logger,
        )

        return orchestrator
