"""Protocol definitions for dependency injection and testability."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Protocol

from .models import ImageItem, ProcessingConfig, ProcessingResult


class S3ClientProtocol(Protocol):
    """Protocol for S3 client operations."""

    def get_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        """Get object from S3."""
        ...

    def put_object(
        self, Bucket: str, Key: str, Body: bytes, ContentType: str
    ) -> Dict[str, Any]:
        """Put object to S3."""
        ...

    def get_paginator(self, operation_name: str) -> Any:
        """Get paginator for S3 operations."""
        ...


class ImageProcessorProtocol(Protocol):
    """Protocol for image processing operations."""

    def apply_transformation(self, image_bytes: bytes, transformation: str) -> bytes:
        """Apply transformation to image bytes."""
        ...

    def extract_metadata(self, image_bytes: bytes) -> Dict[str, Any]:
        """Extract metadata from image bytes."""
        ...


class LoggerProtocol(Protocol):
    """Protocol for logging operations."""

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        ...

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        ...

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        ...

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        ...


class FileDiscoveryService(ABC):
    """Abstract service for discovering files to process."""

    @abstractmethod
    def discover_files(self, bucket: str, prefix: str) -> List[str]:
        """Discover files to process."""
        ...


class ProcessingService(ABC):
    """Abstract service for processing images."""

    @abstractmethod
    def process_image(
        self, item: ImageItem, config: ProcessingConfig
    ) -> ProcessingResult:
        """Process a single image."""
        ...


class BatchProcessor(ABC):
    """Abstract batch processor."""

    @abstractmethod
    def process_batch(
        self, items: List[ImageItem], config: ProcessingConfig
    ) -> List[ProcessingResult]:
        """Process a batch of images."""
        ...
