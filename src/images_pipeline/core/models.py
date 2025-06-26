"""Shared data models for the images pipeline."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ProcessingConfig:
    """Configuration for the processing job."""

    source_bucket: str
    dest_bucket: str
    source_prefix: str = ""
    dest_prefix: str = ""
    transformation: Optional[str] = None
    batch_size: int = 100
    concurrency: int = 20
    debug: bool = False


@dataclass
class ImageItem:
    """Represents an image to be processed."""

    source_key: str
    dest_key: str


@dataclass
class ProcessingResult:
    """Result of processing a single image."""

    source_key: str
    dest_key: str = ""
    success: bool = False
    error: str = ""
    error_message: str = ""  # Alias for compatibility
    exif_data: Dict[str, Any] = field(default_factory=lambda: {})
    processing_time: float = 0.0

    def __post_init__(self):
        # Ensure backward compatibility
        if self.error and not self.error_message:
            self.error_message = self.error
        elif self.error_message and not self.error:
            self.error = self.error_message
