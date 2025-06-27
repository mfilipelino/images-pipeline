"""Shared data models for the images pipeline."""

from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class ProcessingConfig(BaseModel):
    """Configuration for the processing job."""

    source_bucket: str
    dest_bucket: str
    source_prefix: str = ""
    dest_prefix: str = ""
    transformation: Optional[str] = None
    batch_size: int = 100
    concurrency: int = 20
    debug: bool = False


class ImageItem(BaseModel):
    """Represents an image to be processed."""

    source_key: str
    dest_key: str


class ProcessingResult(BaseModel):
    """Result of processing a single image."""

    source_key: str
    dest_key: str = ""
    success: bool = False
    error: str = ""
    exif_data: Dict[str, Any] = Field(default_factory=dict)
    processing_time: float = 0.0
