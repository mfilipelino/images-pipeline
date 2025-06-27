"""Core utilities and shared components for the images pipeline."""

from .image_utils import (
    apply_transformation,
    calculate_dest_key,
    extract_exif_data,
    native_kmeans_quantize,
    sklearn_kmeans_quantize,
)
from .logging_config import (
    configure_multiprocessing_logging,
    get_logger,
    setup_logger,
)
from .exceptions import (
    ImagesPipelineError,
    ImageProcessingError,
    S3Error,
    ConfigurationError,
    with_error_handling,
    batch_error_handler,
)
from .models import ImageItem, ProcessingConfig, ProcessingResult

__all__ = [
    "ProcessingConfig",
    "ImageItem",
    "ProcessingResult",
    "apply_transformation",
    "calculate_dest_key",
    "extract_exif_data",
    "native_kmeans_quantize",
    "sklearn_kmeans_quantize",
    "setup_logger",
    "get_logger",
    "configure_multiprocessing_logging",
    "ImagesPipelineError",
    "ImageProcessingError",
    "S3Error",
    "ConfigurationError",
    "with_error_handling",
    "batch_error_handler",
]
