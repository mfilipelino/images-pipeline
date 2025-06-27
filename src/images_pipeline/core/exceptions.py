# src/images_pipeline/core/exceptions.py

class ImageProcessingError(Exception):
    """Custom exception for errors during image processing."""
    pass

class S3Error(Exception):
    """Custom exception for errors related to S3 operations."""
    pass

class ConfigurationError(Exception):
    """Custom exception for application configuration errors."""
    pass
