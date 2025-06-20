"""Sample module demonstrating project structure."""

from pydantic import BaseModel


class ImageConfig(BaseModel):
    """Configuration for image processing."""

    width: int
    height: int
    format: str = "png"
    quality: int = 95


def process_image(config: ImageConfig) -> str:
    """Process an image based on the provided configuration."""
    return f"Processing image: {config.width}x{config.height} ({config.format})"
