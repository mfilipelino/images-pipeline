"""Tests for module1."""

import pytest
from images_pipeline.module1 import ImageConfig, process_image


def test_image_config_creation() -> None:
    """Test creating an ImageConfig instance."""
    config = ImageConfig(width=800, height=600)
    assert config.width == 800
    assert config.height == 600
    assert config.format == "png"
    assert config.quality == 95


def test_image_config_custom_values() -> None:
    """Test ImageConfig with custom values."""
    config = ImageConfig(width=1920, height=1080, format="jpg", quality=80)
    assert config.width == 1920
    assert config.height == 1080
    assert config.format == "jpg"
    assert config.quality == 80


def test_process_image() -> None:
    """Test the process_image function."""
    config = ImageConfig(width=640, height=480)
    result = process_image(config)
    assert result == "Processing image: 640x480 (png)"