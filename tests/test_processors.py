"""Tests for processor modules."""

import pytest
from unittest.mock import Mock, MagicMock

from src.images_pipeline.core import ProcessingConfig, ImageItem
from src.images_pipeline.processors import serial_process_batch


def test_serial_processor():
    """Test serial processor with mocked S3 client."""
    # Create test config and items
    config = ProcessingConfig(
        source_bucket="test-source",
        dest_bucket="test-dest",
        source_prefix="input",
        dest_prefix="output",
    )
    
    items = [
        ImageItem(source_key="input/test1.jpg", dest_key="output/test1.jpg"),
        ImageItem(source_key="input/test2.jpg", dest_key="output/test2.jpg"),
    ]
    
    # Mock S3 client
    mock_s3 = Mock()
    mock_response = {
        "Body": MagicMock(read=lambda: b"fake image data")
    }
    mock_s3.get_object.return_value = mock_response
    
    # Mock PIL Image
    with pytest.MonkeyPatch.context() as m:
        mock_image = Mock()
        mock_image.size = (100, 100)
        mock_image.format = "JPEG"
        mock_image.mode = "RGB"
        mock_image._getexif.return_value = None
        
        mock_image_open = Mock(return_value=mock_image)
        m.setattr("PIL.Image.open", mock_image_open)
        
        # Process batch
        results = serial_process_batch(items, config, mock_s3)
        
        # Verify results
        assert len(results) == 2
        assert all(r.source_key in ["input/test1.jpg", "input/test2.jpg"] for r in results)
        
        # Should have called get_object for each item
        assert mock_s3.get_object.call_count == 2
        
        # Should have called put_object for each successful item
        successful_results = [r for r in results if r.success]
        assert mock_s3.put_object.call_count == len(successful_results)