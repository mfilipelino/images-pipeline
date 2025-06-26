"""Tests for serial processor functionality."""

from unittest.mock import Mock, patch
from images_pipeline.core.models import ImageItem, ProcessingConfig, ProcessingResult
from images_pipeline.processors.serial import process_batch


class TestSerialProcessor:
    """Tests for serial processor implementation."""

    def test_process_batch_empty_list(self):
        """Test process_batch with empty batch list."""
        mock_s3_client = Mock()
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest"
        )
        
        results = process_batch([], config, mock_s3_client)
        
        assert results == []

    def test_process_batch_single_item(self):
        """Test process_batch with single item."""
        mock_s3_client = Mock()
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest"
        )
        
        batch = [ImageItem(source_key="test.jpg", dest_key="output/test.jpg")]
        
        # Mock the process_single_image function
        with patch('images_pipeline.processors.serial.process_single_image') as mock_process:
            expected_result = ProcessingResult(
                source_key="test.jpg",
                dest_key="output/test.jpg",
                success=True
            )
            mock_process.return_value = expected_result
            
            results = process_batch(batch, config, mock_s3_client)
            
            assert len(results) == 1
            assert results[0] == expected_result
            mock_process.assert_called_once_with(mock_s3_client, batch[0], config)

    def test_process_batch_multiple_items(self):
        """Test process_batch with multiple items."""
        mock_s3_client = Mock()
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest"
        )
        
        batch = [
            ImageItem(source_key="test1.jpg", dest_key="output/test1.jpg"),
            ImageItem(source_key="test2.jpg", dest_key="output/test2.jpg"),
            ImageItem(source_key="test3.jpg", dest_key="output/test3.jpg")
        ]
        
        # Mock the process_single_image function
        with patch('images_pipeline.processors.serial.process_single_image') as mock_process:
            expected_results = [
                ProcessingResult(source_key="test1.jpg", dest_key="output/test1.jpg", success=True),
                ProcessingResult(source_key="test2.jpg", dest_key="output/test2.jpg", success=True),
                ProcessingResult(source_key="test3.jpg", dest_key="output/test3.jpg", success=False, error="Failed")
            ]
            mock_process.side_effect = expected_results
            
            results = process_batch(batch, config, mock_s3_client)
            
            assert len(results) == 3
            assert results == expected_results
            assert mock_process.call_count == 3

    def test_process_batch_preserves_order(self):
        """Test that process_batch preserves the order of items."""
        mock_s3_client = Mock()
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest"
        )
        
        batch = [
            ImageItem(source_key="first.jpg", dest_key="output/first.jpg"),
            ImageItem(source_key="second.jpg", dest_key="output/second.jpg"),
            ImageItem(source_key="third.jpg", dest_key="output/third.jpg")
        ]
        
        with patch('images_pipeline.processors.serial.process_single_image') as mock_process:
            # Return results that we can identify by source_key
            def create_result(s3_client, item, config):
                return ProcessingResult(
                    source_key=item.source_key,
                    dest_key=item.dest_key,
                    success=True
                )
            
            mock_process.side_effect = create_result
            
            results = process_batch(batch, config, mock_s3_client)
            
            # Verify order is preserved
            assert len(results) == 3
            assert results[0].source_key == "first.jpg"
            assert results[1].source_key == "second.jpg"
            assert results[2].source_key == "third.jpg"

    def test_process_batch_calls_process_single_image_with_correct_args(self):
        """Test that process_batch calls process_single_image with correct arguments."""
        mock_s3_client = Mock()
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest",
            transformation="grayscale"
        )
        
        batch = [ImageItem(source_key="test.jpg", dest_key="output/test.jpg")]
        
        with patch('images_pipeline.processors.serial.process_single_image') as mock_process:
            mock_process.return_value = ProcessingResult(source_key="test.jpg")
            
            process_batch(batch, config, mock_s3_client)
            
            # Verify the call arguments
            mock_process.assert_called_once_with(mock_s3_client, batch[0], config)
            
            # Verify the actual arguments passed
            call_args = mock_process.call_args[0]
            assert call_args[0] is mock_s3_client
            assert call_args[1] is batch[0]
            assert call_args[2] is config

    def test_process_batch_handles_different_config_options(self):
        """Test process_batch with different configuration options."""
        mock_s3_client = Mock()
        
        # Test with different configurations
        configs = [
            ProcessingConfig(source_bucket="src", dest_bucket="dst"),
            ProcessingConfig(source_bucket="src", dest_bucket="dst", transformation="grayscale"),
            ProcessingConfig(source_bucket="src", dest_bucket="dst", debug=True),
            ProcessingConfig(source_bucket="src", dest_bucket="dst", batch_size=50)
        ]
        
        batch = [ImageItem(source_key="test.jpg", dest_key="output/test.jpg")]
        
        for config in configs:
            with patch('images_pipeline.processors.serial.process_single_image') as mock_process:
                mock_process.return_value = ProcessingResult(source_key="test.jpg")
                
                results = process_batch(batch, config, mock_s3_client)
                
                assert len(results) == 1
                mock_process.assert_called_once_with(mock_s3_client, batch[0], config)

    def test_process_batch_with_mixed_success_failure(self):
        """Test process_batch handles mixed success and failure results."""
        mock_s3_client = Mock()
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest"
        )
        
        batch = [
            ImageItem(source_key="success.jpg", dest_key="output/success.jpg"),
            ImageItem(source_key="failure.jpg", dest_key="output/failure.jpg")
        ]
        
        with patch('images_pipeline.processors.serial.process_single_image') as mock_process:
            expected_results = [
                ProcessingResult(source_key="success.jpg", success=True),
                ProcessingResult(source_key="failure.jpg", success=False, error="Processing failed")
            ]
            mock_process.side_effect = expected_results
            
            results = process_batch(batch, config, mock_s3_client)
            
            assert len(results) == 2
            assert results[0].success is True
            assert results[1].success is False
            assert results[1].error == "Processing failed"