"""Unit tests for service implementations."""

import pytest
from unittest.mock import Mock

from images_pipeline.core.services import (
    ImageProcessorService,
    S3FileDiscoveryService,
    ImageProcessingService,
    SerialBatchProcessor,
    WorkItemFactory,
    ProcessingOrchestrator,
)
from images_pipeline.core.models import ImageItem, ProcessingConfig, ProcessingResult
from images_pipeline.testing.fakes import (
    FakeS3Client,
    FakeLogger,
    create_test_image,
    setup_test_s3_environment,
)


class TestImageProcessorService:
    """Tests for ImageProcessorService."""

    def test_apply_transformation_no_transformation(self):
        """Test that no transformation returns original bytes."""
        processor = ImageProcessorService()
        image_bytes = create_test_image(100, 100)

        result = processor.apply_transformation(image_bytes, "")

        assert result == image_bytes

    @pytest.mark.parametrize(
        "transformation",
        [
            "grayscale",
            "kmeans",
        ],
    )
    def test_apply_transformation_valid_transformations(self, transformation):
        """Test valid transformations are applied."""
        processor = ImageProcessorService()
        image_bytes = create_test_image(50, 50)

        result = processor.apply_transformation(image_bytes, transformation)

        assert result != image_bytes
        assert len(result) > 0

    def test_apply_transformation_invalid_transformation(self):
        """Test that invalid transformation raises error."""
        processor = ImageProcessorService()
        image_bytes = create_test_image(50, 50)

        with pytest.raises(ValueError, match="Unknown transformation"):
            processor.apply_transformation(image_bytes, "invalid_transform")

    def test_extract_metadata_success(self):
        """Test successful metadata extraction."""
        processor = ImageProcessorService()
        image_bytes = create_test_image(100, 150)

        metadata = processor.extract_metadata(image_bytes)

        assert isinstance(metadata, dict)
        assert metadata["width"] == 100
        assert metadata["height"] == 150
        assert "format" in metadata
        assert "mode" in metadata

    def test_extract_metadata_invalid_image(self):
        """Test metadata extraction with invalid image data."""
        processor = ImageProcessorService()

        with pytest.raises(Exception):
            processor.extract_metadata(b"not an image")


class TestS3FileDiscoveryService:
    """Tests for S3FileDiscoveryService."""

    def test_discover_files_success(self):
        """Test successful file discovery."""
        fake_s3 = setup_test_s3_environment()
        logger = FakeLogger()
        service = S3FileDiscoveryService(fake_s3, logger)

        files = service.discover_files("test-source", "images")

        assert len(files) == 4  # 4 JPEG files
        assert all(f.endswith((".jpg", ".jpeg")) for f in files)
        assert "images/photo1.jpg" in files
        assert "images/subfolder/photo4.jpg" in files
        # Should not include txt and json files
        assert "images/readme.txt" not in files
        assert "images/config.json" not in files

    def test_discover_files_empty_prefix(self):
        """Test file discovery with empty prefix."""
        fake_s3 = setup_test_s3_environment()
        logger = FakeLogger()
        service = S3FileDiscoveryService(fake_s3, logger)

        files = service.discover_files("test-source", "")

        assert len(files) == 4  # All JPEG files

    def test_discover_files_nonexistent_bucket(self):
        """Test file discovery with nonexistent bucket."""
        fake_s3 = FakeS3Client()
        logger = FakeLogger()
        service = S3FileDiscoveryService(fake_s3, logger)

        with pytest.raises(ValueError, match="Bucket.*not found"):
            service.discover_files("nonexistent-bucket", "images")

    def test_discover_files_s3_failure(self):
        """Test file discovery with S3 failure."""
        fake_s3 = setup_test_s3_environment()
        fake_s3.set_failure_mode(True, "S3 Service Unavailable")
        logger = FakeLogger()
        service = S3FileDiscoveryService(fake_s3, logger)

        with pytest.raises(Exception, match="S3 Service Unavailable"):
            service.discover_files("test-source", "images")


class TestImageProcessingService:
    """Tests for ImageProcessingService."""

    def test_process_image_success(self):
        """Test successful image processing."""
        fake_s3 = setup_test_s3_environment()
        logger = FakeLogger()
        image_processor = ImageProcessorService()
        service = ImageProcessingService(fake_s3, image_processor, logger)

        config = ProcessingConfig(source_bucket="test-source", dest_bucket="test-dest")
        item = ImageItem(
            source_key="images/photo1.jpg", dest_key="processed/photo1.jpg"
        )

        result = service.process_image(item, config)

        assert result.success is True
        assert result.source_key == "images/photo1.jpg"
        assert result.dest_key == "processed/photo1.jpg"
        assert result.processing_time > 0
        assert len(result.exif_data) > 0

    def test_process_image_with_transformation(self):
        """Test image processing with transformation."""
        fake_s3 = setup_test_s3_environment()
        logger = FakeLogger()
        image_processor = ImageProcessorService()
        service = ImageProcessingService(fake_s3, image_processor, logger)

        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest",
            transformation="grayscale",
        )
        item = ImageItem(
            source_key="images/photo1.jpg", dest_key="processed/photo1.jpg"
        )

        result = service.process_image(item, config)

        assert result.success is True
        # Verify transformed image was uploaded
        dest_bucket = fake_s3.get_bucket("test-dest")
        assert dest_bucket.get_object("processed/photo1.jpg") is not None

    def test_process_image_nonexistent_source(self):
        """Test processing nonexistent source image."""
        fake_s3 = setup_test_s3_environment()
        logger = FakeLogger()
        image_processor = ImageProcessorService()
        service = ImageProcessingService(fake_s3, image_processor, logger)

        config = ProcessingConfig(source_bucket="test-source", dest_bucket="test-dest")
        item = ImageItem(
            source_key="nonexistent.jpg", dest_key="processed/nonexistent.jpg"
        )

        result = service.process_image(item, config)

        assert result.success is False
        assert "not found" in result.error

    def test_process_image_s3_failure(self):
        """Test processing with S3 failure."""
        fake_s3 = setup_test_s3_environment()
        fake_s3.set_failure_mode(True, "S3 Service Error")
        logger = FakeLogger()
        image_processor = ImageProcessorService()
        service = ImageProcessingService(fake_s3, image_processor, logger)

        config = ProcessingConfig(source_bucket="test-source", dest_bucket="test-dest")
        item = ImageItem(
            source_key="images/photo1.jpg", dest_key="processed/photo1.jpg"
        )

        result = service.process_image(item, config)

        assert result.success is False
        assert "S3 Service Error" in result.error


class TestSerialBatchProcessor:
    """Tests for SerialBatchProcessor."""

    def test_process_batch_empty(self):
        """Test processing empty batch."""
        mock_service = Mock()
        logger = FakeLogger()
        processor = SerialBatchProcessor(mock_service, logger)

        results = processor.process_batch([], ProcessingConfig("src", "dst"))

        assert results == []
        mock_service.process_image.assert_not_called()

    def test_process_batch_single_item(self):
        """Test processing single item batch."""
        mock_service = Mock()
        mock_service.process_image.return_value = ProcessingResult(
            source_key="test.jpg", success=True
        )
        logger = FakeLogger()
        processor = SerialBatchProcessor(mock_service, logger)

        config = ProcessingConfig("src", "dst")
        items = [ImageItem("test.jpg", "out.jpg")]

        results = processor.process_batch(items, config)

        assert len(results) == 1
        assert results[0].success is True
        mock_service.process_image.assert_called_once_with(items[0], config)

    @pytest.mark.parametrize("num_items", [2, 5, 10])
    def test_process_batch_multiple_items(self, num_items):
        """Test processing multiple items."""
        mock_service = Mock()
        mock_service.process_image.side_effect = [
            ProcessingResult(source_key=f"test{i}.jpg", success=True)
            for i in range(num_items)
        ]
        logger = FakeLogger()
        processor = SerialBatchProcessor(mock_service, logger)

        config = ProcessingConfig("src", "dst")
        items = [ImageItem(f"test{i}.jpg", f"out{i}.jpg") for i in range(num_items)]

        results = processor.process_batch(items, config)

        assert len(results) == num_items
        assert all(r.success for r in results)
        assert mock_service.process_image.call_count == num_items

    def test_process_batch_preserves_order(self):
        """Test that batch processing preserves order."""
        mock_service = Mock()

        def process_image_side_effect(item, config):
            return ProcessingResult(source_key=item.source_key, success=True)

        mock_service.process_image.side_effect = process_image_side_effect
        logger = FakeLogger()
        processor = SerialBatchProcessor(mock_service, logger)

        config = ProcessingConfig("src", "dst")
        items = [
            ImageItem("first.jpg", "out1.jpg"),
            ImageItem("second.jpg", "out2.jpg"),
            ImageItem("third.jpg", "out3.jpg"),
        ]

        results = processor.process_batch(items, config)

        assert len(results) == 3
        assert results[0].source_key == "first.jpg"
        assert results[1].source_key == "second.jpg"
        assert results[2].source_key == "third.jpg"


class TestWorkItemFactory:
    """Tests for WorkItemFactory."""

    def test_create_work_items_empty_list(self):
        """Test creating work items from empty list."""
        config = ProcessingConfig("src", "dst")

        items = WorkItemFactory.create_work_items([], config)

        assert items == []

    def test_create_work_items_single_file(self):
        """Test creating work items from single file."""
        config = ProcessingConfig("src", "dst", "input/", "output/")

        items = WorkItemFactory.create_work_items(["input/test.jpg"], config)

        assert len(items) == 1
        assert items[0].source_key == "input/test.jpg"
        assert items[0].dest_key == "output/test.jpg"

    def test_create_work_items_multiple_files(self):
        """Test creating work items from multiple files."""
        config = ProcessingConfig("src", "dst", "photos/", "processed/")
        source_files = ["photos/img1.jpg", "photos/img2.jpg", "photos/subdir/img3.jpg"]

        items = WorkItemFactory.create_work_items(source_files, config)

        assert len(items) == 3
        assert items[0].source_key == "photos/img1.jpg"
        assert items[0].dest_key == "processed/img1.jpg"
        assert items[2].dest_key == "processed/subdir/img3.jpg"

    def test_create_work_items_no_prefixes(self):
        """Test creating work items with no prefixes."""
        config = ProcessingConfig("src", "dst")

        items = WorkItemFactory.create_work_items(["test.jpg"], config)

        assert len(items) == 1
        assert items[0].source_key == "test.jpg"
        assert items[0].dest_key == "test.jpg"


class TestProcessingOrchestrator:
    """Tests for ProcessingOrchestrator."""

    def test_process_all_no_files(self):
        """Test processing when no files are found."""
        mock_discovery = Mock()
        mock_discovery.discover_files.return_value = []
        mock_processor = Mock()
        logger = FakeLogger()

        orchestrator = ProcessingOrchestrator(mock_discovery, mock_processor, logger)
        config = ProcessingConfig("src", "dst")

        result = orchestrator.process_all(config)

        assert result["total_items"] == 0
        assert result["processed_count"] == 0
        assert result["error_count"] == 0
        mock_processor.process_batch.assert_not_called()

    def test_process_all_success(self):
        """Test successful processing of all files."""
        mock_discovery = Mock()
        mock_discovery.discover_files.return_value = ["img1.jpg", "img2.jpg"]

        mock_processor = Mock()
        mock_processor.process_batch.return_value = [
            ProcessingResult(source_key="img1.jpg", success=True),
            ProcessingResult(source_key="img2.jpg", success=True),
        ]

        logger = FakeLogger()
        orchestrator = ProcessingOrchestrator(mock_discovery, mock_processor, logger)

        config = ProcessingConfig("src", "dst", batch_size=2)
        result = orchestrator.process_all(config)

        assert result["total_items"] == 2
        assert result["processed_count"] == 2
        assert result["error_count"] == 0
        assert result["processing_time"] > 0
        mock_processor.process_batch.assert_called_once()

    def test_process_all_with_errors(self):
        """Test processing with some errors."""
        mock_discovery = Mock()
        mock_discovery.discover_files.return_value = [
            "img1.jpg",
            "img2.jpg",
            "img3.jpg",
        ]

        mock_processor = Mock()
        mock_processor.process_batch.return_value = [
            ProcessingResult(source_key="img1.jpg", success=True),
            ProcessingResult(source_key="img2.jpg", success=False, error="Failed"),
            ProcessingResult(source_key="img3.jpg", success=True),
        ]

        logger = FakeLogger()
        orchestrator = ProcessingOrchestrator(mock_discovery, mock_processor, logger)

        config = ProcessingConfig("src", "dst", batch_size=10)
        result = orchestrator.process_all(config)

        assert result["total_items"] == 3
        assert result["processed_count"] == 2
        assert result["error_count"] == 1

    def test_process_all_multiple_batches(self):
        """Test processing with multiple batches."""
        mock_discovery = Mock()
        mock_discovery.discover_files.return_value = [
            "img1.jpg",
            "img2.jpg",
            "img3.jpg",
        ]

        mock_processor = Mock()
        mock_processor.process_batch.side_effect = [
            [
                ProcessingResult(source_key="img1.jpg", success=True),
                ProcessingResult(source_key="img2.jpg", success=True),
            ],
            [ProcessingResult(source_key="img3.jpg", success=True)],
        ]

        logger = FakeLogger()
        orchestrator = ProcessingOrchestrator(mock_discovery, mock_processor, logger)

        config = ProcessingConfig("src", "dst", batch_size=2)
        result = orchestrator.process_all(config)

        assert result["total_items"] == 3
        assert result["processed_count"] == 3
        assert result["error_count"] == 0
        assert mock_processor.process_batch.call_count == 2
