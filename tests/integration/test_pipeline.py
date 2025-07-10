"""Integration tests for the complete pipeline."""

import pytest
from typing import Dict, Any

from images_pipeline.core.models import ProcessingConfig
from images_pipeline.core.factories import ProcessingPipelineFactory
from images_pipeline.testing.fakes import (
    FakeS3Client,
    FakeLogger,
    setup_test_s3_environment,
)


class TestPipelineIntegration:
    """Integration tests for the complete processing pipeline."""

    def test_end_to_end_processing_no_transformation(self):
        """Test complete pipeline without transformation."""
        # Setup
        fake_s3 = setup_test_s3_environment()
        logger = FakeLogger()

        # Create pipeline
        pipeline = ProcessingPipelineFactory.create_pipeline(
            s3_client=fake_s3, logger=logger
        )

        # Configure processing
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest",
            source_prefix="images",
            dest_prefix="processed",
            batch_size=2,
        )

        # Process
        result = pipeline.process_all(config)

        # Verify results
        assert result["total_items"] == 4
        assert result["processed_count"] == 4
        assert result["error_count"] == 0
        assert result["processing_time"] > 0

        # Verify files were processed
        dest_bucket = fake_s3.get_bucket("test-dest")
        assert dest_bucket.get_object("processed/photo1.jpg") is not None
        assert dest_bucket.get_object("processed/photo2.jpg") is not None
        assert dest_bucket.get_object("processed/photo3.jpg") is not None
        assert dest_bucket.get_object("processed/subfolder/photo4.jpg") is not None

    @pytest.mark.parametrize("transformation", ["grayscale", "kmeans"])
    def test_end_to_end_processing_with_transformation(self, transformation):
        """Test complete pipeline with different transformations."""
        # Setup
        fake_s3 = setup_test_s3_environment()
        logger = FakeLogger()

        # Create pipeline
        pipeline = ProcessingPipelineFactory.create_pipeline(
            s3_client=fake_s3, logger=logger
        )

        # Configure processing
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest",
            source_prefix="images",
            dest_prefix="processed",
            transformation=transformation,
            batch_size=3,
        )

        # Process
        result = pipeline.process_all(config)

        # Verify results
        assert result["total_items"] == 4
        assert result["processed_count"] == 4
        assert result["error_count"] == 0

        # Verify transformed files were created
        dest_bucket = fake_s3.get_bucket("test-dest")
        processed_image = dest_bucket.get_object("processed/photo1.jpg")
        assert processed_image is not None

        # Verify the processed image is different from the original
        source_bucket = fake_s3.get_bucket("test-source")
        original_image = source_bucket.get_object("images/photo1.jpg")
        assert processed_image.body != original_image.body

    def test_end_to_end_processing_with_partial_failure(self):
        """Test pipeline handling partial failures."""
        # Setup S3 environment
        fake_s3 = setup_test_s3_environment()
        logger = FakeLogger()

        # Create pipeline
        pipeline = ProcessingPipelineFactory.create_pipeline(
            s3_client=fake_s3, logger=logger
        )

        # Configure S3 to fail on specific operations
        original_get_object = fake_s3.get_object

        def selective_failure(Bucket: str, Key: str) -> Dict[str, Any]:
            if Key == "images/photo2.jpg":
                raise Exception("Simulated failure for photo2.jpg")
            return original_get_object(Bucket=Bucket, Key=Key)

        fake_s3.get_object = selective_failure

        # Configure processing
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest",
            source_prefix="images",
            dest_prefix="processed",
            batch_size=2,
        )

        # Process
        result = pipeline.process_all(config)

        # Verify results
        assert result["total_items"] == 4
        assert result["processed_count"] == 3  # 3 successful, 1 failed
        assert result["error_count"] == 1

        # Verify successful files were processed
        dest_bucket = fake_s3.get_bucket("test-dest")
        assert dest_bucket.get_object("processed/photo1.jpg") is not None
        assert dest_bucket.get_object("processed/photo3.jpg") is not None
        assert dest_bucket.get_object("processed/subfolder/photo4.jpg") is not None

        # Verify failed file was not processed
        assert dest_bucket.get_object("processed/photo2.jpg") is None

    def test_end_to_end_processing_empty_source(self):
        """Test pipeline with empty source bucket."""
        # Setup
        fake_s3 = FakeS3Client()
        fake_s3.create_bucket("empty-source")
        fake_s3.create_bucket("test-dest")

        logger = FakeLogger()

        # Create pipeline
        pipeline = ProcessingPipelineFactory.create_pipeline(
            s3_client=fake_s3, logger=logger
        )

        # Configure processing
        config = ProcessingConfig(
            source_bucket="empty-source",
            dest_bucket="test-dest",
            source_prefix="images",
            dest_prefix="processed",
        )

        # Process
        result = pipeline.process_all(config)

        # Verify results
        assert result["total_items"] == 0
        assert result["processed_count"] == 0
        assert result["error_count"] == 0

    def test_end_to_end_processing_with_logging(self):
        """Test that pipeline generates expected log messages."""
        # Setup
        fake_s3 = setup_test_s3_environment()
        logger = FakeLogger()

        # Create pipeline
        pipeline = ProcessingPipelineFactory.create_pipeline(
            s3_client=fake_s3, logger=logger
        )

        # Configure processing
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest",
            source_prefix="images",
            dest_prefix="processed",
            batch_size=2,
        )

        # Process
        result = pipeline.process_all(config)

        # Verify processing was successful
        assert result["processed_count"] == 4

        # Verify expected log messages
        info_logs = logger.get_logs("INFO")
        debug_logs = logger.get_logs("DEBUG")

        # Should have discovery and batch processing logs
        assert any("Found 4 JPEG files" in log["message"] for log in info_logs)
        assert any("Processing batch" in log["message"] for log in info_logs)

        # Should have detailed processing logs
        assert any("Downloading" in log["message"] for log in debug_logs)
        assert any("Extracting metadata" in log["message"] for log in debug_logs)
        assert any("Uploading" in log["message"] for log in debug_logs)

    def test_end_to_end_processing_different_batch_sizes(self):
        """Test pipeline with different batch sizes."""
        test_cases = [
            {"batch_size": 1, "expected_batches": 4},
            {"batch_size": 2, "expected_batches": 2},
            {"batch_size": 5, "expected_batches": 1},
            {"batch_size": 10, "expected_batches": 1},
        ]

        for test_case in test_cases:
            # Setup
            fake_s3 = setup_test_s3_environment()
            logger = FakeLogger()

            # Create pipeline
            pipeline = ProcessingPipelineFactory.create_pipeline(
                s3_client=fake_s3, logger=logger
            )

            # Configure processing
            config = ProcessingConfig(
                source_bucket="test-source",
                dest_bucket="test-dest",
                source_prefix="images",
                dest_prefix="processed",
                batch_size=test_case["batch_size"],
            )

            # Process
            result = pipeline.process_all(config)

            # Verify results
            assert result["total_items"] == 4
            assert result["processed_count"] == 4
            assert result["error_count"] == 0

            # Verify batch processing occurred correctly
            info_logs = logger.get_logs("INFO")
            batch_logs = [
                log for log in info_logs if "Processing batch" in log["message"]
            ]
            assert len(batch_logs) == test_case["expected_batches"]

    def test_end_to_end_processing_with_prefix_handling(self):
        """Test pipeline with different prefix configurations."""
        # Setup
        fake_s3 = setup_test_s3_environment()
        logger = FakeLogger()

        # Create pipeline
        pipeline = ProcessingPipelineFactory.create_pipeline(
            s3_client=fake_s3, logger=logger
        )

        # Test with nested prefixes
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest",
            source_prefix="images/subfolder",
            dest_prefix="output/processed",
            batch_size=5,
        )

        # Process
        result = pipeline.process_all(config)

        # Verify results
        assert result["total_items"] == 1  # Only photo4.jpg in subfolder
        assert result["processed_count"] == 1
        assert result["error_count"] == 0

        # Verify correct destination path
        dest_bucket = fake_s3.get_bucket("test-dest")
        assert dest_bucket.get_object("output/processed/photo4.jpg") is not None
