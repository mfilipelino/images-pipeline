"""Tests for fake implementations to ensure they work correctly."""

import pytest

from images_pipeline.testing.fakes import (
    FakeS3Client,
    FakeLogger,
    S3Object,
    S3Bucket,
    create_test_image,
    setup_test_s3_environment,
)


class TestFakeS3Client:
    """Tests for FakeS3Client to ensure it behaves correctly."""

    def test_create_bucket(self):
        """Test bucket creation."""
        client = FakeS3Client()

        bucket = client.create_bucket("test-bucket")

        assert bucket.name == "test-bucket"
        assert len(bucket.objects) == 0
        assert client.get_bucket("test-bucket") is bucket

    def test_get_nonexistent_bucket(self):
        """Test getting nonexistent bucket."""
        client = FakeS3Client()

        bucket = client.get_bucket("nonexistent")

        assert bucket is None

    def test_get_object_success(self):
        """Test successful object retrieval."""
        client = FakeS3Client()
        bucket = client.create_bucket("test-bucket")
        test_data = b"test image data"
        bucket.add_object("test.jpg", test_data)

        response = client.get_object(Bucket="test-bucket", Key="test.jpg")

        assert response["Body"].read() == test_data
        assert response["ContentType"] == "image/jpeg"
        assert response["ContentLength"] == len(test_data)

    def test_get_object_not_found(self):
        """Test getting nonexistent object."""
        client = FakeS3Client()
        client.create_bucket("test-bucket")

        with pytest.raises(Exception, match="not found"):
            client.get_object(Bucket="test-bucket", Key="nonexistent.jpg")

    def test_get_object_bucket_not_found(self):
        """Test getting object from nonexistent bucket."""
        client = FakeS3Client()

        with pytest.raises(Exception, match="Bucket.*not found"):
            client.get_object(Bucket="nonexistent", Key="test.jpg")

    def test_put_object_success(self):
        """Test successful object upload."""
        client = FakeS3Client()
        client.create_bucket("test-bucket")
        test_data = b"test image data"

        response = client.put_object(
            Bucket="test-bucket",
            Key="test.jpg",
            Body=test_data,
            ContentType="image/jpeg",
        )

        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ETag" in response

        # Verify object was stored
        bucket = client.get_bucket("test-bucket")
        obj = bucket.get_object("test.jpg")
        assert obj is not None
        assert obj.body == test_data

    def test_put_object_bucket_not_found(self):
        """Test putting object to nonexistent bucket."""
        client = FakeS3Client()

        with pytest.raises(Exception, match="Bucket.*not found"):
            client.put_object(
                Bucket="nonexistent",
                Key="test.jpg",
                Body=b"data",
                ContentType="image/jpeg",
            )

    def test_failure_mode(self):
        """Test failure mode configuration."""
        client = FakeS3Client()
        client.create_bucket("test-bucket")

        # Enable failure mode
        client.set_failure_mode(True, "Custom error message")

        # All operations should fail
        with pytest.raises(Exception, match="Custom error message"):
            client.get_object(Bucket="test-bucket", Key="test.jpg")

        with pytest.raises(Exception, match="Custom error message"):
            client.put_object(
                Bucket="test-bucket",
                Key="test.jpg",
                Body=b"data",
                ContentType="image/jpeg",
            )

    def test_operation_count_tracking(self):
        """Test that operations are counted correctly."""
        client = FakeS3Client()
        bucket = client.create_bucket("test-bucket")
        bucket.add_object("test.jpg", b"test data")

        assert client.operation_count == 0

        client.get_object(Bucket="test-bucket", Key="test.jpg")
        assert client.operation_count == 1

        client.put_object(
            Bucket="test-bucket",
            Key="test2.jpg",
            Body=b"data",
            ContentType="image/jpeg",
        )
        assert client.operation_count == 2

    def test_delay_simulation(self):
        """Test delay simulation for timeout testing."""
        import time

        client = FakeS3Client()
        bucket = client.create_bucket("test-bucket")
        bucket.add_object("test.jpg", b"test data")

        # Set a small delay
        client.set_delay(0.01)

        start_time = time.time()
        client.get_object(Bucket="test-bucket", Key="test.jpg")
        elapsed = time.time() - start_time

        assert elapsed >= 0.01


class TestFakeS3Paginator:
    """Tests for FakeS3Paginator."""

    def test_paginate_success(self):
        """Test successful pagination."""
        client = FakeS3Client()
        bucket = client.create_bucket("test-bucket")
        bucket.add_object("images/photo1.jpg", b"data1")
        bucket.add_object("images/photo2.jpg", b"data2")
        bucket.add_object("docs/readme.txt", b"text")

        paginator = client.get_paginator("list_objects_v2")
        pages = list(paginator.paginate(Bucket="test-bucket", Prefix="images"))

        assert len(pages) == 1
        contents = pages[0]["Contents"]
        assert len(contents) == 2
        assert any(obj["Key"] == "images/photo1.jpg" for obj in contents)
        assert any(obj["Key"] == "images/photo2.jpg" for obj in contents)

    def test_paginate_empty_bucket(self):
        """Test pagination on empty bucket."""
        client = FakeS3Client()
        client.create_bucket("empty-bucket")

        paginator = client.get_paginator("list_objects_v2")
        pages = list(paginator.paginate(Bucket="empty-bucket", Prefix=""))

        assert len(pages) == 1
        assert pages[0]["Contents"] == []

    def test_paginate_nonexistent_bucket(self):
        """Test pagination on nonexistent bucket."""
        client = FakeS3Client()

        paginator = client.get_paginator("list_objects_v2")

        with pytest.raises(Exception, match="Bucket.*not found"):
            list(paginator.paginate(Bucket="nonexistent", Prefix=""))

    def test_paginate_with_failure_mode(self):
        """Test pagination with failure mode enabled."""
        client = FakeS3Client()
        client.create_bucket("test-bucket")
        client.set_failure_mode(True, "Paginator error")

        paginator = client.get_paginator("list_objects_v2")

        with pytest.raises(Exception, match="Paginator error"):
            list(paginator.paginate(Bucket="test-bucket", Prefix=""))


class TestFakeLogger:
    """Tests for FakeLogger."""

    def test_logging_methods(self):
        """Test all logging methods."""
        logger = FakeLogger("test")

        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")

        logs = logger.get_logs()
        assert len(logs) == 4
        assert logs[0]["level"] == "DEBUG"
        assert logs[1]["level"] == "INFO"
        assert logs[2]["level"] == "WARNING"
        assert logs[3]["level"] == "ERROR"

    def test_log_filtering(self):
        """Test log filtering by level."""
        logger = FakeLogger("test")

        logger.debug("Debug message")
        logger.info("Info message 1")
        logger.info("Info message 2")
        logger.error("Error message")

        info_logs = logger.get_logs("INFO")
        assert len(info_logs) == 2
        assert all(log["level"] == "INFO" for log in info_logs)

    def test_log_clearing(self):
        """Test log clearing functionality."""
        logger = FakeLogger("test")

        logger.info("Test message")
        assert len(logger.get_logs()) == 1

        logger.clear_logs()
        assert len(logger.get_logs()) == 0

    def test_log_with_kwargs(self):
        """Test logging with additional kwargs."""
        logger = FakeLogger("test")

        logger.info("Test message", extra_field="value", correlation_id="123")

        logs = logger.get_logs()
        assert len(logs) == 1
        assert logs[0]["extra_field"] == "value"
        assert logs[0]["correlation_id"] == "123"

    def test_logger_failure_mode(self):
        """Test logger failure mode."""
        logger = FakeLogger("test")
        logger.should_fail = True

        with pytest.raises(Exception, match="Simulated logging failure"):
            logger.info("This should fail")


class TestS3Object:
    """Tests for S3Object."""

    def test_object_creation(self):
        """Test S3Object creation."""
        obj = S3Object(key="test.jpg", body=b"test data")

        assert obj.key == "test.jpg"
        assert obj.body == b"test data"
        assert obj.content_type == "image/jpeg"
        assert obj.size == len(b"test data")

    def test_object_creation_with_custom_content_type(self):
        """Test S3Object creation with custom content type."""
        obj = S3Object(key="test.png", body=b"png data", content_type="image/png")

        assert obj.content_type == "image/png"

    def test_object_size_calculation(self):
        """Test automatic size calculation."""
        test_data = b"test data with specific length"
        obj = S3Object(key="test.jpg", body=test_data)

        assert obj.size == len(test_data)

    def test_object_size_override(self):
        """Test explicit size override."""
        obj = S3Object(key="test.jpg", body=b"test", size=100)

        assert obj.size == 100  # Should use explicit size, not calculated


class TestS3Bucket:
    """Tests for S3Bucket."""

    def test_bucket_creation(self):
        """Test bucket creation."""
        bucket = S3Bucket(name="test-bucket")

        assert bucket.name == "test-bucket"
        assert len(bucket.objects) == 0

    def test_add_and_get_object(self):
        """Test adding and retrieving objects."""
        bucket = S3Bucket(name="test-bucket")

        bucket.add_object("test.jpg", b"test data")
        obj = bucket.get_object("test.jpg")

        assert obj is not None
        assert obj.key == "test.jpg"
        assert obj.body == b"test data"

    def test_get_nonexistent_object(self):
        """Test getting nonexistent object."""
        bucket = S3Bucket(name="test-bucket")

        obj = bucket.get_object("nonexistent.jpg")

        assert obj is None

    def test_list_objects_no_prefix(self):
        """Test listing all objects."""
        bucket = S3Bucket(name="test-bucket")
        bucket.add_object("photo1.jpg", b"data1")
        bucket.add_object("photo2.jpg", b"data2")
        bucket.add_object("docs/readme.txt", b"text")

        objects = bucket.list_objects("")

        assert len(objects) == 3
        keys = [obj.key for obj in objects]
        assert "photo1.jpg" in keys
        assert "photo2.jpg" in keys
        assert "docs/readme.txt" in keys

    def test_list_objects_with_prefix(self):
        """Test listing objects with prefix filter."""
        bucket = S3Bucket(name="test-bucket")
        bucket.add_object("images/photo1.jpg", b"data1")
        bucket.add_object("images/photo2.jpg", b"data2")
        bucket.add_object("docs/readme.txt", b"text")

        objects = bucket.list_objects("images/")

        assert len(objects) == 2
        keys = [obj.key for obj in objects]
        assert "images/photo1.jpg" in keys
        assert "images/photo2.jpg" in keys
        assert "docs/readme.txt" not in keys


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_create_test_image(self):
        """Test test image creation."""
        image_bytes = create_test_image(100, 150)

        assert isinstance(image_bytes, bytes)
        assert len(image_bytes) > 0

        # Verify it's a valid JPEG
        from PIL import Image
        import io

        img = Image.open(io.BytesIO(image_bytes))
        assert img.size == (100, 150)
        assert img.format == "JPEG"

    def test_create_test_image_different_sizes(self):
        """Test creating images with different sizes."""
        sizes = [(50, 50), (200, 100), (300, 400)]

        for width, height in sizes:
            image_bytes = create_test_image(width, height)

            from PIL import Image
            import io

            img = Image.open(io.BytesIO(image_bytes))
            assert img.size == (width, height)

    def test_setup_test_s3_environment(self):
        """Test the test environment setup."""
        client = setup_test_s3_environment()

        # Verify source bucket exists with correct files
        source_bucket = client.get_bucket("test-source")
        assert source_bucket is not None

        # Verify JPEG files exist
        jpeg_files = [
            obj
            for obj in source_bucket.list_objects("images/")
            if obj.key.endswith((".jpg", ".jpeg"))
        ]
        assert len(jpeg_files) == 4

        # Verify non-image files exist
        all_files = source_bucket.list_objects("images/")
        assert len(all_files) == 6  # 4 JPEG + 2 non-image

        # Verify destination bucket exists
        dest_bucket = client.get_bucket("test-dest")
        assert dest_bucket is not None
        assert len(dest_bucket.objects) == 0  # Should be empty initially
