"""Fake implementations for testing purposes."""

import io
import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from PIL import Image


@dataclass
class S3Object:
    """Fake S3 object for testing."""

    key: str
    body: bytes
    content_type: str = "image/jpeg"
    last_modified: Optional[str] = None
    size: int = 0

    def __post_init__(self):
        if self.size == 0:
            self.size = len(self.body)


@dataclass
class S3Bucket:
    """Fake S3 bucket for testing."""

    name: str
    objects: Dict[str, S3Object] = field(default_factory=dict)

    def add_object(
        self, key: str, body: bytes, content_type: str = "image/jpeg"
    ) -> None:
        """Add object to bucket."""
        self.objects[key] = S3Object(key=key, body=body, content_type=content_type)

    def get_object(self, key: str) -> Optional[S3Object]:
        """Get object from bucket."""
        return self.objects.get(key)

    def list_objects(self, prefix: str = "") -> List[S3Object]:
        """List objects with optional prefix filter."""
        return [obj for key, obj in self.objects.items() if key.startswith(prefix)]


class FakeS3Client:
    """Fake S3 client for testing."""

    def __init__(self):
        self.buckets: Dict[str, S3Bucket] = {}
        self.operation_count = 0
        self.should_fail = False
        self.failure_message = "Simulated S3 failure"
        self.delay_seconds = 0.0

    def create_bucket(self, name: str) -> S3Bucket:
        """Create a new bucket."""
        bucket = S3Bucket(name=name)
        self.buckets[name] = bucket
        return bucket

    def get_bucket(self, name: str) -> Optional[S3Bucket]:
        """Get bucket by name."""
        return self.buckets.get(name)

    def set_failure_mode(
        self, should_fail: bool, message: str = "Simulated failure"
    ) -> None:
        """Configure failure mode for testing error handling."""
        self.should_fail = should_fail
        self.failure_message = message

    def set_delay(self, seconds: float) -> None:
        """Set artificial delay for testing timeouts."""
        self.delay_seconds = seconds

    def get_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        """Get object from S3."""
        self.operation_count += 1

        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)

        if self.should_fail:
            raise Exception(self.failure_message)

        bucket = self.buckets.get(Bucket)
        if not bucket:
            raise Exception(f"Bucket {Bucket} not found")

        obj = bucket.get_object(Key)
        if not obj:
            raise Exception(f"Object {Key} not found in bucket {Bucket}")

        return {
            "Body": io.BytesIO(obj.body),
            "ContentType": obj.content_type,
            "ContentLength": obj.size,
        }

    def put_object(
        self, Bucket: str, Key: str, Body: bytes, ContentType: str
    ) -> Dict[str, Any]:
        """Put object to S3."""
        self.operation_count += 1

        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)

        if self.should_fail:
            raise Exception(self.failure_message)

        bucket = self.buckets.get(Bucket)
        if not bucket:
            raise Exception(f"Bucket {Bucket} not found")

        bucket.add_object(Key, Body, ContentType)

        return {
            "ETag": f'"fake-etag-{Key}"',
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }

    def get_paginator(self, operation_name: str) -> "FakeS3Paginator":
        """Get paginator for S3 operations."""
        return FakeS3Paginator(self, operation_name)


class FakeS3Paginator:
    """Fake S3 paginator for testing."""

    def __init__(self, s3_client: FakeS3Client, operation_name: str):
        self.s3_client = s3_client
        self.operation_name = operation_name

    def paginate(self, Bucket: str, Prefix: str = "") -> List[Dict[str, Any]]:
        """Paginate through objects."""
        if self.s3_client.should_fail:
            raise Exception(self.s3_client.failure_message)

        bucket = self.s3_client.buckets.get(Bucket)
        if not bucket:
            raise Exception(f"Bucket {Bucket} not found")

        objects = bucket.list_objects(Prefix)

        # Return single page for simplicity
        return [
            {
                "Contents": [
                    {
                        "Key": obj.key,
                        "Size": obj.size,
                        "LastModified": obj.last_modified or "2023-01-01T00:00:00.000Z",
                    }
                    for obj in objects
                ]
            }
        ]


class FakeLogger:
    """Fake logger for testing with support for LogContext."""

    def __init__(self, name: str = "test_logger"):
        self.name = name
        self.logs: List[Dict[str, Any]] = []
        self.should_fail = False

    def _log(
        self, level: str, message: str, context: Any = None, **kwargs: Any
    ) -> None:
        """Internal logging method with context support."""
        if self.should_fail:
            raise Exception("Simulated logging failure")

        log_entry = {
            "level": level,
            "message": message,
            "timestamp": time.time(),
            **kwargs,
        }

        # Handle LogContext if provided
        if context is not None:
            if hasattr(context, "correlation_id"):
                log_entry["correlation_id"] = context.correlation_id
            if hasattr(context, "operation"):
                log_entry["operation"] = context.operation
            if hasattr(context, "component"):
                log_entry["component"] = context.component
            if hasattr(context, "metadata"):
                log_entry.update(context.metadata)

        self.logs.append(log_entry)

    def debug(self, message: str, context: Any = None, **kwargs: Any) -> None:
        """Log debug message."""
        self._log("DEBUG", message, context, **kwargs)

    def info(self, message: str, context: Any = None, **kwargs: Any) -> None:
        """Log info message."""
        self._log("INFO", message, context, **kwargs)

    def warning(self, message: str, context: Any = None, **kwargs: Any) -> None:
        """Log warning message."""
        self._log("WARNING", message, context, **kwargs)

    def error(self, message: str, context: Any = None, **kwargs: Any) -> None:
        """Log error message."""
        self._log("ERROR", message, context, **kwargs)

    def get_logs(self, level: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get logged messages, optionally filtered by level."""
        if level:
            return [log for log in self.logs if log["level"] == level]
        return self.logs.copy()

    def clear_logs(self) -> None:
        """Clear all logged messages."""
        self.logs.clear()


def create_test_image(width: int = 100, height: int = 100, color: str = "RGB") -> bytes:
    """Create a test image in memory."""
    image = Image.new(color, (width, height), color="red")

    # Add some pattern to make it more realistic
    for x in range(0, width, 20):
        for y in range(0, height, 20):
            if (x + y) % 40 == 0:
                # Add some blue pixels
                for i in range(min(10, width - x)):
                    for j in range(min(10, height - y)):
                        if x + i < width and y + j < height:
                            image.putpixel((x + i, y + j), (0, 0, 255))

    # Convert to bytes
    img_bytes = io.BytesIO()
    image.save(img_bytes, format="JPEG", quality=95)
    return img_bytes.getvalue()


def setup_test_s3_environment() -> FakeS3Client:
    """Set up a test S3 environment with sample data."""
    s3_client = FakeS3Client()

    # Create source bucket with test images
    source_bucket = s3_client.create_bucket("test-source")
    source_bucket.add_object("images/photo1.jpg", create_test_image(200, 150))
    source_bucket.add_object("images/photo2.jpg", create_test_image(300, 200))
    source_bucket.add_object("images/photo3.jpg", create_test_image(150, 100))
    source_bucket.add_object("images/subfolder/photo4.jpg", create_test_image(250, 175))

    # Add some non-image files to test filtering
    source_bucket.add_object("images/readme.txt", b"This is not an image")
    source_bucket.add_object("images/config.json", b'{"setting": "value"}')

    # Create destination bucket
    s3_client.create_bucket("test-dest")

    return s3_client
