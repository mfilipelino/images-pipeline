"""Pure service implementations for image processing pipeline."""

import io
import time
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from PIL import Image

from .models import ImageItem, ProcessingConfig, ProcessingResult
from .protocols import (
    S3ClientProtocol,
    LoggerProtocol,
    FileDiscoveryService,
    ProcessingService,
    BatchProcessor,
)
from .image_utils import apply_transformation, extract_exif_data, calculate_dest_key
from .observability import LogContext, MetricsCollector


@dataclass
class ProcessingContext:
    """Context for image processing operations."""

    correlation_id: str
    start_time: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    log_context: LogContext = field(default_factory=LogContext)


class ImageProcessorService:
    """Pure image processing service with no I/O dependencies."""

    def apply_transformation(self, image_bytes: bytes, transformation: str) -> bytes:
        """Apply transformation to image bytes."""
        if not transformation:
            return image_bytes

        # Load image from bytes
        image_stream = io.BytesIO(image_bytes)
        image = Image.open(image_stream)
        image.load()

        # Apply transformation
        transformed_image = apply_transformation(image, transformation)

        # Convert back to bytes
        output_stream = io.BytesIO()

        # Determine format
        if transformation == "grayscale":
            format_type = "JPEG"
        else:
            format_type = image.format or "JPEG"

        transformed_image.save(output_stream, format=format_type, quality=95)
        return output_stream.getvalue()

    def extract_metadata(self, image_bytes: bytes) -> Dict[str, Any]:
        """Extract metadata from image bytes."""
        image_stream = io.BytesIO(image_bytes)
        image = Image.open(image_stream)
        image.load()

        return extract_exif_data(image)


class S3FileDiscoveryService(FileDiscoveryService):
    """Service for discovering JPEG files in S3."""

    def __init__(self, s3_client: S3ClientProtocol, logger: LoggerProtocol):
        self._s3_client = s3_client
        self._logger = logger

    def discover_files(self, bucket: str, prefix: str) -> List[str]:
        """Discover JPEG files in S3 bucket."""
        files = []

        list_prefix = prefix
        if prefix and not prefix.endswith("/"):
            list_prefix = prefix + "/"

        self._logger.debug(f"Discovering files in s3://{bucket}/{list_prefix}")

        try:
            paginator = self._s3_client.get_paginator("list_objects_v2")

            for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.lower().endswith((".jpg", ".jpeg")) and not key.endswith(
                        "/"
                    ):
                        files.append(key)

            self._logger.info(f"Found {len(files)} JPEG files")
            return files

        except Exception as e:
            self._logger.error(f"Error discovering files: {e}")
            # Check if this is a bucket not found error to maintain test compatibility
            if "not found" in str(e).lower():
                raise ValueError(f"Bucket {bucket} not found")
            raise


class ImageProcessingService(ProcessingService):
    """Service for processing individual images."""

    def __init__(
        self,
        s3_client: S3ClientProtocol,
        image_processor: ImageProcessorService,
        logger: LoggerProtocol,
        metrics_collector: Optional[MetricsCollector] = None,
    ):
        self._s3_client = s3_client
        self._image_processor = image_processor
        self._logger = logger
        self._metrics_collector = metrics_collector

    def process_image(
        self, item: ImageItem, config: ProcessingConfig
    ) -> ProcessingResult:
        """Process a single image with full error handling."""
        correlation_id = f"img_{item.source_key}_{int(time.time() * 1000)}"
        log_context = LogContext(
            correlation_id=correlation_id,
            operation="process_image",
            component="image_processing_service",
        ).with_metadata(
            source_key=item.source_key,
            dest_key=item.dest_key,
            transformation=config.transformation or "none",
        )

        context = ProcessingContext(
            correlation_id=correlation_id, log_context=log_context
        )

        result = ProcessingResult(source_key=item.source_key, dest_key=item.dest_key)

        try:
            # Download image
            download_context = log_context.with_operation("download_image")
            self._logger.debug("Downloading image", download_context)

            response = self._s3_client.get_object(
                Bucket=config.source_bucket, Key=item.source_key
            )
            image_bytes = response["Body"].read()

            # Extract metadata
            metadata_context = log_context.with_operation("extract_metadata")
            self._logger.debug("Extracting metadata", metadata_context)
            metadata = self._image_processor.extract_metadata(image_bytes)
            result.exif_data = metadata

            # Apply transformation
            processed_bytes = image_bytes
            if config.transformation:
                transform_context = log_context.with_operation("apply_transformation")
                self._logger.debug(
                    f"Applying {config.transformation}", transform_context
                )
                processed_bytes = self._image_processor.apply_transformation(
                    image_bytes, config.transformation
                )

            # Upload processed image
            upload_context = log_context.with_operation("upload_image")
            self._logger.debug("Uploading processed image", upload_context)

            # Determine content type
            file_ext = item.source_key.lower().split(".")[-1]
            content_type = "image/jpeg" if file_ext in ["jpg", "jpeg"] else "image/png"

            self._s3_client.put_object(
                Bucket=config.dest_bucket,
                Key=item.dest_key,
                Body=processed_bytes,
                ContentType=content_type,
            )

            result.success = True
            result.processing_time = time.time() - context.start_time

            self._logger.info(
                "Successfully processed image",
                log_context,
                processing_time_ms=result.processing_time * 1000,
            )

        except Exception as e:
            result.success = False
            result.error = str(e)
            result.processing_time = time.time() - context.start_time

            error_context = log_context.with_metadata(error=str(e))
            self._logger.error("Image processing failed", error_context)

        return result


class SerialBatchProcessor(BatchProcessor):
    """Serial batch processor implementation."""

    def __init__(self, processing_service: ProcessingService, logger: LoggerProtocol):
        self._processing_service = processing_service
        self._logger = logger

    def process_batch(
        self, items: List[ImageItem], config: ProcessingConfig
    ) -> List[ProcessingResult]:
        """Process batch of images serially."""
        results = []

        for item in items:
            result = self._processing_service.process_image(item, config)
            results.append(result)

        return results


class WorkItemFactory:
    """Factory for creating work items."""

    @staticmethod
    def create_work_items(
        source_files: List[str], config: ProcessingConfig
    ) -> List[ImageItem]:
        """Create work items from source files."""
        work_items = []

        for source_key in source_files:
            dest_key = calculate_dest_key(
                source_key, config.source_prefix, config.dest_prefix
            )
            work_items.append(ImageItem(source_key=source_key, dest_key=dest_key))

        return work_items


class ProcessingOrchestrator:
    """Main orchestrator for the processing pipeline."""

    def __init__(
        self,
        file_discovery: FileDiscoveryService,
        batch_processor: BatchProcessor,
        logger: LoggerProtocol,
    ):
        self._file_discovery = file_discovery
        self._batch_processor = batch_processor
        self._logger = logger

    def process_all(self, config: ProcessingConfig) -> Dict[str, Any]:
        """Process all images according to configuration."""
        start_time = time.time()

        # Discover files
        source_files = self._file_discovery.discover_files(
            config.source_bucket, config.source_prefix
        )

        if not source_files:
            self._logger.info("No files found to process")
            return {
                "total_items": 0,
                "processed_count": 0,
                "error_count": 0,
                "processing_time": 0.0,
            }

        # Create work items
        work_items = WorkItemFactory.create_work_items(source_files, config)

        # Process in batches
        total_processed = 0
        total_errors = 0

        for i in range(0, len(work_items), config.batch_size):
            batch = work_items[i : i + config.batch_size]

            self._logger.info(f"Processing batch {i // config.batch_size + 1}")

            results = self._batch_processor.process_batch(batch, config)

            # Count results
            for result in results:
                if result.success:
                    total_processed += 1
                else:
                    total_errors += 1

        total_time = time.time() - start_time

        return {
            "total_items": len(work_items),
            "processed_count": total_processed,
            "error_count": total_errors,
            "processing_time": total_time,
        }
