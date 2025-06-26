#!/usr/bin/env python3
"""
Simplified Multiprocess S3 Image Processor
Educational Multiprocessing implementation for image processing pipeline.
Download → Process (EXIF/Transform) → Upload
"""

import sys
import time
import logging
import argparse
from typing import Dict, List
from dataclasses import dataclass
from concurrent.futures import ProcessPoolExecutor, as_completed

import boto3
from PIL import Image

from src.images_pipeline.core.image_utils import (
    apply_transformation,
    calculate_dest_key,
    extract_exif_data,
)
from src.images_pipeline.core import ProcessingConfig, ProcessingResult


# Logging setup
def setup_logger(name: str = "s3-multiprocess-processor") -> logging.Logger:
    """Setup consistent logging across the application."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False
    return logger


logger = setup_logger()


# Data structures are now imported from core module


@dataclass
class S3ObjectInfo:
    """Information about an S3 object."""

    key: str
    etag: str
    last_modified: str
    size: int = 0


# Data structures and image processing functions now imported from core modules


# Image processing functions now imported from core.image_utils


# S3 operations - Modified for multiprocessing
def list_s3_objects_worker(bucket: str, prefix: str) -> Dict[str, S3ObjectInfo]:
    """List S3 objects - Worker function for multiprocessing."""
    objects = {}

    # Create S3 client in this process
    s3_client = boto3.client("s3")

    paginator = s3_client.get_paginator("list_objects_v2")

    list_prefix = prefix.rstrip("/") + "/" if prefix else ""

    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        for obj in page.get("Contents", []):
            # Skip directories and non-JPEG files
            key = obj["Key"]
            if key.endswith("/") or not key.lower().endswith((".jpg", ".jpeg")):
                continue

            objects[key] = S3ObjectInfo(
                key=key,
                etag=obj.get("ETag", "").strip('"'),
                last_modified=obj.get("LastModified", "").isoformat()
                if obj.get("LastModified")
                else "",
                size=obj.get("Size", 0),
            )

    return objects


# calculate_dest_key function now imported from core.image_utils


def process_single_image(source_key: str, config: ProcessingConfig) -> ProcessingResult:
    """Process a single image: Download → Process → Upload."""
    start_time = time.time()
    result = ProcessingResult(source_key=source_key)

    try:
        # Create S3 client in this process
        s3_client = boto3.client("s3")

        # Step 1: Download image
        response = s3_client.get_object(Bucket=config.source_bucket, Key=source_key)
        image_bytes = response["Body"].read()

        # Step 2: Load and process image
        import io

        image_stream = io.BytesIO(image_bytes)
        image = Image.open(image_stream)
        image.load()  # Force loading into memory

        # Step 3: Extract EXIF data (always enabled for learning)
        exif_data = extract_exif_data(image)
        result.exif_data = exif_data

        # Step 4: Apply transformation
        processed_image = image
        if config.transformation:
            processed_image = apply_transformation(image, config.transformation)

        # Step 5: Upload processed image
        dest_key = calculate_dest_key(
            source_key, config.source_prefix, config.dest_prefix
        )

        # Convert to bytes
        output_buffer = io.BytesIO()
        file_ext = source_key.lower().split(".")[-1]
        format_type = "JPEG" if file_ext in ["jpg", "jpeg"] else "PNG"

        processed_image.save(output_buffer, format=format_type, quality=95)
        output_buffer.seek(0)

        # Upload to destination
        s3_client.put_object(
            Bucket=config.dest_bucket,
            Key=dest_key,
            Body=output_buffer.getvalue(),
            ContentType=f"image/{file_ext}",
        )

        result.success = True
        result.dest_key = dest_key
        result.processing_time = time.time() - start_time

    except Exception as e:
        result.error = str(e)
        result.processing_time = time.time() - start_time

    return result


def create_work_list(
    source_objects: Dict[str, S3ObjectInfo],
    dest_objects: Dict[str, S3ObjectInfo],
    config: ProcessingConfig,
) -> List[str]:
    """Create list of source keys that need processing."""
    work_list = []

    for source_key, source_info in source_objects.items():
        dest_key = calculate_dest_key(
            source_key, config.source_prefix, config.dest_prefix
        )
        dest_info = dest_objects.get(dest_key)

        # Process if destination doesn't exist or source has changed
        needs_processing = dest_info is None or source_info.etag != dest_info.etag

        if needs_processing:
            work_list.append(source_key)

    return work_list


def run_processing(config: ProcessingConfig):
    """Main processing function."""
    logger.info("=" * 80)
    logger.info("SIMPLIFIED MULTIPROCESS S3 IMAGE PROCESSOR")
    logger.info("=" * 80)

    logger.info("CONFIGURATION:")
    logger.info(f"  Source:        s3://{config.source_bucket}/{config.source_prefix}")
    logger.info(f"  Destination:   s3://{config.dest_bucket}/{config.dest_prefix}")
    logger.info("  EXIF:          ENABLED (always extracted for learning)")
    logger.info(f"  Transform:     {config.transformation or 'None'}")
    logger.info(f"  Concurrency:   {config.concurrency} processes")
    logger.info("")

    start_time = time.time()

    try:
        # Phase 1: Discovery
        logger.info("Phase 1: Discovering images...")

        # List source and destination objects concurrently using multiprocessing
        with ProcessPoolExecutor(max_workers=2) as discovery_executor:
            source_future = discovery_executor.submit(
                list_s3_objects_worker, config.source_bucket, config.source_prefix
            )
            dest_future = discovery_executor.submit(
                list_s3_objects_worker, config.dest_bucket, config.dest_prefix
            )

            source_objects = source_future.result()
            dest_objects = dest_future.result()

        # Create work list
        work_list = create_work_list(source_objects, dest_objects, config)

        if not work_list:
            logger.info("No images need processing. Exiting.")
            return

        logger.info(f"Found {len(work_list)} images to process")

        # Phase 2: Process images
        logger.info("Phase 2: Processing images...")

        # Process in batches for progress reporting
        success_count = 0
        error_count = 0

        with ProcessPoolExecutor(max_workers=config.concurrency) as executor:
            for i in range(0, len(work_list), config.batch_size):
                batch = work_list[i : i + config.batch_size]
                batch_start = time.time()

                # Submit batch to process pool
                future_to_key = {
                    executor.submit(process_single_image, key, config): key
                    for key in batch
                }

                # Collect results as they complete
                batch_results = []
                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        result = future.result()
                        batch_results.append(result)
                        if result.success:
                            success_count += 1
                        else:
                            error_count += 1
                    except Exception as exc:
                        error_count += 1
                        logger.error(f"[{key}] Unexpected error: {exc}")

                # Progress report
                total_processed = i + len(batch)
                progress = (total_processed / len(work_list)) * 100
                batch_time = time.time() - batch_start
                rate = len(batch) / batch_time if batch_time > 0 else 0

                operations = ["EXIF"]  # Always extract EXIF
                if config.transformation:
                    operations.append(config.transformation.upper())
                operation_str = "+".join(operations)

                logger.info(
                    f"Progress: {total_processed}/{len(work_list)} ({progress:.1f}%) | "
                    f"Rate: {rate:.1f} img/sec ({operation_str}) | "
                    f"Success: {success_count}, Errors: {error_count}"
                )

        # Final summary
        total_time = time.time() - start_time
        overall_rate = len(work_list) / total_time if total_time > 0 else 0

        logger.info("=" * 80)
        logger.info("PROCESSING COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Total time:     {total_time:.1f}s")
        logger.info(f"Overall rate:   {overall_rate:.1f} images/sec")
        logger.info(f"Success:        {success_count}")
        logger.info(f"Errors:         {error_count}")
        logger.info(f"Concurrency:    {config.concurrency} processes")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Simplified Multiprocess S3 Image Processor"
    )

    # Required arguments
    parser.add_argument("--source-bucket", required=True, help="Source S3 bucket")
    parser.add_argument("--dest-bucket", required=True, help="Destination S3 bucket")

    # Optional arguments
    parser.add_argument("--source-prefix", default="", help="Source S3 prefix")
    parser.add_argument("--dest-prefix", default="", help="Destination S3 prefix")

    # Processing options
    parser.add_argument(
        "--transformation",
        choices=["grayscale", "kmeans", "native_kmeans"],
        help="Image transformation: grayscale, kmeans (sklearn), native_kmeans (pure Python)",
    )

    # Performance
    parser.add_argument(
        "--concurrency", type=int, default=20, help="Process concurrency limit"
    )
    parser.add_argument(
        "--batch-size", type=int, default=50, help="Batch size for progress reporting"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    return parser.parse_args()


def main():
    """Main entry point."""
    try:
        args = parse_args()
        config = ProcessingConfig(**vars(args))

        if config.debug:
            logger.setLevel(logging.DEBUG)

        run_processing(config)

    except KeyboardInterrupt:
        logger.warning("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
