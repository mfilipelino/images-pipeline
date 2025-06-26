"""Common functions shared across all processor implementations."""

import io
import time
from typing import List

import boto3
from PIL import Image

from ..core import (
    ProcessingConfig,
    ImageItem,
    ProcessingResult,
    get_logger,
)
from ..core.image_utils import (
    apply_transformation,
    calculate_dest_key,
    extract_exif_data,
)


def list_jpeg_files(s3_client, bucket: str, prefix: str) -> List[str]:
    """List all JPEG files in S3 bucket/prefix."""
    logger = get_logger("processor")
    files = []

    list_prefix = prefix
    if prefix and not prefix.endswith("/"):
        list_prefix = prefix + "/"

    logger.debug(f"Listing JPEG files in s3://{bucket}/{list_prefix}")
    try:
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Only process JPEG files
                if key.lower().endswith((".jpg", ".jpeg")) and not key.endswith("/"):
                    files.append(key)

        logger.info(f"Found {len(files)} JPEG files in s3://{bucket}/{list_prefix}")
    except Exception as e:
        logger.error(f"Error listing files in s3://{bucket}/{prefix}: {e}")
        raise

    return files


def process_single_image(
    s3_client, item: ImageItem, config: ProcessingConfig
) -> ProcessingResult:
    """Process a single image: Download → EXIF → Transform → Upload."""
    logger = get_logger("processor")
    result = ProcessingResult(source_key=item.source_key, dest_key=item.dest_key)

    try:
        # Step 1: Download image
        logger.debug(f"[{item.source_key}] Downloading image.")
        response = s3_client.get_object(
            Bucket=config.source_bucket, Key=item.source_key
        )
        image_bytes = response["Body"].read()

        # Step 2: Load image
        image_stream = io.BytesIO(image_bytes)
        image = Image.open(image_stream)
        image.load()
        logger.debug(
            f"[{item.source_key}] Image loaded. Size: {image.size[0]}x{image.size[1]}."
        )

        # Step 3: Extract EXIF (but don't save it)
        logger.debug(f"[{item.source_key}] Extracting EXIF data.")
        exif_data = extract_exif_data(image)
        logger.debug(
            f"[{item.source_key}] EXIF extracted: {len(exif_data)} fields found."
        )

        # Step 4: Apply transformation if specified
        if config.transformation:
            logger.debug(
                f"[{item.source_key}] Applying transformation: {config.transformation}."
            )
            image = apply_transformation(image, config.transformation)

        # Step 5: Upload to destination
        img_bytes = io.BytesIO()

        # Determine format based on original file extension
        file_ext = item.source_key.lower().split(".")[-1]
        if file_ext in ["jpg", "jpeg"]:
            format_type = "JPEG"
        elif file_ext == "png":
            format_type = "PNG"
        else:
            format_type = "JPEG"

        image.save(img_bytes, format=format_type, quality=95)
        img_bytes.seek(0)

        logger.debug(f"[{item.source_key}] Uploading to {item.dest_key}.")
        s3_client.put_object(
            Bucket=config.dest_bucket,
            Key=item.dest_key,
            Body=img_bytes.getvalue(),
            ContentType=f"image/{file_ext}",
        )

        result.success = True
        logger.debug(f"[{item.source_key}] Processing completed successfully.")

    except Exception as e:
        result.error = str(e)
        result.success = False
        logger.error(f"[{item.source_key}] Processing failed: {e}", exc_info=True)

    return result


def create_work_items(
    source_files: List[str], config: ProcessingConfig
) -> List[ImageItem]:
    """Create work items from source file list."""
    work_items = []
    for source_key in source_files:
        dest_key = calculate_dest_key(
            source_key, config.source_prefix, config.dest_prefix
        )
        work_items.append(ImageItem(source_key=source_key, dest_key=dest_key))
    return work_items


def log_configuration(config: ProcessingConfig, processor_name: str):
    """Log processing configuration."""
    logger = get_logger("processor")
    logger.info("=" * 80)
    logger.info(f"{processor_name.upper()} S3 IMAGE PROCESSOR")
    logger.info("=" * 80)

    # Print configuration
    logger.info("CONFIGURATION:")
    logger.info(f"  Source:        s3://{config.source_bucket}/{config.source_prefix}")
    logger.info(f"  Destination:   s3://{config.dest_bucket}/{config.dest_prefix}")
    logger.info("")

    logger.info("PROCESSING OPTIONS:")
    transformation_name = "None"
    if config.transformation:
        if config.transformation == "native_kmeans":
            transformation_name = "K-means (DEPRECATED - Native Python)"
        else:
            transformation_name = config.transformation.replace("_", " ").title()
    logger.info(f"  Transformation: {transformation_name}")
    logger.info("  EXIF Extraction: Enabled (extracted but not saved)")
    logger.info(f"  Batch Size: {config.batch_size}")
    logger.info("=" * 80)


def log_final_statistics(
    total_time: float, total_items: int, processed_count: int, error_count: int
):
    """Log final processing statistics."""
    logger = get_logger("processor")
    overall_rate = total_items / total_time if total_time > 0 else 0

    logger.info("=" * 80)
    logger.info("PROCESSING COMPLETED")
    logger.info("=" * 80)
    logger.info(f"Total execution time: {total_time:.1f}s")
    logger.info(f"Overall processing rate: {overall_rate:.1f} items/sec")
    logger.info(f"Successfully processed: {processed_count}")
    logger.info(f"Errors encountered: {error_count}")
    logger.info("=" * 80)


def log_batch_progress(
    batch_index: int,
    batch_size: int,
    total_items: int,
    batch_time: float,
    processed_count: int,
    error_count: int,
    config: ProcessingConfig,
):
    """Log progress for batch processing."""
    logger = get_logger("processor")
    total_processed = batch_index + batch_size
    progress = (total_processed / total_items) * 100
    rate = batch_size / batch_time if batch_time > 0 else 0

    operation_details = ["EXIF"]
    if config.transformation:
        if config.transformation == "native_kmeans":
            operation_details.append("Native K-means")
        else:
            operation_details.append(f"{config.transformation.title()}")
    operation_str = " + ".join(operation_details)

    logger.info(
        f"Progress: {total_processed}/{total_items} ({progress:.1f}%) - "
        f"Rate: {rate:.1f} items/sec ({operation_str}) - "
        f"Success: {processed_count}, Errors: {error_count}"
    )


def run_processing(
    config: ProcessingConfig,
    processor_name: str,
    process_batch_fn,
) -> None:
    """
    Main processing function that orchestrates the workflow.

    Args:
        config: Processing configuration
        processor_name: Name of the processor for logging
        process_batch_fn: Function to process a batch of items
    """
    log_configuration(config, processor_name)
    start_time = time.time()

    try:
        # Create S3 client
        session = boto3.Session()
        s3_client = session.client("s3")

        # List all JPEG files to process
        logger = get_logger("processor")
        logger.info("Discovering JPEG files to process...")
        source_files = list_jpeg_files(
            s3_client, config.source_bucket, config.source_prefix
        )

        if not source_files:
            logger.info("No JPEG files found. Exiting.")
            return

        # Create work items
        work_items = create_work_items(source_files, config)
        logger.info(f"Processing {len(work_items)} images...")

        # Process images in batches
        processed_count = 0
        error_count = 0

        for i in range(0, len(work_items), config.batch_size):
            batch = work_items[i : i + config.batch_size]
            batch_start_time = time.time()

            # Process batch using provided function
            results = process_batch_fn(batch, config, s3_client)

            # Count results
            for result in results:
                if result.success:
                    processed_count += 1
                else:
                    error_count += 1

            # Progress reporting
            batch_time = time.time() - batch_start_time
            log_batch_progress(
                i,
                len(batch),
                len(work_items),
                batch_time,
                processed_count,
                error_count,
                config,
            )

        # Final statistics
        total_time = time.time() - start_time
        log_final_statistics(total_time, len(work_items), processed_count, error_count)

    except Exception as e:
        logger = get_logger("processor")
        logger.error(f"Fatal error in processing: {e}", exc_info=True)
        raise
