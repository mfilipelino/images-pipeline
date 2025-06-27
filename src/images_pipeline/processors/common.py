"""Common functions shared across all processor implementations."""

import io
import time
from typing import List, Tuple, Callable, Any

import boto3
from PIL import Image

# Conditional import for type checking S3 client
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = Any

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


def list_jpeg_files(s3_client: S3Client, bucket: str, prefix: str) -> List[str]:
    """
    Lists all JPEG files ('.jpg', '.jpeg') in a specified S3 bucket and prefix.

    Args:
        s3_client: A Boto3 S3 client instance.
        bucket: The name of the S3 bucket.
        prefix: The S3 prefix (folder) to search within.

    Returns:
        A list of S3 keys for the found JPEG files.

    Raises:
        Exception: If there's an error listing files from S3.
    """
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
    s3_client: S3Client, item: ImageItem, config: ProcessingConfig
) -> ProcessingResult:
    """
    Processes a single image: downloads from S3, extracts EXIF data,
    applies an optional transformation, and uploads the result to S3.

    Args:
        s3_client: A Boto3 S3 client instance.
        item: An `ImageItem` dataclass instance containing source and destination keys.
        config: A `ProcessingConfig` dataclass instance with processing settings.

    Returns:
        A `ProcessingResult` dataclass instance with the outcome of the processing.
    """
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
    processed_count: int, # Cumulative processed count up to this batch
    error_count: int,   # Cumulative error count up to this batch
    config: ProcessingConfig,
):
    """Log progress for batch processing."""
    logger = get_logger("processor")
    # Calculate how many items have been processed so far in total (including current batch)
    # batch_index is the 0-based index of the first item in the current batch.
    items_processed_so_far = batch_index + batch_size
    progress = (items_processed_so_far / total_items) * 100
    rate = batch_size / batch_time if batch_time > 0 else 0  # Items per second for current batch

    operation_details = ["EXIF"]
    if config.transformation:
        if config.transformation == "native_kmeans":
            operation_details.append("Native K-means")
        else:
            operation_details.append(f"{config.transformation.title()}")
    operation_str = " + ".join(operation_details)

    logger.info(
        f"Progress: {items_processed_so_far}/{total_items} ({progress:.1f}%) - "
        f"Batch Rate: {rate:.1f} items/sec ({operation_str}) - "
        f"Cumulative Success: {processed_count}, Cumulative Errors: {error_count}"
    )


def discover_and_validate_files(s3_client: S3Client, config: ProcessingConfig) -> List[str]:
    """
    Discovers and validates JPEG files to process from the source S3 location.

    Args:
        s3_client: A Boto3 S3 client instance.
        config: `ProcessingConfig` object with source bucket and prefix.

    Returns:
        List of source file keys

    Raises:
        ValueError: If no files found
    """
    logger = get_logger("processor")
    logger.info("Discovering JPEG files to process...")

    source_files = list_jpeg_files(
        s3_client, config.source_bucket, config.source_prefix
    )

    if not source_files:
        logger.info("No JPEG files found. Exiting.")
        raise ValueError("No JPEG files found to process")

    return source_files


def count_batch_results(results: List[ProcessingResult]) -> Tuple[int, int]:
    """
    Count successful and failed results in a batch.

    Args:
        results: List of processing results

    Returns:
        Tuple of (processed_count, error_count)
    """
    processed_count = 0
    error_count = 0

    for result in results:
        if result.success:
            processed_count += 1
        else:
            error_count += 1

    return processed_count, error_count


def process_all_batches(
    work_items: List[ImageItem],
    config: ProcessingConfig,
    s3_client: S3Client,
    process_batch_fn: Callable[
        [List[ImageItem], ProcessingConfig, S3Client], List[ProcessingResult]
    ],
) -> Tuple[int, int]:
    """
    Processes all work items in batches using the provided batch processing function.

    It iterates through the `work_items`, creates batches of `config.batch_size`,
    and calls `process_batch_fn` for each batch. It logs progress and
    accumulates total processed and error counts.

    Args:
        work_items: A list of `ImageItem` objects to be processed.
        config: `ProcessingConfig` object with settings like batch size.
        s3_client: A Boto3 S3 client instance, passed to `process_batch_fn`.
        process_batch_fn: A callable function that takes a batch of `ImageItem`s,
                          the `ProcessingConfig`, and an `S3Client`, and returns
                          a list of `ProcessingResult`s.

    Returns:
        A tuple containing the total number of successfully processed items
        and the total number of items that resulted in errors.
    """
    total_processed = 0
    total_errors = 0

    for i in range(0, len(work_items), config.batch_size):
        batch = work_items[i : i + config.batch_size]
        batch_start_time = time.time()

        # Process batch using provided function
        results = process_batch_fn(batch, config, s3_client)

        # Count results for this batch
        batch_processed, batch_errors = count_batch_results(results)
        total_processed += batch_processed
        total_errors += batch_errors

        # Progress reporting
        batch_time = time.time() - batch_start_time
        log_batch_progress(
            i,
            len(batch),
            len(work_items),
            batch_time,
            total_processed,
            total_errors,
            config,
        )

    return total_processed, total_errors


def run_processing(
    config: ProcessingConfig,
    processor_name: str,
    process_batch_fn: Callable[
        [List[ImageItem], ProcessingConfig, S3Client], List[ProcessingResult]
    ],
) -> None:
    """
    Main orchestrator for the image processing workflow.

    This function initializes logging, creates an S3 client, discovers files,
    creates work items, and then processes them in batches using the
    provided `process_batch_fn`. It logs configuration, progress, and
    final statistics.

    Args:
        config: `ProcessingConfig` object with all settings for the job.
        processor_name: A string name for the processor being used (e.g., "Serial",
                        "Multithread"), used for logging.
        process_batch_fn: A callable function responsible for processing a single
                          batch of images. It must match the signature:
                          `(batch: List[ImageItem], config: ProcessingConfig, s3_client: S3Client) -> List[ProcessingResult]`.
    """
    log_configuration(config, processor_name)
    start_time = time.time()

    try:
        # Create S3 client
        session = boto3.Session()
        s3_client = session.client("s3")

        # Discover and validate files
        try:
            source_files = discover_and_validate_files(s3_client, config)
        except ValueError:
            return  # No files found, exit gracefully

        # Create work items
        work_items = create_work_items(source_files, config)
        logger = get_logger("processor")
        logger.info(f"Processing {len(work_items)} images...")

        # Process all batches
        processed_count, error_count = process_all_batches(
            work_items, config, s3_client, process_batch_fn
        )

        # Final statistics
        total_time = time.time() - start_time
        log_final_statistics(total_time, len(work_items), processed_count, error_count)

    except Exception as e:
        logger = get_logger("processor")
        logger.error(f"Fatal error in processing: {e}", exc_info=True)
        raise
