"""Common functions shared across all processor implementations."""

import io
import time
from typing import List, Tuple

import boto3
from PIL import Image, UnidentifiedImageError as PILUnidentifiedImageError

from ..core import (
    ProcessingConfig,
    ImageItem,
    ProcessingResult,
    get_logger,
)
from ..core.exceptions import ImageProcessingError, S3Error, ConfigurationError
from ..core.error_handling import with_error_handling, retry_s3_operation, BatchOperationContextManager
from ..core.image_utils import (
    apply_transformation,
    calculate_dest_key,
    extract_exif_data,
)


@retry_s3_operation()
@with_error_handling
# @with_error_handling might be redundant if @retry_s3_operation already relies on S3Error from it,
# but it ensures any other unexpected error in the s3_client call itself is also standardized.
# The @with_error_handling decorator will turn BotocoreClientError into S3Error, which @retry_s3_operation expects.
def _download_s3_object(s3_client, bucket: str, key: str, source_key_for_logging: str) -> bytes:
    logger = get_logger("processor") # Assuming get_logger is available
    logger.debug(f"[{source_key_for_logging}] Downloading from s3://{bucket}/{key}")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()

@retry_s3_operation()
@with_error_handling
def _upload_s3_object(s3_client, bucket: str, key: str, data: bytes, content_type: str, source_key_for_logging: str):
    logger = get_logger("processor")
    logger.debug(f"[{source_key_for_logging}] Uploading to s3://{bucket}/{key}")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType=content_type
    )

@retry_s3_operation()
@with_error_handling
def list_jpeg_files(s3_client, bucket: str, prefix: str) -> List[str]:
    """List all JPEG files in S3 bucket/prefix."""
    logger = get_logger("processor")
    files = []
    list_prefix = prefix
    if prefix and not prefix.endswith("/"):
        list_prefix = prefix + "/"

    logger.debug(f"Listing JPEG files in s3://{bucket}/{list_prefix}")
    # Removed try/except, relying on decorators
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith((".jpg", ".jpeg")) and not key.endswith("/"):
                files.append(key)
    logger.info(f"Found {len(files)} JPEG files in s3://{bucket}/{list_prefix}")
    if not files:
        logger.warning(f"No JPEG files found in s3://{bucket}/{list_prefix} after listing.")
        # Consider if this should be an error or just an empty list.
        # The original discover_and_validate_files raises ValueError if no files found.
        # This function's role is just to list.
    return files


@with_error_handling # General wrapper for any other unexpected errors
def process_single_image(
    s3_client, item: ImageItem, config: ProcessingConfig
) -> ProcessingResult:
    """Process a single image: Download → EXIF → Transform → Upload."""
    logger = get_logger("processor")
    result = ProcessingResult(source_key=item.source_key, dest_key=item.dest_key)

    try:
        # Step 1: Download image using the new helper
        image_bytes = _download_s3_object(
            s3_client, config.source_bucket, item.source_key, item.source_key
        )

        # Step 2: Load image & Step 3: Extract EXIF & Step 4: Apply transformation
        try:
            image_stream = io.BytesIO(image_bytes)
            image = Image.open(image_stream)
            image.load() # Ensure image data is loaded
            logger.debug(
                f"[{item.source_key}] Image loaded. Size: {image.size[0]}x{image.size[1]}."
            )

            logger.debug(f"[{item.source_key}] Extracting EXIF data.")
            extract_exif_data(image) # Assuming this doesn't need specific error handling beyond what @with_error_handling on this func provides
                                     # Or if extract_exif_data itself is decorated.

            if config.transformation:
                logger.debug(
                    f"[{item.source_key}] Applying transformation: {config.transformation}."
                )
                # apply_transformation will be refactored later to raise ImageProcessingError
                image = apply_transformation(image, config.transformation)

        except (IOError, SyntaxError, Image.DecompressionBombError, PILUnidentifiedImageError) as img_err:
            # Catching common PIL errors explicitly, plus PILUnidentifiedImageError.
            # SyntaxError can happen with malformed image files.
            logger.error(f"[{item.source_key}] Image loading/processing failed: {img_err}", exc_info=True)
            raise ImageProcessingError(f"Image loading/processing error for {item.source_key}: {img_err}") from img_err


        # Step 5: Upload to destination using the new helper
        img_bytes_to_upload = io.BytesIO()
        file_ext = item.source_key.lower().split(".")[-1]
        format_type = "JPEG" if file_ext in ["jpg", "jpeg"] else "PNG" # Simplified

        image.save(img_bytes_to_upload, format=format_type, quality=95)
        img_bytes_to_upload.seek(0)

        _upload_s3_object(
            s3_client,
            config.dest_bucket,
            item.dest_key,
            img_bytes_to_upload.getvalue(),
            f"image/{file_ext}", # Corrected ContentType
            item.source_key
        )

        result.success = True
        logger.debug(f"[{item.source_key}] Processing completed successfully.")

    # Catch specific custom exceptions first, then broader ones if necessary
    except (S3Error, ImageProcessingError) as e: # Catch errors from helpers or image processing block
        result.error = str(e)
        result.success = False
        # The logger in @with_error_handling for _download/_upload or for process_single_image itself,
        # or the logger in the image processing try-except block above would have already logged details.
        # So, here we just log the fact that this specific item failed at a higher level.
        logger.error(f"[{item.source_key}] Failed processing due to {type(e).__name__}: {e}")

    # The @with_error_handling on process_single_image will catch any other Exception
    # and log it, then re-raise it. If it's a non-custom error,
    # it might propagate up and be caught by run_processing's main try-catch.
    # The goal of this specific try-except is to populate ProcessingResult correctly.

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


def discover_and_validate_files(s3_client, config: ProcessingConfig) -> List[str]:
    """
    Discover and validate JPEG files to process.

    Args:
        s3_client: S3 client instance
        config: Processing configuration

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
    s3_client,
    process_batch_fn, # e.g. serial_process_batch, multithread_process_batch
    processor_name_for_logging: str
) -> Tuple[int, int]:
    logger = get_logger("processor") # Ensure logger is available
    total_processed_items = 0
    total_error_items = 0
    # Use a more descriptive name for the operation if possible
    operation_display_name = f"Image Batch Processing via {processor_name_for_logging}"

    with BatchOperationContextManager(operation_name=operation_display_name) as batch_manager:
        num_batches = (len(work_items) + config.batch_size - 1) // config.batch_size
        logger.info(f"Starting processing of {len(work_items)} items in {num_batches} batches.")

        for i in range(0, len(work_items), config.batch_size):
            batch_items = work_items[i : i + config.batch_size]
            current_batch_number = (i // config.batch_size) + 1
            logger.info(f"Processing batch {current_batch_number}/{num_batches} with {len(batch_items)} items.")
            batch_start_time = time.time()

            # results is List[ProcessingResult]
            # process_batch_fn is, for example, serial_process_batch,
            # which calls process_single_image for each item in batch_items
            current_batch_results = process_batch_fn(batch_items, config, s3_client)

            batch_success_count = 0
            batch_fail_count = 0
            for res_item in current_batch_results:
                if res_item.success:
                    batch_success_count += 1
                else:
                    batch_fail_count += 1
                    # Ensure res_item.error has a meaningful message from process_single_image
                    error_msg_to_log = res_item.error if res_item.error else "Unknown error"
                    batch_manager.add_error(item_identifier=res_item.source_key, error_message=error_msg_to_log)

            total_processed_items += batch_success_count # These are successfully processed items
            total_error_items += batch_fail_count

            batch_duration = time.time() - batch_start_time
            # log_batch_progress logs cumulative success and errors so far
            log_batch_progress(
                i, # Start index of the batch for progress calculation
                len(batch_items),
                len(work_items),
                batch_duration,
                total_processed_items,
                total_error_items,
                config,
            )
            if batch_fail_count > 0:
                logger.info(f"Batch {current_batch_number}/{num_batches} summary: {batch_success_count} succeeded, {batch_fail_count} failed.")
            else:
                logger.info(f"Batch {current_batch_number}/{num_batches} summary: {batch_success_count} succeeded.")


    if total_error_items > 0:
        logger.warning(f"Completed all batches. Total items processed successfully: {total_processed_items}. Total errors: {total_error_items}.")
    else:
        logger.info(f"Completed all batches successfully. Total items processed: {total_processed_items}.")

    return total_processed_items, total_error_items


def run_processing(
    config: ProcessingConfig,
    processor_name: str,
    process_batch_fn, # This is the function like serial_process_batch, etc.
) -> None:
    logger = get_logger("processor") # Moved logger to the top
    log_configuration(config, processor_name)
    start_time = time.time()

    try:
        session = boto3.Session()
        s3_client = session.client("s3")

        # discover_and_validate_files itself raises ValueError if no files, handled below
        source_files = discover_and_validate_files(s3_client, config)
        if not source_files: # Defensive check, though discover_and_validate_files should raise
            logger.info("No JPEG files found. Exiting.") # Should be caught by ValueError below
            return

        work_items = create_work_items(source_files, config)
        logger.info(f"Processing {len(work_items)} images using {processor_name}...")

        # Call the refactored process_all_batches
        processed_count, error_count = process_all_batches(
            work_items, config, s3_client, process_batch_fn, processor_name
        )

        total_time = time.time() - start_time
        log_final_statistics(total_time, len(work_items), processed_count, error_count)

    except ValueError as ve: # Specifically from discover_and_validate_files
        logger.warning(f"Validation error: {ve}. Processing halted.")
        # No sys.exit(1) here, let main handle exit codes based on exceptions.
        # Or re-raise as ConfigurationError if appropriate for no files.
        # For now, matching existing behavior of returning.
        return
    except (S3Error, ImageProcessingError, ConfigurationError) as app_err:
        logger.error(f"Critical application error in {processor_name} processing: {app_err}", exc_info=True)
        raise # Re-raise for main to handle exit
    except Exception as e: # Catch-all for truly unexpected
        logger.error(f"Fatal unexpected error in {processor_name} processing: {e}", exc_info=True)
        raise # Re-raise for main to handle exit
