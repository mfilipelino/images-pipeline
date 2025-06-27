"""Multiprocess processor implementation - uses process pool for parallelism."""

import io
from typing import List, Tuple, Any
from concurrent.futures import ProcessPoolExecutor, as_completed

import boto3
from PIL import Image

# Conditional import for type checking S3 client
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = Any

from ..core import (
    ImageItem,
    ProcessingConfig,
    ProcessingResult,
    configure_multiprocessing_logging,
)
from ..core.image_utils import (
    apply_transformation,
    extract_exif_data,
)


def process_single_image_worker(
    args: Tuple[str, str, ProcessingConfig]
) -> ProcessingResult:
    """
    Worker function designed for use with a `ProcessPoolExecutor`.

    It takes a tuple containing the source key, destination key, and the
    processing configuration. It configures logging for the current process,
    then downloads, processes (EXIF, transform), and uploads an image.

    Args:
        args: A tuple `(source_key: str, dest_key: str, config: ProcessingConfig)`.

    Returns:
        A `ProcessingResult` object detailing the outcome.
    """
    source_key, dest_key, config = args

    # Configure logging for this multiprocessing worker
    configure_multiprocessing_logging()

    result = ProcessingResult(source_key=source_key, dest_key=dest_key)

    try:
        # Create S3 client in this process
        s3_client = boto3.client("s3")

        # Step 1: Download image
        response = s3_client.get_object(Bucket=config.source_bucket, Key=source_key)
        image_bytes = response["Body"].read()

        # Step 2: Load and process image
        image_stream = io.BytesIO(image_bytes)
        image = Image.open(image_stream)
        image.load()  # Force loading into memory

        # Step 3: Extract EXIF data
        exif_data = extract_exif_data(image)
        result.exif_data = exif_data

        # Step 4: Apply transformation
        if config.transformation:
            image = apply_transformation(image, config.transformation)

        # Step 5: Upload processed image
        # Convert to bytes
        output_buffer = io.BytesIO()
        file_ext = source_key.lower().split(".")[-1]
        format_type = "JPEG" if file_ext in ["jpg", "jpeg"] else "PNG"

        image.save(output_buffer, format=format_type, quality=95)
        output_buffer.seek(0)

        # Upload to destination
        s3_client.put_object(
            Bucket=config.dest_bucket,
            Key=dest_key,
            Body=output_buffer.getvalue(),
            ContentType=f"image/{file_ext}",
        )

        result.success = True

    except Exception as e:
        result.error = str(e)
        result.success = False

    return result


def process_batch(
    batch: List[ImageItem], config: ProcessingConfig, s3_client: S3Client  # s3_client is unused
) -> List[ProcessingResult]:
    """
    Processes a batch of images using a `ProcessPoolExecutor` for parallelism.

    Each image in the batch is processed by a separate worker process via
    the `process_single_image_worker` function.

    Args:
        batch: A list of `ImageItem` objects to process.
        config: `ProcessingConfig` object with settings for the batch.
        s3_client: A Boto3 S3 client instance (currently unused in this multiprocess
                   implementation, as each worker creates its own client).

    Returns:
        A list of `ProcessingResult` objects, one for each image processed.
    """
    # Prepare work items as tuples for the worker function
    work_items = [(item.source_key, item.dest_key, config) for item in batch]

    # Process using multiprocessing pool
    results = []
    max_workers = min(4, len(batch))  # Limit workers to avoid overwhelming system

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_item = {
            executor.submit(process_single_image_worker, work_item): batch[i]
            for i, work_item in enumerate(work_items)
        }

        # Collect results as they complete
        for future in as_completed(future_to_item):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                # Handle any unexpected errors
                item = future_to_item[future]
                result = ProcessingResult(
                    source_key=item.source_key,
                    dest_key=item.dest_key,
                    success=False,
                    error=str(e),
                )
                results.append(result)

    return results
