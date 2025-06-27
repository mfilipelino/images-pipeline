"""Multiprocess processor implementation - uses process pool for parallelism."""

import io
from typing import List, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed

import boto3
from mypy_boto3_s3 import S3Client
from PIL import Image

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


def process_single_image_worker(args: Tuple[str, str, ProcessingConfig]) -> ProcessingResult:
    """
    Worker function for multiprocessing.
    Takes a tuple of (source_key, config) to work with multiprocessing.map.
    """
    source_key, dest_key, config = args

    # Configure logging for this multiprocessing worker
    configure_multiprocessing_logging()

    result = ProcessingResult(source_key=source_key, dest_key=dest_key)

    try:
        # Create S3 client in this process
        s3_client: S3Client = boto3.client("s3")  # type: ignore[reportUnknownMemberType]

        # Step 1: Download image
        response = s3_client.get_object(Bucket=config.source_bucket, Key=source_key)
        image_bytes = response["Body"].read()

        # Step 2: Load and process image
        image_stream = io.BytesIO(image_bytes)
        image = Image.open(image_stream)
        image.load()  # type: ignore[reportUnknownMemberType]

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
    batch: List[ImageItem], config: ProcessingConfig, s3_client: S3Client
) -> List[ProcessingResult]:
    """
    Process a batch of images using multiprocessing.

    Args:
        batch: List of image items to process
        config: Processing configuration
        s3_client: S3 client instance (not used in multiprocess)

    Returns:
        List of processing results
    """
    # Prepare work items as tuples for the worker function
    work_items = [(item.source_key, item.dest_key, config) for item in batch]

    # Process using multiprocessing pool
    results: List[ProcessingResult] = []
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
