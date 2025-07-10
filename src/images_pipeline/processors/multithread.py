"""Multithreaded processor implementation - uses thread pool for parallelism."""

from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

from mypy_boto3_s3 import S3Client
from ..core import ImageItem, ProcessingConfig, ProcessingResult
from .common import process_single_image


def process_batch(
    batch: List[ImageItem], config: ProcessingConfig, s3_client: S3Client
) -> List[ProcessingResult]:
    """
    Process a batch of images using multithreading.

    Args:
        batch: List of image items to process
        config: Processing configuration
        s3_client: S3 client instance (thread-safe for reads)

    Returns:
        List of processing results
    """
    results: List[ProcessingResult] = []
    max_workers = min(
        8, len(batch)
    )  # More threads than processes since they're lighter

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_item = {
            executor.submit(process_single_image, s3_client, item, config): item
            for item in batch
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
