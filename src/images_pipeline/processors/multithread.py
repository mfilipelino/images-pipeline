"""Multithreaded processor implementation - uses thread pool for parallelism."""

from typing import List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Conditional import for type checking S3 client
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = Any

from ..core import ImageItem, ProcessingConfig, ProcessingResult
from .common import process_single_image


def process_batch(
    batch: List[ImageItem], config: ProcessingConfig, s3_client: S3Client
) -> List[ProcessingResult]:
    """
    Processes a batch of images using a `ThreadPoolExecutor` for parallelism.

    Each image in the batch is processed by a separate thread using the
    `process_single_image` function from `common.py`. The provided `s3_client`
    is shared among threads (Boto3 S3 client is generally thread-safe for
    read operations and session-level operations if sessions are per-thread,
    but here a single client instance is used).

    Args:
        batch: A list of `ImageItem` objects to process.
        config: `ProcessingConfig` object with settings for the batch.
        s3_client: A Boto3 S3 client instance, shared across threads.

    Returns:
        A list of `ProcessingResult` objects, one for each image processed.
    """
    results = []
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
