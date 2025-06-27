"""Serial processor implementation - processes images one by one."""

from typing import List, Any

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
    Processes a batch of images serially, one by one, in the current thread.

    It iterates through each `ImageItem` in the batch and calls the
    `process_single_image` function from `common.py` for each one.

    Args:
        batch: A list of `ImageItem` objects to process.
        config: `ProcessingConfig` object with settings for the batch.
        s3_client: A Boto3 S3 client instance.

    Returns:
        A list of `ProcessingResult` objects, one for each image processed.
    """
    results = []

    for item in batch:
        result = process_single_image(s3_client, item, config)
        results.append(result)

    return results
