"""Serial processor implementation - processes images one by one."""

from typing import List

from mypy_boto3_s3 import S3Client
from ..core import ImageItem, ProcessingConfig, ProcessingResult
from .common import process_single_image


def process_batch(
    batch: List[ImageItem], config: ProcessingConfig, s3_client: S3Client
) -> List[ProcessingResult]:
    """
    Process a batch of images serially.

    Args:
        batch: List of image items to process
        config: Processing configuration
        s3_client: S3 client instance

    Returns:
        List of processing results
    """
    results: List[ProcessingResult] = []

    for item in batch:
        result = process_single_image(s3_client, item, config)
        results.append(result)

    return results
