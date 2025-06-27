"""AsyncIO processor implementation - uses async/await for concurrent I/O."""

import asyncio
import io
from typing import List, Any

import aioboto3
from PIL import Image
# Conditional import for type checking S3 client
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client as AioS3Client  # For aioboto3
    from mypy_boto3_s3.client import S3Client # For boto3
else:
    AioS3Client = Any
    S3Client = Any

from ..core import (
    ImageItem,
    ProcessingConfig,
    ProcessingResult,
    get_logger,
)
from ..core.image_utils import (
    apply_transformation,
    extract_exif_data,
)


async def process_single_image_async(
    s3_client: AioS3Client, item: ImageItem, config: ProcessingConfig
) -> ProcessingResult:
    """
    Downloads, processes (EXIF extraction and optional transformation),
    and uploads a single image asynchronously using aioboto3.
    """
    logger = get_logger("asyncio-processor")
    result = ProcessingResult(source_key=item.source_key, dest_key=item.dest_key)

    try:
        # Step 1: Download image
        logger.debug(f"[{item.source_key}] Downloading image")
        response = await s3_client.get_object(
            Bucket=config.source_bucket, Key=item.source_key
        )

    async with response["Body"] as stream:  # response["Body"] is an AiobotocoreStreamingBody
        image_bytes = await stream.read()   # Asynchronously read all bytes from the S3 object stream

    # Step 2: Load and process image.
    # Note: Image.open and image.load are synchronous PIL operations.
    # For a truly non-blocking pipeline with CPU-bound image processing,
    # this would typically be run in a separate thread or process pool
    # using `asyncio.to_thread` (Python 3.9+) or `loop.run_in_executor`.
    # However, for this example, it's kept simple, and the primary async benefit
    # comes from concurrent S3 I/O operations.
        image_stream = io.BytesIO(image_bytes)
        image = Image.open(image_stream)
        image.load()

        logger.debug(
            f"[{item.source_key}] Loaded image: {image.size[0]}x{image.size[1]}"
        )

        # Step 3: Extract EXIF data
        exif_data = extract_exif_data(image)
        result.exif_data = exif_data

        # Step 4: Apply transformation
        if config.transformation:
            image = apply_transformation(image, config.transformation)
            logger.debug(
                f"[{item.source_key}] Applied {config.transformation} transformation"
            )

        # Step 5: Upload processed image
        # Convert to bytes
        output_buffer = io.BytesIO()
        file_ext = item.source_key.lower().split(".")[-1]
        format_type = "JPEG" if file_ext in ["jpg", "jpeg"] else "PNG"

        image.save(output_buffer, format=format_type, quality=95)
        output_buffer.seek(0)

        # Upload to destination using shared client
        logger.debug(f"[{item.source_key}] Uploading to {item.dest_key}")
        await s3_client.put_object(
            Bucket=config.dest_bucket,
            Key=item.dest_key,
            Body=output_buffer.getvalue(),
            ContentType=f"image/{file_ext}",
        )

        result.success = True
        logger.debug(f"[{item.source_key}] Processing completed")

    except Exception as e:
        result.error = str(e)
        result.success = False
        logger.error(f"[{item.source_key}] Processing failed: {e}", exc_info=True)

    return result


async def process_batch_async(
    batch: List[ImageItem], config: ProcessingConfig
) -> List[ProcessingResult]:
    """Process a batch of images asynchronously using shared S3 client."""
    # Create shared S3 session and client
    session = aioboto3.Session()
    async with session.client("s3") as s3_client: # Create an S3 client session for this batch
        # Create a list of asyncio tasks, one for each image to be processed.
        # Each task will run the `process_single_image_async` coroutine.
        tasks = [process_single_image_async(s3_client, item, config) for item in batch]

        # Wait for all tasks to complete.
        # `asyncio.gather` runs all tasks concurrently.
        # `return_exceptions=True` means that if a task raises an exception,
        # the exception itself is returned as a result, rather than
        # `gather` immediately raising the first encountered exception.
        # This allows us to collect all results, whether successful or errors.
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to ProcessingResult objects for consistent result handling
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(
                    ProcessingResult(
                        source_key=batch[i].source_key,
                        dest_key=batch[i].dest_key,
                        success=False,
                        error=str(result),
                    )
                )
            else:
                processed_results.append(result)

        return processed_results


def process_batch(
    batch: List[ImageItem], config: ProcessingConfig, s3_client: S3Client  # s3_client is unused
) -> List[ProcessingResult]:
    """
    Process a batch of images using asyncio.

    This is the synchronous wrapper that runs the asynchronous
    `process_batch_async` function.

    Args:
        batch: A list of `ImageItem` objects to process.
        config: `ProcessingConfig` object with settings for the batch.
        s3_client: A Boto3 S3 client instance (currently unused in this asyncio wrapper,
                   as `process_batch_async` creates its own aioboto3 client).

    Returns:
        A list of `ProcessingResult` objects, one for each image processed.
    """
    # Run the async function in an event loop
    return asyncio.run(process_batch_async(batch, config))
