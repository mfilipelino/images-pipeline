"""AsyncIO processor implementation - uses async/await for concurrent I/O."""

import asyncio
import io
from typing import List, Any

import aioboto3
from mypy_boto3_s3 import S3Client
from PIL import Image

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
    s3_client: Any, item: ImageItem, config: ProcessingConfig
) -> ProcessingResult:
    """Process a single image asynchronously."""
    logger = get_logger("asyncio-processor")
    result = ProcessingResult(source_key=item.source_key, dest_key=item.dest_key)

    try:
        # Step 1: Download image
        logger.debug(f"[{item.source_key}] Downloading image")
        response = await s3_client.get_object(
            Bucket=config.source_bucket, Key=item.source_key
        )

        async with response["Body"] as stream:
            image_bytes = await stream.read()

        # Step 2: Load and process image (CPU-bound, but we're doing it anyway)
        image_stream = io.BytesIO(image_bytes)
        image = Image.open(image_stream)
        image.load()  # type: ignore[reportUnknownMemberType]

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
    async with session.client("s3") as s3_client:  # type: ignore[reportUnknownMemberType,reportGeneralTypeIssues]
        # Process all items concurrently
        tasks = [process_single_image_async(s3_client, item, config) for item in batch]

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to ProcessingResult objects
        processed_results: List[ProcessingResult] = []
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
                processed_results.append(result)  # type: ignore[reportArgumentType]

        return processed_results


def process_batch(
    batch: List[ImageItem], config: ProcessingConfig, s3_client: S3Client
) -> List[ProcessingResult]:
    """
    Process a batch of images using asyncio.

    This is the synchronous wrapper that runs the async function.

    Args:
        batch: List of image items to process
        config: Processing configuration
        s3_client: S3 client instance (not used in async version)

    Returns:
        List of processing results
    """
    # Run the async function in an event loop
    return asyncio.run(process_batch_async(batch, config))
