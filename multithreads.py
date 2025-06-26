#!/usr/bin/env python3
"""
Simplified Threaded S3 Image Processor
Educational Threading implementation for image processing pipeline.
Download → Process (EXIF/Transform) → Upload
"""

import os
import sys
import time
import logging
import argparse
import random
from typing import Dict, List, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import boto3
import numpy as np
from sklearn.cluster import KMeans
from PIL import Image
from PIL.ExifTags import TAGS

from src.images_pipeline.core import ProcessingConfig, ProcessingResult


# Logging setup
def setup_logger(name: str = "s3-threaded-processor") -> logging.Logger:
    """Setup consistent logging across the application."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False
    return logger


logger = setup_logger()


# Data structures are now imported from core module


@dataclass
class S3ObjectInfo:
    """Information about an S3 object."""

    key: str
    etag: str
    last_modified: str
    size: int = 0


# Thread-local storage for boto3 clients
thread_local = threading.local()


def get_s3_client():
    """Get thread-local S3 client."""
    if not hasattr(thread_local, "s3_client"):
        thread_local.s3_client = boto3.client("s3")
    return thread_local.s3_client


# Native Python K-means implementation for educational purposes
def native_kmeans_quantize(img, k=8, max_iter=10):
    """
    Native Python K-means quantization implementation.
    Educational example of clustering without external libraries.
    """
    pixels = list(img.getdata())

    # Initialize random centroids
    centroids = random.sample(pixels, k)

    for iteration in range(max_iter):
        # Assignment step: assign each pixel to nearest centroid
        clusters = {i: [] for i in range(k)}
        for pixel in pixels:
            distances = [
                sum((pixel[d] - centroids[c][d]) ** 2 for d in range(3))
                for c in range(k)
            ]
            closest_centroid = distances.index(min(distances))
            clusters[closest_centroid].append(pixel)

        # Update step: recalculate centroids
        new_centroids = []
        for i in range(k):
            if clusters[i]:
                # Calculate mean of each color channel
                mean_color = tuple(
                    sum(pixel[channel] for pixel in clusters[i]) // len(clusters[i])
                    for channel in range(3)
                )
                new_centroids.append(mean_color)
            else:
                # Empty cluster - reinitialize randomly
                new_centroids.append(random.choice(pixels))

        # Check for convergence
        if new_centroids == centroids:
            logger.debug(f"K-means converged after {iteration + 1} iterations")
            break

        centroids = new_centroids

    # Create quantized image
    quantized_pixels = []
    for pixel in pixels:
        distances = [
            sum((pixel[d] - centroids[c][d]) ** 2 for d in range(3)) for c in range(k)
        ]
        closest_centroid = distances.index(min(distances))
        quantized_pixels.append(closest_centroid)

    # Create palette image
    palette = [color for centroid in centroids for color in centroid] + [0] * (
        768 - 3 * k
    )

    quantized_img = Image.new("P", img.size)
    quantized_img.putpalette(palette)
    quantized_img.putdata(quantized_pixels)

    return quantized_img.convert("RGB")


# Image processing functions
def apply_transformation(
    img: Image.Image, transformation: str, source_key: str
) -> Image.Image:
    """Apply selected image transformation."""
    logger.debug(f"[{source_key}] Applying {transformation} transformation")

    if transformation == "grayscale":
        return img.convert("L").convert("RGB")  # Convert back to RGB for consistency

    elif transformation == "kmeans":
        # Scikit-learn K-means
        arr = np.array(img).reshape(-1, 3)
        kmeans = KMeans(n_clusters=8, random_state=42, n_init=10).fit(arr)
        labels = kmeans.predict(arr)
        compressed = kmeans.cluster_centers_[labels].reshape(
            img.size[1], img.size[0], 3
        )
        return Image.fromarray(compressed.astype("uint8"))

    elif transformation == "native_kmeans":
        # Pure Python K-means for educational purposes
        return native_kmeans_quantize(img, k=8, max_iter=10)

    else:
        return img


def extract_exif_data(image: Image.Image, source_key: str) -> Dict[str, Any]:
    """Extract EXIF data from PIL Image object."""
    exif_data = {}

    try:
        # Get basic image info
        exif_data["width"], exif_data["height"] = image.size
        exif_data["format"] = image.format
        exif_data["mode"] = image.mode

        # Extract EXIF tags
        raw_exif = image._getexif()
        if raw_exif:
            for tag_id, value in raw_exif.items():
                tag_name = TAGS.get(tag_id, str(tag_id))

                # Skip GPS data for privacy
                if tag_name == "GPSInfo":
                    continue

                # Convert value to string for logging
                if isinstance(value, bytes):
                    try:
                        value = value.decode("utf-8", errors="replace")
                    except Exception:
                        value = str(value)
                elif isinstance(value, (list, tuple)):
                    value = str(value)

                exif_data[tag_name] = str(value) if value is not None else ""

            logger.debug(f"[{source_key}] Extracted {len(exif_data)} EXIF fields")
        else:
            logger.debug(f"[{source_key}] No EXIF data found")

    except Exception as e:
        logger.warning(f"[{source_key}] EXIF extraction failed: {e}")
        exif_data["exif_error"] = str(e)

    return exif_data


# S3 operations
def list_s3_objects(bucket: str, prefix: str) -> Dict[str, S3ObjectInfo]:
    """List S3 objects."""
    objects = {}
    s3_client = get_s3_client()

    paginator = s3_client.get_paginator("list_objects_v2")

    list_prefix = prefix.rstrip("/") + "/" if prefix else ""

    logger.debug(f"Listing objects in s3://{bucket}/{list_prefix}")

    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        for obj in page.get("Contents", []):
            # Skip directories and non-JPEG files
            key = obj["Key"]
            if key.endswith("/") or not key.lower().endswith((".jpg", ".jpeg")):
                continue

            objects[key] = S3ObjectInfo(
                key=key,
                etag=obj.get("ETag", "").strip('"'),
                last_modified=obj.get("LastModified", "").isoformat()
                if obj.get("LastModified")
                else "",
                size=obj.get("Size", 0),
            )

    logger.info(f"Found {len(objects)} JPEG images in s3://{bucket}/{list_prefix}")

    return objects


def calculate_dest_key(source_key: str, source_prefix: str, dest_prefix: str) -> str:
    """Calculate destination key from source key and prefixes."""
    if source_prefix and source_key.startswith(source_prefix):
        relative_key = source_key[len(source_prefix) :].lstrip("/")
    else:
        relative_key = os.path.basename(source_key)

    if dest_prefix:
        return f"{dest_prefix.rstrip('/')}/{relative_key}"
    return relative_key


def process_single_image(source_key: str, config: ProcessingConfig) -> ProcessingResult:
    """Process a single image: Download → Process → Upload."""
    start_time = time.time()
    result = ProcessingResult(source_key=source_key)

    try:
        s3_client = get_s3_client()

        # Step 1: Download image
        logger.debug(f"[{source_key}] Downloading image")
        response = s3_client.get_object(Bucket=config.source_bucket, Key=source_key)
        image_bytes = response["Body"].read()

        # Step 2: Load and process image
        import io

        image_stream = io.BytesIO(image_bytes)
        image = Image.open(image_stream)
        image.load()  # Force loading into memory

        logger.debug(f"[{source_key}] Loaded image: {image.size[0]}x{image.size[1]}")

        # Step 3: Extract EXIF data (always enabled for learning)
        exif_data = extract_exif_data(image, source_key)
        result.exif_data = exif_data

        # Log some interesting EXIF data for educational purposes
        interesting_fields = [
            "Make",
            "Model",
            "DateTime",
            "ExposureTime",
            "FNumber",
            "ISOSpeedRatings",
        ]
        exif_info = []
        for field in interesting_fields:
            if field in exif_data and exif_data[field]:
                exif_info.append(f"{field}: {exif_data[field]}")

        if exif_info:
            logger.debug(f"[{source_key}] EXIF - {', '.join(exif_info[:3])}")

        # Step 4: Apply transformation
        processed_image = image
        if config.transformation:
            processed_image = apply_transformation(
                image, config.transformation, source_key
            )
            logger.debug(
                f"[{source_key}] Applied {config.transformation} transformation"
            )

        # Step 5: Upload processed image
        dest_key = calculate_dest_key(
            source_key, config.source_prefix, config.dest_prefix
        )

        # Convert to bytes
        output_buffer = io.BytesIO()
        file_ext = source_key.lower().split(".")[-1]
        format_type = "JPEG" if file_ext in ["jpg", "jpeg"] else "PNG"

        processed_image.save(output_buffer, format=format_type, quality=95)
        output_buffer.seek(0)

        # Upload to destination
        logger.debug(f"[{source_key}] Uploading to {dest_key}")
        s3_client.put_object(
            Bucket=config.dest_bucket,
            Key=dest_key,
            Body=output_buffer.getvalue(),
            ContentType=f"image/{file_ext}",
        )

        result.success = True
        result.dest_key = dest_key
        result.processing_time = time.time() - start_time

        logger.debug(
            f"[{source_key}] Processing completed in {result.processing_time:.2f}s"
        )

    except Exception as e:
        result.error = str(e)
        result.processing_time = time.time() - start_time
        logger.error(f"[{source_key}] Processing failed: {e}")

    return result


def create_work_list(
    source_objects: Dict[str, S3ObjectInfo],
    dest_objects: Dict[str, S3ObjectInfo],
    config: ProcessingConfig,
) -> List[str]:
    """Create list of source keys that need processing."""
    work_list = []

    for source_key, source_info in source_objects.items():
        dest_key = calculate_dest_key(
            source_key, config.source_prefix, config.dest_prefix
        )
        dest_info = dest_objects.get(dest_key)

        # Process if destination doesn't exist or source has changed
        needs_processing = dest_info is None or source_info.etag != dest_info.etag

        if needs_processing:
            work_list.append(source_key)
        else:
            logger.debug(f"[{source_key}] Already processed, skipping")

    return work_list


def run_processing(config: ProcessingConfig):
    """Main processing function."""
    logger.info("=" * 80)
    logger.info("SIMPLIFIED THREADED S3 IMAGE PROCESSOR")
    logger.info("=" * 80)

    logger.info("CONFIGURATION:")
    logger.info(f"  Source:        s3://{config.source_bucket}/{config.source_prefix}")
    logger.info(f"  Destination:   s3://{config.dest_bucket}/{config.dest_prefix}")
    logger.info("  EXIF:          ENABLED (always extracted for learning)")
    logger.info(f"  Transform:     {config.transformation or 'None'}")
    logger.info(f"  Concurrency:   {config.concurrency}")
    logger.info("")

    start_time = time.time()

    try:
        # Phase 1: Discovery
        logger.info("Phase 1: Discovering images...")

        # List source and destination objects concurrently
        with ThreadPoolExecutor(max_workers=2) as discovery_executor:
            source_future = discovery_executor.submit(
                list_s3_objects, config.source_bucket, config.source_prefix
            )
            dest_future = discovery_executor.submit(
                list_s3_objects, config.dest_bucket, config.dest_prefix
            )

            source_objects = source_future.result()
            dest_objects = dest_future.result()

        # Create work list
        work_list = create_work_list(source_objects, dest_objects, config)

        if not work_list:
            logger.info("No images need processing. Exiting.")
            return

        logger.info(f"Found {len(work_list)} images to process")

        # Phase 2: Process images
        logger.info("Phase 2: Processing images...")

        # Process in batches for progress reporting
        success_count = 0
        error_count = 0

        with ThreadPoolExecutor(max_workers=config.concurrency) as executor:
            for i in range(0, len(work_list), config.batch_size):
                batch = work_list[i : i + config.batch_size]
                batch_start = time.time()

                # Submit batch to thread pool
                future_to_key = {
                    executor.submit(process_single_image, key, config): key
                    for key in batch
                }

                # Collect results as they complete
                batch_results = []
                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        result = future.result()
                        batch_results.append(result)
                        if result.success:
                            success_count += 1
                        else:
                            error_count += 1
                    except Exception as exc:
                        error_count += 1
                        logger.error(f"[{key}] Unexpected error: {exc}")

                # Progress report
                total_processed = i + len(batch)
                progress = (total_processed / len(work_list)) * 100
                batch_time = time.time() - batch_start
                rate = len(batch) / batch_time if batch_time > 0 else 0

                operations = ["EXIF"]  # Always extract EXIF
                if config.transformation:
                    operations.append(config.transformation.upper())
                operation_str = "+".join(operations)

                logger.info(
                    f"Progress: {total_processed}/{len(work_list)} ({progress:.1f}%) | "
                    f"Rate: {rate:.1f} img/sec ({operation_str}) | "
                    f"Success: {success_count}, Errors: {error_count}"
                )

        # Final summary
        total_time = time.time() - start_time
        overall_rate = len(work_list) / total_time if total_time > 0 else 0

        logger.info("=" * 80)
        logger.info("PROCESSING COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Total time:     {total_time:.1f}s")
        logger.info(f"Overall rate:   {overall_rate:.1f} images/sec")
        logger.info(f"Success:        {success_count}")
        logger.info(f"Errors:         {error_count}")
        logger.info(f"Concurrency:    {config.concurrency} threads")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Simplified Threaded S3 Image Processor"
    )

    # Required arguments
    parser.add_argument("--source-bucket", required=True, help="Source S3 bucket")
    parser.add_argument("--dest-bucket", required=True, help="Destination S3 bucket")

    # Optional arguments
    parser.add_argument("--source-prefix", default="", help="Source S3 prefix")
    parser.add_argument("--dest-prefix", default="", help="Destination S3 prefix")

    # Processing options
    parser.add_argument(
        "--transformation",
        choices=["grayscale", "kmeans", "native_kmeans"],
        help="Image transformation: grayscale, kmeans (sklearn), native_kmeans (pure Python)",
    )

    # Performance
    parser.add_argument(
        "--concurrency", type=int, default=20, help="Thread concurrency limit"
    )
    parser.add_argument(
        "--batch-size", type=int, default=50, help="Batch size for progress reporting"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    return parser.parse_args()


def main():
    """Main entry point."""
    try:
        args = parse_args()
        config = ProcessingConfig(**vars(args))

        if config.debug:
            logger.setLevel(logging.DEBUG)

        run_processing(config)

    except KeyboardInterrupt:
        logger.warning("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Processing failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
