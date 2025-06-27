#!/usr/bin/env python3
"""
Unified S3 Image Processor CLI

Downloads images → Processes (EXIF/Transform) → Uploads to destination
Supports multiple concurrency strategies: serial, multithread, multiprocess, asyncio
"""

import sys
import argparse
from typing import Dict, Tuple, Callable, List, Any

from .core import ProcessingConfig, get_logger, ImageItem, ProcessingResult
# S3Client type for process_batch_fn, assuming it's boto3 client
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = Any

from .processors.common import run_processing
from .processors import (
    serial_process_batch,  # Callable[[List[ImageItem], ProcessingConfig, S3Client], List[ProcessingResult]]
    multithread_process_batch, # Callable[[List[ImageItem], ProcessingConfig, S3Client], List[ProcessingResult]]
    multiprocess_process_batch, # Callable[[List[ImageItem], ProcessingConfig, S3Client], List[ProcessingResult]]
    asyncio_process_batch,  # Callable[[List[ImageItem], ProcessingConfig, S3Client], List[ProcessingResult]]
)

# Define a precise type for the process_batch_fn
ProcessBatchFunction = Callable[
    [List[ImageItem], ProcessingConfig, S3Client], List[ProcessingResult]
]


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments for the S3 image processor.

    Defines arguments for source/destination S3 buckets, prefixes,
    image transformation, processing strategy, debug mode, and batch size.

    Returns:
        An `argparse.Namespace` object containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="S3 Image Processor with multiple concurrency strategies"
    )

    # Required arguments
    parser.add_argument("--source-bucket", required=True, help="Source S3 bucket")
    parser.add_argument("--dest-bucket", required=True, help="Destination S3 bucket")

    # Optional arguments
    parser.add_argument("--source-prefix", default="", help="Source S3 prefix")
    parser.add_argument("--dest-prefix", default="", help="Destination S3 prefix")
    parser.add_argument(
        "--transformation",
        type=str,
        default=None,
        choices=["grayscale", "kmeans", "native_kmeans"],
        help="Image transformation: 'grayscale', 'kmeans' (scikit-learn), 'native_kmeans' (DEPRECATED)",
    )
    parser.add_argument(
        "--processor",
        type=str,
        default="serial",
        choices=["serial", "multithread", "multiprocess", "asyncio"],
        help="Processing strategy to use (default: serial)",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Batch size for progress reporting"
    )

    return parser.parse_args()


def main() -> None:
    """
    Main entry point for the S3 image processing script.

    Parses arguments, initializes configuration, selects the appropriate
    processing strategy (serial, multithread, multiprocess, asyncio),
    and then calls `run_processing` to execute the image processing workflow.
    Handles KeyboardInterrupt and other exceptions gracefully.
    """
    try:
        logger = get_logger("processor")
        logger.info("Starting S3 Image Processor")
        args: argparse.Namespace = parse_args()

        # Initialize config
        config = ProcessingConfig(
            source_bucket=args.source_bucket,
            dest_bucket=args.dest_bucket,
            source_prefix=args.source_prefix,
            dest_prefix=args.dest_prefix,
            transformation=args.transformation,
            debug=args.debug,
            batch_size=args.batch_size,
        )

        # Set debug logging if requested
        if config.debug:
            import logging

            logger.setLevel(logging.DEBUG)
            # Also set root logger for other modules
            logging.getLogger().setLevel(logging.DEBUG)

        # Select processor based on argument
        # The S3Client type for the process_batch_fn is 'Any' here because
        # asyncio_process_batch internally uses aioboto3, while others use boto3.
        # run_processing itself is typed with S3Client (boto3) for its s3_client creation.
        # This specific processors dict accommodates this variance at the call site of process_batch_fn.
        processors: Dict[str, Tuple[str, ProcessBatchFunction]] = {
            "serial": ("Serial", serial_process_batch),
            "multithread": ("Multithreaded", multithread_process_batch),
            "multiprocess": ("Multiprocess", multiprocess_process_batch),
            "asyncio": ("AsyncIO", asyncio_process_batch), # asyncio_process_batch is ProcessBatchFunction, but its s3_client is unused
        }

        processor_name: str
        process_batch_fn: ProcessBatchFunction
        processor_name, process_batch_fn = processors[args.processor]

        # Run processing with selected strategy
        # Note: run_processing will create its own boto3 S3Client.
        # The process_batch_fn from 'processors' dict must be compatible.
        # For asyncio_process_batch, its own s3_client param is effectively ignored.
        run_processing(config, processor_name, process_batch_fn)

    except KeyboardInterrupt:
        logger = get_logger("processor")
        logger.warning("Processing interrupted by user.")
    except Exception as e:
        logger = get_logger("processor")
        logger.error(f"Processing failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
