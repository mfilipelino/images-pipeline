#!/usr/bin/env python3
"""
Unified S3 Image Processor CLI

Downloads images → Processes (EXIF/Transform) → Uploads to destination
Supports multiple concurrency strategies: serial, multithread, multiprocess, asyncio
"""

import sys
import argparse

from .core import ProcessingConfig, get_logger
from .processors.common import run_processing
from .processors import (
    serial_process_batch,
    multithread_process_batch,
    multiprocess_process_batch,
    asyncio_process_batch,
)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
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
    """Main entry point."""
    try:
        logger = get_logger("processor")
        logger.info("Starting S3 Image Processor")
        args = parse_args()

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
        processors = {
            "serial": ("Serial", serial_process_batch),
            "multithread": ("Multithreaded", multithread_process_batch),
            "multiprocess": ("Multiprocess", multiprocess_process_batch),
            "asyncio": ("AsyncIO", asyncio_process_batch),
        }

        processor_name, process_batch_fn = processors[args.processor]

        # Run processing with selected strategy
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
