"""Main module for the images pipeline CLI."""

import sys
import argparse
from typing import Any # For S3Client if needed by process_images_main indirectly

from .process_images import main as process_images_main
# process_images_main is effectively:
# Callable[[ProcessingConfig, str, Callable[[List[ImageItem], ProcessingConfig, Any], List[ProcessingResult]]], None]
# but we are calling it with no args, relying on its internal sys.argv parsing.

def main() -> None:
    """
    Entry point for the unified command-line interface (CLI) of the Images Pipeline.

    This function sets up an `ArgumentParser` to handle different commands
    (e.g., "process", "version"). For the "process" command, it gathers
    arguments and then reconstructs `sys.argv` to call the `main` function
    from `process_images.py`.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        prog="images-pipeline",
        description="Images Pipeline - S3 Image Processing with multiple concurrency strategies",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process images with default settings (serial)
  images-pipeline process --source-bucket my-source --dest-bucket my-dest
  
  # Use multiprocessing with transformations
  images-pipeline process --source-bucket my-source --dest-bucket my-dest \\
                          --processor multiprocess --transformation kmeans
  
  # Show version
  images-pipeline version
        """,
    )

    subparsers: argparse._SubParsersAction = parser.add_subparsers(
        dest="command", help="Available commands"
    )

    # Process subcommand
    process_parser: argparse.ArgumentParser = subparsers.add_parser(
        "process", help="Process images from S3 source to destination"
    )

    # Add all the process-images arguments to the process subcommand
    process_parser.add_argument(
        "--source-bucket", required=True, help="Source S3 bucket"
    )
    process_parser.add_argument(
        "--dest-bucket", required=True, help="Destination S3 bucket"
    )
    process_parser.add_argument("--source-prefix", default="", help="Source S3 prefix")
    process_parser.add_argument(
        "--dest-prefix", default="", help="Destination S3 prefix"
    )
    process_parser.add_argument(
        "--transformation",
        type=str,
        default=None,
        choices=["grayscale", "kmeans", "native_kmeans"],
        help="Image transformation: 'grayscale', 'kmeans' (scikit-learn), 'native_kmeans' (DEPRECATED)",
    )
    process_parser.add_argument(
        "--processor",
        type=str,
        default="serial",
        choices=["serial", "multithread", "multiprocess", "asyncio"],
        help="Processing strategy to use (default: serial)",
    )
    process_parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging"
    )
    process_parser.add_argument(
        "--batch-size", type=int, default=100, help="Batch size for progress reporting"
    )

    # Version subcommand
    subparsers.add_parser("version", help="Show version information")

    # Parse arguments
    args: argparse.Namespace = parser.parse_args()

    if args.command == "process":
        # --- sys.argv Manipulation for Sub-processing ---
        # The `process_images.py` script has its own ArgumentParser setup.
        # To call its `main()` function (`process_images_main`) as if it were
        # invoked from the command line with its own set of arguments, we
        # reconstruct `sys.argv`. The first element of `sys.argv` is typically
        # the script name, which `argparse` uses. The subsequent elements are
        # the arguments.
        #
        # This approach allows `process_images.py` to remain an independent script
        # while also being usable as a module driven by this unified CLI.
        sys.argv = [
            "process-images",  # Dummy script name for `process_images.parse_args()`
            "--source-bucket",
            args.source_bucket,
            "--dest-bucket",
            args.dest_bucket,
        ]
        if args.source_prefix:
            sys.argv.extend(["--source-prefix", args.source_prefix])
        if args.dest_prefix:
            sys.argv.extend(["--dest-prefix", args.dest_prefix])
        if args.transformation:
            sys.argv.extend(["--transformation", args.transformation])
        if args.processor != "serial":
            sys.argv.extend(["--processor", args.processor])
        if args.debug:
            sys.argv.append("--debug")
        if args.batch_size != 100:
            sys.argv.extend(["--batch-size", str(args.batch_size)])

        # Call the process_images main function
        process_images_main()

    elif args.command == "version":
        print("Images Pipeline CLI")
        print("Version 0.1.0")
        print("S3 Image Processing with multiple concurrency strategies")
        sys.exit(0)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
