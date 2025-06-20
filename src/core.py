from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass


@dataclass
class PipelineConfig:
    """Configuration for the image processing pipeline."""

    mode: str
    debug: bool = False


def parse_args(mode: str, args: list[str] | None = None) -> PipelineConfig:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=f"{mode} images processing pipeline")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    parsed = parser.parse_args(args)
    return PipelineConfig(mode=mode, debug=parsed.debug)


def setup_logger(debug: bool) -> logging.Logger:
    """Setup a simple logger."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger("images-pipeline")


def run_pipeline(config: PipelineConfig) -> None:
    """Run the pipeline using the provided configuration."""
    logger = setup_logger(config.debug)
    logger.info("Running pipeline in %s mode", config.mode)
    # Placeholder for the actual processing implementation
    logger.info("Pipeline completed")
