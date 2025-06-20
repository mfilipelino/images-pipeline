#!/usr/bin/env python3
"""
S3 Image Processor with Bulk Copy and Metadata Persistence using Ray.
Extracts EXIF metadata from JPEG images in S3, copies them,
and streams results.
Designed for local Ray execution and AWS Glue 5 Ray jobs.
"""

import boto3
import argparse
import json
import io
import os
import sys
import time
import types  # lightweight “namespace” helper (avoids defining an empty class)
from typing import Optional
from datetime import datetime, timezone
import logging
import ray  # Import Ray

IS_GLUE_ENVIRONMENT = "AWS_EXECUTION_ENV" in os.environ
LOG_FORMAT = "%(levelname)s - %(message)s"


def setup_logger(name="glue-ray-engine"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)  # Same stream Glue captures
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False
    return logger


logger = setup_logger()


# Define a comprehensive list of expected EXIF fields (excluding GPS)
EXPECTED_EXIF_FIELDS = [
    "DateTimeOriginal",
    "DateTimeDigitized",
    "DateTime",
    "Make",
    "Model",
    "Software",
    "LensModel",
    "BodySerialNumber",
    "SerialNumber",
    "ExposureTime",
    "FNumber",
    "ISOSpeedRatings",
    "FocalLength",
    "ExposureProgram",
    "MeteringMode",
    "Flash",
    "Orientation",
    "ResolutionUnit",
    "XResolution",
    "YResolution",
    "ColorSpace",
    "PixelXDimension",
    "PixelYDimension",
    "ImageWidth",
    "ImageLength",
    "ExifVersion",
    "ComponentsConfiguration",
    "CompressedBitsPerPixel",
    "ExposureBiasValue",
    "MaxApertureValue",
    "SubjectDistance",
    "LightSource",
    "FocalPlaneXResolution",
    "FocalPlaneYResolution",
    "FocalPlaneResolutionUnit",
    "CustomRendered",
    "ExposureMode",
    "WhiteBalance",
    "DigitalZoomRatio",
    "FocalLengthIn35mmFilm",
    "SceneCaptureType",
    "GainControl",
    "Contrast",
    "Saturation",
    "Sharpness",
    "SubjectDistanceRange",
    "MakerNote",
]


# Custom exception classes
class TransientError(Exception):
    pass


class PermanentError(Exception):
    pass


# ----------- RAY STREAMING METADATA WRITER ACTOR -----------
@ray.remote
class RayStreamingMetadataWriter:
    """
    Handles streaming metadata writes with S3 uploads as a Ray Actor.
    """

    def __init__(
        self,
        metadata_bucket: str,
        metadata_key: str,
        upload_interval: int = 30,
        batch_size: int = 50,
        debug_logging: bool = False,
    ):
        # Imports needed within the actor process
        import boto3 as actor_boto3  # Use a distinct name to avoid confusion
        import tempfile as actor_tempfile
        import os as actor_os
        import time as actor_time
        import logging as actor_logging
        import sys as actor_sys  # For logging handler

        self.metadata_bucket = metadata_bucket
        self.metadata_key = metadata_key
        self.upload_interval = upload_interval
        self.batch_size = batch_size

        # Setup logging within the actor
        self.logger = actor_logging.getLogger(f"RayMetadataWriter_{actor_os.getpid()}")
        if debug_logging:
            self.logger.setLevel(actor_logging.DEBUG)
        else:
            self.logger.setLevel(actor_logging.INFO)

        # Ensure handler is set up correctly for actor logs
        if not self.logger.hasHandlers():
            handler = actor_logging.StreamHandler(actor_sys.stdout)
            formatter = actor_logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.propagate = False

        self.temp_dir = actor_tempfile.mkdtemp(prefix="ray_s3_proc_")
        self.local_file_path = actor_os.path.join(
            self.temp_dir, "metadata_streaming.jsonl"
        )

        self.metadata_buffer = []  # Use list instead of queue
        self.total_written_current_run = 0
        self.total_uploaded_current_run = 0
        self.last_upload_time = actor_time.time()

        self.s3_client = actor_boto3.client("s3")

        self._prime_local_file_from_s3()
        self.logger.info(
            f"📝 Ray Streaming metadata writer initialized. Temp file: {self.local_file_path}, S3: s3://{self.metadata_bucket}/{self.metadata_key}"
        )

    def _prime_local_file_from_s3(self):
        """Downloads existing metadata from S3."""
        import os as actor_os  # Ensure os is available

        try:
            self.logger.info(
                f"Attempting to prime local metadata from s3://{self.metadata_bucket}/{self.metadata_key}"
            )
            response = self.s3_client.get_object(
                Bucket=self.metadata_bucket, Key=self.metadata_key
            )
            s3_content = response["Body"].read()
            if s3_content:
                with open(self.local_file_path, "wb") as f:
                    f.write(s3_content)
                if not s3_content.endswith(b"\n"):  # Ensure newline terminated
                    with open(self.local_file_path, "ab") as f:
                        f.write(b"\n")
                self.logger.info(
                    f"Successfully primed local file with existing S3 metadata (size: {len(s3_content)} bytes)."
                )
            else:
                self.logger.info(
                    "Existing metadata file on S3 is empty. Starting with a new local file."
                )
        except self.s3_client.exceptions.NoSuchKey:
            self.logger.info(
                f"No existing metadata file found at s3://{self.metadata_bucket}/{self.metadata_key}. Starting with a new local file."
            )
        except Exception as e:
            self.logger.error(
                f"Error priming local metadata from S3: {e}. Starting with a new local file.",
                exc_info=True,
            )

        if not actor_os.path.exists(self.local_file_path):  # Create if not primed
            open(self.local_file_path, "a").close()

    def add_record(self, metadata_record: dict):
        """Add a new metadata record."""
        self.metadata_buffer.append(metadata_record)
        if len(self.metadata_buffer) >= self.batch_size:
            self._write_batch_to_file()
        self._upload_if_needed()

    def _write_batch_to_file(self):
        """Appends the current buffer to the local file."""
        if not self.metadata_buffer:
            return
        import json as actor_json  # Ensure import

        batch_to_write = list(self.metadata_buffer)  # Copy buffer
        self.metadata_buffer.clear()  # Clear original buffer
        try:
            with open(self.local_file_path, "a", encoding="utf-8") as f:
                for record in batch_to_write:
                    f.write(actor_json.dumps(record, separators=(",", ":")) + "\n")
            self.total_written_current_run += len(batch_to_write)
            self.logger.debug(
                f"📝 Appended batch of {len(batch_to_write)} new records to local file (total new this run: {self.total_written_current_run})"
            )
        except Exception as e:
            self.logger.error(
                f"Error appending batch to local file: {e}", exc_info=True
            )
            # Decide on error handling: re-add to buffer, log and drop, etc.
            # For now, logging the error. Consider re-adding to buffer if critical.
            # self.metadata_buffer.extend(batch_to_write) # Example: re-add for retry

    def _upload_if_needed(self):
        """Upload to S3 if interval has passed."""
        import time as actor_time  # Ensure import

        current_time = actor_time.time()
        if current_time - self.last_upload_time >= self.upload_interval:
            if self.metadata_buffer:  # Write any remaining buffer before upload
                self._write_batch_to_file()
            self._upload_to_s3()
            self.last_upload_time = current_time

    def _upload_to_s3(self):
        """Upload current local file to S3."""
        import os as actor_os  # Ensure import

        if (
            not actor_os.path.exists(self.local_file_path)
            or actor_os.path.getsize(self.local_file_path) == 0
        ):
            self.logger.debug(
                f"Local metadata file {self.local_file_path} not found or empty for upload. Skipping."
            )
            return
        try:
            file_size = actor_os.path.getsize(self.local_file_path)
            self.logger.info(
                f"⬆️  Uploading cumulative metadata to S3 (local file size: {file_size} bytes, new records this run: {self.total_written_current_run})"
            )
            self.s3_client.upload_file(
                self.local_file_path, self.metadata_bucket, self.metadata_key
            )
            self.total_uploaded_current_run = self.total_written_current_run
            self.logger.info(
                f"✅ Successfully uploaded cumulative metadata to s3://{self.metadata_bucket}/{self.metadata_key}"
            )
        except Exception as e:
            self.logger.error(
                f"Error uploading cumulative metadata to S3: {e}", exc_info=True
            )

    def _cleanup(self):
        """Clean up temporary files."""
        import os as actor_os  # Ensure import

        try:
            if actor_os.path.exists(self.local_file_path):
                actor_os.remove(self.local_file_path)
            if actor_os.path.exists(self.temp_dir):
                actor_os.rmdir(self.temp_dir)  # Only if empty
            self.logger.debug(
                "Cleaned up temporary files used by RayStreamingMetadataWriter."
            )
        except Exception as e:
            self.logger.warning(
                f"Error cleaning up temporary files for RayStreamingMetadataWriter: {e}"
            )

    def stop_and_upload_final(self):
        """Stop, write remaining, upload final, and clean up."""
        self.logger.info(
            "🔄 Stopping RayStreamingMetadataWriter and uploading final cumulative results..."
        )
        if self.metadata_buffer:  # Write any last records
            self._write_batch_to_file()
        self._upload_to_s3()  # Perform final upload
        self._cleanup()
        self.logger.info("✅ RayStreamingMetadataWriter stopped.")
        return self.get_status()  # Return final status

    def get_status(self):
        """Get current status."""
        import time as actor_time  # Ensure import

        return {
            "total_written_current_run": self.total_written_current_run,
            "total_uploaded_current_run": self.total_uploaded_current_run,
            "buffer_size": len(self.metadata_buffer),
            "last_upload_time": actor_time.strftime(
                "%Y-%m-%d %H:%M:%S %Z", actor_time.localtime(self.last_upload_time)
            ),
        }


# --- UTILITY FUNCTIONS ---
def get_empty_exif_structure():
    exif_data = {f"exif_{field}": "" for field in EXPECTED_EXIF_FIELDS}
    exif_data["exif_extraction_error"] = ""
    return exif_data


def calculate_destination_key(source_key, source_prefix, dest_prefix):
    relative_source_key = source_key
    if source_prefix and source_key.startswith(source_prefix):
        relative_source_key = source_key[len(source_prefix) :]
    elif source_prefix and not source_key.startswith(
        source_prefix
    ):  # Handle case where prefix might not match due to leading /
        relative_source_key = os.path.basename(source_key)  # Fallback to basename
    elif not source_prefix:
        relative_source_key = os.path.basename(
            source_key
        )  # if no source prefix, use basename

    formatted_dest_prefix = dest_prefix.rstrip("/") + "/" if dest_prefix else ""
    return formatted_dest_prefix + relative_source_key.lstrip("/")


def get_metadata_file_key(args):
    metadata_prefix = (
        args.metadata_prefix.rstrip("/") + "/" if args.metadata_prefix else ""
    )
    output_path_segment = args.output_s3_path.lstrip("/")
    return f"{metadata_prefix}{output_path_segment.rstrip('/')}/metadata.jsonl"


# --- S3 OPERATIONS (Synchronous Boto3) ---
def list_s3_objects_details_sync(bucket_name, prefix):
    """Lists objects in an S3 path synchronously using boto3."""
    objects_details = {}
    logger.debug(f"Starting to list objects in s3://{bucket_name}/{prefix}")
    try:
        s3_client = boto3.client("s3")
        paginator = s3_client.get_paginator("list_objects_v2")
        # Ensure prefix for listing ends with a '/' if it's meant to be a folder and not empty
        list_prefix = prefix
        if prefix and not prefix.endswith("/"):
            list_prefix = prefix + "/"

        page_count = 0
        object_count = 0
        for page in paginator.paginate(Bucket=bucket_name, Prefix=list_prefix):
            page_count += 1
            logger.debug(
                f"Processing page {page_count} for s3://{bucket_name}/{list_prefix}"
            )
            for obj in page.get("Contents", []):
                object_count += 1
                # Skip directory markers (objects ending with '/' and size 0)
                if obj["Key"].endswith("/") and obj.get("Size", 0) == 0:
                    logger.debug(f"Skipping directory marker: {obj['Key']}")
                    continue
                objects_details[obj["Key"]] = {
                    "etag": obj.get("ETag", "").strip('"'),
                    "last_modified": obj.get("LastModified"),
                    "size": obj.get("Size", 0),
                    "VersionId": obj.get(
                        "VersionId"
                    ),  # Include if versioning is enabled
                }
        logger.debug(
            f"Finished listing for s3://{bucket_name}/{list_prefix}. Found {object_count} objects across {page_count} pages."
        )
    except Exception as e:
        logger.error(
            f"Error listing objects in s3://{bucket_name}/{list_prefix}: {e}",
            exc_info=True,
        )
        raise TransientError(f"S3 listing failed: {e}")
    return objects_details


def is_versioning_enabled(bucket):
    s3 = boto3.client("s3")
    logger.debug(f"Checking versioning status for bucket s3://{bucket}")
    try:
        versioning = s3.get_bucket_versioning(Bucket=bucket)
        status = versioning.get("Status")
        logger.info(f"Bucket s3://{bucket} versioning status: {status}")
        return status == "Enabled"
    except Exception as e:
        logger.error(
            f"Could not check versioning for bucket s3://{bucket}: {e}", exc_info=True
        )
        return False


# --- EXIF EXTRACTION (core logic) ---
def extract_exif_data_core(image_bytes, source_key="unknown"):
    """Core EXIF extraction logic, designed to be called by Ray task."""
    # Note: PIL imports are done inside the Ray task calling this
    exif_data = get_empty_exif_structure()
    try:
        # These imports are fine here if called by a Ray task that already imported them
        from PIL import Image, ExifTags

        image = Image.open(io.BytesIO(image_bytes))
        image.load()  # Ensure image data is loaded
        exif_data["exif_ImageWidth"], exif_data["exif_ImageLength"] = image.size
        exif_data_raw = image._getexif()  # Use _getexif() for raw EXIF data
        if exif_data_raw:
            logger.debug(f"[{source_key}] EXIF data found. Processing tags.")
            for tag, value in exif_data_raw.items():
                tag_name = ExifTags.TAGS.get(tag, tag)  # Get human-readable tag name
                if tag_name == "GPSInfo":
                    logger.debug(f"[{source_key}] Skipping GPSInfo tag as requested.")
                    continue
                try:
                    if isinstance(value, bytes):
                        try:
                            value = value.decode("utf-8", errors="replace")
                        except Exception:
                            value = repr(value)  # Fallback for non-decodable bytes
                    elif isinstance(value, (list, tuple)):
                        value = str(value)  # Convert sequences to string

                    if (
                        f"exif_{tag_name}" in exif_data
                        or tag_name in ExifTags.TAGS.values()
                    ):  # Check if known tag
                        exif_data[f"exif_{tag_name}"] = value
                    else:
                        logger.debug(
                            f"[{source_key}] Skipping unexpected EXIF tag: {tag_name}"
                        )
                except Exception as e_tag:
                    logger.warning(
                        f"[{source_key}] Error processing EXIF tag {tag_name}: {e_tag}"
                    )
        else:
            logger.info(f"[{source_key}] No EXIF data found in image.")
    except Image.UnidentifiedImageError:
        logger.error(f"[{source_key}] Cannot identify or open image.", exc_info=False)
        exif_data["exif_extraction_error"] = "Pillow could not identify or open image."
        raise PermanentError("Cannot identify image format")
    except Exception as e_main:
        logger.error(
            f"[{source_key}] Overall error during EXIF extraction: {e_main}",
            exc_info=True,
        )
        exif_data["exif_extraction_error"] = str(e_main)
        raise PermanentError(f"EXIF extraction failed: {e_main}")

    # Ensure all values are JSON serializable
    for key, value in exif_data.items():
        if not isinstance(value, (str, int, float, bool, type(None))):
            exif_data[key] = str(value)
        if value is None:
            exif_data[key] = ""
    return exif_data


# --- RAY TASKS ---
@ray.remote(num_cpus=0.1)  # I/O bound, low CPU requirement
def copy_single_file_ray(work_item, args_dict):
    """Ray task for copying a single file. args_dict is vars(args)."""
    import boto3 as task_boto3  # Import within task
    import os as task_os  # For calculate_destination_key if needed

    # Reconstruct args-like object or use dict directly
    class TaskArgs:
        def __init__(self, **entries):
            self.__dict__.update(entries)

    args = TaskArgs(**args_dict)

    source_key = work_item["source_key"]

    # Use a local version of calculate_destination_key or pass all necessary components
    # For simplicity, ensure calculate_destination_key is robust or redefine locally
    def _calculate_destination_key_local(src_key, src_prefix, dst_prefix):
        rel_src_key = src_key
        if src_prefix and src_key.startswith(src_prefix):
            rel_src_key = src_key[len(src_prefix) :]
        elif src_prefix and not src_key.startswith(src_prefix):
            rel_src_key = task_os.path.basename(src_key)
        elif not src_prefix:
            rel_src_key = task_os.path.basename(src_key)
        fmt_dst_prefix = dst_prefix.rstrip("/") + "/" if dst_prefix else ""
        return fmt_dst_prefix + rel_src_key.lstrip("/")

    dest_key = _calculate_destination_key_local(
        source_key, args.source_prefix, args.dest_prefix
    )

    try:
        s3_client = task_boto3.client("s3")
        copy_source = {"Bucket": args.source_bucket, "Key": source_key}
        if work_item.get("s3_source_version_id"):
            copy_source["VersionId"] = work_item["s3_source_version_id"]
        response = s3_client.copy_object(
            CopySource=copy_source, Bucket=args.dest_bucket, Key=dest_key
        )
        return {
            "success": True,
            "source_key": source_key,
            "dest_key": dest_key,
            "dest_etag": response.get("CopyObjectResult", {})
            .get("ETag", "")
            .strip('"'),
            "dest_version_id": response.get("VersionId", ""),
            "error": None,
        }
    except Exception as e:
        # Log error from main thread after getting results, or use actor for logging
        return {
            "success": False,
            "source_key": source_key,
            "dest_key": dest_key,
            "error": str(e),
            "args_source_bucket": args.source_bucket,
        }


@ray.remote(num_cpus=1)  # CPU bound, 1 CPU recommended per task
def download_and_extract_exif_ray(
    work_item, args_dict, source_bucket_versioning_enabled
):
    """Ray task to download and extract EXIF. args_dict is vars(args)."""
    import boto3 as task_boto3
    import io as task_io
    from PIL import Image as task_Image  # Import PIL within the task
    from PIL.ExifTags import TAGS as task_TAGS  # Import TAGS within the task
    import logging as task_logging
    import sys as task_sys

    # Re-configure logging within the task if needed, or rely on Ray's capture
    task_logger = task_logging.getLogger(f"EXIF_Task_{os.getpid()}")
    if not task_logger.hasHandlers():
        handler = task_logging.StreamHandler(task_sys.stdout)
        formatter = task_logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        task_logger.addHandler(handler)
        task_logger.setLevel(task_logging.INFO)  # Or DEBUG based on args

    # Reconstruct args-like object
    class TaskArgs:
        def __init__(self, **entries):
            self.__dict__.update(entries)

    args = TaskArgs(**args_dict)

    source_key = work_item["source_key"]

    # --- Local Core EXIF Extraction (to avoid global issues) ---
    def _extract_exif_data_core_local(image_bytes, source_key="unknown"):
        """Core EXIF extraction logic, self-contained for the task."""
        exif_data_local = get_empty_exif_structure()
        try:
            image = task_Image.open(task_io.BytesIO(image_bytes))
            image.load()
            exif_data_local["exif_ImageWidth"], exif_data_local["exif_ImageLength"] = (
                image.size
            )
            exif_data_raw = image._getexif()
            if exif_data_raw:
                for tag, value in exif_data_raw.items():
                    tag_name = task_TAGS.get(tag, tag)
                    if tag_name == "GPSInfo":
                        continue
                    try:
                        if isinstance(value, bytes):
                            value = value.decode("utf-8", errors="replace")
                        elif isinstance(value, (list, tuple)):
                            value = str(value)
                        if f"exif_{tag_name}" in exif_data_local:
                            exif_data_local[f"exif_{tag_name}"] = value
                    except Exception:
                        pass  # Simplified error handling inside loop
            else:
                task_logger.info(f"[{source_key}] No EXIF data found.")
        except task_Image.UnidentifiedImageError:
            exif_data_local["exif_extraction_error"] = (
                "Pillow could not identify image."
            )
            raise PermanentError("Cannot identify image format")
        except Exception as e_main:
            exif_data_local["exif_extraction_error"] = str(e_main)
            raise PermanentError(f"EXIF extraction failed: {e_main}")
        for key, value in exif_data_local.items():
            if not isinstance(value, (str, int, float, bool, type(None))):
                exif_data_local[key] = str(value)
            if value is None:
                exif_data_local[key] = ""
        return exif_data_local

    # --- End Local Core EXIF ---

    try:
        s3_client = task_boto3.client("s3")
        get_params = {"Bucket": args.source_bucket, "Key": source_key}
        if source_bucket_versioning_enabled and work_item.get("s3_source_version_id"):
            get_params["VersionId"] = work_item["s3_source_version_id"]

        obj = s3_client.get_object(**get_params)
        image_bytes = obj["Body"].read()

        # Call the LOCAL EXIF logic
        exif_data = _extract_exif_data_core_local(image_bytes, source_key)
        return {"source_key": source_key, "exif": exif_data, "error": None}
    except PermanentError as pe:  # Catch permanent errors from extraction
        return {
            "source_key": source_key,
            "exif": get_empty_exif_structure(),
            "error": f"Permanent EXIF Error: {pe}",
        }
    except Exception as e:
        return {
            "source_key": source_key,
            "exif": get_empty_exif_structure(),
            "error": str(e),
        }


# --- METADATA LOADING ---
def load_previous_metadata(bucket, key_path):
    s3 = boto3.client("s3")
    previous_map = {}
    is_first_run = False
    logger.info(f"Attempting to load previous metadata from s3://{bucket}/{key_path}")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key_path)
        content = obj["Body"].read().decode("utf-8")
        line_num = 0
        for (
            line
        ) in content.splitlines():  # Use splitlines to handle various line endings
            line_num += 1
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                k = record.get("s3_source_key")
                if k:
                    previous_map[k] = record
            except json.JSONDecodeError as e:
                logger.warning(
                    f"Skipping malformed JSON line {line_num} in previous metadata: {e}"
                )
            except Exception as e:
                logger.warning(
                    f"Skipping line {line_num} in previous metadata due to error: {e}"
                )

        if not previous_map:
            logger.info(
                "Previous metadata file was empty or unreadable. Considering this a first run."
            )
            is_first_run = True
        else:
            logger.info(
                f"Successfully loaded {len(previous_map)} previous metadata entries."
            )
            is_first_run = False
    except s3.exceptions.NoSuchKey:
        logger.info(
            f"Previous metadata file s3://{bucket}/{key_path} not found. First run."
        )
        is_first_run = True
    except Exception as e:
        logger.error(
            f"Error loading previous metadata from s3://{bucket}/{key_path}: {e}",
            exc_info=True,
        )
        is_first_run = True  # Treat as first run if loading fails
    return previous_map, is_first_run


# --- HELPER FUNCTIONS FOR METADATA RECORD CREATION ---
def create_deleted_record(source_key, previous_record, args):
    """Create a metadata record for a deleted source file."""
    logger.debug(f"Creating 'deleted' record for {source_key}")
    return {
        "s3_source_bucket": previous_record.get("s3_source_bucket", args.source_bucket),
        "s3_source_key": source_key,
        "s3_source_etag": previous_record.get("s3_source_etag", ""),
        "s3_source_last_modified": previous_record.get("s3_source_last_modified", ""),
        "s3_source_version_id": previous_record.get("s3_source_version_id", ""),
        "s3_destination_bucket": previous_record.get("s3_destination_bucket", ""),
        "s3_destination_key": previous_record.get("s3_destination_key", ""),
        "s3_destination_etag": previous_record.get("s3_destination_etag", ""),
        "s3_destination_version_id": previous_record.get(
            "s3_destination_version_id", ""
        ),
        "metadata_last_modified": datetime.now(timezone.utc).isoformat(),
        "metadata_ops": "deleted",
        "metadata_copy_attempt_evaluation": False,  # No copy attempt for deleted
        "metadata_copy_succeeded": False,
        "metadata_copy_decision_reason": "Source file deleted",
        "processing_error": "",
        **get_empty_exif_structure(),
    }


# --- DISCOVERY PHASE (Synchronous) ---
def discovery_phase(args, metadata_writer_actor_ref):
    """Phase 1: Discover what needs to be processed (Sync) & handle deleted."""
    logger.info("🔍 PHASE 1: Discovery and Planning (Sync)")
    start_time = time.time()

    previous_metadata, is_first_run = load_previous_metadata(
        args.metadata_bucket, get_metadata_file_key(args)
    )

    logger.info("Listing S3 objects...")
    source_objects = list_s3_objects_details_sync(
        args.source_bucket, args.source_prefix
    )
    dest_objects = (
        list_s3_objects_details_sync(args.dest_bucket, args.dest_prefix)
        if not args.no_copy
        else {}
    )
    logger.info(
        f"Found {len(source_objects)} source objects, {len(dest_objects) if not args.no_copy else 'N/A (copy disabled)'} destination objects."
    )

    previous_keys = set(previous_metadata.keys())
    current_source_keys = set(
        obj_key
        for obj_key in source_objects
        if obj_key.lower().endswith((".jpg", ".jpeg"))
    )

    deleted_keys = previous_keys - current_source_keys
    if deleted_keys:
        logger.info(
            f"Identified {len(deleted_keys)} deleted files (present in previous metadata, not in current source)."
        )
        for key in deleted_keys:
            if key in previous_metadata:
                deleted_record = create_deleted_record(
                    key, previous_metadata[key], args
                )
                metadata_writer_actor_ref.add_record.remote(
                    deleted_record
                )  # Send to actor
            else:
                logger.warning(
                    f"Key {key} was in deleted_keys but not found in previous_metadata map. Skipping deleted record."
                )
    else:
        logger.info("No deleted files identified based on previous metadata.")

    work_queue = []
    previous_ts_map = {
        k: v.get("s3_source_last_modified") for k, v in previous_metadata.items()
    }

    for source_key, source_details in source_objects.items():
        if not source_key.lower().endswith((".jpg", ".jpeg")):
            continue  # Skip non-JPEG files

        dest_key = calculate_destination_key(
            source_key, args.source_prefix, args.dest_prefix
        )
        dest_item_details = dest_objects.get(dest_key) if not args.no_copy else None

        current_op = None
        needs_copy_op = False
        copy_reason_op = "initial_evaluation"

        if args.no_copy:
            needs_copy_op = False
            copy_reason_op = "copy_disabled_by_flag"
        elif dest_item_details is None:
            current_op = "created"
            needs_copy_op = True
            copy_reason_op = "destination_not_found"
        elif (
            source_details["etag"] != dest_item_details["etag"]
        ):  # Compare ETags for changes
            current_op = "updated"
            needs_copy_op = True
            copy_reason_op = "etag_mismatch"
        else:  # ETags match
            copy_reason_op = "etag_match_skip_copy"
            needs_copy_op = False

        needs_exif_op = should_process_exif(
            source_key,
            source_details,
            previous_ts_map,
            args.force,
            is_first_run,
            args.no_exif_extraction,
        )

        if current_op is None and needs_exif_op:  # If no copy op, but EXIF needed
            current_op = "updated"  # Mark as updated for EXIF processing
            copy_reason_op = (
                "exif_processing_triggered_update"
                if not needs_copy_op
                else copy_reason_op
            )

        if current_op in ["created", "updated"] or (
            needs_exif_op and not current_op
        ):  # If any operation is needed
            if not current_op:
                current_op = "updated"  # Ensure op is set if only EXIF needed
            work_queue.append(
                {
                    "source_key": source_key,
                    "source_etag": source_details["etag"],
                    "source_last_modified": source_details["last_modified"].isoformat()
                    if source_details["last_modified"]
                    else "",
                    "s3_source_version_id": source_details.get(
                        "VersionId", ""
                    ),  # Store version ID
                    "needs_exif": needs_exif_op,
                    "needs_copy": needs_copy_op,
                    "copy_reason": copy_reason_op,
                    "metadata_ops": current_op,
                }
            )
        # else:
        # logger.debug(f"Skipping {source_key}: No 'created' or 'updated' operation identified (copy_reason: {copy_reason_op}, needs_exif: {needs_exif_op}).")

    discovery_time = time.time() - start_time
    logger.info(
        f"✅ Discovery completed in {discovery_time:.1f}s. Work queue for C/U: {len(work_queue)} items."
    )
    return work_queue, is_first_run


def should_process_exif(
    source_key, source_details, previous_ts_map, force_flag, is_first_run, no_exif_flag
):
    """Determine if EXIF processing is needed for this file"""
    if no_exif_flag:
        logger.debug(f"[{source_key}] Skipping EXIF due to --no-exif-extraction flag.")
        return False
    if force_flag:
        logger.debug(f"[{source_key}] Forcing EXIF processing due to --force flag.")
        return True
    if is_first_run:
        logger.debug(f"[{source_key}] Processing EXIF as it's the first run.")
        return True
    if (
        source_key not in previous_ts_map or not previous_ts_map[source_key]
    ):  # New file or no previous timestamp
        logger.debug(
            f"[{source_key}] Processing EXIF as it's new or no previous timestamp."
        )
        return True

    try:
        saved_ts_str = previous_ts_map[source_key]
        # S3 LastModified is already timezone-aware (UTC)
        source_ts_dt = source_details["last_modified"]

        # Parse saved timestamp string, assuming ISO format possibly with 'Z'
        if saved_ts_str.endswith("Z"):
            saved_ts_dt = datetime.fromisoformat(saved_ts_str[:-1] + "+00:00")
        else:
            saved_ts_dt = datetime.fromisoformat(saved_ts_str)

        # Ensure both are timezone-aware for comparison (assume UTC if naive for saved)
        if saved_ts_dt.tzinfo is None:
            saved_ts_dt = saved_ts_dt.replace(tzinfo=timezone.utc)

        if source_ts_dt > saved_ts_dt:
            logger.debug(
                f"[{source_key}] Processing EXIF as source is newer ({source_ts_dt} > {saved_ts_dt})."
            )
            return True
        else:
            logger.debug(
                f"[{source_key}] Skipping EXIF as source is not newer ({source_ts_dt} <= {saved_ts_dt}) and not forced."
            )
            return False
    except (ValueError, TypeError) as e:
        logger.warning(
            f"Error comparing timestamps for {source_key} (saved: '{previous_ts_map.get(source_key)}', current: '{source_details['last_modified']}'): {e}. Will reprocess EXIF."
        )
        return True


# --- PROCESSING PHASE (Ray-based) ---
def processing_phase_ray(work_queue, args, metadata_writer_actor_ref):
    """Processing phase using Ray tasks."""
    logger.info(
        f"⚙️  PHASE 2: Processing {len(work_queue)} Created/Updated items with Ray"
    )
    start_time = time.time()
    if not work_queue:
        logger.info("No Created/Updated items in the work queue to process.")
        return

    copy_futures = []
    exif_futures = []

    args_dict = vars(args)  # Pass args as a dictionary to Ray tasks

    source_bucket_versioning_enabled = is_versioning_enabled(args.source_bucket)

    items_needing_copy_count = 0
    items_needing_exif_count = 0

    for item in work_queue:
        if item["needs_copy"] and not args.no_copy:
            copy_futures.append(copy_single_file_ray.remote(item, args_dict))
            items_needing_copy_count += 1
        if item["needs_exif"] and not args.no_exif_extraction:
            exif_futures.append(
                download_and_extract_exif_ray.remote(
                    item, args_dict, source_bucket_versioning_enabled
                )
            )
            items_needing_exif_count += 1

    logger.info(
        f"Submitted {items_needing_copy_count} copy tasks and {items_needing_exif_count} EXIF tasks to Ray."
    )

    # Get results
    # Use ray.wait for better progress reporting if needed, or ray.get for simplicity
    copy_results_list = ray.get(copy_futures) if copy_futures else []
    exif_results_list = ray.get(exif_futures) if exif_futures else []

    # Log any errors from Ray tasks
    for res in copy_results_list:
        if res.get("error"):
            logger.error(f"Copy task for {res['source_key']} failed: {res['error']}")
    for res in exif_results_list:
        if res.get("error"):
            logger.error(f"EXIF task for {res['source_key']} failed: {res['error']}")

    # Create maps for easy lookup
    copy_results_map = {
        res["source_key"]: res for res in copy_results_list if "source_key" in res
    }
    exif_results_map = {
        res["source_key"]: res for res in exif_results_list if "source_key" in res
    }

    logger.info(
        f"Received results from Ray: {len(copy_results_map)} copy results, {len(exif_results_map)} EXIF results."
    )

    # --- Combine results and create final metadata ---
    logger.info("Combining results and generating final metadata records...")
    processed_count = 0
    failed_count = 0

    for item in work_queue:  # Iterate through the original work_queue items
        source_key = item["source_key"]
        result = {
            "s3_source_bucket": args.source_bucket,
            "s3_source_key": source_key,
            "s3_source_etag": item["source_etag"],
            "s3_source_last_modified": item["source_last_modified"],
            "s3_source_version_id": item.get("s3_source_version_id", ""),
            "metadata_last_modified": datetime.now(timezone.utc).isoformat(),
            "metadata_ops": item["metadata_ops"],
            "metadata_copy_decision_reason": item["copy_reason"],
            "processing_error": "",
            **get_empty_exif_structure(),
        }
        current_item_has_error = False

        # Populate EXIF data
        if item["needs_exif"] and not args.no_exif_extraction:
            exif_res = exif_results_map.get(source_key)
            if exif_res and "exif" in exif_res:
                result.update(exif_res["exif"])  # Update with extracted EXIF fields
                if exif_res.get("error"):
                    result["processing_error"] += f"EXIF Error: {exif_res['error']}; "
                    result["exif_extraction_error"] = exif_res[
                        "error"
                    ]  # Specific EXIF error field
                    current_item_has_error = True
            else:  # EXIF task might have failed before returning structured data or missing
                err_msg = "EXIF result missing or malformed from Ray task."
                result["processing_error"] += f"{err_msg}; "
                result["exif_extraction_error"] = err_msg
                current_item_has_error = True
        elif args.no_exif_extraction:
            result["exif_extraction_error"] = (
                "EXIF extraction skipped by --no-exif-extraction flag."
            )
        else:  # Not needing EXIF
            result["exif_extraction_error"] = (
                "EXIF not processed (not identified as needing update)."
            )

        # Populate Copy data
        result["metadata_copy_attempt_evaluation"] = (
            item["needs_copy"] and not args.no_copy
        )
        if item["needs_copy"] and not args.no_copy:
            copy_res = copy_results_map.get(source_key)
            if copy_res and copy_res.get("success"):
                result["metadata_copy_succeeded"] = True
                result["s3_destination_bucket"] = args.dest_bucket
                result["s3_destination_key"] = copy_res["dest_key"]
                result["s3_destination_etag"] = copy_res["dest_etag"]
                result["s3_destination_version_id"] = copy_res["dest_version_id"]
            elif copy_res:  # Copy attempted but failed
                result["metadata_copy_succeeded"] = False
                err_msg = copy_res.get("error", "Unknown copy error")
                result["processing_error"] += f"Copy Error: {err_msg}; "
                result["s3_destination_bucket"] = (
                    args.dest_bucket
                )  # Still record attempted destination
                result["s3_destination_key"] = copy_res.get(
                    "dest_key",
                    calculate_destination_key(
                        source_key, args.source_prefix, args.dest_prefix
                    ),
                )
                current_item_has_error = True
            else:  # Copy was needed, but no result from Ray task (should not happen if logic is correct)
                result["metadata_copy_succeeded"] = False
                err_msg = (
                    "Copy result missing from Ray task for an item marked for copy."
                )
                result["processing_error"] += f"{err_msg}; "
                result["s3_destination_bucket"] = args.dest_bucket
                result["s3_destination_key"] = calculate_destination_key(
                    source_key, args.source_prefix, args.dest_prefix
                )
                current_item_has_error = True
        else:  # Item did not need a copy attempt
            result["metadata_copy_succeeded"] = (
                True  # No attempt means no failure in this context
            )
            if item["copy_reason"] == "copy_disabled_by_flag":
                result["metadata_copy_succeeded"] = (
                    False  # Explicitly false if copy was disabled
                )
            elif (
                item["copy_reason"] == "etag_match_skip_copy"
            ):  # If copy skipped due to ETag match
                result["s3_destination_bucket"] = args.dest_bucket
                result["s3_destination_key"] = calculate_destination_key(
                    source_key, args.source_prefix, args.dest_prefix
                )
                result["s3_destination_etag"] = item[
                    "source_etag"
                ]  # Dest ETag is same as source
            elif args.no_copy:  # If --no-copy, destination fields should be empty
                result["s3_destination_bucket"] = ""
                result["s3_destination_key"] = ""

        # Ensure destination fields exist if not set above
        result.setdefault("s3_destination_bucket", "")
        result.setdefault("s3_destination_key", "")
        result.setdefault("s3_destination_etag", "")
        result.setdefault("s3_destination_version_id", "")

        result["processing_error"] = result["processing_error"].strip().rstrip(";")
        metadata_writer_actor_ref.add_record.remote(
            result
        )  # Send final record to actor

        if current_item_has_error:
            failed_count += 1
        else:
            processed_count += 1

    processing_time = time.time() - start_time
    logger.info(
        f"✅ Processing phase (Ray) completed in {processing_time:.1f}s. Successfully processed items for metadata: {processed_count}, Items with errors: {failed_count}"
    )


# --- MAIN RAY LOGIC ---
def run_ray_logic(args):
    """Core Ray processing logic."""
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.info("Debug logging enabled for s3_image_processor_ray.")
    else:  # Silence verbose boto logs unless in debug mode
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("s3transfer").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Normalize prefixes
    args.source_prefix = args.source_prefix.rstrip("/") if args.source_prefix else ""
    args.dest_prefix = args.dest_prefix.rstrip("/") if args.dest_prefix else ""
    args.metadata_prefix = (
        args.metadata_prefix.rstrip("/") if args.metadata_prefix else ""
    )

    logger.info("🚀 Starting S3 image processing with Ray")
    logger.info(
        f"Source: s3://{args.source_bucket}/{args.source_prefix or '<bucket_root>'}"
    )
    if not args.no_copy:
        logger.info(
            f"Destination: s3://{args.dest_bucket}/{args.dest_prefix or '<bucket_root>'}"
        )
    else:
        logger.info("Copying disabled (--no-copy).")

    if args.no_exif_extraction:
        logger.info("EXIF extraction disabled (--no-exif-extraction).")
    if args.force:
        logger.info("Forcing EXIF reprocessing (--force).")

    metadata_file_key = get_metadata_file_key(args)
    logger.info(f"Metadata Output: s3://{args.metadata_bucket}/{metadata_file_key}")
    logger.info(
        f"Ray Max Processes (local): {args.max_processes if not IS_GLUE_ENVIRONMENT else 'N/A (Glue managed)'}"
    )

    total_start_time = time.time()

    # Create the Ray actor for metadata writing
    # Pass debug flag to actor for its own logging
    metadata_writer_actor_ref = RayStreamingMetadataWriter.remote(
        metadata_bucket=args.metadata_bucket,
        metadata_key=metadata_file_key,
        upload_interval=args.metadata_upload_interval,
        batch_size=args.metadata_batch_size,
        debug_logging=args.debug,
    )

    try:
        work_queue, is_first_run = discovery_phase(args, metadata_writer_actor_ref)

        logger.info("📊 Discovery Summary:")
        logger.info(f"   - Items for 'created'/'updated' processing: {len(work_queue)}")
        if work_queue:
            needs_exif_count = sum(1 for item in work_queue if item["needs_exif"])
            needs_copy_count = sum(1 for item in work_queue if item["needs_copy"])
            logger.info(f"     - Potentially needing EXIF: {needs_exif_count}")
            logger.info(f"     - Potentially needing Copy: {needs_copy_count}")

        if not work_queue:
            logger.info(
                "No 'created' or 'updated' items to process further. 'Deleted' items (if any) have been logged via actor."
            )
        else:
            processing_phase_ray(work_queue, args, metadata_writer_actor_ref)

        total_time = time.time() - total_start_time
        logger.info(f"🎉 All operations completed in {total_time:.1f}s")

    except KeyboardInterrupt:
        logger.warning("⚠️  Interrupted by user. Attempting to finalize metadata...")
    except Exception as e:
        logger.error(f"💥 Fatal error during Ray execution: {e}", exc_info=True)
    finally:
        logger.info("💾 Finalizing metadata persistence...")
        if "metadata_writer_actor_ref" in locals() and metadata_writer_actor_ref:
            # Wait for the final upload and get status
            try:
                final_status = ray.get(
                    metadata_writer_actor_ref.stop_and_upload_final.remote()
                )
                logger.info("📈 Final Metadata Statistics (Current Run from Actor):")
                logger.info(
                    f"   - New records written by this run: {final_status.get('total_written_current_run', 'N/A')}"
                )
                uploaded_matches_written = final_status.get(
                    "total_uploaded_current_run", -1
                ) == final_status.get("total_written_current_run", -2)
                logger.info(
                    f"   - New records from this run part of last S3 upload: {uploaded_matches_written if final_status else 'N/A'}"
                )
            except Exception as actor_stop_ex:
                logger.error(
                    f"Error stopping metadata writer actor or getting final status: {actor_stop_ex}",
                    exc_info=True,
                )

        logger.info(
            f"   - Final cumulative metadata destination: s3://{args.metadata_bucket}/{metadata_file_key}"
        )


# --- ARGUMENT PARSING ---
def setup_arg_parser():
    """Sets up and returns the ArgumentParser instance for local execution."""
    parser = argparse.ArgumentParser(
        description="S3 image processor (Ray). For local execution or AWS Glue Ray jobs."
    )
    # Required arguments
    parser.add_argument(
        "--source-bucket", required=True, help="S3 bucket for source images."
    )
    parser.add_argument(
        "--dest-bucket",
        required=True,
        help="S3 bucket to copy images to (can be dummy if --no-copy).",
    )
    parser.add_argument(
        "--metadata-bucket", required=True, help="S3 bucket for output metadata."
    )
    parser.add_argument(
        "--output-s3-path",
        required=True,
        help="S3 key stem for metadata file (e.g., 'processed/logs/my_run').",
    )

    # Optional path arguments
    parser.add_argument(
        "--source-prefix",
        default="",
        help="S3 prefix in source bucket (e.g., 'photos/2024/').",
    )
    parser.add_argument(
        "--dest-prefix", default="", help="S3 prefix in destination bucket."
    )
    parser.add_argument(
        "--metadata-prefix",
        default="",
        help="S3 prefix in metadata bucket for the output-s3-path.",
    )

    # Behavior flags
    parser.add_argument(
        "--no-copy",
        action="store_true",
        help="Skip copying images, only process metadata.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force EXIF reprocessing for all source files.",
    )
    parser.add_argument(
        "--no-exif-extraction",
        action="store_true",
        help="Skip EXIF extraction entirely.",
    )

    # Performance tuning (less critical for Ray task submission, more for local Ray init)
    parser.add_argument(
        "--max-processes",
        type=int,
        default=0,
        help="Max CPUs for local Ray init (0=auto). Not used by Glue Ray.",
    )

    # Metadata writer tuning
    parser.add_argument(
        "--metadata-upload-interval",
        type=int,
        default=30,
        help="Metadata upload interval for the actor (seconds).",
    )
    parser.add_argument(
        "--metadata-batch-size",
        type=int,
        default=50,
        help="Number of new records for actor to batch before local write.",
    )

    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    return parser


def _str_to_bool(value: Optional[str]) -> bool:
    """Glue passes booleans as strings; anything truthy → True."""
    if value is None:
        return False
    return value.lower() in ("1", "true", "yes")


def parse_glue_args_ray() -> types.SimpleNamespace:
    """
    Read job parameters injected by AWS Glue Ray from environment variables
    and return them as attributes on a SimpleNamespace (behaves like an object).

    Required job parameters must be present in the environment or we raise.
    Optional ones fall back to sensible defaults.
    """
    logger.info("--- SCRIPT START (GLUE RAY MODE: ENV VAR PARSE) ---")

    # -- Define argument contract ------------------------------------------------
    required = {"source-bucket", "dest-bucket", "metadata-bucket", "output-s3-path"}

    optional_with_defaults: dict[str, str | int | bool] = {
        # path prefixes
        "source-prefix": "",
        "dest-prefix": "",
        "metadata-prefix": "",
        # flags
        "no-copy": False,
        "force": False,
        "no-exif-extraction": False,
        "debug": False,
        # performance / tuning
        "max-processes": 0,  # ignored by Ray init
        "metadata-upload-interval": 60,
        "metadata-batch-size": 300,
    }

    # -- Materialize namespace ---------------------------------------------------
    args = types.SimpleNamespace()

    # First, required params
    for key in required:
        value = os.environ.get(key)
        if value is None:
            raise RuntimeError(f"Missing required Glue Ray param: {key}")
        key = key.replace("-", "_")
        setattr(args, key, value)

    # Then optionals
    for key, default in optional_with_defaults.items():
        raw = os.environ.get(key)
        if isinstance(default, bool):
            parsed = _str_to_bool(raw) if raw is not None else default
        elif isinstance(default, int):
            parsed = int(raw) if raw is not None else default
        else:  # string
            parsed = raw if raw is not None else default
        key = key.replace("-", "_")
        setattr(args, key, parsed)

    logger.info(f"Parsed Glue Ray env vars: {vars(args)}")
    return args


# --- MAIN ENTRY POINT ---
def main():
    """Main entry point for local/Glue Ray execution."""

    args = None
    if IS_GLUE_ENVIRONMENT:
        args = parse_glue_args_ray()
        ray.init()
        logger.info("--- SCRIPT START ON AWS GLUE RAY MODE) ---")
    else:
        logger.info("--- SCRIPT START (LOCAL RAY MODE) ---")
        parser = setup_arg_parser()
        args = parser.parse_args()
        if not ray.is_initialized():
            num_cpus_local = args.max_processes if args.max_processes > 0 else None
            try:
                # Try connecting to an existing cluster first
                ray.init(address="auto", ignore_reinit_error=True)
                logger.info("✅ Connected to existing Ray cluster.")
            except Exception as e:
                logger.warning(
                    f"⚠️  No existing Ray cluster found (address='auto' failed): {e}"
                )
                try:
                    # Start a new local cluster
                    ray.init(
                        include_dashboard=True,
                        dashboard_host="0.0.0.0",
                        ignore_reinit_error=True,
                        num_cpus=num_cpus_local,
                    )
                    logger.info("✅ Started new local Ray cluster with dashboard.")
                except Exception as e2:
                    logger.error(
                        f"❌ Failed to start new local Ray cluster: {e2}", exc_info=True
                    )
                    sys.exit(1)

    if args is None:
        logger.error("Argument parsing failed to produce an args object. Exiting.")
        sys.exit(1)

    try:
        run_ray_logic(args)
    finally:
        # --- MODIFICATION: Only shutdown in local mode ---
        if not IS_GLUE_ENVIRONMENT and ray.is_initialized():
            ray.shutdown()
            logger.info("Local Ray instance shut down.")
        elif IS_GLUE_ENVIRONMENT:
            logger.info(
                "Script finished in Glue Ray environment. Ray lifecycle managed by Glue."
            )


main()  # use like that because glue not suppport __main__
