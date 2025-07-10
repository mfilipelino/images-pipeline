"""Image processing utilities for the images pipeline."""

from collections.abc import Iterable
from typing import Any, Dict, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from PIL import Image
    from PIL.ExifTags import TAGS
else:
    try:
        from PIL import Image
        from PIL.ExifTags import TAGS
    except ImportError:
        # Fallback when PIL is not available
        Image = Any
        TAGS = {}


def native_kmeans_quantize(
    img: "Image.Image", k: int = 8, max_iter: int = 10
) -> "Image.Image":
    """
    DEPRECATED: Use sklearn_kmeans_quantize instead.

    This native Python implementation is kept for educational purposes only.
    For production use, sklearn_kmeans_quantize provides better performance
    and more robust clustering.

    Args:
        img: PIL Image to quantize
        k: Number of color clusters
        max_iter: Maximum iterations for K-means

    Returns:
        Quantized PIL Image
    """
    import warnings

    warnings.warn(
        "native_kmeans_quantize is deprecated and will be removed in a future version. "
        "Use sklearn_kmeans_quantize or apply_transformation(img, 'kmeans') instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    # For backward compatibility, delegate to sklearn implementation
    return sklearn_kmeans_quantize(img, k)


def sklearn_kmeans_quantize(img: "Image.Image", k: int = 8) -> "Image.Image":
    """
    Scikit-learn K-means quantization implementation.

    Args:
        img: PIL Image to quantize
        k: Number of color clusters

    Returns:
        Quantized PIL Image
    """
    try:
        import numpy as np
        from sklearn.cluster import KMeans
    except ImportError:
        raise ImportError(
            "numpy and scikit-learn are required for sklearn_kmeans_quantize"
        )

    # Convert to numpy array
    img_array = np.array(img)

    # Reshape to be a list of pixels
    pixels = img_array.reshape(-1, 3)

    # Apply K-means clustering
    kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto")
    kmeans.fit(pixels)  # type: ignore[reportUnknownMemberType]

    # Replace each pixel with its cluster center
    quantized_pixels = kmeans.cluster_centers_[kmeans.labels_]  # type: ignore[reportUnknownMemberType,reportUnknownVariableType]

    # Reshape back to image dimensions and convert to uint8
    quantized_array = quantized_pixels.reshape(img_array.shape).astype(np.uint8)  # type: ignore[reportUnknownMemberType,reportUnknownVariableType]

    # Convert back to PIL Image
    return Image.fromarray(quantized_array)  # type: ignore[reportUnknownArgumentType]


def apply_transformation(img: "Image.Image", transformation: str) -> "Image.Image":
    """
    Apply the specified transformation to an image.

    Args:
        img: PIL Image to transform
        transformation: Type of transformation ("grayscale", "kmeans", "native_kmeans")
            Note: "native_kmeans" is deprecated, use "kmeans" instead. See DEPRECATION_NOTICE.md for details.

    Returns:
        Transformed PIL Image

    Raises:
        ValueError: If transformation type is unknown
    """
    if transformation == "grayscale":
        return img.convert("L").convert("RGB")
    elif transformation == "kmeans":
        return sklearn_kmeans_quantize(img, k=8)
    elif transformation == "native_kmeans":
        # Deprecated: will show warning and use sklearn implementation
        return native_kmeans_quantize(img, k=8)
    else:
        raise ValueError(f"Unknown transformation: {transformation}")


def extract_exif_data(img: "Image.Image") -> Dict[str, Any]:
    """
    Extract EXIF metadata from PIL Image, handling privacy concerns.

    Args:
        img: PIL Image to extract EXIF from

    Returns:
        Dictionary containing EXIF data and image info
    """
    exif_dict = {}

    # Basic image information
    exif_dict.update(  # type: ignore[reportUnknownMemberType]
        {
            "width": img.width,
            "height": img.height,
            "format": img.format or "unknown",
            "mode": img.mode,
        }
    )

    # Extract EXIF data if available
    if hasattr(img, "_getexif"):
        exif_data_raw = img._getexif()  # type: ignore[reportAttributeAccessIssue]
        if exif_data_raw is not None:
            for tag_id, value in exif_data_raw.items():  # type: ignore[reportUnknownMemberType,reportUnknownVariableType]
                tag = TAGS.get(tag_id, tag_id)  # type: ignore[reportUnknownArgumentType]

                # Skip GPS data for privacy
                if "gps" in str(tag).lower():
                    continue

                # Convert complex types to strings for JSON serialization
                processed_value: Union[str, int, float]
                if isinstance(value, bytes):
                    try:
                        processed_value = value.decode("utf-8")
                    except UnicodeDecodeError:
                        processed_value = str(value)
                elif isinstance(value, (str, int, float)):
                    processed_value = value
                elif isinstance(value, Iterable):
                    processed_value = str(value)  # type: ignore[reportUnknownArgumentType]
                else:
                    processed_value = str(value)  # type: ignore[reportUnknownArgumentType]

                exif_dict[str(tag)] = processed_value

    return exif_dict  # type: ignore[reportUnknownVariableType]


def calculate_dest_key(source_key: str, source_prefix: str, dest_prefix: str) -> str:
    """
    Calculate destination S3 key from source key and prefixes.

    Args:
        source_key: Original S3 key
        source_prefix: Source prefix to remove
        dest_prefix: Destination prefix to add

    Returns:
        Destination S3 key
    """
    # Remove source prefix if present
    if source_prefix and source_key.startswith(source_prefix):
        relative_key = source_key[len(source_prefix) :].lstrip("/")
    else:
        relative_key = source_key

    # Add destination prefix
    if dest_prefix:
        return f"{dest_prefix.rstrip('/')}/{relative_key}"
    else:
        return relative_key
