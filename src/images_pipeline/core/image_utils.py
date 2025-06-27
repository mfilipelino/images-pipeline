"""Image processing utilities for the images pipeline."""

from collections.abc import Iterable
from typing import Any, Dict, TYPE_CHECKING

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
    Performs color quantization on an image using the K-means clustering algorithm.

    The K-means algorithm works by:
    1. Initializing `k` cluster centroids (representative colors).
    2. Assigning each pixel in the image to the nearest cluster centroid.
    3. Recalculating the centroids based on the mean color of the pixels assigned to them.
    4. Repeating steps 2 and 3 until the centroids no longer change significantly or a maximum
       number of iterations is reached.
    The `n_init` parameter controls how many times the K-means algorithm will be run with
    different centroid initializations. The final result will be the one with the best
    inertia (sum of squared distances). Using `n_init='auto'` (or a specific number like 10)
    helps in finding a more stable and optimal clustering.
    `random_state` is used to ensure reproducibility of the results, as K-means
    initialization can be random.

    Args:
        img: The input PIL (Pillow) Image object to be quantized.
        k: The desired number of color clusters (centroids) for quantization.
           Fewer clusters mean fewer colors in the output image.

    Returns:
        A new PIL Image object that has been color-quantized using K-means.
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

    # Reshape to be a list of pixels: each row is a pixel, and columns are R, G, B.
    # The -1 infers the number of pixels based on the original image dimensions and 3 channels.
    pixels = img_array.reshape(-1, 3)

    # Apply K-means clustering.
    # n_init=10 runs the K-means algorithm 10 times with different initial centroids
    # and chooses the best one (lowest inertia) to avoid local optima.
    # random_state ensures reproducibility of centroid initialization.
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(pixels)

    # Replace each pixel with its corresponding cluster center.
    # kmeans.labels_ is an array where each element is the cluster index for a pixel.
    # kmeans.cluster_centers_ is an array of the actual centroid colors.
    # This indexing effectively maps each pixel to its new quantized color.
    quantized_pixels = kmeans.cluster_centers_[kmeans.labels_]

    # Reshape back to original image dimensions and convert to unsigned 8-bit integer type
    quantized_array = quantized_pixels.reshape(img_array.shape).astype(np.uint8)

    # Convert back to PIL Image
    return Image.fromarray(quantized_array)


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
    exif_dict.update(
        {
            "width": img.width,
            "height": img.height,
            "format": img.format,
            "mode": img.mode,
        }
    )

    # Extract EXIF data if available
    if hasattr(img, "_getexif") and img._getexif() is not None:
        exif = img._getexif()
        for tag_id, value in exif.items():
            tag = TAGS.get(tag_id, tag_id)

            # Skip GPS data for privacy
            if "gps" in str(tag).lower():
                continue

            # Convert complex types to strings for JSON serialization
            if isinstance(value, bytes):
                try:
                    value = value.decode("utf-8")  # Attempt to decode bytes to string
                except UnicodeDecodeError:
                    value = str(value)  # If decoding fails, represent as string (e.g., "b'\\xff\\xd8'")
            elif isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
                # Convert other iterable types (like tuples or lists of numbers) to string representations.
                # This ensures serializability (e.g., for JSON) if EXIF data is exported.
                value = str(value)

            exif_dict[str(tag)] = value

    return exif_dict


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
