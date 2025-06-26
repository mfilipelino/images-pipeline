"""Image processing utilities for the images pipeline."""

import random
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
    Native Python K-means quantization implementation.
    Educational example of clustering without external libraries.

    Args:
        img: PIL Image to quantize
        k: Number of color clusters
        max_iter: Maximum iterations for K-means

    Returns:
        Quantized PIL Image
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
                # Calculate mean for each color channel
                mean_pixel = tuple(
                    sum(pixel[channel] for pixel in clusters[i]) // len(clusters[i])
                    for channel in range(3)
                )
                new_centroids.append(mean_pixel)
            else:
                # Reinitialize empty cluster with random pixel
                new_centroids.append(random.choice(pixels))

        # Check for convergence
        if new_centroids == centroids:
            break
        centroids = new_centroids

    # Create quantized image
    quantized_pixels = []
    for pixel in pixels:
        distances = [
            sum((pixel[d] - centroids[c][d]) ** 2 for d in range(3)) for c in range(k)
        ]
        closest_centroid = distances.index(min(distances))
        quantized_pixels.append(centroids[closest_centroid])

    # Create new image with quantized pixels
    quantized_img = Image.new(img.mode, img.size)
    quantized_img.putdata(quantized_pixels)
    return quantized_img


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
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(pixels)

    # Replace each pixel with its cluster center
    quantized_pixels = kmeans.cluster_centers_[kmeans.labels_]

    # Reshape back to image dimensions and convert to uint8
    quantized_array = quantized_pixels.reshape(img_array.shape).astype(np.uint8)

    # Convert back to PIL Image
    return Image.fromarray(quantized_array)


def apply_transformation(img: "Image.Image", transformation: str) -> "Image.Image":
    """
    Apply the specified transformation to an image.

    Args:
        img: PIL Image to transform
        transformation: Type of transformation ("grayscale", "kmeans", "native_kmeans")

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
                    value = value.decode("utf-8")
                except UnicodeDecodeError:
                    value = str(value)
            elif isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
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
