"""Tests for image_utils.py utility functions."""

import pytest
import warnings
from unittest.mock import Mock, patch
from images_pipeline.core.image_utils import (
    calculate_dest_key,
    extract_exif_data,
    apply_transformation,
    sklearn_kmeans_quantize,
    native_kmeans_quantize,
)


class TestCalculateDestKey:
    """Tests for calculate_dest_key function."""

    def test_calculate_dest_key_no_prefixes(self):
        """Test calculate_dest_key with no prefixes."""
        source_key = "image.jpg"
        source_prefix = ""
        dest_prefix = ""

        result = calculate_dest_key(source_key, source_prefix, dest_prefix)
        assert result == "image.jpg"

    def test_calculate_dest_key_with_dest_prefix_only(self):
        """Test calculate_dest_key with destination prefix only."""
        source_key = "image.jpg"
        source_prefix = ""
        dest_prefix = "output"

        result = calculate_dest_key(source_key, source_prefix, dest_prefix)
        assert result == "output/image.jpg"

    def test_calculate_dest_key_with_source_prefix_only(self):
        """Test calculate_dest_key with source prefix only."""
        source_key = "input/image.jpg"
        source_prefix = "input"
        dest_prefix = ""

        result = calculate_dest_key(source_key, source_prefix, dest_prefix)
        assert result == "image.jpg"

    def test_calculate_dest_key_with_both_prefixes(self):
        """Test calculate_dest_key with both prefixes."""
        source_key = "input/photos/image.jpg"
        source_prefix = "input/photos"
        dest_prefix = "output/processed"

        result = calculate_dest_key(source_key, source_prefix, dest_prefix)
        assert result == "output/processed/image.jpg"

    def test_calculate_dest_key_source_prefix_with_trailing_slash(self):
        """Test calculate_dest_key with source prefix having trailing slash."""
        source_key = "input/photos/image.jpg"
        source_prefix = "input/photos/"
        dest_prefix = "output"

        result = calculate_dest_key(source_key, source_prefix, dest_prefix)
        assert result == "output/image.jpg"

    def test_calculate_dest_key_dest_prefix_with_trailing_slash(self):
        """Test calculate_dest_key with destination prefix having trailing slash."""
        source_key = "input/image.jpg"
        source_prefix = "input"
        dest_prefix = "output/"

        result = calculate_dest_key(source_key, source_prefix, dest_prefix)
        assert result == "output/image.jpg"

    def test_calculate_dest_key_source_prefix_not_matching(self):
        """Test calculate_dest_key when source prefix doesn't match."""
        source_key = "different/path/image.jpg"
        source_prefix = "input"
        dest_prefix = "output"

        result = calculate_dest_key(source_key, source_prefix, dest_prefix)
        assert result == "output/different/path/image.jpg"

    def test_calculate_dest_key_nested_paths(self):
        """Test calculate_dest_key with deeply nested paths."""
        source_key = "input/2024/01/photos/vacation/image.jpg"
        source_prefix = "input/2024/01"
        dest_prefix = "output/processed/2024"

        result = calculate_dest_key(source_key, source_prefix, dest_prefix)
        assert result == "output/processed/2024/photos/vacation/image.jpg"

    def test_calculate_dest_key_leading_slash_handling(self):
        """Test calculate_dest_key handles leading slashes correctly."""
        source_key = "input/photos/image.jpg"
        source_prefix = "input/photos"
        dest_prefix = "output"

        result = calculate_dest_key(source_key, source_prefix, dest_prefix)
        assert result == "output/image.jpg"
        assert not result.startswith("output//")

    def test_calculate_dest_key_empty_strings(self):
        """Test calculate_dest_key with empty strings."""
        assert calculate_dest_key("", "", "") == ""
        assert calculate_dest_key("image.jpg", "", "") == "image.jpg"
        assert calculate_dest_key("", "prefix", "dest") == "dest/"


class TestExtractExifData:
    """Tests for extract_exif_data function."""

    def test_extract_exif_data_basic_image_info(self):
        """Test extract_exif_data extracts basic image information."""
        mock_img = Mock()
        mock_img.width = 1920
        mock_img.height = 1080
        mock_img.format = "JPEG"
        mock_img.mode = "RGB"
        mock_img._getexif.return_value = None

        result = extract_exif_data(mock_img)

        assert result["width"] == 1920
        assert result["height"] == 1080
        assert result["format"] == "JPEG"
        assert result["mode"] == "RGB"

    def test_extract_exif_data_no_exif(self):
        """Test extract_exif_data when no EXIF data is present."""
        mock_img = Mock()
        mock_img.width = 800
        mock_img.height = 600
        mock_img.format = "PNG"
        mock_img.mode = "RGB"
        mock_img._getexif.return_value = None

        result = extract_exif_data(mock_img)

        expected = {"width": 800, "height": 600, "format": "PNG", "mode": "RGB"}
        assert result == expected

    @patch(
        "images_pipeline.core.image_utils.TAGS",
        {256: "ImageWidth", 257: "ImageLength", 272: "Make"},
    )
    def test_extract_exif_data_with_exif(self):
        """Test extract_exif_data with EXIF data present."""
        mock_img = Mock()
        mock_img.width = 1920
        mock_img.height = 1080
        mock_img.format = "JPEG"
        mock_img.mode = "RGB"

        # Mock EXIF data
        mock_exif = {
            256: 1920,  # ImageWidth
            257: 1080,  # ImageLength
            272: "Canon",  # Make
        }
        mock_img._getexif.return_value = mock_exif

        result = extract_exif_data(mock_img)

        assert result["width"] == 1920
        assert result["height"] == 1080
        assert result["format"] == "JPEG"
        assert result["mode"] == "RGB"
        assert result["ImageWidth"] == 1920
        assert result["ImageLength"] == 1080
        assert result["Make"] == "Canon"

    @patch("images_pipeline.core.image_utils.TAGS", {34853: "GPSInfo", 272: "Make"})
    def test_extract_exif_data_skips_gps(self):
        """Test extract_exif_data skips GPS data for privacy."""
        mock_img = Mock()
        mock_img.width = 1920
        mock_img.height = 1080
        mock_img.format = "JPEG"
        mock_img.mode = "RGB"

        # Mock EXIF data with GPS
        mock_exif = {
            34853: {"lat": 40.7128, "lon": -74.0060},  # GPSInfo
            272: "Canon",  # Make
        }
        mock_img._getexif.return_value = mock_exif

        result = extract_exif_data(mock_img)

        # Should have Make but not GPSInfo
        assert result["Make"] == "Canon"
        assert "GPSInfo" not in result

    @patch("images_pipeline.core.image_utils.TAGS", {272: "Make", 306: "DateTime"})
    def test_extract_exif_data_handles_bytes(self):
        """Test extract_exif_data converts bytes to strings."""
        mock_img = Mock()
        mock_img.width = 1920
        mock_img.height = 1080
        mock_img.format = "JPEG"
        mock_img.mode = "RGB"

        # Mock EXIF data with bytes
        mock_exif = {
            272: b"Canon",  # Make as bytes
            306: "2024:01:01 12:00:00",  # DateTime as string
        }
        mock_img._getexif.return_value = mock_exif

        result = extract_exif_data(mock_img)

        assert result["Make"] == "Canon"  # Converted from bytes
        assert result["DateTime"] == "2024:01:01 12:00:00"

    @patch("images_pipeline.core.image_utils.TAGS", {272: "Make", 306: "DateTime"})
    def test_extract_exif_data_handles_unicode_decode_error(self):
        """Test extract_exif_data handles bytes that can't be decoded."""
        mock_img = Mock()
        mock_img.width = 1920
        mock_img.height = 1080
        mock_img.format = "JPEG"
        mock_img.mode = "RGB"

        # Mock EXIF data with invalid UTF-8 bytes
        invalid_bytes = b"\xff\xfe\xfd"
        mock_exif = {
            272: invalid_bytes,  # Make with invalid UTF-8
        }
        mock_img._getexif.return_value = mock_exif

        result = extract_exif_data(mock_img)

        # Should convert to string representation
        assert result["Make"] == str(invalid_bytes)

    @patch("images_pipeline.core.image_utils.TAGS", {272: "Make", 34665: "ExifOffset"})
    def test_extract_exif_data_handles_iterables(self):
        """Test extract_exif_data converts iterables to strings."""
        mock_img = Mock()
        mock_img.width = 1920
        mock_img.height = 1080
        mock_img.format = "JPEG"
        mock_img.mode = "RGB"

        # Mock EXIF data with iterable
        mock_exif = {
            272: "Canon",
            34665: [1, 2, 3, 4],  # Some list value
        }
        mock_img._getexif.return_value = mock_exif

        result = extract_exif_data(mock_img)

        assert result["Make"] == "Canon"
        assert result["ExifOffset"] == "[1, 2, 3, 4]"

    def test_extract_exif_data_no_getexif_method(self):
        """Test extract_exif_data when image has no _getexif method."""
        mock_img = Mock()
        mock_img.width = 800
        mock_img.height = 600
        mock_img.format = "PNG"
        mock_img.mode = "RGB"
        del mock_img._getexif  # Remove the _getexif attribute

        result = extract_exif_data(mock_img)

        expected = {"width": 800, "height": 600, "format": "PNG", "mode": "RGB"}
        assert result == expected


class TestApplyTransformation:
    """Tests for apply_transformation function."""

    def test_apply_transformation_grayscale(self):
        """Test apply_transformation with grayscale."""
        mock_img = Mock()
        mock_gray_img = Mock()
        mock_rgb_img = Mock()

        mock_img.convert.return_value = mock_gray_img
        mock_gray_img.convert.return_value = mock_rgb_img

        result = apply_transformation(mock_img, "grayscale")

        mock_img.convert.assert_called_once_with("L")
        mock_gray_img.convert.assert_called_once_with("RGB")
        assert result == mock_rgb_img

    @patch("images_pipeline.core.image_utils.sklearn_kmeans_quantize")
    def test_apply_transformation_kmeans(self, mock_sklearn_kmeans):
        """Test apply_transformation with kmeans."""
        mock_img = Mock()
        mock_result = Mock()
        mock_sklearn_kmeans.return_value = mock_result

        result = apply_transformation(mock_img, "kmeans")

        mock_sklearn_kmeans.assert_called_once_with(mock_img, k=8)
        assert result == mock_result

    @patch("images_pipeline.core.image_utils.native_kmeans_quantize")
    def test_apply_transformation_native_kmeans_deprecated(self, mock_native_kmeans):
        """Test apply_transformation with deprecated native_kmeans."""
        mock_img = Mock()
        mock_result = Mock()
        mock_native_kmeans.return_value = mock_result

        result = apply_transformation(mock_img, "native_kmeans")

        mock_native_kmeans.assert_called_once_with(mock_img, k=8)
        assert result == mock_result

    def test_apply_transformation_unknown_transformation(self):
        """Test apply_transformation with unknown transformation."""
        mock_img = Mock()

        with pytest.raises(ValueError, match="Unknown transformation: unknown"):
            apply_transformation(mock_img, "unknown")


class TestSklearnKmeansQuantize:
    """Tests for sklearn_kmeans_quantize function."""

    def test_sklearn_kmeans_quantize_function_exists(self):
        """Test that sklearn_kmeans_quantize function exists and is callable."""
        assert callable(sklearn_kmeans_quantize)

    def test_sklearn_kmeans_quantize_with_missing_dependencies(self):
        """Test sklearn_kmeans_quantize with missing dependencies."""
        # Simulate a missing dependency by removing it from sys.modules
        with patch.dict("sys.modules", {"numpy": None}):
            with pytest.raises(
                ImportError, match="numpy and scikit-learn are required"
            ):
                # Call the actual function, which should now raise an ImportError
                sklearn_kmeans_quantize(Mock())


class TestNativeKmeansQuantize:
    """Tests for native_kmeans_quantize function (deprecated)."""

    @patch("images_pipeline.core.image_utils.sklearn_kmeans_quantize")
    def test_native_kmeans_quantize_shows_deprecation_warning(
        self, mock_sklearn_kmeans
    ):
        """Test native_kmeans_quantize shows deprecation warning."""
        mock_img = Mock()
        mock_result = Mock()
        mock_sklearn_kmeans.return_value = mock_result

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = native_kmeans_quantize(mock_img, k=5, max_iter=20)

            # Check that deprecation warning was issued
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "native_kmeans_quantize is deprecated" in str(w[0].message)

        # Check that it delegates to sklearn implementation
        mock_sklearn_kmeans.assert_called_once_with(mock_img, 5)
        assert result == mock_result

    @patch("images_pipeline.core.image_utils.sklearn_kmeans_quantize")
    def test_native_kmeans_quantize_ignores_max_iter(self, mock_sklearn_kmeans):
        """Test native_kmeans_quantize ignores max_iter parameter."""
        mock_img = Mock()
        mock_result = Mock()
        mock_sklearn_kmeans.return_value = mock_result

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = native_kmeans_quantize(mock_img, k=8, max_iter=50)

        # max_iter should be ignored, only k should be passed
        mock_sklearn_kmeans.assert_called_once_with(mock_img, 8)
        assert result == mock_result
