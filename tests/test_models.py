"""Tests for core data models."""

from images_pipeline.core.models import ProcessingConfig, ImageItem, ProcessingResult


class TestProcessingConfig:
    """Tests for ProcessingConfig dataclass."""

    def test_processing_config_creation_required_only(self):
        """Test creating ProcessingConfig with required parameters only."""
        config = ProcessingConfig(source_bucket="test-source", dest_bucket="test-dest")
        assert config.source_bucket == "test-source"
        assert config.dest_bucket == "test-dest"
        assert config.source_prefix == ""
        assert config.dest_prefix == ""
        assert config.transformation is None
        assert config.batch_size == 100
        assert config.concurrency == 20
        assert config.debug is False

    def test_processing_config_creation_all_parameters(self):
        """Test creating ProcessingConfig with all parameters."""
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest",
            source_prefix="source/",
            dest_prefix="dest/",
            transformation="grayscale",
            batch_size=50,
            concurrency=10,
            debug=True,
        )
        assert config.source_bucket == "test-source"
        assert config.dest_bucket == "test-dest"
        assert config.source_prefix == "source/"
        assert config.dest_prefix == "dest/"
        assert config.transformation == "grayscale"
        assert config.batch_size == 50
        assert config.concurrency == 10
        assert config.debug is True

    def test_processing_config_default_values(self):
        """Test ProcessingConfig default values."""
        config = ProcessingConfig(source_bucket="test-source", dest_bucket="test-dest")
        assert config.source_prefix == ""
        assert config.dest_prefix == ""
        assert config.transformation is None
        assert config.batch_size == 100
        assert config.concurrency == 20
        assert config.debug is False

    def test_processing_config_field_types(self):
        """Test ProcessingConfig field types."""
        config = ProcessingConfig(
            source_bucket="test-source",
            dest_bucket="test-dest",
            batch_size=200,
            concurrency=40,
            debug=True,
        )
        assert isinstance(config.source_bucket, str)
        assert isinstance(config.dest_bucket, str)
        assert isinstance(config.source_prefix, str)
        assert isinstance(config.dest_prefix, str)
        assert config.transformation is None or isinstance(config.transformation, str)
        assert isinstance(config.batch_size, int)
        assert isinstance(config.concurrency, int)
        assert isinstance(config.debug, bool)

    def test_processing_config_modification(self):
        """Test that ProcessingConfig fields can be modified."""
        config = ProcessingConfig(source_bucket="test-source", dest_bucket="test-dest")
        config.batch_size = 250
        config.debug = True
        config.transformation = "kmeans"

        assert config.batch_size == 250
        assert config.debug is True
        assert config.transformation == "kmeans"


class TestImageItem:
    """Tests for ImageItem dataclass."""

    def test_image_item_creation(self):
        """Test creating ImageItem with required parameters."""
        item = ImageItem(source_key="source/image.jpg", dest_key="dest/image.jpg")
        assert item.source_key == "source/image.jpg"
        assert item.dest_key == "dest/image.jpg"

    def test_image_item_field_types(self):
        """Test ImageItem field types."""
        item = ImageItem(source_key="source/image.jpg", dest_key="dest/image.jpg")
        assert isinstance(item.source_key, str)
        assert isinstance(item.dest_key, str)

    def test_image_item_modification(self):
        """Test that ImageItem fields can be modified."""
        item = ImageItem(source_key="source/image.jpg", dest_key="dest/image.jpg")
        item.source_key = "new_source/image.jpg"
        item.dest_key = "new_dest/image.jpg"

        assert item.source_key == "new_source/image.jpg"
        assert item.dest_key == "new_dest/image.jpg"

    def test_image_item_empty_strings(self):
        """Test ImageItem with empty strings."""
        item = ImageItem(source_key="", dest_key="")
        assert item.source_key == ""
        assert item.dest_key == ""


class TestProcessingResult:
    """Tests for ProcessingResult dataclass."""

    def test_processing_result_creation_minimal(self):
        """Test creating ProcessingResult with minimal parameters."""
        result = ProcessingResult(source_key="test.jpg")
        assert result.source_key == "test.jpg"
        assert result.dest_key == ""
        assert result.success is False
        assert result.error == ""
        assert result.exif_data == {}
        assert result.processing_time == 0.0

    def test_processing_result_creation_complete(self):
        """Test creating ProcessingResult with all parameters."""
        exif_data = {"camera": "Canon", "iso": 100}
        result = ProcessingResult(
            source_key="test.jpg",
            dest_key="output/test.jpg",
            success=True,
            error="",
            exif_data=exif_data,
            processing_time=1.5,
        )
        assert result.source_key == "test.jpg"
        assert result.dest_key == "output/test.jpg"
        assert result.success is True
        assert result.error == ""
        assert result.exif_data == exif_data
        assert result.processing_time == 1.5

    def test_processing_result_default_values(self):
        """Test ProcessingResult default values."""
        result = ProcessingResult(source_key="test.jpg")
        assert result.dest_key == ""
        assert result.success is False
        assert result.error == ""
        assert result.exif_data == {}
        assert result.processing_time == 0.0

    def test_processing_result_field_types(self):
        """Test ProcessingResult field types."""
        result = ProcessingResult(
            source_key="test.jpg",
            dest_key="output/test.jpg",
            success=True,
            error="test error",
            exif_data={"key": "value"},
            processing_time=2.5,
        )
        assert isinstance(result.source_key, str)
        assert isinstance(result.dest_key, str)
        assert isinstance(result.success, bool)
        assert isinstance(result.error, str)
        assert isinstance(result.exif_data, dict)
        assert isinstance(result.processing_time, float)

    def test_processing_result_modification(self):
        """Test that ProcessingResult fields can be modified."""
        result = ProcessingResult(source_key="test.jpg")
        result.dest_key = "modified/test.jpg"
        result.success = True
        result.error = "test error"
        result.exif_data = {"modified": "data"}
        result.processing_time = 3.0

        assert result.dest_key == "modified/test.jpg"
        assert result.success is True
        assert result.error == "test error"
        assert result.exif_data == {"modified": "data"}
        assert result.processing_time == 3.0

    def test_processing_result_exif_data_factory(self):
        """Test that exif_data uses factory function for default."""
        result1 = ProcessingResult(source_key="test1.jpg")
        result2 = ProcessingResult(source_key="test2.jpg")

        # Should be different dict instances
        assert result1.exif_data is not result2.exif_data

        # Modifying one shouldn't affect the other
        result1.exif_data["test"] = "value"
        assert "test" not in result2.exif_data

    def test_processing_result_success_error_scenarios(self):
        """Test different success/error scenarios."""
        # Success case
        success_result = ProcessingResult(
            source_key="success.jpg", success=True, error=""
        )
        assert success_result.success is True
        assert success_result.error == ""

        # Error case
        error_result = ProcessingResult(
            source_key="error.jpg", success=False, error="File not found"
        )
        assert error_result.success is False
        assert error_result.error == "File not found"

    def test_processing_result_with_complex_exif_data(self):
        """Test ProcessingResult with complex EXIF data."""
        complex_exif = {
            "make": "Canon",
            "model": "EOS R5",
            "datetime": "2024:01:01 12:00:00",
            "iso": 100,
            "aperture": 2.8,
            "exposure_time": "1/100",
            "nested": {"gps": {"lat": 40.7128, "lon": -74.0060}},
        }

        result = ProcessingResult(source_key="complex.jpg", exif_data=complex_exif)

        assert result.exif_data == complex_exif
        assert result.exif_data["make"] == "Canon"
        assert result.exif_data["nested"]["gps"]["lat"] == 40.7128
