import logging
from unittest.mock import patch

import pytest

from images_pipeline.core.exceptions import (
    ImageProcessingError,
    with_error_handling,
)


@with_error_handling
def _fail_func() -> None:
    raise ValueError("boom")


def test_with_error_handling_raises_image_processing_error() -> None:
    with pytest.raises(ImageProcessingError):
        _fail_func()


def test_with_error_handling_logs_error() -> None:
    with patch("images_pipeline.core.exceptions.get_logger") as mock_get_logger:
        mock_logger = logging.getLogger("test")
        mock_get_logger.return_value = mock_logger
        with pytest.raises(ImageProcessingError):
            _fail_func()
        assert mock_get_logger.called
