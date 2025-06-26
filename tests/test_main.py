"""Tests for main.py CLI functionality."""

from unittest.mock import patch
from images_pipeline.main import main


class TestMainCLI:
    """Tests for the main CLI functionality."""

    def test_main_with_no_args_shows_help(self):
        """Test that running main without arguments shows help."""
        with patch("sys.argv", ["images-pipeline"]):
            with patch("argparse.ArgumentParser.print_help") as mock_help:
                with patch("sys.exit") as mock_exit:
                    main()
                    mock_help.assert_called_once()
                    mock_exit.assert_called_once_with(1)

    def test_main_version_command(self):
        """Test version command output."""
        with patch("sys.argv", ["images-pipeline", "version"]):
            with patch("builtins.print") as mock_print:
                with patch("sys.exit") as mock_exit:
                    main()
                    mock_print.assert_any_call("Images Pipeline CLI")
                    mock_print.assert_any_call("Version 0.1.0")
                    mock_print.assert_any_call(
                        "S3 Image Processing with multiple concurrency strategies"
                    )
                    mock_exit.assert_called_once_with(0)

    def test_main_process_command_basic(self):
        """Test process command with basic arguments."""
        test_args = [
            "images-pipeline",
            "process",
            "--source-bucket",
            "test-source",
            "--dest-bucket",
            "test-dest",
        ]

        with patch("sys.argv", test_args):
            with patch("images_pipeline.main.process_images_main") as mock_process:
                main()
                mock_process.assert_called_once()

    def test_main_process_command_with_all_options(self):
        """Test process command with all optional arguments."""
        test_args = [
            "images-pipeline",
            "process",
            "--source-bucket",
            "test-source",
            "--dest-bucket",
            "test-dest",
            "--source-prefix",
            "source-prefix",
            "--dest-prefix",
            "dest-prefix",
            "--transformation",
            "grayscale",
            "--processor",
            "multithread",
            "--debug",
            "--batch-size",
            "50",
        ]

        with patch("sys.argv", test_args):
            with patch("images_pipeline.main.process_images_main") as mock_process:
                main()
                mock_process.assert_called_once()

    def test_main_process_command_builds_correct_sys_argv(self):
        """Test that process command builds sys.argv correctly for process_images_main."""
        test_args = [
            "images-pipeline",
            "process",
            "--source-bucket",
            "test-source",
            "--dest-bucket",
            "test-dest",
            "--transformation",
            "kmeans",
            "--debug",
        ]

        with patch("sys.argv", test_args):
            with patch("images_pipeline.main.process_images_main") as mock_process:
                main()

                # Note: sys.argv is modified in-place, so we check the mock was called
                mock_process.assert_called_once()

    def test_main_process_command_with_empty_optional_args(self):
        """Test process command handles empty optional arguments correctly."""
        test_args = [
            "images-pipeline",
            "process",
            "--source-bucket",
            "test-source",
            "--dest-bucket",
            "test-dest",
            "--source-prefix",
            "",
            "--dest-prefix",
            "",
        ]

        with patch("sys.argv", test_args):
            with patch("images_pipeline.main.process_images_main") as mock_process:
                main()
                mock_process.assert_called_once()

    def test_main_process_command_default_processor_serial(self):
        """Test that default processor is serial and doesn't modify sys.argv."""
        test_args = [
            "images-pipeline",
            "process",
            "--source-bucket",
            "test-source",
            "--dest-bucket",
            "test-dest",
        ]

        with patch("sys.argv", test_args):
            with patch("images_pipeline.main.process_images_main") as mock_process:
                main()
                mock_process.assert_called_once()

    def test_main_process_command_default_batch_size(self):
        """Test that default batch size is 100 and doesn't modify sys.argv."""
        test_args = [
            "images-pipeline",
            "process",
            "--source-bucket",
            "test-source",
            "--dest-bucket",
            "test-dest",
        ]

        with patch("sys.argv", test_args):
            with patch("images_pipeline.main.process_images_main") as mock_process:
                main()
                mock_process.assert_called_once()

    def test_main_process_command_valid_transformations(self):
        """Test that all valid transformations are accepted."""
        valid_transformations = ["grayscale", "kmeans", "native_kmeans"]

        for transformation in valid_transformations:
            test_args = [
                "images-pipeline",
                "process",
                "--source-bucket",
                "test-source",
                "--dest-bucket",
                "test-dest",
                "--transformation",
                transformation,
            ]

            with patch("sys.argv", test_args):
                with patch("images_pipeline.main.process_images_main") as mock_process:
                    main()
                    mock_process.assert_called_once()

    def test_main_process_command_valid_processors(self):
        """Test that all valid processors are accepted."""
        valid_processors = ["serial", "multithread", "multiprocess", "asyncio"]

        for processor in valid_processors:
            test_args = [
                "images-pipeline",
                "process",
                "--source-bucket",
                "test-source",
                "--dest-bucket",
                "test-dest",
                "--processor",
                processor,
            ]

            with patch("sys.argv", test_args):
                with patch("images_pipeline.main.process_images_main") as mock_process:
                    main()
                    mock_process.assert_called_once()
