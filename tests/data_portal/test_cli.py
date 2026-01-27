from pathlib import Path
from tempfile import NamedTemporaryFile

from pytest_mock import MockerFixture

from data_portal_worker.cli import run_data_loader


def test_data_loader_cli(mocker: MockerFixture, loader_config: bytes) -> None:
    """Test the data-loader command line interface."""
    mock_run = mocker.patch("data_portal_worker.cli._main")
    mock_reload = mocker.patch("data_portal_worker.cli.run_process")
    with NamedTemporaryFile(suffix=".json") as temp_f:
        Path(temp_f.name).write_bytes(loader_config)

        run_data_loader(
            [
                "-c",
                temp_f.name,
                "-r",
                "redis://example.com:1234",
                "-p",
                "1234",
                "-e",
                "20",
                "-v",
                "--dev",
            ]
        )
        mock_reload.assert_called_once()
        run_data_loader(
            [
                "-c",
                temp_f.name,
                "-r",
                "redis://example.com:1234",
                "-p",
                "4321",
                "-e",
                "10",
            ]
        )
        mock_run.assert_called_once()
        kwargs = mock_run.call_args[1]
        assert kwargs.get("port") == 4321
        assert kwargs.get("exp") == 10
        assert kwargs.get("redis_host") == "redis://example.com:1234"
        assert kwargs.get("dev") is False
