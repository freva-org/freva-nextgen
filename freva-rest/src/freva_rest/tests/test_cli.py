"""Test the command line interface cli."""

from pathlib import Path
from tempfile import TemporaryDirectory
from types import TracebackType

import mock
from freva_rest.cli import cli
from pytest_mock import MockerFixture
from typer.testing import CliRunner


class MockTempfile:
    """Mock the NamedTemporaryFile class."""

    temp_dir: str = "/tmp"

    def __init__(self, *args: str, **kwargs: str) -> None:
        pass

    def __enter__(self) -> "MockTempfile":
        return self

    def __exit__(
        self, exc_type: type, exc_value: Exception, traceback: TracebackType
    ) -> None:
        Path(self.name).unlink()

    @property
    def name(self) -> str:
        """Mock the Tempfile.name method"""
        return str(Path(self.temp_dir) / "foo.txt")


def test_cli(mocker: MockerFixture) -> None:
    """Test the command line interface."""
    mock_run = mocker.patch("uvicorn.run")
    with TemporaryDirectory() as temp_dir:
        MockTempfile.temp_dir = temp_dir
        with mock.patch("freva_rest.cli.NamedTemporaryFile", MockTempfile):
            runner = CliRunner()
            result1 = runner.invoke(cli, ["--dev"])
            assert result1.exit_code == 0
            mock_run.assert_called_once_with(
                "freva_rest.api:app",
                host="0.0.0.0",
                port=8080,
                reload=True,
                log_level=20,
                workers=None,
                env_file=str(Path(temp_dir) / "foo.txt"),
            )
            result2 = runner.invoke(cli, ["--debug"])
            assert result2.exit_code == 0
            mock_run.assert_called_with(
                "freva_rest.api:app",
                host="0.0.0.0",
                port=8080,
                reload=False,
                log_level=10,
                workers=8,
                env_file=str(Path(temp_dir) / "foo.txt"),
            )
