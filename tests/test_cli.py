"""Test the command line interface cli."""

import os
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from types import TracebackType
from typing import Annotated, Dict, List, Optional, Union

import mock
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from freva_client import __version__
from freva_client.cli import app as cli_app
from freva_rest.cli import cli, get_cert_file


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


def test_get_cert_file() -> None:
    """Test getting the cerfiles."""
    out1, out2 = get_cert_file(None, None, None)
    assert out1 == out2 == ""
    out1, out2 = get_cert_file("foo", None, None)
    assert out1.startswith("foo")
    assert out2.startswith("foo")
    out1, out2 = get_cert_file("foo", None, "bar")
    assert out1.startswith("foo")
    assert out2.startswith("bar")
    out1, out2 = get_cert_file("foo", "bar", "baz")
    assert out1.startswith("bar")
    assert out2.startswith("baz")


def test_data_loader_cli(mocker: MockerFixture, loader_config: bytes) -> None:
    """Test the data-loader command line interface."""
    mock_run = mocker.patch("data_portal_worker.cli._main")
    mock_reload = mocker.patch("data_portal_worker.cli.run_process")
    with NamedTemporaryFile(suffix=".json") as temp_f:
        Path(temp_f.name).write_bytes(loader_config)
        from data_portal_worker.cli import run_data_loader

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
        mock_reload.assert_called_once_with(
            Path.cwd()
            / "freva-data-portal-worker"
            / "src"
            / "data_portal_worker",
            target=mock_run,
            args=(Path(temp_f.name),),
            kwargs=dict(
                port=1234,
                exp=20,
                redis_host="redis://example.com:1234",
                dev=True,
                redis_password="secret",
                redis_ssl_certfile=Path(os.getenv("API_REDIS_SSL_CERTFILE", "")),
                redis_ssl_keyfile=Path(os.getenv("API_REDIS_SSL_KEYFILE", "")),
                redis_user="redis",
            ),
        )
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
        mock_run.assert_called_once_with(
            Path(temp_f.name),
            port=4321,
            exp=10,
            redis_host="redis://example.com:1234",
            dev=False,
            redis_password="secret",
            redis_ssl_certfile=Path(os.getenv("API_REDIS_SSL_CERTFILE", "")),
            redis_ssl_keyfile=Path(os.getenv("API_REDIS_SSL_KEYFILE", "")),
            redis_user="redis",
        )


def test_rest_cli(mocker: MockerFixture) -> None:
    """Test the rest api command line interface."""
    mock_run = mocker.patch("uvicorn.run")
    with TemporaryDirectory() as temp_dir:
        MockTempfile.temp_dir = temp_dir
        with mock.patch("freva_rest.cli.NamedTemporaryFile", MockTempfile):
            cli(["--dev"])
            mock_run.assert_called_once_with(
                "freva_rest.api:app",
                host="0.0.0.0",
                port=7777,
                reload=True,
                log_level=20,
                workers=None,
                env_file=str(Path(temp_dir) / "foo.txt"),
            )
            cli(["--debug"])
            mock_run.assert_called_with(
                "freva_rest.api:app",
                host="0.0.0.0",
                port=7777,
                reload=False,
                log_level=10,
                workers=8,
                env_file=str(Path(temp_dir) / "foo.txt"),
            )


def test_main(cli_runner: CliRunner) -> None:
    """Test the functionality of the freva_client main app."""
    res = cli_runner.invoke(cli_app, ["-V"])
    assert res.exit_code == 0
    assert res.stdout
    assert __version__ in res.stdout


def test_cli_utils() -> None:
    """Test functionality of some cli related utils."""
    from freva_rest.cli import _dict_to_defaults, _is_type_annotation
    from freva_rest.config import env_to_int

    with mock.patch.dict(os.environ, {"DEBUG": "1"}, clear=False):
        assert env_to_int("DEBUG", 0) == 1
    assert env_to_int("DEBUG", 0) == 0

    assert _dict_to_defaults({}) == []
    assert _dict_to_defaults({"foo": "bar"}) == [("foo", "bar")]
    assert _dict_to_defaults({"foo": ["1", "2"]}) == [("foo", "1"), ("foo", "2")]

    inner_type = Union[Dict[str, str], List[str]]
    assert _is_type_annotation(inner_type, dict) is True
    assert (
        _is_type_annotation(Annotated[Optional[inner_type], "foo"], dict) is True
    )
