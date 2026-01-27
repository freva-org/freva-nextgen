"""Test the command line interface cli."""

from typer.testing import CliRunner

from freva_client import __version__
from freva_client.cli import app as cli_app


def test_main(cli_runner: CliRunner) -> None:
    """Test the functionality of the freva_client main app."""
    res = cli_runner.invoke(cli_app, ["-V"])
    assert res.exit_code == 0
    assert res.stdout
    assert __version__ in res.stdout
