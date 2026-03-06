"""Test the command line interface cli."""

from typer.testing import CliRunner

from freva_client import __version__
from freva_client.cli import app as cli_app


class TestMainCli:
    """Tests for the main freva-client CLI entry point."""

    def test_version_flag(self, cli_runner: CliRunner) -> None:
        """Test that the -V flag prints the version and exits cleanly."""
        res = cli_runner.invoke(cli_app, ["-V"])
        assert res.exit_code == 0
        assert res.stdout
        assert __version__ in res.stdout
