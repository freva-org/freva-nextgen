"""Test the command line interface cli."""

from typer.testing import CliRunner
from databrowser.cli import cli

from pytest_mock import MockerFixture


def test_cli(mocker: MockerFixture) -> None:
    """Test the command line interface."""
    mock_run = mocker.patch("uvicorn.run")

    runner = CliRunner()
    result1 = runner.invoke(cli, ["--dev"])
    assert result1.exit_code == 0
    # mock_run.assert_called_once_with(
    #    host="0.0.0.0", port=8080, reload=True, log_level=10, workers=None
    # )
    result2 = runner.invoke(cli)
    assert result2.exit_code == 0
