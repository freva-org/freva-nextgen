"""Tests for the commandline interface."""

import json
from typer.testing import CliRunner

from freva_databrowser.databrowser_cli import app


def test_search_files(cli_runner: CliRunner) -> None:
    """Test searching for files."""
    res = cli_runner.invoke(app, ["data-search", "--host", "localhost:8080"])
    assert res.exit_code == 0
    assert res.stdout
    res = cli_runner.invoke(
        app, ["data-search", "--host", "localhost:8080", "model=bar"]
    )
    assert res.exit_code == 0
    assert not res.stdout
    res = cli_runner.invoke(
        app, ["data-search", "--host", "localhost:8080", "--json"]
    )
    assert res.exit_code == 0
    assert isinstance(json.loads(res.stdout), list)


def test_metadata_search(cli_runner: CliRunner) -> None:
    """Test the metadata-search sub command."""
    res = cli_runner.invoke(
        app, ["metadata-search", "--host", "localhost:8080"]
    )
    assert res.exit_code == 0
    assert res.stdout
    res = cli_runner.invoke(
        app, ["metadata-search", "--host", "localhost:8080", "model=bar"]
    )
    assert res.exit_code == 0
    assert res.stdout
    res = cli_runner.invoke(
        app, ["metadata-search", "--host", "localhost:8080", "--json"]
    )
    assert res.exit_code == 0
    output = json.loads(res.stdout)
    assert isinstance(output, dict)
    res = cli_runner.invoke(
        app,
        ["metadata-search", "--host", "localhost:8080", "--json", "model=b"],
    )
    assert res.exit_code == 0
    assert isinstance(json.loads(res.stdout), dict)


def test_count_values(cli_runner: CliRunner) -> None:
    """Test the count sub command."""
    res = cli_runner.invoke(app, ["count", "--host", "localhost:8080"])
    assert res.exit_code == 0
    assert res.stdout
    res = cli_runner.invoke(
        app, ["count", "--host", "localhost:8080", "--json"]
    )
    assert res.exit_code == 0
    assert isinstance(json.loads(res.stdout), int)

    res = cli_runner.invoke(app, ["count", "*", "--host", "localhost:8080"])
    assert res.exit_code == 0
    assert res.stdout
    res = cli_runner.invoke(
        app, ["count", "--facet", "*", "--host", "localhost:8080", "--json"]
    )
    assert res.exit_code == 0
    assert isinstance(json.loads(res.stdout), dict)


def test_failed_command(cli_runner: CliRunner) -> None:
    for cmd in ("count", "data-search", "metadata-search"):
        res = cli_runner.invoke(
            app, [cmd, "--host", "localhost:8080", "foo=b"]
        )
        assert res.exit_code == 0
        assert "warning" in res.stderr.lower()
        res = cli_runner.invoke(
            app, [cmd, "--host", "localhost:8080", "-f", "foo"]
        )
        assert res.exit_code != 0
        res = cli_runner.invoke(app, [cmd, "--host", "foo"])
        assert res.exit_code != 0
