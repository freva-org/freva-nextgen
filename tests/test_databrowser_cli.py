"""Tests for the commandline interface."""

import json
import subprocess
import zipfile
from copy import deepcopy
from pathlib import Path
from tempfile import NamedTemporaryFile

from pytest import LogCaptureFixture
from typer.testing import CliRunner

from freva_client.auth import Auth
from freva_client.cli.databrowser_cli import databrowser_app as app


def test_overview(cli_runner: CliRunner, test_server: str) -> None:
    """Test the overview sub command."""
    res = cli_runner.invoke(app, ["data-overview", "--host", test_server])
    assert res.exit_code == 0
    assert res.stdout


def test_search_files_normal(cli_runner: CliRunner, test_server: str) -> None:
    """Test searching for files (no zarr)."""
    res = cli_runner.invoke(app, ["data-search", "--host", test_server])
    assert res.exit_code == 0
    assert res.stdout
    res = cli_runner.invoke(
        app,
        [
            "data-search",
            "--host",
            test_server,
            "project=cmip6",
            "project=bar",
            "model=foo",
        ],
    )
    assert res.exit_code == 0
    assert not res.stdout
    res = cli_runner.invoke(app, ["data-search", "--host", test_server, "--json"])
    assert res.exit_code == 0
    assert isinstance(json.loads(res.stdout), list)


def test_search_files_zarr(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth, token_file: Path
) -> None:
    """Test searching for files (with zarr)."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None
        res = cli_runner.invoke(
            app, ["data-search", "--host", test_server, "--zar"]
        )
        assert res.exit_code > 0
        auth_instance._auth_token = None
        res = cli_runner.invoke(
            app,
            [
                "data-search",
                "--host",
                test_server,
                "--zarr",
                "--token-file",
                str(token_file),
                "dataset=cmip6-fs",
                "--json",
            ],
        )
        assert res.exit_code == 0
        assert res.stdout
        assert isinstance(json.loads(res.stdout), list)
        auth_instance._auth_token = None
        res = cli_runner.invoke(
            app,
            [
                "data-search",
                "--host",
                test_server,
                "--zarr",
                "dataset=cmip6-fs",
                "--json",
            ],
        )
        assert res.exit_code != 0
    finally:
        auth_instance._auth_token = token


def test_intake_catalogue_no_zarr(
    cli_runner: CliRunner, test_server: str
) -> None:
    """Test intake catalgoue creation without zarr."""

    res = cli_runner.invoke(app, ["intake-catalogue", "--host", test_server])
    assert res.exit_code == 0
    assert res.stdout
    assert isinstance(json.loads(res.stdout), dict)

    with NamedTemporaryFile(suffix=".json") as temp_f:
        res = cli_runner.invoke(
            app, ["intake-catalogue", "--host", test_server, "-f", temp_f.name]
        )
        assert res.exit_code == 0
        with open(temp_f.name, "r") as stream:
            assert isinstance(json.load(stream), dict)


def test_stac_catalogue(
    cli_runner: CliRunner, test_server: str, temp_dir: Path
) -> None:
    """Test STAC Catalogue"""
    output_file = temp_dir / "something.zip"
    # we check the STAC items in the output file
    res = cli_runner.invoke(
        app,
        ["stac-catalogue", "--host", test_server, "--filename", output_file],
    )
    assert res.exit_code == 0
    with zipfile.ZipFile(output_file, "r") as zip_file:
        for member in zip_file.namelist():
            if "/items/" in member and member.endswith(".json"):
                item_content = zip_file.read(member)
                temp_item_path = temp_dir / "test_item.json"
                temp_item_path.write_bytes(item_content)
                res_stac = subprocess.run(
                    ["stac-check", str(temp_item_path)],
                    check=True,
                    capture_output=True,
                )
                assert "ITEM Passed: True" in res_stac.stdout.decode("utf-8")

                break
    # failed test with static STAC catalogue - wrong output
    res = cli_runner.invoke(
        app, ["stac-catalogue", "--host", test_server, "--filename", "/foo/bar"]
    )
    assert res.exit_code == 1

    # failed test with static STAC catalogue - wrong search params
    res = cli_runner.invoke(
        app,
        [
            "stac-catalogue",
            "--host",
            test_server,
            "--filename",
            "/foo/bar" "foo=b",
        ],
    )
    assert res.exit_code == 1

    # None filename
    res = cli_runner.invoke(app, ["stac-catalogue", "--host", test_server])
    assert res.exit_code == 0
    assert (
        "Downloading the STAC catalog started ...\nSTAC catalog saved to: "
        in res.stdout
    )


def test_intake_files_zarr(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth, token_file: Path
) -> None:
    """Test searching for files (with zarr)."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None
        res = cli_runner.invoke(
            app, ["inktake-catalogue", "--host", test_server, "--zar"]
        )
        assert res.exit_code > 0
        auth_instance._auth_token = None
        res = cli_runner.invoke(
            app,
            [
                "intake-catalogue",
                "--host",
                test_server,
                "--zarr",
                "--token-file",
                str(token_file),
                "dataset=cmip6-fs",
            ],
        )
        assert res.exit_code == 0
        assert res.stdout
        assert isinstance(json.loads(res.stdout), dict)
    finally:
        auth_instance._auth_token = token


def test_metadata_search(cli_runner: CliRunner, test_server: str) -> None:
    """Test the metadata-search sub command."""
    res = cli_runner.invoke(app, ["metadata-search", "--host", test_server])
    assert res.exit_code == 0
    assert res.stdout
    res = cli_runner.invoke(
        app, ["metadata-search", "--host", test_server, "model=bar"]
    )
    assert res.exit_code == 0
    assert res.stdout
    res = cli_runner.invoke(
        app, ["metadata-search", "--host", test_server, "--json"]
    )
    assert res.exit_code == 0
    output = json.loads(res.stdout)
    assert isinstance(output, dict)
    res = cli_runner.invoke(
        app,
        ["metadata-search", "--host", test_server, "--json", "model=b"],
    )
    assert res.exit_code == 0
    assert isinstance(json.loads(res.stdout), dict)


def test_count_values(cli_runner: CliRunner, test_server: str) -> None:
    """Test the count sub command."""
    res = cli_runner.invoke(app, ["data-count", "--host", test_server])
    assert res.exit_code == 0
    assert res.stdout
    res = cli_runner.invoke(app, ["data-count", "--host", test_server, "--json"])
    assert res.exit_code == 0
    assert isinstance(json.loads(res.stdout), int)

    res = cli_runner.invoke(app, ["data-count", "*", "--host", test_server])
    assert res.exit_code == 0
    assert res.stdout
    res = cli_runner.invoke(
        app,
        [
            "data-count",
            "--facet",
            "ocean",
            "--host",
            test_server,
            "--json",
            "-d",
        ],
    )
    assert res.exit_code == 0
    assert isinstance(json.loads(res.stdout), dict)
    res = cli_runner.invoke(
        app, ["data-count", "--facet", "ocean", "--host", test_server, "-d"]
    )
    assert res.exit_code == 0
    assert res.stdout
    res = cli_runner.invoke(
        app,
        [
            "data-count",
            "--facet",
            "ocean",
            "--host",
            test_server,
            "realm=atmos",
            "--json",
        ],
    )
    assert res.exit_code == 0
    assert json.loads(res.stdout) == 0


def test_failed_command(
    cli_runner: CliRunner, caplog: LogCaptureFixture, test_server: str
) -> None:
    """Test the handling of bad commands."""
    for cmd in ("data-count", "data-search", "metadata-search"):
        caplog.clear()
        res = cli_runner.invoke(app, [cmd, "--host", test_server, "foo=b"])
        assert res.exit_code == 0
        assert caplog.records
        assert caplog.records[-1].levelname == "WARNING"
        res = cli_runner.invoke(app, [cmd, "--host", test_server, "-f", "foo"])
        assert res.exit_code != 0
        caplog.clear()
        res = cli_runner.invoke(app, [cmd, "--host", "foo"])
        assert res.exit_code != 0
        assert caplog.records
        assert caplog.records[-1].levelname == "ERROR"
        res = cli_runner.invoke(app, [cmd, "--host", "foo", "-vvvvv"])
        assert res.exit_code != 0
        assert caplog.records
        assert caplog.records[-1].levelname == "ERROR"


def test_check_versions(cli_runner: CliRunner) -> None:
    """Check the versions."""
    for cmd in ("data-count", "data-search", "metadata-search"):
        res = cli_runner.invoke(app, [cmd, "-V"])
        assert res.exit_code == 0
