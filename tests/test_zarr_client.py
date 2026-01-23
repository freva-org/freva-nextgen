"""Test client functions for zarr."""

from copy import deepcopy
from pathlib import Path

import requests
import xarray as xr
from typer.testing import CliRunner

from freva_client import databrowser
from freva_client.auth import Auth, Token
from freva_client.cli.databrowser_cli import databrowser_app
from freva_client.cli.zarr_cli import zarr_app
from freva_client.zarr_utils import convert, status


def test_aggregation(test_server: str, auth_instance: Auth, auth: Token) -> None:
    """Test datarowser aggregation."""

    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth
        db = databrowser(host=test_server, dataset="agg")
        url = db.aggregate("auto")
        headers = {"Authorization": f"Bearer {db.auth_token['access_token']}"}
        _ = requests.get(f"{url}/.zgroup", headers=headers)
        url = db.aggregate("auto", zarr_options={"public": True})
        _ = requests.get(f"{url}/.zgroup")

    finally:
        auth_instance._auth_token = token


def test_convert(test_server: str, auth_instance: Auth, auth: Token) -> None:
    """Test zarr conversion."""

    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth
        db = databrowser(host=test_server, dataset="cmip6-fs")
        files = list(db)
        headers = {"Authorization": f"Bearer {db.auth_token['access_token']}"}
        urls = convert(*files, host=test_server)
        assert isinstance(
            xr.open_zarr(urls[0], storage_options={"headers": headers}),
            xr.Dataset,
        )
        urls = convert(*files, host=test_server, zarr_options={"public": True})
        assert isinstance(xr.open_zarr(urls[0]), xr.Dataset)

    finally:
        auth_instance._auth_token = token


def test_aggregation_cli(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth, token_file: Path
) -> None:
    """Test the cli for aggregation."""

    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None
        res = cli_runner.invoke(
            databrowser_app,
            [
                "data-search",
                "dataset=agg",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--public",
                "--aggregate",
                "auto",
            ],
        )
        assert res.exit_code == 0
    finally:
        auth_instance._auth_token = token


def test_convert_cli(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth, token_file: Path
) -> None:
    """Test the cli for aggregation."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None
        files = list(databrowser(dataset="agg", host=test_server))
        res = cli_runner.invoke(
            zarr_app,
            ["convert"]
            + files
            + [
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )
        assert res.exit_code == 0
        res = cli_runner.invoke(
            zarr_app,
            ["convert"]
            + files
            + [
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--json",
            ],
        )
        assert res.exit_code == 0

    finally:
        auth_instance._auth_token = token


def test_status(test_server: str) -> None:
    """Test getting the status."""
    stat = status(
        "{test_server}/data-portal/share/foo/bar.zarr", host=test_server
    )
    assert isinstance(stat, dict)
    assert stat["status"] == 5


def test_status_cli(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth, token_file: Path
) -> None:
    """Test the cli for aggregation."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None
        res = cli_runner.invoke(
            zarr_app,
            [
                "status",
                f"{test_server}/data-portal/zarr/foo.nc",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "-vvv",
            ],
        )
        assert res.exit_code == 0
    finally:
        auth_instance._auth_token = token
