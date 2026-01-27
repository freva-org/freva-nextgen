"""Test client functions for zarr."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
import requests
import xarray as xr
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from freva_client import databrowser
from freva_client.auth import Auth, Token
from freva_client.cli.databrowser_cli import databrowser_app
from freva_client.cli.zarr_cli import zarr_app
from freva_client.zarr_utils import convert, status


class _MockResp:

    def json(self):
        return {"foo": ["bar"]}


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


def test_convert_ok(test_server: str, auth_instance: Auth, auth: Token) -> None:
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


def test_convert_fail(
    test_server: str, auth_instance: Auth, auth: Token, mocker: MockerFixture
) -> None:
    """Test zarr failed conversion."""

    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth
        db = databrowser(host=test_server, dataset="cmip6-fs")
        files = list(db)
        mocker.patch(
            "freva_client.zarr_utils.do_request", return_value=_MockResp()
        )
        with pytest.raises(ValueError):
            _ = convert(*files, host=test_server)

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


def test_status(
    test_server: str, auth_instance: Auth, token_file: Path, mocker: MockerFixture
) -> None:
    """Test getting the status."""
    old_token = deepcopy(auth_instance._auth_token)
    token = json.loads(token_file.read_text())
    try:
        auth_instance._auth_token = token
        stat = status(
            f"   {test_server}/data-portal/share/foo/bar.zarr", host=test_server
        )
        assert isinstance(stat, dict)
        assert stat["status"] == 5
        mocker.patch(
            "freva_client.zarr_utils.choose_token_strategy",
            return_value="use_token",
        )
        stat = status(
            f"   {test_server}/data-portal/share/foo/bar.zarr", host=test_server
        )
        assert isinstance(stat, dict)

    finally:
        auth_instance._auth_token = old_token


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


def test_enum() -> None:
    """Test the enum."""
    from freva_client.cli.zarr_cli import AggregationCombine, AggregationOption

    a1 = AggregationOption(join="minimal").to_dict()
    a2 = AggregationOption(join=AggregationCombine.minimal).to_dict()
    assert a1 == a2


def test_zarr_aggregate_databrowser_fail(
    test_server: str, auth_instance: Auth, token_file: Path
) -> None:
    """Test failing databrowser.aggregate."""
    old_token = deepcopy(auth_instance._auth_token)
    token = json.loads(token_file.read_text())
    try:
        auth_instance._auth_token = token
        db = databrowser(host=test_server, dataset="foobar")
        with pytest.raises(FileNotFoundError):
            db.aggregate("auto")
        db = databrowser(host=test_server, dataset="agg")
        files = list(db)
        with pytest.raises(ValueError):
            db.aggregate("auto", dim="ensemble", compat="inner")
        with pytest.raises(ValueError):
            convert(*files, aggregate="auto", compat="inner")
    finally:
        auth_instance._auth_token = old_token
