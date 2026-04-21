"""Test client functions for zarr.

These tests exercise zarr conversion, aggregation, and status checking
via both the Python API and CLI.

Authentication is handled via ``mock_authenticate`` for Python API tests
and ``--token-file`` for CLI tests.
"""

import time
from pathlib import Path

import pytest
import requests
import xarray as xr
from py_oidc_auth_client import Token
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from freva_client import databrowser
from freva_client.cli.databrowser_cli import databrowser_app
from freva_client.cli.zarr_cli import zarr_app
from freva_client.zarr_utils import convert, status


class _MockResp:
    """Mock response that returns an unexpected JSON payload."""

    def json(self):
        return {"foo": ["bar"]}


class TestAggregation:
    """Tests for databrowser aggregation functionality."""

    def test_aggregation(self, test_server: str, mock_authenticate: Token) -> None:
        """Aggregation should return a valid zarr URL."""
        db = databrowser(host=test_server, dataset="agg")
        url = db.aggregate("auto")
        headers = {"Authorization": f"Bearer {mock_authenticate['access_token']}"}
        _ = requests.get(f"{url}/.zgroup", headers=headers)

    def test_aggregation_public(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Public aggregation should not need auth headers."""
        db = databrowser(host=test_server, dataset="agg")
        url = db.aggregate("auto", zarr_options={"public": True})
        _ = requests.get(f"{url}/.zgroup")


class TestConvert:
    """Tests for zarr conversion functionality."""

    def test_convert_ok(self, test_server: str, mock_authenticate: Token) -> None:
        """Converting files to zarr should produce openable datasets."""
        db = databrowser(host=test_server, dataset="cmip6-fs")
        files = list(db)
        headers = {"Authorization": f"Bearer {mock_authenticate['access_token']}"}
        urls = convert(*files, host=test_server)
        time.sleep(2)
        assert isinstance(
            xr.open_zarr(urls[0], storage_options={"headers": headers}),
            xr.Dataset,
        )

    def test_convert_public(self, test_server: str, mock_authenticate: Token) -> None:
        """Public zarr conversion should produce publicly openable datasets."""
        db = databrowser(host=test_server, dataset="cmip6-fs")
        files = list(db)
        urls = convert(*files, host=test_server, zarr_options={"public": True})
        assert isinstance(xr.open_zarr(urls[0]), xr.Dataset)

    def test_convert_fail(
        self,
        test_server: str,
        mock_authenticate: Token,
        mocker: MockerFixture,
    ) -> None:
        """Conversion with a mocked bad response should raise ValueError."""
        db = databrowser(host=test_server, dataset="cmip6-fs")
        files = list(db)
        mocker.patch("freva_client.zarr_utils.do_request", return_value=_MockResp())
        with pytest.raises(ValueError):
            _ = convert(*files, host=test_server)


class TestAggregationCli:
    """Tests for aggregation through the CLI."""

    def test_aggregation_cli(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """The aggregation CLI command should succeed."""
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


class TestConvertCli:
    """Tests for zarr conversion through the CLI."""

    def test_convert_cli(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """The convert CLI command should succeed."""
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

    def test_convert_cli_json(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """The convert CLI command with --json should succeed."""
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
                "--json",
            ],
        )
        assert res.exit_code == 0


class TestStatus:
    """Tests for zarr store status checking."""

    def test_status(
        self,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """Querying status of a zarr store should return a dict."""
        stat = status(
            f"   {test_server}/data-portal/share/foo/bar.zarr",
            host=test_server,
        )
        assert isinstance(stat, dict)
        assert stat["status"] == 5

    def test_status_with_use_token_strategy(
        self,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
        mocker: MockerFixture,
    ) -> None:
        """Status query with use_token strategy should work."""
        mocker.patch(
            "freva_client.utils.choose_token_strategy"
        ).return_value = "use_token"
        stat = status(
            f"   {test_server}/data-portal/share/foo/bar.zarr",
            host=test_server,
        )
        assert isinstance(stat, dict)

    def test_status_cli(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """The status CLI command should succeed."""
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


class TestEnum:
    """Tests for CLI enum types."""

    def test_enum_equivalence(self) -> None:
        """String and enum enum values should produce equivalent dicts."""
        from freva_client.cli.zarr_cli import (
            AggregationCombine,
            AggregationOption,
        )

        a1 = AggregationOption(join="minimal").to_dict()
        a2 = AggregationOption(join=AggregationCombine.minimal).to_dict()
        assert a1 == a2


class TestAggregationFailures:
    """Tests for failing aggregation scenarios."""

    def test_aggregate_nonexistent_dataset(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Aggregating a non-existent dataset should raise FileNotFoundError."""
        db = databrowser(host=test_server, dataset="foobar")
        with pytest.raises(FileNotFoundError):
            db.aggregate("auto")

    def test_aggregate_bad_compat(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Aggregating with invalid compat should raise ValueError."""
        db = databrowser(host=test_server, dataset="agg")
        _ = list(db)
        with pytest.raises(ValueError):
            db.aggregate("auto", dim="ensemble", compat="inner")

    def test_convert_bad_compat(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Converting with invalid compat should raise ValueError."""
        db = databrowser(host=test_server, dataset="agg")
        files = list(db)
        with pytest.raises(ValueError):
            convert(*files, aggregate="auto", compat="inner", host=test_server)
