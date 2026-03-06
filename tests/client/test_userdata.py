"""Tests for add user data to the databrowser.

These tests exercise both the CLI and REST API interfaces for
adding and deleting user data in the databrowser.

Authentication is handled via ``--token-file`` for CLI commands and
``mock_authenticate`` for Python API tests.
"""

from pathlib import Path
from typing import Dict, List, Union

import requests
from py_oidc_auth_client import Token
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from freva_client import databrowser
from freva_client.cli.databrowser_cli import databrowser_app as app


class TestDeleteUserdataCli:
    """Tests for deleting user data through the CLI."""

    def test_delete_all_userdata(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """Deleting all user data via CLI should succeed."""
        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        res = cli_runner.invoke(
            app,
            [
                "user-data",
                "delete",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )
        _ = cli_runner.invoke(
            app, ["data-count", "--flavour", "user", "--host", test_server]
        )
        assert res.exit_code == 0


class TestUserdataApiDirect:
    """Tests for user data via the REST API directly."""

    def test_add_202(
        self,
        test_server: str,
        auth: Token,
        user_data_payload_sample: Dict[str, Union[List[str], Dict[str, str]]],
    ) -> None:
        """POST user data with valid metadata should return 202."""
        token = auth["access_token"]
        data = user_data_payload_sample
        # first delete
        requests.delete(
            f"{test_server}/databrowser/userdata",
            json={},
            headers={"Authorization": f"Bearer {token}"},
        )
        # then add
        response = requests.post(
            f"{test_server}/databrowser/userdata",
            json=data,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 202

    def test_add_duplicate_202(
        self,
        test_server: str,
        auth: Token,
        user_data_payload_sample_partially_success: Dict[
            str, Union[List[str], Dict[str, str]]
        ],
    ) -> None:
        """POST user data with partial success should return 202."""
        token = auth["access_token"]
        response = requests.post(
            f"{test_server}/databrowser/userdata",
            json=user_data_payload_sample_partially_success,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 202

    def test_add_422(self, test_server: str, auth: Token) -> None:
        """POST user data with invalid metadata should return 422."""
        token = auth["access_token"]
        data = {
            "user_metadata": {
                "project": "cmip5",
                "experiment": "something",
            },
            "facets": {"product": "johndoe"},
        }
        response = requests.post(
            f"{test_server}/databrowser/userdata",
            json=data,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 422

    def test_delete_202(self, test_server: str, auth: Token) -> None:
        """DELETE user data with empty body should return 202."""
        token = auth["access_token"]
        response = requests.delete(
            f"{test_server}/databrowser/userdata",
            json={},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 202

    def test_add_500(self, test_server: str, auth: Token) -> None:
        """POST user data with invalid user_metadata should return 500."""
        token = auth["access_token"]
        data = {
            "user_metadata": [
                {"variable": "tas", "time_frequency": "mon"}
            ],
            "facets": {"product": "johndoe"},
        }
        response = requests.post(
            f"{test_server}/databrowser/userdata",
            json=data,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 500

    def test_delete_422(self, test_server: str, auth: Token) -> None:
        """DELETE user data with invalid body should return 422."""
        token = auth["access_token"]
        data = {
            "user_metadata": [
                {"variable": "tas", "time_frequency": "mon"}
            ],
            "facets": {"product": "johndoe"},
        }
        response = requests.delete(
            f"{test_server}/databrowser/userdata",
            json=data,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 422


class TestAddUserdataCli:
    """Tests for adding user data through the CLI."""

    def test_add_standard(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """Adding user data from a directory should increase count."""
        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        res_count_before = len(databrowser(flavour="user", host=test_server))

        res = cli_runner.invoke(
            app,
            [
                "user-data",
                "add",
                "--path",
                "./freva-rest",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )
        _ = cli_runner.invoke(
            app, ["data-count", "--flavour", "user", "--host", test_server]
        )
        res_count_after = len(databrowser(flavour="user", host=test_server))
        assert res.exit_code == 0
        assert res_count_after > res_count_before

    def test_add_single_file_and_delete(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """Adding a single file and deleting it."""
        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        res_count_before = len(databrowser(flavour="user", host=test_server))

        res_add = cli_runner.invoke(
            app,
            [
                "user-data",
                "add",
                "--path",
                (
                    "./freva-rest/src/freva_rest/databrowser_api/mock/data/model/global/"
                    "cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/Amon/ua/"
                    "gn/v20190815/"
                    "ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc"
                ),
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )
        assert res_add.exit_code == 0
        res_count_middle = len(databrowser(flavour="user", host=test_server))
        assert res_count_before <= res_count_middle

        res_del = cli_runner.invoke(
            app,
            [
                "user-data",
                "delete",
                "-s",
                (
                    "file=./freva-rest/src/freva_rest/databrowser_api/mock/data/model/"
                    "global/cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/"
                    "Amon/ua/gn/v20190815/"
                    "ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc"
                ),
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )
        res_count_after = len(databrowser(flavour="user", host=test_server))
        assert res_del.exit_code == 0
        assert res_count_middle == res_count_after

    def test_add_broken_file(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """Adding a broken netCDF file should not change the count."""
        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        res_count_before = len(databrowser(flavour="user", host=test_server))
        res = cli_runner.invoke(
            app,
            [
                "user-data",
                "add",
                "--path",
                "./freva-rest/src/freva_rest/databrowser_api/mock_broken/bears.nc",
                "--facet",
                "product=johndoe",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )
        assert res.exit_code == 0
        res_count_after = len(databrowser(flavour="user", host=test_server))
        assert res_count_before == res_count_after

        # cleanup
        cli_runner.invoke(
            app,
            [
                "user-data",
                "delete",
                "janedoe",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )


class TestWrongFacetFormat:
    """Tests for invalid facet format in CLI user data commands."""

    def test_wrong_equal_facet(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """Using ':' instead of '=' in facets should fail."""
        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        res_count_before = len(databrowser(flavour="user", host=test_server))
        res = cli_runner.invoke(
            app,
            [
                "user-data",
                "add",
                "--path",
                (
                    "./freva-rest/src/freva_rest/databrowser_api/mock/data/model/global/"
                    "cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/Amon/"
                    "ua/gn/v20190815/"
                    "ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc"
                ),
                "--facet",
                "product:johndoe",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )
        assert res.exit_code == 1
        res_count_after = len(databrowser(flavour="user", host=test_server))
        assert res_count_before == res_count_after

        # cleanup with wrong format should not affect anything
        cli_runner.invoke(
            app,
            [
                "user-data",
                "delete",
                "-s",
                (
                    "file:./freva-rest/src/freva_rest/databrowser_api/mock/data/model/"
                    "global/cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/"
                    "Amon/ua/gn/v20190815/"
                    "ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc"
                ),
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )


class TestSolrFailures:
    """Tests for behaviour when Solr is unavailable."""

    def test_no_solr_post(
        self, test_server: str, auth: Token, mocker: MockerFixture
    ) -> None:
        """POST user data with Solr down should return 500."""
        token = auth["access_token"]
        data = {
            "user_metadata": [
                {
                    "variable": "tas",
                    "time_frequency": "mon",
                    "time": "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                    "file": "path of the file",
                }
            ],
            "facets": {},
        }
        mocker.patch("freva_rest.rest.server_config.solr_host", "foo.bar")
        res = requests.post(
            f"{test_server}/databrowser/userdata",
            json=data,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert res.status_code == 500

    def test_no_solr_delete(
        self, test_server: str, auth: Token, mocker: MockerFixture
    ) -> None:
        """DELETE user data with Solr down should return 500."""
        token = auth["access_token"]
        mocker.patch("freva_rest.rest.server_config.solr_host", "foo.bar")
        res = requests.delete(
            f"{test_server}/databrowser/userdata",
            json={},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert res.status_code == 500
