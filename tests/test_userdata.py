"""Tests for add user data to the databrowser."""

from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Union

import mock
import requests
from typer.testing import CliRunner

from freva_client import databrowser
from freva_client.auth import Auth, Token
from freva_client.cli.databrowser_cli import databrowser_app as app


def test_delete_all_userdata_cli(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth, token_file: Path
) -> None:
    """Test deleting all user data through the CLI."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None

        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        auth_instance._auth_token = None
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
    finally:
        auth_instance._auth_token = token


def test_userdata_add_api_202(
    test_server: str,
    auth: Token,
    user_data_payload_sample: Dict[str, Union[List[str], Dict[str, str]]],
) -> None:
    """Test user data through the API with valid metadata."""
    token = auth["access_token"]
    data = user_data_payload_sample
    # first delete:
    requests.delete(
        f"{test_server}/databrowser/userdata",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    # then add:
    response = requests.post(
        f"{test_server}/databrowser/userdata",
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 202


def test_add_userdata_cli_standard(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth, token_file: Path
) -> None:
    """Test adding user data through the CLI."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None

        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        auth_instance._auth_token = None
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
    finally:
        auth_instance._auth_token = token


def test_add_userdata_cli_all_successful_and_escape_char(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth, token_file: Path
) -> None:
    """Test adding user data through the CLI."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None

        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        auth_instance._auth_token = None
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
        res_count_after = cli_runner.invoke(
            app, ["data-count", "--flavour", "user", "--host", test_server]
        )
        res_count_after = len(databrowser(flavour="user", host=test_server))
        assert res_del.exit_code == 0
        assert res_count_middle == res_count_after
    finally:
        auth_instance._auth_token = token


def test_userdata_add_api_202_duplicate_bulk_error_mongo(
    test_server: str,
    auth: Token,
    user_data_payload_sample_partially_success: Dict[
        str, Union[List[str], Dict[str, str]]
    ],
) -> None:
    """Test user data through the API with valid metadata."""
    token = auth["access_token"]
    response = requests.post(
        f"{test_server}/databrowser/userdata",
        json=user_data_payload_sample_partially_success,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 202


def test_userdata_add_api_422(test_server: str, auth: Token) -> None:
    """Test user data through the API with invalid metadata."""
    token = auth["access_token"]
    data = {
        "user_metadata": {
            "project": "cmip5",
            "experiment": "something",
        },
        "facets": {
            "product": "johndoe",
        },
    }
    response = requests.post(
        f"{test_server}/databrowser/userdata",
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 422


def test_userdata_delete_api_202(test_server: str, auth: Token) -> None:
    """Test user data through the API with valid metadata."""
    token = auth["access_token"]
    response = requests.delete(
        f"{test_server}/databrowser/userdata",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 202


def test_userdata_add_api_500(test_server: str, auth: Token) -> None:
    """Test user data through the API with invalid user_metadata."""
    token = auth["access_token"]
    data = {
        "user_metadata": [{"variable": "tas", "time_frequency": "mon"}],
        "facets": {
            "product": "johndoe",
        },
    }
    response = requests.post(
        f"{test_server}/databrowser/userdata",
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 500


def test_userdata_delete_api_422(test_server: str, auth: Token) -> None:
    """Test user data through the API with invalid metadata."""
    token = auth["access_token"]
    data = {
        "user_metadata": [{"variable": "tas", "time_frequency": "mon"}],
        "facets": {
            "product": "johndoe",
        },
    }
    response = requests.delete(
        f"{test_server}/databrowser/userdata",
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 422


def test_add_userdata_cli_broken_file(
    cli_runner: CliRunner,
    test_server: str,
    auth_instance: Auth,
    token_file: Path,
) -> None:
    """Test adding user broken data through the CLI."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None

        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        auth_instance._auth_token = None
        res_count_before = len(databrowser(flavour="user", host=test_server))
        # add the broken file
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
        res_count_after = cli_runner.invoke(
            app, ["data-count", "--flavour", "user", "--host", test_server]
        )
        assert res.exit_code == 0
        res_count_after = len(databrowser(flavour="user", host=test_server))
        assert res_count_before == res_count_after
        # remove whatever is existing
        res = cli_runner.invoke(
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

    finally:
        auth_instance._auth_token = token


def test_wrong_equal_facet(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth, token_file: Path
) -> None:
    """Test adding user data through the CLI."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None

        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        auth_instance._auth_token = None
        # First add the file
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
        res_count_after = cli_runner.invoke(
            app, ["data-count", "--flavour", "user", "--host", test_server]
        )
        assert res.exit_code == 1
        res_count_after = len(databrowser(flavour="user", host=test_server))
        assert res_count_before == res_count_after
        # remove whatever is existing
        res = cli_runner.invoke(
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
    finally:
        auth_instance._auth_token = token


def test_no_solr_post(test_server: str, auth: Token) -> None:
    """Test what happens if there is no connection to Solr during a PUT request."""
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
    with mock.patch("freva_rest.rest.server_config.solr_host", "foo.bar"):
        res = requests.post(
            f"{test_server}/databrowser/userdata",
            json=data,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert res.status_code == 500


def test_no_solr_delete(test_server: str, auth: Token) -> None:
    """Test what happens if there is no connection to Solr during a PUT request."""
    token = auth["access_token"]
    with mock.patch("freva_rest.rest.server_config.solr_host", "foo.bar"):
        res = requests.delete(
            f"{test_server}/databrowser/userdata",
            json={},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert res.status_code == 500
