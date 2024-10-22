from typing import Dict
from fastapi.testclient import TestClient
import mongomock
from unittest.mock import patch
from copy import deepcopy
from freva_client.auth import Auth, authenticate
from freva_client.cli.databrowser_cli import databrowser_app as app
from typer.testing import CliRunner
import mongomock
from unittest.mock import patch



def test_add_userdata_cli_standard(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth
) -> None:
    """Test adding user data through the CLI."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None

        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        token_data = authenticate(username="janedoe", host=test_server)
        auth_instance._auth_token = None
        res_count_before = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])

        res = cli_runner.invoke(
            app,
            [
                "user-data",
                "add",
                "--path",
                "/Users/mo/dev/20241018/freva-nextgen/freva-rest",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
        res_count_after = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])
        assert int(res_count_before.output) < int(res_count_after.output)
    finally:
        auth_instance._auth_token = token


def test_add_userdata_cli_all_successful_and_escape_char(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth
) -> None:
    """Test adding user data through the CLI."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None

        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        token_data = authenticate(username="janedoe", host=test_server)
        auth_instance._auth_token = None
        res_count_before = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])

        res_add = cli_runner.invoke(
            app,
            [
                "user-data",
                "add",
                "--path",
                "./freva-rest/src/databrowser_api/mock/data/model/global/cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/Amon/ua/gn/v20190815/ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
        assert res_add.exit_code == 0
        res_count_middle = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])
        assert int(res_count_before.output) <= int(res_count_middle.output)

        res_del = cli_runner.invoke(
            app,
            [
                "user-data",
                "delete",
                "-s",
                "file=./freva-rest/src/databrowser_api/mock/data/model/global/cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/Amon/ua/gn/v20190815/ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
        res_count_after = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])
        assert res_del.exit_code == 0
        assert int(res_count_middle.output) == int(res_count_after.output)
    finally:
        auth_instance._auth_token = token


def test_userdata_add_api_202(
    client: TestClient, auth: Dict[str, str], user_data_payload_sample: Dict
) -> None:
    """Test user data through the API with valid metadata."""
    token = auth["access_token"]
    data = user_data_payload_sample
    response = client.post(
        "/api/databrowser/userdata",
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    print(response.text)
    assert response.status_code == 202



def test_userdata_add_api_202_duplicate_bulk_error_mongo(
    client: TestClient, auth: Dict[str, str], user_data_payload_sample: Dict
) -> None:
    """Test user data through the API with valid metadata."""
    token = auth["access_token"]
    data = user_data_payload_sample
    response = client.post(
        "/api/databrowser/userdata",
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    print(response.text)
    assert response.status_code == 202

def test_userdata_add_api_422(
    client: TestClient, auth: Dict[str, str]
) -> None:
    """Test user data through the API with invalid metadata."""
    token = auth["access_token"]
    data = {
        "user_metadata": {
            "project": "cmip5",
            "experiment": "something",
        },
        "facets": {
            "product": "johndoe",
        }
    }
    print(client)
    response = client.post(
        "/api/databrowser/userdata",
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    print(response.text)
    assert response.status_code == 422

def test_userdata_add_api_500(
    client: TestClient, auth: Dict[str, str]
) -> None:
    """Test user data through the API with invalid user_metadata."""
    token = auth["access_token"]
    data = {
        "user_metadata": [
            {"variable": "tas", "time_frequency": "mon"}
        ],
        "facets": {
            "product": "johndoe",
        }
    }
    response = client.post(
        "/api/databrowser/userdata",
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 500

def test_userdata_delete_api_422(
    client: TestClient, auth: Dict[str, str]
) -> None:
    """Test user data through the API with invalid metadata."""
    token = auth["access_token"]
    data = {
        "user_metadata": [
            {"variable": "tas", "time_frequency": "mon"}
        ],
        "facets": {
            "product": "johndoe",
        }
    }
    response = client.request(
        "DELETE",
        f"/api/databrowser/userdata",
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 422

def test_userdata_delete_api_202(
    client: TestClient, auth: Dict[str, str]
) -> None:
    """Test user data through the API with valid metadata."""
    token = auth["access_token"]
    data = {}
    response = client.request(
        "DELETE",
        f"/api/databrowser/userdata",
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 202

@patch("pymongo.MongoClient", new=mongomock.MongoClient)
def test_user_falvour(
    client: TestClient
) -> None:
    """Test user flavour through the API."""
    
    first_res = client.get(
        "/api/databrowser/data_search/user/uri",
        params={
            "translate": "false",
        },
    )
    second_res = client.get(
        "/api/databrowser/data_search/freva/uri",
        params={
            "translate": "false",
        },
    )
    assert all(item not in second_res.text.split() for item in first_res.text.split())

@patch("pymongo.MongoClient", new=mongomock.MongoClient)
def test_add_userdata_cli_broken_file(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth
) -> None:
    """Test adding user broken data through the CLI."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None

        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        token_data = authenticate(username="janedoe", host=test_server)
        auth_instance._auth_token = None
        res_count_before = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])
        # add the broken file
        res = cli_runner.invoke(
            app,
            [
                "user-data",
                "add",
                "--path",
                "./freva-rest/src/databrowser_api/mock_broken/bears.nc",
                "--facet",
                "product=johndoe",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
        res_count_after = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])
        assert res.exit_code == 0
        assert int(res_count_before.output) == int(res_count_after.output)
        # remove whatever is existing
        res = cli_runner.invoke(
            app,
            [
                "user-data",
                "delete",
                "janedoe",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )        
        
    finally:
        auth_instance._auth_token = token

@patch("pymongo.MongoClient", new=mongomock.MongoClient)
def test_wrong_equal_facet(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth
) -> None:
    """Test adding user data through the CLI."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None

        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        token_data = authenticate(username="janedoe", host=test_server)
        auth_instance._auth_token = None
        # First add the file
        res_count_before = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])
        res = cli_runner.invoke(
            app,
            [
                "user-data",
                "add",
                "--path",
                "./freva-rest/src/databrowser_api/mock/data/model/global/cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/Amon/ua/gn/v20190815/ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc",
                "--facet",
                "product:johndoe",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
        res_count_after = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])
        assert res.exit_code == 1
        assert int(res_count_before.output) == int(res_count_after.output)
        # remove whatever is existing
        res = cli_runner.invoke(
            app,
            [
                "user-data",
                "delete",
                "-s",
                "file:./freva-rest/src/databrowser_api/mock/data/model/global/cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/Amon/ua/gn/v20190815/ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
    finally:
        auth_instance._auth_token = token

def test_no_solr_post(client_no_solr: TestClient, auth: Dict[str, str]) -> None:
    """Test what happens if there is no connection to Solr during a PUT request."""
    token = auth["access_token"]
    data = {
        "user_metadata": [{"variable": "tas", "time_frequency": "mon", "time": "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]", "file": "path of the file"}],
        "facets": {}
    }
    res = client_no_solr.post(
        "/api/databrowser/userdata",
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
