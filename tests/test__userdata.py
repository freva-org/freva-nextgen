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




@patch("pymongo.MongoClient", new=mongomock.MongoClient)
def test_userdata_add_filenotfound(
    client: TestClient, auth: Dict[str, str]
) -> None:
    """Test user data ingestion with a file that does not exist through the API."""
    token = auth["access_token"]
    data = {}
    params = {
        "paths": "./wrong/destination/" 
    }
    response = client.put(
        "/api/databrowser/userdata/janedoe",
        params=params,
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid paths provided: No valid paths found"}

@patch("pymongo.MongoClient", new=mongomock.MongoClient)
def test_userdata_ingestion_batch_files(
    client: TestClient, auth: Dict[str, str]
) -> None:
    """Test user data ingestion with batch files through the API."""
    token = auth["access_token"]
    data = {}
    params = {
        "paths": "./freva-rest/src/databrowser_api/" 
    }
    response = client.put(
        "/api/databrowser/userdata/janedoe",
        params=params,
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 202
    assert response.json() == {"status": "Crawling has been started. Check the Data Browser for updates."}

 
@patch("pymongo.MongoClient", new=mongomock.MongoClient)
def test_user_falvour(
    client: TestClient, auth: Dict[str, str]
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
def test_userdata_purge_batch_files(
    client: TestClient, auth: Dict[str, str]
) -> None:
    """Test user data purging with batch files through the API."""
    token = auth["access_token"]
    response = client.request(
        "DELETE",
        f"/api/databrowser/userdata/janedoe",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    print(response.json())
    assert response.status_code == 202
    assert response.json() == {"status": "User data has been deleted successfully"}

def test_add_userdata(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth
) -> None:
    """Test adding user data through the CLI."""
    import time
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
                "janedoe",
                "--path",
                "./freva-rest/src/databrowser_api/mock/data/",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
        assert res.exit_code == 0
        res_count_after = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])
        assert int(res_count_before.output) < int(res_count_after.output)
    finally:
        auth_instance._auth_token = token


@patch("pymongo.MongoClient", new=mongomock.MongoClient)
def test_add_userdata_broken_file(
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
                "janedoe",
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

def test_add_userdata_file(
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
                "janedoe",
                "--path",
                "./freva-rest/src/databrowser_api/mock/data/model/global/cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/Amon/ua/gn/v20190815/ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc",
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
        assert int(res_count_before.output) == int(res_count_after.output) - 1
        # remove whatever is existing
        res = cli_runner.invoke(
            app,
            [
                "user-data",
                "delete",
                "janedoe",
                "-s",
                "file=./freva-rest/src/databrowser_api/mock/data/model/global/cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/Amon/ua/gn/v20190815/ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
    finally:
        auth_instance._auth_token = token

def test_delete_userdata(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth
) -> None:
    """Test removing user data through the CLI."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None

        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0
        token_data = authenticate(username="janedoe", host=test_server)
        auth_instance._auth_token = None

        cli_runner.invoke(
            app,
            [
                "user-data",
                "add",
                "janedoe",
                "--path",
                "./freva-rest/src/databrowser_api/mock/data/",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
        res_count_before = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])
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
        res_count_after = cli_runner.invoke(app, ["data-count", "--flavour", "user", "--host", test_server])
        assert res.exit_code == 0
        assert int(res_count_before.output) > int(res_count_after.output) 
        # try to delete what is not there
        res = cli_runner.invoke(
            app,
            [
                "user-data",
                "delete",
                "janedoe",
                "-s",
                "product=johndoe",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
        assert res.exit_code == 0
    finally:
        auth_instance._auth_token = token


def test_userdata_permission_denied(
    cli_runner: CliRunner, test_server: str, auth_instance: Auth
) -> None:
    """Test permission denied user data through the CLI."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None

        res = cli_runner.invoke(app, ["--host", test_server])
        assert res.exit_code > 0

        token_data = authenticate(username="janedoe", host=test_server)
        auth_instance._auth_token = None
        add = cli_runner.invoke(
            app,
            [
                "user-data",
                "add",
                "johndoe",
                "--path",
                "./freva-rest/src/databrowser_api/mock/data/",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
        assert add.exit_code == 1

        delete = cli_runner.invoke(
            app,
            [
                "user-data",
                "delete",
                "johndoe",
                "-s",
                "product=johndoe",
                "--host",
                test_server,
                "--access-token",
                token_data["access_token"],
            ],
        )
        assert delete.exit_code == 1
    finally:
        auth_instance._auth_token = token


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
                "janedoe",
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
                "janedoe",
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

# TODO: determine better wat to assert the output
def test_no_solr_put(client_no_solr: TestClient, auth: Dict[str, str]) -> None:
    """Test what happens if there is no connection to Solr during a PUT request."""
    token = auth["access_token"]
    data = {}
    params = {
        "paths": "./freva-rest/src/databrowser_api/mock/data/"  # URL-encoded path
    }
    client_no_solr.put(
        "/api/databrowser/userdata/janedoe",
        params=params,
        json=data,
        headers={"Authorization": f"Bearer {token}"},
    )