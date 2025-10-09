"""Tests for the databrowser class."""

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from freva_client import databrowser
from freva_client.auth import Auth, AuthError, Token
from freva_client.utils.logger import DatabrowserWarning
import pandas as pd

def test_search_files(test_server: str) -> None:
    """Test searching for files."""
    db = databrowser(host=test_server)
    assert len(list(db)) > 0
    assert len(list(db)) == len(db)
    db = databrowser(host=test_server, foo="bar", fail_on_error=True)
    with pytest.raises(ValueError):
        len(db)
    db = databrowser(host=test_server, foo="bar", time="2000 to 2050")
    assert len(db) == 0
    db = databrowser(host=test_server, foo="bar", bbox=(10, 20, 30, 40))
    assert len(db) == 0
    db = databrowser(host=test_server, model="bar")
    assert len(db) == len(list(db)) == 0
    db = databrowser(host="foo")
    with pytest.raises(ValueError):
        len(db)
    assert (
        len(
            databrowser(
                "land",
                realm="ocean",
                product="reanalysis",
                host=test_server,
            )
        )
        == 0
        == 0
    )

def test_count_values(test_server: str) -> None:
    """Test counting the facets."""
    db = databrowser(host=test_server)
    assert isinstance(len(db), int)
    counts1 = databrowser.count_values("*", host=test_server)
    assert isinstance(counts1, dict)
    assert "dataset" not in counts1
    counts2 = databrowser.count_values(
        "ocean",
        realm="ocean",
        product="reanalysis",
        host=test_server,
        extended_search=True,
    )
    assert isinstance(counts2, dict)
    assert "dataset" in counts2
    assert isinstance(counts2["dataset"], dict)
    entry = list(counts2["dataset"].keys())[0]
    assert isinstance(counts2["dataset"][entry], int)

def test_metadata_search(test_server: str) -> None:
    """Test the metadata search."""
    db = databrowser(host=test_server)
    assert isinstance(db.metadata, pd.Series)
    metadata = databrowser.metadata_search(host=test_server)
    assert isinstance(metadata, pd.Series)
    assert len(db.metadata) > len(metadata)
    metadata = databrowser.metadata_search(host=test_server, extended_search=True)
    assert len(db.metadata) == len(metadata)
    db_filtered = databrowser(host=test_server)
    assert set(db_filtered.metadata[['project', 'model']].keys()) <= {'project', 'model'}
    assert len(db_filtered.metadata[['project', 'model']]) <= 2

def test_bad_hostnames() -> None:
    """Test the behaviour of non existing host queries."""
    db = databrowser(host="foo")
    with pytest.raises(ValueError):
        len(db)
    with pytest.raises(ValueError):
        databrowser.metadata_search(host="foo")
    with pytest.raises(ValueError):
        databrowser.count_values(host="foo")


def test_bad_queries(test_server: str) -> None:
    """Test the behaviour of bad queries."""
    db = databrowser(host=test_server, foo="bar")
    with pytest.warns(DatabrowserWarning):
        len(db)
    with pytest.warns(DatabrowserWarning):
        databrowser.count_values(host=test_server, foo="bar")
    with pytest.warns(DatabrowserWarning):
        databrowser.metadata_search(host=test_server, foo="bar")
    db = databrowser(host=test_server, foo="bar", fail_on_error=True)
    with pytest.raises(ValueError):
        len(db)
    with pytest.raises(ValueError):
        databrowser.count_values(host=test_server, foo="bar", fail_on_error=True)
    with pytest.raises(ValueError):
        databrowser.metadata_search(
            host=test_server, foo="bar", fail_on_error=True
        )


def test_repr(test_server: str) -> None:
    """Test the str rep."""
    db = databrowser(host=test_server)
    assert test_server in repr(db)
    assert str(len(db)) in db._repr_html_()
    overview = db.overview(host=test_server)
    assert isinstance(overview, str)
    assert "flavour" in overview
    assert "cmip6" in overview
    assert "freva" in overview

    # test overview with a non-existing host
    with pytest.raises(ValueError):
        databrowser.overview(host="foo.bar.de:7777")



def test_intake_without_zarr(test_server: str) -> None:
    """Test the intake catalogue creation."""
    db = databrowser(host=test_server, dataset="cmip6-fs")
    cat = db.intake_catalogue()
    assert hasattr(cat, "df")
    with TemporaryDirectory() as temp_dir:
        with pytest.raises(ValueError):
            db._create_intake_catalogue_file(temp_dir)
    db = databrowser(host=test_server, dataset="foooo")
    with pytest.raises(ValueError):
        db.intake_catalogue()


def test_stac_catalogue(test_server: str, temp_dir: Path) -> None:
    """Test the STAC Catalogue functionality."""
    # static STAC catalogue
    db = databrowser(host=test_server, dataset="cmip6-fs")
    res = db.stac_catalogue(filename=temp_dir / "something.zip")
    assert f"STAC catalog saved to: {temp_dir / 'something.zip'}" in res

    # static STAC catalogue with non-existing directory
    db = databrowser(host=test_server, dataset="cmip6-fs")
    res = db.stac_catalogue(filename=temp_dir / "anywhere/s")
    assert f"STAC catalog saved to: {temp_dir / 'anywhere/s'}" in res

    # static STAC catalogue with existing directory
    db = databrowser(host=test_server, dataset="cmip6-fs")
    res = db.stac_catalogue(filename=temp_dir)
    assert f"STAC catalog saved to: {temp_dir}" in res


def test_intake_with_zarr(
    test_server: str, auth_instance: Auth, auth: Token
) -> None:
    """Test the intake zarr catalogue creation."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None
        db = databrowser(host=test_server, dataset="cmip6-fs", stream_zarr=True)
        with pytest.raises(AuthError):
            cat = db.intake_catalogue()
        auth_instance._auth_token = auth
        cat = db.intake_catalogue()
    finally:
        auth_instance._auth_token = token
    assert hasattr(cat, "df")


def test_zarr_stream(test_server: str, auth_instance: Auth, auth: Token) -> None:
    """Test creating zarr endpoints for loading."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None
        db = databrowser(host=test_server, dataset="cmip6-fs", stream_zarr=True)
        with pytest.raises(AuthError):
            _ = list(db)
        auth_instance._auth_token = auth
        files = list(db)
        assert len(files) == 2
    finally:
        auth_instance._auth_token = token


def test_userdata_add_path_xarray_py(
    test_server: str,
    auth_instance: Auth,
    auth: Token,
) -> None:
    """Test adding path and xarray user data."""
    import xarray as xr

    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth

        databrowser.userdata("delete", metadata={}, host=test_server)
        filename = (
            "./freva-rest/src/freva_rest/databrowser_api/mock/data/model/regional"
            "/cordex/output/EUR-11/CLMcom/MPI-M-MPI-ESM-LR/historical"
            "/r0i0p0/CLMcom-CCLM4-8-17/v1/fx/orog/v20140515/"
            "orog_EUR-11_MPI-M-MPI-ESM-LR_historical_r1i1p1_"
            "CLMcom-CCLM4-8-17_v1_fx_another.nc"
        )
        xarray_data = xr.open_dataset(filename)
        databrowser.userdata(
            "add",
            userdata_items=[xarray_data, xarray_data, filename],
            metadata={},
            host=test_server,
        )
        assert len(databrowser(flavour="user", host=test_server)) == 1

    finally:
        auth_instance._auth_token = token


def test_userdata_failed(
    test_server: str, auth_instance: Auth, auth: Token
) -> None:
    """Test user data wrong paths."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth
        db = databrowser(host=test_server)
        length = len(db)
        with pytest.raises(FileNotFoundError) as exc_info:
            databrowser.userdata(
                "add",
                userdata_items=["/somewhere/wrong"],
                metadata={"username": "johndoe"},
                host=test_server,
            )
        assert "No valid file paths or xarray datasets found." in str(
            exc_info.value
        )
    finally:
        auth_instance._auth_token = token
    assert len(db) == length


def test_userdata_post_delete_failure(
    test_server: str, auth_instance: Auth, auth: Token
) -> None:
    """Test failure of adding user data."""

    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = deepcopy(auth)
        auth_instance._auth_token["access_token"] = "foo"
        with pytest.raises(ValueError):
            databrowser.userdata(
                "add",
                userdata_items=[
                    "./freva-rest/src/freva_rest/databrowser_api/mock_broken/bears.nc"
                ],
                metadata={"username": "janedoe"},
                host="foo.bar.de:7777",
                fail_on_error=True,
            )
        with pytest.raises(ValueError):
            databrowser.userdata(
                "delete",
                metadata={"username": "janedoe"},
                host="foo.bar.de:7777",
                fail_on_error=True,
            )
    finally:
        auth_instance._auth_token = token


def test_userdata_post_delete_without_failure(
    test_server: str, auth_instance: Auth, auth: Token
) -> None:
    """Test successful deleting user data."""

    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth
        with pytest.raises(ValueError):
            databrowser.userdata(
                "add",
                userdata_items=[
                    "./freva-rest/src/freva_rest/databrowser_api/mock_broken/bears.nc"
                ],
                metadata={"username": "janedoe"},
                host="foo.bar.de:7777",
            )
        with pytest.raises(ValueError):
            databrowser.userdata(
                "delete",
                metadata={"username": "janedoe"},
                host="foo.bar.de:7777",
            )
    finally:
        auth_instance._auth_token = token


def test_userdata_correct_args_wrong_place(
    test_server: str,
    auth_instance: Auth,
    auth: Token,
) -> None:
    """Test adding user data with wrong arguments."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth
        with pytest.raises(FileNotFoundError):
            databrowser.userdata(
                "add", metadata={"username": "johndoe"}, host=test_server
            )
        databrowser.userdata(
            "delete",
            userdata_items=[
                "./freva-rest/src/freva_rest/databrowser_api/mock_broken/bears.nc"
            ],
            metadata={"username": "johndoe"},
            host=test_server,
        )
    finally:
        auth_instance._auth_token = token


def test_userdata_empty_metadata_value_error(
    test_server: str, auth_instance: Auth, auth: Token
) -> None:
    """Test adding user data with wrong arguments."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth
        with pytest.raises(ValueError):
            databrowser.userdata(
                "add",
                userdata_items=[
                    (
                        "./freva-rest/src/freva_rest/databrowser_api/mock/data/model/obs/"
                        "reanalysis/reanalysis/NOAA/NODC/OC5/mon/ocean/Omon/"
                        "r1i1p1/v20200101/hc700/"
                        "hc700_mon_NODC_OC5_r1i1p1_201201-201212.nc"
                    )
                ],
                metadata={"username": "johndoe"},
                host=test_server,
            )
    finally:
        auth_instance._auth_token = token


def test_userdata_non_path_xarray(
    test_server: str, auth_instance: Auth, auth: Token
) -> None:
    """Test adding user data with wrong arguments."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth
        with pytest.raises(FileNotFoundError):
            databrowser.userdata(
                "add",
                userdata_items=[[1]],
                metadata={"username": "johndoe"},
                host=test_server,
            )
    finally:
        auth_instance._auth_token = token


def test_add_userdata_wild_card(
    test_server: str, auth_instance: Auth, auth: Token
) -> None:
    """Test adding user data with wild card."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth
        databrowser.userdata("delete", host=test_server)
        databrowser.userdata(
            "add",
            userdata_items=[
                (
                    "./freva-rest/src/freva_rest/databrowser_api/mock/data/model/global/"
                    "cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/"
                    "Amon/ua/gn/v20190815/*.nc"
                ),
                (
                    "./freva-rest/src/freva_rest/databrowser_api/mock/data/model/global/"
                    "cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/r2i1p1f1/Amon/"
                    "ua/gn/v20190815/somewhere_wrong/*.nc"
                ),
            ],
            metadata={"username": "johndoe"},
            host=test_server,
        )
    finally:
        auth_instance._auth_token = token


def test_flavour_operations(test_server: str, auth_instance: Auth, auth: Token) -> None:
    """Test query flavour add, list, and delete operations."""
    from copy import deepcopy

    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth
        
        # listing flavours
        result = databrowser.flavour(action="list", host=test_server)
        assert isinstance(result["flavours"], list)
        assert len(result["flavours"]) >= 5
        flavour_names = [f["flavour_name"] for f in result["flavours"]]
        assert "freva" in flavour_names
        assert "cmip6" in flavour_names
        
        # adding a custom flavour
        custom_mapping = {
            "project": "projekt", 
            "variable": "variable_name",
            "model": "modell"
        }
        databrowser.flavour(
            action="add",
            name="test_flavour_client",
            mapping=custom_mapping,
            is_global=False,
            host=test_server
        )
        
        # custom flavour appears in list
        result_after = databrowser.flavour(action="list", host=test_server)
        assert len(result_after["flavours"]) > len(result["flavours"])
        new_flavour_names = [f["flavour_name"] for f in result_after["flavours"]]
        assert f"test_flavour_client" in new_flavour_names
        valid_token = deepcopy(auth_instance._auth_token)
        auth_instance._auth_token["access_token"] = "foo"
        # deleting a flavour with invalid token raises an error
        with pytest.raises(ValueError):
            databrowser.flavour(
                action="list",
                host="http://non-existent-host:9999",
                fail_on_error=True
            )

        auth_instance._auth_token = valid_token
        # custom flavour can be used in searches
        
        db = databrowser(flavour="test_flavour_client", host=test_server)
        # not fail even if no results since flavour is custom
        assert len(db) >= 0

        # checking count values with custom flavour
        db_count = databrowser.count_values(
            "projekt", flavour="test_flavour_client", host=test_server
        )
        assert isinstance(db_count, dict)

        # updating the custom flavour
        databrowser.flavour(
            action="update",
            name="test_flavour_client",
            mapping={"experiment": "exp_updated"},
            is_global=False,
            host=test_server
        )

        # update with rename
        databrowser.flavour(
            action="update",
            name="test_flavour_client",
            new_name="test_flavour_client_renamed",
            is_global=False,
            host=test_server
        )

        # verify rename
        result_renamed = databrowser.flavour(action="list", host=test_server)
        renamed_names = [f["flavour_name"] for f in result_renamed["flavours"]]
        assert "test_flavour_client_renamed" in renamed_names

        # deleting the custom flavour
        databrowser.flavour(
            action="delete",
            name="test_flavour_client_renamed",
            host=test_server
        )

        # custom flavour is gone
        flavours_final = databrowser.flavour(action="list", host=test_server)
        final_flavour_names = [f["flavour_name"] for f in flavours_final["flavours"]]
        assert "test_flavour_client_renamed" not in final_flavour_names
        assert len(flavours_final["flavours"]) == len(result["flavours"])
    finally:
        auth_instance._auth_token = token


def test_flavour_error_cases(test_server: str, auth_instance: Auth, auth: Token) -> None:
    """Test flavour error handling."""
    from copy import deepcopy
    
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = auth
        # wrong facet key
        custom_mapping = {
            "projecta": "projekt",
        }
        Added = databrowser.flavour(action="add", name="test_flavour_no_auth", mapping=custom_mapping, host=test_server)
        assert Added is None

        # test the missing name and mapping parameters
        with pytest.raises(ValueError, match="Both 'name' and 'mapping' are required"):
            databrowser.flavour(action="add", host=test_server)

        # updating flavour without name
        with pytest.raises(ValueError, match="'name' is required for update action"):
            databrowser.flavour(action="update", host=test_server)

        # deleting flavour without name
        with pytest.raises(ValueError, match="'name' is required for delete action"):
            databrowser.flavour(action="delete", host=test_server)

    finally:
        auth_instance._auth_token = token


def test_flavour_without_auth(test_server: str) -> None:
    """Test listing flavours without authentication"""
    flavours = databrowser.flavour(action="list", host=test_server)
    assert isinstance(flavours, dict)
