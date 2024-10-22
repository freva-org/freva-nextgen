"""Tests for the databrowser class."""

from copy import deepcopy
from tempfile import TemporaryDirectory

import pytest
from freva_client import databrowser
from freva_client.auth import Auth, authenticate
from freva_client.utils.logger import DatabrowserWarning
from fastapi import HTTPException

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
    assert isinstance(db.metadata, dict)
    metadata = databrowser.metadata_search(host=test_server)
    assert isinstance(metadata, dict)
    assert len(db.metadata) > len(metadata)
    metadata = databrowser.metadata_search(host=test_server, extended_search=True)
    assert len(db.metadata) == len(metadata)


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
        databrowser.metadata_search(host=test_server, foo="bar", fail_on_error=True)
    db = databrowser(host=test_server, foo="bar", flavour="foo")  # type: ignore
    with pytest.raises(ValueError):
        len(db)


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


def test_intake_with_zarr(test_server: str, auth_instance: Auth) -> None:
    """Test the intake zarr catalogue creation."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance.auth_instance = None
        db = databrowser(host=test_server, dataset="cmip6-fs", stream_zarr=True)
        with pytest.raises(ValueError):
            cat = db.intake_catalogue()
        _ = authenticate(username="janedoe", host=test_server)
        cat = db.intake_catalogue()
        assert hasattr(cat, "df")
    finally:
        auth_instance._auth_token = token


def test_zarr_stream(test_server: str, auth_instance: Auth) -> None:
    """Test creating zarr endpoints for loading."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance.auth_instance = None
        db = databrowser(host=test_server, dataset="cmip6-fs", stream_zarr=True)
        with pytest.raises(ValueError):
            files = list(db)
        _ = authenticate(username="janedoe", host=test_server)
        files = list(db)
        assert len(files) == 2
    finally:
        auth_instance._auth_token = token

def test_userdata_add_path_xarray_py(test_server: str, auth_instance: Auth) -> None:
    """Test adding path and xarray user data."""
    import xarray as xr
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance.auth_instance = None
        db = databrowser(host=test_server)
        _ = authenticate(username="janedoe", host=test_server)

        db.userdata("delete", metadata={})
        filename1 = "./freva-rest/src/databrowser_api/mock/data/model/regional/cordex/output/EUR-11/GERICS/NCC-NorESM1-M/rcp85/r1i1p1/GERICS-REMO2015/v1/3hr/pr/v20181212/pr_EUR-11_NCC-NorESM1-M_rcp85_r1i1p1_GERICS-REMO2015_v2_3hr_200701020130-200701020430.nc"
        filename2 = "./freva-rest/src/databrowser_api/mock/data/model/regional/cordex/output/EUR-11/CLMcom/MPI-M-MPI-ESM-LR/historical/r0i0p0/CLMcom-CCLM4-8-17/v1/fx/orog/v20140515/orog_EUR-11_MPI-M-MPI-ESM-LR_historical_r1i1p1_CLMcom-CCLM4-8-17_v1_fx.nc"
        xarray_data = xr.open_dataset(filename1)
        db.userdata(
            "add", userdata_items=[xarray_data, filename2],
            metadata={}
        )
        assert len(databrowser(flavour="user")) == 2

    finally:
        auth_instance._auth_token = token


def test_userdata_add_path_py_batch(test_server: str, auth_instance: Auth) -> None:
    """Test adding path user data."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance.auth_instance = None
        db = databrowser(host=test_server)
        _ = authenticate(username="janedoe", host=test_server)

        db.userdata("delete", metadata={})
        filename1 = "./freva-rest/src/databrowser_api/mock/data/model/regional/cordex/output/EUR-11/"
        db.batch_size = 1
        db.userdata(
            "add", userdata_items=[filename1],
            metadata={}
        )
        assert len(databrowser(flavour="user")) > 1
    finally:
        auth_instance._auth_token = token


def test_userdata_add_xarray_py_batch(test_server: str, auth_instance: Auth) -> None:
    """Test adding xarray user data."""
    import xarray as xr
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance.auth_instance = None
        db = databrowser(host=test_server)
        _ = authenticate(username="janedoe", host=test_server)

        db.userdata("delete", metadata={})
        filename1 = "./freva-rest/src/databrowser_api/mock/data/model/regional/cordex/output/EUR-11/GERICS/NCC-NorESM1-M/rcp85/r1i1p1/GERICS-REMO2015/v1/3hr/pr/v20181212/pr_EUR-11_NCC-NorESM1-M_rcp85_r1i1p1_GERICS-REMO2015_v2_3hr_200701020130-200701020430.nc"
        filename2 = "./freva-rest/src/databrowser_api/mock/data/model/regional/cordex/output/EUR-11/CLMcom/MPI-M-MPI-ESM-LR/historical/r0i0p0/CLMcom-CCLM4-8-17/v1/fx/orog/v20140515/orog_EUR-11_MPI-M-MPI-ESM-LR_historical_r1i1p1_CLMcom-CCLM4-8-17_v1_fx.nc"
        filename3 = "./freva-rest/src/databrowser_api/mock/data/model/regional/cordex/output/EUR-11/CLMcom/MPI-M-MPI-ESM-LR/historical/r1i1p1/CLMcom-CCLM4-8-17/v1/daypt/tas/v20140515/tas_EUR-11_MPI-M-MPI-ESM-LR_historical_r1i1p1_CLMcom-CCLM4-8-17_v1_daypt_194912011200-194912101200.nc"
        xarray_data1 = xr.open_dataset(filename1)
        xarray_data2 = xr.open_dataset(filename2)
        xarray_data3 = xr.open_dataset(filename3)

        db.batch_size = 1
        db.userdata(
            "add", userdata_items=[xarray_data1, xarray_data2, xarray_data3],
            metadata={}
        )
        assert len(databrowser(flavour="user")) == 3
    finally:
        auth_instance._auth_token = token

def test_userdata_failed(test_server: str, auth_instance: Auth) -> None:
    """Test user data wrong paths."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance.auth_instance = None
        db = databrowser(host=test_server)
        _ = authenticate(username="janedoe", host=test_server)
        length = len(db)
        with pytest.raises(FileNotFoundError) as exc_info:
            db.userdata(
                "add", userdata_items=["/somewhere/wrong"], metadata={"username": "johndoe"}
            )
        assert "No valid file paths or xarray datasets found." in str(exc_info.value)
        assert len(db) == length
    finally:
        auth_instance._auth_token = token


def test_userdata_post_delete_failure(test_server: str, auth_instance: Auth) -> None:

    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance.auth_instance = None
        _ = authenticate(username="janedoe", host=test_server)
        db = databrowser(host="foo.bar.de:7777", fail_on_error=True)
        with pytest.raises(ValueError):
            db.userdata("add",userdata_items=["./freva-rest/src/databrowser_api/mock_broken/bears.nc"], metadata={"username": "janedoe"})
        with pytest.raises(ValueError):
            db.userdata("delete",metadata={"username": "janedoe"})
    finally:
        auth_instance._auth_token = token
def test_userdata_post_delete_without_failure(test_server: str, auth_instance: Auth) -> None:

    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance.auth_instance = None
        _ = authenticate(username="janedoe", host=test_server)
        db = databrowser(host="foo.bar.de:7777")
        with pytest.raises(ValueError):
            db.userdata("add",userdata_items=["./freva-rest/src/databrowser_api/mock_broken/bears.nc"], metadata={"username": "janedoe"})
        with pytest.raises(ValueError):
            db.userdata("delete",metadata={"username": "janedoe"})
    finally:
        auth_instance._auth_token = token

def test_userdata_correct_args_wrong_place(
    test_server: str, auth_instance: Auth
) -> None:
    """Test adding user data with wrong arguments."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance.auth_instance = None
        db = databrowser(host=test_server)
        _ = authenticate(username="janedoe", host=test_server)
        with pytest.raises(FileNotFoundError):
            db.userdata("add", metadata={"username": "johndoe"})
        db.userdata("delete", userdata_items=["./freva-rest/src/databrowser_api/mock_broken/bears.nc"], metadata={"username": "johndoe"})
    finally:
        auth_instance._auth_token = token

def test_userdata_empty_metadata_value_error(
    test_server: str, auth_instance: Auth
) -> None:
    """Test adding user data with wrong arguments."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance.auth_instance = None
        db = databrowser(host=test_server)
        _ = authenticate(username="janedoe", host=test_server)
        with pytest.raises(ValueError):
            db.userdata("add", userdata_items=["./freva-rest/src/databrowser_api/mock/data/model/obs/reanalysis/reanalysis/NOAA/NODC/OC5/mon/ocean/Omon/r1i1p1/v20200101/hc700/hc700_mon_NODC_OC5_r1i1p1_201201-201212.nc"], metadata={"username": "johndoe"})
    finally:
        auth_instance._auth_token = token


def test_userdata_non_path_xarray(
    test_server: str, auth_instance: Auth
) -> None:
    """Test adding user data with wrong arguments."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance.auth_instance = None
        db = databrowser(host=test_server)
        _ = authenticate(username="janedoe", host=test_server)
        with pytest.raises(FileNotFoundError):
            db.userdata("add", userdata_items=[[1]], metadata={"username": "johndoe"})
    finally:
        auth_instance._auth_token = token