"""Tests for the databrowser class.

These tests exercise the Python API of the databrowser, including file
search, metadata search, count, intake/STAC catalogue creation, zarr
streaming, user data management, and custom flavour operations.

Authentication is handled via the ``mock_authenticate`` /
``mock_authenticate_fail`` fixtures which patch
``py_oidc_auth_client.authenticate`` instead of the old Auth singleton.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import namegenerator
import pandas as pd
import pytest
from py_oidc_auth_client import Token
from pytest_mock import MockerFixture

from freva_client import databrowser
from freva_client.utils.logger import DatabrowserWarning


class TestSearchFiles:
    """Tests for basic file search functionality."""

    def test_auth_token_valid(
        self, test_server: str, mock_authenticate: Token, mocker: MockerFixture
    ) -> None:
        """Test if the can create a auth token."""
        db = databrowser(host=test_server)
        mocker.patch("freva_client.utils.choose_token_strategy").return_value = (
            "use_token"
        )
        assert isinstance(db.auth_token, dict)

    def test_auth_token_invalid(
        self, test_server: str, mock_authenticate: Token, mocker: MockerFixture
    ) -> None:
        db = databrowser(host=test_server)
        mocker.patch("freva_client.utils.choose_token_strategy").return_value = (
            "fail"
        )
        assert db.auth_token is None

    def test_search_returns_results(self, test_server: str) -> None:
        """Searching with no constraints should return files."""
        db = databrowser(host=test_server)
        assert len(list(db)) > 0
        assert len(list(db)) == len(db)

    def test_search_with_invalid_keys_raises(self, test_server: str) -> None:
        """Searching with invalid keys and fail_on_error should raise."""
        db = databrowser(host=test_server, foo="bar", fail_on_error=True)
        with pytest.raises(ValueError):
            len(db)

    def test_search_with_invalid_keys_returns_empty(
        self, test_server: str
    ) -> None:
        """Searching with invalid keys and time filter returns 0."""
        db = databrowser(host=test_server, foo="bar", time="2000 to 2050")
        assert len(db) == 0

    def test_search_with_bbox(self, test_server: str) -> None:
        """Searching with invalid keys plus bbox returns 0."""
        db = databrowser(host=test_server, foo="bar", bbox=(10, 20, 30, 40))
        assert len(db) == 0

    def test_search_no_match(self, test_server: str) -> None:
        """Searching with a non-existing model returns nothing."""
        db = databrowser(host=test_server, model="bar")
        assert len(db) == len(list(db)) == 0

    def test_search_bad_host_raises(self) -> None:
        """A non-reachable host should raise on len()."""
        db = databrowser(host="foo")
        with pytest.raises(ValueError):
            len(db)

    def test_search_with_positional_facets(self, test_server: str) -> None:
        """Searching with positional facets that don't match returns 0."""
        result = len(
            databrowser(
                "land",
                realm="ocean",
                product="reanalysis",
                host=test_server,
            )
        )
        assert result == 0


class TestCountValues:
    """Tests for the count and count_values functionality."""

    def test_len_returns_int(self, test_server: str) -> None:
        """len(db) should return an integer."""
        db = databrowser(host=test_server)
        assert isinstance(len(db), int)

    def test_count_values_wildcard(self, test_server: str) -> None:
        """count_values('*') should return a dict without 'dataset'."""
        counts = databrowser.count_values("*", host=test_server)
        assert isinstance(counts, dict)
        assert "dataset" not in counts

    def test_count_values_extended(self, test_server: str) -> None:
        """Extended search should include 'dataset' in counts."""
        counts = databrowser.count_values(
            "ocean",
            realm="ocean",
            product="reanalysis",
            host=test_server,
            extended_search=True,
        )
        assert isinstance(counts, dict)
        assert "dataset" in counts
        assert isinstance(counts["dataset"], dict)
        entry = list(counts["dataset"].keys())[0]
        assert isinstance(counts["dataset"][entry], int)


class TestMetadataSearch:
    """Tests for the metadata_search functionality."""

    def test_metadata_returns_series(self, test_server: str) -> None:
        """db.metadata should return a pandas Series."""
        db = databrowser(host=test_server)
        assert isinstance(db.metadata, pd.Series)

    def test_metadata_search_classmethod(self, test_server: str) -> None:
        """The classmethod metadata_search should return a Series."""
        metadata = databrowser.metadata_search(host=test_server)
        assert isinstance(metadata, pd.Series)

    def test_metadata_extended_vs_normal(self, test_server: str) -> None:
        """Extended search should return more metadata than normal."""
        db = databrowser(host=test_server)
        normal = databrowser.metadata_search(host=test_server)
        extended = databrowser.metadata_search(
            host=test_server, extended_search=True
        )
        assert len(db.metadata) > len(normal)
        assert len(db.metadata) == len(extended)

    def test_metadata_filter_facets(self, test_server: str) -> None:
        """Filtering metadata by specific facets should work."""
        db = databrowser(host=test_server)
        filtered = db.metadata[["project", "model"]]
        assert set(filtered.keys()) <= {"project", "model"}
        assert len(filtered) <= 2


class TestBadHostnames:
    """Tests for error handling with invalid hostnames."""

    def test_bad_host_len_raises(self) -> None:
        """A non-existing host should raise ValueError on len()."""
        db = databrowser(host="foo")
        with pytest.raises(ValueError):
            len(db)

    def test_bad_host_metadata_raises(self) -> None:
        """metadata_search with a bad host should raise ValueError."""
        with pytest.raises(ValueError):
            databrowser.metadata_search(host="foo")

    def test_bad_host_count_raises(self) -> None:
        """count_values with a bad host should raise ValueError."""
        with pytest.raises(ValueError):
            databrowser.count_values(host="foo")


class TestBadQueries:
    """Tests for warning/error behaviour with unknown search keys."""

    def test_unknown_key_warns(self, test_server: str) -> None:
        """An unknown search key should emit a DatabrowserWarning."""
        db = databrowser(host=test_server, foo="bar")
        with pytest.warns(DatabrowserWarning):
            len(db)

    def test_unknown_key_count_warns(self, test_server: str) -> None:
        """count_values with unknown key should warn."""
        with pytest.warns(DatabrowserWarning):
            databrowser.count_values(host=test_server, foo="bar")

    def test_unknown_key_metadata_warns(self, test_server: str) -> None:
        """metadata_search with unknown key should warn."""
        with pytest.warns(DatabrowserWarning):
            databrowser.metadata_search(host=test_server, foo="bar")

    def test_unknown_key_fail_on_error_raises(self, test_server: str) -> None:
        """With fail_on_error=True, unknown keys should raise."""
        db = databrowser(host=test_server, foo="bar", fail_on_error=True)
        with pytest.raises(ValueError):
            len(db)
        with pytest.raises(ValueError):
            databrowser.count_values(
                host=test_server, foo="bar", fail_on_error=True
            )
        with pytest.raises(ValueError):
            databrowser.metadata_search(
                host=test_server, foo="bar", fail_on_error=True
            )


class TestRepr:
    """Tests for the string representations of databrowser."""

    def test_repr_contains_host(self, test_server: str) -> None:
        """repr(db) should contain the host."""
        db = databrowser(host=test_server)
        assert test_server in repr(db)

    def test_html_repr_contains_count(self, test_server: str) -> None:
        """_repr_html_ should contain the object count."""
        db = databrowser(host=test_server)
        assert str(len(db)) in db._repr_html_()

    def test_overview(self, test_server: str) -> None:
        """overview() should contain flavour information."""
        db = databrowser(host=test_server)
        overview = db.overview(host=test_server)
        assert isinstance(overview, str)
        assert "flavour" in overview
        assert "cmip6" in overview
        assert "freva" in overview

    def test_overview_bad_host_raises(self) -> None:
        """overview() with a bad host should raise ValueError."""
        with pytest.raises(ValueError):
            databrowser.overview(host="foo.bar.de:7777")


class TestIntakeCatalogue:
    """Tests for intake catalogue creation."""

    def test_intake_without_zarr(self, test_server: str) -> None:
        """Creating an intake catalogue without zarr should work."""
        db = databrowser(host=test_server, dataset="cmip6-fs")
        cat = db.intake_catalogue()
        assert hasattr(cat, "df")

    def test_intake_file_creation_bad_path(self, test_server: str) -> None:
        """Writing to a directory should raise ValueError."""
        db = databrowser(host=test_server, dataset="cmip6-fs")
        with TemporaryDirectory() as temp_dir:
            with pytest.raises(ValueError):
                db._create_intake_catalogue_file(temp_dir)

    def test_intake_empty_result(self, test_server: str) -> None:
        """An intake catalogue with no results should raise ValueError."""
        db = databrowser(host=test_server, dataset="foooo")
        with pytest.raises(ValueError):
            db.intake_catalogue()

    def test_intake_with_zarr(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Intake catalogue with zarr requires authentication."""
        db = databrowser(host=test_server, dataset="cmip6-fs", stream_zarr=True)
        cat = db.intake_catalogue()
        assert hasattr(cat, "df")

    def test_intake_with_zarr_unauthenticated(
        self, test_server: str, mock_authenticate_fail
    ) -> None:
        """Intake catalogue with zarr should fail without auth."""
        db = databrowser(host=test_server, dataset="cmip6-fs", stream_zarr=True)
        with pytest.raises(Exception):
            db.intake_catalogue()

    def test_intake_with_public_zarr(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Public zarr stores should include 'share' in their URI."""
        db = databrowser(
            host=test_server,
            dataset="cmip6-fs",
            stream_zarr=True,
            zarr_options={"public": True},
        )
        cat = db.intake_catalogue()
        assert hasattr(cat, "df")
        assert "share" in cat.df.iloc[0]["uri"]


class TestStacCatalogue:
    """Tests for STAC catalogue creation."""

    def test_stac_catalogue_to_file(
        self, test_server: str, temp_dir: Path
    ) -> None:
        """Creating a STAC catalogue to a zip file should work."""
        db = databrowser(host=test_server, dataset="cmip6-fs")
        res = db.stac_catalogue(filename=temp_dir / "something.zip")
        assert f"STAC catalog saved to: {temp_dir / 'something.zip'}" in res

    def test_stac_catalogue_nonexisting_dir(
        self, test_server: str, temp_dir: Path
    ) -> None:
        """STAC catalogue should create parent directories."""
        db = databrowser(host=test_server, dataset="cmip6-fs")
        res = db.stac_catalogue(filename=temp_dir / "anywhere/s")
        assert f"STAC catalog saved to: {temp_dir / 'anywhere/s'}" in res

    def test_stac_catalogue_existing_dir(
        self, test_server: str, temp_dir: Path
    ) -> None:
        """STAC catalogue with an existing directory target."""
        db = databrowser(host=test_server, dataset="cmip6-fs")
        res = db.stac_catalogue(filename=temp_dir)
        assert f"STAC catalog saved to: {temp_dir}" in res


class TestZarrStream:
    """Tests for zarr streaming functionality."""

    def test_zarr_stream_authenticated(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Authenticated zarr streaming should return files."""
        db = databrowser(host=test_server, dataset="cmip6-fs", stream_zarr=True)
        files = list(db)
        assert len(files) == 2

    def test_zarr_stream_unauthenticated(
        self, test_server: str, mock_authenticate_fail
    ) -> None:
        """Unauthenticated zarr streaming should raise."""
        db = databrowser(host=test_server, dataset="cmip6-fs", stream_zarr=True)
        with pytest.raises(Exception):
            _ = list(db)

    def test_public_zarr_stream(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Public zarr URLs should be openable with xarray."""
        import xarray as xr

        db = databrowser(
            host=test_server,
            dataset="cmip6-fs",
            stream_zarr=True,
            zarr_options={"public": True},
        )
        files = list(db)
        xr.open_dataset(files[0], engine="zarr")


class TestUserdataPythonApi:
    """Tests for user data add/delete via the Python API."""

    def test_add_path_and_xarray(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Adding path and xarray user data should work."""
        import xarray as xr

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

    def test_userdata_wrong_paths(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Adding non-existing paths should raise FileNotFoundError."""
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
        assert len(db) == length

    def test_userdata_post_delete_bad_host(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Add/delete to a non-existing host should raise ValueError."""
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

    def test_userdata_post_delete_without_failure(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Add/delete to a non-existing host raises ValueError."""
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

    def test_userdata_correct_args_wrong_place(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Correct metadata keys but wrong file paths."""
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

    def test_userdata_empty_metadata_value_error(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Adding user data with missing metadata value raises ValueError."""
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

    def test_userdata_non_path_xarray(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Adding non-path, non-xarray items raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            databrowser.userdata(
                "add",
                userdata_items=[[1]],
                metadata={"username": "johndoe"},
                host=test_server,
            )

    def test_add_userdata_wildcard(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Adding user data with wildcard paths."""
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


class TestFlavourOperations:
    """Tests for custom flavour add, list, update, delete operations."""

    def test_flavour_lifecycle(
        self,
        test_server: str,
        mock_authenticate: Token,
        mocker: MockerFixture,
    ) -> None:
        """Test the full lifecycle: list, add, update, rename, delete."""
        # list existing flavours
        flavour_name = namegenerator.gen()
        mocker.patch("freva_client.utils.choose_token_strategy").return_value = (
            "use_token"
        )
        result = databrowser.flavour(action="list", host=test_server)
        assert isinstance(result["flavours"], list)
        assert len(result["flavours"]) >= 5
        flavour_names = [f["flavour_name"] for f in result["flavours"]]
        assert "freva" in flavour_names
        assert "cmip6" in flavour_names

        # add a custom flavour
        custom_mapping = {
            "project": "projekt",
            "variable": "variable_name",
            "model": "modell",
        }
        databrowser.flavour(
            action="add",
            name=flavour_name,
            mapping=custom_mapping,
            is_global=False,
            host=test_server,
        )

        # verify it appears
        result_after = databrowser.flavour(action="list", host=test_server)
        assert len(result_after["flavours"]) > len(result["flavours"])
        new_names = [f["flavour_name"] for f in result_after["flavours"]]
        assert flavour_name in new_names

        # use the custom flavour in a search
        db = databrowser(flavour=flavour_name, host=test_server)
        assert len(db) >= 0

        # count values with custom flavour
        db_count = databrowser.count_values(
            "projekt", flavour=flavour_name, host=test_server
        )
        assert isinstance(db_count, dict)

        # update the custom flavour
        databrowser.flavour(
            action="update",
            name=flavour_name,
            mapping={"experiment": "exp_updated"},
            is_global=False,
            host=test_server,
        )

        # rename
        databrowser.flavour(
            action="update",
            name=flavour_name,
            new_name="test_flavour_client_renamed",
            is_global=False,
            host=test_server,
        )

        # verify rename
        result_renamed = databrowser.flavour(action="list", host=test_server)
        renamed_names = [f["flavour_name"] for f in result_renamed["flavours"]]
        assert "test_flavour_client_renamed" in renamed_names

        # delete the custom flavour
        databrowser.flavour(
            action="delete",
            name="test_flavour_client_renamed",
            host=test_server,
        )

        # verify it's gone
        flavours_final = databrowser.flavour(action="list", host=test_server)
        final_names = [f["flavour_name"] for f in flavours_final["flavours"]]
        assert "test_flavour_client_renamed" not in final_names
        assert len(flavours_final["flavours"]) == len(result["flavours"])

    def test_flavour_list_non_existent_host_raises(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Listing flavours on an unreachable host raises ValueError."""
        with pytest.raises(ValueError):
            databrowser.flavour(
                action="list",
                host="http://non-existent-host:9999",
                fail_on_error=True,
            )


class TestFlavourErrorCases:
    """Tests for flavour error handling."""

    def test_wrong_facet_key(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Adding a flavour with an invalid facet key returns None."""
        result = databrowser.flavour(
            action="add",
            name="test_flavour_no_auth",
            mapping={"projecta": "projekt"},
            host=test_server,
        )
        assert result is None

    def test_add_missing_name_and_mapping(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Adding without name/mapping should raise ValueError."""
        with pytest.raises(
            ValueError, match="Both 'name' and 'mapping' are required"
        ):
            databrowser.flavour(action="add", host=test_server)

    def test_update_missing_name(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Updating without name should raise ValueError."""
        with pytest.raises(
            ValueError, match="'name' is required for update action"
        ):
            databrowser.flavour(action="update", host=test_server)

    def test_delete_missing_name(
        self, test_server: str, mock_authenticate: Token
    ) -> None:
        """Deleting without name should raise ValueError."""
        with pytest.raises(
            ValueError, match="'name' is required for delete action"
        ):
            databrowser.flavour(action="delete", host=test_server)


class TestFlavourWithoutAuth:
    """Tests for flavour operations that don't require authentication."""

    def test_list_without_auth(self, test_server: str) -> None:
        """Listing flavours without authentication should still work."""
        flavours = databrowser.flavour(action="list", host=test_server)
        assert isinstance(flavours, dict)
