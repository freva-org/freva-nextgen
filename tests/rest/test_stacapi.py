"""Unit tests for data queries via the rest-api."""

import subprocess

import pytest
import requests


def test_stacapi_basic(test_server: str) -> None:
    """Test the default stacapi functionality."""
    result_catalog = requests.get(f"{test_server}/stacapi/")
    assert result_catalog.json()["stac_version"] == "1.0.0"
    assert result_catalog.json()["type"] == "Catalog"

    result_collections = requests.get(f"{test_server}/stacapi/collections")
    assert isinstance(result_collections.json()["collections"], list)
    assert len(result_collections.json()["links"]) > 0

    result_collection = requests.get(f"{test_server}/stacapi/collections/cmip6")
    assert result_collection.status_code == 200
    assert result_collection.json()["id"] == "cmip6"
    assert result_collection.json()["stac_version"] == "1.0.0"
    assert result_collection.json()["type"] == "Collection"

    result_items = requests.get(f"{test_server}/stacapi/collections/cmip6/items")
    assert result_items.status_code == 200
    assert isinstance(result_items.json()["features"], list)
    assert len(result_items.json()["features"]) > 0
    assert result_items.json()["type"] == "FeatureCollection"


def test_stacapi_conformance(test_server: str) -> None:
    """Test the default stacapi conformance functionality."""
    result = requests.get(f"{test_server}/stacapi/conformance")
    assert result.status_code == 200
    assert isinstance(result.json()["conformsTo"], list)
    assert len(result.json()["conformsTo"]) > 0
    assert "https://api.stacspec.org/v1.0.0/core" in result.json()["conformsTo"]
    assert (
        "https://api.stacspec.org/v1.0.0/ogcapi-features"
        in result.json()["conformsTo"]
    )


def test_stacapi_item_params(test_server: str) -> None:
    """Test the default stacapi item parameters functionality."""
    result = requests.get(
        f"{test_server}/stacapi/collections/cmip6/items", params={"limit": 1}
    )
    assert result.status_code == 200
    assert len(result.json()["features"]) == 1

    # invalid parameter
    result = requests.get(
        f"{test_server}/stacapi/collections/cmip6/items", params={"limit": 0}
    )
    assert result.status_code == 422

    result = requests.get(
        f"{test_server}/stacapi/collections/cmip6/items", params={"limitx": 1001}
    )
    assert result.status_code == 400

    result = requests.get(
        f"{test_server}/stacapi/collections/cmip6/items",
        params={"datetime": "2023-10-01/2023-10-31", "bbox": "10,20,30,40"},
    )
    assert result.status_code == 200
    assert isinstance(result.json()["features"], list)

    result = requests.get(
        f"{test_server}/stacapi/collections/cmip6/items",
        params={"datetime": "2023-10-01", "bbox": "10,20,30,40"},
    )
    assert result.status_code == 200
    assert isinstance(result.json()["features"], list)

    result = requests.get(
        f"{test_server}/stacapi/collections/cmip6/items",
        params={"datetime": "/2023-10-01", "bbox": "10,20,30,40"},
    )
    assert result.status_code == 422

    # test the next and previous token
    result_get = requests.get(f"{test_server}/stacapi/collections/cordex/items")
    cordex_length = len(result_get.json().get("features"))
    last_item_id = result_get.json().get("features")[cordex_length - 1].get("id")
    result = requests.get(
        f"{test_server}/stacapi/collections/cordex/items",
        params={"limit": 1, "token": f"prev:cordex:{last_item_id}"},
    )
    assert result.status_code == 200
    assert len(result.json()["features"]) == 1

    first_item_id = result_get.json().get("features")[0].get("id")
    result = requests.get(
        f"{test_server}/stacapi/collections/cordex/items",
        params={"limit": 1, "token": f"next:cordex:{first_item_id}"},
    )
    assert result.status_code == 200
    assert len(result.json()["features"]) == 1

    result = requests.get(
        f"{test_server}/stacapi/collections/cordex/items",
        params={"limit": 1, "token": f"wrong_direction:cordex:{last_item_id}"},
    )
    assert result.status_code == 422

    result = requests.get(
        f"{test_server}/stacapi/collections/cordex/items",
        params={"limit": 1, "token": f"wrong_direction:cordex:"},
    )
    assert result.status_code == 422


def test_stacapi_staccheck(test_server: str) -> None:
    """Test the stacapi staccheck functionality."""
    result_output = subprocess.run(
        ["stac-check", f"{test_server}/stacapi/"], check=True, capture_output=True
    )
    assert "CATALOG Passed: True" in result_output.stdout.decode("utf-8")

    result_output = subprocess.run(
        ["stac-check", f"{test_server}/stacapi/collections/cmip6/"],
        check=True,
        capture_output=True,
    )
    assert "COLLECTION Passed: True" in result_output.stdout.decode("utf-8")

    result_get = requests.get(
        f"{test_server}/stacapi/collections/nextgems/items?limit=1"
    )
    item_id = result_get.json().get("features")[0].get("id")
    result_output = subprocess.run(
        [
            "stac-check",
            f"{test_server}/stacapi/collections/nextgems/items/{item_id}/",
        ],
        check=True,
        capture_output=True,
    )

    assert "ITEM Passed: True" in result_output.stdout.decode("utf-8")


def test_stacapi_fail(test_server: str) -> None:
    """Test the stacapi fail functionality."""
    result = requests.get(f"{test_server}/stacapi/collections/cmip69/")
    assert result.status_code == 404

    result = requests.get(
        f"{test_server}/stacapi/collections/cmip6/items/wrong_item_id"
    )
    assert result.status_code == 404


def test_stacapi_search_get(test_server: str) -> None:
    """Test the STAC API search GET endpoint."""
    res1 = requests.get(f"{test_server}/stacapi/search")
    assert res1.status_code == 200
    data = res1.json()
    assert "features" in data
    assert "type" in data
    assert data["type"] == "FeatureCollection"

    res2 = requests.get(
        f"{test_server}/stacapi/search",
        params={"collections": "cmip6", "limit": 5},
    )
    assert res2.status_code == 200

    res3 = requests.get(
        f"{test_server}/stacapi/search",
        params={"bbox": "10,20,30,40", "limit": 3},
    )
    assert res3.status_code == 200

    # Invalid bbox format
    res4 = requests.get(
        f"{test_server}/stacapi/search", params={"bbox": "invalid_bbox"}
    )
    assert res4.status_code == 422


def test_stacapi_search_post(test_server: str) -> None:
    """Test the STAC API search POST endpoint."""
    search_body = {"limit": 5}
    res1 = requests.post(f"{test_server}/stacapi/search", json=search_body)
    assert res1.status_code == 200
    data = res1.json()
    assert "features" in data
    assert "type" in data

    search_body = {"collections": ["cmip6"], "limit": 3}
    res2 = requests.post(f"{test_server}/stacapi/search", json=search_body)
    assert res2.status_code == 200

    search_body = {"bbox": [10, 20, 30, 40], "limit": 2}
    res3 = requests.post(f"{test_server}/stacapi/search", json=search_body)
    assert res3.status_code == 200

    # Invalid POST body
    res4 = requests.post(f"{test_server}/stacapi/search", json={"limit": 0})
    assert res4.status_code == 422

    # POST search with Free Text Search list
    search_body = {"q": "[cmip, temperature]", "limit": 2}
    res5 = requests.post(f"{test_server}/stacapi/search", json=search_body)
    assert res5.status_code == 200

    search_body = {"q": "cmip", "limit": 2}
    res6 = requests.post(f"{test_server}/stacapi/search", json=search_body)
    assert res6.status_code == 200


def test_stacapi_queryables(test_server: str) -> None:
    """Test the STAC API queryables endpoints."""
    res1 = requests.get(f"{test_server}/stacapi/queryables")
    assert res1.status_code == 200
    data = res1.json()
    assert "$schema" in data
    assert "properties" in data

    res2 = requests.get(f"{test_server}/stacapi/collections/cmip6/queryables")
    assert res2.status_code == 200
    data = res2.json()
    assert "$schema" in data
    assert "properties" in data

    # Invalid collection queryables
    res3 = requests.get(f"{test_server}/stacapi/collections/invalid/queryables")
    assert res3.status_code == 404


def test_stacapi_ping(test_server: str) -> None:
    """Test the nextgen STAC API ping endpoint."""
    res = requests.get(f"{test_server}/stacapi/_mgmt/ping")
    assert res.status_code == 200
    data = res.json()
    assert data["message"] == "PONG"


def test_stacapi_search_params(test_server: str) -> None:
    """Test the nextgen STAC API search parameter validation."""

    res1 = requests.get(
        f"{test_server}/stacapi/search",
        params={"datetime": "2023-01-01/2023-12-31", "limit": 2},
    )
    assert res1.status_code == 200

    res2 = requests.get(
        f"{test_server}/stacapi/search", params={"ids": "some_id", "limit": 1}
    )
    assert res2.status_code == 200

    # with free text search
    res3 = requests.get(
        f"{test_server}/stacapi/search",
        params={"q": "climate,temperature", "limit": 2},
    )
    assert res3.status_code == 200

    res4 = requests.get(
        f"{test_server}/stacapi/search",
        params={"token": "next:search:some_id", "limit": 1},
    )
    assert res4.status_code == 200

    # Invalid token format
    res5 = requests.get(
        f"{test_server}/stacapi/search", params={"token": "invalid_token_format"}
    )
    assert res5.status_code == 422


def test_stacapi_search_filter(test_server: str) -> None:
    """test for CQL2 filters."""

    # =, !=, <, <=, >, >=, isNull
    filters = [
        '{"op": "=", "args": [{"property": "project"}, "cmip6"]}',
        '{"op": "!=", "args": [{"property": "project"}, "cmip6"]}',
        '{"op": "<", "args": [{"property": "variable"}, "z"]}',
        '{"op": "<=", "args": [{"property": "variable"}, "tas"]}',
        '{"op": ">", "args": [{"property": "variable"}, "a"]}',
        '{"op": ">=", "args": [{"property": "variable"}, "a"]}',
        '{"op": "isNull", "args": [{"property": "nonexistent"}]}',
    ]

    for filter_json in filters:
        res = requests.get(
            f"{test_server}/stacapi/search",
            params={"filter": filter_json, "limit": 2},
        )
        assert res.status_code == 200

    search_body = {
        "limit": 3,
        "filter": {
            "op": "and",
            "args": [
                {
                    "op": "or",
                    "args": [
                        {
                            "op": "=",
                            "args": [{"property": "collection"}, "cmip6"],
                        },  # collection to project mapping
                        {"op": "=", "args": [{"property": "project"}, "cordex"]},
                    ],
                },
                {
                    "op": "not",
                    "args": [
                        {
                            "op": "=",
                            "args": [{"property": "id"}, "nonexistent"],
                        }  # id to file/uri mapping
                    ],
                },
            ],
        },
    }
    res = requests.post(f"{test_server}/stacapi/search", json=search_body)
    assert res.status_code == 200

    # t_after, t_before, t_during
    temporal_filters = [
        # timestamp dict
        {
            "op": "t_after",
            "args": [
                {"property": "datetime"},
                {"timestamp": "2020-01-01T00:00:00Z"},
            ],
        },
        {
            "op": "t_before",
            "args": [
                {"property": "datetime"},
                {"timestamp": "2025-01-01T00:00:00Z"},
            ],
        },
        # string
        {
            "op": "t_before",
            "args": [{"property": "datetime"}, "2025-01-01T00:00:00Z"],
        },
        {
            "op": "t_after",
            "args": [{"property": "datetime"}, "2020-01-01T00:00:00Z"],
        },
        # interval dict
        {
            "op": "t_during",
            "args": [
                {"property": "datetime"},
                {"interval": ["2020-01-01T00:00:00Z", "2023-12-31T23:59:59Z"]},
            ],
        },
        # interval list
        {
            "op": "t_during",
            "args": [
                {"property": "datetime"},
                ["2020-01-01T00:00:00Z", "2023-12-31T23:59:59Z"],
            ],
        },
    ]

    for temp_filter in temporal_filters:
        search_body = {
            "limit": 2,
            "filter": {
                "op": "and",
                "args": [
                    {
                        "op": "s_intersects",
                        "args": [
                            {"property": "geometry"},
                            {
                                "type": "Polygon",
                                "coordinates": [
                                    [
                                        [10.0, 20.0],
                                        [30.0, 20.0],
                                        [30.0, 40.0],
                                        [10.0, 40.0],
                                        [10.0, 20.0],
                                    ]
                                ],
                            },
                        ],
                    },
                    temp_filter,
                ],
            },
        }
        res = requests.post(f"{test_server}/stacapi/search", json=search_body)
        assert res.status_code == 200

    # filter + collections + bbox + datetime
    search_body = {
        "collections": ["cmip6"],
        "bbox": [10, 20, 30, 40],
        "datetime": "2020-01-01/2023-12-31",
        "limit": 2,
        "filter": {"op": "=", "args": [{"property": "realm"}, "atmos"]},
    }
    res = requests.post(f"{test_server}/stacapi/search", json=search_body)
    assert res.status_code == 200

    # empty filters errors
    error_cases = [
        # empty
        {"filter": {}},
        # missing operator
        {"filter": {"args": [{"property": "project"}, "cmip6"]}},
        # Invalid operator
        {
            "filter": {
                "op": "invalid_op",
                "args": [{"property": "project"}, "cmip6"],
            }
        },
        # missing args for comparison operators
        {"filter": {"op": "=", "args": [{"property": "project"}]}},
        {"filter": {"op": "!=", "args": []}},
        {"filter": {"op": "<", "args": [{"property": "field"}]}},
        {"filter": {"op": "<=", "args": []}},
        {"filter": {"op": ">", "args": [{"property": "field"}]}},
        {"filter": {"op": ">=", "args": []}},
        {"filter": {"op": "isNull", "args": []}},
        # missed property in args
        {"filter": {"op": "=", "args": [{"notproperty": "field"}, "value"]}},
        {"filter": {"op": "isNull", "args": [{"notproperty": "field"}]}},
        # Logical operators invalid args
        {"filter": {"op": "and", "args": []}},
        {"filter": {"op": "or", "args": []}},
        {"filter": {"op": "not", "args": []}},
        {"filter": {"op": "not", "args": [{"invalid": "structure"}]}},
        # bbox invalid
        {"filter": {"op": "s_intersects", "args": []}},
        {"filter": {"op": "s_intersects", "args": [{"property": "geometry"}]}},
        {
            "filter": {
                "op": "s_intersects",
                "args": [{"property": "notgeometry"}, {"type": "Polygon"}],
            }
        },
        {
            "filter": {
                "op": "s_intersects",
                "args": [{"property": "geometry"}, {"type": "Point"}],
            }
        },
        {
            "filter": {
                "op": "s_intersects",
                "args": [
                    {"property": "geometry"},
                    {"type": "Polygon", "coordinates": []},
                ],
            }
        },
        {
            "filter": {
                "op": "s_intersects",
                "args": [
                    {"property": "geometry"},
                    {"type": "Polygon", "coordinates": [[[10, 20]]]},
                ],
            }
        },
        # time invalid
        {"filter": {"op": "t_after", "args": []}},
        {"filter": {"op": "t_after", "args": [{"property": "datetime"}]}},
        {
            "filter": {
                "op": "t_after",
                "args": [{"property": "notdatetime"}, "2020-01-01T00:00:00Z"],
            }
        },
        {
            "filter": {
                "op": "t_after",
                "args": [
                    {"property": "datetime"},
                    {"nottimestamp": "2020-01-01T00:00:00Z"},
                ],
            }
        },
        {"filter": {"op": "t_before", "args": []}},
        {"filter": {"op": "t_before", "args": [{"property": "datetime"}]}},
        {
            "filter": {
                "op": "t_before",
                "args": [{"property": "notdatetime"}, "2020-01-01T00:00:00Z"],
            }
        },
        {
            "filter": {
                "op": "t_before",
                "args": [
                    {"property": "datetime"},
                    {"nottimestamp": "2020-01-01T00:00:00Z"},
                ],
            }
        },
        {"filter": {"op": "t_during", "args": []}},
        {"filter": {"op": "t_during", "args": [{"property": "datetime"}]}},
        {
            "filter": {
                "op": "t_during",
                "args": [
                    {"property": "notdatetime"},
                    ["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"],
                ],
            }
        },
        {
            "filter": {
                "op": "t_during",
                "args": [
                    {"property": "datetime"},
                    {
                        "notinterval": [
                            "2020-01-01T00:00:00Z",
                            "2021-01-01T00:00:00Z",
                        ]
                    },
                ],
            }
        },
        {
            "filter": {
                "op": "t_during",
                "args": [{"property": "datetime"}, ["2020-01-01T00:00:00Z"]],
            }
        },
        {
            "filter": {
                "op": "=",
                "args": [
                    {"property": "project"},
                    "test:value/with+special-chars",
                ],
            }
        },
        {"filter": {"op": ">", "args": [{"property": "numeric_field"}, 100]}},
    ]

    for case in error_cases:
        res = requests.post(
            f"{test_server}/stacapi/search", json={**case, "limit": 1}
        )
        assert res.status_code == 200

    # project, id mapping
    mapping_tests = [
        {"filter": {"op": "=", "args": [{"property": "collection"}, "cmip6"]}},
        {"filter": {"op": "=", "args": [{"property": "id"}, "some_file"]}},
        # collection -> project mapping with non-string value (hits else: return [f'{field}:{value}'])
        {"filter": {"op": "=", "args": [{"property": "collection"}, 123]}},
        # id non-string value
        {"filter": {"op": "=", "args": [{"property": "id"}, 456]}},
        # fields with non-string value =
        {"filter": {"op": "=", "args": [{"property": "numeric_field"}, 789]}},
        {"filter": {"op": "=", "args": [{"property": "boolean_field"}, True]}},
        {"filter": {"op": "!=", "args": [{"property": "collection"}, "cmip6"]}},
        # id -> uniq_key mapping with string value
        {"filter": {"op": "!=", "args": [{"property": "id"}, "some_file"]}},
        # collection non-string
        {"filter": {"op": "!=", "args": [{"property": "collection"}, 123]}},
        # id non-string
        {"filter": {"op": "!=", "args": [{"property": "id"}, 456]}},
        # fields with non-string value !=, !=, <, <=, >, >=
        {"filter": {"op": "!=", "args": [{"property": "numeric_field"}, 789]}},
        {"filter": {"op": "!=", "args": [{"property": "boolean_field"}, True]}},
        {"filter": {"op": "<", "args": [{"property": "collection"}, "cmip6"]}},
        {"filter": {"op": ">", "args": [{"property": "collection"}, "cmip6"]}},
        {"filter": {"op": ">=", "args": [{"property": "collection"}, "cmip6"]}},
        {"filter": {"op": "<=", "args": [{"property": "collection"}, "cmip6"]}},
    ]

    for case in mapping_tests:
        res = requests.post(
            f"{test_server}/stacapi/search", json={**case, "limit": 1}
        )
        assert res.status_code == 200

    # Invalid JSON: an unparseable CQL2 filter must be rejected (400),
    # not silently ignored.
    res = requests.get(
        f"{test_server}/stacapi/search",
        params={"filter": "invalid_json", "limit": 1},
    )
    assert res.status_code == 400


def test_classify_storage() -> None:
    """fs_type decides local vs remote"""
    from freva_rest.utils.stac_assets import classify_storage

    assert classify_storage("/work/bm1159/data.nc", "posix") == "local"
    assert classify_storage("s3://bucket/data.zarr", "swift") == "remote"
    assert classify_storage("/mnt/data.nc", "s3") == "remote"

    for fs_type in ("nfs", "lustre", "LUSTRE", " posix "):
        assert classify_storage("/work/data.nc", fs_type) == "local"

    assert classify_storage("/random/path/to/data.zarr") == "local"
    assert classify_storage("file:///random/path/data.nc") == "local"
    assert classify_storage("gs://bucket/data.zarr") == "remote"


@pytest.mark.parametrize(
    "path,media_type,engine",
    [
        # Zarr stores, with and without the trailing slash fsspec
        ("/data/tas.zarr", "application/vnd+zarr", "zarr"),
        ("s3://b/tas.zarr/", "application/vnd+zarr", "zarr"),
        # netCDF spellings
        ("/data/tas.nc", "application/netcdf", "h5netcdf"),
        ("/data/tas.nc4", "application/netcdf", "h5netcdf"),
        ("/data/tas.cdf", "application/netcdf", "h5netcdf"),
        ("/data/tas.netcdf", "application/netcdf", "h5netcdf"),
        # GRIB needs a different engine as well as a different type
        ("/data/tas.grb", "application/wmo-GRIB2", "cfgrib"),
        ("/data/tas.grib", "application/wmo-GRIB2", "cfgrib"),
        ("/data/tas.grb2", "application/wmo-GRIB2", "cfgrib"),
        ("/data/tas.grib2", "application/wmo-GRIB2", "cfgrib"),
        # HDF5 and GeoTIFF are typed, but open with the netCDF engine
        ("/data/tas.h5", "application/x-hdf5", "h5netcdf"),
        ("/data/tas.hdf5", "application/x-hdf5", "h5netcdf"),
        ("/data/tas.hdf", "application/x-hdf5", "h5netcdf"),
        ("/data/dem.tif", "image/tiff; application=geotiff", "h5netcdf"),
        ("/data/dem.tiff", "image/tiff; application=geotiff", "h5netcdf"),
        # Unknown suffixes must degrade, not raise
        ("/data/tas.dat", "application/octet-stream", "h5netcdf"),
        ("/data/tas", "application/octet-stream", "h5netcdf"),
        # Case is irrelevant
        ("/data/TAS.GRIB2", "application/wmo-GRIB2", "cfgrib"),
        ("/data/TAS.NC", "application/netcdf", "h5netcdf"),
    ],
)
def test_guess_media_type_and_engine(path, media_type, engine) -> None:
    """Every supported suffix maps to a media type and an xarray engine."""
    from freva_rest.utils.stac_assets import guess_engine, guess_media_type

    assert guess_media_type(path) == media_type
    assert guess_engine(path) == engine


@pytest.mark.parametrize(
    "path,media_type,engine",
    [
        ("s3://b/tas.grib2", "application/wmo-GRIB2", "cfgrib"),
        ("s3://b/tas.h5", "application/x-hdf5", "h5netcdf"),
        ("s3://b/dem.tif", "image/tiff; application=geotiff", "h5netcdf"),
        ("s3://b/tas.dat", "application/octet-stream", "h5netcdf"),
    ],
)
def test_remote_asset_uses_sniffed_format(path, media_type, engine) -> None:
    """The sniffed format reaches the asset the user actually sees."""
    from freva_rest.utils.stac_assets import AssetContext, build_item_assets

    asset = build_item_assets(
        AssetContext("https://host"), path, fs_type="s3"
    )["access-data"]

    assert asset.media_type == media_type
    assert f'engine="{engine}"' in asset.description
    assert f"pip install xarray fsspec {engine}" in asset.description


def test_local_asset_ignores_format() -> None:
    """Local data is always streamed as Zarr, whatever the source format."""
    from freva_rest.utils.stac_assets import AssetContext, build_item_assets

    ctx = AssetContext("https://host")
    for path in ("/w/tas.grib2", "/w/tas.h5", "/w/dem.tif", "/w/tas.dat"):
        asset = build_item_assets(ctx, path, fs_type="posix")["access-data"]
        assert asset.media_type == "application/vnd+zarr"
        assert 'engine="zarr"' in asset.description
        assert "cfgrib" not in asset.description


@pytest.mark.parametrize(
    "url",
    [
        "https://s3.waterpark.dkrz.de/cmip6/healpix/P1M/level_5.zarr",
        "http://data.example.org/tas.zarr",
        "https://data.example.org/tas.nc",
    ],
)
def test_remote_http_never_passes_anon(url) -> None:
    """
    anon is not a generic fsspec option.
    """
    from freva_rest.utils.stac_assets import AssetContext, build_item_assets

    desc = build_item_assets(
        AssetContext("https://host"), url, fs_type="swift"
    )["access-data"].description

    assert "anon" not in desc
    assert "aiohttp" in desc


@pytest.mark.parametrize(
    "url,backend",
    [
        ("s3://bucket/tas.zarr", "s3fs"),
        ("gs://bucket/tas.zarr", "gcsfs"),
        ("az://container/tas.zarr", "adlfs"),
        ("s3://bucket/tas.nc", "s3fs"),
    ],
)
def test_remote_object_store_passes_anon(url, backend) -> None:
    """Object stores do understand anon, and need a backend package."""
    from freva_rest.utils.stac_assets import AssetContext, build_item_assets

    desc = build_item_assets(
        AssetContext("https://host"), url, fs_type="s3"
    )["access-data"].description

    assert "anon" in desc
    assert backend in desc


def test_remote_zarr_does_not_use_fsspec() -> None:
    """xarray opens a Zarr URL directly; a mapper adds nothing."""
    from freva_rest.utils.stac_assets import AssetContext, build_item_assets

    desc = build_item_assets(
        AssetContext("https://host"), "s3://b/tas.zarr", fs_type="s3"
    )["access-data"].description

    assert "get_mapper" not in desc
    assert "import fsspec" not in desc
    assert 'storage_options={"anon": True}' in desc


def test_remote_snippets_are_executable_python() -> None:
    """The generated snippets must at least parse and import cleanly."""
    import ast

    from freva_rest.utils.stac_assets import AssetContext, build_item_assets

    ctx = AssetContext("https://host")
    for url in (
        "https://d.org/tas.zarr",
        "s3://b/tas.zarr",
        "https://d.org/tas.nc",
        "s3://b/tas.grib2",
        "gs://b/tas.h5",
    ):
        desc = build_item_assets(ctx, url, fs_type="s3")["access-data"].description
        code = desc.split("```python")[1].split("```")[0]
        ast.parse(code)


def test_pystac_snippets_are_correct() -> None:
    """
    The pystac snippets must not wrap describe() in print().
    """
    from freva_rest.utils.stac_assets import (
        STATIC_COLLECTION_ASSETS,
        AssetContext,
        build_collection_assets,
        build_item_assets,
    )

    ctx = AssetContext("https://host")
    download = build_item_assets(ctx, "/w/x.nc", fs_type="posix")[
        "stac-catalogue"
    ].description
    archive = build_collection_assets(ctx, include=STATIC_COLLECTION_ASSETS)[
        "stac-catalogue"
    ].description

    for desc in (download, archive):
        assert "print(catalog.describe())" not in desc
        assert "catalog.describe()" in desc
        assert 'from_file("stac-catalog/catalog.json")' in desc
        assert "stac-catalog/stac-catalog" not in desc

    assert "unzip stac-catalog.zip -d stac-catalog" not in download


def test_streamed_access_uses_documented_auth() -> None:
    """
    The generated snippets must match the real freva-client auth API.
    """
    from freva_rest.utils.stac_assets import (
        STATIC_COLLECTION_ASSETS,
        AssetContext,
        build_collection_assets,
        build_item_assets,
    )

    ctx = AssetContext("https://host", params={"project": "cmip6"})
    item = build_item_assets(ctx, "/w/tas.nc", fs_type="posix")["access-data"]
    collection = build_collection_assets(
        ctx, include=STATIC_COLLECTION_ASSETS
    )["access-data"]

    for desc in (item.description, collection.description):
        assert "from freva_client import authenticate, databrowser" in desc
        assert 'authenticate(host="https://host")["headers"]' in desc
        assert "storage_options=storage_options" in desc
        assert "auth_token" not in desc
        assert "Authorization: Bearer" not in desc.split("## 4.")[0]

        assert "freva-client auth --host https://host" in desc
        assert "--token-file ~/.freva-token.json" in desc
        assert "chmod 600" in desc

        assert "Freva web portal" in desc
        assert "authenticate()" in desc


def test_classify_storage_vocabularies_are_separate() -> None:
    """
    fs_type names and URI schemes are different vocabularies.
    """
    from freva_rest.utils.stac_assets import (
        LOCAL_FS_TYPES,
        LOCAL_PROTOCOLS,
        classify_storage,
    )

    assert "lustre" not in LOCAL_PROTOCOLS
    assert "file" not in LOCAL_FS_TYPES
    assert classify_storage("/work/data.nc", "lustre") == "local"
    assert classify_storage("file:///work/data.nc") == "local"


def test_access_desc_remote_files() -> None:
    """Remote data is opened directly with xarray, with an access caveat."""
    from freva_rest.utils.stac_assets import AssetContext, build_item_assets

    ctx = AssetContext("https://host")

    zarr_asset = build_item_assets(
        ctx, "gs://bucket/data.zarr", fs_type="s3"
    )["access-data"]
    assert zarr_asset.href == "gs://bucket/data.zarr"
    assert zarr_asset.media_type == "application/vnd+zarr"
    assert 'xr.open_dataset(' in zarr_asset.description
    assert 'engine="zarr"' in zarr_asset.description

    nc_asset = build_item_assets(
        ctx, "s3://random/bucket/data.nc", fs_type="s3"
    )["access-data"]
    assert nc_asset.href == "s3://random/bucket/data.nc"
    assert nc_asset.media_type == "application/netcdf"
    assert "fsspec.open" in nc_asset.description
    assert 'engine="h5netcdf"' in nc_asset.description

    for asset in (zarr_asset, nc_asset):
        assert asset.to_dict()["freva:storage"] == "remote"
        assert "may be restricted" in asset.description
        assert "project coordinator" in asset.description
        assert "requires" not in asset.to_dict()


def test_access_desc_local_files() -> None:
    """Local data is not handed out as a path -- it is streamed as Zarr."""
    from freva_rest.utils.stac_assets import AssetContext, build_item_assets

    ctx = AssetContext("https://host")
    asset = build_item_assets(
        ctx, "/random/path/to/data.zarr", fs_type="posix"
    )["access-data"]

    assert asset.href == (
        "/random/path/to/data.zarr"
    )
    assert asset.media_type == "application/vnd+zarr"
    assert asset.to_dict()["requires"] == ["oauth2"]
    assert asset.to_dict()["freva:storage"] == "local"

    assert "pip install freva-client" in asset.description
    assert "stream_zarr=True" in asset.description
    assert 'engine="zarr"' in asset.description


def test_item_asset_keys() -> None:
    """Items carry the five standard assets; the static catalogue drops the
    self-referential STAC download."""
    from freva_rest.utils.stac_assets import AssetContext, build_item_assets

    ctx = AssetContext("https://host")
    assets = build_item_assets(ctx, "/data.nc", fs_type="posix")
    assert set(assets) == {
        "freva-databrowser",
        "freva-data-viewer",
        "access-data",
        "intake-catalogue",
        "stac-catalogue",
    }
    assert assets["freva-data-viewer"].href == (
        "https://host/inspect/?file=%2Fdata.nc"
    )

    static = build_item_assets(
        ctx,
        "/data.nc",
        fs_type="posix",
        include=(
            "freva-databrowser",
            "freva-data-viewer",
            "access-data",
            "intake-catalogue",
        ),
    )
    assert "stac-catalogue" not in static


def test_collection_asset_keys() -> None:
    """
    The two catalogue flavours expose deliberately different collection
    assets.
    """
    from freva_rest.utils.stac_assets import (
        API_COLLECTION_ASSETS,
        STATIC_COLLECTION_ASSETS,
        AssetContext,
        build_collection_assets,
    )

    ctx = AssetContext("https://host", params={"project": "cmip6"})

    api_assets = build_collection_assets(
        ctx, facet="project", value="cmip6", include=API_COLLECTION_ASSETS
    )
    assert set(api_assets) == {"freva-databrowser"}
    assert api_assets["freva-databrowser"].href == (
        "https://host/databrowser/?project=cmip6"
    )
    static_assets = build_collection_assets(
        ctx, include=STATIC_COLLECTION_ASSETS
    )
    assert set(static_assets) == {
        "freva-databrowser",
        "access-data",
        "intake-catalogue",
        "stac-catalogue",
    }
    archive = static_assets["stac-catalogue"]
    assert "already been downloaded" in archive.description
    assert "pystac" in archive.description
    assert static_assets["access-data"].to_dict()["requires"] == ["oauth2"]


def test_stacapi_axis_backwards_compatible(test_server: str) -> None:
    """
    Unscoped (legacy) routes must behave exactly as before.
    """
    res = requests.get(f"{test_server}/stacapi/")
    assert res.status_code == 200
    assert res.json()["type"] == "Catalog"

    # No axis segment in any self/child link of the default landing page.
    for link in res.json()["links"]:
        assert "/stacapi/project/" not in link["href"]

    res_col = requests.get(f"{test_server}/stacapi/collections/cmip6")
    assert res_col.status_code == 200
    assert res_col.json()["id"] == "cmip6"


def test_stacapi_axis_project_alias(test_server: str) -> None:
    """The explicit ``project`` axis root mirrors the default behaviour."""
    res = requests.get(f"{test_server}/stacapi/project/")
    assert res.status_code == 200
    assert res.json()["type"] == "Catalog"

    res_col = requests.get(f"{test_server}/stacapi/project/collections/cmip6")
    assert res_col.status_code == 200
    assert res_col.json()["id"] == "cmip6"
    self_links = [
        link
        for link in res_col.json()["links"]
        if link["rel"] == "self"
    ]
    assert self_links
    assert "/stacapi/project/collections/cmip6" in self_links[0]["href"]


def test_stacapi_axis_product(test_server: str) -> None:
    """
    different axis exposes a different set of collections.
    """
    res = requests.get(f"{test_server}/stacapi/product/collections")
    assert res.status_code == 200
    collection_ids = {c["id"] for c in res.json()["collections"]}
    assert collection_ids
    assert "cmip6" not in collection_ids
    some_product = sorted(collection_ids)[0]
    res_items = requests.get(
        f"{test_server}/stacapi/product/collections/{some_product}/items",
        params={"limit": 1},
    )
    assert res_items.status_code == 200
    assert res_items.json()["type"] == "FeatureCollection"


def test_stacapi_axis_unknown(test_server: str) -> None:
    """An unknown / unavailable axis must return 404."""
    res = requests.get(f"{test_server}/stacapi/not_a_facet/collections")
    assert res.status_code == 404


def test_stacapi_nonexistent_collection_items_404(test_server: str) -> None:
    """Items of a collection that does not exist return 404, not an empty
    FeatureCollection."""
    res = requests.get(
        f"{test_server}/stacapi/collections/does-not-exist/items"
    )
    assert res.status_code == 404


def test_stacapi_axis_query_param_ignored(test_server: str) -> None:
    """``?axis=`` on a legacy route must NOT switch the axis (the axis is a
    path segment only)"""
    res = requests.get(f"{test_server}/stacapi/", params={"axis": "product"})
    assert res.status_code == 200
    for link in res.json()["links"]:
        assert "/stacapi/product/" not in link["href"]


def test_stacapi_invalid_cql2_400(test_server: str) -> None:
    """An invalid CQL2 filter returns 400 rather than being ignored."""
    res = requests.get(
        f"{test_server}/stacapi/search",
        params={"filter": "{not valid json"},
    )
    assert res.status_code == 400


def test_stacapi_token_context_mismatch_400(test_server: str) -> None:
    """A pagination token minted for another scope is rejected."""
    res = requests.get(
        f"{test_server}/stacapi/collections/cmip6/items",
        params={"token": "next:cordex:0"},
    )
    assert res.status_code == 400


def test_stacapi_visible_collections_landing(test_server: str) -> None:
    """``visible_collections`` filters the landing page child links and
    keeps them propagated into those links."""
    res = requests.get(
        f"{test_server}/stacapi/", params={"visible_collections": "cmip6"}
    )
    assert res.status_code == 200
    child_links = [
        link for link in res.json()["links"] if link["rel"] == "child"
    ]
    assert child_links
    for link in child_links:
        assert "/collections/cmip6" in link["href"]
        # The filter must be carried forward into the child link.
        assert "visible_collections=cmip6" in link["href"]


def test_stacapi_visible_collections_listing(test_server: str) -> None:
    """The /collections listing respects the visibility filter, and it
    agrees with the landing page."""
    res = requests.get(
        f"{test_server}/stacapi/collections",
        params={"visible_collections": "cmip6"},
    )
    assert res.status_code == 200
    ids = {c["id"] for c in res.json()["collections"]}
    assert ids == {"cmip6"}


def test_stacapi_visible_collections_hidden_404(test_server: str) -> None:
    """A collection hidden by the view must 404 on detail and items."""
    res = requests.get(
        f"{test_server}/stacapi/collections/cordex",
        params={"visible_collections": "cmip6"},
    )
    assert res.status_code == 404

    res_items = requests.get(
        f"{test_server}/stacapi/collections/cordex/items",
        params={"visible_collections": "cmip6"},
    )
    assert res_items.status_code == 404


def test_stacapi_visible_collections_search_400(test_server: str) -> None:
    """Searching an explicit collection outside the view returns 400."""
    res = requests.get(
        f"{test_server}/stacapi/search",
        params={"collections": "cordex", "visible_collections": "cmip6"},
    )
    assert res.status_code == 400


def test_stacapi_visible_collections_search_scoped(test_server: str) -> None:
    """A search with no explicit collections is constrained to the view."""
    res = requests.get(
        f"{test_server}/stacapi/search",
        params={"visible_collections": "cmip6", "limit": 5},
    )
    assert res.status_code == 200
    for feature in res.json()["features"]:
        assert feature.get("collection") == "cmip6"


def test_stacapi_visible_collections_post(test_server: str) -> None:
    """POST search honours the body ``visible_collections`` field."""
    res = requests.post(
        f"{test_server}/stacapi/search",
        json={"visible_collections": ["cmip6"], "limit": 5},
    )
    assert res.status_code == 200
    for feature in res.json()["features"]:
        assert feature.get("collection") == "cmip6"


def test_stacapi_axis_queryables(test_server: str) -> None:
    """Queryables advertise the active axis in the ``collection`` enum."""
    res = requests.get(f"{test_server}/stacapi/product/queryables")
    assert res.status_code == 200
    collection = res.json()["properties"]["collection"]
    # The enum should contain product values, not project values.
    if "enum" in collection:
        assert "cmip6" not in collection["enum"]


def test_stacapi_visible_collections_queryables_enum(test_server: str) -> None:
    """The global queryables ``collection`` enum respects the view filter."""
    res = requests.get(
        f"{test_server}/stacapi/queryables",
        params={"visible_collections": "cmip6"},
    )
    assert res.status_code == 200
    collection = res.json()["properties"]["collection"]
    if "enum" in collection:
        assert collection["enum"] == ["cmip6"]


def test_stacapi_visible_collections_queryables_hidden_404(
    test_server: str,
) -> None:
    """Collection queryables for a hidden collection 404."""
    res = requests.get(
        f"{test_server}/stacapi/collections/cordex/queryables",
        params={"visible_collections": "cmip6"},
    )
    assert res.status_code == 404


def test_stacapi_visible_collections_post_query(test_server: str) -> None:
    """POST search accepts visibility from the query string."""
    res = requests.post(
        f"{test_server}/stacapi/search?visible_collections=cmip6",
        json={"limit": 5},
    )
    assert res.status_code == 200
    for feature in res.json()["features"]:
        assert feature.get("collection") == "cmip6"


def test_stacapi_visible_collections_post_conflict(test_server: str) -> None:
    """Conflicting query vs body visibility in POST search returns 400."""
    res = requests.post(
        f"{test_server}/stacapi/search?visible_collections=cmip6",
        json={"limit": 5, "visible_collections": ["cordex"]},
    )
    assert res.status_code == 400


def test_unit_default_axis(stac_module, make_api) -> None:
    """No axis given -> defaults to ``project`` and unscoped links."""
    assert stac_module.DEFAULT_COLLECTION_AXIS == "project"
    api = make_api()
    assert api.collection_axis == "project"
    assert api.axis_in_path is False
    assert api._stac_base() == "https://host/api/freva-nextgen/stacapi"
    assert (
        api._href("/collections")
        == "https://host/api/freva-nextgen/stacapi/collections"
    )


def test_unit_axis_in_path(make_api) -> None:
    """Axis in path"""
    api = make_api(collection_axis="product", axis_in_path=True)
    assert api.collection_axis == "product"
    assert (
        api._stac_base()
        == "https://host/api/freva-nextgen/stacapi/product"
    )


def test_unit_href_carries_visibility(make_api) -> None:
    """Navigational links carry the visibility filter; identifiers do not."""
    api = make_api(
        collection_axis="product",
        visible_collections=["tas", "pr"],
        axis_in_path=True,
    )
    nav = api._href("/collections")
    assert "visible_collections=tas%2Cpr" in nav  # urlencoded comma
    ident = api._href("/queryables", navigational=False)
    assert "visible_collections" not in ident
    assert ident.endswith("/product/queryables")


def test_unit_unknown_axis_404(make_api) -> None:
    """An axis not in the hierarchy raises 404."""
    with pytest.raises(Exception) as exc:
        make_api(collection_axis="bogus", axis_in_path=True)
    assert getattr(exc.value, "status_code", None) == 404


def test_unit_axis_not_in_solr_404(make_api) -> None:
    """An axis in the hierarchy but absent from Solr raises 404."""
    # ``institute`` is in the hierarchy but not in the fake solr_fields.
    with pytest.raises(Exception) as exc:
        make_api(collection_axis="institute", axis_in_path=True)
    assert getattr(exc.value, "status_code", None) == 404


def test_unit_map_collection_field(make_api) -> None:
    """``collection`` -> active axis, ``id`` -> uniq_key, others unchanged."""
    api = make_api(collection_axis="product", axis_in_path=True)
    assert api._map_collection_field("collection") == "product"
    assert api._map_collection_field("id") == api.uniq_key
    assert api._map_collection_field("variable") == "variable"


def test_unit_apply_visibility(make_api) -> None:
    """Visibility filter keeps only the allowed collection ids."""
    api = make_api(
        collection_axis="project", visible_collections=["cmip6"]
    )
    assert api._apply_visibility(["cmip6", "cordex", "nextgems"]) == ["cmip6"]
    # No filter -> list passes through unchanged.
    api2 = make_api(collection_axis="project")
    assert api2._apply_visibility(["a", "b"]) == ["a", "b"]


def test_unit_collection_fq_escaping(make_api) -> None:
    """Collection filter values are quoted and Solr-escaped on the active
    axis."""
    api = make_api(collection_axis="product", axis_in_path=True)
    fq = api._collection_fq("foo:bar")
    assert fq.startswith('product:"')
    # the colon inside the value must be escaped
    assert "\\:" in fq


@pytest.mark.asyncio
async def test_unit_assert_collection_visible(make_api) -> None:
    """The direct-access guard 404s collections that are absent from the
    enumerated (visibility-filtered) set, and passes visible ones."""

    async def _fake_facets():
        return ["cmip6"]

    api = make_api(collection_axis="project", visible_collections=["cmip6"])
    api.get_all_collection_facets = _fake_facets
    # visible/existing; no raise
    await api._assert_collection_visible("cmip6")
    # hidden or non-existent; 404
    with pytest.raises(Exception) as exc:
        await api._assert_collection_visible("cordex")
    assert getattr(exc.value, "status_code", None) == 404


@pytest.mark.parametrize(
    "expr,expected",
    [
        ({"op": "=", "args": [{"property": "collection"}, "tas"]},
         ['product:"tas"']),
        ({"op": "!=", "args": [{"property": "collection"}, "tas"]},
         ['-product:"tas"']),
        ({"op": "<", "args": [{"property": "collection"}, "x"]},
         ["product:{* TO x}"]),
        ({"op": "<=", "args": [{"property": "collection"}, "x"]},
         ["product:[* TO x]"]),
        ({"op": ">", "args": [{"property": "collection"}, "x"]},
         ["product:{x TO *}"]),
        ({"op": ">=", "args": [{"property": "collection"}, "x"]},
         ["product:[x TO *]"]),
        ({"op": "isNull", "args": [{"property": "collection"}]},
         ["-product:[* TO *]"]),
        ({"op": "=", "args": [{"property": "id"}, "abc"]},
         ['file:"abc"']),
        ({"op": "=", "args": [{"property": "variable"}, "tas"]},
         ['variable:"tas"']),
    ],
)
def test_unit_cql2_collection_mapping(make_api, expr, expected) -> None:
    """Every CQL2 operator branch maps ``collection`` to the active axis."""
    api = make_api(collection_axis="product", axis_in_path=True)
    assert api._parse_cql2_filter(expr) == expected


def test_unit_parse_visible_forms() -> None:
    """visible_collections accepts comma-separated, repeated, and mixed
    forms, and normalises empties to None."""
    from freva_rest.stac_api.schema import parse_visible as pv

    # comma-separated single param
    assert pv(["cmip6,cordex"]) == ["cmip6", "cordex"]
    # repeated param
    assert pv(["cmip6", "cordex"]) == ["cmip6", "cordex"]
    # mix of both
    assert pv(["cmip6,cordex", "nextgems"]) == ["cmip6", "cordex", "nextgems"]
    # whitespace trimmed
    assert pv([" cmip6 , cordex "]) == ["cmip6", "cordex"]
    # absent / empty -> None
    assert pv(None) is None
    assert pv([""]) is None


@pytest.mark.asyncio
async def test_unit_collection_metadata_fallback(make_api) -> None:
    """get_collection falls back to generated defaults when no collection
    metadata is stored."""

    async def _facets():
        return ["cmip6"]

    async def _no_meta(_cid):
        return {}

    async def _noop():
        return None

    api = make_api(collection_axis="project", axis_in_path=True)
    api.get_all_collection_facets = _facets
    api._get_collection_metadata = _no_meta
    api._set_solr_query = _noop

    col = await api.get_collection("cmip6")
    assert col.title == "CMIP6"
    assert col.description == "Collection CMIP6"
    assert col.license == "proprietary"
    assert col.extent.spatial["bbox"] == [[-180, -90, 180, 90]]
    assert col.extent.temporal["interval"] == [[None, None]]
    assert col.assets is not None
    assert "thumbnail" not in col.assets
    assert col.assets["freva-databrowser"]["href"].endswith(
        "/databrowser/?project=cmip6"
    )


def test_unit_tagged_values(make_api) -> None:
    """_tagged_values returns only values for the active axis, ignores
    unknown/untagged entries, and splits on the first '|' only."""
    api = make_api(collection_axis="project", axis_in_path=True)
    raw = [
        "project|CMIP6 Global Climate",
        "product|Model Output",
        # value may contain pipes
        "project|extra | with | pipes",
        # unknown tag; ignored
        "bogus|ignored",
        # no separator; ignored
        "no-tag-here",
    ]
    assert api._tagged_values(raw) == [
        "CMIP6 Global Climate",
        "extra | with | pipes",
    ]
    assert api._tagged_single(raw) == "CMIP6 Global Climate"
    # different axis selects different entries
    api2 = make_api(collection_axis="product", axis_in_path=True)
    assert api2._tagged_single(raw) == "Model Output"


# Shared multi-axis tagged metadata
_TAGGED_META = {
    "stac_collection_title": [
        "project|CMIP6 Global Climate",
        "product|Model Output",
    ],
    "stac_collection_description": [
        "project|The CMIP6 ensemble",
        "product|Raw output across projects",
    ],
    "stac_collection_license": ["project|CC-BY-4.0", "product|various"],
    "stac_collection_license_url": [
        "project|https://creativecommons.org/licenses/by/4.0/",
    ],
    "stac_collection_keywords": [
        "project|cmip6",
        "project|climate",
        "product|output",
    ],
    "stac_collection_thumbnail_url": ["project|https://example/cmip6.png"],
    "stac_collection_thumbnail_type": ["project|image/png"],
    "stac_collection_documentation_url": ["product|https://docs.example/output"],
    "stac_collection_bbox": [
        "project|-180,-90,180,90",
        "product|-10,35,40,70",
    ],
    "stac_collection_time_start": [
        "project|1850-01-01T00:00:00Z",
        "product|1900-01-01T00:00:00Z",
    ],
    "stac_collection_time_end": [
        "project|2014-12-31T00:00:00Z",
        "product|2100-12-31T00:00:00Z",
    ],
}


@pytest.mark.asyncio
async def test_unit_collection_metadata_project_axis(make_api) -> None:
    """Under axis=project, the project-tagged entries are used."""

    async def _facets():
        return ["cmip6"]

    async def _meta(_cid):
        return _TAGGED_META

    async def _noop():
        return None

    api = make_api(collection_axis="project", axis_in_path=True)
    api.get_all_collection_facets = _facets
    api._get_collection_metadata = _meta
    api._set_solr_query = _noop

    col = await api.get_collection("cmip6")
    assert col.title == "CMIP6 Global Climate"
    assert col.license == "CC-BY-4.0"
    assert col.keywords == ["cmip6", "climate"]
    assert col.extent.spatial["bbox"] == [[-180.0, -90.0, 180.0, 90.0]]
    assert col.assets and col.assets["thumbnail"]["href"] == (
        "https://example/cmip6.png"
    )
    # documentation is product-tagged only
    assert not [link for link in col.links if link.rel == "describedby"]


@pytest.mark.asyncio
async def test_unit_collection_metadata_product_axis(make_api) -> None:
    """The SAME doc under axis=product yields the product-tagged entries."""

    async def _facets():
        return ["output"]

    async def _meta(_cid):
        return _TAGGED_META

    async def _noop():
        return None

    api = make_api(collection_axis="product", axis_in_path=True)
    api.get_all_collection_facets = _facets
    api._get_collection_metadata = _meta
    api._set_solr_query = _noop

    col = await api.get_collection("output")
    assert col.title == "Model Output"
    assert col.license == "various"
    assert col.keywords == ["output"]
    assert col.extent.spatial["bbox"] == [[-10.0, 35.0, 40.0, 70.0]]
    doc_links = [link for link in col.links if link.rel == "describedby"]
    assert doc_links and doc_links[0].href == "https://docs.example/output"


@pytest.mark.asyncio
async def test_unit_collection_metadata_axis_mismatch_fallback(
    make_api,
) -> None:
    """When the active axis has NO tagged entries, fall back to generated
    defaults rather than leaking another axis's metadata."""

    async def _facets():
        return ["output"]

    async def _meta(_cid):
        # only project tags present; active axis is product
        return {
            "stac_collection_title": ["project|CMIP6 Global Climate"],
            "stac_collection_bbox": ["project|-180,-90,180,90"],
        }

    async def _noop():
        return None

    api = make_api(collection_axis="product", axis_in_path=True)
    api.get_all_collection_facets = _facets
    api._get_collection_metadata = _meta
    api._set_solr_query = _noop

    col = await api.get_collection("output")
    assert col.title == "OUTPUT"  # generated, NOT "CMIP6 Global Climate"
    assert col.extent.spatial["bbox"] == [[-180, -90, 180, 90]]
    assert col.assets is not None
    assert "thumbnail" not in col.assets


def test_unit_visibility_glob(make_api) -> None:
    """visible_collections entries are glob patterns: prefix globs expand,
    literals match exactly, results de-duplicate."""
    all_ids = ["cmip6", "cmip6_amon", "cmip6_omon", "cordex", "nextgems"]
    api = make_api(collection_axis="project", visible_collections=["cmip6*"])
    assert api._apply_visibility(all_ids) == [
        "cmip6",
        "cmip6_amon",
        "cmip6_omon",
    ]
    # literal still matches exactly itself
    api = make_api(collection_axis="project", visible_collections=["cmip6"])
    assert api._apply_visibility(all_ids) == ["cmip6"]
    # mixed glob + literal, de-duplicated
    api = make_api(
        collection_axis="project",
        visible_collections=["cmip6*", "cmip6_amon", "cordex"],
    )
    assert api._apply_visibility(all_ids) == [
        "cmip6",
        "cmip6_amon",
        "cmip6_omon",
        "cordex",
    ]


def test_unit_visibility_glob_no_match_400(make_api) -> None:
    """A pattern matching no collection raises 400 naming that pattern."""
    all_ids = ["cmip6", "cordex"]
    api = make_api(collection_axis="project", visible_collections=["typo*"])
    with pytest.raises(Exception) as exc:
        api._apply_visibility(all_ids)
    assert getattr(exc.value, "status_code", None) == 400
    assert "typo*" in getattr(exc.value, "detail", "")
    api = make_api(
        collection_axis="project", visible_collections=["cmip6*", "typo*"]
    )
    with pytest.raises(Exception) as exc:
        api._apply_visibility(all_ids)
    assert getattr(exc.value, "status_code", None) == 400

def test_unit_visibility_expansion_cap(make_api) -> None:
    """A pattern expanding past MAX_VISIBLE_COLLECTIONS_EXPANSION is rejected
    with 400 naming the limit, rather than building an oversized Solr filter."""
    api = make_api(collection_axis="project", visible_collections=["c*"])
    cap = api.MAX_VISIBLE_COLLECTIONS_EXPANSION
    # one more id than the cap allows, all matching the glob
    all_ids = [f"c{i}" for i in range(cap + 1)]
    with pytest.raises(Exception) as exc:
        api._apply_visibility(all_ids)
    assert getattr(exc.value, "status_code", None) == 400
    assert "too many collections" in getattr(exc.value, "detail", "")
    # exactly at the cap is still allowed
    api2 = make_api(collection_axis="project", visible_collections=["c*"])
    at_cap = [f"c{i}" for i in range(cap)]
    assert api2._apply_visibility(at_cap) == at_cap


@pytest.mark.asyncio
async def test_unit_search_out_of_view_400(make_api) -> None:
    """prepare_search rejects an explicit collection outside the visible view
    with 400 naming the offending collection(s)."""

    async def _facets():
        return ["cmip6"]

    api = make_api(collection_axis="project", visible_collections=["cmip6"])
    api.get_all_collection_facets = _facets

    with pytest.raises(Exception) as exc:
        await api.prepare_search(collections="cordex")
    assert getattr(exc.value, "status_code", None) == 400
    detail = getattr(exc.value, "detail", "")
    assert "not visible in this view" in detail
    assert "cordex" in detail
    # a collection inside the view does not raise
    await api.prepare_search(collections="cmip6")


@pytest.mark.asyncio
async def test_unit_search_cql2_json_vs_unmappable(make_api) -> None:
    """prepare_search returns 400 for malformed CQL2 JSON but ignores valid
    JSON that does not map to a predicate (lenient CQL2 contract)."""

    async def _no_visible():
        return None

    api = make_api(collection_axis="project")
    api._resolved_visible = _no_visible

    # malformed JSON; 400
    with pytest.raises(Exception) as exc:
        await api.prepare_search(filter="{not valid json")
    assert getattr(exc.value, "status_code", None) == 400
    assert "Invalid CQL2 JSON" in getattr(exc.value, "detail", "")

    # valid JSON but unmappable filters -> no raise (search proceeds)
    for unmappable in (
        "{}",
        '{"op": "invalid_op", "args": []}',
        '{"op": "=", "args": [{"property": "project"}]}',
        '{"op": "s_intersects", "args": [{"property": "geometry"},'
        ' {"type": "Polygon", "coordinates": []}]}',
    ):
        await api.prepare_search(filter=unmappable)


def test_unit_cql2_empty_coordinates_no_filter(make_api) -> None:
    """_parse_cql2_filter yields no filter (rather than raising) for an
    s_intersects polygon with empty coordinates."""
    api = make_api(collection_axis="project")
    geom_empty = {
        "op": "s_intersects",
        "args": [
            {"property": "geometry"},
            {"type": "Polygon", "coordinates": []},
        ],
    }
    geom_short = {
        "op": "s_intersects",
        "args": [
            {"property": "geometry"},
            {"type": "Polygon", "coordinates": [[[10, 20]]]},
        ],
    }
    assert api._parse_cql2_filter(geom_empty) == []
    assert api._parse_cql2_filter(geom_short) == []


@pytest.mark.asyncio
async def test_unit_collection_bbox_malformed_falls_back(make_api) -> None:
    """A malformed collection_bbox falls back to the global extent instead
    of raising."""
    async def _facets():
        return ["cmip6"]
    async def _meta(_cid):
        return {"stac_collection_bbox": ["project|not,a,valid,bbox"]}
    async def _noop():
        return None

    api = make_api(collection_axis="project", axis_in_path=True)
    api.get_all_collection_facets = _facets
    api._get_collection_metadata = _meta
    api._set_solr_query = _noop

    col = await api.get_collection("cmip6")
    assert col.extent.spatial["bbox"] == [[-180.0, -90.0, 180.0, 90.0]]


async def _drain(gen):
    return [c async for c in gen]


@pytest.mark.asyncio
async def test_get_search_out_of_view_400(make_api) -> None:
    """get_search itself rejects out-of-view collections (defense in depth,
    independent of the endpoint-level prepare_search check)."""
    async def _facets():
        return ["cmip6"]

    api = make_api(collection_axis="project", visible_collections=["cmip6"])
    api.get_all_collection_facets = _facets

    with pytest.raises(Exception) as exc:
        await _drain(api.get_search(collections="cordex"))
    assert getattr(exc.value, "status_code", None) == 400
    assert "not visible in this view" in getattr(exc.value, "detail", "")


@pytest.mark.asyncio
async def test_get_search_bad_json_400(make_api) -> None:
    """get_search returns 400 on malformed CQL2 JSON."""
    async def _nov():
        return None

    api = make_api(collection_axis="project")
    api._resolved_visible = _nov

    with pytest.raises(Exception) as exc:
        await _drain(api.get_search(filter="{not valid json"))
    assert getattr(exc.value, "status_code", None) == 400
    assert "Invalid CQL2 JSON" in getattr(exc.value, "detail", "")


@pytest.mark.asyncio
async def test_get_search_parse_exception_400(make_api) -> None:
    """An unexpected error from _parse_cql2_filter is converted to a 400
    'Invalid CQL2 filter' rather than surfacing as a 500."""
    async def _nov():
        return None

    api = make_api(collection_axis="project")
    api._resolved_visible = _nov

    def _boom(_obj):
        raise RuntimeError("boom")

    api._parse_cql2_filter = _boom

    with pytest.raises(Exception) as exc:
        await _drain(api.get_search(filter='{"op": "=", "args": []}'))
    assert getattr(exc.value, "status_code", None) == 400
    assert "Invalid CQL2 filter" in getattr(exc.value, "detail", "")

def test_unit_href_propagates_scope(make_api) -> None:
    """Navigational links carry visible_collections; non-navigational don't."""
    api = make_api(
        collection_axis="product",
        axis_in_path=True,
        visible_collections=["cmip6", "dyamond"],
    )
    # navigational links keep the scope
    assert "visible_collections=cmip6%2Cdyamond" in api._href("/collections")
    assert "visible_collections=cmip6%2Cdyamond" in api._href(
        "/collections/cmip6"
    )
    # non-navigational links stay bare
    assert "visible_collections" not in api._href(
        "/conformance", navigational=False
    )
    # no visibility -> no param
    api2 = make_api(collection_axis="product", axis_in_path=True)
    assert "?" not in api2._href("/collections")

def test_unit_href_propagates_scope(make_api) -> None:
    """Navigational links carry visible_collections; non-navigational don't."""
    api = make_api(
        collection_axis="product",
        axis_in_path=True,
        visible_collections=["cmip6", "dyamond"],
    )
    # navigational links keep the scope
    assert "visible_collections=cmip6%2Cdyamond" in api._href("/collections")
    assert "visible_collections=cmip6%2Cdyamond" in api._href(
        "/collections/cmip6"
    )
    # non-navigational links stay bare
    assert "visible_collections" not in api._href(
        "/conformance", navigational=False
    )
    # no visibility -> no param
    api2 = make_api(collection_axis="product", axis_in_path=True)
    assert "?" not in api2._href("/collections")
