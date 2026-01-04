"""Unit tests for data queries via the rest-api."""

import json
import os
import time
from typing import Dict

import mock
import requests
from pymongo import MongoClient

from freva_rest.config import ServerConfig


def test_attributes(test_server: str) -> None:
    """Test getting the attributes."""
    res1 = requests.get(f"{test_server}/databrowser/overview")
    assert isinstance(res1.json()["flavours"], list)
    assert isinstance(res1.json()["attributes"], dict)


def test_databrowser(test_server: str) -> None:
    """Test the default databrowser functionality."""
    res1 = requests.get(
        f"{test_server}/databrowser/data-search/cmip6/uri",
        params={"activity_id": "cmipx"},
    )
    assert res1.status_code == 200
    res2 = requests.get(
        f"{test_server}/databrowser/data-search/cmip6/uri",
        params={"translate": "false", "product": "cmip"},
    )
    res3 = requests.get(
        f"{test_server}/databrowser/data-search/freva/uri",
        params={"product": "cmip"},
    )
    assert len(res1.text.split()) == 0
    assert res2.text == res3.text
    res4 = requests.get(
        f"{test_server}/databrowser/data-search/cmip6/uri",
        params={"foo": "bar"},
    )
    assert res4.status_code == 422
    res5 = requests.get(
        f"{test_server}/databrowser/data-search/cmip6/uri",
        params={
            "translate": "false",
            "product": "cmip",
            "multi-version": "true",
        },
    )
    assert len(res2.text.split()) < len(res5.text.split())

    # wrong flavour
    res6 = requests.get(f"{test_server}/databrowser/data-search/cmipx/file")
    assert res6.status_code == 422


def test_multiversion(test_server: str) -> None:
    """Test the behaviour for multi versions."""
    res1 = requests.get(
        f"{test_server}/databrowser/metadata-search/freva/file",
        params={"multi-version": True},
    )
    assert res1.status_code == 200
    assert "facets" in res1.json()
    assert "version" in res1.json()["facets"]
    version = res1.json()["facets"]["version"][0]

    res2 = requests.get(
        f"{test_server}/databrowser/metadata-search/freva/file",
        params={"multi-version": False},
    )
    assert res2.status_code == 200
    assert "facets" in res2.json()
    assert "version" not in res2.json()["facets"]

    res3 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"multi-version": True},
    )
    assert res3.status_code == 200
    res4 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"multi-version": False},
    )
    assert res4.status_code == 200
    assert len(list(res4.text.split())) < len(list(res3.text.split()))

    res5 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"multi-version": True, "version": version},
    )
    assert res5.status_code == 200

    res6 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"multi-version": False, "version": version},
    )
    assert res6.status_code != 200


def test_no_solr(test_server: str) -> None:
    """Test what happens if there is no connection to the solr."""
    with mock.patch("freva_rest.rest.server_config.solr_host", "foo.bar"):
        res = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={"activity_id": "cmipx"},
        )
        assert res.status_code == 503


def test_file_select(test_server: str) -> None:
    """Test what happens if we search for files/uris."""
    res = requests.get(
        f"{test_server}/databrowser/data-search/cmip6/file",
        params={"file": "/arch/bb1203/*/CPC/*"},
    )
    assert res.status_code == 200
    assert len(res.text.split()) > 0
    res = requests.get(
        f"{test_server}/databrowser/metadata-search/cmip6/file",
        params={"uri": "slk:///arch/bb1203/*/CPC/*"},
    )
    assert res.status_code == 200
    data = res.json()
    assert len(data) > 0
    assert "cmorph" in data["facets"]["experiment_id"]


def test_time_selection(test_server: str) -> None:
    """Test the time select functionality of the API."""
    res1 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"time": "1898 to 1901"},
    )
    assert len(res1.text.split()) == 1
    res2 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"time": "1898 to 1901", "time_select": "foo"},
    )
    assert res2.status_code == 500
    res3 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"time": "fx"},
    )
    assert res3.status_code == 500


def test_bbox_selection(test_server: str) -> None:
    """Test the bbox select functionality of the API."""
    res1 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"bbox": "-10,10,-10,10"},
    )
    assert len(res1.text.split()) == 61
    res2 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"bbox": "-10,10,-10,10", "bbox_select": "foo"},
    )
    assert res2.status_code == 500
    res3 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"bbox": "fx"},
    )
    assert res3.status_code == 500

    res3 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"bbox": "-181,181,-10,10"},
    )
    assert res3.status_code == 500

    res4 = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"bbox": "-10,10,-91,91"},
    )
    assert res4.status_code == 500


def test_primary_facets(test_server: str) -> None:
    """Test the functionality of primary facet definitions."""
    res1 = requests.get(
        f"{test_server}/databrowser/metadata-search/freva/file"
    ).json()
    res2 = requests.get(
        f"{test_server}/databrowser/metadata-search/cmip6/file"
    ).json()
    res3 = requests.get(
        f"{test_server}/databrowser/metadata-search/cmip6/file",
        params={"translate": "f"},
    ).json()
    assert "primary_facets" in res1
    assert "primary_facets" in res2
    assert "primary_facets" in res3
    assert (
        len(res1["primary_facets"])
        == len(res2["primary_facets"])
        == len(res3["primary_facets"])
    )
    assert res1["primary_facets"] == res3["primary_facets"]
    assert res1["primary_facets"] != res2["primary_facets"]


def test_extended_search(test_server: str, auth: Dict[str, str]) -> None:
    """Test the facet search functionality."""
    res1 = requests.get(
        f"{test_server}/databrowser/extended-search/cmip6/uri",
        params={"start": 0, "activity_id": "cmip", "max-results": 2},
    ).json()
    assert len(res1["search_results"]) > 0
    assert "activity_id" in res1["facets"]
    res2 = requests.get(
        f"{test_server}/databrowser/extended-search/cmip6/uri",
        params={"start": 1000, "activity_id": "cmip", "max-results": 2},
    ).json()
    assert "rcm_name" not in res2["primary_facets"]
    assert res2["search_results"] == []
    res3 = requests.get(
        f"{test_server}/databrowser/extended-search/cmip5/uri",
        params={"activity_id": "cmip", "translate": "false", "max-results": 2},
    )
    assert res3.status_code == 422
    res4 = requests.get(
        f"{test_server}/databrowser/extended-search/cmip6/uri",
        params={"activity_id": "cmipx", "translate": "true", "max-results": 2},
    )
    assert res4.status_code == 200
    res5 = requests.get(
        f"{test_server}/databrowser/extended-search/cordex/uri",
        params={"domain": "eur-11", "translate": "true"},
    ).json()
    assert "rcm_name" in res5["facets"]
    assert "rcm_name" in res5["primary_facets"]

    res6 = requests.get(
        f"{test_server}/databrowser/extended-search/cmip6/uri",
        params={"facets": "activity_id"},
    ).json()
    assert len(res6["facets"].keys()) == 1
    res7 = requests.get(
        f"{test_server}/databrowser/extended-search/cmip6/uri",
        params={"facets": "activity_id", "max-results": 0},
    ).json()
    assert len(res7["search_results"]) == 0
    # test the zarr stream pagination functionality
    res8 = requests.get(
        f"{test_server}/databrowser/extended-search/cmip6/uri",
        params={"facets": "activity_id", "max-results": 1, "zarr_stream": True},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    ).json()
    assert len(res8["search_results"]) == 1

    res9 = requests.get(
        f"{test_server}/databrowser/extended-search/cmip6/uri",
        params={"facets": "activity_id", "max-results": 1, "zarr_stream": True},
    )
    assert res9.status_code == 401
    with mock.patch("freva_rest.utils.base_utils.Cache", "foo"):
        res10 = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            params={
                "facets": "activity_id",
                "max-results": 1,
                "zarr_stream": True,
            },
            headers={"Authorization": f"Bearer {auth['access_token']}"},
        ).json()
        assert (
            res10["search_results"][0]["uri"]
            == "Internal error, service not able to publish"
        )

    with mock.patch("freva_rest.rest.server_config.services", "databrowser"):
        res11 = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            params={
                "facets": "activity_id",
                "max-results": 1,
                "zarr_stream": True,
            },
        )
        # get the normal response
        assert res11.status_code == 200


def test_metadata_search(test_server: str) -> None:
    """Test the facet search functionality."""
    res1 = requests.get(
        f"{test_server}/databrowser/metadata-search/cmip6/uri",
        params={"activity_id": "cmip"},
    ).json()
    assert "search_results" not in res1.keys()
    assert "activity_id" in res1["facets"]
    res3 = requests.get(
        f"{test_server}/databrowser/extended-search/cmip5/uri",
        params={"activity_id": "cmip", "translate": "false"},
    )
    assert res3.status_code == 422
    res4 = requests.get(
        f"{test_server}/databrowser/metadata-search/cmip6/uri",
        params={"activity_id": "cmipx", "translate": "true"},
    )
    assert res4.status_code == 200
    res5 = requests.get(
        f"{test_server}/databrowser/metadata-search/cordex/uri",
        params={"domain": "eur-11", "translate": "true"},
    ).json()
    assert "rcm_name" in res5["facets"]
    assert "rcm_name" in res5["primary_facets"]

    res6 = requests.get(
        f"{test_server}/databrowser/metadata-search/cmip6/uri",
        params={"facets": "activity_id"},
    ).json()
    assert len(res6["facets"].keys()) == 1


def test_contains_not(test_server: str) -> None:
    """Test for searches that should *not* contain values."""

    res1 = requests.get(
        f"{test_server}/databrowser/metadata-search/freva/file",
        params={
            "dataset": ["-cmip6-swift", "not cmip6-fs"],
            "project": "cmip6",
        },
    ).json()
    assert "cmip6-swift" not in res1["facets"]["dataset"]
    assert "cmip6-fs" not in res1["facets"]["dataset"]
    assert "cmip6-hsm" in res1["facets"]["dataset"]
    res2 = requests.get(
        f"{test_server}/databrowser/metadata-search/freva/file",
        params={
            "project_not_": "cmip6",
        },
    ).json()
    assert "cmip6" not in res2["facets"]["project"]


def test_intake_search(test_server: str) -> None:
    """Test the creation of intake catalogues."""
    res1 = requests.get(
        f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
        params={"activity_id": "cmip", "multi-version": True},
    )
    assert res1.json() == json.loads(res1.text)
    res2 = requests.get(
        f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
    )
    assert len(res2.json()["catalog_dict"]) > len(res1.json()["catalog_dict"])
    res3 = requests.get(
        f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
        params={
            "activity_id": "cmip",
            "multi-version": False,
        },
    )
    assert len(res1.json()["catalog_dict"]) > len(res3.json()["catalog_dict"])
    res4 = requests.get(
        f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
        params={
            "multi-version": False,
            "max-results": 1,
        },
    )
    assert res4.status_code == 413


def test_stac_catalogue(test_server: str) -> None:
    """Test the creation of STAC Catalogue."""

    # 200 OK from STAC static endpoint
    res = requests.get(
        f"{test_server}/databrowser/stac-catalogue/cmip6/uri",
        params={"activity_id": "cmip", "multi-version": True, "max_results": 2},
        allow_redirects=False,
    )
    assert res.status_code == 200

    # 413 Request Entity Too Large
    res3 = requests.get(
        f"{test_server}/databrowser/stac-catalogue/cmip6/uri",
        params={"activity_id": "cmip", "multi-version": False, "max-results": 1},
    )
    assert res3.status_code == 413
    # 422 Unprocessable Entity
    res4 = requests.get(
        f"{test_server}/databrowser/stac-catalogue/cmip6/uri",
        params={"collection": "cmip2", "multi-version": False},
    )
    assert res4.status_code == 422
    # 404 Not Found
    res5 = requests.get(
        f"{test_server}/databrowser/stac-catalogue/cmip6/uri",
        params={"activity_id": "cmip3", "multi-version": False},
    )
    assert res5.status_code == 404


def test_bad_intake_request(test_server: str) -> None:
    """Test for a wrong intake request."""
    res1 = requests.get(
        f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
        params={"activity_id": "cmip2"},
    )
    assert res1.status_code == 404


def test_parameter_validation(test_server: str) -> None:
    """Test if only valid parameter requests make it."""

    res1 = requests.get(
        f"{test_server}/databrowser/data-search/cmip6/uri",
        params={"activity_id": "cmip", "translate": "false"},
    ).status_code
    res2 = requests.get(
        f"{test_server}/databrowser/data-search/cmip6/uri",
        params={"product": "cmip", "translate": "true"},
    ).status_code
    res3 = requests.get(
        f"{test_server}/databrowser/data-search/cmip6/uri",
        params={"activity_": "cmip"},
    ).status_code
    assert res1 == res2 == res3 == 422


def test_no_mongo_parameter_insert(test_server: str) -> None:
    """Test the insertion of data into the mongodb."""
    from freva_rest.rest import server_config

    with MongoClient(server_config.mongo_url) as mongo_client:
        collection = mongo_client[server_config.mongo_db]["search_queries"]
        collection.drop()
        assert collection.find_one() is None
    with mock.patch("freva_rest.rest.server_config.mongo_password", "foo"):
        server_config.power_cycle_mongodb()
        res1 = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={"activity_id": "cmip"},
        ).status_code
        assert res1 == 200
    server_config.power_cycle_mongodb()
    with MongoClient(server_config.mongo_url) as mongo_client:
        collection = mongo_client[server_config.mongo_db]["search_queries"]
        assert collection.find_one() is None


def test_zarr_stream_not_implemented(
    test_server: str, auth: Dict[str, str]
) -> None:
    """Test if zarr request is not served when told not to do so."""
    with mock.patch("freva_rest.rest.server_config.services", ""):
        res = requests.get(
            f"{test_server}/databrowser/load/freva",
            headers={"Authorization": f"Bearer {auth['access_token']}"},
        )
        assert res.status_code == 503


def test_mongo_parameter_insert(test_server: str, cfg: ServerConfig) -> None:
    """Test the successfull insertion of the search stats."""
    res1 = requests.get(
        f"{test_server}/databrowser/data-search/cordex/uri",
        params={"variable": ["wind", "cape"]},
    )
    assert res1.status_code == 200
    time.sleep(2)
    mongo_client = MongoClient(cfg.mongo_url)
    collection = mongo_client[cfg.mongo_db]["search_queries"]
    assert len(res1.text.split()) > 0
    stats = list(collection.find())
    assert len(stats) > 0
    assert isinstance(stats[0], dict)
    assert "metadata" in stats[0]
    assert "query" in stats[0]
    assert isinstance(stats[0]["query"], dict)
    assert isinstance(stats[0]["metadata"], dict)
    # The search keys should have been converted to strings.
    assert (
        len([k for k in stats[0]["query"].values() if not isinstance(k, str)])
        == 0
    )


def test_flavours_endpoints(flavour_server: str, auth: Dict[str, str]) -> None:
    """Test all flavour endpoints: list, add, update, delete."""
    auth_admin = auth["admin"]
    # ========== GET METHOD TESTS ==========

    # GET: listing flavours without authentication
    res1 = requests.get(f"{flavour_server}/databrowser/flavours")
    assert res1.status_code == 200
    flavours_data = res1.json()
    assert "total" in flavours_data
    assert "flavours" in flavours_data
    assert flavours_data["total"] >= 5
    built_in_names = [f["flavour_name"] for f in flavours_data["flavours"]]
    assert "freva" in built_in_names
    assert "cmip6" in built_in_names

    # GET: listing flavours with invalid query parameter returns 422
    res8 = requests.get(
        f"{flavour_server}/databrowser/flavours", params={"invalid_param": "test"}
    )
    assert res8.status_code == 422

    # ========== POST METHOD TESTS ==========

    # POST: adding flavour without authentication returns 401
    custom_flavour = {
        "flavour_name": "test_flavour",
        "mapping": {"project": "my_project", "variable": "my_variable"},
        "is_global": False,
    }
    res_post_1 = requests.post(
        f"{flavour_server}/databrowser/flavours", json=custom_flavour
    )
    assert res_post_1.status_code == 401

    # POST: adding a custom personal flavour
    res_psot_2 = requests.post(
        f"{flavour_server}/databrowser/flavours",
        json=custom_flavour,
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_psot_2.status_code == 201
    assert "status" in res_psot_2.json()

    # POST: adding a double custom personal flavour with different name
    # for other usage in later tests
    custom_flavour_double = {
        "flavour_name": "test_flavour_double",
        "mapping": {"project": "my_project", "variable": "my_variable"},
        "is_global": False,
    }
    res_psot_3 = requests.post(
        f"{flavour_server}/databrowser/flavours",
        json=custom_flavour_double,
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_psot_3.status_code == 201
    assert "status" in res_psot_3.json()

    # POST: getting 409 for duplicate flavour
    res_post_2_duplicate = requests.post(
        f"{flavour_server}/databrowser/flavours",
        json=custom_flavour,
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_post_2_duplicate.status_code == 409

    # POST: adding flavour with restricted characters returns 422
    flavour_with_restriction_char = {
        "flavour_name": "test:flav></*'our",
        "mapping": {"project": "my_project", "variable": "my_variable"},
        "is_global": False,
    }
    res_post_4 = requests.post(
        f"{flavour_server}/databrowser/flavours",
        json=flavour_with_restriction_char,
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_post_4.status_code == 422

    # POST: non-admin user cannot add global flavour (403)
    conflict_map = {
        "flavour_name": "freva",
        "mapping": {"project": "my_project", "variable": "my_variable"},
        "is_global": True,
    }
    res_post_5 = requests.post(
        f"{flavour_server}/databrowser/flavours",
        json=conflict_map,
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_post_5.status_code == 403

    # POST: admin user can add global flavour
    with mock.patch(
        "freva_rest.rest.server_config.admins_token_claims",
        {"resource_access.realm-management.roles": ["admin"]},
    ):
        res_post_6 = requests.post(
            f"{flavour_server}/databrowser/flavours",
            json={
                "flavour_name": "testx",
                "is_global": True,
                "mapping": {"project": "my_project", "variable": "my_variable"},
            },
            headers={"Authorization": f"Bearer {auth_admin['access_token']}"},
        )
        assert res_post_6.status_code == 201

        # Test: cannot add global flavour with same name as existing (409)
        res_post_7 = requests.post(
            f"{flavour_server}/databrowser/flavours",
            json=conflict_map,
            headers={"Authorization": f"Bearer {auth_admin['access_token']}"},
        )
        assert res_post_7.status_code == 409

    # POST: listing flavours with authentication shows custom flavours
    res_post_8 = requests.get(
        f"{flavour_server}/databrowser/flavours",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_post_8.status_code == 200
    flavours_data = res_post_8.json()
    assert flavours_data["total"] > res1.json()["total"]
    custom_names = [f["flavour_name"] for f in flavours_data["flavours"]]
    assert "test_flavour" in custom_names

    # ========== PUT METHOD TESTS ==========

    # PUT: updating flavour without authentication returns 401
    res_put_1 = requests.put(
        f"{flavour_server}/databrowser/flavours/test_flavour",
        json={
            "flavour_name": "test_flavour",
            "mapping": {"model": "updated_model"},
            "is_global": False,
        },
    )
    assert res_put_1.status_code == 401

    # PUT: updating personal flavour successfully (partial update)
    res_put_2 = requests.put(
        f"{flavour_server}/databrowser/flavours/test_flavour",
        json={
            "flavour_name": "test_flavour",
            "mapping": {
                "model": "updated_model",
                "experiment": "updated_experiment",
            },
            "is_global": False,
        },
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_put_2.status_code == 200
    assert "status" in res_put_2.json()

    # PUT: verify flavour was updated with new keys
    res_put_3 = requests.get(
        f"{flavour_server}/databrowser/flavours",
        params={"flavour_name": "test_flavour"},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_put_3.status_code == 200
    updated_flavour = next(
        f
        for f in res_put_3.json()["flavours"]
        if f["flavour_name"] == "test_flavour"
    )
    assert updated_flavour["mapping"]["model"] == "updated_model"
    assert updated_flavour["mapping"]["experiment"] == "updated_experiment"
    assert (
        updated_flavour["mapping"]["project"] == "my_project"
    )  # Original key preserved

    # PUT: Update the flavour with non-valid flavour name returns 422
    res_put_5 = requests.put(
        f"{flavour_server}/databrowser/flavours/test_flavour",
        json={
            "flavour_name": "invalid:flav></*'our",
            "mapping": {"model": "some_model"},
            "is_global": False,
        },
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_put_5.status_code == 422

    # PUT: updating an already up-to-date flavour
    res_put_6 = requests.put(
        f"{flavour_server}/databrowser/flavours/test_flavour",
        json={
            "flavour_name": "test_flavour",
            "mapping": {
                "model": "updated_model",
                "experiment": "updated_experiment",
                "project": "my_project",
            },
            "is_global": False,
        },
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_put_6.status_code == 200

    # PUT: updating flavour with name change but the flavour exists already returns 409
    res_put_7 = requests.put(
        f"{flavour_server}/databrowser/flavours/test_flavour",
        json={
            "flavour_name": "test_flavour_double",
            "mapping": {"model": "some_model"},
            "is_global": False,
        },
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_put_7.status_code == 409

    # PUT: updating non-existent flavour returns 404
    res_put_8 = requests.put(
        f"{flavour_server}/databrowser/flavours/non_existent",
        json={
            "flavour_name": "non_existent",
            "mapping": {"model": "some_model"},
            "is_global": False,
        },
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_put_8.status_code == 404

    # PUT: non-admin user cannot update global flavour (403)
    res_put_9 = requests.put(
        f"{flavour_server}/databrowser/flavours/testx",
        json={
            "flavour_name": "testx",
            "mapping": {"model": "new_model"},
            "is_global": True,
        },
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_put_9.status_code == 403

    # PUT: admin user can update global flavour
    with mock.patch(
        "freva_rest.rest.server_config.admins_token_claims",
        {"resource_access.realm-management.roles": ["admin"]},
    ):
        res_put_10 = requests.put(
            f"{flavour_server}/databrowser/flavours/testx",
            json={
                "flavour_name": "testx",
                "mapping": {"model": "admin_updated_model"},
                "is_global": True,
            },
            headers={"Authorization": f"Bearer {auth_admin['access_token']}"},
        )
        assert res_put_10.status_code == 200
        assert "status" in res_put_10.json()

    # PUT: Even admin user cannot update the built-in flavours (422)
    with mock.patch(
        "freva_rest.rest.server_config.admins_token_claims",
        {"resource_access.realm-management.roles": ["admin"]},
    ):
        res_put_11 = requests.put(
            f"{flavour_server}/databrowser/flavours/freva",
            json={
                "flavour_name": "freva",
                "mapping": {"model": "some_model"},
                "is_global": True,
            },
            headers={"Authorization": f"Bearer {auth_admin['access_token']}"},
        )
        assert res_put_11.status_code == 422
        assert "Cannot update built-in flavour" in res_put_11.json()["detail"]
    # ========== DELETE METHOD TESTS ==========

    # DELETE: deleting custom personal flavour
    res_delete_1 = requests.delete(
        f"{flavour_server}/databrowser/flavours/test_flavour",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_delete_1.status_code == 200
    assert "status" in res_delete_1.json()

    # DELETE: built-in flavours cannot be deleted
    res_delete_2 = requests.delete(
        f"{flavour_server}/databrowser/flavours/freva",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_delete_2.status_code == 422
    assert "built-in or does not exist" in res_delete_2.json()["detail"]

    # DELETE: deleting non-existent flavour returns 422
    res_delete_3 = requests.delete(
        f"{flavour_server}/databrowser/flavours/non_existent_flavour",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    print(res_delete_3.text)
    assert res_delete_3.status_code == 422

    # DELETE: non-admin user cannot delete global flavour (403)
    res_delete_4 = requests.delete(
        f"{flavour_server}/databrowser/flavours/testx?is_global=true",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_delete_4.status_code == 403

    # DELETE: admin user can delete global flavour
    with mock.patch(
        "freva_rest.rest.server_config.admins_token_claims",
        {"resource_access.realm-management.roles": "admin"},
    ):
        res_delete_5 = requests.delete(
            f"{flavour_server}/databrowser/flavours/testx?is_global=true",
            headers={"Authorization": f"Bearer {auth_admin['access_token']}"},
        )
        assert res_delete_5.status_code == 200
    # DELETE: delete test_flavour_double created earlier
    res_delete_6 = requests.delete(
        f"{flavour_server}/databrowser/flavours/test_flavour_double",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res_delete_6.status_code == 200
    # ========== Mixed SCENARIOS ==========

    # Scenario: add both global and personal flavour with same name
    flavour_with_same_name = {
        "flavour_name": "test_flavour",
        "mapping": {
            "project": "my_project",
            "variable": "my_variable",
        },
        "is_global": True,
    }
    with mock.patch(
        "freva_rest.rest.server_config.admins_token_claims",
        {"resource_access.realm-management.roles": ["admin"]},
    ):
        res11 = requests.post(
            f"{flavour_server}/databrowser/flavours",
            json=flavour_with_same_name,
            headers={"Authorization": f"Bearer {auth_admin['access_token']}"},
        )
        assert res11.status_code == 201

    flavour_with_same_name = {
        "flavour_name": "test_flavour",
        "mapping": {
            "project": "my_project",
            "variable": "my_variable",
        },
        "is_global": False,
    }
    res12 = requests.post(
        f"{flavour_server}/databrowser/flavours",
        json=flavour_with_same_name,
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res12.status_code == 201

    # Test: both global and personal flavours appear in listing
    res13 = requests.get(
        f"{flavour_server}/databrowser/flavours",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    flavour_names = [f["flavour_name"] for f in res13.json()["flavours"]]
    assert set(["test_flavour", "janedoe:test_flavour"]).issubset(
        set(flavour_names)
    )

    # Cleanup: delete both flavours
    with mock.patch(
        "freva_rest.rest.server_config.admins_token_claims",
        {"resource_access.realm-management.roles": "admin"},
    ):
        requests.delete(
            f"{flavour_server}/databrowser/flavours/test_flavour?is_global=true",
            headers={"Authorization": f"Bearer {auth_admin['access_token']}"},
        )

    requests.delete(
        f"{flavour_server}/databrowser/flavours/test_flavour",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )

    # Scenario: user cannot delete another user's personal flavour
    admin_personal_flavour = {
        "flavour_name": "test_flavour",
        "mapping": {"project": "my_project", "variable": "my_variable"},
        "is_global": False,
    }
    with mock.patch(
        "freva_rest.rest.server_config.admins_token_claims",
        {"resource_access.realm-management.roles": "admin"},
    ):
        res14 = requests.post(
            f"{flavour_server}/databrowser/flavours",
            json=admin_personal_flavour,
            headers={"Authorization": f"Bearer {auth_admin['access_token']}"},
        )
        assert res14.status_code == 201

    # Test: another user cannot delete someone else's personal flavour
    res15 = requests.delete(
        f"{flavour_server}/databrowser/flavours/test_flavour",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res15.status_code == 422

    # Cleanup: admin deletes their own personal flavour
    with mock.patch(
        "freva_rest.rest.server_config.admins_token_claims",
        {"resource_access.realm-management.roles": "admin"},
    ):
        requests.delete(
            f"{flavour_server}/databrowser/flavours/test_flavour",
            headers={"Authorization": f"Bearer {auth_admin['access_token']}"},
        )

    # Scenario: personal flavour can have same name as built-in flavour
    freva_flavour = {
        "flavour_name": "cmip6",
        "mapping": {"project": "my_project", "variable": "my_variable"},
        "is_global": False,
    }
    res16 = requests.post(
        f"{flavour_server}/databrowser/flavours",
        json=freva_flavour,
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res16.status_code == 201

    # Test: personal flavour appears with user prefix in overview
    res17 = requests.get(
        f"{flavour_server}/databrowser/overview",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res17.status_code == 200
    flavours = res17.json()["flavours"]
    assert any(f == "janedoe:cmip6" for f in flavours)

    # Test: delete personal flavour with user prefix
    res18 = requests.delete(
        f"{flavour_server}/databrowser/flavours/janedoe:cmip6",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res18.status_code == 200

    # Test: built-in flavours cannot be deleted even by admin
    with mock.patch(
        "freva_rest.rest.server_config.admins_token_claims",
        {"resource_access.realm-management.roles": "admin"},
    ):
        res18 = requests.delete(
            f"{flavour_server}/databrowser/flavours/cmip6?is_global=true",
            headers={"Authorization": f"Bearer {auth_admin['access_token']}"},
        )
        assert res18.status_code == 422
