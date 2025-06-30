"""Unit tests for data queries via the rest-api."""

import subprocess
import requests


def test_stacapi_basic(test_server: str) -> None:
    """Test the default stacapi functionality."""
    result_catalog = requests.get(f"{test_server}/stacapi/")
    assert result_catalog.json()["stac_version"] == "1.1.0"
    assert result_catalog.json()["type"] == "Catalog"

    result_collections = requests.get(f"{test_server}/stacapi/collections")
    assert isinstance(result_collections.json()["collections"], list)
    assert len(result_collections.json()["links"]) > 0

    result_collection = requests.get(f"{test_server}/stacapi/collections/cmip6")
    assert result_collection.status_code == 200
    assert result_collection.json()["id"] == "cmip6"
    assert result_collection.json()["stac_version"] == "1.1.0"
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
    assert "https://api.stacspec.org/v1.0.0/ogcapi-features" in result.json()["conformsTo"]

def test_stacapi_item_params(test_server: str) -> None:
    """Test the default stacapi item parameters functionality."""
    result = requests.get(f"{test_server}/stacapi/collections/cmip6/items", params={"limit": 1})
    assert result.status_code == 200
    assert len(result.json()["features"]) == 1

    # invalid parameter
    result = requests.get(f"{test_server}/stacapi/collections/cmip6/items", params={"limit": 0})
    assert result.status_code == 422
    
    result = requests.get(f"{test_server}/stacapi/collections/cmip6/items", params={"limitx": 1001})
    assert result.status_code == 422

    result = requests.get(f"{test_server}/stacapi/collections/cmip6/items", params={"datetime": "2023-10-01/2023-10-31", "bbox": "10,20,30,40"})
    assert result.status_code == 200
    assert isinstance(result.json()["features"], list)

    result = requests.get(f"{test_server}/stacapi/collections/cmip6/items", params={"datetime": "2023-10-01", "bbox": "10,20,30,40"})
    assert result.status_code == 200
    assert isinstance(result.json()["features"], list)

    result = requests.get(f"{test_server}/stacapi/collections/cmip6/items", params={"datetime": "/2023-10-01", "bbox": "10,20,30,40"})
    assert result.status_code == 422


    # test the next and previous token
    result_get = requests.get(f"{test_server}/stacapi/collections/cordex/items")
    cordex_length = len(result_get.json().get('features'))
    last_item_id = result_get.json().get('features')[cordex_length - 1].get('id')
    result = requests.get(f"{test_server}/stacapi/collections/cordex/items", params={"limit": 1, "token": f"prev:cordex:{last_item_id}"})
    assert result.status_code == 200
    assert len(result.json()["features"]) == 1


    first_item_id = result_get.json().get('features')[0].get('id')
    result = requests.get(f"{test_server}/stacapi/collections/cordex/items", params={"limit": 1, "token": f"next:cordex:{first_item_id}"})
    assert result.status_code == 200
    assert len(result.json()["features"]) == 1


    result = requests.get(f"{test_server}/stacapi/collections/cordex/items", params={"limit": 1, "token": f"wrong_direction:cordex:{last_item_id}"})
    assert result.status_code == 422

    result = requests.get(f"{test_server}/stacapi/collections/cordex/items", params={"limit": 1, "token": f"wrong_direction:cordex:"})
    assert result.status_code == 422

def test_stacapi_staccheck(test_server: str) -> None:
    """Test the stacapi staccheck functionality."""
    result_output = subprocess.run(["stac-check", f"{test_server}/stacapi/"], check=True, capture_output=True)
    print(result_output.stdout)
    assert "CATALOG Passed: True" in result_output.stdout.decode("utf-8")

    result_output = subprocess.run(["stac-check", f"{test_server}/stacapi/collections/cmip6/"], check=True, capture_output=True)
    assert "COLLECTION Passed: True" in result_output.stdout.decode("utf-8")

    result_get = requests.get(f"{test_server}/stacapi/collections/nextgems/items?limit=1")
    item_id = result_get.json().get('features')[0].get('id')
    result_output = subprocess.run(["stac-check", f"{test_server}/stacapi/collections/nextgems/items/{item_id}/"], check=True, capture_output=True)
    
    assert "ITEM Passed: True" in result_output.stdout.decode("utf-8")

def test_stacapi_fail(test_server: str) -> None:
    """Test the stacapi fail functionality."""
    result = requests.get(f"{test_server}/stacapi/collections/cmip69/")
    assert result.status_code == 404

    result = requests.get(f"{test_server}/stacapi/collections/cmip6/items/wrong_item_id")
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
        params={"collections": "cmip6", "limit": 5}
    )
    assert res2.status_code == 200
    
    res3 = requests.get(
        f"{test_server}/stacapi/search",
        params={"bbox": "10,20,30,40", "limit": 3}
    )
    assert res3.status_code == 200
    
    # Invalid bbox format
    res4 = requests.get(
        f"{test_server}/stacapi/search",
        params={"bbox": "invalid_bbox"}
    )
    assert res4.status_code == 422


def test_stacapi_search_post(test_server: str) -> None:
    """Test the STAC API search POST endpoint."""
    search_body = {"limit": 5}
    res1 = requests.post(
        f"{test_server}/stacapi/search",
        json=search_body
    )
    assert res1.status_code == 200
    data = res1.json()
    assert "features" in data
    assert "type" in data
    
    search_body = {
        "collections": ["cmip6"],
        "limit": 3
    }
    res2 = requests.post(
        f"{test_server}/stacapi/search",
        json=search_body
    )
    assert res2.status_code == 200
    
    search_body = {
        "bbox": [10, 20, 30, 40],
        "limit": 2
    }
    res3 = requests.post(
        f"{test_server}/stacapi/search",
        json=search_body
    )
    assert res3.status_code == 200
    
    # Invalid POST body
    res4 = requests.post(
        f"{test_server}/stacapi/search",
        json={"limit": 0}
    )
    assert res4.status_code == 422

    # POST search with Free Text Search list
    search_body = {
        "q": "[cmip, temperature]",
        "limit": 2
    }
    res5 = requests.post(
        f"{test_server}/stacapi/search",
        json=search_body
    )
    assert res5.status_code == 200

    search_body = {
        "q": "cmip",
        "limit": 2
    }
    res6 = requests.post(
        f"{test_server}/stacapi/search",
        json=search_body
    )
    assert res6.status_code == 200
    


def test_stacapi_queryables(test_server: str) -> None:
    """Test the STAC API queryables endpoints."""
    res1 = requests.get(f"{test_server}/stacapi/queryables")
    assert res1.status_code == 200
    data = res1.json()
    assert "$schema" in data
    assert "properties" in data
    
    res2 = requests.get(
        f"{test_server}/stacapi/collections/cmip6/queryables"
    )
    assert res2.status_code == 200
    data = res2.json()
    assert "$schema" in data
    assert "properties" in data
    
    # Invalid collection queryables
    res3 = requests.get(
        f"{test_server}/stacapi/collections/invalid/queryables"
    )
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
        params={"datetime": "2023-01-01/2023-12-31", "limit": 2}
    )
    assert res1.status_code == 200
    

    res2 = requests.get(
        f"{test_server}/stacapi/search",
        params={"ids": "some_id", "limit": 1}
    )
    assert res2.status_code == 200
    
    # with free text search
    res3 = requests.get(
        f"{test_server}/stacapi/search",
        params={"q": "climate,temperature", "limit": 2}
    )
    assert res3.status_code == 200
    
    res4 = requests.get(
        f"{test_server}/stacapi/search",
        params={"token": "next:search:some_id", "limit": 1}
    )
    assert res4.status_code == 200
    
    # Invalid token format
    res5 = requests.get(
        f"{test_server}/stacapi/search",
        params={"token": "invalid_token_format"}
    )
    assert res5.status_code == 422