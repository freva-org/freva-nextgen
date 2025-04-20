"""Unit tests for data queries via the rest-api."""

import json
import os
import time
from typing import Dict
import subprocess
import mock
import requests
from pymongo import MongoClient

from freva_rest.config import ServerConfig


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
    assert "Valid CATALOG: True" in result_output.stdout.decode("utf-8")

    result_output = subprocess.run(["stac-check", f"{test_server}/stacapi/collections/cmip6/"], check=True, capture_output=True)
    assert "Valid COLLECTION: True" in result_output.stdout.decode("utf-8")

    result_get = requests.get(f"{test_server}/stacapi/collections/nextgems/items?limit=1")
    item_id = result_get.json().get('features')[0].get('id')
    result_output = subprocess.run(["stac-check", f"{test_server}/stacapi/collections/nextgems/items/{item_id}/"], check=True, capture_output=True)
    
    assert "Valid ITEM: True" in result_output.stdout.decode("utf-8")

def test_stacapi_fail(test_server: str) -> None:
    """Test the stacapi fail functionality."""
    result = requests.get(f"{test_server}/stacapi/collections/cmip69/")
    assert result.status_code == 404

    result = requests.get(f"{test_server}/stacapi/collections/cmip6/items/wrong_item_id")
    assert result.status_code == 404
