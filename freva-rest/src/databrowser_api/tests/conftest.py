"""Pytest configuration settings."""

import asyncio
import os
from typing import Iterator

import mock
import pytest
from freva_rest.config import ServerConfig, defaults
from freva_rest.rest import app
from databrowser_api.tests.mock import read_data
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def cfg() -> Iterator[ServerConfig]:
    """Create a valid server config."""
    cfg = ServerConfig(defaults["API_CONFIG"], debug=True)
    for core in cfg.solr_cores:
        asyncio.run(read_data(core, cfg.solr_host, cfg.solr_port))
    yield cfg


@pytest.fixture(scope="module")
def client(cfg: ServerConfig) -> Iterator[TestClient]:
    """Setup the test client for the unit test."""

    with mock.patch("databrowser_api.run.SolrSearch.batch_size", 3):
        with TestClient(app) as test_client:
            yield test_client


@pytest.fixture(scope="function")
def client_no_mongo(cfg: ServerConfig) -> Iterator[TestClient]:
    """Setup a client with an invalid mongodb."""
    env = os.environ.copy()
    env["MONGO_HOST"] = "foo.bar.de"
    with mock.patch.dict(os.environ, env, clear=True):
        cfg = ServerConfig(defaults["API_CONFIG"], debug=True)
        for core in cfg.solr_cores:
            asyncio.run(read_data(core, cfg.solr_host, cfg.solr_port))
        with mock.patch("freva_rest.rest.server_config.mongo_collection", None):
            with TestClient(app) as test_client:
                yield test_client


@pytest.fixture(scope="function")
def client_no_solr(cfg: ServerConfig) -> Iterator[TestClient]:
    """Setup a client with an invalid mongodb."""
    env = os.environ.copy()
    env["SOLR_HOST"] = "foo.bar.de"
    with mock.patch.dict(os.environ, env, clear=True):
        ServerConfig(defaults["API_CONFIG"], debug=True)
        with TestClient(app) as test_client:
            yield test_client
