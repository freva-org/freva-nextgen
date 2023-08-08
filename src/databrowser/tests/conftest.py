"""Pytest configuration settings."""

import asyncio
import os
from typing import Iterator

import mock
import pytest
from fastapi.testclient import TestClient

from databrowser.config import ServerConfig, defaults
from databrowser.run import app
from databrowser.tests.mock import read_data


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

    with mock.patch("databrowser.run.SolrSearch.batch_size", 3):
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
        with mock.patch("databrowser.run.solr_config.mongo_collection", None):
            with TestClient(app) as test_client:
                yield test_client
