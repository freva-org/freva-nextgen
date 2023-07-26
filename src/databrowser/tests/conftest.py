"""Pytest configuration settings."""

import asyncio
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from databrowser.config import defaults, ServerConfig
from databrowser.tests.mock import read_data
from databrowser.run import app


@pytest.fixture(scope="module")
def client() -> Iterator[TestClient]:
    """Setup the test client for the unit test."""
    cfg = ServerConfig(defaults["API_CONFIG"], debug=True)
    for core in cfg.solr_cores:
        asyncio.run(read_data(core, cfg.solr_host, cfg.solr_port))
    with TestClient(app) as test_client:
        yield test_client
