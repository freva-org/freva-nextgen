"""Unit tests for the configuration."""

import logging
from pathlib import Path
from typing import List

import pytest
from pytest import LogCaptureFixture
from pytest_mock import MockerFixture

from freva_rest.config import ServerConfig


def test_valid_config() -> None:
    """Test the creation of a valid config file."""

    cfg = ServerConfig(debug=True, solr_host="https://localhost")
    assert cfg.log_level == 10
    assert cfg.solr_url == "https://localhost:8983"
    cfg.debug = False
    cfg.reload()
    assert cfg.solr_url == "https://localhost:8983"
    cfg = ServerConfig(solr_host="localhost:123")
    assert cfg.solr_url == "http://localhost:123"


def test_invalid_config(caplog: LogCaptureFixture) -> None:
    """Test the behaviour for an invalid config file."""
    caplog.clear()
    _ = ServerConfig(config=Path("/foo/bar.toml"), debug=True)
    assert caplog.records
    records: List[logging.LogRecord] = caplog.records
    assert any([record.levelname == "CRITICAL" for record in records])
    assert any(["Failed to load" in record.message for record in records])


def test_oidc_overview_fetch_failure(mocker: MockerFixture) -> None:
    """oidc_overview raises when the discovery URL is unreachable."""
    from freva_rest.config import ServerConfig

    config = ServerConfig()
    config._oidc_overview = None  # clear cache
    mocker.patch("requests.get", side_effect=Exception("unreachable"))

    with pytest.raises(Exception):
        _ = config.oidc_overview


def test_oidc_overview_cached(test_server: str) -> None:
    """oidc_overview returns cached result without hitting the network."""
    from freva_rest.config import ServerConfig

    config = ServerConfig()
    config._oidc_overview = {"issuer": "https://example.com"}
    assert config.oidc_overview == {"issuer": "https://example.com"}
