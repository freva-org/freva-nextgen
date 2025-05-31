"""Unit tests for the configuration."""

import logging
from pathlib import Path
from typing import List

from pytest import LogCaptureFixture

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


def test_token_claims() -> None:
    """Test the token claims of the rest server config."""

    from freva_rest.config import ServerConfig

    cfg = ServerConfig(oidc_token_filter="aud.scope:foo,role:bar")
    assert len(cfg.oidc_token_match) == 2
    assert cfg.oidc_token_match[-1].claim == "role"
    assert cfg.oidc_token_match[-1].pattern == "bar"
    cfg = ServerConfig()
    assert len(cfg.oidc_token_match) == 0
