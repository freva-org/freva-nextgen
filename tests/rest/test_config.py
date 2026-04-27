"""Unit tests for the configuration."""

import logging
import os
from pathlib import Path
from typing import List
from unittest import mock

from pytest import LogCaptureFixture

from freva_rest.config import ServerConfig, env_to_dict


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


class TestEnvToDict:
    """Tests for env_to_dict."""

    def test_basic_key_value_pairs(self) -> None:
        """Standard key:value pairs are parsed correctly."""
        with mock.patch.dict(os.environ, {"TEST_VAR": "k1:v1,k2:v2"}):
            result = env_to_dict("TEST_VAR")
        assert result == {"k1": ["v1"], "k2": ["v2"]}

    def test_multiple_values_same_key(self) -> None:
        """Multiple values for the same key are grouped into a list."""
        with mock.patch.dict(os.environ, {"TEST_VAR": "k1:v1,k2:v2,k1:v3"}):
            result = env_to_dict("TEST_VAR")
        assert result == {"k1": ["v1", "v3"], "k2": ["v2"]}

    def test_duplicate_values_deduplicated(self) -> None:
        """Duplicate values for the same key are not repeated."""
        with mock.patch.dict(os.environ, {"TEST_VAR": "k1:v1,k1:v1"}):
            result = env_to_dict("TEST_VAR")
        assert result == {"k1": ["v1"]}

    def test_missing_env_var_returns_empty(self) -> None:
        """An unset environment variable returns an empty dict."""
        with mock.patch.dict(os.environ, {}, clear=True):
            result = env_to_dict("NONEXISTENT_VAR")
        assert result == {}

    def test_empty_env_var_returns_empty(self) -> None:
        """An empty environment variable returns an empty dict."""
        with mock.patch.dict(os.environ, {"TEST_VAR": ""}):
            result = env_to_dict("TEST_VAR")
        assert result == {}

    def test_default_key_used_when_no_colon(self) -> None:
        """When no colon is present, default_key is used as the key."""
        with mock.patch.dict(os.environ, {"TEST_VAR": "value_only"}):
            result = env_to_dict("TEST_VAR", default_key="fallback")
        assert result == {"fallback": ["value_only"]}

    def test_no_default_key_no_colon_skipped(self) -> None:
        """Without a default_key, entries with no colon are skipped."""
        with mock.patch.dict(os.environ, {"TEST_VAR": "value_only"}):
            result = env_to_dict("TEST_VAR")
        assert result == {}

    def test_empty_value_skipped(self) -> None:
        """Entries with an empty value after the colon are skipped."""
        with mock.patch.dict(os.environ, {"TEST_VAR": "k1:,k2:v2"}):
            result = env_to_dict("TEST_VAR")
        assert result == {"k2": ["v2"]}

    def test_value_with_colons_preserved(self) -> None:
        """Only the first colon splits key from value."""
        with mock.patch.dict(os.environ, {"TEST_VAR": "k1:v1:v2:v3"}):
            result = env_to_dict("TEST_VAR")
        assert result == {"k1": ["v1:v2:v3"]}

    def test_mixed_default_and_keyed(self) -> None:
        """Default key and explicit keys coexist correctly."""
        with mock.patch.dict(os.environ, {"TEST_VAR": "bare,k1:v1"}):
            result = env_to_dict("TEST_VAR", default_key="default")
        assert result == {"default": ["bare"], "k1": ["v1"]}
