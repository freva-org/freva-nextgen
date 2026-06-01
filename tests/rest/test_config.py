"""Unit tests for the configuration."""

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, List
from unittest import mock

import pytest
from cachetools import TTLCache
from pytest import LogCaptureFixture

from freva_rest.config import AsyncTTLCache, ServerConfig, env_to_dict


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


@pytest.mark.asyncio
class TestAsyncTTLCache:
    async def test_get_returns_none_for_missing_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cache_backend: TTLCache[str, Any] = TTLCache(maxsize=10, ttl=60)
        monkeypatch.setattr("freva_rest.config.SEARCH_CACHE", cache_backend)

        cache: AsyncTTLCache[str] = AsyncTTLCache()

        assert await cache.get("missing") is None

    async def test_set_and_get_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cache_backend: TTLCache[str, Any] = TTLCache(maxsize=10, ttl=60)
        monkeypatch.setattr("freva_rest.config.SEARCH_CACHE", cache_backend)

        cache: AsyncTTLCache[str] = AsyncTTLCache()

        await cache.set("foo", "bar")

        assert await cache.get("foo") == "bar"

    async def test_clear_removes_cached_values(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cache_backend: TTLCache[str, Any] = TTLCache(maxsize=10, ttl=60)
        monkeypatch.setattr("freva_rest.config.SEARCH_CACHE", cache_backend)

        cache: AsyncTTLCache[str] = AsyncTTLCache()

        await cache.set("foo", "bar")
        await cache.clear()

        assert await cache.get("foo") is None

    async def test_get_returns_deepcopy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cache_backend: TTLCache[str, Any] = TTLCache(maxsize=10, ttl=60)
        monkeypatch.setattr("freva_rest.config.SEARCH_CACHE", cache_backend)

        cache: AsyncTTLCache[dict[str, list[int]]] = AsyncTTLCache()

        await cache.set("foo", {"values": [1, 2, 3]})

        cached = await cache.get("foo")
        assert cached == {"values": [1, 2, 3]}

        assert cached is not None
        cached["values"].append(4)

        cached_again = await cache.get("foo")

        assert cached_again == {"values": [1, 2, 3]}

    async def test_set_stores_deepcopy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cache_backend: TTLCache[str, Any] = TTLCache(maxsize=10, ttl=60)
        monkeypatch.setattr("freva_rest.config.SEARCH_CACHE", cache_backend)

        cache: AsyncTTLCache[dict[str, list[int]]] = AsyncTTLCache()

        value = {"values": [1, 2, 3]}
        await cache.set("foo", value)

        value["values"].append(4)

        cached = await cache.get("foo")

        assert cached == {"values": [1, 2, 3]}

    async def test_cache_uses_ttl_backend(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cache_backend: TTLCache[str, Any] = TTLCache(maxsize=10, ttl=0.01)
        monkeypatch.setattr("freva_rest.config.SEARCH_CACHE", cache_backend)

        cache: AsyncTTLCache[str] = AsyncTTLCache()

        await cache.set("foo", "bar")
        assert await cache.get("foo") == "bar"

        await asyncio.sleep(0.02)

        assert await cache.get("foo") is None

    async def test_concurrent_get_and_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cache_backend: TTLCache[str, Any] = TTLCache(maxsize=100, ttl=60)
        monkeypatch.setattr("freva_rest.config.SEARCH_CACHE", cache_backend)

        cache: AsyncTTLCache[int] = AsyncTTLCache()

        async def set_value(index: int) -> None:
            await cache.set(f"key-{index}", index)

        await asyncio.gather(*(set_value(index) for index in range(20)))

        values = await asyncio.gather(
            *(cache.get(f"key-{index}") for index in range(20))
        )

        assert values == list(range(20))


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


class TestSolrConnection:
    """Tests for handling apache solr connections."""

    def test_solr_fields_retry_after_startup_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An empty startup fetch should be retried on the next access."""
        ServerConfig._instance = None
        ServerConfig._initialised = False
        monkeypatch.setenv("API_TESTS", "1")

        calls = 0

        def fake_get_solr_fields(this: ServerConfig) -> list[str]:
            nonlocal calls
            calls += 1
            if calls == 1:
                return []
            return ["project", "experiment"]

        monkeypatch.setattr(ServerConfig, "_get_solr_fields", fake_get_solr_fields)

        cfg = ServerConfig()

        # First call happens during model_post_init()
        assert cfg._solr_fields == []

        # Second access should retry and refill the cache
        assert cfg.solr_fields == ["project", "experiment"]
        assert cfg._solr_fields == ["project", "experiment"]
        assert calls == 2

    def test_solr_fields_failure_stays_empty_list(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A failed Solr field fetch should not create ['']."""
        ServerConfig._instance = None
        ServerConfig._initialised = False
        monkeypatch.setenv("API_TESTS", "1")

        monkeypatch.setattr(ServerConfig, "_get_solr_fields", lambda this: [])

        cfg = ServerConfig()

        assert cfg.solr_fields == []
        assert cfg.solr_fields != [""]
