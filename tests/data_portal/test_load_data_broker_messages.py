"""Funktional-ish broker-message tests for data_portal_worker.load_data.

These tests exercise the worker at the Redis message boundary: every main test
starts by delivering a broker payload to ProcessQueue.redis_callback and then
asserts the result written to the cache. Heavy xarray/netCDF work is patched in
the success-path tests so the tests stay fast and deterministic.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional

import cloudpickle
import numpy as np
import pytest
import xarray as xr

from data_portal_worker.load_data import ProcessQueue, StateEnum


class InMemoryCache:
    """Small Redis-like cache for worker message tests."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.values: dict[str, Any] = {}
        self.lists: dict[str, list[Any]] = {}
        self.expires: dict[str, int] = {}

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            return self.values.get(key)

    def setex(self, key: str, ttl: int, value: Any) -> bool:
        with self._lock:
            self.values[key] = value
            self.expires[key] = ttl
            return True

    def lpush(self, key: str, value: Any) -> int:
        with self._lock:
            self.lists.setdefault(key, []).insert(0, value)
            return len(self.lists[key])

    def expire(self, key: str, ttl: int) -> bool:
        with self._lock:
            self.expires[key] = ttl
            return True


def _make_queue(cache: InMemoryCache) -> ProcessQueue:
    """Create a ProcessQueue wired to an in-memory cache."""
    queue = ProcessQueue()
    queue._cache = cache
    return queue


def _send_broker_message(queue: ProcessQueue, payload: dict[str, Any]) -> None:
    """Deliver a JSON broker message to the worker callback."""
    queue.redis_callback(json.dumps(payload).encode("utf-8"))


def _load_pickle(raw: Any) -> dict[str, Any]:
    """Load a cloudpickle payload from the fake cache."""
    assert raw is not None
    return cloudpickle.loads(raw)


def _wait_for(
    predicate: Callable[[], bool],
    timeout: float = 2.0,
    interval: float = 0.02,
) -> None:
    """Wait until a background worker thread has written its result."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError("Timed out waiting for worker result")


def _wait_for_cached_status(
    cache: InMemoryCache,
    key: str,
    expected: int,
    timeout: float = 2.0,
) -> dict[str, Any]:
    """Wait until key contains a pickled status dict with expected status."""
    result: dict[str, Any] = {}

    def _ready() -> bool:
        raw = cache.get(key)
        if raw is None:
            return False
        result.clear()
        result.update(_load_pickle(raw))
        return result.get("status") == expected

    _wait_for(_ready, timeout=timeout)
    return result


def _wait_for_cache_key(
    cache: InMemoryCache,
    key: str,
    timeout: float = 2.0,
) -> dict[str, Any]:
    """Wait until a pickled cache key exists."""
    result: dict[str, Any] = {}

    def _ready() -> bool:
        raw = cache.get(key)
        if raw is None:
            return False
        result.clear()
        result.update(_load_pickle(raw))
        return True

    _wait_for(_ready, timeout=timeout)
    return result


class TestAccessCheckBrokerMessages:
    """Broker-driven tests for access_check messages."""

    def test_access_check_allowed_writes_positive_reply(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An allowed permission check produces an allowed Redis reply."""
        cache = InMemoryCache()
        queue = _make_queue(cache)
        source = tmp_path / "readable.nc"
        source.write_bytes(b"dummy")

        monkeypatch.setattr(
            "data_portal_worker.load_data.user_can_read",
            lambda path, username: True,
        )

        _send_broker_message(
            queue,
            {
                "access_check": {
                    "request_id": "req-allowed",
                    "username": "alice",
                    "paths": [str(source)],
                }
            },
        )

        reply_key = "access-reply:req-allowed"
        assert json.loads(cache.lists[reply_key][0]) == {"allowed": True}
        assert cache.expires[reply_key] == 30

    def test_access_check_denied_writes_negative_reply(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A denied permission check produces a denied Redis reply."""
        cache = InMemoryCache()
        queue = _make_queue(cache)
        source = tmp_path / "private.nc"
        source.write_bytes(b"dummy")

        monkeypatch.setattr(
            "data_portal_worker.load_data.user_can_read",
            lambda path, username: False,
        )

        _send_broker_message(
            queue,
            {
                "access_check": {
                    "request_id": "req-denied",
                    "username": "bob",
                    "paths": [str(source)],
                }
            },
        )

        reply_key = "access-reply:req-denied"
        assert json.loads(cache.lists[reply_key][0]) == {"allowed": False}
        assert cache.expires[reply_key] == 30


class TestUriBrokerMessages:
    """Broker-driven tests for uri/spawn messages."""

    def test_uri_message_loads_dataset_and_writes_metadata(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A valid uri message loads data and writes status + dset cache."""
        cache = InMemoryCache()
        queue = _make_queue(cache)
        source = tmp_path / "input.nc"
        source.write_bytes(b"dummy")
        token = "load-ok-token"
        dataset = xr.Dataset({"temp": ("x", np.arange(4, dtype="i4"))})

        monkeypatch.setattr(
            "data_portal_worker.load_data.user_can_read",
            lambda path, username: True,
        )
        monkeypatch.setattr(
            "data_portal_worker.load_data.load_data",
            lambda path: dataset,
        )

        class DummyAggregator:
            def aggregate(self, datasets, job_id, plan):
                assert job_id == token
                assert plan == {"mode": "merge"}
                return {"root": datasets[0]}

        class DummyOptimizer:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

            def apply(self, ds):
                return ds

        monkeypatch.setattr(
            "data_portal_worker.load_data.DatasetAggregator",
            DummyAggregator,
        )
        monkeypatch.setattr(
            "data_portal_worker.load_data.ChunkOptimizer",
            DummyOptimizer,
        )
        monkeypatch.setattr(
            "data_portal_worker.load_data.write_grouped_zarr",
            lambda dsets: {"metadata": {".zgroup": {"zarr_format": 2}}},
        )
        monkeypatch.setattr(
            "data_portal_worker.load_data.xr_repr_html",
            lambda dsets: "<b>dataset</b>",
        )
        monkeypatch.setattr(
            "data_portal_worker.load_data.DataLoadFactory._preload_coordinate_chunks",
            lambda self, token, meta, dsets, ttl: None,
        )

        _send_broker_message(
            queue,
            {
                "uri": {
                    "path": [str(source)],
                    "uuid": token,
                    "username": "alice",
                    "assembly": {"mode": "merge"},
                    "access_pattern": "map",
                    "map_primary_chunksize": 2,
                    "chunk_size": 16.0,
                }
            },
        )

        status = _wait_for_cached_status(
            cache,
            token,
            StateEnum.finished_ok.value,
        )
        assert status["data"] == {"metadata": {".zgroup": {"zarr_format": 2}}}
        assert status["repr_html"] == "<b>dataset</b>"
        assert cache.get(f"{token}-dset") is not None

    def test_uri_message_denied_by_worker_side_permission_check(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A direct Redis uri message is denied if the worker check fails."""
        cache = InMemoryCache()
        queue = _make_queue(cache)
        source = tmp_path / "private.nc"
        source.write_bytes(b"dummy")
        token = "load-denied-token"

        monkeypatch.setattr(
            "data_portal_worker.load_data.user_can_read",
            lambda path, username: False,
        )

        _send_broker_message(
            queue,
            {
                "uri": {
                    "path": [str(source)],
                    "uuid": token,
                    "username": "mallory",
                }
            },
        )

        status = _wait_for_cached_status(
            cache,
            token,
            StateEnum.finished_permission_denied.value,
        )
        assert "Permission denied" in status["reason"]
        assert cache.get(f"{token}-dset") is None

    def test_uri_message_does_not_reload_finished_dataset_without_reload_flag(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A cached successful dataset is not loaded again by default."""
        cache = InMemoryCache()
        queue = _make_queue(cache)
        token = "already-loaded-token"
        existing = {"status": StateEnum.finished_ok.value, "reason": "cached"}
        cache.setex(token, 60, cloudpickle.dumps(existing))

        def fail_if_called(*args, **kwargs):
            raise AssertionError("from_object_path should not be called")

        monkeypatch.setattr(ProcessQueue, "from_object_path", fail_if_called)

        _send_broker_message(
            queue,
            {
                "uri": {
                    "path": ["/some/path.nc"],
                    "uuid": token,
                    "username": "alice",
                }
            },
        )

        assert _load_pickle(cache.get(token)) == existing

    def test_uri_message_reload_forces_new_cache_result(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """reload=True forces the uri message down the load path."""
        cache = InMemoryCache()
        queue = _make_queue(cache)
        token = "reload-token"
        cache.setex(
            token,
            60,
            cloudpickle.dumps({"status": StateEnum.finished_ok.value}),
        )

        def fake_from_object_path(self, input_paths, path_id, **kwargs):
            cache.setex(
                path_id,
                60,
                cloudpickle.dumps(
                    {
                        "status": StateEnum.finished_ok.value,
                        "reason": "reloaded",
                        "data": {
                            "paths": input_paths,
                            "username": kwargs["username"],
                        },
                    }
                ),
            )

        monkeypatch.setattr(ProcessQueue, "from_object_path", fake_from_object_path)

        _send_broker_message(
            queue,
            {
                "uri": {
                    "path": ["/some/path.nc"],
                    "uuid": token,
                    "username": "alice",
                    "reload": True,
                }
            },
        )

        status = _load_pickle(cache.get(token))
        assert status["status"] == StateEnum.finished_ok.value
        assert status["reason"] == "reloaded"
        assert status["data"]["username"] == "alice"


class TestChunkBrokerMessages:
    """Broker-driven tests for chunk messages."""

    def test_chunk_message_reads_cached_dataset_and_writes_encoded_chunk(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A chunk message reads the cached dset and writes chunk bytes."""
        cache = InMemoryCache()
        queue = _make_queue(cache)
        token = "chunk-token"

        dataset = xr.Dataset({"temp": ("x", np.arange(4, dtype="i4"))})
        metadata = {
            "metadata": {
                "temp/.zarray": {
                    "chunks": [2],
                    "filters": None,
                    "compressor": None,
                }
            }
        }
        cache.setex(
            token,
            60,
            cloudpickle.dumps(
                {
                    "status": StateEnum.finished_ok.value,
                    "data": metadata,
                }
            ),
        )
        cache.setex(f"{token}-dset", 60, cloudpickle.dumps({"root": dataset}))

        monkeypatch.setattr(
            "data_portal_worker.load_data.numcodecs.get_codec",
            lambda compressor: None,
        )
        monkeypatch.setattr(
            "data_portal_worker.load_data.get_data_chunk",
            lambda data, chunk, out_shape: np.array([10, 11], dtype="i4"),
        )
        monkeypatch.setattr(
            "data_portal_worker.load_data.encode_chunk",
            lambda raw, filters, compressor: b"encoded-chunk",
        )

        _send_broker_message(
            queue,
            {
                "chunk": {
                    "uuid": token,
                    "variable": "temp",
                    "chunk": "0",
                }
            },
        )

        result = _wait_for_cache_key(cache, f"{token}-temp-0")
        assert result["status"] == StateEnum.finished_ok.value
        assert result["data"] == b"encoded-chunk"
        assert result["reason"] == ""

    def test_chunk_message_for_missing_dataset_writes_not_found_status(self) -> None:
        """A chunk request for an unknown token writes a not-found package."""
        cache = InMemoryCache()
        queue = _make_queue(cache)
        token = "missing-token"

        _send_broker_message(
            queue,
            {
                "chunk": {
                    "uuid": token,
                    "variable": "temp",
                    "chunk": "0",
                }
            },
        )

        result = _wait_for_cache_key(cache, f"{token}-temp-0")
        assert result["status"] == StateEnum.finished_not_found.value
        assert "uuid does not exist" in result["reason"]


class TestMalformedBrokerMessages:
    """Low-level broker message error handling."""

    def test_invalid_json_message_is_ignored(self) -> None:
        """Invalid broker payloads do not write anything to the cache."""
        cache = InMemoryCache()
        queue = _make_queue(cache)

        queue.redis_callback(b"{not-json")

        assert cache.values == {}
        assert cache.lists == {}


class TestCurrentPermissionEdgeCases:
    """Document current edge cases outside the existing permission test module."""

    @pytest.mark.xfail(
        reason=(
            "Current check_for_access_permissions uses all([... for existing "
            "paths]) so an all-missing path list becomes all([]) == True."
        ),
        strict=True,
    )
    def test_uri_message_with_missing_source_path_should_be_permission_denied(
        self,
    ) -> None:
        """Missing source paths should not pass the worker-side access gate."""
        cache = InMemoryCache()
        queue = _make_queue(cache)
        token = "missing-source-token"

        _send_broker_message(
            queue,
            {
                "uri": {
                    "path": ["/definitely/not/present.nc"],
                    "uuid": token,
                    "username": "alice",
                }
            },
        )

        status = _wait_for_cached_status(
            cache,
            token,
            StateEnum.finished_permission_denied.value,
        )
        assert "Permission denied" in status["reason"]
