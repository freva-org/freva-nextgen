"""Tests for read_redis_data retry and cache behaviour.

These tests focus on the REST-side Redis read helper without starting the
full test server. Redis I/O is patched at the module boundary, while cache
payloads are still encoded exactly as the production code expects.
"""

from typing import Any
from unittest.mock import AsyncMock, patch

import cloudpickle
import pytest
from fastapi import HTTPException

from freva_rest.freva_data_portal.utils import LoadStatus, read_redis_data
from freva_rest.utils.base_utils import encode_cache_token

pytestmark = [pytest.mark.portal_endpoints, pytest.mark.rest, pytest.mark.asyncio]


def _payload(status: LoadStatus, **extra: Any) -> bytes:
    """Create a pickled Redis payload matching the data-portal cache format."""
    return cloudpickle.dumps({"status": status.value, "reason": "", **extra})


class TestReadRedisData:
    """Tests for reading data-portal cache entries."""

    async def test_returns_requested_subkey_from_finished_cache_entry(self) -> None:
        """A finished cache entry returns the requested subkey."""
        token = encode_cache_token("s3://bucket/source.nc", assembly=None)
        cached = _payload(LoadStatus.finished_ok, data={"hello": "world"})

        with patch(
            "freva_rest.freva_data_portal.utils.Cache.check_connection",
            new=AsyncMock(return_value=None),
        ), patch(
            "freva_rest.freva_data_portal.utils.Cache.get",
            new=AsyncMock(side_effect=[cached, cached]),
        ), patch(
            "freva_rest.freva_data_portal.utils._trigger_loading",
            new=AsyncMock(),
        ) as trigger_loading:
            result = await read_redis_data(token)

        assert result == {"hello": "world"}
        trigger_loading.assert_not_awaited()

    async def test_missing_metadata_triggers_lazy_loading(self) -> None:
        """A missing metadata entry publishes a loading request."""
        assembly = {"mode": "merge"}
        path = ["s3://bucket/source.nc"]
        token = encode_cache_token(path, assembly=assembly)
        ready = _payload(LoadStatus.finished_ok, data={"zarr_format": 2})

        with patch(
            "freva_rest.freva_data_portal.utils.Cache.check_connection",
            new=AsyncMock(return_value=None),
        ), patch(
            "freva_rest.freva_data_portal.utils.Cache.get",
            new=AsyncMock(side_effect=[None, ready]),
        ), patch(
            "freva_rest.freva_data_portal.utils._trigger_loading",
            new=AsyncMock(),
        ) as trigger_loading:
            result = await read_redis_data(token, timeout=0)

        assert result == {"zarr_format": 2}
        trigger_loading.assert_awaited_once_with(
            path,
            token,
            assembly=assembly,
        )

    async def test_failed_metadata_entry_triggers_reload(self) -> None:
        """A previously failed load is submitted again with reload=True."""
        assembly = {"mode": "concat", "dim": "time"}
        path = ["s3://bucket/one.nc", "s3://bucket/two.nc"]
        token = encode_cache_token(path, assembly=assembly)

        failed = _payload(
            LoadStatus.finished_failed,
            reason="previous backend failure",
        )
        ready = _payload(LoadStatus.finished_ok, data={"metadata": {}})

        with patch(
            "freva_rest.freva_data_portal.utils.Cache.check_connection",
            new=AsyncMock(return_value=None),
        ), patch(
            "freva_rest.freva_data_portal.utils.Cache.get",
            new=AsyncMock(side_effect=[failed, ready]),
        ), patch(
            "freva_rest.freva_data_portal.utils._trigger_loading",
            new=AsyncMock(),
        ) as trigger_loading:
            result = await read_redis_data(token, timeout=0)

        assert result == {"metadata": {}}
        trigger_loading.assert_awaited_once_with(
            path,
            token,
            assembly=assembly,
            reload=True,
        )

    async def test_retryable_status_raises_retry_after_on_timeout(self) -> None:
        """Waiting/processing entries raise HTTP 503 with Retry-After."""
        token = encode_cache_token("/work/source.nc", assembly=None)
        waiting = _payload(LoadStatus.waiting)

        with patch(
            "freva_rest.freva_data_portal.utils.Cache.check_connection",
            new=AsyncMock(return_value=None),
        ), patch(
            "freva_rest.freva_data_portal.utils.Cache.get",
            new=AsyncMock(side_effect=[waiting, waiting]),
        ), patch(
            "freva_rest.freva_data_portal.utils._trigger_loading",
            new=AsyncMock(),
        ):
            with pytest.raises(HTTPException) as exc_info:
                await read_redis_data(token, timeout=0)

        assert exc_info.value.status_code == 503
        assert exc_info.value.headers == {"Retry-After": "2"}

    async def test_invalid_token_raises_bad_request(self) -> None:
        """Invalid cache tokens are rejected before any loading is triggered."""
        with patch(
            "freva_rest.freva_data_portal.utils.Cache.check_connection",
            new=AsyncMock(return_value=None),
        ), patch(
            "freva_rest.freva_data_portal.utils._trigger_loading",
            new=AsyncMock(),
        ) as trigger_loading:
            with pytest.raises(HTTPException) as exc_info:
                await read_redis_data("not-a-valid-token")

        assert exc_info.value.status_code == 400
        trigger_loading.assert_not_awaited()

    async def test_chunk_suffix_reads_suffixed_cache_key(self) -> None:
        """Chunk-level reads return data from token + token_suffix."""
        token = encode_cache_token("/work/source.nc", assembly=None)
        token_meta = _payload(LoadStatus.finished_ok, data={"metadata": {}})
        chunk_data = _payload(LoadStatus.finished_ok, data=b"chunk-bytes")

        get_mock = AsyncMock(side_effect=[token_meta, chunk_data])

        with patch(
            "freva_rest.freva_data_portal.utils.Cache.check_connection",
            new=AsyncMock(return_value=None),
        ), patch(
            "freva_rest.freva_data_portal.utils.Cache.get",
            new=get_mock,
        ), patch(
            "freva_rest.freva_data_portal.utils._trigger_loading",
            new=AsyncMock(),
        ) as trigger_loading:
            result = await read_redis_data(
                token,
                token_suffix="-tas-0.0.0",
                timeout=0,
            )

        assert result == b"chunk-bytes"
        assert get_mock.await_args_list[0].args == (token,)
        assert get_mock.await_args_list[1].args == (f"{token}-tas-0.0.0",)
        trigger_loading.assert_not_awaited()
