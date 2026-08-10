"""Tests for read_redis_data retry and cache behaviour.

These tests focus on the REST-side Redis read helper without starting the
full test server. Redis I/O is patched at the module boundary, while cache
payloads are still encoded exactly as the production code expects.
"""

import json
from typing import Any
from unittest.mock import AsyncMock, patch

import cloudpickle
import pytest
from fastapi import HTTPException

from freva_rest.freva_data_portal.utils import LoadStatus, read_redis_data
from freva_rest.utils.base_utils import (
    REDUCTION_DEFAULTS,
    b64url,
    canonical_reduction,
    decode_cache_token,
    encode_cache_token,
)

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
            reduce=None,
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
            reduce=None,
            reload=True,
        )

    async def test_lazy_reload_preserves_the_reduction_plan(self) -> None:
        """The re-trigger must carry the reduction the token encodes.

        Without it a cache miss on a reduced store would re-materialise the
        *unreduced* dataset under the reduced store's key -- the client would
        get plausible-looking data at the wrong frequency, with no error.
        """
        reduce = {"time_freq": "monthly", "min_coverage": 0.8}
        path = ["s3://bucket/daily.nc"]
        token = encode_cache_token(path, assembly=None, reduce=reduce)
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
            await read_redis_data(token, timeout=0)

        trigger_loading.assert_awaited_once_with(
            path,
            token,
            assembly=None,
            reduce=reduce,
        )

    async def test_failed_reduced_entry_reloads_with_the_plan(self) -> None:
        """The retry-after-failure path must not lose the plan either."""
        reduce = {"time_freq": "seasonal"}
        path = ["s3://bucket/daily.nc"]
        token = encode_cache_token(path, assembly=None, reduce=reduce)
        failed = _payload(LoadStatus.finished_failed, reason="boom")
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
            await read_redis_data(token, timeout=0)

        trigger_loading.assert_awaited_once_with(
            path,
            token,
            assembly=None,
            reduce=reduce,
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


class TestCacheTokenIdentity:
    """The token *is* the cache key, so it must encode the whole request."""

    def test_reduction_changes_the_token(self) -> None:
        path = ["/work/daily.nc"]
        plain = encode_cache_token(path, assembly=None)
        monthly = encode_cache_token(
            path, assembly=None, reduce={"time_freq": "monthly"}
        )
        yearly = encode_cache_token(
            path, assembly=None, reduce={"time_freq": "yearly"}
        )
        climatology = encode_cache_token(
            path,
            assembly=None,
            reduce={"time_freq": "monthly", "climatology": True},
        )
        assert len({plain, monthly, yearly, climatology}) == 4

    def test_defaults_are_canonicalised_away(self) -> None:
        """Spelling out the defaults must not miss the cache.

        Falsy-stripping alone is not enough: ``time_method="mean"`` and
        ``dtype="float32"`` are truthy but say nothing the worker would not
        do anyway.
        """
        path = ["/work/daily.nc"]
        terse = encode_cache_token(
            path, assembly=None, reduce={"time_freq": "monthly"}
        )
        verbose = encode_cache_token(
            path,
            assembly=None,
            reduce={
                "time_freq": "monthly",
                "time_method": "mean",
                "climatology": False,
                "min_coverage": 0.0,
                "dtype": "float32",
            },
        )
        assert terse == verbose

    @pytest.mark.parametrize(
        "plan",
        [
            {"time_freq": "monthly", "time_method": "max"},
            {"time_freq": "monthly", "dtype": "keep"},
            {"time_freq": "monthly", "dtype": "float64"},
            {"time_freq": "monthly", "min_coverage": 0.8},
            {"time_freq": "monthly", "climatology": True},
        ],
    )
    def test_non_default_options_still_change_the_token(
        self, plan: dict
    ) -> None:
        """Canonicalisation must not collapse a real difference."""
        path = ["/work/daily.nc"]
        assert encode_cache_token(path, assembly=None, reduce=plan) != (
            encode_cache_token(path, assembly=None, reduce={"time_freq": "monthly"})
        )

    def test_canonical_reduction_matches_the_worker_defaults(self) -> None:
        """The dropped values must be exactly what the worker assumes.

        Because the defaults are resolved here rather than carried in the
        token, they are part of the wire contract: changing one changes what
        existing tokens mean.
        """
        assert REDUCTION_DEFAULTS == {
            "time_method": "mean",
            "dtype": "float32",
            "climatology": False,
            "min_coverage": 0.0,
            "weighting": "auto",
        }
        assert canonical_reduction({"time_freq": "monthly", "time_method": "mean"}) == (
            {"time_freq": "monthly"}
        )
        assert canonical_reduction({"dtype": "float32"}) is None
        assert canonical_reduction(None) is None

    def test_key_order_does_not_matter(self) -> None:
        path = ["/work/daily.nc"]
        first = encode_cache_token(
            path,
            assembly=None,
            reduce={"time_freq": "monthly", "time_method": "max"},
        )
        second = encode_cache_token(
            path,
            assembly=None,
            reduce={"time_method": "max", "time_freq": "monthly"},
        )
        assert first == second

    def test_empty_plan_matches_no_plan(self) -> None:
        path = ["/work/daily.nc"]
        assert encode_cache_token(path, assembly=None, reduce={}) == (
            encode_cache_token(path, assembly=None)
        )

    def test_reduction_round_trips_through_decoding(self) -> None:
        reduce = {"time_freq": "monthly", "min_coverage": 0.8}
        token = encode_cache_token(["/work/daily.nc"], assembly=None, reduce=reduce)
        payload = decode_cache_token(token)
        assert payload["reduce"] == reduce
        assert payload["path"] == ["/work/daily.nc"]

    def test_legacy_tokens_without_a_plan_still_decode(self) -> None:
        """Tokens minted before reduction existed must keep working."""
        legacy = b64url(
            json.dumps(
                {"path": ["/work/daily.nc"], "exp": 0.0, "assembly": None},
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
        payload = decode_cache_token(legacy)
        assert payload["reduce"] is None
        assert payload["path"] == ["/work/daily.nc"]
