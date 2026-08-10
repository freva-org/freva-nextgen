"""Tests that the two chunk-serving paths agree.

The worker serves Zarr chunks through two different code paths:

* ``DataLoadFactory._preload_coordinate_chunks`` eagerly pushes every
  coordinate chunk into the cache when a dataset is converted, and
* ``DataLoadFactory.get_zarr_chunk`` materialises a data chunk on demand
  when a client asks for it.

Both must produce byte-identical output for the same key, because both are
read back through a single ``.zmetadata`` that ``create_zmetadata`` derived
by CF-*encoding* the variable.  The preload path used to slice
``variable.values`` directly, which is only equivalent while the loader
keeps everything undecoded -- there, encoding is a no-op.

Reduction broke that assumption: a reduced dataset carries a decoded
``datetime64`` time axis, so the preload path shipped
nanoseconds-since-1970 under a ``.zattrs`` advertising CF units such as
``hours since 2016-09-02``.  Clients then failed with
``ValueError: unable to decode time units ...``.

These tests pin the invariant rather than the symptom, so the same class of
bug cannot come back for scaled or offset coordinates either.
"""

from __future__ import annotations

import itertools
from typing import Any, Dict

import cloudpickle
import numcodecs
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from xarray.backends.zarr import encode_zarr_variable

from data_portal_worker.load_data import DataLoadFactory
from data_portal_worker.zarr_utils import encode_chunk, get_data_chunk, jsonify_zmetadata


class _CaptureCache:
    """Minimal cache that just records what was written."""

    def __init__(self) -> None:
        self.values: Dict[str, Any] = {}

    def setex(self, key: str, ttl: int, value: Any) -> bool:
        self.values[key] = value
        return True


def _factory(cache: _CaptureCache) -> DataLoadFactory:
    factory = DataLoadFactory()
    factory._cache = cache  # type: ignore[assignment]
    return factory


def _serve_on_demand(ds: xr.Dataset, meta: Dict[str, Any], name: str) -> Dict[str, bytes]:
    """Reproduce what ``get_zarr_chunk`` would return for every chunk."""
    arr_meta = meta["metadata"][f"{name}/.zarray"]
    compressor = (
        numcodecs.get_codec(arr_meta["compressor"])
        if arr_meta.get("compressor")
        else None
    )
    variable = encode_zarr_variable(ds.variables[name], name=name)
    out: Dict[str, bytes] = {}
    ranges = [
        range(-(-s // c)) for s, c in zip(arr_meta["shape"], arr_meta["chunks"])
    ]
    for idx in itertools.product(*ranges):
        chunk_id = ".".join(map(str, idx)) or "0"
        out[chunk_id] = encode_chunk(
            get_data_chunk(
                variable.data, chunk_id, out_shape=arr_meta["chunks"]
            ).tobytes(),
            filters=arr_meta["filters"],
            compressor=compressor,
        )
    return out


def _preloaded(cache: _CaptureCache, token: str, name: str) -> Dict[str, bytes]:
    """Pull the preloaded chunks for ``name`` back out of the cache."""
    prefix = f"{token}-{name}-"
    return {
        key[len(prefix) :]: cloudpickle.loads(value)["data"]
        for key, value in cache.values.items()
        if key.startswith(prefix)
    }


def _decoded_time_dataset() -> xr.Dataset:
    """A dataset whose time axis is decoded, as a reduction leaves it."""
    return xr.Dataset(
        {"tas": (("time",), np.arange(4.0, dtype="float32"))},
        coords={
            "time": pd.date_range("2016-09-02", periods=4, freq="MS"),
            "ref": ("time", np.arange(4.0)),
        },
    )


@pytest.mark.parametrize(
    "coord,dataset",
    [
        ("time", _decoded_time_dataset()),
        (
            "level",
            xr.Dataset(
                {"tas": (("level",), np.arange(3.0))},
                coords={
                    "level": (
                        "level",
                        np.array([1000, 850, 500], dtype="int16"),
                        {"units": "hPa"},
                    )
                },
            ),
        ),
    ],
)
def test_preloaded_coordinate_matches_on_demand_encoding(
    coord: str, dataset: xr.Dataset
) -> None:
    """The two serving paths must produce identical bytes."""
    token = "coord-token"
    cache = _CaptureCache()
    meta = jsonify_zmetadata(dataset)

    _factory(cache)._preload_coordinate_chunks(token, meta, {"root": dataset})

    assert _preloaded(cache, token, coord) == _serve_on_demand(dataset, meta, coord)


def test_decoded_time_coordinate_is_cf_encoded_not_raw() -> None:
    """The specific failure: raw ``datetime64`` under CF ``units``.

    Without encoding, the preloaded bytes are nanoseconds since 1970 while
    ``.zattrs`` advertises an offset from the reference date, so the client
    overflows while decoding.
    """
    token = "time-token"
    dataset = _decoded_time_dataset()
    cache = _CaptureCache()
    meta = jsonify_zmetadata(dataset)

    _factory(cache)._preload_coordinate_chunks(token, meta, {"root": dataset})

    arr_meta = meta["metadata"]["time/.zarray"]
    compressor = (
        numcodecs.get_codec(arr_meta["compressor"])
        if arr_meta.get("compressor")
        else None
    )
    served = _preloaded(cache, token, "time")
    values = np.concatenate(
        [
            np.frombuffer(
                compressor.decode(served[cid]) if compressor else served[cid],
                dtype=arr_meta["dtype"],
            )
            for cid in sorted(served, key=lambda c: int(c.split(".")[0]))
        ]
    )

    # The units are relative to the first step, so offsets start at zero and
    # stay small.  Raw datetime64 would be ~1.47e18.
    assert values[0] == 0
    assert values.max() < 10**7
    np.testing.assert_array_equal(
        values, np.asarray(encode_zarr_variable(dataset.variables["time"]).data)
    )


def test_preload_skips_coordinates_missing_from_the_metadata() -> None:
    """Coordinates without a ``.zarray`` entry are ignored, not fatal."""
    token = "partial-token"
    dataset = _decoded_time_dataset()
    cache = _CaptureCache()
    meta = jsonify_zmetadata(dataset)
    meta["metadata"].pop("ref/.zarray")

    _factory(cache)._preload_coordinate_chunks(token, meta, {"root": dataset})

    assert _preloaded(cache, token, "time")
    assert not _preloaded(cache, token, "ref")


def test_preload_prefixes_group_coordinates() -> None:
    """Non-root groups are cached under their group prefix."""
    token = "group-token"
    dataset = _decoded_time_dataset()
    cache = _CaptureCache()
    meta = jsonify_zmetadata(dataset)
    meta["metadata"] = {
        f"group0/{key}": value for key, value in meta["metadata"].items()
    }

    _factory(cache)._preload_coordinate_chunks(token, meta, {"group0": dataset})

    assert _preloaded(cache, token, "group0/time")
