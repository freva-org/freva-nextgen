"""Tests for data_portal_worker.zarr_utils covering previously uncovered branches."""

import base64
from unittest.mock import MagicMock

import dask.array as da
import numpy as np
import pytest
import xarray as xr

from data_portal_worker.zarr_utils import (
    encode_chunk,
    encode_fill_value,
    extract_dataarray_coords,
    extract_dataarray_zattrs,
    get_data_chunk,
    normalize_shape,
)

# ---------------------------------------------------------------------------
# extract_dataarray_coords — lines 75-76
# ---------------------------------------------------------------------------


def test_extract_dataarray_coords_with_nondim_coords() -> None:
    """Non-dimension coordinates are added to zattrs under 'coordinates'."""
    da_arr = xr.DataArray(
        [[1, 2], [3, 4]],
        dims=["x", "y"],
        coords={
            "x": [0, 1],
            "y": [0, 1],
            "label": ("x", ["a", "b"]),  # non-dim coord
        },
        name="data",
    )
    zattrs = extract_dataarray_zattrs(da_arr)
    result = extract_dataarray_coords(da_arr, zattrs)
    assert "coordinates" in result
    assert "label" in result["coordinates"]


def test_extract_dataarray_coords_no_nondim_coords() -> None:
    """Only dimension coordinates — 'coordinates' key not added."""
    da_arr = xr.DataArray(
        [1, 2, 3],
        dims=["x"],
        coords={"x": [0, 1, 2]},
        name="data",
    )
    zattrs = extract_dataarray_zattrs(da_arr)
    result = extract_dataarray_coords(da_arr, zattrs)
    assert "coordinates" not in result


# ---------------------------------------------------------------------------
# encode_chunk — lines 185-196
# ---------------------------------------------------------------------------


def test_encode_chunk_with_filters() -> None:
    """Filters are applied before compression."""
    mock_filter = MagicMock()
    mock_filter.encode.return_value = np.array([1, 2, 3], dtype=np.uint8)
    chunk = np.array([1, 2, 3], dtype=np.uint8)
    result = encode_chunk(chunk, filters=[mock_filter])
    mock_filter.encode.assert_called_once()
    assert isinstance(result, bytes)


def test_encode_chunk_with_compressor() -> None:
    """Compressor is applied when provided."""
    mock_compressor = MagicMock()
    mock_compressor.encode.return_value = b"compressed"
    chunk = np.array([1, 2, 3], dtype=np.uint8)
    result = encode_chunk(chunk, compressor=mock_compressor)
    mock_compressor.encode.assert_called_once()
    assert result == b"compressed"


def test_encode_chunk_no_filters_no_compressor() -> None:
    """Without filters or compressor, raw bytes are returned."""
    chunk = np.array([1, 2, 3], dtype=np.uint8)
    result = encode_chunk(chunk)
    assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# get_data_chunk — lines 215-233
# ---------------------------------------------------------------------------


def test_get_data_chunk_dask_array() -> None:
    """get_data_chunk works with a dask array."""
    arr = da.from_array(np.arange(16).reshape(4, 4), chunks=(2, 2))
    result = get_data_chunk(arr, "0.0", out_shape=(2, 2))
    assert result.shape == (2, 2)


def test_get_data_chunk_dask_edge_chunk_padded() -> None:
    """Edge chunks smaller than out_shape are padded."""
    arr = da.from_array(np.arange(6).reshape(2, 3), chunks=(2, 2))
    # chunk "0.1" is the edge chunk with shape (2, 1) but out_shape is (2, 2)
    result = get_data_chunk(arr, "0.1", out_shape=(2, 2))
    assert result.shape == (2, 2)


def test_get_data_chunk_numpy_array() -> None:
    """get_data_chunk works with a plain numpy array (chunk_id must be 0s)."""
    arr = np.array([[1, 2], [3, 4]])
    result = get_data_chunk(arr, "0.0", out_shape=(2, 2))
    assert result.shape == (2, 2)
    np.testing.assert_array_equal(result, arr)


def test_get_data_chunk_numpy_invalid_chunk_id() -> None:
    """Non-zero chunk_id for numpy array raises ValueError."""
    arr = np.array([[1, 2], [3, 4]])
    with pytest.raises(ValueError, match="Invalid chunk_id"):
        get_data_chunk(arr, "1.0", out_shape=(2, 2))


# ---------------------------------------------------------------------------
# encode_fill_value — lines 246-290
# ---------------------------------------------------------------------------


def test_encode_fill_value_none() -> None:
    assert encode_fill_value(None, np.dtype("f4")) is None


def test_encode_fill_value_float_nan() -> None:
    assert encode_fill_value(float("nan"), np.dtype("f4")) == "NaN"


def test_encode_fill_value_float_posinf() -> None:
    assert encode_fill_value(float("inf"), np.dtype("f4")) == "Infinity"


def test_encode_fill_value_float_neginf() -> None:
    assert encode_fill_value(float("-inf"), np.dtype("f4")) == "-Infinity"


def test_encode_fill_value_float_regular() -> None:
    assert encode_fill_value(1.5, np.dtype("f4")) == 1.5


def test_encode_fill_value_uint() -> None:
    assert encode_fill_value(5, np.dtype("u4")) == 5


def test_encode_fill_value_int() -> None:
    assert encode_fill_value(-3, np.dtype("i4")) == -3


def test_encode_fill_value_bool() -> None:
    # np.dtype("b") is int8 — use np.dtype("bool") for actual boolean
    assert encode_fill_value(True, np.dtype("bool")) is True


def test_encode_fill_value_complex() -> None:
    v = 1 + 2j
    result = encode_fill_value(v, np.dtype("c16"))
    assert isinstance(result, tuple)
    assert len(result) == 2


def test_encode_fill_value_bytes() -> None:
    v = b"hello"
    result = encode_fill_value(v, np.dtype("S5"))
    assert result == base64.standard_b64encode(v).decode("ascii")


def test_encode_fill_value_unicode() -> None:
    assert encode_fill_value("hello", np.dtype("U5")) == "hello"


def test_encode_fill_value_datetime() -> None:
    v = np.datetime64("2020-01-01")
    result = encode_fill_value(v, np.dtype("datetime64[ns]"))
    assert isinstance(result, int)


def test_encode_fill_value_timedelta() -> None:
    v = np.timedelta64(1, "D")
    result = encode_fill_value(v, np.dtype("timedelta64[D]"))
    assert isinstance(result, int)


def test_encode_fill_value_object_codec_missing() -> None:
    """Void dtype with object field and no object_codec raises ValueError."""
    dtype = np.dtype([("f", object)])
    with pytest.raises(ValueError, match="missing object_codec"):
        encode_fill_value(b"\x00" * dtype.itemsize, dtype)


def test_encode_fill_value_object_with_codec() -> None:
    """Void dtype with object_codec encodes correctly."""
    mock_codec = MagicMock()
    mock_codec.encode.return_value = b"encoded"
    dtype = np.dtype([("f", object)])
    result = encode_fill_value(b"\x00" * dtype.itemsize, dtype, object_codec=mock_codec)
    assert result == base64.standard_b64encode(b"encoded").decode("ascii")


# ---------------------------------------------------------------------------
# normalize_shape
# ---------------------------------------------------------------------------


def test_normalize_shape_none_raises() -> None:
    with pytest.raises(TypeError, match="shape is None"):
        normalize_shape(None)


def test_normalize_shape_int() -> None:
    assert normalize_shape(5) == (5,)


def test_normalize_shape_tuple() -> None:
    assert normalize_shape((3, 4)) == (3, 4)
