"""Tests for testing the zarr utilities."""

import base64
from unittest.mock import patch

import dask.array as da
import numpy as np
import pytest
import xarray as xr

from data_portal_worker.zarr_utils import (
    encode_chunk,
    encode_fill_value,
    extract_dataarray_coords,
    get_data_chunk,
    normalize_shape,
)


class DummyCodec:
    """Dummy codec class to test the coding."""

    def __init__(self, name="", dtype=float):
        self.name = name
        self.dtype = dtype

    def encode(self, data):
        if isinstance(data, bytes):
            data = np.frombuffer(data, dtype=self.dtype)
        return (data + 1).tobytes()


def test_encode_chunk_with_filters_and_compressor() -> None:
    data = np.array([1, 2, 3])
    filters = [DummyCodec(dtype=data.dtype), DummyCodec(dtype=data.dtype)]
    compressor = DummyCodec(dtype=data.dtype)

    result = encode_chunk(data, filters=filters, compressor=compressor)
    expected = data + 2 + 1

    np.testing.assert_array_equal(
        np.frombuffer(result, dtype=data.dtype), expected
    )


def test_encode_chunk_without_filters() -> None:
    data = np.array([1, 2, 3])
    compressor = DummyCodec(dtype=data.dtype)

    result = encode_chunk(data, filters=None, compressor=compressor)
    expected = data + 1

    np.testing.assert_array_equal(result, expected.tobytes())


def test_encode_chunk_with_object_array_raises() -> None:
    data = np.array(["a", "b", "c"], dtype=object)

    with pytest.raises(RuntimeError, match="cannot write object array"):
        encode_chunk(data)


def test_encode_chunk_without_filters_or_compressor() -> None:
    data = np.array([1, 2, 3])
    result = encode_chunk(data)
    np.testing.assert_array_equal(result, data.tobytes())


def test_get_data_chunk_numpy() -> None:
    da = xr.DataArray(np.arange(6).reshape(2, 3))
    result = get_data_chunk(da, "0.0", (2, 3))
    np.testing.assert_array_equal(result, da.values)


def test_get_data_chunk_numpy_invalid_chunk_id() -> None:
    da = xr.DataArray(np.arange(6).reshape(2, 3))

    with pytest.raises(ValueError, match="Invalid chunk_id"):
        get_data_chunk(da, "1.0", (2, 3))


def test_get_data_chunk_numpy_with_padding() -> None:
    da = xr.DataArray(np.array([[1, 2]]))
    out_shape = (2, 3)
    result = get_data_chunk(da, "0.0", out_shape)

    expected = np.empty(out_shape, dtype=da.dtype)
    expected[:1, :2] = da.values

    np.testing.assert_array_equal(result[:1, :2], expected[:1, :2])


def test_get_data_chunk_dask() -> None:

    darr = da.from_array(np.arange(4).reshape(2, 2), chunks=(1, 2))
    data = xr.DataArray(darr)

    result = get_data_chunk(data.data, "0.0", (1, 2))
    assert isinstance(result, np.ndarray)
    np.testing.assert_array_equal(result, np.array([[0, 1]]))


def test_get_data_chunk_dask_with_padding() -> None:

    darr = da.from_array(np.array([[1, 2]]), chunks=(1, 2))
    data = xr.DataArray(darr)

    result = get_data_chunk(data, "0.0", (2, 3))

    expected = np.empty((2, 3), dtype=data.dtype)
    expected[:1, :2] = np.array([[1, 2]])
    np.testing.assert_array_equal(result[:1, :2], expected[:1, :2])


@patch(
    "data_portal_worker.zarr_utils.encode_zarr_attr_value",
    side_effect=lambda x: f"encoded:{x}",
)
def test_extract_with_nondim_coords(mock_encode) -> None:
    da = xr.DataArray(
        data=[[1, 2], [3, 4]],
        dims=("x", "y"),
        coords={
            "x": [10, 20],
            "y": [100, 200],
            "lat": (("x", "y"), [[1.0, 1.1], [1.2, 1.3]]),
            "lon": (("x", "y"), [[2.0, 2.1], [2.2, 2.3]]),
        },
        name="temperature",
    )

    zattrs = {}
    result = extract_dataarray_coords(da, zattrs)

    # Should encode "lat lon" (sorted) into zattrs["coordinates"]
    assert result["coordinates"] == "encoded:lat lon"
    mock_encode.assert_called_once_with("lat lon")


@patch("data_portal_worker.zarr_utils.encode_zarr_attr_value")
def test_extract_with_only_dim_coords(mock_encode) -> None:
    da = xr.DataArray(
        data=[[1, 2], [3, 4]],
        dims=("x", "y"),
        coords={
            "x": [0, 1],
            "y": [0, 1],
        },
        name="mydata",
    )

    zattrs = {}
    result = extract_dataarray_coords(da, zattrs)

    # Should not add 'coordinates' key
    assert "coordinates" not in result
    mock_encode.assert_not_called()


@patch("data_portal_worker.zarr_utils.encode_zarr_attr_value")
def test_extract_when_name_is_in_coords(mock_encode) -> None:
    da = xr.DataArray(
        data=[[1, 2], [3, 4]],
        dims=("x", "y"),
        coords={
            "x": [0, 1],
            "y": [0, 1],
            "temperature": (("x", "y"), [[5, 6], [7, 8]]),
        },
        name="temperature",
    )

    zattrs = {}
    result = extract_dataarray_coords(da, zattrs)

    # 'temperature' is in coords and matches name => should not encode
    assert "coordinates" not in result
    mock_encode.assert_not_called()


def test_extract_with_no_coords() -> None:
    da = xr.DataArray(data=[1, 2, 3], dims="x", name="a")

    result = extract_dataarray_coords(da, {"foo": "bar"})
    assert result == {"foo": "bar"}


def test_encode_fill_value_datetime() -> None:
    assert encode_fill_value(None, np.dtype("f8")) is None
    assert encode_fill_value(3.14, np.dtype("f8")) == 3.14 and isinstance(
        encode_fill_value(3.14, np.dtype("f8")), float
    )
    assert encode_fill_value(42, np.dtype("i4")) == 42 and isinstance(
        encode_fill_value(42, np.dtype("i4")), int
    )
    assert (
        encode_fill_value(True, np.dtype("bool")) is True
        and encode_fill_value(False, np.dtype("bool")) is False
    )

    assert encode_fill_value(np.nan, np.dtype("f8")) == "NaN"
    assert encode_fill_value(np.inf, np.dtype("f8")) == "Infinity"
    assert encode_fill_value(-np.inf, np.dtype("f8")) == "-Infinity"

    c_result = encode_fill_value(complex(1.0, 2.0), np.dtype("complex128"))
    assert isinstance(c_result, tuple) and c_result == (1.0, 2.0)
    assert encode_fill_value(complex(np.inf, np.nan), np.dtype("complex128")) == (
        "Infinity",
        "NaN",
    )

    test_bytes = b"freva"
    assert encode_fill_value(test_bytes, np.dtype("S5")) == str(
        base64.standard_b64encode(test_bytes), "ascii"
    )
    assert encode_fill_value("freva", np.dtype("U5")) == "freva"
    assert isinstance(
        encode_fill_value(
            np.datetime64("2022-01-01"), np.dtype("datetime64[ns]")
        ),
        int,
    )
    assert encode_fill_value(object(), np.dtype("O")) is not None

    class MockCodec:
        def encode(self, value):
            return b"encoded"

    object_dtype = np.dtype([("f", "O")])
    assert object_dtype.hasobject
    mock_codec = MockCodec()
    result = encode_fill_value("test_obj", object_dtype, mock_codec)
    assert result == str(base64.standard_b64encode(b"encoded"), "ascii")
    with pytest.raises(ValueError, match="missing object_codec for object array"):
        encode_fill_value("test_obj", object_dtype)


def test_normalize_shape() -> None:
    assert normalize_shape(5) == (5,)
    assert normalize_shape(()) == ()
    with pytest.raises(TypeError, match="shape is None"):
        normalize_shape(None)
