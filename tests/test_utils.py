"""Test various utilities."""

import os.path
from tempfile import TemporaryDirectory

import netCDF4
import numpy as np
import rasterio
import zarr
from unittest.mock import patch
from data_portal_worker.backends.posix import get_xr_engine as get_xr_engine_posix
from data_portal_worker.backends.cloud import get_xr_engine as get_xr_engine_cloud


from data_portal_worker.utils import str_to_int as str_to_int2
from freva_rest.utils.base_utils import get_userinfo, str_to_int


def create_netcdf4_file(temp_dir: str) -> str:
    """Create a netcdf file."""
    temp_file = os.path.join(temp_dir, "out.nc")
    with netCDF4.Dataset(temp_file, "w") as dataset:
        dataset.createDimension("dim", 4)
        var = dataset.createVariable("var", "f4", ("dim",))
        var[:] = [1, 2, 3, 4]
    return temp_file


def create_rasterio_file(temp_dir: str) -> str:
    """Create a geotif file."""
    temp_file = os.path.join(temp_dir, "out.tiff")
    with rasterio.open(
        temp_file,
        "w",
        driver="GTiff",
        width=10,
        height=10,
        count=1,
        dtype="uint8",
    ) as dataset:
        array = np.zeros((10, 10), dtype=np.uint8)
        dataset.write(array, 1)
    return temp_file


def create_zarr_file(temp_dir: str) -> str:
    """Create a zarr dataset."""
    temp = os.path.join(temp_dir, "out.zarr")
    zarr.save(temp, np.array([1, 2, 3, 4]))
    return temp


def test_str_to_int() -> None:
    """Test str to int utility."""
    for func in (str_to_int, str_to_int2):
        assert func(None, 3) == 3
        assert func("a", 3) == 3
        assert func("4", 3) == 4


def test_get_auth_userinfo() -> None:
    """Test getting the authenticated user information."""
    out = get_userinfo(
        {"email": "foo@bar", "lastname": "Doe", "given_name": "Jane"}
    )
    assert out["email"] == "foo@bar"
    assert out["last_name"] == "Doe"
    assert out["first_name"] == "Jane"


def test_get_xr_posix_engine() -> None:
    """Test the right xarray engine."""
    with TemporaryDirectory() as temp_dir:
        assert get_xr_engine_posix(create_netcdf4_file(temp_dir)) == "netcdf4"
        assert get_xr_engine_posix(create_rasterio_file(temp_dir)) == "rasterio"
        assert get_xr_engine_posix(create_zarr_file(temp_dir)) == "zarr"
    with TemporaryDirectory() as temp_dir:
        assert get_xr_engine_posix(temp_dir) is None


def test_get_xr_cloud_engine() -> None:
    """Test the cloud xarray engine."""
    with TemporaryDirectory() as temp_dir:
        zarr_file = create_zarr_file(temp_dir)
        engine = get_xr_engine_cloud(zarr_file)
        assert engine == "zarr"

        nc_file = create_netcdf4_file(temp_dir)
        engine = get_xr_engine_cloud(nc_file)
        assert engine == "h5netcdf"

        engine = get_xr_engine_cloud(temp_dir)
        assert engine is None

    with patch("zarr.open") as mock_zarr:
        mock_zarr.return_value = True
        engine = get_xr_engine_cloud("https://example.com/data.zarr")
        assert engine == "zarr"

    with patch("zarr.open", side_effect=Exception), \
         patch("xarray.open_dataset") as mock_xr:
        mock_xr.return_value.close = lambda: None
        engine = get_xr_engine_cloud("s3://bucket/data.nc")
        assert engine == "h5netcdf"
