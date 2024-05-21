"""Load data from a posix file system."""

from pathlib import Path
from typing import Optional, Union

import cfgrib
import rasterio
import netCDF4
import h5netcdf
import zarr

import xarray as xr


def get_xr_engine(file_path: str) -> Optional[str]:
    """Get the engine, to open the xarray dataset."""
    try:
        with netCDF4.Dataset(file_path):
            return "netcdf4"
    except:
        pass

    try:
        with cfgrib.open_file(file_path):
            return "cfgrib"
    except:
        pass

    try:
        with rasterio.open(file_path):
            return "rasterio"
    except:
        pass

    try:

        with h5netcdf.File(file_path, "r"):
            return "h5netcdf"
    except:
        pass
    try:
        with zarr.open(file_path):
            return "zarr"
    except:
        pass
    return None


def load_posix(inp_file: Union[str, Path]) -> xr.Dataset:
    """Open a dataset with xarray."""
    inp_file = Path(inp_file)
    return xr.open_dataset(
        inp_file,
        decode_cf=False,
        use_cftime=False,
        chunks="auto",
        cache=False,
        decode_coords=False,
        engine=get_xr_engine(str(inp_file)),
    )
