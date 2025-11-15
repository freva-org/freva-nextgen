"""Load data."""

from pathlib import Path
from typing import Optional, Union

import netCDF4
import rasterio
import xarray as xr
import zarr

from data_portal_worker.utils import data_logger

try:
    import cfgrib
except (ImportError, RuntimeError):  # pragma: no cover
    data_logger.warning("Could not import cfgrib, loading GRB files is disabled")


def get_xr_engine(file_path: str) -> Optional[str]:
    """Get the engine, to open the xarray dataset."""
    try:
        with netCDF4.Dataset(file_path, mode="r"):
            return "netcdf4"
    except Exception:
        pass

    try:
        with cfgrib.open_file(file_path):
            return "cfgrib"  # pragma: no cover
    except Exception:
        pass
    try:
        _ = zarr.open(file_path, mode="r")
        return "zarr"
    except Exception:
        pass

    try:
        with rasterio.open(file_path, mode="r"):
            return "rasterio"
    except Exception:
        pass

    return None


def posix(inp_file: Union[str, Path]) -> xr.Dataset:
    """Open a dataset with xarray."""
    inp_str = str(inp_file)
    return xr.open_dataset(
        inp_str,
        decode_cf=False,
        use_cftime=False,
        chunks="auto",
        cache=False,
        decode_coords=False,
        engine=get_xr_engine(str(inp_str))
    )
