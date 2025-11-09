"""Load data."""

from pathlib import Path
from typing import Optional, Union
from urllib.parse import urlparse

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

    try:
        with xr.open_dataset(
            file_path,
            engine="h5netcdf",
            decode_cf=False,
            use_cftime=False,
            chunks=None,
            cache=False,
            decode_coords=False,
        ) as _:
            return "h5netcdf"
    except Exception:
        pass

    return None


def posix_and_cloud(inp_file: Union[str, Path]) -> xr.Dataset:
    """Open a dataset with xarray."""
    inp_str = str(inp_file)
    parsed = urlparse(inp_str)
    target: Union[str, Path]
    target = Path(inp_str) if parsed.scheme in ("", "file") else inp_str
    engine = get_xr_engine(str(target))
    return xr.open_dataset(
        target,
        decode_cf=False,
        use_cftime=False,
        chunks="auto" if engine != "h5netcdf" else None,
        cache=False,
        decode_coords=False,
        engine=engine,
    )
