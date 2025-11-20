"""Load data."""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Union, cast

import h5py
import requests
import xarray as xr
import zarr


def _is_hdf5(path: str, nbytes: int = 32, timeout: float = 30.0) -> bool:
    """HDF5 probe.
    HTTP Range, write small header to tmp file, h5py.is_hdf5
    """
    headers = {"Range": f"bytes=0-{nbytes - 1}"}
    with requests.get(path, headers=headers, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        header = r.raw.read(nbytes)

    fd, tmp_path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(header)
        return cast(bool, h5py.is_hdf5(tmp_path))
    finally:
        os.remove(tmp_path)


def get_xr_engine(file_path: str) -> Optional[str]:
    """Get the engine to open an xarray dataset."""
    try:
        _ = zarr.open(file_path, mode="r")
        return "zarr"
    except Exception:
        pass

    if _is_hdf5(file_path):
        return "h5netcdf"

    return None


def cloud(inp_file: Union[str, Path]) -> xr.Dataset:
    """Open a dataset with xarray."""
    inp_str = str(inp_file)
    engine = get_xr_engine(inp_str)

    kwargs: Dict[str, Any] = {
        "decode_cf": False,
        "use_cftime": False,
        "cache": False,
        "decode_coords": False,
        "engine": engine,
    }
    if engine != "h5netcdf":
        kwargs["chunks"] = "auto"

    return xr.open_dataset(inp_str, **kwargs)
