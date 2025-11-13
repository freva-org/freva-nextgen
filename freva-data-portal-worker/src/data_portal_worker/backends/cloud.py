"""Load data."""

from pathlib import Path
from typing import Any, Dict, Optional, Union

import xarray as xr
import zarr


def get_xr_engine(file_path: str) -> Optional[str]:
    """Get the engine and possibly dataset to open
    the xarray dataset."""
    try:
        _ = zarr.open(file_path, mode="r")
        return "zarr"
    except Exception:
        pass

    try:
        # the cheapest way to check for h5netcdf
        # compatibility via xarray is to keep chunks=None
        # and close the dataset right away
        xr.open_dataset(
            file_path,
            engine="h5netcdf",
            decode_cf=False,
            use_cftime=False,
            chunks=None,
            cache=False,
            decode_coords=False,
        ).close()
        return "h5netcdf"
    except Exception:
        pass

    return None


def cloud(inp_file: Union[str, Path]) -> xr.Dataset:
    """Open a dataset with xarray."""
    inp_str = str(inp_file)
    engine = get_xr_engine(str(inp_str))
    kwargs: Dict[str, Any] = {
        "decode_cf": False,
        "use_cftime": False,
        "cache": False,
        "decode_coords": False,
        "engine": engine
    }
    if engine != "h5netcdf":
        kwargs["chunks"] = "auto"
    return xr.open_dataset(
        inp_str,
        **kwargs
    )
