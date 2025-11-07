"""Load data files."""

import xarray as xr

from .backend import load_every_data


def load_data(inp_path: str) -> xr.Dataset:
    """Open a datasets."""
    return load_every_data(inp_path)
