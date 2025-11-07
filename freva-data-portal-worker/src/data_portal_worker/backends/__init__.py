"""Load data files."""

from urllib.parse import urlparse

import xarray as xr

from .backend import load_data


def load_data(inp_path: str) -> xr.Dataset:
    """Open a datasets."""
    return load_data(inp_path)
