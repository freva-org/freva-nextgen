"""Load data files."""

from urllib.parse import urlparse

import xarray as xr

from .posix_and_cloud import posix_and_cloud


def load_data(inp_path: str) -> xr.Dataset:
    """Open an xarray dataset.

    Parameters
    ----------

    inp_path: str
        Uri (Path or URL) to the data object that should be opened.

    Returns
    -------
    xr.Dataset

    """

    parsed_url = urlparse(inp_path)

    implemented_methods = {
        "file": posix_and_cloud,
        "": posix_and_cloud,
        "http": posix_and_cloud,
        "https": posix_and_cloud,
        "s3": posix_and_cloud,
        "gs": posix_and_cloud,
    }
    return implemented_methods.get(parsed_url.scheme, posix_and_cloud)(inp_path)
