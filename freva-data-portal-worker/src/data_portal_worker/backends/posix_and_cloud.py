"""Load data."""

from pathlib import Path
from typing import Any, Union
from urllib.parse import urlparse

import xarray as xr


def posix_and_cloud(
    inp_file: Union[str, Path], chunk_size: float = 16.0, **kwargs: Any
) -> xr.Dataset:
    """Open a dataset with xarray."""
    engine = "prism"
    inp_str = str(inp_file)
    parsed = urlparse(inp_str)
    target: Union[str, Path]
    target = Path(inp_str) if parsed.scheme in ("", "file") else inp_str
    _ = kwargs.pop("chunks", None)
    for key in ("decode_cf", "use_cftime", "cache", "decode_coords"):
        kwargs[key] = False
    kwargs["chunks"] = "auto"
    kwargs["engine"] = engine
    return xr.open_dataset(target, **kwargs).unify_chunks()
