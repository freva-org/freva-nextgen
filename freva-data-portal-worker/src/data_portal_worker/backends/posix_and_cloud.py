"""Load data."""

from pathlib import Path
from typing import Any, Union
from urllib.parse import urlparse

import xarray as xr


def posix_and_cloud(
    inp_file: Union[str, Path], chunk_size: float = 16.0, **kwargs: Any
) -> xr.Dataset:
    """Open a dataset with xarray."""
    inp_str = str(inp_file)
    parsed = urlparse(inp_str)
    target: Union[str, Path]
    target = Path(inp_str) if parsed.scheme in ("", "file") else inp_str
    _ = kwargs.pop("chunks", None)
    kwargs['backend_kwargs'] = { key: False for key in ("decode_cf", "decode_coords")}
    kwargs['cache']=False
    kwargs["chunks"] = "auto"
    return xr.open_dataset(target, engine='prism', **kwargs).unify_chunks()
