"""Utility functions for loading data."""

import logging
import os
from collections.abc import Mapping
from html import escape
from logging.handlers import RotatingFileHandler
from pathlib import Path
from socket import gethostname
from typing import Dict, List, Optional, TypeAlias, Union

import xarray as xr
from platformdirs import user_log_dir

try:
    from freva_rest.logger import logger  # noqa: F401
except ImportError:
    pass

BASE_NAME = f"data-loader @ {gethostname()}"
logging.basicConfig(
    level="ERROR",
    format="%(name)s - %(asctime)s - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

data_logger = logging.getLogger(BASE_NAME)

data_logger.setLevel(logging.INFO)
log_dir = (
    Path("/var/log" if os.access("/var/log", os.W_OK) else user_log_dir())
    / "data-loader"
)
log_dir = Path(os.getenv("API_LOGDIR") or log_dir)
log_dir.mkdir(exist_ok=True, parents=True)
logger_file_handle = RotatingFileHandler(
    log_dir / "data-loader.log",
    mode="a",
    maxBytes=5 * 1024**2,
    backupCount=5,
    encoding="utf-8",
    delay=False,
)
logger_file_handle.setLevel(logging.INFO)
data_logger.addHandler(logger_file_handle)

XrGroups = Mapping[str, xr.Dataset]
JSONScalar: TypeAlias = Union[str, int, float, bool, None]
JSONValue: TypeAlias = Union[
    JSONScalar,
    list["JSONValue"],
    dict[str, "JSONValue"],
]
JSONObject: TypeAlias = Dict[str, JSONValue]
JSONArray: TypeAlias = List[JSONValue]


def str_to_int(inp: Optional[str], default: int) -> int:
    """Convert a string to int."""
    inp = inp or ""
    try:
        return int(inp)
    except (TypeError, ValueError):
        return default


def xr_repr_html(groups: XrGroups) -> str:
    """
    Return HTML for either:
      - a single xarray.Dataset (native ds._repr_html_())
      - a mapping of named group datasets rendered as <details> accordions

    The returned HTML is self-contained and can be stored/transmitted.
    """
    if not groups:
        return "<div class='xr-groups-empty'><i>(no groups)</i></div>"
    parts: list[str] = []

    parts.append("<div class='xr-groups' style='margin-top:.25rem'>")

    first = True
    for name, ds in groups.items():
        ds_html = ds._repr_html_().replace("xarray.", "")
        is_open = " open" if first else ""
        first = False

        parts.append(
            "<details style='margin:.35rem 0; border:1px solid #ddd; "
            "border-radius:6px; padding:.35rem .5rem'"
            f"{is_open}>"
            "<summary style='cursor:pointer; font-weight:600'>"
            f"Group: {escape(str(name))}</summary>"
            "<div style='margin-top:.35rem'>"
            f"{ds_html}"
            "</div>"
            "</details>"
        )

    parts.append("</div>")
    return "".join(parts)
