"""Utility functions for loading data."""

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from socket import gethostname
from typing import Optional

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


def str_to_int(inp: Optional[str], default: int) -> int:
    """Convert a string to int."""
    inp = inp or ""
    try:
        return int(inp)
    except (TypeError, ValueError):
        return default
