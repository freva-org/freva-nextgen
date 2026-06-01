"""Definition of the central logging system."""

import asyncio
import contextvars
import logging
import os
import sys
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import FrozenSet, Iterator, List, Optional, Tuple

import rich.logging
import uvicorn
from rich.console import Console

from .loop import get_async_model


def isatty() -> bool:
    """Check if we are in an interactive env."""
    _isatty = int(getattr(sys.stdout, "isatty", lambda: False)())
    return bool(int(os.getenv("API_USE_TTY", str(_isatty))))


def make_log_handler() -> logging.Handler:
    if "PYTEST_CURRENT_TEST" in os.environ or os.getenv("API_USE_TTY", "1") == "0":
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s %(name)s - %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        return handler
    return rich.logging.RichHandler(
        rich_tracebacks=isatty(),
        show_path=True,
        tracebacks_suppress=[
            get_async_model(),
            asyncio,
            uvicorn,
        ],
        console=Console(
            soft_wrap=False,
            force_jupyter=False,
            stderr=True,
            force_terminal=isatty(),
        ),
    )


class EndpointFilter(logging.Filter):
    """Filter certain logging events from an endpoint."""

    def __init__(self, excluded_endpoints: List[str]) -> None:
        self.excluded = excluded_endpoints
        super().__init__()

    def filter(self, record: logging.LogRecord) -> bool:
        return not any(
            ep in record.getMessage() for ep in self.excluded
        )  # pragma: no cover


class QuietedLoggers(logging.Filter):
    _ctx: contextvars.ContextVar[Optional[Tuple[int, FrozenSet[str]]]] = (
        contextvars.ContextVar("log_floor", default=None)
    )
    _instance: Optional["QuietedLoggers"] = None

    def __new__(cls) -> "QuietedLoggers":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter for loggers in the contestvars."""
        ctx = self._ctx.get()
        if ctx is not None:
            floor, names = ctx
            if record.name in names and record.levelno < floor:
                record.levelno = logging.DEBUG
                record.levelname = logging.getLevelName(logging.DEBUG)
        return True

    @classmethod
    @contextmanager
    def floor(cls, *names: str, level: int = logging.WARNING) -> Iterator[None]:
        """Floor the log level of a given set of loggers to a given level.

        The filter must be attached to the target loggers at startup via
        ``reset_loggers()`` before this context manager has any effect.
        """
        this = cls()
        token = this._ctx.set((level, frozenset(names)))
        try:
            yield
        finally:
            this._ctx.reset(token)


_LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "10": logging.DEBUG,
    "INFO": logging.INFO,
    "20": logging.INFO,
    "WARNING": logging.WARNING,
    "30": logging.WARNING,
    "ERROR": logging.ERROR,
    "40": logging.ERROR,
    "50": logging.CRITICAL,
    "CRITICAL": logging.CRITICAL,
}


THIS_NAME: str = "freva-rest"
DEFAULT_LOG_LEVEL: int = _LOG_LEVELS.get(
    os.getenv("API_LOGLEVEL", "INFO"), logging.INFO
)
LOG_DIR: Path = Path(os.environ.get("API_LOGDIR") or Path(f"/tmp/log/{THIS_NAME}"))

logfmt = "%(name)s - %(message)s"
datefmt = "%Y-%m-%dT%H:%M"

LOG_DIR.mkdir(exist_ok=True, parents=True)
logger_format = logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s - %(message)s", datefmt
)
logger_file_handle = RotatingFileHandler(
    LOG_DIR / f"{THIS_NAME}.log",
    mode="a",
    maxBytes=5 * 1024**2,
    backupCount=2,
    encoding="utf-8",
    delay=False,
)
logger_file_handle.setFormatter(logger_format)
logger_file_handle.setLevel(DEFAULT_LOG_LEVEL)
logger_stream_handle = make_log_handler()
logger_stream_handle.setLevel(DEFAULT_LOG_LEVEL)
logger = logging.getLogger(THIS_NAME)
logger.setLevel(DEFAULT_LOG_LEVEL)


logging.basicConfig(
    level=DEFAULT_LOG_LEVEL,
    format=logfmt,
    datefmt=datefmt,
    handlers=[logger_file_handle, logger_stream_handle],
)


def reset_loggers(level: int = DEFAULT_LOG_LEVEL) -> None:
    """Unify all loggers that we have currently aboard."""
    quieted = QuietedLoggers()
    for name in logging.root.manager.loggerDict.keys():
        log = logging.getLogger(name)
        if name != THIS_NAME:
            log.handlers = []  # let root handle it via propagation
            log.propagate = True
            log.setLevel(level)
        log.addFilter(quieted)
    for name in "topology", "connection":
        logging.getLogger(f"pymongo.{name}").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").addFilter(
        EndpointFilter(["/api/freva-nextgen/ping"])
    )


reset_loggers()
