"""Command line interface for the freva-client library."""

from .databrowser_cli import *  # noqa: F401
from .cli_app import app

__all__ = ["app"]
