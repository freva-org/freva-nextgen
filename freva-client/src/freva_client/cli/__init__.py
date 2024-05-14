"""Command line interface for the freva-client library."""

from .cli_app import app
from .databrowser_cli import *  # noqa: F401

__all__ = ["app"]
