"""Freva command line interface."""

import sys
from freva_client import cli

__all__ = ["app"]

if __name__ == "__main__":
    sys.exit(cli.app())
