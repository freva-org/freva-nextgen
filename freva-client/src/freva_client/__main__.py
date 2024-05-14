"""Freva command line interface."""

import sys

from freva_client import cli

__all__ = ["cli"]

if __name__ == "__main__":
    sys.exit(cli.app())
