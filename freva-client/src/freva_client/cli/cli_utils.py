"""Utilities for the command line interface."""

from typing import Any, Dict, List

import pandas as pd
import typer
from rich import print as pprint
from rich.console import Console
from rich.table import Table

from freva_client import __version__
from freva_client.utils import logger

APP_NAME: str = "freva-client"


def version_callback(version: bool) -> None:
    """Print the version and exit."""
    if version:
        pprint(f"{APP_NAME}: {__version__}")
        raise typer.Exit()


def parse_cli_args(cli_args: List[str]) -> Dict[str, List[str]]:
    """Convert the cli arguments to a dictionary."""
    logger.debug("parsing command line arguments.")
    kwargs = {}
    for entry in cli_args:
        key, _, value = entry.partition("=")
        if value and key not in kwargs:
            kwargs[key] = [value]
        elif value:
            kwargs[key].append(value)
    logger.debug(kwargs)
    return kwargs


def _summarize(val: Any, max_items: int = 6) -> str:
    """Summarize values for table display, truncating long lists."""
    n = len(val)
    head = ", ".join(map(str, val[:max_items]))
    if n > max_items:
        return f"{head} … (+{n - max_items} more)"
    return head


def print_df(s: pd.Series, max_items: int = 6) -> None:
    """Print a pandas Series as a rich table.

    Parameters
    ----------
    s : pd.Series
        The pandas Series to print.
    max_items : int, optional
        Maximum number of items to display for list-like values,
        by default 6.
    """
    left_col: str = s.index.name or "index"
    right_col: str = s.name or "value"

    table: Table = Table(show_header=True, header_style="bold magenta")
    table.add_column(left_col, style="cyan", no_wrap=True)
    table.add_column(right_col, style="green")

    for key, val in s.items():
        table.add_row(str(key), _summarize(val, max_items=max_items))

    console: Console = Console()
    console.print(table)
