"""Freva the Free Evaluation System command line interface."""

from rich import print as pprint
import typer

from freva_client import __version__
from freva_client.utils import logger

APP_NAME: str = "freva-client"

app = typer.Typer(
    name=APP_NAME,
    help=__doc__,
    add_completion=False,
    callback=logger.set_cli,
)


def version_callback(version: bool) -> None:
    """Print the version and exit."""
    if version:
        pprint(f"{APP_NAME}: {__version__}")
        raise typer.Exit()
