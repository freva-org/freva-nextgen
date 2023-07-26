"""Command line interface (cli) for running the rest server."""

import asyncio
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional

import typer
import uvicorn

from .config import ServerConfig, defaults

cli = typer.Typer(help="Run the databrowser rest API")


@cli.command()
def start(
    config_file: Optional[Path] = typer.Option(
        os.environ.get("API_CONFIG", defaults["API_CONFIG"]),
        help="Path to the server configuration file",
    ),
    port: int = typer.Option(
        os.environ.get("API_PORT", 8080), help="The port the api is running on"
    ),
    dev: bool = typer.Option(False, help="Turn on developing mode"),
    debug: bool = typer.Option(False, help="Turn on debug mode"),
) -> None:
    """Start rest API databrowser server."""

    defaults["API_CONFIG"] = (config_file or defaults["API_CONFIG"]).absolute()
    defaults["DEBUG"] = debug
    cfg = ServerConfig(defaults["API_CONFIG"], debug=dev)
    if dev:
        from .tests.mock import read_data

        for core in cfg.solr_cores:
            asyncio.run(read_data(core, cfg.solr_host, cfg.solr_port))
    workers = {False: int(os.environ.get("API_WORKER", 8)), True: None}
    with NamedTemporaryFile(suffix=".conf", prefix="env") as temp_f:
        Path(temp_f.name).write_text(
            (f"DEBUG={int(debug)}\n" f"API_CONFIG={defaults['API_CONFIG']}"),
            encoding="utf-8",
        )
        uvicorn.run(
            "databrowser.run:app",
            host="0.0.0.0",
            port=port,
            reload=dev,
            log_level=cfg.log_level,
            workers=workers[dev],
            env_file=temp_f.name,
        )


if __name__ == "__main__":
    cli()
