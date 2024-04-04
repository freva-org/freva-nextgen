"""Define the fastAPI rest app."""

import os
from pathlib import Path

from fastapi import FastAPI
from freva_rest import __version__

from .config import ServerConfig, defaults

app = FastAPI(
    debug=bool(int(os.environ.get("DEBUG", "0"))),
    title=defaults["NAME"],
    description="Rest API for the freva framework.",
    version=__version__,
)

server_config = ServerConfig(
    Path(os.environ.get("API_CONFIG", defaults["API_CONFIG"])),
    debug=bool(os.environ.get("DEBUG", int(defaults["DEBUG"]))),
)
