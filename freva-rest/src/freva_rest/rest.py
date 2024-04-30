"""Welcome to the RestAPI for freva 🧉
--------------------------------------

Freva, the free evaluation system framework, is a data search and analysis
platform developed by the atmospheric science community for the atmospheric
science community. With help of Freva researchers can:

- quickly and intuitively search for data stored at typical data centers that
  host many datasets.
- create a common interface for user defined data analysis tools.
- apply data analysis tools in a reproducible manner.


"""

import os
from pathlib import Path

from fastapi import FastAPI
from freva_rest import __version__

from .config import ServerConfig, defaults

metadata_tags = [
    {
        "name": "Data search",
        "description": "Search for data based on `key=value` search queries.",
    },
    {"name": "Load data", "description": "Load the data via `zarr` files."},
]

app = FastAPI(
    debug=bool(int(os.environ.get("DEBUG", "0"))),
    title="Freva RestAPI",
    version=__version__,
    description=__doc__,
    openapi_url="/api/storage/docs/openapi.json",
    docs_url="/api/freva/docs",
    redoc_url=None,
    contact={"name": "DKRZ, Clint", "email": "freva@dkrz.de"},
    license_info={
        "name": "BSD 2-Clause License",
        "url": "https://opensource.org/license/bsd-2-clause",
    },
)

server_config = ServerConfig(
    Path(os.environ.get("API_CONFIG", defaults["API_CONFIG"])),
    debug=bool(os.environ.get("DEBUG", int(defaults["DEBUG"]))),
)
