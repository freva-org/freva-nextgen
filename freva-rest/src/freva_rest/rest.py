"""## Welcome to the RestAPI for freva 🧉

Freva, the free evaluation system framework, is a data search and analysis
platform developed by the atmospheric science community for the atmospheric
science community. With help of Freva researchers can:

- quickly and intuitively search for data stored at typical data centers that
  host many datasets.
- create a common interface for user defined data analysis tools.
- apply data analysis tools in a reproducible manner.

### Authentication

The API supports token-based authentication using OAuth2. To obtain an access
token, clients can use the `/api/freva/auth/v2/token` endpoint by providing valid
username and password credentials. The access token should then be included in
the Authorization header for secured endpoints.

"""

import asyncio
import os
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.openapi.docs import get_redoc_html
from fastapi.requests import Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from freva_rest import __version__

from .auth import auth_router
from .config import ServerConfig
from .logger import logger, reset_loggers, set_logger_level
from .loop import get_async_model

server_config = ServerConfig()
get_async_model()

metadata_tags = [
    {
        "name": "Data search",
        "description": (
            "The following endpoints can be used to search for data."
            "Search queries can be refined by applying "
            "`key=value` based constraints."
        ),
    },
    {
        "name": "User data",
        "description": (
            "With help of the following endpoints you can add your own data "
            "to the data search system, aka databrwoser."
        ),
    },
    {
        "name": "Load data",
        "description": (
            "With help of the following endpoints you can "
            "conveniently load and access data via `zarr`."
        ),
    },
    {
        "name": "Authentication",
        "description": "These endpoints are for authentication.",
    },
    {
        "name": "System",
        "description": "System utility endpoints for monitoring and diagnostics.",
    },
]

if "stacapi" in server_config.services:
    metadata_tags.append(
        {
            "name": "STAC API",
            "description": (
                "The SpatioTemporal Asset Catalog (STAC) family of specifications"
                " is a community-driven effort to make geospatial data more "
                "discoverable and usable. The STAC API is a standard for "
                "building APIs that provide access to STAC items and "
                "collections. The STAC API is designed to be simple and easy "
                "to use, while also being powerful and flexible enough to "
                "support a wide range of use cases."
            ),
        }
    )


async def refresh_extended_search_cache_periodically(
    stop_event: asyncio.Event,
    interval_seconds: int,
) -> None:
    """Refresh the extended-search cache until shutdown is requested."""
    from .databrowser_api.core import Solr

    while not stop_event.is_set():
        try:
            with set_logger_level("httpx", "httpcore", "freva-rest"):
                for key in ("file", "uri"):
                    await Solr.refresh_extended_search_cache(
                        uniq_key=key, max_results=100
                    )
        except asyncio.CancelledError:  # pragma: no cover
            raise  # pragma: no cover
        except Exception as error:
            logger.warning(
                "Could not refresh extended-search cache: %s",
                error,
                exc_info=True,
            )  # pragma: no cover

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=interval_seconds,
            )
        except asyncio.TimeoutError:  # pragma: no cover
            pass  # pragma: no cover


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start and end things before and after shutdown.

    Things before yield are executed on startup. Things after on teardown.
    """
    cache_stop_event = asyncio.Event()
    cache_refresh_task = asyncio.create_task(
        refresh_extended_search_cache_periodically(
            cache_stop_event,
            300,
        ),
        name="extended-search-cache-refresh",
    )
    try:
        _ = await server_config.mongo_collection_share_key.create_index(
            [("expires_at", 1)],
            expireAfterSeconds=0,
        )
        yield
    finally:
        cache_stop_event.set()
        cache_refresh_task.cancel()
        with suppress(asyncio.CancelledError):
            await cache_refresh_task

        try:  # pragma: no cover
            await server_config.mongo_client.close()
        except Exception as error:
            logger.warning("Could not shutdown mongodb connection: %s", error)


reset_loggers()

app = FastAPI(
    debug=bool(int(os.environ.get("DEBUG", "0"))),
    title="Freva RestAPI",
    version=__version__,
    description=__doc__,
    openapi_url="/api/freva-nextgen/help/openapi.json",
    docs_url=None,
    openapi_tags=metadata_tags,
    lifespan=lifespan,
    contact={"name": "DKRZ, Clint", "email": "freva@dkrz.de"},
    license_info={
        "name": "BSD 2-Clause License",
        "url": "https://opensource.org/license/bsd-2-clause",
        "x-logo": {"url": "https://freva-org.github.io/freva-nextgen/_static/logo.png"},
    },
)

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

app.include_router(auth_router)

# ---------------------------------------------------------------------------
# System
# ---------------------------------------------------------------------------


@app.get("/api/freva-nextgen/help", include_in_schema=False)
async def custom_redoc_ui_html(request: Request) -> HTMLResponse:
    return get_redoc_html(
        openapi_url="/api/freva-nextgen/help/openapi.json",
        title="Freva RestAPI",
        redoc_favicon_url="/favicon.ico",
    )


@app.get("/api/freva-nextgen/ping", tags=["System"], summary="Health check endpoint")
async def ping(request: Request) -> JSONResponse:
    """Health check endpoint that returns
    `pong` when the API is operational."""
    return JSONResponse(
        content={"ping": "pong"},
        status_code=200,
    )


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> FileResponse:
    return FileResponse(Path(__file__).parent / "favicon.ico")
