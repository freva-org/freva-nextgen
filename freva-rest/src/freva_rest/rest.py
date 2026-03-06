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

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, AsyncIterator, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.openapi.docs import get_redoc_html
from fastapi.requests import Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from py_oidc_auth import FastApiOIDCAuth, IDToken
from py_oidc_auth.exceptions import InvalidRequest
from pydantic import BaseModel, Field

from freva_rest import __version__

from .config import ServerConfig
from .logger import logger, reset_loggers

server_config = ServerConfig()


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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start and end things before and after shutdown.

    Things before yield are executed on startup. Things after on teardown.
    """
    try:
        _ = await server_config.mongo_collection_share_key.create_index(
            [("expires_at", 1)],
            expireAfterSeconds=0,
        )
        yield
    finally:
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
        "x-logo": {
            "url": "https://freva-org.github.io/freva-nextgen/_static/logo.png"
        },
    },
)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


class AuthPorts(BaseModel):
    """Response for valid auth ports."""

    valid_ports: Annotated[
        List[int],
        Field(
            title="Valid local auth ports.",
            description=(
                "List valid redirect portss that being used for authentication"
                " flow via localhost."
            ),
        ),
    ]


class TokenPayload(BaseModel):
    """Model representing the payload of a JWT token."""

    sub: str
    exp: int
    email: Optional[str] = None


auth = FastApiOIDCAuth(
    client_id=server_config.oidc_client_id,
    client_secret=server_config.oidc_client_secret or None,
    discovery_url=server_config.oidc_discovery_url,
    scopes=server_config.oidc_scopes,
    proxy=server_config.proxy,
    claims=server_config.oidc_token_claims or None,
)

app.include_router(auth.create_auth_router(prefix="/api/freva-nextgen"))


async def check_token(authorization: Optional[str]) -> IDToken:
    """Validate a Bearer token from a raw Authorization header value."""
    bearer = (authorization or "").removeprefix("Bearer ").strip() or None
    try:
        return await auth._get_token(
            bearer,
            effective_claims=server_config.oidc_token_claims or None,
        )
    except InvalidRequest as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)


# Freva-specific endpoints (not provided by py-oidc-auth)
@app.get(
    "/api/freva-nextgen/.well-known/openid-configuration",
    tags=["Authentication"],
    response_class=JSONResponse,
    responses={
        200: {
            "description": ("Metadata for interacting with the OIDC provider."),
            "content": {
                "application/json": {
                    "example": {
                        "issuer": "http://localhost:8080/realms/freva",
                        "authorization_endpoint": "http://localhost:8080/realms/...",
                        "token_endpoint": "http://localhost:8080/realms/...",
                    }
                },
            },
        },
        503: {"description": "Could not connect of OIDC server."},
    },
)
async def well_known_url() -> JSONResponse:
    """Proxy the identity provider's discovery document."""
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(5), follow_redirects=True
        ) as client:
            resp = await client.get(auth.config.discovery_url)
            resp.raise_for_status()
            return JSONResponse(content=resp.json(), status_code=resp.status_code)
    except Exception as error:
        raise HTTPException(
            status_code=503, detail="Could not connect to OIDC server."
        ) from error


@app.get(
    "/api/freva-nextgen/auth/v2/auth-ports",
    tags=["Authentication"],
    response_model=AuthPorts,
    response_description="Pre-defined ports available for the localhost auth flow.",
)
async def valid_ports() -> AuthPorts:
    """Get the open id connect configuration."""
    return AuthPorts(valid_ports=server_config.oidc_auth_ports)


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


@app.get(
    "/api/freva-nextgen/ping", tags=["System"], summary="Health check endpoint"
)
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
