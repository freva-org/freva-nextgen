"""Definition of routes for authentication."""

import os
from typing import Annotated, Dict, Literal, Optional, cast

import aiohttp
from fastapi import Form, HTTPException, Security
from fastapi.responses import RedirectResponse
from fastapi_third_party_auth import Auth, IDToken
from pydantic import BaseModel, Field

from .logger import logger
from .rest import app, server_config

auth = Auth(openid_connect_url=server_config.oidc_discovery_url)

TIMEOUT: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=5)
"""5 seconds for timeout for key cloak interaction."""


class TokenPayload(BaseModel):
    """Model representing the payload of a JWT token."""

    sub: str
    exp: int
    email: Optional[str] = None


class Token(BaseModel):
    """Model representing an OAuth2 token response."""

    access_token: str
    token_type: str
    expires_in: int
    refresh_token: str
    refresh_expires_in: int
    scope: str
    not_before_policy: Annotated[int, Field(alias="not-before-policy")]


@app.get("/api/auth/v2/status", tags=["Authentication"])
async def get_token_status(
    id_token: IDToken = Security(auth.required),
) -> TokenPayload:
    """Check the status of an access token."""
    return cast(TokenPayload, id_token)


@app.get(
    "/api/auth/v2/.well-known/openid-configuration",
    tags=["Authentication"],
    response_class=RedirectResponse,
)
async def open_id_config() -> RedirectResponse:
    """Get the open id connect configuration."""
    return RedirectResponse(server_config.oidc_discovery_url)


@app.post("/api/auth/v2/token", tags=["Authentication"])
async def fetch_or_refresh_token(
    username: Annotated[
        Optional[str],
        Form(
            title="Username",
            help="Username to create a OAuth2 token.",
        ),
    ] = None,
    password: Annotated[
        Optional[str],
        Form(
            title="Password",
            help="Password to create a OAuth2 token.",
        ),
    ] = None,
    grant_type: Annotated[
        Literal["password", "refresh_token"],
        Form(
            title="Grant type",
            alias="grant_type",
            help="The authorization code grant type.",
        ),
    ] = "password",
    refresh_token: Annotated[
        Optional[str],
        Form(
            title="Refresh token",
            alias="refresh-token",
            help="The refresh token used to renew the OAuth2 token",
        ),
    ] = None,
    client_id: Annotated[
        Optional[str],
        Form(
            title="Client id",
            alias="client_id",
            help="The client id that is used for the refresh token",
        ),
    ] = None,
    client_secret: Annotated[
        Optional[str],
        Form(
            title="Client secret",
            alias="client_secret",
            help="The client secret that is used for the refresh token",
        ),
    ] = None,
) -> Token:
    """Interact with the openID connect endpoint for client authentication."""
    data: Dict[str, Optional[str]] = {
        "client_id": (client_id or "").replace("None", "") or server_config.oidc_client,
        "client_secret": client_secret or os.getenv("OIDC_CLIENT_SECRET", ""),
        "grant_type": grant_type,
    }
    if grant_type == "password":
        data["password"] = password
        data["username"] = username
    else:
        data["refresh_token"] = refresh_token
    async with aiohttp.ClientSession(timeout=TIMEOUT) as client:
        try:
            response = await client.post(
                server_config.oidc_overview["token_endpoint"],
                data={k: v for (k, v) in data.items() if v},
            )
            response.raise_for_status()
        except Exception as error:
            logger.error(error)
            raise HTTPException(status_code=404)
        token_data = await response.json()
    return Token(**token_data)
