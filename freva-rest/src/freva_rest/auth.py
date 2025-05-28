"""Definition of routes for authentication."""

import asyncio
import datetime
import secrets
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Dict,
    Literal,
    Optional,
    Union,
    cast,
)
from urllib.parse import urlencode, urljoin

import aiohttp
from fastapi import Depends, Form, HTTPException, Request, Security
from fastapi.responses import RedirectResponse
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBearer,
    SecurityScopes,
)
from fastapi_third_party_auth import Auth, IDToken
from pydantic import BaseModel, Field, ValidationError

from .logger import logger
from .rest import app, server_config
from .utils import get_userinfo

Required: Any = Ellipsis

TIMEOUT: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=5)
"""5 seconds for timeout for key cloak interaction."""


class SafeAuth:
    """
    A wrapper around fastapi_third_party_auth.Auth that safely delays
    initialization until the OIDC discovery URL is reachable.

    This allows FastAPI routes to use the Auth.required() dependency without
    failing at application startup if the OIDC server is temporarily
    unavailable.
    """

    _lock: asyncio.Lock = asyncio.Lock()

    def __init__(self, discovery_url: Optional[str] = None) -> None:
        """
        Initialize the SafeAuth wrapper.

        Parameters:
            discovery_url (str): The full URL to the OIDC discovery document,
                                 e.g., "https://issuer/.well-known/openid-configuration"
        """
        self.discovery_url: str = (discovery_url or "").strip()
        self._auth: Optional[Auth] = None

    async def _check_server_available(self) -> bool:
        """
        Check whether the OIDC server is reachable by requesting the
            discovery document.

        Returns
        -------
            bool: True if the server is up and the document is reachable,
                  False otherwise.
        """
        if not self.discovery_url:
            return False
        try:
            async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
                async with session.get(self.discovery_url) as response:
                    return response.status == 200
        except aiohttp.ClientError:
            return False

    async def _ensure_auth_initialized(self) -> None:
        """
        Initialize the internal Auth instance if the server is available
        and not yet initialized.
        """
        async with self._lock:
            if self._auth is None and await self._check_server_available():
                self._auth = Auth(self.discovery_url)

    def required_dependency(
        self,
    ) -> Callable[
        [SecurityScopes, Optional[HTTPAuthorizationCredentials]],
        Awaitable[IDToken],
    ]:
        """
        Return a FastAPI dependency function to validate a token.

        Returns
        -------
            Callable: A dependency function to use with `Security(...)` in
                      FastAPI routes.

        Raises
        ------
        HTTPException: 503 if the auth server is not available
        """

        async def dependency(
            security_scopes: SecurityScopes,
            authorization_credentials: Optional[
                HTTPAuthorizationCredentials
            ] = Depends(HTTPBearer()),
        ) -> IDToken:
            await self._ensure_auth_initialized()

            if self._auth is None:
                raise HTTPException(
                    status_code=503,
                    detail="OIDC server unavailable, cannot validate token.",
                )

            return self._auth.required(security_scopes, authorization_credentials)

        return dependency


auth = SafeAuth(server_config.oidc_discovery_url)


class UserInfo(BaseModel):
    """Basic user info."""

    username: Annotated[
        str,
        Field(
            title="User name",
            description="Username / uid of the user the token belongs to.",
            min_length=1,
        ),
    ]
    last_name: Annotated[
        str,
        Field(
            title="Last name",
            description="Surename of the user the token belongs to.",
        ),
    ]
    first_name: Annotated[
        str,
        Field(
            title="First Name",
            description="Given name of the person the token belongs to.",
        ),
    ]
    is_guest: Annotated[
        bool,
        Field(
            title="Guest User?",
            description=(
                "Flag that indicates whehter or not the user is "
                "System Security Services Daemon (SSSD)"
            ),
        ),
    ]
    email: Annotated[
        Optional[str],
        Field(
            title="Email",
            description="Email address of the user the token belongs to.",
        ),
    ] = None
    home: Annotated[
        Optional[str],
        Field(
            title="Home Dir",
            description="The home directory of the user - if applicable.",
        ),
    ] = None


class TokenPayload(BaseModel):
    """Model representing the payload of a JWT token."""

    sub: str
    exp: int
    email: Optional[str] = None


class Token(BaseModel):
    """Model representing an OAuth2 token response."""

    access_token: str
    token_type: str
    expires: int
    refresh_token: str
    refresh_expires: int
    scope: str


@app.get("/api/freva-nextgen/auth/v2/status", tags=["Authentication"])
async def get_token_status(
    id_token: IDToken = Security(auth.required_dependency()),
) -> TokenPayload:
    """Check the status of an access token."""
    return cast(TokenPayload, id_token)


async def oicd_request(
    method: Literal["GET", "POST"],
    endpoint: str,
    headers: Optional[Dict[str, str]] = None,
    json: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, str]] = None,
) -> aiohttp.client_reqrep.ClientResponse:
    """Make a request to the openID connect server."""
    async with aiohttp.ClientSession(
        timeout=TIMEOUT, raise_for_status=True
    ) as client:
        try:
            url = server_config.oidc_overview[endpoint]
            logger.info(
                "Making request with data: %s, json: %s, headers: %s to url: %s",
                data,
                json,
                headers,
                url,
            )

            return await client.request(
                method, url, headers=headers, json=json, data=data
            )
        except aiohttp.client_exceptions.ClientResponseError as error:
            logger.error(error)
            raise HTTPException(status_code=401) from error
        except Exception as error:
            logger.error("Could not connect to OIDC server")
            logger.exception(error)
            raise HTTPException(status_code=503) from error


@app.get("/api/freva-nextgen/auth/v2/userinfo", tags=["Authentication"])
async def userinfo(
    id_token: IDToken = Security(auth.required_dependency()),
    request: Request = Required,
) -> UserInfo:
    """Get userinfo for the current token."""
    token_data = {k.lower(): str(v) for (k, v) in dict(id_token).items()}
    try:
        return UserInfo(**get_userinfo(token_data))
    except ValidationError:
        authorization = dict(request.headers)["authorization"]
        response = await oicd_request(
            "GET",
            "userinfo_endpoint",
            headers={"Authorization": authorization},
        )
        token_data = await response.json()
        try:
            return UserInfo(
                **get_userinfo(
                    {k.lower(): str(v) for (k, v) in token_data.items()}
                )
            )
        except ValidationError:
            raise HTTPException(status_code=404)


@app.get(
    "/api/freva-nextgen/auth/v2/.well-known/openid-configuration",
    tags=["Authentication"],
    response_class=RedirectResponse,
)
async def open_id_config() -> RedirectResponse:
    """Get the open id connect configuration."""
    return RedirectResponse(server_config.oidc_discovery_url)


@app.get("/api/freva-nextgen/auth/v2/login")
async def login(request: Request) -> RedirectResponse:
    state = secrets.token_urlsafe(16)
    nonce = secrets.token_urlsafe(16)

    redirect_uri = request.query_params.get("redirect_uri")
    if not redirect_uri:
        raise HTTPException(status_code=400, detail="Missing redirect_uri")

    query = {
        "response_type": "code",
        "client_id": server_config.oidc_client_id,
        "redirect_uri": redirect_uri,
        "scope": "openid profile",
        "state": state,
        "nonce": nonce,
    }
    query = {k: v for (k, v) in query.items() if v}

    auth_url = (
        f"{server_config.oidc_overview['authorization_endpoint']}"
        f"?{urlencode(query)}"
    )
    return RedirectResponse(auth_url)


@app.get("/api/freva-nextgen/auth/v2/callback")
async def callback(request: Request) -> Dict[str, Union[str, int]]:
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code or state")

    try:
        state_token, redirect_uri = state.split("|", 1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid state format")

    data: Dict[str, Optional[str]] = {
        "client_id": server_config.oidc_client_id,
        "client_secret": server_config.oidc_client_secret,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    response = await oicd_request(
        "POST", "token_endpoint", data={k: v for (k, v) in data.items() if v}
    )
    token_data: Dict[str, Union[str, int]] = await response.json()
    return token_data


@app.post("/api/freva-nextgen/auth/v2/token", tags=["Authentication"])
async def fetch_or_refresh_token(
    code: Annotated[
        Optional[str],
        Form(
            title="Authorization Code",
            description=(
                "The code received as part of the OAuth2 authorization "
                "code flow."
            ),
            examples=["abc123xyz"],
        ),
    ] = None,
    redirect_uri: Annotated[
        Optional[str],
        Form(
            title="Redirect URI",
            description=(
                "The URI to which the authorization server will redirect the "
                "user after authentication. It must match one of the URIs "
                "registered with the OAuth2 provider."
            ),
            examples=["http://localhost:8080/callback"],
        ),
    ] = None,
    refresh_token: Annotated[
        Optional[str],
        Form(
            title="Refresh token",
            alias="refresh-token",
            help="The refresh token used to renew the OAuth2 token",
        ),
    ] = None,
) -> Token:
    """Interact with the openID connect endpoint for client authentication."""
    data: Dict[str, Optional[str]] = {
        "client_id": server_config.oidc_client_id,
        "client_secret": server_config.oidc_client_secret,
    }
    if code:
        data["redirect_uri"] = redirect_uri or urljoin(
            server_config.proxy, "/api/freva-nextgen/auth/v2/callback"
        )
        data["grant_type"] = "authorization_code"
        data["code"] = code
    elif refresh_token:
        data["grant_type"] = "refresh_token"
        data["refresh_token"] = refresh_token
    else:
        raise HTTPException(
            status_code=400, detail="Missing code or refresh_token"
        )
    response = await oicd_request(
        "POST",
        "token_endpoint",
        data={k: v for (k, v) in data.items() if v},
    )
    token_data = await response.json()
    expires_at = (
        token_data.get("exp")
        or token_data.get("expires")
        or token_data.get("expires_at")
    )
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    refresh_expires_at = (
        token_data.get("refresh_exp")
        or token_data.get("refresh_expires")
        or token_data.get("refresh_expires_at")
    )
    expires_at = expires_at or now + token_data.get("expires_in", 180)
    refresh_expires_at = refresh_expires_at or now + token_data.get(
        "refresh_expires_in", 180
    )
    return Token(
        access_token=token_data["access_token"],
        token_type=token_data["token_type"],
        expires=int(expires_at),
        refresh_token=token_data["refresh_token"],
        refresh_expires=int(refresh_expires_at),
        scope=token_data["scope"],
    )
