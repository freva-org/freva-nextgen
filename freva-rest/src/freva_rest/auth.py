"""Definition of routes for authentication."""

import asyncio
import base64
import datetime
import secrets
from enum import Enum
from pwd import getpwnam
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Union,
    cast,
)
from urllib.parse import urlencode, urljoin

import aiohttp
from fastapi import Depends, Form, HTTPException, Query, Request, Security
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBearer,
    SecurityScopes,
)
from fastapi_third_party_auth import Auth, IDToken
from pydantic import BaseModel, Field, ValidationError

from .logger import logger
from .rest import app, server_config
from .utils.base_utils import get_userinfo, token_field_matches

Required: Any = Ellipsis

TIMEOUT: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=5)
"""5 seconds for timeout for key cloak interaction."""


class DeviceStartResponse(BaseModel):
    """Response class for the device auth flow."""

    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: Optional[str] = None
    expires_in: int
    interval: int = 5


class Prompt(str, Enum):
    none = "none"
    login = "login"
    consent = "consent"
    select_account = "select_account"


class AuthPorts(BaseModel):
    """Response for vaid authports."""

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
        self.timeout = TIMEOUT

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
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(self.discovery_url) as response:
                    return response.status == 200
        except Exception as error:
            logger.debug("Could not connect to %s: %s", self.discovery_url, error)
            return False

    async def _ensure_auth_initialized(self) -> None:
        """
        Initialize the internal Auth instance if the server is available
        and not yet initialized.
        """
        async with self._lock:
            if self._auth is None and await self._check_server_available():
                self._auth = Auth(self.discovery_url)

    def create_auth_dependency(self, required: bool = True) -> Callable[
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
            ] = Depends(HTTPBearer(auto_error=required)),
        ) -> IDToken:
            await self._ensure_auth_initialized()

            if self._auth is None:
                if required:
                    raise HTTPException(
                        status_code=503,
                        detail="OIDC server unavailable, cannot validate token.",
                    )
                else:
                    logger.info(
                        "[Optional Auth]: OIDC server unavailable"
                        ", cannot validate token"
                    )
                    return None

            try:
                claim_check = "oidc.claims" in (security_scopes.scopes or [])
                scopes = [
                    c for c in security_scopes.scopes or [] if c != "oidc.claims"
                ]
                token = self._auth.required(
                    SecurityScopes(scopes or None), authorization_credentials
                )
                if authorization_credentials is not None and claim_check:
                    if not token_field_matches(
                        authorization_credentials.credentials
                    ):
                        raise HTTPException(
                            status_code=401,
                            detail="Insufficient permissions based on token claims.",
                        )
                return token
            except HTTPException:
                if not required:
                    # skip the exception if not required
                    logger.info(
                        "[Optional Auth]: OIDC validation failed,"
                        "but not required for this endpoint"
                    )
                    return None
                raise

        return dependency


def set_request_header(
    client_id: str,
    client_secret: Optional[str],
    data: Dict[str, str],
    header: Dict[str, str],
) -> None:
    """Construct the oidc request header."""
    header["Content-Type"] = "application/x-www-form-urlencoded"
    if client_secret:
        _auth = base64.b64encode(
            f"{server_config.oidc_client_id}:"
            f"{server_config.oidc_client_secret}".encode()
        ).decode()
        header["Authorization"] = f"Basic {_auth}"
    else:
        data["client_id"] = client_id


auth = SafeAuth(server_config.oidc_discovery_url)


class SystemUser(BaseModel):
    """Represents a Unix system user as returned by `pwd.getpwnam`.

    This model maps the standard fields of a user's passwd entry and is suitable
    for serializing system-level account information in APIs. Note that this
    does not include shadow password data (e.g., from `/etc/shadow`), only what
    is available from `/etc/passwd`.
    """

    pw_name: Annotated[
        str, Field(description="Username string", examples=["janedoe"])
    ]
    pw_passwd: Annotated[
        str,
        Field(
            default="x",
            description=(
                "Password field (usually 'x' for shadow " "password entries)"
            ),
            examples=["x"],
        ),
    ]
    pw_uid: Annotated[int, Field(description="User ID (UID)", examples=[1001])]
    pw_gid: Annotated[int, Field(description="Group ID (GID)", examples=[1001])]
    pw_gecos: Annotated[
        str,
        Field(
            description="User's full name or additional info (GECOS field)",
            examples=["Jane Doe"],
        ),
    ]
    pw_dir: Annotated[
        str,
        Field(
            default="",
            description="User's home directory",
            examples=["/home/jane"],
        ),
    ]
    pw_shell: Annotated[
        str,
        Field(
            default="", description="User's login shell", examples=["/bin/bash"]
        ),
    ]


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
    email: Annotated[
        Optional[str],
        Field(
            default=None,
            title="Email",
            description="Email address of the user the token belongs to.",
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


@app.get(
    "/api/freva-nextgen/auth/v2/auth-ports",
    tags=["Authentication"],
    response_model=AuthPorts,
    response_description="Pre-defined ports available for code login flow.",
)
async def valid_ports() -> AuthPorts:
    """Get the open id connect configuration."""
    return AuthPorts(valid_ports=server_config.oidc_auth_ports)


@app.get("/api/freva-nextgen/auth/v2/status", tags=["Authentication"])
async def get_token_status(
    id_token: IDToken = Security(auth.create_auth_dependency()),
) -> TokenPayload:
    """Check the status of an access token."""
    return cast(TokenPayload, id_token)


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
async def well_kown_url() -> JSONResponse:
    """Get configuration information about the identity provider in use."""

    try:
        async with aiohttp.ClientSession(
            timeout=TIMEOUT, raise_for_status=True
        ) as session:
            async with session.get(auth.discovery_url) as res:
                return JSONResponse(
                    content=await res.json(), status_code=res.status
                )
    except Exception as error:
        raise HTTPException(
            status_code=503, detail="Could not connect of OIDC server."
        ) from error


async def oidc_request(
    method: Literal["GET", "POST"],
    endpoint: str,
    headers: Optional[Dict[str, str]] = None,
    json: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
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

            res = await client.request(
                method, url, headers=headers, json=json, data=data
            )
            return cast(Dict[str, Any], await res.json())
        except aiohttp.client_exceptions.ClientResponseError as error:
            logger.warning(error)
            raise HTTPException(status_code=401) from error
        except Exception as error:
            logger.warning("Could not connect to OIDC server")
            logger.exception(error)
            raise HTTPException(status_code=503) from error


@app.get("/api/freva-nextgen/auth/v2/userinfo", tags=["Authentication"])
async def userinfo(
    id_token: IDToken = Security(auth.create_auth_dependency()),
    request: Request = Required,
) -> UserInfo:
    """Get userinfo for the current token."""
    token_data = {k.lower(): str(v) for (k, v) in dict(id_token).items()}
    try:
        return UserInfo(**get_userinfo(token_data))
    except ValidationError:
        authorization = dict(request.headers)["authorization"]
        token_data = await oidc_request(
            "GET",
            "userinfo_endpoint",
            headers={"Authorization": authorization},
        )
        try:
            return UserInfo(
                **get_userinfo(
                    {k.lower(): str(v) for (k, v) in token_data.items()}
                )
            )
        except ValidationError:
            raise HTTPException(status_code=404)


@app.get(
    "/api/freva-nextgen/auth/v2/systemuser",
    tags=["Authentication"],
    response_model=SystemUser,
    response_description="Information about a system user",
)
async def system_user(
    id_token: IDToken = Security(
        auth.create_auth_dependency(), scopes=["oidc.claims"]
    ),
    request: Request = Required,
) -> SystemUser:
    """Return the password database entry for the given user name."""
    keys = ("preferred-username", "user-name", "uid")
    token_data = {k.lower(): str(v) for (k, v) in dict(id_token).items()}
    uid = ""
    for key in keys:
        for _id in set((key, key.replace("-", "_"), key.replace("-", ""))):
            if token_data.get(_id):
                uid = token_data[_id]
        if uid:
            break
    try:
        pw_entry = getpwnam(uid)
    except KeyError:
        raise HTTPException(status_code=401, detail="User unkown.")
    return SystemUser(
        pw_name=pw_entry.pw_name,
        pw_passwd=pw_entry.pw_passwd,
        pw_uid=pw_entry.pw_uid,
        pw_gid=pw_entry.pw_gid,
        pw_gecos=pw_entry.pw_gecos,
        pw_dir=pw_entry.pw_dir,
        pw_shell=pw_entry.pw_shell,
    )


@app.get(
    "/api/freva-nextgen/auth/v2/login",
    tags=["Authentication"],
    response_class=RedirectResponse,
    responses={
        307: {
            "description": (
                "Redirect to the identity provider's login page. "
                "The user must authenticate and will be redirected back to the "
                "callback URL."
            ),
            "content": {"text/html": {"example": "Redirecting to Keycloak..."}},
        },
        400: {"description": "Missing redirect_uri query parameter."},
    },
)
async def login(
    redirect_uri: Annotated[
        Optional[str],
        Query(
            title="Redirect URI",
            description=(
                "The URI to redirect back to after successful login. "
                "Must match the URI registered with your OpenID provider."
            ),
            examples=["http://localhost:8080/callback"],
        ),
    ] = None,
    prompt: Annotated[
        Prompt,
        Query(
            title="Prompt",
            description="Prompt parameter for OIDC login (none or login)",
            examples=["login"],
        ),
    ] = Prompt.none,
    offline_access: bool = Query(
        False,
        title="Request a long term token.",
        description=(
            "If true, include ``scope=offline_access`` to obtain an "
            "offline refresh token with a long TTL. This must be"
            " supported by the Authentication system."
        ),
    ),
) -> RedirectResponse:
    """
    Initiate the OpenID Connect authorization code flow.

    This endpoint redirects the user to the identity provider's login screen.
    It generates and includes `state` and `nonce` parameters to help prevent CSRF
    and replay attacks. After the user logs in, the identity provider will redirect
    back to the provided `redirect_uri` with an authorization code.

    !!! tip
        Normal users should **not call this endpoint directly**. Use the Freva website,
        Python client, or CLI instead. This endpoint is designed for service
        provider (SP) implementations that need to integrate code-based
        authentication flows.
    """
    state = secrets.token_urlsafe(16)
    nonce = secrets.token_urlsafe(16)

    if not redirect_uri:
        raise HTTPException(status_code=400, detail="Missing redirect_uri")

    query = {
        "response_type": "code",
        "client_id": server_config.oidc_client_id,
        "redirect_uri": redirect_uri,
        "scope": (
            "openid profile"
            if offline_access is False
            else "openid profile offline_access"
        ),
        "state": state,
        "nonce": nonce,
        "prompt": prompt.value.replace("none", ""),
    }
    query = {k: v for (k, v) in query.items() if v}
    auth_url = (
        f"{server_config.oidc_overview['authorization_endpoint']}"
        f"?{urlencode(query)}"
    )
    return RedirectResponse(auth_url)


@app.get(
    "/api/freva-nextgen/auth/v2/callback",
    tags=["Authentication"],
    responses={
        200: {
            "description": "OAuth2 token exchange successful.",
            "content": {
                "application/json": {
                    "example": {
                        "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                        "refresh_token": "dGhpc2lzYXJlZnJlc2h0b2tlbg==",
                        "token_type": "bearer",
                        "expires_in": 3600,
                    }
                }
            },
        },
        400: {"description": "Missing or invalid code/state."},
        500: {"description": "Internal server or Keycloak error."},
    },
)
async def callback(
    code: Annotated[
        Optional[str],
        Query(
            title="Authorization Code",
            description=(
                "Temporary code received from the identity provider after "
                "login."
            ),
            examples={
                "example": {
                    "summary": "Typical Authorization Code",
                    "value": "abc123.def456.ghi789",
                }
            },
        ),
    ] = None,
    state: Annotated[
        Optional[str],
        Query(
            title="State Token",
            examples={
                "example": {
                    "summary": "Typical state with redirect_uri",
                    "value": "abcxyz|http://localhost:8080/callback",
                }
            },
            description=(
                "Opaque value combining anti-CSRF state and the redirect URI, "
                "separated by '|'. Returned as-is from the authorization server."
            ),
        ),
    ] = None,
) -> Dict[str, Union[str, int]]:
    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code or state")

    try:
        state_token, redirect_uri = state.split("|", 1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid state format")

    data: Dict[str, str] = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    headers: Dict[str, str] = {}
    set_request_header(
        server_config.oidc_client_id,
        server_config.oidc_client_secret,
        data,
        headers,
    )
    token_data: Dict[str, Union[str, int]] = await oidc_request(
        "POST",
        "token_endpoint",
        data={k: v for (k, v) in data.items() if v},
        headers=headers,
    )
    return token_data


@app.post(
    "/api/freva-nextgen/auth/v2/device",
    tags=["Authentication"],
    response_model=DeviceStartResponse,
)
async def device_flow() -> DeviceStartResponse:
    """Start device flow by proxying to the oicd server's `/auth/v2/device`.

    Returns verification URIs and codes as JSON (no redirects).
    """
    data = {"scope": "openid offline_access"}
    headers: Dict[str, str] = {}
    set_request_header(
        server_config.oidc_client_id,
        server_config.oidc_client_secret,
        data,
        headers,
    )
    js = await oidc_request(
        "POST",
        "device_authorization_endpoint",
        data=data,
        headers=headers,
    )
    for k in ("device_code", "user_code", "verification_uri", "expires_in"):
        if k not in js:
            raise HTTPException(
                502, detail=f"upstream_malformed_response, missing: {k}"
            )
    return DeviceStartResponse(
        device_code=js["device_code"],
        user_code=js["user_code"],
        verification_uri=js["verification_uri"],
        verification_uri_complete=js.get("verification_uri_complete"),
        expires_in=int(js["expires_in"]),
        interval=int(js.get("interval", 5)),
    )


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
    device_code: Annotated[
        Optional[str],
        Form(
            title="Device code",
            alias="device-code",
            help=(
                "The code received as part of the OAuth2 authorization "
                "device code flow."
            ),
            examples=["abc123xyz"],
        ),
    ] = None,
) -> Token:
    """Interact with the openID connect endpoint for client authentication."""
    data: Dict[str, str] = {}
    headers: Dict[str, str] = {}
    if code:
        data["redirect_uri"] = redirect_uri or urljoin(
            server_config.proxy, "/api/freva-nextgen/auth/v2/callback"
        )
        data["grant_type"] = "authorization_code"
        data["code"] = code
    elif refresh_token:
        data["grant_type"] = "refresh_token"
        data["refresh_token"] = refresh_token
    elif device_code:
        data["grant_type"] = "urn:ietf:params:oauth:grant-type:device_code"
        data["device_code"] = device_code
    else:
        raise HTTPException(
            status_code=400, detail="Missing (device) code or refresh_token"
        )
    set_request_header(
        server_config.oidc_client_id,
        server_config.oidc_client_secret,
        data,
        headers,
    )
    token_data = await oidc_request(
        "POST",
        "token_endpoint",
        data={k: v for (k, v) in data.items() if v},
        headers=headers,
    )
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
    try:
        return Token(
            access_token=token_data["access_token"],
            token_type=token_data["token_type"],
            expires=int(expires_at),
            refresh_token=token_data["refresh_token"],
            refresh_expires=int(refresh_expires_at),
            scope=token_data["scope"],
        )
    except KeyError:
        raise HTTPException(status_code=400, detail="Token creation failed.")
