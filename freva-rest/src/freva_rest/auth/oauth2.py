"""Definition of routes for authentication."""

from typing import Annotated, Any, List, Optional, cast
from urllib.parse import urlparse

import httpx
from fastapi import Form, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from py_oidc_auth import FastApiOIDCAuth, IDToken
from py_oidc_auth.exceptions import InvalidRequest
from py_oidc_auth.schema import PromptField
from py_oidc_auth.utils import get_username as _get_username  # noqa: F401
from pydantic import BaseModel, Field

from ..rest import app, server_config

Required: Any = Ellipsis


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


class TokenisedUser(BaseModel):
    """Tokenised entries of username/userid."""

    pw_name: Annotated[
        str,
        Field(
            description="Username/userid.",
            examples=["janedoe"],
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
            description="Surname of the user the token belongs to."
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


def _http_exc(exc: InvalidRequest) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


auth = FastApiOIDCAuth(
    client_id=server_config.oidc_client_id,
    client_secret=server_config.oidc_client_secret or None,
    discovery_url=server_config.oidc_discovery_url,
    scopes=server_config.oidc_scopes,
    proxy=server_config.proxy,
    claims=server_config.oidc_token_claims or None,
)


async def get_username(
    current_user: Optional[IDToken],
    request: Request,
) -> Any:
    """Extract username from token, falling back to the userinfo endpoint."""
    return await _get_username(current_user, dict(request.headers), auth.config)


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
    id_token: IDToken = auth.required(),
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
async def well_known_url() -> JSONResponse:
    """Get configuration information about the identity provider in use."""
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
        PromptField,
        Query(
            title="Prompt",
            description="Prompt parameter for OIDC login (none or login)",
            examples=["login"],
        ),
    ] = "none",
    offline_access: Annotated[
        bool,
        Query(
            title="Request a long term token.",
            description=(
                "If true, include ``scope=offline_access`` to obtain an "
                "offline refresh token with a long TTL. This must be"
                " supported by the Authentication system."
            ),
        ),
    ] = False,
    scope: Annotated[
        Optional[str],
        Query(
            title="Scope",
            description=(
                "Specify the access level the application needs to request. "
                f"Defaults to {server_config.oidc_scopes}",
            ),
        ),
    ] = None,
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

    if redirect_uri:
        parsed = urlparse(redirect_uri)
        if parsed.hostname == "localhost":
            port = parsed.port or 80
            if port not in server_config.oidc_auth_ports:
                raise HTTPException(
                    status_code=400,
                    detail=f"Port {port} is not in the list of valid auth ports.",
                )
    try:
        auth_url = await auth.login(
            redirect_uri=redirect_uri,
            prompt=prompt,
            offline_access=offline_access,
            scope=scope,
        )
    except InvalidRequest as exc:
        raise _http_exc(exc)
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
) -> Any:
    """Handle the authorization code callback."""
    try:
        return await auth.callback(code=code, state=state)
    except InvalidRequest as exc:
        raise _http_exc(exc)


@app.post(
    "/api/freva-nextgen/auth/v2/device",
    tags=["Authentication"],
)
async def device_flow() -> Any:
    """Start device flow by proxying to the OIDC server's device endpoint."""
    try:
        result = await auth.device_flow()
    except InvalidRequest as exc:
        raise _http_exc(exc)
    return result.model_dump()


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
            alias="refresh-token",
            title="Refresh token",
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
    code_verifier: Annotated[Optional[str], Form()] = None,
) -> Token:
    """Interact with the openID connect endpoint for client authentication."""
    try:
        result = await auth.token(
            "/api/freva-nextgen/auth/v2/callback",
            code=code,
            redirect_uri=redirect_uri,
            refresh_token=refresh_token,
            device_code=device_code,
            code_verifier=code_verifier,
        )
    except InvalidRequest as exc:
        raise _http_exc(exc)
    return cast(Token, result)


@app.get(
    "/api/freva-nextgen/auth/v2/logout",
    tags=["Authentication"],
    responses={
        307: {"description": "Redirect to post-logout URI"},
        400: {"description": "Invalid post_logout_redirect_uri."},
    },
)
async def logout(
    post_logout_redirect_uri: Annotated[
        Optional[str],
        Query(
            title="Post-logout redirect URI",
            description="Where to redirect after logout completes",
        ),
    ] = None,
) -> RedirectResponse:
    """Logout endpoint — redirects to IDP logout if supported, otherwise local redirect.
    """
    target = await auth.logout(post_logout_redirect_uri)
    return RedirectResponse(target)


@app.get("/api/freva-nextgen/auth/v2/userinfo", tags=["Authentication"])
async def userinfo(
    id_token: IDToken = auth.required(),
    request: Request = Required,
) -> UserInfo:
    """Get userinfo for the current token."""
    try:
        lib_user = await auth.userinfo(id_token, dict(request.headers))
    except InvalidRequest as exc:
        raise _http_exc(exc)
    return UserInfo(
        username=lib_user.username,
        last_name=lib_user.last_name,
        first_name=lib_user.first_name,
        email=lib_user.email,
    )


@app.get(
    "/api/freva-nextgen/auth/v2/systemuser",
    include_in_schema=False,
    response_model=TokenisedUser,
)
@app.get(
    "/api/freva-nextgen/auth/v2/checkuser",
    tags=["Authentication"],
    response_model=TokenisedUser,
    response_description="Check if user claim is authorised.",
)
async def system_user(
    id_token: IDToken = auth.required(claims=server_config.oidc_token_claims),
    request: Request = Required,
) -> TokenisedUser:
    """Check user authorisation and get a url-safe version of the username."""
    try:
        lib_user = await auth.userinfo(id_token, dict(request.headers))
    except InvalidRequest as exc:
        raise _http_exc(exc)
    return TokenisedUser(pw_name=lib_user.username)


async def check_token(authorization: Optional[str]) -> IDToken:
    """Validate a Bearer token from a raw Authorization header value.
    """
    bearer = (authorization or "").removeprefix("Bearer ").strip() or None
    try:
        return await auth._get_token(
            bearer,
            effective_claims=server_config.oidc_token_claims or None,
        )
    except InvalidRequest as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
