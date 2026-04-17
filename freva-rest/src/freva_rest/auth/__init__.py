"""Authentication-related endpoints and utilities for the Freva REST API.

Architecture overview
---------------------
The REST API acts as an *auth broker*: it handles all IDP communication
(Keycloak, Entra ID, device flow, auth code flow, etc.) via py-oidc-auth,
but issues its **own** signed JWTs to clients.

Flow:
  1. Client completes any auth flow (device code, auth code) via the
     py-oidc-auth endpoints (``/device/code``, ``/callback``, etc.)
  2. Client POSTs the resulting code / device-code to ``POST /auth/v2/token``.
  3. The API calls the IDP internally, validates the IDP token, mints a
     freva JWT (RS256, aud=freva-api), stores the IDP refresh token in
     MongoDB, and returns the freva JWT as both ``access_token`` and
     ``refresh_token``.
  4. Clients use the freva JWT on all protected endpoints.
  5. When the freva JWT nears expiry the client POSTs it back as
     ``refresh-token`` to the same ``/auth/v2/token`` endpoint. The API
     silently exchanges the stored IDP refresh token for fresh IDP tokens,
     rotates the session, and returns a new freva JWT.

Clients never see or store IDP tokens. The IDP is a pure implementation
detail of this API.
"""

from datetime import datetime, timedelta, timezone
from typing import Annotated, List, Optional, Tuple, Callable

import jwt as pyjwt
from fastapi import Form, Header, HTTPException, Security
from fastapi.responses import JSONResponse
from fastapi.routing import APIRouter
from py_oidc_auth import FastApiOIDCAuth, IDToken
from py_oidc_auth.utils import get_username
from py_oidc_auth.exceptions import InvalidRequest
from py_oidc_auth.schema import Token
from pydantic import BaseModel, Field

from ..config import ServerConfig
from .session_store import SessionStore
from .token_issuer import TokenIssuer

server_config = ServerConfig()

_TOKEN_ENDPOINT = "/api/freva-nextgen/auth/v2/token"
_TOKEN_EXPIRY_SECONDS = 3600

# ---------------------------------------------------------------------------
# py-oidc-auth — IDP broker, used internally only
# ---------------------------------------------------------------------------

auth = FastApiOIDCAuth(
    client_id=server_config.oidc_client_id,
    client_secret=server_config.oidc_client_secret or None,
    discovery_url=server_config.oidc_discovery_url,
    scopes=server_config.oidc_scopes,
    proxy=server_config.proxy,
    claims=server_config.oidc_token_claims or None,
)


# token=None — we own the token endpoint, py-oidc-auth only provides
# the device/auth-code flow endpoints (device/code, callback, etc.)
auth_router: APIRouter = auth.create_auth_router(prefix="/api/freva-nextgen", token="")

# ---------------------------------------------------------------------------
# Freva token issuer and session store
# ---------------------------------------------------------------------------

token_issuer = TokenIssuer(issuer=server_config.proxy, audience="freva-api")
session_store = SessionStore()
# ---------------------------------------------------------------------------
# Misc response models
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


# ---------------------------------------------------------------------------
# Token validation — dependency for all protected endpoints
# ---------------------------------------------------------------------------


async def check_token(
    authorization: Optional[str],
    required_roles: Tuple[str, ...] = (),
) -> IDToken:
    """Validate the access token and apply authorization."""
    bearer = (authorization or "").removeprefix("Bearer ").strip() or None
    if not bearer:
        raise HTTPException(status_code=401, detail="Missing Bearer token.")
    try:
        claims = token_issuer.verify(bearer)
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired.")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    if required_roles and not any(r in claims.roles for r in required_roles):
        raise HTTPException(status_code=403, detail="Insufficient roles.")
    return claims


def token_wrapper(*required_roles: str) -> Callable:
    async def _dep(
        authorization: Annotated[Optional[str], Header()] = None,
    ) -> IDToken:
        return await check_token(authorization, required_roles)

    return _dep


def token_optional_wrapper(*required_roles: str) -> Callable:
    async def _dep(
        authorization: Annotated[Optional[str], Header()] = None,
    ) -> Optional[IDToken]:
        bearer = (authorization or "").strip() or None
        if not bearer:
            return None
        return await check_token(authorization, required_roles)

    return _dep


RequiredUser = Annotated[IDToken, Security(token_wrapper())]
OptionalUser = Annotated[Optional[IDToken], Security(token_optional_wrapper())]


# ---------------------------------------------------------------------------
# Unified token endpoint
# ---------------------------------------------------------------------------


@auth_router.post(
    "/auth/v2/token",
    response_model=Token,
    tags=["Authentication"],
    summary="Obtain or refresh a freva API token",
    responses={
        200: {"description": "Freva JWT issued."},
        401: {"description": "Token or credentials invalid."},
        503: {"description": "IDP unreachable."},
    },
)
async def freva_token(
    code: Annotated[Optional[str], Form()] = None,
    redirect_uri: Annotated[Optional[str], Form()] = None,
    refresh_token: Annotated[Optional[str], Form(alias="refresh-token")] = None,
    device_code: Annotated[Optional[str], Form(alias="device-code")] = None,
    code_verifier: Annotated[Optional[str], Form()] = None,
) -> Token:
    """Unified token endpoint — mirrors the py-oidc-auth ``Token`` interface.

    **Auth code / device code flow**
    Pass ``code`` + ``redirect_uri`` or ``device-code`` as form fields.
    The API calls the IDP internally, validates the IDP token, mints a
    freva JWT, and stores the IDP refresh token in MongoDB.

    **Refresh flow**
    Pass the current freva JWT as ``refresh-token``.
    The API looks up the stored IDP refresh token by ``jti``, silently
    refreshes against the IDP, rotates the session, and returns a new
    freva JWT. The client never handles IDP tokens or IDP refresh tokens.

    In both cases the response ``refresh_token`` field contains the freva
    JWT itself — pass it back here when you need to renew.
    """
    try:
        if refresh_token and not code and not device_code:
            idp_token_obj = await _idp_refresh(refresh_token)
        else:
            idp_token_obj = await auth.token(
                _TOKEN_ENDPOINT,
                code=code,
                redirect_uri=redirect_uri,
                device_code=device_code,
                code_verifier=code_verifier,
            )
    except InvalidRequest as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)

    return await _mint_and_store(idp_token_obj)


# ---------------------------------------------------------------------------
# Supporting endpoints
# ---------------------------------------------------------------------------


@auth_router.get(
    "/auth/v2/.well-known/jwks.json",
    response_class=JSONResponse,
    tags=["Authentication"],
    summary="Freva public key (JWKS)",
)
async def jwks() -> JSONResponse:
    """Expose the public key so external services can verify freva JWTs."""
    return JSONResponse(content=token_issuer.jwks())


class TokenPayload(BaseModel):
    """Model representing the payload of a JWT token."""

    sub: str
    exp: int
    email: Optional[str] = None


# Freva-specific endpoints (not provided by py-oidc-auth)
@auth_router.get(
    "/auth/v2/.well-known/openid-configuration",
    response_class=JSONResponse,
    tags=["Authentication"],
    responses={
        200: {
            "description": "Upstream IDP discovery document.",
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
        503: {"description": "Could not connect to OIDC server."},
    },
)
async def well_known_url() -> JSONResponse:
    """Proxy the identity provider's discovery document."""
    try:
        doc = auth.config.oidc_overview
        doc["jwks_uri"] = (
            f"{server_config.proxy}/api/freva-nextgen/auth/v2/.well-known/jwks.json"
        )
        return JSONResponse(content=doc, status_code=200)
    except Exception as error:
        raise HTTPException(
            status_code=503, detail="Could not connect to OIDC server."
        ) from error


@auth_router.get(
    "/auth/v2/auth-ports",
    response_model=AuthPorts,
    response_description="Pre-defined ports available for the localhost auth flow.",
)
async def valid_ports() -> AuthPorts:
    """Return the valid localhost redirect ports for the auth flow."""
    return AuthPorts(valid_ports=server_config.oidc_auth_ports)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _idp_refresh(freva_jwt: str) -> Token:
    """Resolve the stored IDP refresh token from a freva JWT and refresh."""
    # Accept expired tokens — we only need the jti to find the session
    try:
        jti = token_issuer.verify(freva_jwt).jti
    except pyjwt.ExpiredSignatureError:
        unverified = pyjwt.decode(
            freva_jwt,
            options={"verify_signature": False, "verify_exp": False},
        )
        jti = unverified.get("jti")
    except pyjwt.PyJWTError as exc:
        raise InvalidRequest(401, detail=f"Invalid refresh token: {exc}")

    if not jti:
        raise InvalidRequest(401, detail="Invalid refresh token: missing jti.")

    session = await session_store.get(server_config.mongo_collection_sessions, jti)
    if not session:
        raise InvalidRequest(
            401, detail="Session expired or not found. Please re-authenticate."
        )
    _, idp_refresh_token = session

    # Rotate: delete old session before issuing new one
    await session_store.delete(server_config.mongo_collection_sessions, jti)

    return await auth.token(_TOKEN_ENDPOINT, refresh_token=idp_refresh_token)


async def _mint_and_store(idp_token_obj: Token) -> Token:
    """Validate an IDP Token, mint a freva JWT, persist the session."""
    try:
        idp_claims: IDToken = await auth._get_token(
            idp_token_obj.access_token,
            effective_claims=server_config.oidc_token_claims or None,
        )
    except InvalidRequest as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)

    username = await get_username(
        current_user=idp_claims,
        header={"authorization": f"Bearer {idp_token_obj.access_token}"},
        cfg=auth.config,
    )

    freva_jwt, jti = token_issuer.mint(
        sub=username or idp_claims.sub or "",
        email=getattr(idp_claims, "email", None),
        roles=_extract_roles(idp_claims),
        preferred_username=username,
    )

    await session_store.save(
        collection=server_config.mongo_collection_sessions,
        jti=jti,
        sub=idp_claims.sub,
        refresh_token=idp_token_obj.refresh_token,
        expires_at=idp_token_obj.refresh_expires,
    )

    freva_expires = int(
        (
            datetime.now(tz=timezone.utc) + timedelta(seconds=_TOKEN_EXPIRY_SECONDS)
        ).timestamp()
    )

    # Return freva JWT as both access_token and refresh_token.
    # The client passes it back as refresh-token when renewing.
    return Token(
        access_token=freva_jwt,
        token_type="Bearer",
        expires=freva_expires,
        refresh_token=freva_jwt,
        refresh_expires=idp_token_obj.refresh_expires,
        scope=idp_token_obj.scope,
    )


def _extract_roles(idp_token: IDToken) -> List[str]:
    """Extract roles from an IDToken regardless of IDP encoding."""
    raw = idp_token.model_dump()
    roles: List[str] = []

    # Keycloak: realm_access.roles
    roles += raw.get("realm_access", {}).get("roles") or []
    # Keycloak: resource_access.*.roles (any client)
    for client in (raw.get("resource_access") or {}).values():
        roles += client.get("roles") or []
    # Entra ID / generic: flat roles claim
    roles += raw.get("roles") or []
    # Generic: groups claim
    roles += raw.get("groups") or []

    return list(set(roles))


__all__ = ["OptionalUser", "RequiredUser", "auth", "check_token"]
