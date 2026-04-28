"""Authentication-related endpoints and utilities for the Freva REST API."""

import logging
from typing import Annotated, Dict, List, Optional

import httpx
import jmespath
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from py_oidc_auth import FastApiOIDCAuth, MongoDBBrokerStore
from py_oidc_auth.schema import IDToken, Payload
from py_oidc_auth.utils import get_userinfo
from pydantic import BaseModel, Field

from .config import ServerConfig

logger = logging.getLogger(__name__)

server_config = ServerConfig()

auth = FastApiOIDCAuth(
    client_id=server_config.oidc_client_id,
    client_secret=server_config.oidc_client_secret or None,
    discovery_url=server_config.oidc_discovery_url,
    scopes=server_config.oidc_scopes,
    proxy=server_config.proxy,
    claims=server_config.oidc_token_claims or None,
    broker_mode=True,
    broker_audience="freva-nextgen",
    broker_store_obj=MongoDBBrokerStore(
        db=server_config.mongo_client[server_config.mongo_db]
    ),
    trusted_issuers=server_config.oidc_trusted_issuers or [],
)
auth_router = auth.create_auth_router(prefix="/api/freva-nextgen")


_user_claim_check = jmespath.compile(
    server_config.oidc_systemuser_claim.strip() or "preferred_username"
)


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


class SystemUser(BaseModel):
    """Response model for the system user endpoint."""

    username: Annotated[
        str,
        Field(
            title="Username",
            description="Username of the authenticated user.",
        ),
    ]
    email: Annotated[
        str,
        Field(
            title="Email",
            description="Email address of the authenticated user.",
        ),
    ]


async def query_user_info(token_data: IDToken) -> Dict[str, Payload]:
    """Query the user endpoint of the of the IDP server."""
    jti: Optional[str] = (
        str(
            getattr(token_data, "jti", None)
            or (token_data.model_extra or {}).get("jti")
            or ""
        )
        or None
    )
    broker = await auth._ensure_broker_ready()
    user_data = await broker.get_user_info(jti or "")
    return user_data


async def get_system_username(token_data: Optional[IDToken]) -> Optional[str]:
    """Check if a user must be considered as guest."""
    user_info = (await query_user_info(token_data)) if token_data else {}
    return _user_claim_check.search(user_info)


# Freva-specific endpoints (not provided by py-oidc-auth)
@auth_router.get(
    "/auth/v2/systemuser",
    response_model=SystemUser,
    summary="Check if the authenticated user is a valid system user.",
    responses={
        200: {"description": "User is a valid system user."},
        401: {"description": "Missing or invalid token."},
        403: {"description": "User is not part of the required claims; so guest."},
    },
)
async def systemuser(
    current_user: IDToken = auth.required(),
) -> SystemUser:
    """Validate the bearer token against the configured
    ``token_claims`` to recognize the guests.
    """
    payload = await query_user_info(current_user)
    if not payload:
        raise HTTPException(403, detail="Token expired.")
    _user = get_userinfo(payload)
    username = _user_claim_check.search(payload)
    return SystemUser(
        username=username or current_user.preferred_username or "",
        email=_user.get("email", current_user.email or ""),
    )


@auth_router.get(
    "/auth/v2/.well-known/openid-configuration",
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
            doc = resp.json()
            doc["jwks_uri"] = (
                f"{server_config.proxy}/api/freva-nextgen{auth.broker_jwks_path}"
            )
            doc["token_endpoint"] = (
                f"{server_config.proxy}/api/freva-nextgen/auth/v2/token"
            )
            return JSONResponse(content=doc, status_code=resp.status_code)
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
    """Get the open id connect configuration."""
    return AuthPorts(valid_ports=server_config.oidc_auth_ports)
