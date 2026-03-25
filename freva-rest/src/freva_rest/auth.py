"""Authentication-related endpoints and utilities for the Freva REST API."""
from typing import Annotated, List, Optional

import httpx
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from py_oidc_auth import IDToken
from py_oidc_auth.exceptions import InvalidRequest
from pydantic import BaseModel, Field

from .config import ServerConfig
from .rest import app, auth

server_config = ServerConfig()


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
