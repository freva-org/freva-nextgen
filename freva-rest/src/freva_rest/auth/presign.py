"""
Pre-signed URL support for Freva REST.

This module adds:

* A `POST /api/freva-nextgen/data-portal/presign-url` endpoint:
    - Requires a normal OAuth2 access token.
    - Accepts a target path and TTL.
    - Returns a one-time sharable URL with `expires` and `sig` query params.

* A reusable dependency `verify_presigned_or_oauth` that you can plug into
  specific routes to allow access via either:
    - a normal OAuth2 token, or
    - a valid pre-signed URL.

The signing scheme is an HMAC-SHA256 over:

    METHOD + "\\n" + PATH + "\\n" + EXPIRES

All times are Unix seconds since epoch.
"""

import base64
import hmac
import json
import os
import re
import time
from hashlib import sha256
from typing import Annotated, Dict, Final, cast

from fastapi import (
    HTTPException,
    Request,
    Security,
    status,
)
from fastapi_third_party_auth import IDToken as TokenPayload
from pydantic import AnyHttpUrl, BaseModel, Field

from ..rest import app, server_config
from ..utils.base_utils import (
    decode_path_token,
    encode_path_token,
    sign_token_path,
)
from .oauth2 import auth

# ---------------------------------------------------------------------------
# Settings & helpers
# ---------------------------------------------------------------------------

# The cache password is set once and for. Hence we can use it here as the
# signing secret.
SIGNING_SECRET: Final[str] = server_config.redis_password
MAX_TTL_SECONDS: Final[int] = int(
    os.environ.get("PRESIGN_URL_MAX_TTL", "432000")
)  # max 5 days
MIN_TTL_SECONDS: Final[int] = 60


def path_from_url(path: str) -> str:
    """Extract the uuid from a path."""
    pattern = r"/(?:zarr|zarr-utils)/([A-Za-z0-9_-]+)\.zarr"
    match = re.search(pattern, path)
    if match:
        try:
            path = decode_path_token(match.group(1))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The path does not contain a UUID.",
            )
    return path


def verify_token(token: str, sig_b64: str) -> Dict[str, str]:

    # decode payload
    def pad(s: str) -> str:
        return s + "=" * (-len(s) % 4)

    try:
        payload_bytes = base64.urlsafe_b64decode(pad(token))
        payload = cast(Dict[str, str], json.loads(payload_bytes))
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail="Invalid share token payload."
        ) from exc

    expected_sig = hmac.new(
        SIGNING_SECRET.encode("utf-8"), token.encode("utf-8"), sha256
    ).digest()
    real_sig_bytes = base64.urlsafe_b64decode(pad(sig_b64))

    if not hmac.compare_digest(expected_sig, real_sig_bytes):
        raise HTTPException(
            status_code=403, detail="Invalid share token signature."
        )

    now = int(time.time())
    if now >= int(payload.get("exp", 0)):
        raise HTTPException(status_code=403, detail="Share link has expired.")
    payload["_id"] = encode_path_token(payload.get("path", ""))
    return payload


def _normalise_path(path: str) -> str:
    """Normalise and validate a resource path that may be pre-signed.

    Restrict pre-signing to paths under the Zarr chunk endpoint base.
    """
    allowed_urls = [
        "/api/freva-nextgen/data-portal/zarr/",
    ]
    if not any([url in path for url in allowed_urls]) or ".." in path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only valid Zarr paths can be pre-signed.",
        )
    return path


# ---------------------------------------------------------------------------
# API models
# ---------------------------------------------------------------------------


class PresignUrlRequest(BaseModel):
    """Request body for creating a new pre-signed URL."""

    path: str = Field(
        ...,
        title="Resource path",
        description=(
            "Absolute path of the resource to pre-sign, relative to this API. "
            "Must start with `/api/freva-nextgen/data-portal/zarr/` "
            "and typically points to a single Zarr chunk.\n\n"
            "Example:\n"
            "`/api/freva-nextgen/data-portal/zarr/123e4567.zarr`"
        ),
        examples=["/api/freva-nextgen/data-portal/zarr/123e4567.zarr"],
    )
    ttl_seconds: int = Field(
        600,
        title="Time-to-live (seconds)",
        description=(
            "How long the pre-signed URL should remain valid, in seconds. "
            "Must be between 60 seconds and the configured maximum "
            f"({MAX_TTL_SECONDS} seconds)."
        ),
        ge=MIN_TTL_SECONDS,
        le=MAX_TTL_SECONDS,
        examples=[600, 3600],
    )
    method: str = Field(
        "GET",
        title="HTTP method",
        description=(
            "HTTP method that the URL will be valid for. "
            "Currently only `GET` is supported."
        ),
        pattern="^(?i:get)$",
        examples=["GET"],
    )


class PresignUrlResponse(BaseModel):
    """Response body containing a pre-signed URL."""

    url: Annotated[
        AnyHttpUrl,
        Field(
            title="Pre-signed URL",
            description=(
                "Full URL including `expires` and `sig` query parameters. "
                "Anyone with this URL can access the resource until it expires, "
                "without needing an OAuth2 token."
            ),
        ),
    ]
    token: Annotated[
        str,
        Field(
            title="Token",
            description="URL safe encoded path to the data.",
        ),
    ]
    sig: Annotated[
        str,
        Field(
            title="Signature",
            description="Signature that validates the rquested data.",
        ),
    ]
    expires_at: Annotated[
        int,
        Field(
            title="Expiry timestamp",
            description=(
                "Unix timestamp (seconds since epoch) when the URL "
                "becomes invalid."
            ),
            examples=[int(time.time()) + 600],
        ),
    ]
    method: Annotated[
        str,
        Field(
            title="HTTP method",
            description="HTTP method for which the URL is valid (usually `GET`).",
            examples=["GET"],
        ),
    ]


@app.post(
    "/api/freva-nextgen/data-portal/share-zarr",
    tags=["Authentication"],
    status_code=status.HTTP_201_CREATED,
    summary="Create a pre-signed URL for a Zarr chunk",
    description=(
        "Create a short-lived, shareable pre-signed URL for a specific Zarr "
        "chunk. The caller must authenticate with a normal OAuth2 access "
        "token.\n\n The returned URL includes `expires` and `sig` query "
        "parameters. Anyone who knows the URL can perform a `GET` request on "
        "the target resource until the expiry time is reached, without "
        "needing an access token."
    ),
    responses={
        201: {
            "description": "Pre-signed URL created successfully.",
            "content": {
                "application/json": {
                    "example": {
                        "url": (
                            "https://api.example.org"
                            "/api/freva-nextgen/data-portal/zarr/"
                            "123e4567.zarr"
                            "?expires=1731600000&sig=AbCdEf..."
                        ),
                        "expires_at": int(time.time()) + 600,
                        "method": "GET",
                    }
                }
            },
        },
        400: {"description": "Invalid path or parameters."},
        401: {"description": "Missing or invalid OAuth2 access token."},
        403: {
            "description": "Authenticated user is not allowed to pre-sign this "
            "resource."
        },
    },
)
def create_presigned_url(
    request: Request,
    body: PresignUrlRequest,
    token: TokenPayload = Security(
        auth.create_auth_dependency(),
        scopes=["oidc.claims"],
    ),
) -> PresignUrlResponse:
    """Create a new pre-signed URL.

    This endpoint is intended for authenticated users who want to share
    short-lived links to individual Zarr chunks. Authorisation rules for
    *who may pre-sign which chunk* can be implemented based on `token`.
    """
    path = path_from_url(_normalise_path(str(body.path)))
    # TODO: we should check if the user is allowed to read the dataset.
    ttl = max(MIN_TTL_SECONDS, min(body.ttl_seconds, MAX_TTL_SECONDS))
    expires_at = int(time.time()) + ttl
    token, sign = sign_token_path(path, expires_at)
    url = (
        f"{server_config.proxy}/api/freva-nextgen/data-portal/share/"
        f"{sign}/{token}.zarr"
    )
    return PresignUrlResponse(
        url=cast(AnyHttpUrl, url),
        token=token,
        sig=sign,
        expires_at=expires_at,
        method=body.method.upper(),
    )
