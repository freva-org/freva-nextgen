"""
Pre-signed URL utilities for Freva REST.

This module provides helper functions and constants used for creating
and validating pre-signed URLs for Zarr data access.

The signing scheme is an HMAC-SHA256 over:

    METHOD + "\\n" + PATH + "\\n" + EXPIRES

All times are Unix seconds since epoch.
"""

import hmac
import json
import os
import re
import time
from hashlib import sha256
from typing import Any, Dict, Final, cast

from fastapi import (
    HTTPException,
    status,
)

from ..rest import server_config
from ..utils.base_utils import (
    CacheTokenPayload,
    b64url_decode,
    decode_cache_token,
    encode_cache_token,
    get_token_from_cache,
)
from ..utils.exceptions import EmptyError

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


def get_cache_token(path: str) -> str:
    """Extract the uuid from a path."""
    pattern = r"/(?:zarr|zarr-utils)/([A-Za-z0-9_-]+)\.zarr"
    match = re.search(pattern, path)
    if match:
        return match.group(1)
    return ""


def payload_from_url(path: str) -> CacheTokenPayload:
    """Get the token payload from a token."""
    try:
        payload = decode_cache_token(get_cache_token(path))
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The path does not contain a UUID.",
        )
    return payload


async def verify_token(key: str, slug: str) -> Dict[str, str]:
    try:
        token, sig_b64 = await get_token_from_cache(slug)
        payload_bytes = b64url_decode(token)
        payload = cast(Dict[str, Any], json.loads(payload_bytes))
    except EmptyError as error:
        raise HTTPException(status_code=403, detail=str(error))

    except Exception as exc:
        raise HTTPException(
            status_code=400, detail="Invalid share token payload."
        ) from exc
    expected_sig = hmac.new(
        SIGNING_SECRET.encode("utf-8"), token.encode("utf-8"), sha256
    ).digest()
    real_sig_bytes = b64url_decode(sig_b64)

    if not hmac.compare_digest(expected_sig, real_sig_bytes):
        raise HTTPException(
            status_code=403, detail="Invalid share token signature."
        )

    now = int(time.time())
    if now >= int(payload.get("exp", 0)):
        raise HTTPException(status_code=403, detail="Share link has expired.")
    payload["_id"] = encode_cache_token(
        payload.get("path", ""), assembly=payload.get("assembly")
    )
    return payload


def normalise_path(path: str) -> str:
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
