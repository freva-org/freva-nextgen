"""Various utilities for the restAPI."""

import base64
import hmac
import json
import re
import ssl
import time
from hashlib import sha256
from typing import Any, Awaitable, Dict, List, Optional, Tuple, cast

import jwt
import redis.asyncio as redis
from fastapi import HTTPException, status
from typing_extensions import NotRequired, TypedDict

from freva_rest.config import ServerConfig
from freva_rest.logger import logger
from freva_rest.rest import server_config

REDIS_CACHE: Optional[redis.Redis] = None
CACHING_SERVICES = set(("zarr-stream",))
"""All the services that need the redis cache."""
CONFIG = ServerConfig()


class SystemUserInfo(TypedDict):
    """Encoded token inforamation."""

    email: NotRequired[str]
    last_name: NotRequired[str]
    first_name: NotRequired[str]
    username: NotRequired[str]


def token_field_matches(token: str) -> bool:
    """
    Flattens the token[key] value to a string and checks if the regex pattern matches.

    Parameters:
        token (dict): The encoded JWT token.

    Returns:
        bool: True if a match is found, False otherwise.
    """

    def _walk_dict(inp: Any, keys: List[str]) -> Any:
        if not keys or not isinstance(inp, dict) or not inp:
            return inp or ""
        return _walk_dict(inp.get(keys[0]), keys[1:])

    matches: List[bool] = []
    token_data: Dict[str, Any] = {}
    for claim, pattern in (CONFIG.oidc_token_claims or {}).items():
        if not token_data:
            token_data = jwt.decode(token, options={"verify_signature": False})
        value_str = str(_walk_dict(token_data, claim.split(".")))
        for p in pattern:
            matches.append(
                bool(re.search(rf"\b{re.escape(str(p))}\b", value_str))
            )
    return all(matches)


def get_userinfo(
    user_info: Dict[str, str],
) -> SystemUserInfo:
    """Convert a user_info dictionary to the UserInfo Model."""
    output: Dict[str, str] = {}
    keys = {
        "email": ("mail", "email"),
        "username": ("preferred-username", "user-name", "uid"),
        "last_name": ("last-name", "family-name", "name", "surname"),
        "first_name": ("first-name", "given-name"),
    }
    for key, entries in keys.items():
        for entry in entries:
            if user_info.get(entry):
                output[key] = user_info[entry]
                break
            if user_info.get(entry.replace("-", "_")):
                output[key] = user_info[entry.replace("-", "_")]
                break
            if user_info.get(entry.replace("-", "")):
                output[key] = user_info[entry.replace("-", "")]
                break
    # Strip all the middle names
    name = output.get("first_name", "") + " " + output.get("last_name", "")
    output["first_name"] = name.partition(" ")[0]
    output["last_name"] = name.rpartition(" ")[-1]
    return cast(SystemUserInfo, output)


async def create_redis_connection(
    cache: Optional[redis.Redis] = REDIS_CACHE,
) -> redis.Redis:
    """Reuse a potentially created redis connection."""
    kwargs = dict(
        host=CONFIG.redis_url,
        port=CONFIG.redis_port,
        username=CONFIG.redis_user or None,
        password=CONFIG.redis_password or None,
        ssl=(CONFIG.redis_ssl_certfile or None) is not None,
        ssl_certfile=CONFIG.redis_ssl_certfile or None,
        ssl_keyfile=CONFIG.redis_ssl_keyfile or None,
        ssl_ca_certs=CONFIG.redis_ssl_certfile or None,
        db=0,
    )
    if CACHING_SERVICES - set(CONFIG.services or []) == CACHING_SERVICES:
        # All services that would need caching are disabled.
        # If this is the case and we ended up here, we shouldn't be here.
        # tell the users.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not enabled.",
        )

    if cache is None:
        logger.info("Creating redis connection using: %s", kwargs)
    cache = cache or redis.Redis(
        host=CONFIG.redis_url,
        port=CONFIG.redis_port,
        username=CONFIG.redis_user or None,
        password=CONFIG.redis_password or None,
        ssl=(CONFIG.redis_ssl_certfile or None) is not None,
        ssl_certfile=CONFIG.redis_ssl_certfile or None,
        ssl_keyfile=CONFIG.redis_ssl_keyfile or None,
        ssl_ca_certs=CONFIG.redis_ssl_certfile or None,
        ssl_cert_reqs=ssl.CERT_NONE,
        db=0,
    )
    try:
        await cast(Awaitable[bool], cache.ping())
    except Exception as error:
        logger.error("Cloud not connect to redis cache: %s", error)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cache gone.",
        ) from None
    return cache


def str_to_int(inp_str: Optional[str], default: int) -> int:
    """Convert an integer from a string. If it's not working return default."""
    inp_str = inp_str or ""
    try:
        return int(inp_str)
    except ValueError:
        return default


def b64url(data: bytes) -> str:
    """URL-safe base64 with padding."""
    return base64.urlsafe_b64encode(data).decode("ascii")


def b64url_decode(s: str) -> bytes:
    """Decode URL-safe base64 with padding."""
    return base64.urlsafe_b64decode(s)


def sign_token_path(path: str, expires_at: int) -> Tuple[str, str]:
    """Create a base64 endcoded token and a signature of that token."""
    secret = server_config.redis_password
    token = encode_path_token(path, expires_at)
    sig = hmac.new(secret.encode("utf-8"), token.encode("utf-8"), sha256).digest()
    return token, b64url(sig)


def encode_path_token(path: str, expires_at: int = 0) -> str:
    """Create a URL-safe token that encodes `path` and expiry.

    Returns an opaque id you can embed in a URL or use as "uuid".
    """
    payload = {"path": path, "exp": expires_at}
    return b64url(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )


def decode_path_token(token: str) -> str:
    """Decode a URL-safe token and return the original path.

    Raises ValueError if token is invalid or expired.
    """
    payload = json.loads(b64url_decode(token))
    return cast(str, payload.get("path", ""))


async def publish_dataset(
    path: str,
    cache: Optional[redis.Redis] = None,
    public: bool = False,
    ttl_seconds: int = 86400,
    publish: bool = False,
) -> str:
    """Publish a path on disk for zarr conversion to the broker.

    Parameters
    ----------
    path:
        The path that needs to be converted.
    cache:
        An instance of an already established Redis connection.
    public: bool, default: False
        Create a public zarr store.
    ttl_seconds: int, default: 84600
        TTL of the public zarr url, if any
    publish: bool, default: False
        Send the loading instruction to the broker.

    Returns
    -------
    str:
        The url to the converted zarr endpoint

    """
    cache = cache or await create_redis_connection()
    token = encode_path_token(path)
    share_token, sig = sign_token_path(path, int(time.time()) + ttl_seconds)
    api_path = f"{server_config.proxy}/api/freva-nextgen/data-portal"
    path = path.replace("file:///", "/")
    if publish:
        await cache.publish(
            "data-portal",
            json.dumps({"uri": {"path": path, "uuid": token}}).encode("utf-8"),
        )
    if public is True:
        return f"{api_path}/share/{sig}/{share_token}.zarr"
    return f"{api_path}/zarr/{token}.zarr"
