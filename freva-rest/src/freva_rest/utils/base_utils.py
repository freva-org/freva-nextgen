"""Various utilities for the restAPI."""

import base64
import hmac
import json
import re
import ssl
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import (
    Any,
    Awaitable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import jwt
import redis.asyncio as redis
from fastapi import HTTPException, status
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import RedisError
from typing_extensions import NotRequired, TypedDict

from freva_rest.config import ServerConfig
from freva_rest.logger import logger
from freva_rest.rest import server_config

from .exceptions import EmptyError
from .namegenerator import generate_names, generate_slug

CACHING_SERVICES = set(("zarr-stream",))
"""All the services that need the redis cache."""
CONFIG = ServerConfig()


class PresignDict(TypedDict):
    """The response of the pre sign process."""

    signature: str
    expires_at: datetime
    token: str
    key: str
    assembly: Optional[Dict[str, Optional[str]]]


class CacheKwArgs(TypedDict, total=False):
    """Connection arguments for the cache."""

    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    ssl: bool
    ssl_certfile: Optional[str]
    ssl_keyfile: Optional[str]
    ssl_ca_certs: Optional[str]
    db: int
    ssl_cert_reqs: ssl.VerifyMode
    health_check_interval: int
    retry: Retry
    retry_on_error: List[Type[Exception]]
    retry_on_timeout: bool
    socket_keepalive: bool


class RedisCache(redis.Redis):
    """Define a custom redis cache."""

    def __init__(self, db: int = 0, retry_interval: int = 30) -> None:
        self._kwargs = CacheKwArgs(
            host=CONFIG.redis_url,
            port=CONFIG.redis_port,
            username=CONFIG.redis_user or None,
            password=CONFIG.redis_password or None,
            ssl=(CONFIG.redis_ssl_certfile or None) is not None,
            ssl_certfile=CONFIG.redis_ssl_certfile or None,
            ssl_keyfile=CONFIG.redis_ssl_keyfile or None,
            ssl_ca_certs=CONFIG.redis_ssl_certfile or None,
            ssl_cert_reqs=ssl.CERT_NONE,
            db=db,
            health_check_interval=retry_interval,
            socket_keepalive=True,
            retry=Retry(ExponentialBackoff(cap=10, base=0.1), retries=25),
            retry_on_error=[RedisError, OSError],
            retry_on_timeout=True,
        )
        logger.info("Creating redis connection using: %s", self._kwargs)
        self._connection_checked = False
        super().__init__(**self._kwargs)

    async def check_connection(self) -> None:
        if self._connection_checked is True:
            return None
        if CACHING_SERVICES - set(CONFIG.services or []) == CACHING_SERVICES:
            # All services that would need caching are disabled.
            # If this is the case and we ended up here, we shouldn't be here.
            # tell the users.
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Service not enabled.",
            )
        try:
            await cast(Awaitable[bool], self.ping())
        except Exception as error:
            logger.error("Cloud not connect to redis cache: %s", error)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Cache gone.",
            ) from None
        self._connection_checked = True


Cache = RedisCache()


class SystemUserInfo(TypedDict):
    """Encoded token information."""

    email: NotRequired[str]
    last_name: NotRequired[str]
    first_name: NotRequired[str]
    username: NotRequired[str]


class CacheTokenPayload(TypedDict):
    """The information encoded in a cache token."""

    path: List[str]
    exp: float
    assembly: Optional[Dict[str, Optional[str]]]


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


def str_to_int(inp_str: Optional[str], default: int) -> int:
    """Convert an integer from a string. If it's not working return default."""
    inp_str = inp_str or ""
    try:
        return int(inp_str)
    except ValueError:
        return default


def b64url(data: bytes) -> str:
    """URL-safe base64 without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_decode(s: str) -> bytes:
    """Decode URL-safe base64 with padding."""
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def sign_token_path(
    path: Union[str, List[str]],
    expires_at: float,
    assembly: Optional[Dict[str, Optional[str]]],
) -> Tuple[str, str]:
    """Create a base64 encoded token and a signature of that token."""
    secret = server_config.redis_password
    token = encode_cache_token(path, expires_at, assembly)
    sig = hmac.new(secret.encode("utf-8"), token.encode("utf-8"), sha256).digest()
    return token, b64url(sig)


def encode_cache_token(
    path: Union[str, List[str]],
    expires_at: float = 0.0,
    assembly: Optional[Dict[str, Optional[str]]] = None,
) -> str:
    """Create a URL-safe token that encodes `path` and expiry.

    Returns an opaque id you can embed in a URL or use as "uuid".
    """
    payload = CacheTokenPayload(
        path=path if isinstance(path, list) else [path],
        exp=expires_at,
        assembly=assembly,
    )
    return b64url(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )


def decode_cache_token(token: str) -> CacheTokenPayload:
    """Decode a URL-safe token and return the original path.

    Raises ValueError if token is invalid or expired.
    """
    payload = json.loads(b64url_decode(token))
    return CacheTokenPayload(
        path=payload["path"], exp=payload["exp"], assembly=payload["assembly"]
    )


async def get_token_from_cache(_id: str) -> Tuple[str, str]:
    """Get the token and signature from cache.

    1. Use redis as hot lookup.
    2. Redis has no entries -> MongoDB lookup
    3. Add entry back to redis for hot lookup with updated TTL
    """
    await Cache.check_connection()
    data = json.loads(
        await cast(Awaitable[Optional[str]], Cache.get(_id)) or "{}"
    )
    token, sig = data.get("token"), data.get("signature")
    if token and sig:
        return token, sig
    now = datetime.now(timezone.utc)
    doc = cast(
        PresignDict,
        await server_config.mongo_collection_share_key.find_one({"_id": _id})
        or {},
    )
    expires_at = doc.get("expires_at", now).replace(tzinfo=timezone.utc)
    ttl_remaining = expires_at - now
    if ttl_remaining.total_seconds() <= 0 or not doc:
        await server_config.mongo_collection_share_key.delete_one({"_id": _id})
        raise EmptyError("The shared link has expired or doesn't exist.")
    await Cache.set(
        _id,
        json.dumps(
            {
                "signature": doc["signature"],
                "token": doc["token"],
            }
        ),
    )
    await Cache.expire(_id, int(ttl_remaining.total_seconds()))
    ttl = await Cache.ttl(_id)
    logger.debug("Sig %s was added with a new ttl of %i", _id, ttl)
    return doc["token"], doc["signature"]


async def add_ttl_key_to_db_and_cache(
    path: Union[List[str], str],
    ttl_seconds: float,
    assembly: Optional[Dict[str, Optional[str]]] = None,
) -> PresignDict:
    """Create an entry of a signature."""

    await Cache.check_connection()
    expires_in = timedelta(seconds=ttl_seconds)
    expires_at = datetime.now(timezone.utc) + expires_in
    token, signature = sign_token_path(path, expires_at.timestamp(), assembly)
    _id = generate_slug()
    mapping = {"signature": signature, "token": token, "assembly": assembly}
    doc = cast(
        Optional[PresignDict],
        await server_config.mongo_collection_share_key.find_one({"_id": _id}),
    )
    if not doc or not doc.get("_id"):
        await server_config.mongo_collection_share_key.replace_one(
            {"_id": _id},
            {**{"_id": _id, "expires_at": expires_at}, **mapping},
            upsert=True,
        )
    await Cache.set(_id, json.dumps(mapping))
    await Cache.expire(_id, expires_in)
    ttl = await Cache.ttl(_id)
    logger.debug("Sig %s was added with a ttl of %i", _id, ttl)
    return PresignDict(
        key=f"{_id}/{generate_names()}",
        expires_at=expires_at,
        token=token,
        signature=signature,
        assembly=assembly,
    )
