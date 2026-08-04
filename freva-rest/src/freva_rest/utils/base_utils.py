"""Various utilities for the restAPI."""

import base64
import hmac
import json
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


class ReductionDict(TypedDict, total=False):
    """Dimension-reduction plan carried in a cache token.

    Mixed-typed (unlike ``assembly``) because it carries a boolean, a float
    and a list of dimension names alongside the vocabulary strings.  Values
    are JSON-native so the mapping can be embedded verbatim in the token.
    """

    time_freq: str
    time_method: str
    climatology: bool
    min_coverage: float
    dtype: str
    # Reserved for spatial reduction; the wire format is final already so
    # enabling it later needs no token-format change.
    space: str
    space_dims: List[str]
    weighting: str


class PresignDict(TypedDict):
    """The response of the pre sign process."""

    signature: str
    expires_at: datetime
    token: str
    key: str
    assembly: Optional[Dict[str, Optional[str]]]
    reduce: Optional[ReductionDict]


class CacheKwArgs(TypedDict, total=False):
    """Connection arguments for the cache."""

    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
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
    max_connections: int


class RedisCache(redis.Redis):
    """Define a custom redis cache."""

    def __init__(self, db: int = 0, retry_interval: int = 30, timeout: int = 5) -> None:
        _ssl = (CONFIG.redis_ssl_certfile or None) is not None
        self._kwargs = CacheKwArgs(
            host=CONFIG.redis_url,
            port=CONFIG.redis_port,
            username=CONFIG.redis_user or None,
            password=CONFIG.redis_password or None,
            db=db,
            health_check_interval=retry_interval,
            socket_keepalive=True,
            retry=Retry(ExponentialBackoff(cap=10, base=0.1), retries=25),
            retry_on_error=[RedisError, OSError],
            retry_on_timeout=True,
            max_connections=50,
        )
        ssl_args: CacheKwArgs = {
            "ssl_certfile": CONFIG.redis_ssl_certfile or None,
            "ssl_keyfile": CONFIG.redis_ssl_keyfile or None,
            "ssl_ca_certs": CONFIG.redis_ssl_certfile or None,
            "ssl_cert_reqs": ssl.CERT_NONE,
        }
        if _ssl:
            self._kwargs.update(ssl_args)
        obscure = (
            "username",
            "password",
            "ssl_certfile",
            "ssl_keyfile",
            "ssl_ca_certs",
        )
        conn_info = [
            f"{k}=***" if k in obscure and s else f"{k}={s}"
            for (k, s) in self._kwargs.items()
        ]
        connection_class = redis.Connection if _ssl is False else redis.SSLConnection
        logger.info(
            "Creating redis connection pool using: %s via %s",
            " ".join(conn_info),
            connection_class,
        )
        pool = redis.BlockingConnectionPool(
            timeout=timeout,
            connection_class=connection_class,
            **self._kwargs,
        )

        self._connection_checked = False
        super().__init__(connection_pool=pool)

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
            await self.ping()
        except Exception as error:
            logger.error("Cloud not connect to redis cache: %s", error)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Cache gone.",
            ) from None
        self._connection_checked = True

    async def lpush(self, name: str, *values: bytes) -> int:  # type: ignore[override]
        """Async wrapper matching the event-loop-aware connection pool."""
        return await super().lpush(name, *values)

    async def blpop(  # type: ignore[override]
        self,
        keys: Union[str, List[str]],
        timeout: Union[int, float] = 5.0,
    ) -> Optional[List[bytes]]:
        """Async wrapper matching the event-loop-aware connection pool."""
        return await cast(
            Awaitable[Optional[List[bytes]]],
            super().blpop(keys, timeout=timeout),
        )


Cache = RedisCache()


class SystemUserInfo(TypedDict):
    """Encoded token information."""

    email: NotRequired[str]
    last_name: NotRequired[str]
    first_name: NotRequired[str]
    username: NotRequired[str]


class CacheTokenPayload(TypedDict):
    """The information encoded in a cache token.

    The token *is* the cache key, so everything that changes the bytes the
    client will receive has to be in here.  ``reduce`` in particular: without
    it a reduced and an unreduced view of the same path collide on one key,
    and the lazy re-trigger in ``read_redis_data`` would silently
    re-materialise the unreduced dataset under the reduced dataset's key.
    """

    path: List[str]
    exp: float
    assembly: Optional[Dict[str, Optional[str]]]
    reduce: Optional[ReductionDict]


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
    reduce: Optional[ReductionDict] = None,
) -> Tuple[str, str]:
    """Create a base64 encoded token and a signature of that token."""
    secret = server_config.redis_password
    token = encode_cache_token(path, expires_at, assembly, reduce)
    sig = hmac.new(secret.encode("utf-8"), token.encode("utf-8"), sha256).digest()
    return token, b64url(sig)


#: Sentinel for "this key has no default", so that a legitimate value is
#: never accidentally equal to it.
_MISSING = object()

#: Reduction options whose value carries no information when it equals the
#: default the worker would apply anyway.  Stripping them keeps the cache
#: token canonical: a client that spells out ``time_method="mean"`` must land
#: on the same store as one that leaves it out.  Note that falsy values alone
#: are not enough here -- ``"mean"`` and ``"float32"`` are truthy.
REDUCTION_DEFAULTS: Dict[str, Any] = {
    "time_method": "mean",
    "dtype": "float32",
    "climatology": False,
    "min_coverage": 0.0,
    "weighting": "auto",
}


def canonical_reduction(
    reduce: Optional[ReductionDict],
) -> Optional[ReductionDict]:
    """Reduce a plan to the options that actually change the result.

    Drops entries that are empty or that restate a default, then returns
    ``None`` for a plan that says nothing at all.  This is the single source
    of truth for reduction-plan identity: every route that mints a token goes
    through :func:`encode_cache_token`, which calls this.

    Because defaults are resolved here rather than in the token, changing a
    default changes what old tokens mean.  That is intentional -- the
    alternative is a cache keyed on noise -- but it makes the defaults part
    of the wire contract.
    """
    canonical = {
        key: value
        for key, value in (reduce or {}).items()
        if value and value != REDUCTION_DEFAULTS.get(key, _MISSING)
    }
    return cast(Optional[ReductionDict], canonical or None)


def encode_cache_token(
    path: Union[str, List[str]],
    expires_at: float = 0.0,
    assembly: Optional[Dict[str, Optional[str]]] = None,
    reduce: Optional[ReductionDict] = None,
) -> str:
    """Create a URL-safe token that encodes `path`, plan and expiry.

    Returns an opaque id you can embed in a URL or use as "uuid".

    The reduction plan is canonicalised (see :func:`canonical_reduction`) so
    that an explicitly-defaulted option and an omitted one produce the same
    token; combined with the sorted keys below this keeps tokens canonical.
    """
    payload = CacheTokenPayload(
        path=path if isinstance(path, list) else [path],
        exp=expires_at,
        assembly=assembly,
        reduce=canonical_reduction(reduce),
    )
    return b64url(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )


def decode_cache_token(token: str) -> CacheTokenPayload:
    """Decode a URL-safe token and return the original path.

    Raises ValueError if token is invalid or expired.
    """
    payload = json.loads(b64url_decode(token))
    # ``reduce`` is read with .get() so tokens minted before reduction
    # existed stay decodable.
    return CacheTokenPayload(
        path=payload["path"],
        exp=payload["exp"],
        assembly=payload["assembly"],
        reduce=payload.get("reduce"),
    )


async def get_token_from_cache(_id: str) -> Tuple[str, str]:
    """Get the token and signature from cache.

    1. Use redis as hot lookup.
    2. Redis has no entries -> MongoDB lookup
    3. Add entry back to redis for hot lookup with updated TTL
    """
    await Cache.check_connection()
    data = json.loads(await cast(Awaitable[Optional[str]], Cache.get(_id)) or "{}")
    token, sig = data.get("token"), data.get("signature")
    if token and sig:
        return token, sig
    now = datetime.now(timezone.utc)
    doc = cast(
        PresignDict,
        await server_config.mongo_collection_share_key.find_one({"_id": _id}) or {},
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
    reduce: Optional[ReductionDict] = None,
) -> PresignDict:
    """Create an entry of a signature."""
    await Cache.check_connection()
    expires_in = timedelta(seconds=ttl_seconds)
    expires_at = datetime.now(timezone.utc) + expires_in
    token, signature = sign_token_path(
        path, expires_at.timestamp(), assembly, reduce
    )
    _id = generate_slug()
    mapping = {
        "signature": signature,
        "token": token,
        "assembly": assembly,
        "reduce": reduce,
    }
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
        reduce=reduce,
    )
