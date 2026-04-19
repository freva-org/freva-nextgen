"""Freva JWT issuance and verification.

The signing key is stored in MongoDB so all uvicorn workers share the same
key without any filesystem coordination. It is loaded once at startup and
cached in memory — no per-request DB round-trips.

If no key exists yet (first startup), one is generated and persisted.

Federation
----------
Trusted peer instances (configured via ``API_TRUSTED_ISSUERS``) expose their
public keys at ``/api/freva-nextgen/auth/v2/.well-known/jwks.json``. On
startup these keys are fetched from peers, persisted to MongoDB, and cached
in memory. This means:

* Workers that start after peers are unreachable still load keys from MongoDB.
* Keys survive restarts without re-fetching every time.
* All uvicorn workers share the same peer key store via MongoDB.

If a token arrives signed with an unknown ``kid``, a lazy sync refresh is
attempted using sync httpx and sync pymongo (rate-limited by cooldown).
"""

import hashlib
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypedDict, cast

import httpx
import jwt
import pymongo
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from jwt.algorithms import RSAAlgorithm
from py_oidc_auth import IDToken
from pymongo.asynchronous.collection import AsyncCollection

from ..config import ServerConfig
from ..logger import logger

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

ALGORITHM = "RS256"
_KEY_DOC_ID = "freva-signing-key"
_PEER_KEY_DOC_PREFIX = "peer-jwks:"  # e.g. "peer-jwks:https://freva-b.dkrz.de"
_PEER_JWKS_PATH = "/api/freva-nextgen/auth/v2/.well-known/jwks.json"
_PEER_REFRESH_COOLDOWN = 60.0  # seconds between refresh attempts per peer

TOKEN_EXPIRY_SECONDS = 3600
TOKEN_ENDPOINT = "/api/freva-nextgen/auth/v2/token"
DEVICE_TOKEN_EXPIRY_SECONDS = 2592000


class JWKDict(TypedDict):
    kty: str
    n: str
    e: str
    kid: str
    use: str
    alg: str


class JWKSDict(TypedDict):
    keys: list[JWKDict]


class TokenIssuer:
    """Issues and verifies Freva RS256 JWTs.

    The RSA key pair lives in MongoDB (``freva_keys`` collection). All
    workers load the same key at startup and cache it in memory.

    Trusted peer instances are configured via
    ``server_config.oidc_trusted_issuers`` Their public keys are fetched at
    startup, persisted to MongoDB, and cached in memory.
    Unknown ``kid`` values trigger a lazy sync refresh using sync httpx and
    sync pymongo (rate-limited by cooldown).

    Usage::

        issuer = TokenIssuer(issuer="https://freva-api.dkrz.de")
        await issuer.setup(server_config.mongo_collection_keys)
        await issuer.load_peer_keys(server_config.mongo_collection_keys)
        token, jti = issuer.mint(sub="martin", email=None, roles=["hpcuser"])
        claims = issuer.verify(token)
    """

    def __init__(self, issuer: str, audience: str = "freva-api") -> None:
        cfg = ServerConfig()
        self.trusted_issuers: List[str] = cfg.oidc_trusted_issuers or []
        self.issuer = issuer
        self.audience = audience
        self._mongo_url: str = cfg.mongo_url
        self._mongo_db: str = cfg.mongo_db
        self._private_key: Optional["RSAPrivateKey"] = None  # loaded in setup()
        # kid -> public key for trusted peer instances
        self._peer_keys: Dict[str, RSAPublicKey] = {}
        # issuer URL -> last refresh attempt timestamp (for cooldown)
        self._peer_last_refresh: Dict[str, float] = {}

    @property
    def private_key(self) -> "RSAPrivateKey":
        """Return the private key or raise if setup() was not called."""
        if self._private_key is None:
            raise RuntimeError("TokenIssuer.setup() must be called before use.")
        return self._private_key

    async def setup(self, collection: AsyncCollection) -> None:  # type: ignore
        """Load or generate the signing key. Call once in the lifespan handler."""
        doc = await collection.find_one({"_id": _KEY_DOC_ID})
        pem: str = ""
        if doc:
            pem = doc["pem"]
            self._private_key = cast(
                "RSAPrivateKey",
                serialization.load_pem_private_key(pem.encode(), password=None),
            )
            return

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ).decode()
        # upsert - safe if two workers race on first startup
        await collection.update_one(
            {"_id": _KEY_DOC_ID},
            {"$setOnInsert": {"pem": pem}},
            upsert=True,
        )
        # Re-read to get whichever worker's key won the race
        doc = await collection.find_one({"_id": _KEY_DOC_ID})
        if doc:
            pem = doc["pem"]
            self._private_key = cast(
                "RSAPrivateKey",
                serialization.load_pem_private_key(pem.encode(), password=None),
            )

    async def load_peer_keys(self, collection: AsyncCollection[Any]) -> None:
        """Fetch peer JWKS, persist to MongoDB, and populate in-memory cache.

        Called once at startup. For each trusted peer:

        1. Attempt to fetch JWKS from the peer's live endpoint.
        2. On success, upsert into MongoDB and update the in-memory cache.
        3. After all peers are attempted, load any previously stored JWKS
           from MongoDB — this covers peers that were unreachable at startup
           but had keys stored from a previous run.

        Individual peer failures are logged and skipped — the instance starts
        normally with whatever keys are available.
        """
        if not self.trusted_issuers:
            return

        async with httpx.AsyncClient(timeout=5, verify=True) as client:
            for issuer_url in self.trusted_issuers:
                await self._fetch_and_store_peer_keys(issuer_url, client, collection)

        # Load any stored keys for peers that were unreachable above
        await self._load_peer_keys_from_db(collection)

    async def _fetch_and_store_peer_keys(
        self,
        issuer_url: str,
        client: httpx.AsyncClient,
        collection: AsyncCollection,  # type: ignore[type-arg]
    ) -> None:
        """Fetch JWKS from a single peer, persist to MongoDB, update cache."""
        url = f"{issuer_url.rstrip('/')}{_PEER_JWKS_PATH}"
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            jwks = resp.json()
            await collection.replace_one(
                {"_id": f"{_PEER_KEY_DOC_PREFIX}{issuer_url}"},
                {
                    "_id": f"{_PEER_KEY_DOC_PREFIX}{issuer_url}",
                    "issuer_url": issuer_url,
                    "jwks": jwks,
                    "fetched_at": datetime.now(tz=timezone.utc),
                },
                upsert=True,
            )
            self._cache_jwks(jwks)
            self._peer_last_refresh[issuer_url] = time.monotonic()
            logger.info(
                "Loaded and stored peer JWKS from %s (%d keys)",
                issuer_url,
                len(self._peer_keys),
            )
        except Exception as exc:
            logger.warning("Could not fetch JWKS from peer %s: %s", issuer_url, exc)

    async def _load_peer_keys_from_db(
        self,
        collection: AsyncCollection,  # type: ignore[type-arg]
    ) -> None:
        """Populate in-memory cache from previously stored peer JWKS in MongoDB."""
        async for doc in collection.find(
            {"_id": {"$regex": f"^{_PEER_KEY_DOC_PREFIX}"}}
        ):
            self._cache_jwks(doc["jwks"])
            logger.info(
                "Restored peer JWKS from MongoDB for %s",
                doc.get("issuer_url", doc["_id"]),
            )

    def _cache_jwks(self, jwks: Dict[str, List[Dict[str, str]]]) -> None:
        """Populate in-memory _peer_keys from a JWKS document."""
        for jwk in jwks.get("keys", []):
            kid = jwk.get("kid")
            if kid:
                self._peer_keys[kid] = cast(RSAPublicKey, RSAAlgorithm.from_jwk(jwk))

    def _maybe_refresh_peer_keys_for(self, kid: str) -> None:
        """Synchronously refresh peer keys if the kid is unknown and cooldown passed.

        Uses sync httpx and sync pymongo — no event loop needed.
        Persists updated keys to MongoDB so they survive the next restart.
        Fires at most once per cooldown period per peer.
        """
        now = time.monotonic()

        for issuer_url in self.trusted_issuers:
            last = self._peer_last_refresh.get(issuer_url, 0.0)
            if now - last < _PEER_REFRESH_COOLDOWN:
                continue
            url = f"{issuer_url.rstrip('/')}{_PEER_JWKS_PATH}"
            try:
                resp = httpx.get(url, timeout=3, verify=True)
                resp.raise_for_status()
                jwks = resp.json()
                self._cache_jwks(jwks)
                self._peer_last_refresh[issuer_url] = now
                logger.info("Refreshed peer JWKS from %s", issuer_url)

                # Persist via sync pymongo — _mongo_url/_mongo_db set at init
                sync_client: pymongo.MongoClient[Any] = pymongo.MongoClient(
                    self._mongo_url
                )
                try:
                    sync_client[self._mongo_db]["freva_keys"].replace_one(
                        {"_id": f"{_PEER_KEY_DOC_PREFIX}{issuer_url}"},
                        {
                            "_id": f"{_PEER_KEY_DOC_PREFIX}{issuer_url}",
                            "issuer_url": issuer_url,
                            "jwks": jwks,
                            "fetched_at": datetime.now(tz=timezone.utc),
                        },
                        upsert=True,
                    )
                finally:
                    sync_client.close()

                if kid in self._peer_keys:
                    return  # found it, no need to check remaining peers
            except Exception as exc:
                logger.warning(
                    "Could not refresh JWKS from peer %s: %s", issuer_url, exc
                )
                self._peer_last_refresh[issuer_url] = (
                    now  # set cooldown even on failure
                )

    def _key_id(self) -> str:
        pub = self.private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        return hashlib.sha256(pub).hexdigest()[:16]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def mint(
        self,
        sub: str,
        email: Optional[str],
        roles: List[str],
        expiry_seconds: int = TOKEN_EXPIRY_SECONDS,
        preferred_username: Optional[str] = None,
    ) -> tuple[str, str]:
        """Mint a freva JWT. Returns ``(token, jti)``."""
        jti = str(uuid.uuid4())
        now = datetime.now(tz=timezone.utc)
        payload = {
            "sub": sub,
            "preferred_username": preferred_username or sub,
            "email": email,
            "roles": roles,
            "jti": jti,
            "iss": self.issuer,
            "aud": self.audience,
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(seconds=expiry_seconds)).timestamp()),
        }
        token = jwt.encode(
            payload,
            self.private_key,
            algorithm=ALGORITHM,
            headers={"kid": self._key_id()},
        )
        return token, jti

    def verify(self, token: str) -> IDToken:
        """Verify a freva JWT and return its claims.

        Checks own key first. If the ``kid`` header points to a peer key,
        verifies the issuer is trusted before touching the key cache, then
        verifies against the cached peer public key. If the ``kid`` is
        unknown, attempts a lazy one-time sync refresh (rate-limited).

        Raises ``jwt.PyJWTError`` if invalid, expired, or wrong audience.
        Raises ``jwt.InvalidIssuerError`` if the issuer is not trusted.
        """
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        own_kid = self._key_id()

        if kid and kid != own_kid:
            # Not our key — check issuer before any key lookup
            unverified = jwt.decode(token, options={"verify_signature": False})
            iss = unverified.get("iss", "")
            if iss not in self.trusted_issuers:
                raise jwt.exceptions.InvalidIssuerError(f"Untrusted issuer: {iss!r}")

            if kid not in self._peer_keys:
                # Unknown kid: attempt lazy refresh before giving up
                self._maybe_refresh_peer_keys_for(kid)

            if kid in self._peer_keys:
                payload = jwt.decode(
                    token,
                    self._peer_keys[kid],
                    algorithms=[ALGORITHM],
                    audience=self.audience,
                    # issuer not checked — peer has a different iss
                )
                return IDToken(**payload)

        # Own key (or unknown kid that didn't resolve to any peer)
        payload = jwt.decode(
            token,
            self.private_key.public_key(),
            algorithms=[ALGORITHM],
            audience=self.audience,
            issuer=self.issuer,
        )
        return IDToken(**payload)

    def jwks(self) -> JWKSDict:
        """Return the public key as a JWKS document."""
        jwk = json.loads(RSAAlgorithm.to_jwk(self.private_key.public_key()))
        jwk["kid"] = self._key_id()
        jwk.setdefault("use", "sig")
        jwk.setdefault("alg", ALGORITHM)
        return {"keys": [jwk]}
