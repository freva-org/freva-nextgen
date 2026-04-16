"""Freva JWT issuance and verification.

The signing key is stored in MongoDB so all uvicorn workers share the same
key without any filesystem coordination. It is loaded once at startup and
cached in memory — no per-request DB round-trips.

If no key exists yet (first startup), one is generated and persisted.
"""

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, List, Optional, TypedDict, cast

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from py_oidc_auth import IDToken
from pymongo.asynchronous.collection import AsyncCollection

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
ALGORITHM = "RS256"
_KEY_DOC_ID = "freva-signing-key"
_TOKEN_EXPIRY_SECONDS = 3600


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

    Usage::

        issuer = TokenIssuer(issuer="https://freva-api.dkrz.de")
        await issuer.setup(server_config.mongo_client["freva"]["freva_keys"])
        token, jti = issuer.mint(sub="martin", email=None, roles=["hpcuser"])
        claims = issuer.verify(token)
    """

    def __init__(self, issuer: str, audience: str = "freva-api") -> None:
        self.issuer = issuer
        self.audience = audience
        self._private_key: Optional["RSAPrivateKey"] = None  # loaded in setup()

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
        # upsert — safe if two workers race on first startup
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
        expiry_seconds: int = _TOKEN_EXPIRY_SECONDS,
    ) -> tuple[str, str]:
        """Mint a freva JWT. Returns ``(token, jti)``."""
        jti = str(uuid.uuid4())
        now = datetime.now(tz=timezone.utc)
        payload = {
            "sub": sub,
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

        Raises ``jwt.PyJWTError`` if invalid, expired, or wrong audience.
        """
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
