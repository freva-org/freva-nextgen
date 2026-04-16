"""MongoDB-backed session store for IDP refresh tokens.

Sessions are stored in the ``freva_sessions`` collection. A TTL index on
``expires_at`` lets MongoDB expire old entries automatically — no manual
purge needed.

Document shape::

    {
        "_id":           "<jti>",
        "sub":           "martin@dkrz.de",
        "refresh_token": "<IDP refresh token>",
        "expires_at":    <datetime>      # TTL index field — must be datetime
    }

Note: MongoDB TTL indexes require a ``datetime`` field, not a Unix timestamp.
"""

from datetime import datetime, timezone
from typing import Optional

from pymongo.asynchronous.collection import AsyncCollection


class SessionStore:
    """Async MongoDB session store for IDP refresh tokens.

    Usage::

        store = SessionStore()
        await store.setup(server_config.mongo_collection_sessions)
        await store.save(jti, sub, refresh_token, expires_at)
        session = await store.get(jti)   # -> (sub, refresh_token) | None
        await store.delete(jti)
    """

    async def setup(self, collection: AsyncCollection) -> None:  # type: ignore
        """Ensure the TTL index exists. Call once in the lifespan handler."""
        await collection.create_index(
            [("expires_at", 1)],
            expireAfterSeconds=0,
            name="sessions_ttl",
        )

    async def save(
        self,
        collection: AsyncCollection,  # type: ignore[type-arg]
        jti: str,
        sub: str,
        refresh_token: str,
        expires_at: int,  # Unix timestamp
    ) -> None:
        """Persist or replace a session entry keyed by jti."""
        await collection.replace_one(
            {"_id": jti},
            {
                "_id": jti,
                "sub": sub,
                "refresh_token": refresh_token,
                # TTL index requires a datetime, not a raw int
                "expires_at": datetime.fromtimestamp(expires_at, tz=timezone.utc),
            },
            upsert=True,
        )

    async def get(
        self,
        collection: AsyncCollection,  # type: ignore[type-arg]
        jti: str,
    ) -> Optional[tuple[str, str]]:
        """Return ``(sub, refresh_token)`` for the given jti, or None."""
        doc = await collection.find_one({"_id": jti})
        if doc is None:
            return None
        return doc["sub"], doc["refresh_token"]

    async def delete(
        self,
        collection: AsyncCollection,  # type: ignore[type-arg]
        jti: str,
    ) -> None:
        """Remove a session entry (called on token rotation or logout)."""
        await collection.delete_one({"_id": jti})
