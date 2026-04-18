"""Tests for the freva auth broker endpoints.

The token broker tests mock ``httpx.AsyncClient.request`` so that the
server's internal Keycloak calls return controlled fake responses, while
all freva-side code (_mint_and_store, session_store, token_issuer) runs
for real. This is the same strategy used in py-oidc-auth itself.

Federation tests use a second in-memory TokenIssuer as a stand-in peer
instance — no real second server needed. Since ``trusted_issuers``,
``_mongo_url`` and ``_mongo_db`` are instance attributes set at
construction, tests patch them directly on the module-level singleton.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import jwt as pyjwt
import pytest
import requests
from cryptography.hazmat.primitives.asymmetric import rsa
from pytest_mock import MockerFixture

_MOCK_TARGET = "httpx.AsyncClient.request"


# ---------------------------------------------------------------------------
# Helpers — IDP mock responses
# ---------------------------------------------------------------------------


def _httpx_response(
    status_code: int = 200,
    payload: Any = None,
    text: str = "",
    url: str = "https://idp.example.com/token",
) -> httpx.Response:
    if payload is not None:
        content = json.dumps(payload).encode()
        headers = {"content-type": "application/json"}
    else:
        content = text.encode()
        headers = {"content-type": "text/plain"}
    return httpx.Response(
        status_code=status_code,
        content=content,
        headers=headers,
        request=httpx.Request("POST", url),
    )


def _idp_token_payload() -> Dict[str, Any]:
    """A fake IDP token response from Keycloak."""
    now = int(time.time())
    access_token = pyjwt.encode(
        {
            "sub": "abc123-uuid",
            "preferred_username": "janedoe",
            "email": "janedoe@example.com",
            "aud": ["freva", "account"],
            "iat": now,
            "exp": now + 3600,
        },
        "secret",
    )
    return {
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": "idp-refresh-token-xyz",
        "refresh_expires_in": 7200,
        "scope": "openid profile email",
    }


def _fake_idp_claims() -> Any:
    """Return a fake IDToken as _get_token would return it."""
    from py_oidc_auth import IDToken

    return IDToken(
        sub="abc123-uuid",
        preferred_username="janedoe",
        email="janedoe@example.com",
        aud=["freva", "account"],
    )


# ---------------------------------------------------------------------------
# Helpers — federation peer
# ---------------------------------------------------------------------------


def _make_peer_issuer(peer_url: str = "https://freva-peer.dkrz.de") -> Any:
    """Create an in-memory TokenIssuer acting as a trusted peer instance."""
    from freva_rest.auth.token_issuer import TokenIssuer

    peer = TokenIssuer(issuer=peer_url, audience="freva-api")
    peer._private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return peer


def _peer_jwks_response(peer: Any) -> httpx.Response:
    """Build a fake JWKS httpx.Response from a peer TokenIssuer."""
    return httpx.Response(
        status_code=200,
        content=json.dumps(peer.jwks()).encode(),
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", "https://freva-peer.dkrz.de/jwks"),
    )


def _peer_token(peer: Any) -> str:
    """Mint a valid token from the peer instance."""
    token, _ = peer.mint(
        sub="janedoe",
        email="janedoe@dkrz.de",
        roles=["hpcuser"],
        preferred_username="janedoe",
    )
    return token


# ---------------------------------------------------------------------------
# Token broker tests
# ---------------------------------------------------------------------------


def test_token_endpoint_mints_freva_jwt(
    test_server: str, mocker: MockerFixture
) -> None:
    """POST /auth/v2/token with a device-code mints a freva JWT.

    Exercises _mint_and_store and session_store.save.
    """
    mocker.patch(
        _MOCK_TARGET,
        new_callable=AsyncMock,
        return_value=_httpx_response(200, _idp_token_payload()),
    )
    mocker.patch(
        "freva_rest.auth.auth._get_token",
        new_callable=AsyncMock,
        return_value=_fake_idp_claims(),
    )
    res = requests.post(
        f"{test_server}/auth/v2/token",
        data={"device-code": "DEV-123"},
    )
    assert res.status_code == 200
    js = res.json()
    assert "access_token" in js
    assert js["token_type"] == "Bearer"

    decoded = pyjwt.decode(js["access_token"], options={"verify_signature": False})
    assert decoded["preferred_username"] == "janedoe"
    assert "roles" in decoded


def test_token_refresh_rotates_session(test_server: str, mocker: MockerFixture) -> None:
    """Refreshing a freva JWT exercises _idp_refresh and session_store.get/delete."""
    mocker.patch(
        _MOCK_TARGET,
        new_callable=AsyncMock,
        return_value=_httpx_response(200, _idp_token_payload()),
    )
    mocker.patch(
        "freva_rest.auth.auth._get_token",
        new_callable=AsyncMock,
        return_value=_fake_idp_claims(),
    )
    res = requests.post(
        f"{test_server}/auth/v2/token",
        data={"device-code": "DEV-123"},
    )
    assert res.status_code == 200
    freva_jwt = res.json()["access_token"]

    mocker.patch(
        _MOCK_TARGET,
        new_callable=AsyncMock,
        return_value=_httpx_response(200, _idp_token_payload()),
    )
    mocker.patch(
        "freva_rest.auth.auth._get_token",
        new_callable=AsyncMock,
        return_value=_fake_idp_claims(),
    )
    res2 = requests.post(
        f"{test_server}/auth/v2/token",
        data={"refresh-token": freva_jwt},
    )
    assert res2.status_code == 200
    assert res2.json()["access_token"] != freva_jwt

    # Old session deleted — second refresh must fail
    res3 = requests.post(
        f"{test_server}/auth/v2/token",
        data={"refresh-token": freva_jwt},
    )
    assert res3.status_code == 401


def test_idp_refresh_missing_jti_rejected(test_server: str) -> None:
    """A freva JWT without a jti claim returns 401."""
    from freva_rest.auth import token_issuer

    no_jti_token = pyjwt.encode(
        {
            "sub": "janedoe",
            "preferred_username": "janedoe",
            "roles": ["hpcuser"],
            "aud": "freva-api",
            "iss": token_issuer.issuer,
            "exp": int(time.time()) + 3600,
        },
        token_issuer.private_key,
        algorithm="RS256",
    )
    res = requests.post(
        f"{test_server}/auth/v2/token",
        data={"refresh-token": no_jti_token},
    )
    assert res.status_code == 401
    assert "jti" in res.json()["detail"].lower()


def test_mint_and_store_invalid_claims_returns_401(
    test_server: str, mocker: MockerFixture
) -> None:
    """When _get_token raises InvalidRequest the endpoint returns 401."""
    from py_oidc_auth.exceptions import InvalidRequest as IR

    mocker.patch(
        _MOCK_TARGET,
        new_callable=AsyncMock,
        return_value=_httpx_response(200, _idp_token_payload()),
    )
    mocker.patch(
        "freva_rest.auth.auth._get_token",
        new_callable=AsyncMock,
        side_effect=IR(status_code=401, detail="Invalid token claims."),
    )
    res = requests.post(
        f"{test_server}/auth/v2/token",
        data={"device-code": "DEV-123"},
    )
    assert res.status_code == 401
    assert "claims" in res.json()["detail"].lower()


def test_check_token_expired(test_server: str) -> None:
    """An expired freva JWT raises 401 with 'Token has expired' detail."""
    from freva_rest.auth import token_issuer

    expired_token = pyjwt.encode(
        {
            "sub": "janedoe",
            "preferred_username": "janedoe",
            "roles": ["hpcuser"],
            "aud": "freva-api",
            "iss": token_issuer.issuer,
            "exp": int(time.time()) - 10,
        },
        token_issuer.private_key,
        algorithm="RS256",
    )
    res = requests.get(
        f"{test_server}/data-portal/zarr-utils/status",
        params={"url": "foo.zar"},
        headers={"Authorization": f"Bearer {expired_token}"},
    )
    assert res.status_code == 401
    assert "expired" in res.json()["detail"].lower()


def test_check_token_insufficient_roles(
    test_server: str, mocker: MockerFixture, auth: Dict[str, str]
) -> None:
    """A valid token that fails role check returns 403."""
    mocker.patch("freva_rest.auth.token_field_matches", return_value=False)
    res = requests.get(
        f"{test_server}/data-portal/zarr-utils/status",
        params={"url": "foo.zar"},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res.status_code == 403
    assert "roles" in res.json()["detail"].lower()


def test_token_refresh_with_expired_freva_jwt(
    test_server: str, mocker: MockerFixture
) -> None:
    """An expired freva JWT can still refresh (jti extracted without sig check)."""
    mocker.patch(
        _MOCK_TARGET,
        new_callable=AsyncMock,
        return_value=_httpx_response(200, _idp_token_payload()),
    )
    mocker.patch(
        "freva_rest.auth.auth._get_token",
        new_callable=AsyncMock,
        return_value=_fake_idp_claims(),
    )
    res = requests.post(
        f"{test_server}/auth/v2/token",
        data={"device-code": "DEV-123"},
    )
    assert res.status_code == 200
    freva_jwt = res.json()["access_token"]

    from freva_rest.auth import token_issuer

    decoded = pyjwt.decode(freva_jwt, options={"verify_signature": False})
    expired_jwt = pyjwt.encode(
        {**decoded, "exp": int(time.time()) - 10},
        token_issuer.private_key,
        algorithm="RS256",
    )

    mocker.patch(
        _MOCK_TARGET,
        new_callable=AsyncMock,
        return_value=_httpx_response(200, _idp_token_payload()),
    )
    mocker.patch(
        "freva_rest.auth.auth._get_token",
        new_callable=AsyncMock,
        return_value=_fake_idp_claims(),
    )
    res2 = requests.post(
        f"{test_server}/auth/v2/token",
        data={"refresh-token": expired_jwt},
    )
    assert res2.status_code == 200


def test_token_upstream_error_propagated(
    test_server: str, mocker: MockerFixture
) -> None:
    """Upstream Keycloak error is forwarded as an HTTP error."""
    mocker.patch(
        _MOCK_TARGET,
        new_callable=AsyncMock,
        return_value=_httpx_response(
            400, {"error": "invalid_grant", "error_description": "Bad code"}
        ),
    )
    res = requests.post(
        f"{test_server}/auth/v2/token",
        data={"device-code": "BAD-CODE"},
    )
    assert res.status_code == 400


def test_refresh_invalid_token_rejected(test_server: str) -> None:
    """A completely invalid refresh token is rejected with 401."""
    res = requests.post(
        f"{test_server}/auth/v2/token",
        data={"refresh-token": "not.a.jwt"},
    )
    assert res.status_code == 401


def test_token_issuer_generates_new_key() -> None:
    """Covers the key-generation branch in TokenIssuer.setup()."""
    import asyncio

    import pymongo

    from freva_rest.auth.token_issuer import _KEY_DOC_ID, TokenIssuer
    from freva_rest.config import ServerConfig

    server_config = ServerConfig()
    client = pymongo.MongoClient(server_config.mongo_url)
    client[server_config.mongo_db]["freva_keys"].delete_one({"_id": _KEY_DOC_ID})
    client.close()

    issuer = TokenIssuer(issuer="http://test", audience="test")
    asyncio.run(issuer.setup(server_config.mongo_collection_keys))
    assert issuer._private_key is not None


def test_status_forbidden_invalid_token(test_server: str) -> None:
    """A token signed with a wrong key should be rejected with 401."""
    wrong_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    fake_token = pyjwt.encode(
        {"sub": "janedoe", "roles": [], "aud": "freva-api", "exp": 9999999999},
        wrong_key,
        algorithm="RS256",
    )
    res = requests.get(
        f"{test_server}/data-portal/zarr-utils/status",
        params={"url": ["foo.zar"]},
        headers={"Authorization": f"Bearer {fake_token}"},
    )
    assert res.status_code == 401


def test_wrong_token_claims(test_server: str) -> None:
    """A token with a wrong audience should be rejected with 401."""
    from freva_rest.auth import token_issuer

    wrong_aud_token = pyjwt.encode(
        {
            "sub": "janedoe",
            "preferred_username": "janedoe",
            "roles": ["hpcuser"],
            "aud": "wrong-audience",
            "exp": 9999999999,
        },
        token_issuer.private_key,
        algorithm="RS256",
    )
    res = requests.get(
        f"{test_server}/data-portal/zarr-utils/status",
        params={"url": "foo.zar"},
        headers={"Authorization": f"Bearer {wrong_aud_token}"},
    )
    assert res.status_code == 401
    assert "Invalid token" in res.json()["detail"]


# ---------------------------------------------------------------------------
# Federation — load_peer_keys
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_load_peer_keys_success() -> None:
    """load_peer_keys populates _peer_keys and persists to MongoDB."""
    from freva_rest.auth import token_issuer

    peer_url = "https://freva-peer.dkrz.de"
    peer = _make_peer_issuer(peer_url)
    peer_kid = peer._key_id()  # compute before patching anything
    original_trusted = token_issuer.trusted_issuers
    token_issuer.trusted_issuers = [peer_url]

    try:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=_peer_jwks_response(peer))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        mock_col = AsyncMock()
        mock_col.replace_one = AsyncMock()
        mock_col.find = MagicMock(return_value=_async_iter([]))

        with patch(
            "freva_rest.auth.token_issuer.httpx.AsyncClient",
            return_value=mock_client,
        ):
            await token_issuer.load_peer_keys(mock_col)

        assert peer_kid in token_issuer._peer_keys  # use pre-computed kid
        mock_col.replace_one.assert_called_once()
    finally:
        token_issuer.trusted_issuers = original_trusted
        token_issuer._peer_keys.pop(peer_kid, None)


@pytest.mark.asyncio
async def test_load_peer_keys_falls_back_to_db() -> None:
    """When a peer is unreachable, stored keys are loaded from MongoDB."""
    from freva_rest.auth import token_issuer
    from freva_rest.auth.token_issuer import _PEER_KEY_DOC_PREFIX

    peer_url = "https://freva-peer.dkrz.de"
    peer = _make_peer_issuer(peer_url)
    original_trusted = token_issuer.trusted_issuers
    token_issuer.trusted_issuers = [peer_url]
    token_issuer._peer_keys.pop(peer._key_id(), None)

    stored_doc = {
        "_id": f"{_PEER_KEY_DOC_PREFIX}{peer_url}",
        "issuer_url": peer_url,
        "jwks": peer.jwks(),
    }

    try:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=httpx.ConnectError("unreachable"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)

        mock_col = AsyncMock()
        mock_col.replace_one = AsyncMock()
        mock_col.find = MagicMock(return_value=_async_iter([stored_doc]))

        with patch(
            "freva_rest.auth.token_issuer.httpx.AsyncClient",
            return_value=mock_client,
        ):
            await token_issuer.load_peer_keys(mock_col)

        assert peer._key_id() in token_issuer._peer_keys
    finally:
        token_issuer.trusted_issuers = original_trusted
        token_issuer._peer_keys.pop(peer._key_id(), None)


@pytest.mark.asyncio
async def test_load_peer_keys_empty_is_noop() -> None:
    """No trusted issuers — load_peer_keys makes no HTTP calls."""
    from freva_rest.auth import token_issuer

    original_trusted = token_issuer.trusted_issuers
    token_issuer.trusted_issuers = []
    try:
        mock_col = AsyncMock()
        with patch("freva_rest.auth.token_issuer.httpx.AsyncClient") as mock_cls:
            await token_issuer.load_peer_keys(mock_col)
            mock_cls.assert_not_called()
    finally:
        token_issuer.trusted_issuers = original_trusted


# ---------------------------------------------------------------------------
# Federation — verify() peer key branch
# ---------------------------------------------------------------------------


def test_verify_accepts_cached_peer_token() -> None:
    """verify() accepts a token signed by a cached peer key."""
    from freva_rest.auth import token_issuer

    peer_url = "https://freva-peer.dkrz.de"
    peer = _make_peer_issuer(peer_url)
    peer_kid = peer._key_id()

    original_trusted = token_issuer.trusted_issuers
    token_issuer.trusted_issuers = [peer_url]
    token_issuer._peer_keys[peer_kid] = peer.private_key.public_key()
    try:
        claims = token_issuer.verify(_peer_token(peer))
        assert claims.preferred_username == "janedoe"
    finally:
        token_issuer.trusted_issuers = original_trusted
        token_issuer._peer_keys.pop(peer_kid, None)


def test_verify_rejects_untrusted_issuer_without_http_call() -> None:
    """verify() rejects an untrusted issuer without making any HTTP calls."""
    from freva_rest.auth import token_issuer

    peer = _make_peer_issuer("https://evil.example.com")
    original_trusted = token_issuer.trusted_issuers
    token_issuer.trusted_issuers = []

    try:
        with patch("freva_rest.auth.token_issuer.httpx.get") as mock_get:
            with pytest.raises(pyjwt.exceptions.InvalidIssuerError):
                token_issuer.verify(_peer_token(peer))
            mock_get.assert_not_called()
    finally:
        token_issuer.trusted_issuers = original_trusted


def test_verify_rejects_unknown_peer_after_failed_refresh() -> None:
    """verify() raises PyJWTError when peer kid can't be resolved."""
    from freva_rest.auth import token_issuer

    peer_url = "https://freva-peer.dkrz.de"
    peer = _make_peer_issuer(peer_url)
    peer_kid = peer._key_id()

    original_trusted = token_issuer.trusted_issuers
    token_issuer.trusted_issuers = [peer_url]
    token_issuer._peer_keys.pop(peer._key_id(), None)
    token_issuer._peer_last_refresh.pop(peer_url, None)

    try:
        with patch(
            "freva_rest.auth.token_issuer.httpx.get",
            side_effect=Exception("unreachable"),
        ):
            with patch("freva_rest.auth.token_issuer.pymongo.MongoClient"):
                with pytest.raises(pyjwt.PyJWTError):
                    token_issuer.verify(_peer_token(peer))
    finally:
        token_issuer.trusted_issuers = original_trusted


# ---------------------------------------------------------------------------
# Federation — lazy refresh and cooldown
# ---------------------------------------------------------------------------


def test_lazy_refresh_fetches_and_persists() -> None:
    """Unknown kid triggers a sync JWKS fetch and a MongoDB write."""
    from freva_rest.auth import token_issuer

    peer_url = "https://freva-peer.dkrz.de"
    peer = _make_peer_issuer(peer_url)
    peer_kid = peer._key_id()

    original_trusted = token_issuer.trusted_issuers
    token_issuer.trusted_issuers = [peer_url]
    token_issuer._peer_keys.pop(peer_kid, None)
    token_issuer._peer_last_refresh.pop(peer_url, None)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = peer.jwks()

    mock_mongo_col = MagicMock()
    mock_mongo_client = MagicMock()
    mock_mongo_client.__getitem__ = MagicMock(
        return_value=MagicMock(__getitem__=MagicMock(return_value=mock_mongo_col))
    )

    try:
        with patch(
            "freva_rest.auth.token_issuer.httpx.get",
            return_value=mock_response,
        ) as mock_get:
            with patch(
                "freva_rest.auth.token_issuer.pymongo.MongoClient",
                return_value=mock_mongo_client,
            ):
                token_issuer._maybe_refresh_peer_keys_for(peer_kid)

            mock_get.assert_called_once()
            mock_mongo_col.replace_one.assert_called_once()

        assert peer_kid in token_issuer._peer_keys
    finally:
        token_issuer.trusted_issuers = original_trusted
        token_issuer._peer_keys.pop(peer_kid, None)


def test_lazy_refresh_respects_cooldown() -> None:
    """A peer within cooldown is not re-fetched."""
    from freva_rest.auth import token_issuer

    peer_url = "https://freva-peer.dkrz.de"
    original_trusted = token_issuer.trusted_issuers
    token_issuer.trusted_issuers = [peer_url]
    token_issuer._peer_last_refresh[peer_url] = time.monotonic()

    try:
        with patch("freva_rest.auth.token_issuer.httpx.get") as mock_get:
            token_issuer._maybe_refresh_peer_keys_for("unknown-kid")
            mock_get.assert_not_called()
    finally:
        token_issuer.trusted_issuers = original_trusted


def test_lazy_refresh_sets_cooldown_on_failure() -> None:
    """A failed peer refresh still sets the cooldown timestamp."""
    from freva_rest.auth import token_issuer

    peer_url = "https://freva-peer.dkrz.de"
    original_trusted = token_issuer.trusted_issuers
    token_issuer.trusted_issuers = [peer_url]
    token_issuer._peer_last_refresh.pop(peer_url, None)

    try:
        with patch(
            "freva_rest.auth.token_issuer.httpx.get",
            side_effect=Exception("timeout"),
        ):
            token_issuer._maybe_refresh_peer_keys_for("unknown-kid")
        assert peer_url in token_issuer._peer_last_refresh
    finally:
        token_issuer.trusted_issuers = original_trusted


def test_verify_resolves_peer_token_via_lazy_refresh() -> None:
    """verify() discovers an unknown kid via lazy refresh and accepts the token."""
    from freva_rest.auth import token_issuer

    peer_url = "https://freva-peer.dkrz.de"
    peer = _make_peer_issuer(peer_url)
    peer_kid = peer._key_id()

    original_trusted = token_issuer.trusted_issuers
    token_issuer.trusted_issuers = [peer_url]
    token_issuer._peer_keys.pop(peer_kid, None)
    token_issuer._peer_last_refresh.pop(peer_url, None)

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = peer.jwks()

    mock_mongo_client = MagicMock()
    mock_mongo_client.__getitem__ = MagicMock(
        return_value=MagicMock(__getitem__=MagicMock(return_value=MagicMock()))
    )

    try:
        with patch(
            "freva_rest.auth.token_issuer.httpx.get",
            return_value=mock_response,
        ):
            with patch(
                "freva_rest.auth.token_issuer.pymongo.MongoClient",
                return_value=mock_mongo_client,
            ):
                claims = token_issuer.verify(_peer_token(peer))
        assert claims.preferred_username == "janedoe"
    finally:
        token_issuer.trusted_issuers = original_trusted
        token_issuer._peer_keys.pop(peer_kid, None)


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------


def test_request_headers() -> None:
    from py_oidc_auth.auth_base import (
        _set_request_header as set_request_header,
    )

    header, data = {"Content-Type": "foo"}, {}
    set_request_header("foo", "bar", data, header)
    assert header["Content-Type"] != "foo"
    assert "Authorization" in header
    assert header["Authorization"].startswith("Basic")

    header, data = {"Content-Type": "foo"}, {}
    set_request_header("foo", None, data, header)
    assert "Authorization" not in header
    assert "client_id" in data


# ---------------------------------------------------------------------------
# Internal helper for async iteration in mocks
# ---------------------------------------------------------------------------


class _async_iter:
    """Minimal async iterator for mocking motor cursor results."""

    def __init__(self, items: list) -> None:
        self._items = iter(items)

    def __aiter__(self) -> "_async_iter":
        return self

    async def __anext__(self) -> Any:
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration
