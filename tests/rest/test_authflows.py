"""Tests for the freva auth broker endpoints.

The token broker tests mock ``httpx.AsyncClient.request`` so that the
server's internal Keycloak calls return controlled fake responses, while
all freva-side code (_mint_and_store, session_store, token_issuer) runs
for real. This is the same strategy used in py-oidc-auth itself.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict
from unittest.mock import AsyncMock

import httpx
import jwt as pyjwt
import requests
from pytest_mock import MockerFixture

_MOCK_TARGET = "httpx.AsyncClient.request"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _httpx_response(
    status_code: int = 200,
    payload: Any = None,
    text: str = "",
) -> httpx.Response:
    """Build a real ``httpx.Response`` suitable for mocking."""
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
# Token broker tests
# ---------------------------------------------------------------------------


def test_token_endpoint_mints_freva_jwt(
    test_server: str, mocker: MockerFixture
) -> None:
    """POST /auth/v2/token with a device-code mints a freva JWT.

    Exercises _mint_and_store and session_store.save.
    """
    # Mock the IDP token fetch
    mocker.patch(
        _MOCK_TARGET,
        new_callable=AsyncMock,
        return_value=_httpx_response(200, _idp_token_payload()),
    )
    # Bypass JWKS verification — auth is a module-level singleton
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

    # The returned token must be a valid freva JWT (RS256, aud=freva-api)
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

    # Step 1: get a freva JWT
    res = requests.post(
        f"{test_server}/auth/v2/token",
        data={"device-code": "DEV-123"},
    )
    assert res.status_code == 200
    freva_jwt = res.json()["access_token"]

    # Step 2: refresh — the server exchanges the stored IDP refresh token
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
    new_jwt = res2.json()["access_token"]
    assert new_jwt != freva_jwt  # new jti, new token

    # Step 3: old session is deleted — refreshing the old token must fail
    res3 = requests.post(
        f"{test_server}/auth/v2/token",
        data={"refresh-token": freva_jwt},
    )
    assert res3.status_code == 401


def test_idp_refresh_missing_jti_rejected(
    test_server: str, mocker: MockerFixture
) -> None:
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
            # no jti field
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
            "iss": "",
            "exp": int(time.time()) - 10,  # already expired
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
    # token_field_matches runs in the server thread — patch via full module path
    mocker.patch(
        "freva_rest.auth.token_field_matches",
        return_value=False,
    )
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

    # Forge an expired version with the same jti
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
    # Use sync pymongo to clear the key
    client = pymongo.MongoClient(server_config.mongo_url)
    col = client[server_config.mongo_db]["freva_keys"]
    col.delete_one({"_id": _KEY_DOC_ID})
    client.close()

    # Run async setup in a fresh event loop
    issuer = TokenIssuer(issuer="http://test", audience="test")
    asyncio.run(issuer.setup(server_config.mongo_collection_keys))
    assert issuer._private_key is not None


def test_status_forbidden_invalid_token(
    test_server: str,
) -> None:
    """A token signed with a wrong key should be rejected with 401."""
    import jwt
    from cryptography.hazmat.primitives.asymmetric import rsa

    # Generate a random key — not the server's key
    wrong_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    fake_token = jwt.encode(
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


def test_wrong_token_claims(
    test_server: str,
) -> None:
    """A token with a wrong audience should be rejected with 401."""
    import jwt

    from freva_rest.auth import token_issuer

    # Mint a token signed with the real key but wrong audience
    wrong_aud_token = jwt.encode(
        {
            "sub": "janedoe",
            "preferred_username": "janedoe",
            "roles": ["hpcuser"],
            "aud": "wrong-audience",  # real aud is "freva-api"
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
