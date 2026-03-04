"""Test for the authorisation utilities."""

import time
from typing import Any, Dict
from unittest.mock import AsyncMock, patch

import jwt
import requests
import pytest
from pytest_mock import MockerFixture
from py_oidc_auth.exceptions import InvalidRequest


def _device_start_payload(**overrides: Any) -> Dict[str, Any]:
    base = {
        "device_code": "DEV-123",
        "user_code": "ABCD-EFGH",
        "verification_uri": "https://auth.example.com/verify",
        "verification_uri_complete": "https://auth.example.com/verify?user_code=ABCD-EFGH",
        "expires_in": 600,
        "interval": 2,
    }
    base.update(overrides)
    return base


def _token_success_payload() -> Dict[str, Any]:
    # Minimal realistic token payload (shape matches your client usage)
    now = int(time.time())
    encoded = jwt.encode(
        {"result": "test_access_token", "iat": now, "exp": now + 300}, "PyJWK"
    )
    return {
        "access_token": encoded,
        "token_type": "Bearer",
        "expires": now + 300,
        "refresh_token": "test_refresh_token",
        "refresh_expires": now + 3600,
        "scope": "profile email address",
    }


# ---------------------------
# Device flow
# ---------------------------


def test_rest_device_start_success(
    test_server: str, mocker: MockerFixture
) -> None:
    """Proxy start endpoint returns device start JSON."""
    mocker.patch(
        "py_oidc_auth.auth_base.oidc_request",
        new=AsyncMock(return_value=_device_start_payload()),
    )
    res = requests.post(f"{test_server}/auth/v2/device")
    assert res.status_code == 200
    js = res.json()
    assert "device_code" in js and "user_code" in js
    assert "verification_uri" in js and "expires_in" in js


def test_rest_device_token_success(
    test_server: str, mocker: MockerFixture
) -> None:
    """Proxy token endpoint returns access/refresh tokens."""
    mocker.patch(
        "py_oidc_auth.auth_base.oidc_request",
        new=AsyncMock(return_value=_token_success_payload()),
    )
    res = requests.post(
        f"{test_server}/auth/v2/token", data={"device-code": "DEV-123"}
    )
    assert res.status_code == 200
    js = res.json()
    assert "access_token" in js and "refresh_token" in js


def test_rest_device_token_error_pending(
    test_server: str, mocker: MockerFixture
) -> None:
    """Proxy token endpoint forwards OAuth errors (authorization_pending)."""
    mocker.patch(
        "py_oidc_auth.auth_base.oidc_request",
        new=AsyncMock(side_effect=InvalidRequest(400, "authorization_pending")),
    )
    res = requests.post(
        f"{test_server}/auth/v2/token", data={"device-code": "DEV-123"}
    )
    assert res.status_code == 400


def test_rest_device_upstream_malformed(
    test_server: str, mocker: MockerFixture
) -> None:
    """Proxy token endpoint forwards OAuth errors (authorization_pending)."""
    mocker.patch(
        "py_oidc_auth.auth_base.oidc_request",
        new=AsyncMock(return_value={}),
    )
    res = requests.post(f"{test_server}/auth/v2/device")
    assert res.status_code == 502


def test_token_status(test_server: str, auth: Dict[str, str]) -> None:
    """Check the token status methods."""
    res1 = requests.get(
        f"{test_server}/auth/v2/status",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res1.status_code == 200
    assert "exp" in res1.json()
    res2 = requests.get(
        f"{test_server}/auth/v2/status",
        headers={"Authorization": "Bearer foo"},
    )
    assert res2.status_code != 200


def test_wrong_token_claims(
    test_server: str, mocker: MockerFixture, auth: Dict[str, str]
) -> None:
    """Test rejection for wrong token claims."""
    mocker.patch("py_oidc_auth.auth_base.token_field_matches", return_value=False)
    res = requests.get(
        f"{test_server}/data-portal/zarr-utils/status",
        params={"url": "foo.zar"},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res.status_code == 401
    assert "token claims" in res.json()["detail"]


# ---------------------------
# Other utilities.
# ---------------------------


def test_request_headers() -> None:
    from py_oidc_auth.auth_base import _set_request_header as set_request_header

    header, data = {"Content-Type": "foo"}, {}
    set_request_header("foo", "bar", data, header)
    assert header["Content-Type"] != "foo"
    assert "Authorization" in header
    assert header["Authorization"].startswith("Basic")

    header, data = {"Content-Type": "foo"}, {}
    set_request_header("foo", None, data, header)
    assert "Authorization" not in header
    assert "client_id" in data
