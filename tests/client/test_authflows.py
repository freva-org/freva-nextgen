"""Test for the authorisation utilities."""

import json
import os
import threading
import time
from copy import deepcopy
from tempfile import NamedTemporaryFile
from typing import Any, Dict
from unittest.mock import patch

import jwt
import pytest
import requests
from pytest_mock import MockerFixture

from freva_client.auth import Auth, AuthError, authenticate
from freva_client.utils.auth_utils import (
    TOKEN_ENV_VAR,
    CodeAuthClient,
    DeviceAuthClient,
    is_job_env,
)
from tests.conftest import mock_token_data

# ---------------------------
# Helpers for mocked responses
# ---------------------------


def mock_device_login(auto_open: bool = False) -> None:
    """Raise an error for device login flow."""
    raise AuthError("foo", status_code=503)


class _Resp:
    def __init__(self, status: int, payload: Dict[str, Any]):
        self.status_code = status
        self._payload = payload
        self.text = json.dumps(payload)

    async def json(self) -> Dict[str, Any]:
        return self._payload

    async def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}: {self.text}")


class _RespSync:
    def __init__(self, status: int, payload: Dict[str, Any]):
        self.status_code = status
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self) -> Dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}: {self.text}")


def _device_start_payload(**overrides: Any) -> Dict[str, Any]:
    base = {
        "device_code": "DEV-123",
        "user_code": "ABCD-EFGH",
        "verification_uri": "https://auth.example.com/verify",
        "verification_uri_complete": (
            "https://auth.example.com/verify?" "user_code=ABCD-EFGH"
        ),
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


def _oauth_error_payload(code: str, desc: str = "") -> Dict[str, Any]:
    return {"error": code, "error_description": desc or code}


# ---------------------------
# Device flow
# ---------------------------


def test_device_flow_fail(mocker: MockerFixture) -> None:
    """Test failing for device flow."""

    def mock_device_login(auto_open: bool = False) -> None:
        """Raise an error for device login flow."""
        raise AuthError("foo", status_code=500)

    mocker.patch(
        "freva_client.auth.DeviceAuthClient.login",
        side_effect=mock_device_login,
    )
    auth_instance = Auth()
    with pytest.raises(AuthError):
        auth_instance._login("https://example.com")


def test_rest_device_start_success(
    test_server: str, mocker: MockerFixture
) -> None:
    """Proxy start endpoint returns device start JSON."""

    # Mock upstream (Keycloak) POST
    async def _ok_post(*args: Any, **kwargs: Any):
        return _Resp(200, _device_start_payload())

    mocker.patch("aiohttp.ClientSession.request", side_effect=_ok_post)

    res = requests.post(f"{test_server}/auth/v2/device")
    assert res.status_code == 200
    js = res.json()
    assert "device_code" in js and "user_code" in js
    assert "verification_uri" in js and "expires_in" in js


def test_rest_device_token_success(
    test_server: str, mocker: MockerFixture
) -> None:
    """Proxy token endpoint returns access/refresh tokens."""

    async def _ok_post(*args: Any, **kwargs: Any):
        # When the API proxies to KC /token it should respond with success
        return _Resp(200, _token_success_payload())

    mocker.patch("aiohttp.ClientSession.request", side_effect=_ok_post)

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

    async def _pending(*args: Any, **kwargs: Any):
        return _Resp(400, _oauth_error_payload("authorization_pending"))

    mocker.patch("aiohttp.ClientSession.request", side_effect=_pending)

    res = requests.post(
        f"{test_server}/auth/v2/token", data={"device-code": "DEV-123"}
    )
    assert res.status_code == 400


def test_rest_device_upstream_malformed(
    test_server: str, mocker: MockerFixture
) -> None:
    """Proxy token endpoint forwards OAuth errors (authorization_pending)."""

    async def _pending(*args: Any, **kwargs: Any):
        return _Resp(200, {})

    mocker.patch("aiohttp.ClientSession.request", side_effect=_pending)

    res = requests.post(f"{test_server}/auth/v2/device")
    assert res.status_code == 502


def test_device_login_success(mocker: MockerFixture) -> None:
    """Happy path: start -> pending -> success, with auto-open."""
    # Mock browser open (don’t actually open a window)
    mocker.patch("webbrowser.open", return_value=True)

    # Sequence: device start (200), then token pending (400), then token success (200)
    seq = [
        _RespSync(200, _device_start_payload(interval=1)),
        _RespSync(400, _oauth_error_payload("authorization_pending")),
        _RespSync(200, _token_success_payload()),
    ]

    def _post(*args: Any, **kwargs: Any):
        return seq.pop(0)

    mocker.patch("requests.Session.post", side_effect=_post)
    interactive = int(is_job_env())
    with patch.dict(
        os.environ, {"INTERACIVE_SESSION": str(interactive)}, clear=False
    ):
        client = DeviceAuthClient(
            device_endpoint="https://api.example.com/auth/v2/device",
            token_endpoint="https://api.example.com/auth/v2/token",
            timeout=10,
        )
        tokens = client.login(auto_open=True)
    assert "access_token" in tokens and "refresh_token" in tokens


def test_device_client_login_success(
    test_server: str, mocker: MockerFixture
) -> None:
    """Happy path: start -> pending -> success, with auto-open."""
    # Mock browser open (don’t actually open a window)
    mocker.patch("webbrowser.open", return_value=True)

    # Sequence: device start (200), then token pending (400), then token success (200)
    seq = [
        _RespSync(200, _device_start_payload(interval=1)),
        _RespSync(400, _oauth_error_payload("authorization_pending")),
        _RespSync(200, _token_success_payload()),
    ]

    def _post(*args: Any, **kwargs: Any):
        return seq.pop(0)

    mocker.patch("requests.Session.post", side_effect=_post)
    with patch.dict(os.environ, {"INTERACIVE_SESSION": "1"}, clear=False):
        tokens = authenticate(host=test_server, force=True)
    assert "access_token" in tokens and "refresh_token" in tokens


def test_device_client_login_respects_slow_down(mocker: MockerFixture) -> None:
    """Handle slow_down then pending then success."""
    seq = [
        _RespSync(200, _device_start_payload(interval=0.5)),
        _RespSync(400, _oauth_error_payload("slow_down")),
        _RespSync(400, _oauth_error_payload("authorization_pending")),
        _RespSync(200, _token_success_payload()),
    ]

    def _post(*args: Any, **kwargs: Any):
        return seq.pop(0)

    mocker.patch("requests.Session.post", side_effect=_post)

    client = DeviceAuthClient(
        device_endpoint="https://api.example.com/auth/v2/device",
        token_endpoint="https://api.example.com/auth/v2/token",
        timeout=20,
    )
    tokens = client.login(auto_open=False)
    assert "access_token" in tokens


def test_device_client_timeout(mocker: MockerFixture) -> None:
    """If only 'authorization_pending' responses are received until timeout."""
    seq = [
        _RespSync(200, _device_start_payload(interval=0)),  # start
        _RespSync(400, _oauth_error_payload("authorization_pending")),
        _RespSync(400, _oauth_error_payload("authorization_pending")),
        _RespSync(400, _oauth_error_payload("authorization_pending")),
    ]

    def _post(*args: Any, **kwargs: Any):
        # Keep cycling the last pending response to force timeout
        if len(seq) > 1:
            return seq.pop(0)
        return seq[0]

    mocker.patch("requests.Session.post", side_effect=_post)

    client = DeviceAuthClient(
        device_endpoint="https://api.example.com/auth/v2/device",
        token_endpoint="https://api.example.com/auth/v2/token",
        timeout=1,  # very small timeout
    )
    with pytest.raises(AuthError, match="allotted time"):
        client.login(auto_open=False)


def test_device_client_access_denied(mocker: MockerFixture) -> None:
    """access_denied from token endpoint should raise AuthError immediately."""
    seq = [
        _RespSync(200, _device_start_payload(interval=1)),
        _RespSync(400, _oauth_error_payload("access_denied")),
    ]

    def _post(*args: Any, **kwargs: Any):
        return seq.pop(0)

    mocker.patch("requests.Session.post", side_effect=_post)

    client = DeviceAuthClient(
        device_endpoint="https://api.example.com/auth/v2/device",
        token_endpoint="https://api.example.com/auth/v2/token",
        timeout=10,
    )
    with pytest.raises(AuthError, match="access_denied"):
        client.login(auto_open=False)


def test_device_client_upstream_malformed(mocker: MockerFixture) -> None:
    """Missing required keys from /device/start should raise AuthError."""
    seq = [
        _RespSync(
            200,
            {  # missing 'device_code', etc.
                "verification_uri": "https://auth.example.com/verify",
                "expires_in": 600,
            },
        ),
    ]

    mocker.patch("requests.Session.post", side_effect=lambda *a, **k: seq.pop(0))

    client = DeviceAuthClient(
        device_endpoint="https://api.example.com/auth/v2/device/start",
        token_endpoint="https://api.example.com/auth/v2/device/token",
        timeout=10,
    )
    with pytest.raises(AuthError, match="missing 'device_code'"):
        client.login(auto_open=False)


# ---------------------------
# Code flow
# ---------------------------


def test_authenticate_with_code_login_flow(
    mocker: MockerFixture, free_port: int
) -> None:
    """Test interactive authentication flow."""
    auth_instance = Auth()
    token = deepcopy(auth_instance._auth_token)
    try:
        mocker.patch(
            "freva_client.auth.DeviceAuthClient.login",
            side_effect=mock_device_login,
        )
        port_patch = mocker.patch(
            "freva_client.auth.CodeAuthClient._find_free_port"
        )
        port_patch.return_value = free_port
        check_token = mock_token_data()
        mocker.patch("webbrowser.open", return_value=True)
        mock_post = mocker.patch("requests.post")
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = check_token
        mock_post.return_value.raise_for_status.return_value = None
        mocker.patch(
            "freva_client.utils.auth_utils.is_interactive_auth_possible",
            return_value=True,
        )
        with NamedTemporaryFile(suffix=".json") as temp_f:
            with patch.dict(
                os.environ, {TOKEN_ENV_VAR: temp_f.name}, clear=False
            ):
                auth_instance._auth_token = None
                login_thread = threading.Thread(
                    target=auth_instance.authenticate,
                    args=("https://example.com",),
                    kwargs={"force": True},
                    daemon=True,
                )
                login_thread.start()
                code = "fake-code"
                CodeAuthClient._wait_for_port("localhost", free_port)
                requests.get(f"http://localhost:{free_port}/callback?code={code}")
                login_thread.join(timeout=2)
                result_token = auth_instance._auth_token

            assert result_token["access_token"] == check_token["access_token"]
            assert result_token["refresh_token"] == check_token["refresh_token"]
        with NamedTemporaryFile(suffix=".json") as temp_f:
            with patch.dict(
                os.environ, {TOKEN_ENV_VAR: temp_f.name}, clear=False
            ):
                auth_instance._auth_token = None
                login_thread = threading.Thread(
                    target=auth_instance.authenticate,
                    args=("https://example.com",),
                    kwargs={"force": True},
                    daemon=True,
                )
                login_thread.start()
                CodeAuthClient._wait_for_port("localhost", free_port)
                requests.get(f"http://localhost:{free_port}/callback?foo=bar")
                login_thread.join(timeout=2)

    finally:
        auth_instance._auth_token = token


def test_authenticate_manual_failure_code_flow(
    mocker: MockerFixture, auth_instance: Auth, test_server: str
) -> None:
    """Test failure in manual login flow."""

    def timeout(host: str, port: int) -> None:
        raise TimeoutError("Timeout")

    mocker.patch(
        "freva_client.auth.DeviceAuthClient.login",
        side_effect=mock_device_login,
    )
    mocker.patch("webbrowser.open", retrun_value=True)
    mocker.patch(
        "freva_client.auth.CodeAuthClient._start_local_server", return_value=None
    )
    mocker.patch("threading.Event.wait", return_value=True)
    mocker.patch(
        "freva_client.auth.CodeAuthClient._wait_for_port", return_value=True
    )
    with patch("freva_rest.auth.oauth2.server_config.oidc_auth_ports", [8080]):
        with pytest.raises(OSError, match="No free ports"):
            authenticate(host=test_server, force=True)

    with pytest.raises(AuthError, match="Login failed"):
        authenticate(
            host=test_server,
            force=True,
        )
    mocker.patch("threading.Event.wait", return_value=False)
    with pytest.raises(AuthError, match="did not complete"):
        authenticate(host=test_server, force=True)
    with mocker.patch(
        "freva_client.auth.CodeAuthClient._wait_for_port", side_effect=timeout
    ):
        with pytest.raises(AuthError, match="Timeout"):
            authenticate(host=test_server, force=True)


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


# ---------------------------
# Other utilities.
# ---------------------------


def test_auth_utils(free_port: int) -> None:
    """Test the rest of the client auth utils."""

    assert is_job_env() is True
    with pytest.raises(TimeoutError):
        CodeAuthClient._wait_for_port("localhost", free_port)


def test_request_headers() -> None:
    from freva_rest.auth.oauth2 import set_request_header

    header, data = {"Content-Type": "foo"}, {}
    set_request_header("foo", "bar", data, header)
    assert header["Content-Type"] != "foo"
    assert "Authorization" in header
    assert header["Authorization"].startswith("Basic")
    header, data = {"Content-Type": "foo"}, {}
    set_request_header("foo", None, data, header)
    assert "Authorization" not in header
    assert "client_id" in data
