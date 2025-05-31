"""Test for the authorisation utilities."""

import json
import os
import threading
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, PropertyMock, patch

import jwt
import pytest
import requests
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from freva_client.auth import Auth, authenticate, start_local_server
from freva_client.cli import app as cli_app


def raise_for_status() -> None:
    """Mock function used for requests result rais_for_status method."""
    raise requests.HTTPError("Invalid")


def mock_token_data(
    valid_for: int = 3600,
    refresh_for: int = 7200,
) -> Dict[str, str]:
    now = int(datetime.now(timezone.utc).timestamp())
    token_data = {
        "result": "test_access_token",
        "exp": now + valid_for,
        "iat": now + valid_for,
        "auth_time": now,
        "aud": ["freva", "account"],
        "realm_access": {"groups": ["/foo"]},
    }
    return {
        "access_token": jwt.encode(token_data, "PyJWK"),
        "token_type": "Bearer",
        "expires": now + valid_for,
        "refresh_token": "test_refresh_token",
        "refresh_expires": now + refresh_for,
        "scope": "profile email address",
    }


async def mock_request(*args: Any, **kwargs: Any) -> AsyncMock:
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json.return_value = mock_token_data()
    return mock_resp


def test_missing_ocid_server(test_server: str) -> None:
    """Test the behviour of a missing ocid server."""
    for url in ("", "http://example.org/foo", "http://muhah.zupap"):
        with patch(
            "freva_rest.auth.auth.discovery_url",
            url,
        ):
            res = requests.get(
                f"{test_server}/auth/v2/status",
                headers={"Authorization": "Bearer foo"},
            )
            assert res.status_code == 503


def test_authenticate_with_refresh_token(
    test_server: str, mocker: MockerFixture, auth_instance: Auth
) -> None:
    """Test authentication using a refresh token."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None
        with patch("aiohttp.ClientSession.request", new=mock_request):
            auth_instance.authenticate(
                host=test_server, refresh_token="test_refresh_token"
            )

            assert isinstance(auth_instance._auth_token, dict)
            token_data = jwt.decode(
                auth_instance._auth_token["access_token"],
                options={"verify_signature": False},
            )
            assert token_data["result"] == "test_access_token"
            assert (
                auth_instance._auth_token["refresh_token"] == "test_refresh_token"
            )
            auth_instance._auth_token["expires"] = 0
            auth_instance.authenticate(host=test_server)
            assert isinstance(auth_instance._auth_token, dict)
            token_data = jwt.decode(
                auth_instance._auth_token["access_token"],
                options={"verify_signature": False},
            )

            assert (
                auth_instance._auth_token["refresh_token"] == "test_refresh_token"
            )
            assert token_data["result"] == "test_access_token"
    finally:
        auth_instance._auth_token = token


def test_refresh_token(
    test_server: str, mocker: MockerFixture, auth_instance: Auth
) -> None:
    """Test the token refresh functionality."""
    token = deepcopy(auth_instance._auth_token)
    now = int(datetime.now(timezone.utc).timestamp())
    token_data = {
        "result": "old_access_token",
        "exp": now - 3600,
        "iat": now - 3600,
        "auth_time": now,
        "aud": ["freva", "account"],
    }

    new_token = {
        "access_token": jwt.encode(token_data, "PyJWK"),
        "token_type": "Bearer",
        "expires": now - 3600,
        "refresh_token": "old_refresh_token",
        "refresh_expires": now + 7200,
        "scope": "profile email address",
    }
    try:
        with patch("aiohttp.ClientSession.request", new=mock_request):
            auth_instance._auth_token = new_token
            auth_instance.check_authentication(auth_url=f"{test_server}/auth/v2")
            assert isinstance(auth_instance._auth_token, dict)
            access_token = jwt.decode(
                auth_instance._auth_token["access_token"],
                options={"verify_signature": False},
            )

            assert access_token["result"] == "test_access_token"
            assert (
                auth_instance._auth_token["refresh_token"] == "test_refresh_token"
            )
        auth_instance._auth_token["refresh_expires"] = int(
            datetime.now().timestamp() - 3600
        )
        with pytest.raises(ValueError):
            auth_instance.check_authentication(auth_url=f"{test_server}/auth/v2")

    finally:
        auth_instance._auth_token = token


def test_callback(test_server: str):
    """Test the /callback endpoint."""

    params = {
        "code": "fake",
        "state": "teststate|http://localhost:8080/callback",
    }

    with patch("aiohttp.ClientSession.request", new=mock_request):

        response = requests.get(f"{test_server}/auth/v2/callback", params=params)

        assert response.status_code == 200
        token = response.json()
        assert "access_token" in token
        access_token = jwt.decode(
            token["access_token"],
            options={"verify_signature": False},
        )
        assert access_token["result"] == "test_access_token"

    response = requests.get(f"{test_server}/auth/v2/callback", params=params)
    assert response.status_code == 401
    response = requests.get(f"{test_server}/auth/v2/callback")
    assert response.status_code == 400
    params = {
        "code": "fake",
        "state": "teststate,http://localhost:8080/callback",
    }
    response = requests.get(f"{test_server}/auth/v2/callback", params=params)
    assert response.status_code == 400


def test_auth_via_code_exchange(test_server: str) -> None:
    """The the token endpoint."""

    with patch("aiohttp.ClientSession.request", new=mock_request):
        response = requests.post(
            f"{test_server}/auth/v2/token",
            data={
                "code": "fake",
                "redirect_uri": "http://localhost:8080/callback",
            },
        )

        assert response.status_code == 200
        token = response.json()
        access_token = jwt.decode(
            token["access_token"],
            options={"verify_signature": False},
        )
        assert access_token["result"] == "test_access_token"

    res = requests.post(
        f"{test_server}/auth/v2/token",
        data={
            "code": "foo",
            "redirect_uri": "http://localhost:8080/callback",
        },
    )
    assert res.status_code == 401

    res = requests.post(f"{test_server}/auth/v2/token")
    assert res.status_code == 400


def test_auth_login_endpoint(test_server: str) -> None:
    """Test the login endpoint."""

    res = requests.get(
        f"{test_server}/auth/v2/login",
        params={"redirect_uri": "http://localhost:8080/callback"},
    )
    assert res.status_code == 200
    res = requests.get(f"{test_server}/auth/v2/login")
    assert res.status_code == 400


def test_authenticate_with_login_flow(
    mocker: MockerFixture, auth_instance: Auth
) -> None:
    """Test interactive authentication flow."""
    token_data = mock_token_data()

    mock_open = mocker.patch("webbrowser.open")
    mock_server = mocker.patch("freva_client.auth.start_local_server")
    mock_post = mocker.patch("requests.post")

    mock_open.return_value = True
    mock_server.return_value = "fake_auth_code"
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = token_data
    mock_post.return_value.raise_for_status.return_value = None

    token = authenticate(host="https://example.com", force=True)

    assert token["access_token"] == token_data["access_token"]
    assert token["refresh_token"] == token_data["refresh_token"]
    mock_open.assert_called_once()


def test_check_authentication_refresh(
    mocker: MockerFixture, auth_instance: Auth
) -> None:
    """Test that expired access token triggers refresh."""
    new_token_data = mock_token_data(valid_for=3600, refresh_for=7200)
    expired_token = mock_token_data(valid_for=-10, refresh_for=7200)

    mock_post = mocker.patch("requests.post")
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = new_token_data
    mock_post.return_value.raise_for_status.return_value = None

    auth_instance._auth_token = expired_token
    token = auth_instance.check_authentication(auth_url="https://example.com")

    assert token["access_token"] == new_token_data["access_token"]


def test_auth_with_token_claims(mocker: MockerFixture, test_server: str) -> None:
    """Test the behviour of different token claim matches."""
    from freva_rest.config import _TokenFilter

    with patch("aiohttp.ClientSession.request", new=mock_request):

        mocker.patch(
            "freva_rest.utils.CONFIG._oidc_token_match",
            [_TokenFilter(claim="realm_access.groups", pattern="foo")],
        )
        response = requests.post(
            f"{test_server}/auth/v2/token",
            data={
                "code": "fake",
                "redirect_uri": "http://localhost:8080/callback",
            },
        )
        assert response.status_code == 200
        mocker.patch(
            "freva_rest.utils.CONFIG._oidc_token_match",
            [_TokenFilter(claim="realm_access.groups", pattern="bar")],
        )

        response = requests.post(
            f"{test_server}/auth/v2/token",
            data={
                "code": "fake",
                "redirect_uri": "http://localhost:8080/callback",
            },
        )
        assert response.status_code == 401


def test_authenticate_function_fallback_to_login(
    mocker: MockerFixture, auth_instance: Auth
) -> None:
    """Test fallback to login flow when refresh fails."""
    expired_token = mock_token_data(valid_for=-10, refresh_for=-10)
    login_token = mock_token_data()

    mock_post = mocker.patch("requests.post")
    login_mock = mocker.Mock(
        status_code=200,
        json=lambda: login_token,
        raise_for_status=lambda: None,
    )

    mock_post.side_effect = [
        requests.exceptions.HTTPError("refresh failed"),
        login_mock,
        login_mock,  # refresh fails
    ]
    mocker.patch("webbrowser.open")
    mocker.patch("freva_client.auth.start_local_server", return_value="code")

    auth_instance._auth_token = expired_token
    token = authenticate(
        host="https://example.com", refresh_token=expired_token, force=False
    )

    assert token["access_token"] == login_token["access_token"]
    token = authenticate(host="https://example.com", force=True)
    assert token["access_token"] == login_token["access_token"]


def test_authenticate_manual_failure(
    mocker: MockerFixture, auth_instance: Auth
) -> None:
    """Test failure in manual login flow."""
    mocker.patch("webbrowser.open")
    mocker.patch("freva_client.auth.start_local_server", return_value=None)

    with pytest.raises(ValueError, match="No code received"):
        authenticate(host="https://example.com", force=True)


def test_cli_auth(
    mocker: MockerFixture,
    test_server: str,
    cli_runner: CliRunner,
    auth_instance: Auth,
) -> None:
    """Test authentication."""
    old_token = deepcopy(auth_instance._auth_token)
    try:
        mocker.patch("webbrowser.open")
        mocker.patch("freva_client.auth.start_local_server", return_value=None)

        res = cli_runner.invoke(cli_app, ["auth", "--host", test_server])
        assert res.exit_code == 0
        assert res.stdout
        assert "access_token" in json.loads(res.stdout)
        with NamedTemporaryFile(suffix=".json") as temp_f:
            Path(temp_f.name).write_text(res.stdout)
            res = cli_runner.invoke(
                cli_app,
                ["auth", "--host", test_server, "--token-file", temp_f.name],
            )
            assert res.exit_code == 0
            assert res.stdout

    finally:
        auth_instance._auth_token = old_token


def test_userinfo(
    mocker: MockerFixture, test_server: str, auth: Dict[str, str]
) -> None:
    """Test getting the user info."""

    res = requests.get(
        f"{test_server}/auth/v2/userinfo",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
        timeout=3,
    )
    assert res.status_code == 200
    assert "last_name" in res.json()
    with mocker.patch("freva_rest.auth.get_userinfo", return_value={}):
        res = requests.get(
            f"{test_server}/auth/v2/userinfo",
            headers={"Authorization": f"Bearer {auth['access_token']}"},
            timeout=3,
        )
        assert res.status_code > 400 and res.status_code < 500


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


def test_get_overview(test_server: str) -> None:
    """Test the open id connect discovery endpoint."""
    res = requests.get(f"{test_server}/auth/v2/.well-known/openid-configuration")
    assert res.status_code == 200


def test_start_local_server_receives_code() -> None:
    """Test the OAuthCallbackHandler."""
    port = Auth.find_free_port()  # must not conflict with dev
    thread = threading.Thread(target=start_local_server, args=(port,))
    thread.daemon = True
    thread.start()
    time.sleep(0.1)

    response = requests.get(f"http://localhost:{port}/?code=abc123")

    assert response.status_code == 200
    assert "Login successful" in response.text

    port = Auth.find_free_port()  # must not conflict with dev
    thread = threading.Thread(target=start_local_server, args=(port,))
    thread.daemon = True
    thread.start()
    time.sleep(0.1)

    response = requests.get(f"http://localhost:{port}/?foo=abc123")
    assert response.status_code == 400
    assert "code not found" in response.text
