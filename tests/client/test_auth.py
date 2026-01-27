"""Test for the authorisation utilities."""

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, patch, Mock, PropertyMock

import jwt
import pytest
import requests
from pytest_mock import MockerFixture
from typer.testing import CliRunner
from fastapi import HTTPException

from freva_client.auth import Auth
from freva_client.cli import app as cli_app
from freva_client.query import databrowser
from freva_client.utils.auth_utils import (
    TOKEN_ENV_VAR,
    AuthError,
)
from tests.conftest import mock_token_data


def raise_for_status() -> None:
    """Mock function used for requests result rais_for_status method."""
    raise requests.HTTPError("Invalid")


async def mock_request(*args: Any, **kwargs: Any) -> AsyncMock:
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json.return_value = mock_token_data()
    return mock_resp


async def mock_request_failed(*args: Any, **kwargs: Any) -> AsyncMock:

    mock_resp = AsyncMock()
    mock_resp.status = 401
    mock_resp.json.return_value = {"reason": "Forbidden"}
    return mock_resp


def test_authenticate_with_refresh_token(
    test_server: str, mocker: MockerFixture, auth_instance: Auth
) -> None:
    """Test authentication using a refresh token."""
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = {"refresh_token": "foo"}
        with patch("aiohttp.ClientSession.request", new=mock_request):
            with patch(
                "freva_client.auth.choose_token_strategy",
                return_value="refresh_token",
            ):
                auth_instance.authenticate(host=test_server)

                assert isinstance(auth_instance._auth_token, dict)
                token_data = jwt.decode(
                    auth_instance._auth_token["access_token"],
                    options={"verify_signature": False},
                )
                assert token_data["result"] == "test_access_token"
                assert (
                    auth_instance._auth_token["refresh_token"]
                    == "test_refresh_token"
                )
                auth_instance._auth_token["expires"] = 0
                auth_instance.authenticate(host=test_server)
                assert isinstance(auth_instance._auth_token, dict)
                token_data = jwt.decode(
                    auth_instance._auth_token["access_token"],
                    options={"verify_signature": False},
                )

                assert (
                    auth_instance._auth_token["refresh_token"]
                    == "test_refresh_token"
                )
                assert token_data["result"] == "test_access_token"

    finally:
        auth_instance._auth_token = token


def test_authenticate_with_refresh_token_failed(
    test_server: str, mocker: MockerFixture, auth_instance: Auth
) -> None:
    """Test authentication using a refresh token."""
    token = deepcopy(auth_instance._auth_token)

    def mock_login(url: str, port: int = 0, _timeout: int = 30) -> None:
        raise AuthError("foo")

    try:
        auth_instance._auth_token = {"refresh_token": "foo"}
        mocker.patch.object(auth_instance, "_login", side_effect=mock_login)
        with patch("aiohttp.ClientSession.request", new=mock_request_failed):
            with patch(
                "freva_client.auth.choose_token_strategy",
                return_value="refresh_token",
            ):
                with pytest.raises(AuthError):
                    auth_instance.authenticate(host=test_server)
    finally:
        auth_instance._auth_token = token


def test_authenticate(
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

    exp = int(datetime.now().timestamp() - 3600)
    new_token = {
        "access_token": jwt.encode(token_data, "PyJWK"),
        "token_type": "Bearer",
        "expires": exp,
        "refresh_token": "old_refresh_token",
        "refresh_expires": now + 7200,
        "scope": "profile email address",
    }
    try:
        with patch("aiohttp.ClientSession.request", new=mock_request):
            auth_instance._auth_token = new_token
            auth_instance.authenticate(host=test_server)
            assert isinstance(auth_instance._auth_token, dict)
            assert int(auth_instance._auth_token["expires"]) > exp
            assert isinstance(auth_instance._auth_token, dict)
            access_token = jwt.decode(
                auth_instance._auth_token["access_token"],
                options={"verify_signature": False},
            )

            assert access_token["result"] == "test_access_token"
            assert (
                auth_instance._auth_token["refresh_token"] == "test_refresh_token"
            )
            auth_instance._auth_token["refresh_expires"] = exp
            auth_instance._auth_token["expires"] = exp
            with pytest.raises(AuthError):
                auth_instance.authenticate(host=test_server)

    finally:
        auth_instance._auth_token = token


def test_callback(test_server: str):
    """Test the /callback endpoint."""

    params = {
        "code": "fake",
        "state": "teststate|http://localhost:8080/callback|dummy_code_verifier",
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
                "code_verifier": "dummy_code_verifier",
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
        params={"redirect_uri": "http://localhost/callback"},
    )
    assert res.status_code == 400
    res = requests.get(
        f"{test_server}/auth/v2/login",
        params={"redirect_uri": "http://localhost:53105/callback"},
    )
    assert res.status_code == 200
    res = requests.get(f"{test_server}/auth/v2/login")
    assert res.status_code == 400


@pytest.mark.parametrize(
    "strategy, expected",
    [
        ("browser_auth", None),
        ("access_token", "whatever"),  # note the correct spelling
    ],
)
def test_auth_token_simple_strategies(
    strategy: str, expected: Optional[str]
) -> None:
    """The databrowser auth_token preperty for non-refresh methods."""
    auth_instance = Auth()
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None
        db = databrowser()
        with (
            patch("freva_client.query.load_token", return_value="whatever"),
            patch(
                "freva_client.query.choose_token_strategy",
                return_value=strategy,
            ),
        ):
            result = db.auth_token
            assert result == expected
    finally:
        auth_instance._auth_token = token


def test_auth_token_refresh_token_calls_authenticate():
    auth_instance = Auth()
    token = deepcopy(auth_instance._auth_token)
    try:
        auth_instance._auth_token = None
        db = databrowser()
        with (
            patch("freva_client.query.load_token", return_value="whatever"),
            patch(
                "freva_client.query.choose_token_strategy",
                return_value="refresh_token",
            ),
            patch.object(
                db._auth, "authenticate", return_value="NEW_TOKEN"
            ) as mock_auth,
        ):
            result = db.auth_token
            mock_auth.assert_called_once_with(config=db._cfg)
            assert result == "NEW_TOKEN"
    finally:
        auth_instance._auth_token = token


def test_cli_auth(
    mocker: MockerFixture,
    test_server: str,
    cli_runner: CliRunner,
    auth_instance: Auth,
) -> None:
    """Test authentication."""
    old_token = deepcopy(auth_instance._auth_token)

    mocker.patch.object(auth_instance, "_login", return_value=mock_token_data())
    mocker.patch(
        "freva_client.utils.auth_utils.is_interactive_auth_possible",
        return_value=True,
    )
    try:

        with NamedTemporaryFile(suffix=".json") as temp_f:
            auth_instance._auth_token = None
            with patch.dict(
                os.environ, {TOKEN_ENV_VAR: temp_f.name}, clear=False
            ):
                res = cli_runner.invoke(cli_app, ["auth", "--host", test_server])
            assert res.exit_code == 0
            assert res.stdout
            assert "access_token" in json.loads(res.stdout)
        with NamedTemporaryFile(suffix=".json") as temp_f:
            auth_instance._auth_token = None
            Path(temp_f.name).write_text(res.stdout)
            res = cli_runner.invoke(
                cli_app,
                ["auth", "--host", test_server, "--token-file", temp_f.name],
            )
            assert res.exit_code == 0
            assert res.stdout

    finally:
        auth_instance._auth_token = old_token


def test_cli_auth_failed(
    mocker: MockerFixture,
    test_server: str,
    cli_runner: CliRunner,
    auth_instance: Auth,
) -> None:
    """Test authentication."""
    old_token = deepcopy(auth_instance._auth_token)

    def failed_login(*args, **kwargs):
        raise AuthError("Timetout")

    mocker.patch.object(auth_instance, "_login", side_effect=failed_login)
    mocker.patch(
        "freva_client.auth.choose_token_strategy", return_value="browser_auth"
    )
    try:
        res = cli_runner.invoke(cli_app, ["auth", "--host", test_server])
        assert res.exit_code == 1

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


def test_checkuser(
    mocker: MockerFixture, test_server: str, auth: Dict[str, str]
) -> None:
    """Test getting the user info."""
    res = requests.get(
        f"{test_server}/auth/v2/checkuser",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
        timeout=3,
    )
    assert res.status_code == 200
    assert "pw_name" in res.json()


def test_userinfo_failed(
    mocker: MockerFixture, test_server: str, auth: Dict[str, str]
) -> None:
    """Test failing user info."""

    mocker.patch("freva_rest.auth.oauth2.get_userinfo", return_value={})
    with patch("aiohttp.ClientSession.request", new=mock_request):
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


def test_logout(test_server: str, mocker: MockerFixture) -> None:
    """Test the logout endpoint."""
    res1 = requests.get(f"{test_server}/auth/v2/logout", allow_redirects=False)
    assert res1.status_code == 307

    redirect_uri = "https://somewhere.com/afterlogout"
    res2 = requests.get(
        f"{test_server}/auth/v2/logout",
        params={"post_logout_redirect_uri": redirect_uri},
        allow_redirects=False,
    )
    assert res2.status_code == 307

    # no end_session_endpoint
    mocker.patch(
        "freva_rest.config.ServerConfig.oidc_overview",
        new_callable=PropertyMock,
        return_value={}
    )

    res3 = requests.get(f"{test_server}/auth/v2/logout", allow_redirects=False)
    assert res3.status_code == 307
    assert res3.headers["location"] == "/"

    res4 = requests.get(
        f"{test_server}/auth/v2/logout",
        params={"post_logout_redirect_uri": redirect_uri},
        allow_redirects=False,
    )
    assert res4.status_code == 307
    assert res4.headers["location"] == redirect_uri


@pytest.mark.asyncio
async def test_get_username_fallback_userinfo(mocker: MockerFixture):
    """Test fallback to userinfo when token lacks username.
    HelmholtzAAI case"""
    from freva_rest.auth.oauth2 import get_username

    mock_user = Mock(
        preferred_username=None,
        username=None,
        user_name=None,
        sub="456"
    )
    mock_user.keys.return_value = ["sub"]
    mock_user.__getitem__ = lambda self, k: "456" if k == "sub" else None

    mock_request = Mock(headers={"authorization": "Bearer xyz"})

    mock_userinfo = Mock(
        preferred_username=None,
        username="from_userinfo_endpoint",
        user_name=None
    )

    mocker.patch("freva_rest.auth.oauth2.query_user", return_value=mock_userinfo)
    
    result = await get_username(mock_user, mock_request)
    assert result == "from_userinfo_endpoint"


@pytest.mark.asyncio
async def test_get_username_fallback_sub(mocker: MockerFixture):
    """Test final fallback to sub when userinfo also fails."""
    from freva_rest.auth.oauth2 import get_username
    
    mock_user = Mock(
        preferred_username=None,
        username=None,
        user_name=None,
        sub="randomnumebr"
    )
    mock_user.keys.return_value = ["sub"]
    mock_user.__getitem__ = lambda self, k: "randomnumebr" if k == "sub" else None
    
    mock_request = Mock(headers={"authorization": "Bearer xyz"})
    
    mocker.patch("freva_rest.auth.oauth2.query_user", side_effect=HTTPException(404))
    
    result = await get_username(mock_user, mock_request)
    assert result == "randomnumebr"
