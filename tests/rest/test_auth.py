"""Test for the authorisation utilities."""

from typing import Any, Dict
from unittest.mock import AsyncMock, patch, Mock, PropertyMock

import jwt
import requests
import pytest
from pytest_mock import MockerFixture

from tests.conftest import mock_token_data
from py_oidc_auth.exceptions import InvalidRequest


def test_missing_ocid_server(test_server: str, mocker: MockerFixture) -> None:
    """Test the behaviour of a missing oidc server."""
    from freva_rest.auth.oauth2 import auth as oidc_auth

    async def _noop() -> None:
        pass

    mocker.patch.object(oidc_auth, "_verifier", None)
    mocker.patch.object(oidc_auth, "_ensure_auth_initialized", _noop)

    for url in ("", "http://example.org/foo", "http://muhah.zupap"):
        mocker.patch.object(oidc_auth.config, "discovery_url", url)
        res1 = requests.get(
            f"{test_server}/auth/v2/status",
            headers={"Authorization": "Bearer foo"},
        )
        assert res1.status_code == 503
        # optional auth endpoint - still return 200
        res2 = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            headers={"Authorization": "Bearer foo"},
        )
        assert res2.status_code == 200
        res3 = requests.get(f"{test_server}/.well-known/openid-configuration")
        assert res3.status_code == 503


def test_well_kown_endpoint(test_server: str) -> None:
    """Test the .well-known oidc endpoint when it is available."""
    res = requests.get(f"{test_server}/.well-known/openid-configuration")
    assert res.status_code == 200


def test_callback(test_server: str) -> None:
    """Test the /callback endpoint."""
    params = {
        "code": "fake",
        "state": "teststate|http://localhost:8080/callback|dummy_code_verifier",
    }

    with patch(
        "py_oidc_auth.auth_base.oidc_request",
        new=AsyncMock(return_value=mock_token_data()),
    ):
        response = requests.get(f"{test_server}/auth/v2/callback", params=params)
        assert response.status_code == 200
        token = response.json()
        assert "access_token" in token
        access_token = jwt.decode(
            token["access_token"], options={"verify_signature": False}
        )
        assert access_token["result"] == "test_access_token"

    response = requests.get(f"{test_server}/auth/v2/callback", params=params)
    assert response.status_code in (400, 401)
    response = requests.get(f"{test_server}/auth/v2/callback")
    assert response.status_code == 400
    params = {
        "code": "fake",
        "state": "teststate,http://localhost:8080/callback",
    }
    response = requests.get(f"{test_server}/auth/v2/callback", params=params)
    assert response.status_code == 400


def test_auth_via_code_exchange(test_server: str) -> None:
    """Test the token endpoint."""
    with patch(
        "py_oidc_auth.auth_base.oidc_request",
        new=AsyncMock(return_value=mock_token_data()),
    ):
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
            token["access_token"], options={"verify_signature": False}
        )
        assert access_token["result"] == "test_access_token"

    res = requests.post(
        f"{test_server}/auth/v2/token",
        data={
            "code": "foo",
            "redirect_uri": "http://localhost:8080/callback",
        },
    )
    assert res.status_code in (400, 401)

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
    mocker.patch("py_oidc_auth.utils.get_userinfo", return_value={})
    res = requests.get(
        f"{test_server}/auth/v2/userinfo",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
        timeout=3,
    )
    assert 400 < res.status_code < 500


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
    from py_oidc_auth.utils import OIDCConfig

    mocker.patch.object(
        OIDCConfig,
        "oidc_overview",
        new_callable=PropertyMock,
        return_value={},
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
async def test_get_username_fallback_userinfo(mocker: MockerFixture) -> None:
    """Test fallback to userinfo when token lacks username (HelmholtzAAI case)."""
    from freva_rest.auth.oauth2 import get_username

    from py_oidc_auth.schema import IDToken as _IDToken
    mock_user = _IDToken(sub="456")
    mock_request = Mock(headers={"authorization": "Bearer xyz"})
    mock_userinfo = Mock(
        preferred_username=None, username="from_userinfo_endpoint", user_name=None
    )
    mocker.patch("py_oidc_auth.utils.query_user", return_value=mock_userinfo)

    result = await get_username(mock_user, mock_request)
    assert result == "from_userinfo_endpoint"


@pytest.mark.asyncio
async def test_get_username_fallback_sub(mocker: MockerFixture) -> None:
    """Test final fallback to sub when userinfo also fails."""
    from freva_rest.auth.oauth2 import get_username

    from py_oidc_auth.schema import IDToken as _IDToken
    mock_user = _IDToken(sub="randomnumber")
    mock_request = Mock(headers={"authorization": "Bearer xyz"})
    mocker.patch("py_oidc_auth.utils.query_user", side_effect=InvalidRequest(404))

    result = await get_username(mock_user, mock_request)
    assert result == "randomnumber"

def test_oidc_overview_cached(test_server: str) -> None:
    """Test oidc_overview returns cached result."""
    from freva_rest.config import ServerConfig
    config = ServerConfig()
    config._oidc_overview = {"issuer": "https://example.com"}

    result = config.oidc_overview
    assert result == {"issuer": "https://example.com"}