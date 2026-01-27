"""Test for the authorisation utilities."""

from typing import Any, Dict
from unittest.mock import AsyncMock, patch

import jwt
import requests
from aiohttp import ClientTimeout
from pytest_mock import MockerFixture

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


def test_missing_ocid_server(test_server: str, mocker: MockerFixture) -> None:
    """Test the behviour of a missing ocid server."""
    mocker.patch("freva_rest.auth.auth.timeout", ClientTimeout(total=0.5))
    mocker.patch("freva_rest.auth.auth._auth", None)
    for url in ("", "http://example.org/foo", "http://muhah.zupap"):
        mocker.patch("freva_rest.auth.auth.discovery_url", url)
        res1 = requests.get(
            f"{test_server}/auth/v2/status",
            headers={"Authorization": "Bearer foo"},
        )
        assert res1.status_code == 503
        # mock the optional auth in extended search endpoint
        res2 = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            headers={"Authorization": "Bearer foo"},
        )
        assert res2.status_code == 200
        res3 = requests.get(f"{test_server}/.well-known/openid-configuration")
        assert res3.status_code == 503


def test_well_kown_endpoint(test_server: str) -> None:
    """Test the .well-kown oidc endpoint when it is available."""
    res = requests.get(f"{test_server}/.well-known/openid-configuration")
    assert res.status_code == 200


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


def test_logout(test_server: str) -> None:
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
