"""Test for the authorisation utilities."""

from typing import Any, Dict
from unittest.mock import Mock, PropertyMock

import requests
import pytest
from pytest_mock import MockerFixture

from py_oidc_auth.exceptions import InvalidRequest


def test_missing_oidc_server(test_server: str, mocker: MockerFixture) -> None:
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


def test_well_known_endpoint(test_server: str) -> None:
    """Test the .well-known oidc endpoint when it is available."""
    res = requests.get(f"{test_server}/.well-known/openid-configuration")
    assert res.status_code == 200


def test_token_status(test_server: str, auth: Dict[str, str]) -> None:
    """Test status endpoint returns claims for a valid token."""
    res = requests.get(
        f"{test_server}/auth/v2/status",
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res.status_code == 200
    assert "exp" in res.json()

    res_bad = requests.get(
        f"{test_server}/auth/v2/status",
        headers={"Authorization": "Bearer bad"},
    )
    assert res_bad.status_code != 200




@pytest.mark.asyncio
async def test_get_username_fallback_userinfo(mocker: MockerFixture) -> None:
    """Test fallback to userinfo when token lacks username (HelmholtzAAI case)."""
    from freva_rest.auth.oauth2 import get_username
    from py_oidc_auth.schema import IDToken

    mock_user = IDToken(sub="456")
    mock_request = Mock(headers={"authorization": "Bearer xyz"})
    mock_userinfo = Mock(
        preferred_username=None,
        username="from_userinfo_endpoint",
        user_name=None,
    )
    mocker.patch("py_oidc_auth.utils.query_user", return_value=mock_userinfo)

    result = await get_username(mock_user, mock_request)
    assert result == "from_userinfo_endpoint"


@pytest.mark.asyncio
async def test_get_username_fallback_sub(mocker: MockerFixture) -> None:
    """Test final fallback to sub when userinfo also fails."""
    from freva_rest.auth.oauth2 import get_username
    from py_oidc_auth.schema import IDToken

    mock_user = IDToken(sub="randomnumber")
    mock_request = Mock(headers={"authorization": "Bearer xyz"})
    mocker.patch("py_oidc_auth.utils.query_user", side_effect=InvalidRequest(404))

    result = await get_username(mock_user, mock_request)
    assert result == "randomnumber"


def test_oidc_overview_cached(test_server: str) -> None:
    """Test oidc_overview returns cached result."""
    from freva_rest.config import ServerConfig

    config = ServerConfig()
    config._oidc_overview = {"issuer": "https://example.com"}
    assert config.oidc_overview == {"issuer": "https://example.com"}
