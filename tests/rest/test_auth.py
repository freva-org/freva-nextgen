"""Test for the authorisation utilities."""

from typing import Any, Dict
from unittest.mock import Mock, PropertyMock

import requests
import pytest
from pytest_mock import MockerFixture

from py_oidc_auth.exceptions import InvalidRequest


def test_missing_oidc_server(test_server: str, mocker: MockerFixture) -> None:
    """Test the behaviour of a missing oidc server."""
    from freva_rest.auth import auth as oidc_auth

    async def _noop() -> None:
        pass

    mocker.patch.object(oidc_auth, "_verifier", None)
    mocker.patch.object(oidc_auth, "_ensure_auth_initialized", _noop)

    for url in ("", "http://example.org/foo", "http://muhah.zupap"):
        mocker.patch.object(oidc_auth.config, "discovery_url", url)
        # optional auth endpoint - still return 200
        res2 = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            headers={"Authorization": "Bearer foo"},
        )
        assert res2.status_code == 200
        res3 = requests.get(f"{test_server}/auth/v2/.well-known/openid-configuration")
        assert res3.status_code == 503


def test_well_known_endpoint(test_server: str) -> None:
    """Test the .well-known oidc endpoint when it is available."""
    res = requests.get(f"{test_server}/auth/v2/.well-known/openid-configuration")
    assert res.status_code == 200

def test_oidc_overview_cached(test_server: str) -> None:
    """Test oidc_overview returns cached result."""
    from freva_rest.config import ServerConfig

    config = ServerConfig()
    config._oidc_overview = {"issuer": "https://example.com"}
    assert config.oidc_overview == {"issuer": "https://example.com"}
