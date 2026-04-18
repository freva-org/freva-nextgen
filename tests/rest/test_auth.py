"""Test for the authorisation utilities."""

import requests


def test_well_known_endpoint(test_server: str) -> None:
    """Test the .well-known oidc endpoint when it is available."""
    res = requests.get(f"{test_server}/auth/v2/.well-known/openid-configuration")
    assert res.status_code == 200
    doc = res.json()
    assert "jwks_uri" in doc
    res = requests.get(f"{test_server}/auth/v2/.well-known/jwks.json")
    assert res.status_code == 200


def test_oidc_overview_cached(test_server: str) -> None:
    """Test oidc_overview returns cached result."""
    from freva_rest.config import ServerConfig

    config = ServerConfig()
    config._oidc_overview = {"issuer": "https://example.com"}
    assert config.oidc_overview == {"issuer": "https://example.com"}
