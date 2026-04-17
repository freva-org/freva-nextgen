"""Test for the authorisation utilities."""

import requests


def test_status_forbidden_invalid_token(
    test_server: str,
) -> None:
    """A token signed with a wrong key should be rejected with 401."""
    from cryptography.hazmat.primitives.asymmetric import rsa
    import jwt

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
    from freva_rest.auth import token_issuer
    import jwt

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
