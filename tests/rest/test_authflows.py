"""Test for the authorisation utilities."""

from typing import Dict

import requests
import pytest
from pytest_mock import MockerFixture


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
