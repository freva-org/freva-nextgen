"""Test for zarr utilities."""

import time
from typing import Dict, List

import pytest
import requests
from fastapi import HTTPException
from pytest_mock import MockerFixture

pytestmark = [pytest.mark.portal_endpoints, pytest.mark.rest]


def test_status_forbidden_no_token(test_server: str) -> None:
    """No Authorization header on a non-public URL should return 401."""
    res = requests.get(
        f"{test_server}/data-portal/zarr-utils/status",
        params={"url": ["foo.zar"]},
    )
    assert res.status_code == 401


def test_status_forbidden_invalid_token(
    test_server: str,
    auth: Dict[str, str],
) -> None:
    """A token signed with a wrong key should be rejected with 401."""
    from cryptography.hazmat.primitives.asymmetric import rsa
    import jwt

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


def test_aggregate_success(test_server: str, auth: Dict[str, str]) -> None:
    """Test if we can aggregate data via the rest api."""
    files = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"dataset": "agg"},
        timeout=10,
    ).text.splitlines()
    assert files

    res = requests.post(
        f"{test_server}/data-portal/zarr/convert",
        json={
            "path": files,
            "aggregate": "auto",
            "public": True,
            "join": "outer",
            "compat": "override",
            "data-vars": "minimal",
            "coords": "minimal",
            "dim": None,
            "group_by": None,
        },
        headers={"Authorization": f"Bearer {auth['access_token']}"},
        timeout=10,
    )
    assert res.status_code == 200, res.text
    out = res.json()
    files = out["urls"][0]
    res = requests.get(
        f"{files}/.zmetadata",
        timeout=20,
    )
    res = requests.get(
        f"{test_server}/data-portal/zarr-utils/status",
        params={"url": files, "timeout": 8},
        timeout=10,
    )
    assert res.status_code == 200
    assert res.json()["status"] in (0, 4)


def test_aggregate_failed(
    test_server: str, auth: Dict[str, str], aggregation_files: List[str]
) -> None:
    """Test if we can aggregate data via the rest api."""
    res = requests.post(
        f"{test_server}/data-portal/zarr/convert",
        json={
            "path": aggregation_files,
            "aggregate": "auto",
            "dim": "ensemble",
            "public": True,
        },
        headers={"Authorization": f"Bearer {auth['access_token']}"},
        timeout=10,
    )
    assert res.status_code == 200, res.text
    out = res.json()
    url = out["urls"][0]
    res = requests.get(
        f"{url}/.zmetadata",
        timeout=20,
    )
    time.sleep(5)
    res = requests.get(
        f"{test_server}/data-portal/zarr-utils/status",
        params={"url": url, "timeout": 8},
        timeout=10,
    )
    assert res.status_code == 200
