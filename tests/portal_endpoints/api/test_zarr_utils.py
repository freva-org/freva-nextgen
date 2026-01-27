"""Test for zarr utilities."""

import time
from typing import Dict, List

import pytest
import requests
from pytest_mock import MockerFixture

pytestmark = [pytest.mark.portal_endpoints, pytest.mark.rest]


@pytest.mark.parametrize(
    "headers",
    [
        {},  # no Authorization header
        lambda auth: {"Authorization": f"Bearer {auth['access_token']}"},
    ],
)
def test_status_forbidden(
    test_server: str,
    mocker: MockerFixture,
    auth: Dict[str, str],
    headers,
) -> None:
    """
    Verify that invalid or missing token claims result in a 401 response.
    """
    # Resolve headers if it's a callable (so auth can be used)
    resolved_headers = headers(auth) if callable(headers) else headers

    # Force token_field_matches to return False for all calls
    with mocker.patch(
        "freva_rest.auth.oauth2.token_field_matches", return_value=False
    ):
        res = requests.get(
            f"{test_server}/data-portal/zarr-utils/status",
            params={"url": ["foo.zar"]},
            headers=resolved_headers,
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
