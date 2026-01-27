"""Tests for generic Zarr key retrieval endpoint"""

from typing import Dict

import pytest
import requests

# Mark tests as belonging to the portal_endpoints and REST API domains.  These
# tests exercise the generic Zarr key endpoint provided by the Freva data
# portal.  They can be selected via the ``-m portal_endpoints`` marker and
# remain part of the REST API test suite.
pytestmark = [pytest.mark.portal_endpoints, pytest.mark.rest]


def test_zarr_generic_invalid_key(
    test_server: str, auth: Dict[str, str]
) -> None:
    """
    Test the catch-all zarr key endpoint with an invalid nested key.

    This test loads a sample dataset, verifies that the root metadata
    endpoint works and then requests a nested path that does not exist.
    The request should return a client error (HTTP 400 or 404).
    """
    token = auth["access_token"]
    res = requests.get(
        f"{test_server}/databrowser/load/freva/",
        params={"dataset": "cmip6-fs"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=7,
        stream=True,
    )
    assert res.status_code == 201
    files = list(res.iter_lines(decode_unicode=True))
    assert files
    # root metadata call should succeed
    meta = requests.get(
        f"{files[0]}/.zmetadata",
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert meta.status_code == 200
    # request invalid nested path
    bad = requests.get(
        f"{files[0]}/foo/bar/.zarray",
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert bad.status_code in (400, 404)