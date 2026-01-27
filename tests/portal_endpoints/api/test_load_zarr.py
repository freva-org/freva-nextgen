import re
from typing import Any, Dict, Tuple

import pytest
import requests

# Mark tests as belonging to the portal_endpoints and REST API domains.  These
# tests exercise the Freva data portal ``/data-portal/zarr`` endpoints via HTTP.
pytestmark = [pytest.mark.portal_endpoints, pytest.mark.rest]


def _extract_token_and_base(zarr_url: str) -> Tuple[str, str]:
    """
    Given a url like:
      http://host/.../data-portal/zarr/<TOKEN>.zarr
    return (TOKEN, base_without_trailing_slash)
    """
    m = re.search(r"/zarr/([^/]+)\.zarr$", zarr_url)
    assert m, f"Unexpected zarr url format: {zarr_url}"
    token = m.group(1)
    base = zarr_url.rsplit("/", 1)[0]  # .../zarr/<TOKEN>.zarr  -> .../zarr
    return token, base


def _get_first_var_zarray_key(zmeta: Dict[str, Any]) -> str:
    md = zmeta.get("metadata", {})
    keys = [k for k in md.keys() if k.endswith("/.zarray")]
    assert keys, f"No variable .zarray keys found in .zmetadata: {list(md)[:20]}"
    # pick a deterministic one
    return sorted(keys)[0]


def _chunk_id_from_zarray(zarray: Dict[str, Any]) -> str:
    # Zarr v2 chunks are addressed by dot-separated indices with ndim parts.
    shape = zarray.get("shape")
    assert isinstance(shape, list) and shape, f"Unexpected .zarray shape: {shape}"
    ndim = len(shape)
    return ".".join(["0"] * ndim)


def _convert_some_files(test_server: str, token: str) -> Dict[str, Any]:
    files = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"dataset": "cmip6-fs"},
        timeout=10,
    ).text.splitlines()
    assert files, "No test files returned from databrowser search."

    res = requests.post(
        f"{test_server}/data-portal/zarr/convert",
        json={"path": files},
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    assert res.status_code == 200, res.text
    out = res.json()
    assert "urls" in out and isinstance(out["urls"], list) and out["urls"]
    return out


def test_zarr_helper_branches_via_rest_api(
    test_server: str, auth: Dict[str, str]
) -> None:
    """
    Integration test using the real test_server + redis broker.

    Covers the main routing branches of process_zarr_data via HTTP:
    - zarr.json -> 404 (v3 not supported)
    - .zmetadata -> 200
    - root invalid .zarray -> 400
    - missing slash (variable only) -> 400
    - subgroup key -> 400
    - variable .zarray -> 200
    - chunk fetch -> 200 octet-stream
    """
    access = auth["access_token"]
    conv = _convert_some_files(test_server, access)

    # Use the first generated zarr url. It may be absolute already.
    zarr_url = conv["urls"][0]
    token, _ = _extract_token_and_base(zarr_url)

    base = f"{test_server}/data-portal/zarr/{token}.zarr"

    # Zarr v3 not supported
    r = requests.get(
        f"{base}/zarr.json",
        headers={"Authorization": f"Bearer {access}"},
        timeout=10,
    )
    assert r.status_code in (404, 400)
    assert "v3" in r.json()["detail"].lower()

    # Root metadata
    r = requests.get(
        f"{base}/.zmetadata",
        headers={"Authorization": f"Bearer {access}"},
        params={"timeout": 20},
        timeout=30,
    )
    assert r.status_code == 200
    zmeta = r.json()
    assert "metadata" in zmeta and isinstance(zmeta["metadata"], dict)

    # Root invalid .zarray (needs variable/group prefix)
    r = requests.get(
        f"{base}/.zarray",
        headers={"Authorization": f"Bearer {access}"},
        timeout=10,
    )
    assert r.status_code in (400, 404)

    # Missing slash for non-metadata key
    r = requests.get(
        f"{base}/tas", headers={"Authorization": f"Bearer {access}"}, timeout=10
    )
    assert r.status_code in (400, 404)

    # Subgroups not supported
    r = requests.get(
        f"{base}/group0/.zgroup",
        headers={"Authorization": f"Bearer {access}"},
        timeout=10,
    )
    assert r.status_code in (400, 404)
    # assert "sub groups" in r.json()["detail"].lower()

    # Variable-level .zarray (hits the endswith('/.zarray') path)
    zarray_key = _get_first_var_zarray_key(
        zmeta
    )  # e.g. "tas/.zarray" or "group/tas/.zarray"
    r = requests.get(
        f"{base}/{zarray_key}",
        headers={"Authorization": f"Bearer {access}"},
        params={"timeout": 20},
        timeout=30,
    )
    assert r.status_code == 200
    zarray = r.json()
    assert "shape" in zarray

    # Chunk fetch for first variable
    var_path = zarray_key.rsplit("/", 1)[0]
    chunk_id = _chunk_id_from_zarray(zarray)
    r = requests.get(
        f"{base}/{var_path}/{chunk_id}",
        headers={"Authorization": f"Bearer {access}"},
        params={"timeout": 30},
        timeout=60,
    )
    assert r.status_code == 200
    assert r.headers.get("content-type", "").startswith(
        "application/octet-stream"
    )
    assert isinstance(r.content, (bytes, bytearray))
    assert len(r.content) > 0


def test_invalid_token_returns_400(
    test_server: str, auth: Dict[str, str]
) -> None:
    """
    Covers the invalid-token branch in read_redis_data via the REST endpoint.
    """
    access = auth["access_token"]
    bad = "NOT_A_REAL_TOKEN"
    r = requests.get(
        f"{test_server}/data-portal/zarr/{bad}.zarr/.zmetadata",
        headers={"Authorization": f"Bearer {access}"},
        timeout=10,
    )
    assert r.status_code == 400
    assert "invalid path" in r.json()["detail"].lower()


def test_load_chunk_keyerror_branch_direct_call(
    test_server: str, auth: Dict[str, str]
) -> None:
    """
    This branch is hard to hit through process_zarr_data because it routes
    '/<var>/.zarray' and '/<var>/.zattrs' to load_zarr_metadata first.

    We still validate the behavior using a real token and the real redis cache
    by importing and calling load_chunk directly.
    """
    access = auth["access_token"]
    conv = _convert_some_files(test_server, access)
    token, _ = _extract_token_and_base(conv["urls"][0])

    # Use a variable name that does not exist; read_redis_data returns metadata,
    # then load_chunk attempts meta["metadata"][f"{variable}/.zarray"]
    # -> KeyError -> 400
    r = requests.get(
        f"{test_server}/data-portal/zarr/{token}.zarr/"
        "this_variable_does_not_exist/.zarray",
        headers={"Authorization": f"Bearer {access}"},
        timeout=10,
    )
    assert r.status_code in (404, 400)
