"""Test for loading the zarr enpoint."""

import os
import time
from tempfile import NamedTemporaryFile
from typing import Dict

import intake
import mock
import pytest
import requests
import xarray as xr
from fastapi import HTTPException
from pytest_mock import MockerFixture

from freva_rest.auth.presign import verify_token
from freva_rest.utils.base_utils import encode_path_token, sign_token_path


def test_zarr_conversion(test_server: str, auth: Dict[str, str]) -> None:
    """Test the single file loading functionlity."""
    token = auth["access_token"]
    files = requests.get(
        f"{test_server}/databrowser/data-search/freva/file",
        params={"dataset": "cmip6-fs"},
    ).text.splitlines()
    res = requests.get(
        f"{test_server}/data-portal/zarr/convert",
        params={"path": files},
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert res.status_code == 200
    out = res.json()
    assert "urls" in out
    assert isinstance(out["urls"], list)
    assert len(out["urls"]) == len(files)


def test_load_files_success(test_server: str, auth: Dict[str, str]) -> None:
    """Test loading single files."""
    token = auth["access_token"]
    res1 = requests.get(
        f"{test_server}/databrowser/load/freva/",
        params={"dataset": "cmip6-fs"},
        headers={"Authorization": "Bearer foo"},
        timeout=3,
    )
    assert res1.status_code == 401
    res2 = requests.get(
        f"{test_server}/databrowser/load/freva/",
        params={"dataset": "cmip6-fs"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
        stream=True,
    )
    assert res2.status_code == 201
    files = list(res2.iter_lines(decode_unicode=True))
    assert len(files) == 2
    time.sleep(4)
    # zarr metadata json
    data = requests.get(
        f"{files[0]}/.zmetadata",
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert data.status_code == 200
    assert "metadata" in data.json()
    data = requests.get(
        f"{files[0]}/.zgroup",
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert data.status_code == 200
    assert "zarr_format" in data.json()
    data = requests.get(
        f"{files[0]}/.zattrs",
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert data.status_code == 200
    assert "activity_id" in data.json()
    dset = xr.open_dataset(
        files[0],
        engine="zarr",
        storage_options={"headers": {"Authorization": f"Bearer {token}"}},
    )
    dset.load()
    assert "ua" in dset
    data = requests.get(
        f"{files[0]}/.zattrs",
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert data.status_code == 200
    res3 = requests.get(
        f"{test_server}/databrowser/load/freva/",
        params={"dataset": "cmip6-fs", "catalogue-type": "intake"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
        stream=True,
    )
    assert res3.status_code == 201
    with NamedTemporaryFile(suffix=".json") as temp_f:
        with open(temp_f.name, "w", encoding="utf-8") as stream:
            stream.write(res3.text)
        cat = intake.open_esm_datastore(temp_f.name)
    # Smoke test for intake, I don't really know what else todo.
    assert hasattr(cat, "df")

    for attr in (".zarray", ".zattrs"):
        data = requests.get(
            f"{files[0]}/lon/{attr}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=3,
        )
        assert data.status_code == 200


def test_zarr_utils(test_server: str, auth: Dict[str, str]) -> None:
    """Test utils"""
    # zarr metadata xarray-html-formatted
    token = auth["access_token"]
    res = requests.get(
        f"{test_server}/databrowser/load/freva/",
        params={"dataset": "cmip6-fs"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
        stream=True,
    )
    assert res.status_code == 201
    files = list(res.iter_lines(decode_unicode=True))
    print(files)
    data = requests.get(
        f"{test_server}/data-portal/zarr-utils/html",
        params={"url": f"{files[0]}", "timeout": 3},
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert data.status_code == 200
    assert "<div><" in data.text
    data = requests.get(
        f"{test_server}/data-portal/zarr-utils/status",
        params={"url": f"{files[0]}", "timeout": 3},
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert data.status_code == 200
    _id = encode_path_token("foo.zarr")
    data = requests.get(
        f"{test_server}/data-portal/zarr-utils/status",
        params={
            "url": f"{test_server}/data-portal/zarr/{_id}.zarr",
            "timeout": 3,
        },
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert data.status_code == 200
    assert data.json()["status"] == 5


def test_load_files_fail(test_server: str, auth: Dict[str, str]) -> None:
    """Test for things that can go wrong when loading the data."""
    token = auth["access_token"]
    res2 = requests.get(
        f"{test_server}/databrowser/load/freva/",
        params={"dataset": "*fs", "project": "cmip6"},
        headers={"Authorization": f"Bearer {token}"},
        stream=True,
        timeout=3,
    )
    assert res2.status_code == 201
    files = list(res2.iter_lines(decode_unicode=True))
    for attr in (".zarray", ".zattrs"):
        data = requests.get(
            f"{files[0]}/foo/{attr}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=3,
        )
        assert data.status_code in (404, 400)
    data = requests.get(
        f"{files[0]}/lon/.zgroup",
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert data.status_code in (400, 404)
    data = requests.get(
        f"{test_server}/data-portal/zarr/foobar.zarr/lon/.zmetadata",
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert data.status_code in (404, 400)
    res2 = requests.get(
        f"{test_server}/databrowser/load/freva/",
        params={"dataset": "foo"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert res2.status_code in (400, 404)
    with pytest.warns():
        for _ in range(2):
            res3 = requests.get(
                f"{test_server}/databrowser/load/freva/",
                params={"project": "mock"},
                headers={"Authorization": f"Bearer {token}"},
                stream=True,
                timeout=3,
            )
            files = list(res3.iter_lines(decode_unicode=True))
            assert len(files) == 1
            assert res3.status_code == 201


def test_no_broker(
    test_server: str, auth: Dict[str, str], mocker: MockerFixture
) -> None:
    """Test the behviour if no broker is present."""
    mocker.patch("freva_rest.utils.base_utils.create_redis_connection", "foo")
    res = requests.get(
        f"{test_server}/databrowser/load/freva/",
        params={"dataset": "cmip6-fs"},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
        timeout=7,
        stream=True,
    )
    file = list(res.iter_lines(decode_unicode=True))[0]
    assert "error" in file
    mocker.patch(
        "freva_rest.freva_data_portal.endpoints.create_redis_connection", "foo"
    )
    res = requests.get(
        f"{test_server}/data-portal/zarr/convert",
        params={"path": ["/foo/bar.nc"]},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
        timeout=7,
    )
    assert res.status_code == 500


def test_no_cache(
    test_server: str, auth: Dict[str, str], mocker: MockerFixture
) -> None:
    """Test the behviour if no cache is present."""

    _id = encode_path_token("foo.zarr")
    mocker.patch("freva_rest.utils.base_utils.REDIS_CACHE", None)
    with mock.patch("freva_rest.utils.base_utils.CONFIG.redis_user", "foo"):
        res = requests.get(
            f"{test_server}/data-portal/zarr-utils/status",
            params={"url": f"{test_server}/data-portal/zarr/foo.zarr"},
            headers={"Authorization": f"Bearer {auth['access_token']}"},
            timeout=7,
        )
        assert res.status_code == 400
        res = requests.get(
            f"{test_server}/data-portal/zarr-utils/status",
            params={"url": f"{test_server}/data-portal/zarr/{_id}.zarr"},
            headers={"Authorization": f"Bearer {auth['access_token']}"},
            timeout=7,
        )
        assert res.status_code == 503

    with mock.patch("freva_rest.utils.base_utils.CONFIG.services", ""):
        with mock.patch.dict(
            os.environ, {"API_SERVICES": "databrowser"}, clear=False
        ):
            res = requests.get(
                f"{test_server}/data-portal/zarr-utils/status",
                params={"url": f"{test_server}/data-portal/zarr/{_id}.zarr"},
                headers={"Authorization": f"Bearer {auth['access_token']}"},
                timeout=7,
            )
            assert res.status_code == 503
            res = requests.get(
                f"{test_server}/data-portal/zarr/convert",
                params={"path": ["/foo/bar.nc"]},
                headers={"Authorization": f"Bearer {auth['access_token']}"},
                timeout=7,
            )
            assert res.status_code == 503


def test_presigend_url(test_server: str, auth: Dict[str, str]) -> None:
    """Test pre-signing a url and accessing it."""

    res = requests.get(
        f"{test_server}/databrowser/load/freva/",
        params={"dataset": "cmip6-fs"},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
        timeout=7,
        stream=True,
    )
    assert res.status_code == 201
    protected_uri = list(res.iter_lines(decode_unicode=True))[0]
    res = requests.post(
        f"{test_server}/data-portal/share-zarr",
        json={"path": protected_uri},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res.status_code == 201
    assert "url" in res.json()
    public_uri = res.json()["url"]
    dset = xr.open_dataset(public_uri, engine="zarr")
    assert "ua" in dset.data_vars


def test_presigend_url_failed(
    test_server: str, auth: Dict[str, str], mocker: MockerFixture
) -> None:
    """The the functionlity of token/sig verification."""

    res = requests.get(
        f"{test_server}/databrowser/load/freva/",
        params={"dataset": "cmip6-fs"},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
        timeout=7,
        stream=True,
    )
    assert res.status_code == 201
    protected_uri = list(res.iter_lines(decode_unicode=True))[0]
    res = requests.post(
        f"{test_server}/data-portal/share-zarr",
        json={"path": protected_uri},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res.status_code == 201
    assert "url" in res.json()
    sig = res.json()["sig"]

    token = f'/zarr/{encode_path_token("/work.foo")}.zarr'
    token_bad, sig_bad = sign_token_path(token, -1)

    # Expired TTL
    res = requests.get(
        f"{test_server}/data-portal/share/{sig_bad}/{token_bad}.zarr/.zgroup"
    )
    assert res.status_code == 403
    assert "expired" in res.json()["detail"].lower()

    # Wrong signature
    res = requests.get(
        f"{test_server}/data-portal/share/{sig}/{token_bad}.zarr/.zgroup"
    )
    assert res.status_code == 403
    assert "invalid" in res.json()["detail"].lower()

    # Wrong path
    res = requests.post(
        f"{test_server}/data-portal/share-zarr",
        json={"path": "foo.zarr"},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res.status_code == 400

    res = requests.post(
        f"{test_server}/data-portal/share-zarr",
        json={"path": "/api/freva-nextgen/data-portal/zarr/foo.zarr"},
        headers={"Authorization": f"Bearer {auth['access_token']}"},
    )
    assert res.status_code == 400

    res = requests.get(f"{test_server}/data-portal/share/{sig}/foo.zarr/.zgroup")
    assert res.status_code >= 400


def test_zarr_token_verification() -> None:
    """Test the token verification."""
    with pytest.raises(HTTPException, match="Invalid share token"):
        verify_token("foo", "bar")
