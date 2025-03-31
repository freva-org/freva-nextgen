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


def test_auth(test_server: str) -> None:
    """Test the authentication methods."""
    res1 = requests.post(
        f"{test_server}/auth/v2/token",
        data={"username": "foo", "password": "bar"},
    )
    assert res1.status_code == 401
    res2 = requests.post(
        f"{test_server}/auth/v2/token",
        data={
            "username": "janedoe",
            "password": "janedoe123",
            "grant_type": "password",
        },
    )
    assert res2.status_code == 200


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
    ).load()
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
    data = requests.get(
        f"{files[0]}/status",
        params={"timeout": 3},
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert data.status_code == 200


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
    data = requests.get(
        f"{test_server}/data-portal/foo.zarr/status",
        params={"timeout": 5},
        headers={"Authorization": f"Bearer {token}"},
        timeout=7,
    )
    assert data.status_code == 404
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

    data = requests.get(
        f"{files[0]}/status",
        params={"timeout": 3},
        headers={"Authorization": f"Bearer {token}"},
        timeout=3,
    )
    assert data.status_code >= 500
    for _ in range(2):
        res3 = requests.get(
            f"{test_server}/databrowser/load/freva/",
            params={"file": "*.json"},
            headers={"Authorization": f"Bearer {token}"},
            stream=True,
            timeout=3,
        )
        assert res3.status_code == 201
        files = list(res3.iter_lines(decode_unicode=True))
        assert files
        time.sleep(4)
        res = requests.get(
            f"{files[0]}/status",
            params={"timeout": 3},
            headers={"Authorization": f"Bearer {token}"},
            timeout=3,
        )
        assert res.status_code >= 500


def test_no_broker(test_server: str, auth: Dict[str, str]) -> None:
    """Test the behviour if no broker is present."""
    with mock.patch(
        "freva_rest.databrowser_api.core.create_redis_connection", None
    ):
        res = requests.get(
            f"{test_server}/databrowser/load/freva/",
            params={"dataset": "cmip6-fs"},
            headers={"Authorization": f"Bearer {auth['access_token']}"},
            timeout=7,
            stream=True,
        )
        file = list(res.iter_lines(decode_unicode=True))[0]
        assert "error" in file


def test_no_cache(test_server: str, auth: Dict[str, str]) -> None:
    """Test the behviour if no cache is present."""
    _id = "c0f32204-57a7-5157-bdc8-a79cee618f70.zarr"
    with mock.patch("freva_rest.utils.REDIS_CACHE", None):
        with mock.patch("freva_rest.utils.CONFIG.redis_user", "foo"):
            res = requests.get(
                f"{test_server}/data-portal/zarr/{_id}/status",
                headers={"Authorization": f"Bearer {auth['access_token']}"},
                timeout=7,
            )
            assert res.status_code == 503
    with mock.patch("freva_rest.utils.REDIS_CACHE", None):
        with mock.patch("freva_rest.utils.CONFIG.api_services", ""):
            os.environ["API_SERVICES"] = "databrowser"
            res = requests.get(
                f"{test_server}/data-portal/zarr/{_id}/status",
                headers={"Authorization": f"Bearer {auth['access_token']}"},
                timeout=7,
            )
        assert res.status_code == 503
