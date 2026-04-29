"""Integration tests for the data-portal zarr endpoints.

These tests exercise zarr conversion, metadata routing, chunk fetching,
and error handling via HTTP against a live test server with a Redis broker.
"""

import re
from typing import Any, Dict, List, Tuple

import pytest
import requests

pytestmark = [pytest.mark.portal_endpoints, pytest.mark.rest]


def _extract_token_and_base(zarr_url: str) -> Tuple[str, str]:
    """Extract the token and base URL from a zarr URL."""
    m = re.search(r"/zarr/([^/]+)\.zarr$", zarr_url)
    assert m, f"Unexpected zarr url format: {zarr_url}"
    token = m.group(1)
    base = zarr_url.rsplit("/", 1)[0]
    return token, base


def _first_zarray_key(zmeta: Dict[str, Any]) -> str:
    """Return the first variable .zarray key from .zmetadata."""
    md = zmeta.get("metadata", {})
    keys = sorted(k for k in md if k.endswith("/.zarray"))
    assert keys, f"No .zarray keys in .zmetadata: {list(md)[:20]}"
    return keys[0]


def _origin_chunk_id(zarray: Dict[str, Any]) -> str:
    """Return the all-zeros chunk id for a given .zarray."""
    shape = zarray.get("shape")
    assert isinstance(shape, list) and shape, f"Unexpected shape: {shape}"
    return ".".join(["0"] * len(shape))


class TestZarrConversion:
    """Tests for the zarr/convert endpoint."""

    def _convert(self, test_server: str, token: str) -> Dict[str, Any]:
        """Helper: convert cmip6-fs files and return the response."""
        files = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"dataset": "cmip6-fs"},
            timeout=10,
        ).text.splitlines()
        assert files
        res = requests.post(
            f"{test_server}/data-portal/zarr/convert",
            json={"path": files},
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        assert res.status_code == 200, res.text
        out = res.json()
        assert "urls" in out and out["urls"]
        return out

    def test_convert_returns_urls(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """Conversion returns a list of zarr URLs."""
        out = self._convert(test_server, auth["access_token"])
        assert isinstance(out["urls"], list)
        assert all(".zarr" in u for u in out["urls"])


class TestZarrMetadata:
    """Tests for metadata routing (.zmetadata, .zgroup, .zattrs)."""

    @pytest.fixture(autouse=True)
    def _setup(self, test_server: str, auth: Dict[str, str]) -> None:
        """Convert files once and expose the base URL + token."""
        self.access = auth["access_token"]
        self.headers = {"Authorization": f"Bearer {self.access}"}
        files = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"dataset": "cmip6-fs"},
            timeout=10,
        ).text.splitlines()
        res = requests.post(
            f"{test_server}/data-portal/zarr/convert",
            json={"path": files},
            headers=self.headers,
            timeout=30,
        )
        assert res.status_code == 200
        token, _ = _extract_token_and_base(res.json()["urls"][0])
        self.base = f"{test_server}/data-portal/zarr/{token}.zarr"

    def test_zmetadata_returns_metadata_dict(self) -> None:
        """Root .zmetadata returns a dict with a metadata key."""
        r = requests.get(
            f"{self.base}/.zmetadata",
            headers=self.headers,
            params={"timeout": 20},
            timeout=30,
        )
        assert r.status_code == 200
        zmeta = r.json()
        assert "metadata" in zmeta
        assert isinstance(zmeta["metadata"], dict)

    def test_zarr_v3_not_supported(self) -> None:
        """Requesting zarr.json returns 404."""
        r = requests.get(
            f"{self.base}/zarr.json",
            headers=self.headers,
            timeout=10,
        )
        assert r.status_code == 404
        assert "v3" in r.json()["detail"].lower()

    def test_variable_zarray(self) -> None:
        """Variable-level .zarray returns shape metadata."""
        r = requests.get(
            f"{self.base}/.zmetadata",
            headers=self.headers,
            params={"timeout": 20},
            timeout=30,
        )
        zarray_key = _first_zarray_key(r.json())
        r = requests.get(
            f"{self.base}/{zarray_key}",
            headers=self.headers,
            params={"timeout": 20},
            timeout=30,
        )
        assert r.status_code == 200
        assert "shape" in r.json()


class TestZarrChunks:
    """Tests for on-demand chunk loading."""

    @pytest.fixture(autouse=True)
    def _setup(self, test_server: str, auth: Dict[str, str]) -> None:
        """Convert files and fetch metadata for chunk tests."""
        self.access = auth["access_token"]
        self.headers = {"Authorization": f"Bearer {self.access}"}
        files = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"dataset": "cmip6-fs"},
            timeout=10,
        ).text.splitlines()
        res = requests.post(
            f"{test_server}/data-portal/zarr/convert",
            json={"path": files},
            headers=self.headers,
            timeout=30,
        )
        assert res.status_code == 200
        token, _ = _extract_token_and_base(res.json()["urls"][0])
        self.base = f"{test_server}/data-portal/zarr/{token}.zarr"

        r = requests.get(
            f"{self.base}/.zmetadata",
            headers=self.headers,
            params={"timeout": 20},
            timeout=30,
        )
        assert r.status_code == 200
        self.zmeta = r.json()

    def test_chunk_fetch_returns_bytes(self) -> None:
        """Fetching a data chunk returns binary content."""
        zarray_key = _first_zarray_key(self.zmeta)
        var_path = zarray_key.rsplit("/", 1)[0]

        r = requests.get(
            f"{self.base}/{zarray_key}",
            headers=self.headers,
            params={"timeout": 20},
            timeout=30,
        )
        zarray = r.json()
        chunk_id = _origin_chunk_id(zarray)

        r = requests.get(
            f"{self.base}/{var_path}/{chunk_id}",
            headers=self.headers,
            params={"timeout": 30},
            timeout=60,
        )
        assert r.status_code == 200
        assert r.headers.get("content-type", "").startswith("application/octet-stream")
        assert len(r.content) > 0

    def test_nonexistent_variable_returns_404(self) -> None:
        """Requesting .zarray for a missing variable returns 404."""
        r = requests.get(
            f"{self.base}/no_such_variable/.zarray",
            headers=self.headers,
            timeout=10,
        )
        assert r.status_code in (404, 400)


class TestZarrErrorHandling:
    """Tests for invalid requests and error branches."""

    @pytest.fixture(autouse=True)
    def _setup(self, test_server: str, auth: Dict[str, str]) -> None:
        self.test_server = test_server
        self.headers = {"Authorization": f"Bearer {auth['access_token']}"}
        files = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"dataset": "cmip6-fs"},
            timeout=10,
        ).text.splitlines()
        res = requests.post(
            f"{test_server}/data-portal/zarr/convert",
            json={"path": files},
            headers=self.headers,
            timeout=30,
        )
        assert res.status_code == 200
        token, _ = _extract_token_and_base(res.json()["urls"][0])
        self.base = f"{test_server}/data-portal/zarr/{token}.zarr"

    def test_root_zarray_rejected(self) -> None:
        """Root-level .zarray without a variable prefix returns 400."""
        r = requests.get(
            f"{self.base}/.zarray",
            headers=self.headers,
            timeout=10,
        )
        assert r.status_code == 400

    def test_bare_key_without_slash_rejected(self) -> None:
        """A key with no slash separator returns 400."""
        r = requests.get(
            f"{self.base}/tas",
            headers=self.headers,
            timeout=10,
        )
        assert r.status_code == 400

    def test_nonexistent_subgroup(self) -> None:
        """Requesting .zgroup for a nonexistent subgroup returns 404."""
        r = requests.get(
            f"{self.base}/group0/.zgroup",
            headers=self.headers,
            timeout=10,
        )
        assert r.status_code in (404, 400)

    def test_invalid_token_returns_400(self) -> None:
        """A garbage zarr token returns 400."""
        r = requests.get(
            f"{self.test_server}/data-portal/zarr/NOT_A_REAL_TOKEN.zarr/.zmetadata",
            headers=self.headers,
            timeout=10,
        )
        assert r.status_code in (400, 401)
        assert "invalid" in r.json()["detail"].lower()
