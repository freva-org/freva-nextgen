"""Integration tests for the data-portal zarr endpoints.

These tests exercise zarr conversion, metadata routing, chunk fetching,
and error handling via HTTP against a live test server with a Redis broker.
"""

import re
import time
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


class TestZarrReduction:
    """Tests for temporal reduction on the zarr/convert endpoint."""

    @staticmethod
    def _files(test_server: str) -> List[str]:
        """Files to convert.

        Uses ``agg`` rather than ``cmip6-fs``: it is contiguous monthly
        data, so a coarser frequency is an actual reduction whatever the
        deployment holds.  Datasets that span disjoint periods make
        resampling *inflate* the axis instead, which is a real behaviour
        but too data-dependent to assert on here -- ``test_zarr_reducer``
        covers it against a fixed fixture.
        """
        files = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"dataset": "agg"},
            timeout=10,
        ).text.splitlines()
        assert files
        return files

    @staticmethod
    def _convert(
        test_server: str, token: str, files: List[str], **body: Any
    ) -> List[str]:
        res = requests.post(
            f"{test_server}/data-portal/zarr/convert",
            json={"path": files, **body},
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        assert res.status_code == 200, res.text
        urls: List[str] = res.json()["urls"]
        return urls

    @staticmethod
    def _zmetadata(url: str, token: str) -> Dict[str, Any]:
        base = url.rsplit("/", 1)[0]
        name = url.rsplit("/", 1)[-1]
        for _ in range(20):
            res = requests.get(
                f"{base}/{name}/.zmetadata",
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            if res.status_code == 503:
                time.sleep(1)
                continue
            assert res.status_code == 200, res.text
            return dict(res.json())
        raise AssertionError(f"Store never became ready: {url}")

    def test_reduction_changes_the_store_identity(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """Same paths, different reduction, different token.

        If these collided, the lazy re-trigger in ``read_redis_data`` would
        re-materialise one view under the other's cache key.
        """
        files = self._files(test_server)
        token = auth["access_token"]
        # Every variant must aggregate identically, so that the reduction is
        # the only thing that differs. Without `aggregate` the endpoint
        # returns one url per file, which is a different store entirely.
        plain = self._convert(test_server, token, files, aggregate="auto")
        monthly = self._convert(
            test_server, token, files, aggregate="auto", time_freq="monthly"
        )
        climatology = self._convert(
            test_server,
            token,
            files,
            aggregate="auto",
            time_freq="monthly",
            climatology=True,
        )
        yearly = self._convert(
            test_server, token, files, aggregate="auto", time_freq="yearly"
        )
        assert len({plain[0], monthly[0], climatology[0], yearly[0]}) == 4

    def test_defaulted_options_do_not_change_the_token(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """Explicit defaults must be canonicalised away.

        Otherwise a client that spells out the defaults misses the cache
        entry created by one that does not.
        """
        files = self._files(test_server)
        token = auth["access_token"]
        terse = self._convert(
            test_server, token, files, aggregate="auto", time_freq="yearly"
        )
        verbose = self._convert(
            test_server,
            token,
            files,
            aggregate="auto",
            time_freq="yearly",
            time_method="mean",
            climatology=False,
            min_coverage=0.0,
            dtype="float32",
        )
        assert terse == verbose

    def test_reduced_store_serves_decoded_metadata(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """A reduced store must be readable, with a shorter time axis."""
        files = self._files(test_server)
        token = auth["access_token"]

        plain = self._zmetadata(
            self._convert(test_server, token, files, aggregate="auto")[0], token
        )
        reduced = self._zmetadata(
            self._convert(
                test_server, token, files, aggregate="auto", time_freq="yearly"
            )[0],
            token,
        )
        keys = [k for k in plain["metadata"] if k.endswith("time/.zarray")]
        assert keys, "no time axis in the store"
        for key in keys:
            assert reduced["metadata"][key]["shape"][0] < (
                plain["metadata"][key]["shape"][0]
            ), f"{key} was not reduced"
        # `cell_methods` records what was done, per CF.
        variables = [
            key
            for key in reduced["metadata"]
            if key.endswith("/.zattrs") and key.count("/") == 1
        ]
        assert any(
            "time: mean" in str(reduced["metadata"][key].get("cell_methods", ""))
            for key in variables
        )

    def test_unknown_frequency_is_rejected(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """The closed vocabulary is enforced by the schema, before the broker."""
        res = requests.post(
            f"{test_server}/data-portal/zarr/convert",
            json={"path": self._files(test_server), "time_freq": "fortnightly"},
            headers={"Authorization": f"Bearer {auth['access_token']}"},
            timeout=30,
        )
        assert res.status_code == 422

    @pytest.mark.parametrize("bad", [-0.5, 1.5])
    def test_out_of_range_min_coverage_is_rejected(
        self, test_server: str, auth: Dict[str, str], bad: float
    ) -> None:
        res = requests.post(
            f"{test_server}/data-portal/zarr/convert",
            json={
                "path": self._files(test_server),
                "time_freq": "yearly",
                "min_coverage": bad,
            },
            headers={"Authorization": f"Bearer {auth['access_token']}"},
            timeout=30,
        )
        assert res.status_code == 422

    def test_upsampling_reports_a_readable_reason(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """The mock model data is monthly, so 6hourly is upsampling.

        Without the guard this inflates the time axis ~121x into a 99% NaN
        array that ``resample`` materialises eagerly in the worker.
        """
        files = self._files(test_server)
        token = auth["access_token"]
        url = self._convert(
            test_server, token, files, aggregate="auto", time_freq="6hourly"
        )[0]
        assert "not a reduction" in self._failure_reason(test_server, token, url)

    def _failure_reason(self, test_server: str, token: str, url: str) -> str:
        for _ in range(20):
            res = requests.get(
                f"{test_server}/data-portal/zarr-utils/status",
                params={"url": url},
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            body = res.json()
            if body.get("status") in (3, 4):
                time.sleep(1)
                continue
            return str(body.get("reason", ""))
        raise AssertionError(f"Job never settled: {url}")

    def test_meaningless_climatology_reports_a_readable_reason(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """Plan errors surface as a status reason, not a stack trace."""
        files = self._files(test_server)
        token = auth["access_token"]
        url = self._convert(
            test_server,
            token,
            files,
            aggregate="auto",
            # A yearly climatology would pool every year into one value:
            # meaningless, unlike the monthly one used above.
            time_freq="yearly",
            climatology=True,
        )[0]
        assert "not meaningful" in self._failure_reason(test_server, token, url)


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
