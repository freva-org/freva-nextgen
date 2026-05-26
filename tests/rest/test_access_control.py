"""Tests for filesystem-level access control on data portal endpoints.

These tests verify that the REST API correctly enforces read permissions.
Permission denial is mocked at the ``check_read_permission`` level (above
the Redis cache) so that cached results from earlier tests do not
interfere.
"""

from typing import Dict, List
from unittest.mock import AsyncMock, patch

import pytest
import requests
from fastapi import HTTPException

pytestmark = [pytest.mark.portal_endpoints, pytest.mark.rest]


class TestPermissions:
    """Tests for permission checks on the zarr/convert endpoint."""

    _files = []
    _token = []

    def files(self, test_server: str) -> List[str]:
        if not self._files:
            files = requests.get(
                f"{test_server}/databrowser/data-search/freva/file",
                params={"dataset": "agg"},
                timeout=10,
            ).text.splitlines()
            assert files
            self._files += files
        return self._files[0]

    def make_zarr_url(self, test_server: str, access_token: str) -> str:
        if not self._token:
            res = requests.post(
                f"{test_server}/data-portal/zarr/convert",
                json={"path": self.files(test_server)},
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=10,
            )
            self._token += res.json()["urls"]
        return self._token[0]

    def make_request(
        self, test_server: str, access_token: str, method: str
    ) -> requests.Response:

        headers = {"Authorization": f"Bearer {access_token}"} if access_token else None
        if method == "convert":
            return requests.post(
                f"{test_server}/data-portal/zarr/convert",
                json={"path": self.files(test_server)},
                headers=headers,
                timeout=10,
            )
        else:
            token = self.make_zarr_url(test_server, access_token)
            return requests.get(
                f"{token}/.zmetadata",
                headers=headers,
                timeout=10,
            )

    @pytest.mark.parametrize("method", ["convert", "zarr"])
    def test_allowed(self, test_server: str, auth: Dict[str, str], method: str) -> None:
        """An authenticated user with read access can convert data."""
        res = self.make_request(test_server, auth["access_token"], method)
        assert res.status_code == 200

    @pytest.mark.parametrize("method", ["convert", "zarr"])
    def test_denied(self, test_server: str, auth: Dict[str, str], method: str) -> None:
        """A user without read access gets 403 on convert."""
        with patch(
            "freva_rest.freva_data_portal.utils.check_read_permission",
            new=AsyncMock(side_effect=HTTPException(status_code=403, detail="denied")),
        ):
            with patch(
                "freva_rest.freva_data_portal.endpoints.check_read_permission",
                new=AsyncMock(
                    side_effect=HTTPException(status_code=403, detail="denied")
                ),
            ):
                res = self.make_request(test_server, auth["access_token"], method)
                assert res.status_code == 403

    @pytest.mark.parametrize("method", ["convert", "zarr"])
    def test_no_auth(self, test_server: str, method: str) -> None:
        """Unauthenticated request to convert returns 401."""
        res = self.make_request(test_server, "", method)
        assert res.status_code == 401


class TestPresignPermissions:
    """Tests for permission checks on the share-zarr (presign) endpoint."""

    def test_presign_denied(self, test_server: str, auth: Dict[str, str]) -> None:
        """A user without read access gets 403 on presign."""
        files = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"dataset": "agg"},
            timeout=10,
        ).text.splitlines()
        res = requests.post(
            f"{test_server}/data-portal/zarr/convert",
            json={"path": files[0]},
            headers={"Authorization": f"Bearer {auth['access_token']}"},
            timeout=10,
        )
        assert res.status_code == 200
        zarr_url = res.json()["urls"][0]

        with patch(
            "freva_rest.freva_data_portal.endpoints.check_read_permission",
            new=AsyncMock(side_effect=HTTPException(status_code=403, detail="denied")),
        ):
            res = requests.post(
                f"{test_server}/data-portal/share-zarr",
                json={"path": zarr_url, "ttl_seconds": 600},
                headers={"Authorization": f"Bearer {auth['access_token']}"},
                timeout=10,
            )
        assert res.status_code == 403

    def test_presign_no_auth(self, test_server: str) -> None:
        """Unauthenticated request to presign returns 401."""
        res = requests.post(
            f"{test_server}/data-portal/share-zarr",
            json={
                "path": "http://localhost/data-portal/zarr/abc.zarr",
                "ttl_seconds": 600,
            },
            timeout=10,
        )
        assert res.status_code == 401


class TestGuestAccess:
    """Tests for guest user (no system username) access control."""

    _files = []

    def files(self, test_server: str) -> List[str]:
        if not self._files:
            files = requests.get(
                f"{test_server}/databrowser/data-search/freva/file",
                params={"dataset": "agg"},
                timeout=10,
            ).text.splitlines()
            assert files
            self._files += files
        return self._files

    def test_guest_denied_non_world_readable(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """A guest user cannot access non-world-readable data."""

        with patch(
            "freva_rest.freva_data_portal.utils.Cache.get",
            new=AsyncMock(return_value=b"0"),
        ):
            res = requests.post(
                f"{test_server}/data-portal/zarr/convert",
                json={"path": self.files(test_server)[0]},
                headers={"Authorization": f"Bearer {auth['access_token']}"},
                timeout=10,
            )
        assert res.status_code == 403

    def test_guest_allowed_world_readable(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """A guest user can access non-world-readable data."""

        with patch(
            "freva_rest.freva_data_portal.utils.Cache.get",
            new=AsyncMock(return_value=b"1"),
        ):
            res = requests.post(
                f"{test_server}/data-portal/zarr/convert",
                json={"path": self.files(test_server)[0]},
                headers={"Authorization": f"Bearer {auth['access_token']}"},
                timeout=10,
            )
        assert res.status_code == 200

    def test_broker_timeout(self, test_server: str, auth: Dict[str, str]) -> None:
        """Test broker timeout."""
        with patch(
            "freva_rest.freva_data_portal.utils.Cache.publish",
            new=AsyncMock(return_value=None),
        ):
            with patch(
                "freva_rest.freva_data_portal.utils.Cache.get",
                new=AsyncMock(return_value=None),
            ):
                with patch(
                    "freva_rest.freva_data_portal.utils.Cache.blpop",
                    new=AsyncMock(return_value=None),
                ):
                    res = requests.post(
                        f"{test_server}/data-portal/zarr/convert",
                        json={"path": self.files(test_server)[-1]},
                        headers={"Authorization": f"Bearer {auth['access_token']}"},
                        timeout=10,
                    )
                assert res.status_code == 503
