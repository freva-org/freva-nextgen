"""Tests for filesystem-level access control on data portal endpoints.

These tests verify that the REST API correctly enforces read permissions.
Permission denial is mocked at the ``check_read_permission`` level (above
the Redis cache) so that cached results from earlier tests do not
interfere.
"""

from typing import Dict
from unittest.mock import AsyncMock, patch

import pytest
import requests
from fastapi import HTTPException

pytestmark = [pytest.mark.portal_endpoints, pytest.mark.rest]


class TestConvertPermissions:
    """Tests for permission checks on the zarr/convert endpoint."""

    def test_convert_allowed(self, test_server: str, auth: Dict[str, str]) -> None:
        """An authenticated user with read access can convert data."""
        files = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"dataset": "agg"},
            timeout=10,
        ).text.splitlines()
        assert files

        res = requests.post(
            f"{test_server}/data-portal/zarr/convert",
            json={"path": files[0]},
            headers={"Authorization": f"Bearer {auth['access_token']}"},
            timeout=10,
        )
        assert res.status_code == 200

    def test_convert_denied(self, test_server: str, auth: Dict[str, str]) -> None:
        """A user without read access gets 403 on convert."""
        files = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"dataset": "agg"},
            timeout=10,
        ).text.splitlines()
        assert files

        with patch(
            "freva_rest.freva_data_portal.utils.check_read_permission",
            new=AsyncMock(side_effect=HTTPException(status_code=403, detail="denied")),
        ):
            res = requests.post(
                f"{test_server}/data-portal/zarr/convert",
                json={"path": files[0]},
                headers={"Authorization": f"Bearer {auth['access_token']}"},
                timeout=10,
            )
        assert res.status_code == 403

    def test_convert_no_auth(self, test_server: str) -> None:
        """Unauthenticated request to convert returns 401."""
        res = requests.post(
            f"{test_server}/data-portal/zarr/convert",
            json={"path": "/some/file.nc"},
            timeout=10,
        )
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

    def test_guest_denied_non_world_readable(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """A guest user cannot access non-world-readable data."""
        files = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"dataset": "agg"},
            timeout=10,
        ).text.splitlines()
        assert files

        with patch(
            "freva_rest.freva_data_portal.utils.check_read_permission",
            new=AsyncMock(side_effect=HTTPException(status_code=403, detail="denied")),
        ):
            res = requests.post(
                f"{test_server}/data-portal/zarr/convert",
                json={"path": files[0]},
                headers={"Authorization": f"Bearer {auth['access_token']}"},
                timeout=10,
            )
        assert res.status_code == 403
