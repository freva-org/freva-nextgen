"""Tests for filesystem-level access control on data portal endpoints.

These tests verify that the REST API correctly enforces read permissions
by communicating with the data-loader via Redis. The data-loader's
``user_can_read`` function is mocked at the worker level so that the
full publish → RPC → reply flow is exercised.
"""

from typing import Dict
from unittest.mock import patch

import pytest
import requests

pytestmark = [pytest.mark.portal_endpoints, pytest.mark.rest]


class TestConvertPermissions:
    """Tests for permission checks on the zarr/convert endpoint."""

    def test_convert_allowed(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
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

    def test_convert_denied(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """A user without read access gets 403 on convert."""
        files = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"dataset": "agg"},
            timeout=10,
        ).text.splitlines()
        assert files

        with patch(
            "data_portal_worker.utils.user_can_read", return_value=False
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

    def _get_zarr_url(
        self, test_server: str, auth: Dict[str, str]
    ) -> str:
        """Helper: convert a file and return its private zarr URL."""
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
        return res.json()["urls"][0]

    def test_presign_denied(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """A user without read access gets 403 on presign."""
        zarr_url = self._get_zarr_url(test_server, auth)
        with patch(
            "data_portal_worker.utils.user_can_read", return_value=False
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
            json={"path": "http://localhost/data-portal/zarr/abc.zarr", "ttl_seconds": 600},
            timeout=10,
        )
        assert res.status_code == 401


class TestGuestAccess:
    """Tests for guest user (no system username) access control."""

    def test_guest_denied_non_world_readable(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """A guest user (no IDP username claim) cannot convert
        non-world-readable data."""
        from unittest.mock import AsyncMock

        files = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"dataset": "agg"},
            timeout=10,
        ).text.splitlines()
        assert files

        # Mock get_system_username to return None (guest)
        # and user_can_read to deny (file not world-readable)
        with patch(
            "freva_rest.auth.get_system_username",
            new=AsyncMock(return_value=None),
        ):
            with patch(
                "data_portal_worker.utils.user_can_read", return_value=False
            ):
                res = requests.post(
                    f"{test_server}/data-portal/zarr/convert",
                    json={"path": files[0]},
                    headers={
                        "Authorization": f"Bearer {auth['access_token']}"
                    },
                    timeout=10,
                )
        assert res.status_code == 403
