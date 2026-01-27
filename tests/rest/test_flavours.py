"""Unit tests for flavour endpoints of the REST API.

This module tests all flavour-related endpoints:
- GET /databrowser/flavours (list flavours)
- GET /databrowser/overview (get flavour overview)
- POST /databrowser/flavours (add flavour)
- PUT /databrowser/flavours/{name} (update flavour)
- DELETE /databrowser/flavours/{name} (delete flavour)
"""

from typing import Any, Dict

import mock
import pytest
import requests


# -------------------------
# Helper request wrappers
# -------------------------


def _auth_headers(token: str) -> Dict[str, str]:
    """Create authorization headers."""
    return {"Authorization": f"Bearer {token}"}


def _get_flavours(
    base: str, *, params: Dict[str, Any] | None = None, token: str | None = None
):
    """GET /databrowser/flavours."""
    return requests.get(
        f"{base}/databrowser/flavours",
        params=params,
        headers=_auth_headers(token) if token else None,
    )


def _post_flavour(
    base: str, payload: Dict[str, Any], *, token: str | None = None
):
    """POST /databrowser/flavours."""
    return requests.post(
        f"{base}/databrowser/flavours",
        json=payload,
        headers=_auth_headers(token) if token else None,
    )


def _put_flavour(
    base: str, name: str, payload: Dict[str, Any], *, token: str | None = None
):
    """PUT /databrowser/flavours/{name}."""
    return requests.put(
        f"{base}/databrowser/flavours/{name}",
        json=payload,
        headers=_auth_headers(token) if token else None,
    )


def _delete_flavour(
    base: str, name: str, *, token: str | None = None, query: str | None = None
):
    """DELETE /databrowser/flavours/{name}."""
    url = f"{base}/databrowser/flavours/{name}"
    if query:
        url = f"{url}?{query}"
    return requests.delete(
        url,
        headers=_auth_headers(token) if token else None,
    )


def _get_overview(base: str, *, token: str | None = None):
    """GET /databrowser/overview."""
    return requests.get(
        f"{base}/databrowser/overview",
        headers=_auth_headers(token) if token else None,
    )


# =========================================================================
# GET /databrowser/flavours - List Flavours Tests
# =========================================================================


class TestListFlavours:
    """Tests for GET /databrowser/flavours endpoint."""

    def test_list_flavours_without_auth(self, flavour_server: str) -> None:
        """GET: listing flavours without authentication succeeds."""
        res = _get_flavours(flavour_server)
        assert res.status_code == 200
        data = res.json()
        assert "total" in data
        assert "flavours" in data
        assert data["total"] >= 5

    def test_list_flavours_contains_builtins(self, flavour_server: str) -> None:
        """GET: listing includes built-in flavours."""
        res = _get_flavours(flavour_server)
        assert res.status_code == 200
        built_in_names = [f["flavour_name"] for f in res.json()["flavours"]]
        assert "freva" in built_in_names
        assert "cmip6" in built_in_names

    def test_list_flavours_with_auth(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """GET: listing flavours with authentication succeeds."""
        res = _get_flavours(flavour_server, token=auth["access_token"])
        assert res.status_code == 200
        assert "flavours" in res.json()

    def test_list_flavours_invalid_param(self, flavour_server: str) -> None:
        """GET: listing with invalid query parameter returns 422."""
        res = _get_flavours(flavour_server, params={"invalid_param": "test"})
        assert res.status_code == 422

    def test_list_flavours_filter_by_name(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """GET: filter flavours by flavour_name parameter."""
        res = _get_flavours(
            flavour_server,
            params={"flavour_name": "freva"},
            token=auth["access_token"],
        )
        assert res.status_code == 200
        data = res.json()
        assert data["total"] >= 1
        flavour_names = [f["flavour_name"] for f in data["flavours"]]
        assert "freva" in flavour_names

    def test_list_flavours_filter_nonexistent(self, flavour_server: str) -> None:
        """GET: filter by non-existent flavour returns empty list."""
        res = _get_flavours(
            flavour_server, params={"flavour_name": "nonexistent_xyz"}
        )
        assert res.status_code == 200
        assert res.json()["total"] == 0


# =========================================================================
# GET /databrowser/overview - Overview Tests
# =========================================================================


class TestOverview:
    """Tests for GET /databrowser/overview endpoint."""

    def test_overview_without_auth(self, flavour_server: str) -> None:
        """GET: overview without authentication returns global flavours."""
        res = _get_overview(flavour_server)
        assert res.status_code == 200
        data = res.json()
        assert "flavours" in data
        assert "attributes" in data
        assert isinstance(data["flavours"], list)
        assert isinstance(data["attributes"], dict)

    def test_overview_with_auth(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """GET: overview with authentication includes personal flavours."""
        res = _get_overview(flavour_server, token=auth["access_token"])
        assert res.status_code == 200
        data = res.json()
        assert "flavours" in data


# =========================================================================
# POST /databrowser/flavours - Add Flavour Tests
# =========================================================================


class TestAddFlavour:
    """Tests for POST /databrowser/flavours endpoint."""

    def test_add_flavour_no_auth(self, flavour_server: str) -> None:
        """POST: adding flavour without authentication returns 401."""
        custom_flavour = {
            "flavour_name": "test_no_auth",
            "mapping": {"project": "my_project"},
            "is_global": False,
        }
        res = _post_flavour(flavour_server, custom_flavour)
        assert res.status_code == 401

    def test_add_personal_flavour(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """POST: adding a custom personal flavour succeeds."""
        custom_flavour = {
            "flavour_name": "test_personal_add",
            "mapping": {"project": "my_project", "variable": "my_variable"},
            "is_global": False,
        }
        res = _post_flavour(
            flavour_server, custom_flavour, token=auth["access_token"]
        )
        assert res.status_code == 201
        assert "status" in res.json()

        # Cleanup
        _delete_flavour(
            flavour_server, "test_personal_add", token=auth["access_token"]
        )

    def test_add_duplicate_flavour(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """POST: adding duplicate flavour returns 409."""
        custom_flavour = {
            "flavour_name": "test_duplicate",
            "mapping": {"project": "my_project"},
            "is_global": False,
        }
        # Create first
        res1 = _post_flavour(
            flavour_server, custom_flavour, token=auth["access_token"]
        )
        assert res1.status_code == 201

        # Try duplicate
        res2 = _post_flavour(
            flavour_server, custom_flavour, token=auth["access_token"]
        )
        assert res2.status_code == 409

        # Cleanup
        _delete_flavour(
            flavour_server, "test_duplicate", token=auth["access_token"]
        )

    def test_add_flavour_restricted_chars(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """POST: flavour with restricted characters returns 422."""
        flavour_with_bad_chars = {
            "flavour_name": "test:flav></*'our",
            "mapping": {"project": "my_project"},
            "is_global": False,
        }
        res = _post_flavour(
            flavour_server, flavour_with_bad_chars, token=auth["access_token"]
        )
        assert res.status_code == 422

    def test_add_global_flavour_non_admin(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """POST: non-admin user cannot add global flavour (403)."""
        global_flavour = {
            "flavour_name": "test_global_nonadmin",
            "mapping": {"project": "my_project"},
            "is_global": True,
        }
        res = _post_flavour(
            flavour_server, global_flavour, token=auth["access_token"]
        )
        assert res.status_code == 403

    def test_add_global_flavour_admin(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """POST: admin user can add global flavour."""
        auth_admin = auth["admin"]
        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            res = _post_flavour(
                flavour_server,
                {
                    "flavour_name": "test_global_admin",
                    "is_global": True,
                    "mapping": {"project": "my_project"},
                },
                token=auth_admin["access_token"],
            )
            assert res.status_code == 201

            # Cleanup
            _delete_flavour(
                flavour_server,
                "test_global_admin",
                token=auth_admin["access_token"],
                query="is_global=true",
            )

    def test_add_global_flavour_duplicate_admin(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """POST: admin cannot add global flavour with same name as existing (409)."""
        auth_admin = auth["admin"]
        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            # Create first
            _post_flavour(
                flavour_server,
                {
                    "flavour_name": "test_global_dup",
                    "is_global": True,
                    "mapping": {"project": "my_project"},
                },
                token=auth_admin["access_token"],
            )

            # Try duplicate (using built-in name)
            res = _post_flavour(
                flavour_server,
                {
                    "flavour_name": "freva",
                    "mapping": {"project": "my_project"},
                    "is_global": True,
                },
                token=auth_admin["access_token"],
            )
            assert res.status_code == 409

            # Cleanup
            _delete_flavour(
                flavour_server,
                "test_global_dup",
                token=auth_admin["access_token"],
                query="is_global=true",
            )

    def test_add_flavour_shows_in_listing(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """POST: custom flavour appears in listing after creation."""
        custom_flavour = {
            "flavour_name": "test_visible",
            "mapping": {"project": "my_project"},
            "is_global": False,
        }
        _post_flavour(flavour_server, custom_flavour, token=auth["access_token"])

        res = _get_flavours(flavour_server, token=auth["access_token"])
        assert res.status_code == 200
        names = [f["flavour_name"] for f in res.json()["flavours"]]
        assert "test_visible" in names

        # Cleanup
        _delete_flavour(
            flavour_server, "test_visible", token=auth["access_token"]
        )


# =========================================================================
# PUT /databrowser/flavours/{name} - Update Flavour Tests
# =========================================================================


class TestUpdateFlavour:
    """Tests for PUT /databrowser/flavours/{name} endpoint."""

    def test_update_flavour_no_auth(self, flavour_server: str) -> None:
        """PUT: updating flavour without authentication returns 401."""
        res = _put_flavour(
            flavour_server,
            "any_flavour",
            {
                "flavour_name": "any_flavour",
                "mapping": {"model": "updated"},
                "is_global": False,
            },
        )
        assert res.status_code == 401

    def test_update_personal_flavour(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """PUT: updating personal flavour succeeds."""
        # Create flavour first
        _post_flavour(
            flavour_server,
            {
                "flavour_name": "test_update",
                "mapping": {"project": "original"},
                "is_global": False,
            },
            token=auth["access_token"],
        )

        # Update it
        res = _put_flavour(
            flavour_server,
            "test_update",
            {
                "flavour_name": "test_update",
                "mapping": {"model": "updated_model", "experiment": "updated_exp"},
                "is_global": False,
            },
            token=auth["access_token"],
        )
        assert res.status_code == 200
        assert "status" in res.json()

        # Cleanup
        _delete_flavour(
            flavour_server, "test_update", token=auth["access_token"]
        )

    def test_update_flavour_preserves_original_keys(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """PUT: partial update preserves original keys."""
        # Create flavour
        _post_flavour(
            flavour_server,
            {
                "flavour_name": "test_preserve",
                "mapping": {"project": "my_project", "variable": "my_variable"},
                "is_global": False,
            },
            token=auth["access_token"],
        )

        # Update with new key
        _put_flavour(
            flavour_server,
            "test_preserve",
            {
                "flavour_name": "test_preserve",
                "mapping": {"model": "new_model"},
                "is_global": False,
            },
            token=auth["access_token"],
        )

        # Verify original keys preserved
        res = _get_flavours(
            flavour_server,
            params={"flavour_name": "test_preserve"},
            token=auth["access_token"],
        )
        updated = next(
            f for f in res.json()["flavours"] if f["flavour_name"] == "test_preserve"
        )
        assert updated["mapping"]["model"] == "new_model"
        assert updated["mapping"]["project"] == "my_project"  # Original preserved

        # Cleanup
        _delete_flavour(
            flavour_server, "test_preserve", token=auth["access_token"]
        )

    def test_update_flavour_invalid_name(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """PUT: update with invalid characters in new name returns 422."""
        # Create flavour
        _post_flavour(
            flavour_server,
            {
                "flavour_name": "test_invalid_rename",
                "mapping": {"project": "my_project"},
                "is_global": False,
            },
            token=auth["access_token"],
        )

        # Try to rename with invalid chars
        res = _put_flavour(
            flavour_server,
            "test_invalid_rename",
            {
                "flavour_name": "invalid:flav></*'our",
                "mapping": {"model": "some_model"},
                "is_global": False,
            },
            token=auth["access_token"],
        )
        assert res.status_code == 422

        # Cleanup
        _delete_flavour(
            flavour_server, "test_invalid_rename", token=auth["access_token"]
        )

    def test_update_flavour_already_uptodate(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """PUT: updating already up-to-date flavour succeeds."""
        # Create flavour
        _post_flavour(
            flavour_server,
            {
                "flavour_name": "test_uptodate",
                "mapping": {"project": "my_project"},
                "is_global": False,
            },
            token=auth["access_token"],
        )

        # Update with same values
        res = _put_flavour(
            flavour_server,
            "test_uptodate",
            {
                "flavour_name": "test_uptodate",
                "mapping": {"project": "my_project"},
                "is_global": False,
            },
            token=auth["access_token"],
        )
        assert res.status_code == 200

        # Cleanup
        _delete_flavour(
            flavour_server, "test_uptodate", token=auth["access_token"]
        )

    def test_update_flavour_name_conflict(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """PUT: renaming to existing flavour name returns 409."""
        # Create two flavours
        _post_flavour(
            flavour_server,
            {
                "flavour_name": "test_rename_a",
                "mapping": {"project": "proj_a"},
                "is_global": False,
            },
            token=auth["access_token"],
        )
        _post_flavour(
            flavour_server,
            {
                "flavour_name": "test_rename_b",
                "mapping": {"project": "proj_b"},
                "is_global": False,
            },
            token=auth["access_token"],
        )

        # Try to rename A to B
        res = _put_flavour(
            flavour_server,
            "test_rename_a",
            {
                "flavour_name": "test_rename_b",
                "mapping": {"model": "some_model"},
                "is_global": False,
            },
            token=auth["access_token"],
        )
        assert res.status_code == 409

        # Cleanup
        _delete_flavour(
            flavour_server, "test_rename_a", token=auth["access_token"]
        )
        _delete_flavour(
            flavour_server, "test_rename_b", token=auth["access_token"]
        )

    def test_update_nonexistent_flavour(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """PUT: updating non-existent flavour returns 404."""
        res = _put_flavour(
            flavour_server,
            "non_existent_flavour",
            {
                "flavour_name": "non_existent_flavour",
                "mapping": {"model": "some_model"},
                "is_global": False,
            },
            token=auth["access_token"],
        )
        assert res.status_code == 404

    def test_update_global_flavour_non_admin(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """PUT: non-admin user cannot update global flavour (403)."""
        auth_admin = auth["admin"]

        # Create global flavour as admin
        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            _post_flavour(
                flavour_server,
                {
                    "flavour_name": "test_global_noupdate",
                    "is_global": True,
                    "mapping": {"project": "my_project"},
                },
                token=auth_admin["access_token"],
            )

        # Non-admin tries to update
        res = _put_flavour(
            flavour_server,
            "test_global_noupdate",
            {
                "flavour_name": "test_global_noupdate",
                "mapping": {"model": "new_model"},
                "is_global": True,
            },
            token=auth["access_token"],
        )
        assert res.status_code == 403

        # Cleanup
        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            _delete_flavour(
                flavour_server,
                "test_global_noupdate",
                token=auth_admin["access_token"],
                query="is_global=true",
            )

    def test_update_global_flavour_admin(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """PUT: admin user can update global flavour."""
        auth_admin = auth["admin"]

        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            # Create global flavour
            _post_flavour(
                flavour_server,
                {
                    "flavour_name": "test_global_update",
                    "is_global": True,
                    "mapping": {"project": "original"},
                },
                token=auth_admin["access_token"],
            )

            # Admin updates it
            res = _put_flavour(
                flavour_server,
                "test_global_update",
                {
                    "flavour_name": "test_global_update",
                    "mapping": {"model": "admin_updated"},
                    "is_global": True,
                },
                token=auth_admin["access_token"],
            )
            assert res.status_code == 200
            assert "status" in res.json()

            # Cleanup
            _delete_flavour(
                flavour_server,
                "test_global_update",
                token=auth_admin["access_token"],
                query="is_global=true",
            )

    def test_update_builtin_flavour_admin(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """PUT: even admin cannot update built-in flavours (422)."""
        auth_admin = auth["admin"]

        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            res = _put_flavour(
                flavour_server,
                "freva",
                {
                    "flavour_name": "freva",
                    "mapping": {"model": "some_model"},
                    "is_global": True,
                },
                token=auth_admin["access_token"],
            )
            assert res.status_code == 422
            assert "Cannot update built-in flavour" in res.json()["detail"]


# =========================================================================
# DELETE /databrowser/flavours/{name} - Delete Flavour Tests
# =========================================================================


class TestDeleteFlavour:
    """Tests for DELETE /databrowser/flavours/{name} endpoint."""

    def test_delete_flavour_no_auth(self, flavour_server: str) -> None:
        """DELETE: deleting without authentication returns 401."""
        res = _delete_flavour(flavour_server, "any_flavour")
        assert res.status_code == 401

    def test_delete_personal_flavour(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """DELETE: deleting custom personal flavour succeeds."""
        # Create flavour first
        _post_flavour(
            flavour_server,
            {
                "flavour_name": "test_delete_personal",
                "mapping": {"project": "my_project"},
                "is_global": False,
            },
            token=auth["access_token"],
        )

        # Delete it
        res = _delete_flavour(
            flavour_server, "test_delete_personal", token=auth["access_token"]
        )
        assert res.status_code == 200
        assert "status" in res.json()

    def test_delete_builtin_flavour(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """DELETE: built-in flavours cannot be deleted."""
        res = _delete_flavour(
            flavour_server, "freva", token=auth["access_token"]
        )
        assert res.status_code == 422
        assert "built-in or does not exist" in res.json()["detail"]

    def test_delete_nonexistent_flavour(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """DELETE: deleting non-existent flavour returns 422."""
        res = _delete_flavour(
            flavour_server, "non_existent_flavour", token=auth["access_token"]
        )
        assert res.status_code == 422

    def test_delete_global_flavour_non_admin(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """DELETE: non-admin user cannot delete global flavour (403)."""
        auth_admin = auth["admin"]

        # Create global flavour as admin
        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            _post_flavour(
                flavour_server,
                {
                    "flavour_name": "test_global_nodelete",
                    "is_global": True,
                    "mapping": {"project": "my_project"},
                },
                token=auth_admin["access_token"],
            )

        # Non-admin tries to delete
        res = _delete_flavour(
            flavour_server,
            "test_global_nodelete",
            token=auth["access_token"],
            query="is_global=true",
        )
        assert res.status_code == 403

        # Cleanup
        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            _delete_flavour(
                flavour_server,
                "test_global_nodelete",
                token=auth_admin["access_token"],
                query="is_global=true",
            )

    def test_delete_global_flavour_admin(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """DELETE: admin user can delete global flavour."""
        auth_admin = auth["admin"]

        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            # Create global flavour
            _post_flavour(
                flavour_server,
                {
                    "flavour_name": "test_global_delete",
                    "is_global": True,
                    "mapping": {"project": "my_project"},
                },
                token=auth_admin["access_token"],
            )

            # Admin deletes it
            res = _delete_flavour(
                flavour_server,
                "test_global_delete",
                token=auth_admin["access_token"],
                query="is_global=true",
            )
            assert res.status_code == 200

    def test_delete_builtin_flavour_admin(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """DELETE: built-in flavours cannot be deleted even by admin."""
        auth_admin = auth["admin"]

        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            res = _delete_flavour(
                flavour_server,
                "cmip6",
                token=auth_admin["access_token"],
                query="is_global=true",
            )
            assert res.status_code == 422

    def test_delete_another_users_flavour(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """DELETE: user cannot delete another user's personal flavour."""
        auth_admin = auth["admin"]

        # Admin creates personal flavour
        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            _post_flavour(
                flavour_server,
                {
                    "flavour_name": "admin_personal",
                    "mapping": {"project": "admin_project"},
                    "is_global": False,
                },
                token=auth_admin["access_token"],
            )

        # Regular user tries to delete admin's personal flavour
        res = _delete_flavour(
            flavour_server, "admin_personal", token=auth["access_token"]
        )
        assert res.status_code == 422

        # Cleanup
        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            _delete_flavour(
                flavour_server, "admin_personal", token=auth_admin["access_token"]
            )

    def test_delete_flavour_with_user_prefix(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """DELETE: personal flavour can be deleted using user prefix."""
        # Create personal flavour with same name as built-in
        _post_flavour(
            flavour_server,
            {
                "flavour_name": "cmip6",
                "mapping": {"project": "my_project"},
                "is_global": False,
            },
            token=auth["access_token"],
        )

        # Verify it appears in overview with prefix
        res = _get_overview(flavour_server, token=auth["access_token"])
        assert "janedoe:cmip6" in res.json()["flavours"]

        # Delete using prefix
        res = _delete_flavour(
            flavour_server, "janedoe:cmip6", token=auth["access_token"]
        )
        assert res.status_code == 200


# =========================================================================
# Mixed Scenarios / Integration Tests
# =========================================================================


class TestMixedScenarios:
    """Integration tests for complex flavour scenarios."""

    def test_global_and_personal_same_name(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """Scenario: both global and personal flavour can have same name."""
        auth_admin = auth["admin"]

        # Admin creates global flavour
        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            res1 = _post_flavour(
                flavour_server,
                {
                    "flavour_name": "dual_name",
                    "mapping": {"project": "global_project"},
                    "is_global": True,
                },
                token=auth_admin["access_token"],
            )
            assert res1.status_code == 201

        # User creates personal flavour with same name
        res2 = _post_flavour(
            flavour_server,
            {
                "flavour_name": "dual_name",
                "mapping": {"project": "personal_project"},
                "is_global": False,
            },
            token=auth["access_token"],
        )
        assert res2.status_code == 201

        # Both appear in listing
        res3 = _get_flavours(flavour_server, token=auth["access_token"])
        names = [f["flavour_name"] for f in res3.json()["flavours"]]
        assert "dual_name" in names
        assert "janedoe:dual_name" in names

        # Cleanup
        with mock.patch(
            "freva_rest.rest.server_config.admins_token_claims",
            {"resource_access.realm-management.roles": ["admin"]},
        ):
            _delete_flavour(
                flavour_server,
                "dual_name",
                token=auth_admin["access_token"],
                query="is_global=true",
            )
        _delete_flavour(
            flavour_server, "dual_name", token=auth["access_token"]
        )

    def test_personal_flavour_same_as_builtin(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """Scenario: personal flavour can have same name as built-in."""
        # Create personal flavour with built-in name
        res = _post_flavour(
            flavour_server,
            {
                "flavour_name": "cmip6",
                "mapping": {"project": "my_cmip6"},
                "is_global": False,
            },
            token=auth["access_token"],
        )
        assert res.status_code == 201

        # Appears in overview with user prefix
        res2 = _get_overview(flavour_server, token=auth["access_token"])
        assert "janedoe:cmip6" in res2.json()["flavours"]

        # Cleanup
        _delete_flavour(
            flavour_server, "janedoe:cmip6", token=auth["access_token"]
        )

    def test_create_update_delete_lifecycle(
        self, flavour_server: str, auth: Dict[str, str]
    ) -> None:
        """Scenario: full lifecycle of flavour creation, update, and deletion."""
        # Create
        res1 = _post_flavour(
            flavour_server,
            {
                "flavour_name": "lifecycle_test",
                "mapping": {"project": "initial"},
                "is_global": False,
            },
            token=auth["access_token"],
        )
        assert res1.status_code == 201

        # Update
        res2 = _put_flavour(
            flavour_server,
            "lifecycle_test",
            {
                "flavour_name": "lifecycle_test",
                "mapping": {"project": "updated", "model": "new_model"},
                "is_global": False,
            },
            token=auth["access_token"],
        )
        assert res2.status_code == 200

        # Verify update
        res3 = _get_flavours(
            flavour_server,
            params={"flavour_name": "lifecycle_test"},
            token=auth["access_token"],
        )
        flavour = next(
            f for f in res3.json()["flavours"] if f["flavour_name"] == "lifecycle_test"
        )
        assert flavour["mapping"]["project"] == "updated"
        assert flavour["mapping"]["model"] == "new_model"

        # Delete
        res4 = _delete_flavour(
            flavour_server, "lifecycle_test", token=auth["access_token"]
        )
        assert res4.status_code == 200

        # Verify deletion
        res5 = _get_flavours(
            flavour_server,
            params={"flavour_name": "lifecycle_test"},
            token=auth["access_token"],
        )
        assert res5.json()["total"] == 0
