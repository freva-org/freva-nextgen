"""
Endpoint tests for the settings and ui-content API.
"""

import hashlib
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

import pymongo
import pytest
import requests

from freva_rest.config import ServerConfig

BASE = "{server}/settings"
OPENAPI = "{server}/help/openapi.json"
ADMIN_CLAIMS = {"roles": ["admin"]}


@pytest.fixture(scope="function")
def settings_server(test_server: str) -> Iterator[str]:
    """Clean both collections and install the admin claim filter durably."""
    from freva_rest.rest import server_config as live_config

    config = ServerConfig()
    client = pymongo.MongoClient(config.mongo_url)
    settings = client[config.mongo_db]["settings"]
    contents = client[config.mongo_db]["ui_contents"]
    settings_backup = list(settings.find({}))
    contents_backup = list(contents.find({}))

    from freva_rest.settings_api.core import reset_caches

    original_claims = live_config.admin_token_claims
    live_config.admin_token_claims = ADMIN_CLAIMS
    try:
        settings.delete_many({})
        contents.delete_many({})
        reset_caches()
        yield test_server
    finally:
        live_config.admin_token_claims = original_claims
        settings.delete_many({})
        contents.delete_many({})
        reset_caches()
        if settings_backup:
            settings.insert_many(settings_backup)
        if contents_backup:
            contents.insert_many(contents_backup)
        client.close()


# helpers


@contextmanager
def _mongo() -> Iterator[Any]:
    """
    A synchronous mongo client that is always closed.
    """
    client = pymongo.MongoClient(ServerConfig().mongo_url)
    try:
        yield client
    finally:
        client.close()


def _get(url: str, headers: Optional[Dict[str, str]] = None) -> requests.Response:
    return requests.get(url, headers=headers or {})


def _patch(
    url: str,
    payload: Dict[str, Any],
    token: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> requests.Response:
    head = dict(headers or {})
    if token:
        head["Authorization"] = f"Bearer {token}"
    return requests.patch(url, json=payload, headers=head)


def _delete(url: str, token: Optional[str] = None) -> requests.Response:
    head = {"Authorization": f"Bearer {token}"} if token else {}
    return requests.delete(url, headers=head)


def _admin(auth: Dict[str, Any]) -> str:
    return str(auth["admin"]["access_token"])


def _user(auth: Dict[str, Any]) -> str:
    """
    The non-admin token.
    """
    return str(auth["access_token"])


def _tok(auth: Dict[str, Any], user: str) -> Dict[str, str]:
    token = _admin(auth) if user == "admin" else _user(auth)
    return {"Authorization": f"Bearer {token}"}


def _ui(server: str, record: str = "default") -> str:
    return f"{BASE.format(server=server)}/ui/{record}"


def _content(server: str, ui_id: str, content_id: str) -> str:
    return f"{BASE.format(server=server)}/ui/{ui_id}/contents/{content_id}"


def _seed_content(
    server: str, token: str, ui: str, cid: str, **kw: Any
) -> requests.Response:
    return _patch(_content(server, ui, cid), kw, token=token)


# the harness itself
class TestHarnessAddressesTheRealRoutes:
    """
    Guard the URL and token plumbing this module builds every request from.
    """

    PREFIX = "/api/freva-nextgen"

    def test_the_code_under_test_is_this_checkout(self) -> None:
        """A run that imports an installed ``freva_rest`` from site-packages
        exercises a different tree, and every result from it is meaningless."""
        from pathlib import Path

        import freva_rest

        imported = Path(freva_rest.__file__ or "").resolve()
        checkout = Path(__file__).resolve().parents[2]
        expected = checkout / "freva-rest" / "src" / "freva_rest"
        assert (
            imported.parent == expected
        ), f"freva_rest imported from {imported}, expected {expected}"

    def test_the_fixture_already_carries_the_api_prefix(
        self, settings_server: str
    ) -> None:
        assert settings_server.endswith(self.PREFIX), settings_server

    def test_built_urls_match_the_registered_route_paths(
        self, settings_server: str
    ) -> None:
        import freva_rest.settings_api.endpoints  # noqa: F401
        from freva_rest.rest import app

        root = settings_server[: -len(self.PREFIX)]
        paths = {getattr(route, "path", None) for route in app.routes}
        assert _ui(settings_server, "default").startswith(root)
        assert _ui(settings_server, "default")[len(root) :] == (
            f"{self.PREFIX}/settings/ui/default"
        )
        assert f"{self.PREFIX}/settings/{{resource}}/{{record_id}}" in paths
        assert _content(settings_server, "default", "about")[len(root) :] == (
            f"{self.PREFIX}/settings/ui/default/contents/about"
        )
        assert OPENAPI.format(server=settings_server)[len(root) :] == app.openapi_url

    def test_both_tokens_resolve(self, auth: Dict[str, Any]) -> None:
        assert _admin(auth)
        assert _user(auth)
        assert _admin(auth) != _user(auth)
        assert _tok(auth, "user")["Authorization"].endswith(_user(auth))
        assert _tok(auth, "admin")["Authorization"].endswith(_admin(auth))


# settings records: defaults, named configs, the alias, isolation
class TestSettingsRecords:
    def test_default_is_public_and_complete(self, settings_server: str) -> None:
        res = _get(_ui(settings_server))
        assert res.status_code == 200
        assert res.json()["site_title"] == "Freva"

    def test_no_short_alias(self, settings_server: str) -> None:
        alias = _get(f"{BASE.format(server=settings_server)}/ui")
        assert alias.status_code == 404
        assert _get(_ui(settings_server, "default")).status_code == 200

    def test_default_is_synthesised_without_a_document(
        self, settings_server: str
    ) -> None:
        body = _get(_ui(settings_server, "default"))
        assert body.status_code == 200
        assert body.json()["site_title"] == "Freva"

    def test_unknown_named_record_is_404(self, settings_server: str) -> None:
        res = _get(_ui(settings_server, "neverwritten"))
        assert res.status_code == 404

    def test_anonymous_patch_rejected(self, settings_server: str) -> None:
        assert _patch(_ui(settings_server), {"site_title": "x"}).status_code == 401

    def test_non_admin_patch_forbidden(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"site_title": "x"},
            token=_user(auth),
        )
        assert res.status_code == 403

    def test_admin_patch(self, settings_server: str, auth: Dict[str, Any]) -> None:
        res = _patch(
            _ui(settings_server), {"main_color": "#0a84ff"}, token=_admin(auth)
        )
        assert res.status_code == 200
        assert res.json()["main_color"] == "#0a84ff"

    def test_named_records_are_isolated(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        _patch(
            _ui(settings_server, "waterpark"),
            {"main_color": "#0a84ff"},
            token=_admin(auth),
        )
        waterpark = _get(_ui(settings_server, "waterpark")).json()
        default = _get(_ui(settings_server, "default")).json()
        assert waterpark["main_color"] == "#0a84ff"
        assert default["main_color"] == "#286a9a"

    def test_three_independent_uis(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _patch(_ui(settings_server, "default"), {"site_title": "D"}, token=token)
        _patch(_ui(settings_server, "waterpark"), {"site_title": "W"}, token=token)
        _patch(_ui(settings_server, "lab"), {"site_title": "L"}, token=token)
        assert _get(_ui(settings_server, "default")).json()["site_title"] == "D"
        assert _get(_ui(settings_server, "waterpark")).json()["site_title"] == "W"
        assert _get(_ui(settings_server, "lab")).json()["site_title"] == "L"

    def test_delete_record(self, settings_server: str, auth: Dict[str, Any]) -> None:
        token = _admin(auth)
        _patch(_ui(settings_server, "temp"), {"site_title": "T"}, token=token)
        assert _delete(_ui(settings_server, "temp"), token=token).status_code == 204
        assert _get(_ui(settings_server, "temp")).status_code == 404

    def test_unknown_resource_404(self, settings_server: str) -> None:
        res = _get(f"{BASE.format(server=settings_server)}/nope/default")
        assert res.status_code == 404

    def test_invalid_record_id_422(self, settings_server: str) -> None:
        assert _get(_ui(settings_server, "Bad.Id")).status_code == 422


class TestSchemaEndpoint:
    """Introspection lives at ``_schema``. A record id may not start with an
    underscore, so the route can never collide with a record"""

    def _url(self, server: str, resource: str = "ui", query: str = "") -> str:
        return f"{BASE.format(server=server)}/{resource}/_schema{query}"

    def test_schema_public(self, settings_server: str) -> None:
        res = _get(self._url(settings_server))
        assert res.status_code == 200
        assert "x-widget" in res.text

    def test_schema_unknown_resource_404(self, settings_server: str) -> None:
        assert _get(self._url(settings_server, "nope")).status_code == 404

    def test_schema_is_cacheable(self, settings_server: str) -> None:
        res = _get(self._url(settings_server))
        assert res.headers["Cache-Control"].startswith("public")

    def test_the_read_variant_marks_unpatchable_fields(
        self, settings_server: str
    ) -> None:
        schema = _get(self._url(settings_server)).json()
        assert schema["properties"]["schemaVersion"]["readOnly"] is True

    def test_the_update_variant_is_the_patch_body(self, settings_server: str) -> None:
        schema = _get(self._url(settings_server, query="?variant=update")).json()
        assert "schemaVersion" not in schema["properties"]
        assert "site_title" in schema["properties"]

    def test_an_unknown_variant_is_422(self, settings_server: str) -> None:
        res = _get(self._url(settings_server, query="?variant=sideways"))
        assert res.status_code == 422

    def test_a_form_built_from_the_update_schema_is_accepted(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        schema = _get(self._url(settings_server, query="?variant=update")).json()
        assert "schemaVersion" not in schema["properties"]
        res = _patch(
            _ui(settings_server), {"site_title": "From a form"}, token=_admin(auth)
        )
        assert res.status_code == 200
        read_schema = _get(self._url(settings_server)).json()
        rejected = _patch(
            _ui(settings_server),
            {"schemaVersion": read_schema["properties"]["schemaVersion"]["default"]},
            token=_admin(auth),
        )
        assert rejected.status_code == 422


class TestRecordNamedSchemaIsAddressable:
    """Every method on a record called ``schema`` reaches the record."""

    def _url(self, server: str) -> str:
        return f"{BASE.format(server=server)}/ui/schema"

    def test_it_can_be_written_then_read_then_deleted(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        written = _patch(self._url(settings_server), {"site_title": "R"}, token=token)
        assert written.status_code == 200, written.text

        read = _get(self._url(settings_server))
        assert read.status_code == 200
        # the record, not the json schema of the resource
        assert read.json()["site_title"] == "R"
        assert "properties" not in read.json()

        removed = _delete(self._url(settings_server), token=token)
        assert removed.status_code == 204
        assert _get(self._url(settings_server)).status_code == 404

    def test_introspection_is_not_shadowed_by_that_record(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        _patch(self._url(settings_server), {"site_title": "R"}, token=_admin(auth))
        schema = _get(f"{BASE.format(server=settings_server)}/ui/_schema")
        assert schema.status_code == 200
        assert "properties" in schema.json()


class TestCacheHeaders:
    def test_public_get_is_public(self, settings_server: str) -> None:
        res = _get(_ui(settings_server))
        assert res.headers["Cache-Control"].startswith("public")

    def test_patch_is_private(self, settings_server: str, auth: Dict[str, Any]) -> None:
        res = _patch(_ui(settings_server), {"site_title": "x"}, token=_admin(auth))
        assert "no-store" in res.headers["Cache-Control"]

    def test_admin_source_is_private(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "c",
            format="markdown",
            source="# s",
        )
        url = _content(settings_server, "default", "c") + "?include_source=true"
        res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
        assert "no-store" in res.headers["Cache-Control"]


class TestSettingsConcurrency:
    def test_etag_and_304(self, settings_server: str) -> None:
        res = _get(_ui(settings_server))
        etag = res.headers["ETag"]
        assert etag == f'"{hashlib.sha256(res.content).hexdigest()[:32]}"'
        second = _get(_ui(settings_server), {"If-None-Match": etag})
        assert second.status_code == 304

    def test_stale_if_match_412(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"site_title": "x"},
            token=_admin(auth),
            headers={"If-Match": '"stale"'},
        )
        assert res.status_code == 412

    def test_current_if_match_ok(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        etag = _get(_ui(settings_server)).headers["ETag"]
        res = _patch(
            _ui(settings_server),
            {"site_title": "x"},
            token=_admin(auth),
            headers={"If-Match": etag},
        )
        assert res.status_code == 200

    def test_concurrent_field_updates_compose(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _patch(_ui(settings_server), {"site_title": "DKRZ"}, token=token)
        _patch(_ui(settings_server), {"homepage_heading": "Welcome"}, token=token)
        body = _get(_ui(settings_server)).json()
        assert body["site_title"] == "DKRZ"
        assert body["homepage_heading"] == "Welcome"


class TestPatchSemantics:
    def test_omitted_kept_null_resets(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _patch(_ui(settings_server), {"site_title": "DKRZ"}, token=token)
        kept = _patch(_ui(settings_server), {"homepage_heading": "H"}, token=token)
        assert kept.json()["site_title"] == "DKRZ"
        reset = _patch(_ui(settings_server), {"site_title": None}, token=token)
        assert reset.json()["site_title"] == "Freva"

    def test_open_map_false_and_zero_preserved(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"public_extensions": {"beta": False, "count": 0}},
            token=_admin(auth),
        )
        ext = res.json()["public_extensions"]
        assert ext["beta"] is False and ext["count"] == 0

    def test_open_map_null_deletes_key(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _patch(
            _ui(settings_server),
            {"public_extensions": {"a": "1", "b": "2"}},
            token=token,
        )
        res = _patch(
            _ui(settings_server), {"public_extensions": {"a": None}}, token=token
        )
        assert res.json()["public_extensions"] == {"b": "2"}


class TestUrlValidation:
    def test_protocol_relative_url_rejected(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server), {"docs_url": "//evil.example"}, token=_admin(auth)
        )
        assert res.status_code == 422

    def test_javascript_url_rejected(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"docs_url": "javascript:alert(1)"},
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_https_url_accepted(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"docs_url": "https://docs.dkrz.de"},
            token=_admin(auth),
        )
        assert res.status_code == 200


class TestTypedUiStructures:
    def test_typed_feature_options(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"features": {"stac": {"enabled": True}}},
            token=_admin(auth),
        )
        assert res.status_code == 200
        assert res.json()["features"]["stac"]["enabled"] is True

    def test_unknown_feature_rejected(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"features": {"evil": {"x": 1}}},
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_typed_landing_block(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"landing_blocks": [{"block": "hero", "heading": "Hi"}]},
            token=_admin(auth),
        )
        assert res.status_code == 200

    def test_links_and_feature_link_blocks(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {
                        "kind": "feature",
                        "id": "stac",
                        "path": "/stac",
                        "feature": "stac",
                    }
                ],
                "landing_blocks": [
                    {
                        "block": "links",
                        "links": [{"title": "D", "url": "https://d.de"}],
                    },
                    {
                        "block": "feature-link",
                        "route_id": "stac",
                        "label": "STAC",
                    },
                ],
            },
            token=_admin(auth),
        )
        assert res.status_code == 200, res.text
        blocks = res.json()["landing_blocks"]
        assert blocks[1]["route_id"] == "stac"

    def test_typed_routes_accepted(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {"kind": "landing", "id": "home"},
                    {
                        "kind": "feature",
                        "id": "db",
                        "path": "/databrowser",
                        "feature": "databrowser",
                    },
                ],
                "navigation": [
                    {"route_id": "home", "label": "Home"},
                    {"route_id": "db", "label": "Search"},
                ],
            },
            token=_admin(auth),
        )
        assert res.status_code == 200
        body = res.json()
        assert body["navigation"][0]["route_id"] == "home"

    def test_navigation_must_reference_route(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"navigation": [{"route_id": "ghost", "label": "X"}]},
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_at_most_one_landing_route(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {"kind": "landing", "id": "a"},
                    {"kind": "landing", "id": "b", "path": "/b"},
                ]
            },
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_duplicate_route_id_rejected(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {
                        "kind": "content",
                        "id": "dup",
                        "path": "/a",
                        "ui_id": "default",
                        "content_id": "a",
                    },
                    {
                        "kind": "content",
                        "id": "dup",
                        "path": "/b",
                        "ui_id": "default",
                        "content_id": "b",
                    },
                ]
            },
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_unknown_builtin_feature_rejected(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {
                        "kind": "feature",
                        "id": "x",
                        "path": "/x",
                        "feature": "notafeature",
                    }
                ]
            },
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_route_format_compatibility(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "page",
            format="markdown",
            source="# p",
        )
        _seed_content(
            settings_server,
            token,
            "default",
            "wgt",
            format="sandbox-html",
            source="<script>x</script>",
        )
        sandbox_on_rendered = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {
                        "kind": "sandbox",
                        "id": "s1",
                        "path": "/s1",
                        "ui_id": "default",
                        "content_id": "page",
                    }
                ]
            },
            token=token,
        )
        assert sandbox_on_rendered.status_code == 422
        content_on_sandbox = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {
                        "kind": "content",
                        "id": "c1",
                        "path": "/c1",
                        "ui_id": "default",
                        "content_id": "wgt",
                    }
                ]
            },
            token=token,
        )
        assert content_on_sandbox.status_code == 422

    def test_databrowser_defaults_single_source(self, settings_server: str) -> None:
        body = _get(_ui(settings_server)).json()
        assert "default_flavour" not in body
        assert body["features"]["databrowser"]["default_flavour"] == "freva"

    def test_footer_groups_and_legal_links(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {
                "footer": {
                    "groups": [
                        {
                            "title": "About",
                            "links": [{"label": "Team", "url": "https://t.de"}],
                        }
                    ],
                    "legal_links": [{"label": "Imprint", "url": "https://i.de"}],
                }
            },
            token=_admin(auth),
        )
        assert res.status_code == 200
        assert res.json()["footer"]["groups"][0]["title"] == "About"

    def test_theme_token_validated(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"extra_colors": {"bad token!": "#ffffff"}},
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_route_count_bounded(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {
                        "kind": "content",
                        "id": f"i{n}",
                        "path": f"/p{n}",
                        "ui_id": "default",
                        "content_id": "a",
                    }
                    for n in range(51)
                ]
            },
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_manifest_schema_version(self, settings_server: str) -> None:
        res = _get(_ui(settings_server))
        assert res.json()["schemaVersion"] == 1

    def test_branding_can_be_disabled(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"branding_enabled": False},
            token=_admin(auth),
        )
        assert res.status_code == 200
        assert res.json()["branding_enabled"] is False

    def test_external_nav_url_scheme_checked(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {
                        "kind": "external",
                        "id": "x",
                        "url": "javascript:alert(1)",
                    }
                ]
            },
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_open_map_explicit_empty_clears(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _patch(
            _ui(settings_server),
            {"public_extensions": {"a": "1"}},
            token=token,
        )
        res = _patch(_ui(settings_server), {"public_extensions": {}}, token=token)
        assert res.json()["public_extensions"] == {}


# content documents

class TestContent:
    def test_render_on_write(self, settings_server: str, auth: Dict[str, Any]) -> None:
        res = _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "home",
            format="markdown",
            source="# Hi\n\n**bold**",
        )
        assert res.status_code == 200
        assert "<h1>Hi</h1>" in res.json()["rendered_html"]

    def test_public_hides_source(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "home",
            format="markdown",
            source="# Hi",
        )
        res = _get(_content(settings_server, "default", "home"))
        assert "source" not in res.json()

    def test_admin_source_view(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "home",
            format="markdown",
            source="# Secret",
        )
        url = _content(settings_server, "default", "home") + "?include_source=true"
        res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
        assert res.status_code == 200
        assert res.json()["source"] == "# Secret"

    def test_admin_source_anonymous_forbidden(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "home",
            format="markdown",
            source="# Secret",
        )
        url = _content(settings_server, "default", "home") + "?include_source=true"
        assert requests.get(url).status_code in (401, 403)

    def test_same_content_id_isolated_by_ui(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "home",
            format="markdown",
            source="# Default",
        )
        _seed_content(
            settings_server,
            token,
            "waterpark",
            "home",
            format="markdown",
            source="# Waterpark",
        )
        d = _get(_content(settings_server, "default", "home")).json()
        w = _get(_content(settings_server, "waterpark", "home")).json()
        assert "<h1>Default</h1>" in d["rendered_html"]
        assert "<h1>Waterpark</h1>" in w["rendered_html"]

    def test_shared_content(self, settings_server: str, auth: Dict[str, Any]) -> None:
        _seed_content(
            settings_server,
            _admin(auth),
            "_shared",
            "data-policy",
            format="markdown",
            source="# Policy",
        )
        res = _get(_content(settings_server, "_shared", "data-policy"))
        assert res.status_code == 200
        assert "<h1>Policy</h1>" in res.json()["rendered_html"]

    def test_metadata_only_patch_reuses_render(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "p", format="markdown", source="# Body"
        )
        url = _content(settings_server, "default", "p") + "?include_source=true"
        before = requests.get(url, headers={"Authorization": f"Bearer {token}"}).json()
        _seed_content(settings_server, token, "default", "p", title="New")
        after = requests.get(url, headers={"Authorization": f"Bearer {token}"}).json()
        assert after["title"] == "New"
        assert after["rendered_html"] == before["rendered_html"]
        assert after["source_hash"] == before["source_hash"]

    def test_atomic_failure_preserves_previous(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "p", format="markdown", source="# Good"
        )
        url = _content(settings_server, "default", "p") + "?include_source=true"
        good = requests.get(url, headers={"Authorization": f"Bearer {token}"}).json()
        bad = _seed_content(
            settings_server,
            token,
            "default",
            "p",
            source="x" * (256 * 1024 + 1),
        )
        assert bad.status_code == 422
        still = requests.get(url, headers={"Authorization": f"Bearer {token}"}).json()
        assert still["source"] == good["source"]

    def test_content_is_stale_flag(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "home",
            format="markdown",
            source="# Hi",
        )
        res = _get(_content(settings_server, "default", "home")).json()
        assert res["is_stale"] is False

    def test_missing_content_404(self, settings_server: str) -> None:
        assert _get(_content(settings_server, "default", "nope")).status_code == 404

    def test_delete_content(self, settings_server: str, auth: Dict[str, Any]) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "gone", format="markdown", source="# x"
        )
        assert (
            _delete(
                _content(settings_server, "default", "gone"), token=token
            ).status_code
            == 204
        )
        assert _get(_content(settings_server, "default", "gone")).status_code == 404

    def test_content_etag_304(self, settings_server: str, auth: Dict[str, Any]) -> None:
        _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "home",
            format="markdown",
            source="# Hi",
        )
        res = _get(_content(settings_server, "default", "home"))
        etag = res.headers["ETag"]
        second = _get(
            _content(settings_server, "default", "home"),
            {"If-None-Match": etag},
        )
        assert second.status_code == 304


class TestContentReferences:
    def test_reference_to_missing_content_rejected(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"content_refs": [{"ui_id": "default", "content_id": "ghost"}]},
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_reference_to_existing_content_ok(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "real", format="markdown", source="# r"
        )
        res = _patch(
            _ui(settings_server),
            {"content_refs": [{"ui_id": "default", "content_id": "real"}]},
            token=token,
        )
        assert res.status_code == 200

    def test_referenced_content_protected_from_delete(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "reffed", format="markdown", source="# r"
        )
        _patch(
            _ui(settings_server),
            {"content_refs": [{"ui_id": "default", "content_id": "reffed"}]},
            token=token,
        )
        blocked = _delete(_content(settings_server, "default", "reffed"), token=token)
        assert blocked.status_code == 409
        forced = _delete(
            _content(settings_server, "default", "reffed") + "?force=true",
            token=token,
        )
        assert forced.status_code == 204


class TestSanitizationEndToEnd:
    @pytest.mark.parametrize(
        "fmt,source",
        [
            ("markdown", "# Hi\n<script>evil()</script>"),
            ("markdown", "[x](javascript:alert(1))"),
            ("rst", ".. raw:: html\n\n   <script>evil()</script>"),
            ("rst", ".. include:: /etc/passwd"),
            ("html-fragment", '<img src=x onerror="steal()">'),
            ("html-fragment", '<a href="javascript:alert(1)">x</a>'),
            ("html-fragment", '<iframe src="//evil"></iframe>'),
        ],
    )
    def test_dangerous_content_neutralised(
        self, settings_server: str, auth: Dict[str, Any], fmt: str, source: str
    ) -> None:
        res = _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "danger",
            format=fmt,
            source=source,
        )
        assert res.status_code == 200
        html = res.json()["rendered_html"].lower()
        for token in ("<script", "onerror=", "javascript:", "<iframe", "root:"):
            assert token not in html

    def test_target_blank_link_hardened(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "link",
            format="html-fragment",
            source='<a href="https://x.de" target="_blank">l</a>',
        )
        assert "noopener" in res.json()["rendered_html"]


class TestSandboxDocument:
    def test_public_get_omits_source_and_render(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "widget",
            format="sandbox-html",
            source="<html><body><script>go()</script></body></html>",
        )
        res = _get(_content(settings_server, "default", "widget")).json()
        assert res["rendered_html"] == ""
        assert res["is_sandbox"] is True
        assert "source" not in res

    def test_document_csp_and_headers(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "widget",
            format="sandbox-html",
            source="<html><body><script>go()</script></body></html>",
        )
        doc = requests.get(_content(settings_server, "default", "widget") + "/document")
        assert doc.status_code == 200
        assert doc.headers["content-type"].startswith("text/html")
        assert "<script>go()</script>" in doc.text
        csp = doc.headers.get("content-security-policy", "")
        assert "sandbox allow-scripts" in csp
        assert "script-src 'unsafe-inline'" in csp
        assert doc.headers.get("x-content-type-options") == "nosniff"
        # must not send X-Frame-Options, which would block a cross-origin portal
        assert "x-frame-options" not in {k.lower() for k in doc.headers}

    def test_document_404_for_non_sandbox(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "widget",
            format="markdown",
            source="# Hi",
        )
        doc = requests.get(_content(settings_server, "default", "widget") + "/document")
        assert doc.status_code == 404


class TestRendererVersion:
    def test_metadata_patch_preserves_stale_flag(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        """A metadata-only patch must not relabel old html as current."""

        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "rv",
            format="markdown",
            source="# v1",
        )
        # age the stored renderer_version so the doc is stale
        cfg = ServerConfig()
        with _mongo() as client:
            client[cfg.mongo_db]["ui_contents"].update_one(
                {"_id": "default:rv"}, {"$set": {"renderer_version": "0"}}
            )
            reset_caches()
            before = _get(_content(settings_server, "default", "rv")).json()
            assert before["is_stale"] is True
            # metadata-only patch: is_stale must remain True
            _seed_content(settings_server, token, "default", "rv", title="T")
            reset_caches()
            after = _get(_content(settings_server, "default", "rv")).json()
            assert after["is_stale"] is True
            assert after["title"] == "T"
            # a source patch refreshes the version -> not stale
            _seed_content(
                settings_server,
                token,
                "default",
                "rv",
                format="markdown",
                source="# v2",
            )
            reset_caches()
            fresh = _get(_content(settings_server, "default", "rv")).json()
            assert fresh["is_stale"] is False


class TestContentEtag:
    def test_patch_accepts_admin_body_etag(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        """An editor gets the admin-body ETag from include_source and must be
        able to PATCH with it."""
        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "et",
            format="markdown",
            source="# e",
        )
        url = _content(settings_server, "default", "et") + "?include_source=true"
        admin_etag = requests.get(
            url, headers={"Authorization": f"Bearer {token}"}
        ).headers["ETag"]
        res = _patch(
            _content(settings_server, "default", "et"),
            {"title": "X"},
            token=token,
            headers={"If-Match": admin_etag},
        )
        assert res.status_code == 200


class TestRenderedProtocolRelative:
    def test_protocol_relative_stripped_in_rendered_html(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "pr",
            format="html-fragment",
            source='<a href="//evil.example">x</a>',
        )
        assert res.status_code == 200
        assert "//evil" not in res.json()["rendered_html"]

    def test_relative_and_https_survive(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "pr2",
            format="html-fragment",
            source='<a href="/ok">a</a><a href="https://good.de">b</a>',
        )
        html = res.json()["rendered_html"]
        assert "/ok" in html and "https://good.de" in html


class TestGenericResource:
    """The registry must stay reusable: a non-ui resource proves the storage and
    endpoint layer is not ui-specific. Registered only for this test."""

    def test_registry_is_generic(self, settings_server: str) -> None:
        from pydantic import BaseModel

        from freva_rest.settings_api.registry import REGISTRY, Resource

        class Widget(BaseModel):
            enabled: bool = True

        class WidgetUpdate(BaseModel):
            enabled: Optional[bool] = None

        REGISTRY["widget"] = Resource(Widget, WidgetUpdate)
        try:
            res = _get(f"{BASE.format(server=settings_server)}/widget/default")
            assert res.status_code == 200
            assert res.json() == {"enabled": True}
        finally:
            del REGISTRY["widget"]


class TestNestedPatchOverHttp:
    """Patching one sub-field of a nested object must not reset its siblings."""

    def test_stac_patch_preserves_databrowser(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _patch(
            _ui(settings_server),
            {
                "features": {
                    "databrowser": {
                        "default_flavour": "cmip6",
                        "fixed_facets": {"project": ["a"]},
                    }
                }
            },
            token=token,
        )
        res = _patch(
            _ui(settings_server), {"features": {"stac": {"enabled": True}}}, token=token
        )
        assert res.status_code == 200, res.text
        features = res.json()["features"]
        assert features["stac"]["enabled"] is True
        assert features["databrowser"]["default_flavour"] == "cmip6"
        assert features["databrowser"]["fixed_facets"] == {"project": ["a"]}

    def test_nested_fixed_facets_merge_and_delete(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        url = _ui(settings_server)
        _patch(
            url,
            {"features": {"databrowser": {"fixed_facets": {"project": ["a"]}}}},
            token=token,
        )
        merged = _patch(
            url,
            {"features": {"databrowser": {"fixed_facets": {"experiment": ["b"]}}}},
            token=token,
        )
        assert merged.json()["features"]["databrowser"]["fixed_facets"] == {
            "project": ["a"],
            "experiment": ["b"],
        }
        deleted = _patch(
            url,
            {"features": {"databrowser": {"fixed_facets": {"project": None}}}},
            token=token,
        )
        assert deleted.json()["features"]["databrowser"]["fixed_facets"] == {
            "experiment": ["b"]
        }

    def test_partial_header_patch_keeps_links(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        url = _ui(settings_server)
        _patch(
            url,
            {"header": {"links": [{"label": "L", "url": "https://x.de"}]}},
            token=token,
        )
        res = _patch(url, {"header": {"show": False}}, token=token)
        header = res.json()["header"]
        assert header["show"] is False
        assert header["links"] == [{"label": "L", "url": "https://x.de"}]


class TestIfMatchAsteriskOverHttp:
    def test_asterisk_on_missing_record_is_412(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server, "nosuchui"),
            {"site_title": "X"},
            token=_admin(auth),
            headers={"If-Match": "*"},
        )
        assert res.status_code == 412
        assert _get(_ui(settings_server, "nosuchui")).status_code == 404

    def test_asterisk_on_existing_record_succeeds(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _patch(_ui(settings_server, "wp"), {"site_title": "First"}, token=token)
        res = _patch(
            _ui(settings_server, "wp"),
            {"site_title": "Second"},
            token=token,
            headers={"If-Match": "*"},
        )
        assert res.status_code == 200
        assert res.json()["site_title"] == "Second"


class TestFormatClassGuard:
    """Referenced content must not slip across the rendered/sandbox boundary and
    silently break the ui that points at it."""

    def _seed_referenced(self, server: str, token: str) -> None:
        _seed_content(server, token, "default", "page", format="markdown", source="# a")
        res = _patch(
            _ui(server),
            {
                "routes": [
                    {
                        "kind": "content",
                        "id": "page",
                        "path": "/page",
                        "ui_id": "default",
                        "content_id": "page",
                    }
                ]
            },
            token=token,
        )
        assert res.status_code == 200, res.text

    def test_rendered_to_sandbox_refused_while_referenced(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        self._seed_referenced(settings_server, token)
        res = _seed_content(
            settings_server,
            token,
            "default",
            "page",
            format="sandbox-html",
            source="<html></html>",
        )
        assert res.status_code == 409
        still = _get(_content(settings_server, "default", "page")).json()
        assert still["format"] == "markdown"

    def test_force_allows_the_change(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        self._seed_referenced(settings_server, token)
        res = _patch(
            _content(settings_server, "default", "page") + "?force=true",
            {"format": "sandbox-html", "source": "<html></html>"},
            token=token,
        )
        assert res.status_code == 200
        assert _get(_content(settings_server, "default", "page")).json()["is_sandbox"]

    def test_rendered_to_rendered_is_free(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        self._seed_referenced(settings_server, token)
        res = _seed_content(
            settings_server, token, "default", "page", format="rst", source="A\n=\n"
        )
        assert res.status_code == 200


class TestRebuildEndpoint:
    """The only remedy for is_stale needs to be reachable."""

    def _url(self, server: str) -> str:
        return f"{BASE.format(server=server)}/ui/contents/rebuild"

    def test_requires_admin(self, settings_server: str, auth: Dict[str, Any]) -> None:
        assert requests.post(self._url(settings_server)).status_code in (401, 403)
        res = requests.post(self._url(settings_server), headers=_tok(auth, "user"))
        assert res.status_code == 403

    def test_rebuild_clears_stale(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:

        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "old", format="markdown", source="# Hi"
        )
        config = ServerConfig()
        with _mongo() as client:
            client[config.mongo_db]["ui_contents"].update_one(
                {"_id": "default:old"}, {"$set": {"renderer_version": "0"}}
            )
            reset_caches()
            assert _get(_content(settings_server, "default", "old")).json()["is_stale"]

            res = requests.post(
                self._url(settings_server), headers={"Authorization": f"Bearer {token}"}
            )
            assert res.status_code == 200, res.text
            assert res.json()["rebuilt"] == 1
            reset_caches()
            after = _get(_content(settings_server, "default", "old")).json()
            assert after["is_stale"] is False
            assert "Hi" in after["rendered_html"]


class TestHtmlWrittenDirectlyToMongo:
    """
    The threat model the sanitizer exists for: someone with database write
    access, not API access.
    """

    HOSTILE = '<p>ok</p><script>alert(1)</script><img src="x" onerror="alert(1)">'

    def test_hostile_html_with_a_valid_digest_is_not_served(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:

        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches
        from freva_rest.settings_api.renderers import rendered_hash

        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "forged", format="markdown", source="# a"
        )
        config = ServerConfig()
        with _mongo() as client:
            client[config.mongo_db]["ui_contents"].update_one(
                {"_id": "default:forged"},
                {
                    "$set": {
                        "rendered_html": self.HOSTILE,
                        "rendered_hash": rendered_hash(self.HOSTILE),
                    }
                },
            )
            reset_caches()
            served = _get(_content(settings_server, "default", "forged")).json()
            html = served["rendered_html"]
            assert "<script>" not in html and "alert(1)" not in html
            assert "onerror" not in html
            assert "<p>ok</p>" in html
            assert served["is_stale"] is True

            rebuilt = requests.post(
                f"{BASE.format(server=settings_server)}/ui/contents/rebuild",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert rebuilt.status_code == 200, rebuilt.text
            reset_caches()
            after = _get(_content(settings_server, "default", "forged")).json()
            assert after["is_stale"] is False
            assert "<script>" not in after["rendered_html"]

    def test_html_without_a_digest_is_stale_but_still_served(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        res = _seed_content(
            settings_server,
            token,
            "default",
            "legacyhtml",
            format="markdown",
            source="# Hello",
        )
        rendered = res.json()["rendered_html"]
        config = ServerConfig()
        with _mongo() as client:
            client[config.mongo_db]["ui_contents"].update_one(
                {"_id": "default:legacyhtml"}, {"$unset": {"rendered_hash": ""}}
            )
            reset_caches()
            served = _get(_content(settings_server, "default", "legacyhtml")).json()
            assert served["is_stale"] is True
            assert served["rendered_html"] == rendered

            requests.post(
                f"{BASE.format(server=settings_server)}/ui/contents/rebuild",
                headers={"Authorization": f"Bearer {token}"},
            )
            reset_caches()
            after = _get(_content(settings_server, "default", "legacyhtml")).json()
            assert after["is_stale"] is False
            assert after["rendered_html"] == rendered


class TestRenderedSizeCeilingHoldsForWhatIsServed:
    """The size check has to measure the bytes that actually ship"""

    PATHOLOGICAL = "<a><ul><div><table></ul></div><a><caption>" * 3799

    def test_an_ordinary_patch_cannot_exceed_the_ceiling(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "toobig",
            format="html-fragment",
            source=self.PATHOLOGICAL,
        )
        assert res.status_code == 422, res.status_code
        assert "larger than" in res.text

    def test_what_is_stored_is_stable_under_the_sanitiser(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        from freva_rest.settings_api.sanitizer import sanitize_html

        res = _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "nesting",
            format="html-fragment",
            source="<div><p><table><ul></p></table>",
        )
        assert res.status_code == 200, res.text
        html = res.json()["rendered_html"]
        assert sanitize_html(html) == html

        first = _get(_content(settings_server, "default", "nesting"))
        second = _get(_content(settings_server, "default", "nesting"))
        assert first.json()["rendered_html"] == html
        assert first.headers["ETag"] == second.headers["ETag"]


class TestDeleteHonoursIfMatch:
    """The internal CAS only covers the window *inside* the request, so a client
    that read revision 1, waited while someone else wrote revision 2 and then
    deleted must not destroy a generation it never saw."""

    def _seed(self, server: str, token: str, cid: str = "delme") -> str:
        res = _seed_content(
            server, token, "default", cid, format="markdown", source="# a"
        )
        assert res.status_code == 200, res.text
        return str(res.headers["ETag"])

    def test_the_current_tag_deletes(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        self._seed(settings_server, token, "d1")
        tag = _get(_content(settings_server, "default", "d1")).headers["ETag"]
        res = requests.delete(
            _content(settings_server, "default", "d1"),
            headers={"Authorization": f"Bearer {token}", "If-Match": tag},
        )
        assert res.status_code == 204, res.text
        assert _get(_content(settings_server, "default", "d1")).status_code == 404

    def test_a_stale_tag_is_412_and_the_newer_version_survives(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        self._seed(settings_server, token, "d2")
        stale = _get(_content(settings_server, "default", "d2")).headers["ETag"]
        assert (
            _seed_content(
                settings_server, token, "default", "d2", title="Renamed"
            ).status_code
            == 200
        )
        res = requests.delete(
            _content(settings_server, "default", "d2"),
            headers={"Authorization": f"Bearer {token}", "If-Match": stale},
        )
        assert res.status_code == 412, res.text
        survivor = _get(_content(settings_server, "default", "d2"))
        assert survivor.status_code == 200
        assert survivor.json()["title"] == "Renamed"

    def test_the_admin_tag_is_accepted_too(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        self._seed(settings_server, token, "d3")
        admin_tag = _get(
            _content(settings_server, "default", "d3") + "?include_source=true",
            headers={"Authorization": f"Bearer {token}"},
        ).headers["ETag"]
        public_tag = _get(_content(settings_server, "default", "d3")).headers["ETag"]
        assert admin_tag != public_tag
        res = requests.delete(
            _content(settings_server, "default", "d3"),
            headers={"Authorization": f"Bearer {token}", "If-Match": admin_tag},
        )
        assert res.status_code == 204, res.text

    def test_a_weak_tag_never_matches(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        self._seed(settings_server, token, "d4")
        tag = _get(_content(settings_server, "default", "d4")).headers["ETag"]
        res = requests.delete(
            _content(settings_server, "default", "d4"),
            headers={"Authorization": f"Bearer {token}", "If-Match": f"W/{tag}"},
        )
        assert res.status_code == 412
        assert _get(_content(settings_server, "default", "d4")).status_code == 200

    def test_star_succeeds_when_the_document_exists(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        self._seed(settings_server, token, "d5")
        res = requests.delete(
            _content(settings_server, "default", "d5"),
            headers={"Authorization": f"Bearer {token}", "If-Match": "*"},
        )
        assert res.status_code == 204

    def test_star_on_a_missing_document_is_404_not_412(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = requests.delete(
            _content(settings_server, "default", "never-existed"),
            headers={
                "Authorization": f"Bearer {_admin(auth)}",
                "If-Match": "*",
            },
        )
        assert res.status_code == 404

    def test_no_header_stays_unconditional(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        self._seed(settings_server, token, "d6")
        assert (
            _delete(_content(settings_server, "default", "d6"), token=token).status_code
            == 204
        )

    def test_a_referenced_document_is_still_409_before_any_precondition(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        self._seed(settings_server, token, "d7")
        tag = _get(_content(settings_server, "default", "d7")).headers["ETag"]
        assert (
            _patch(
                _ui(settings_server),
                {"content_refs": [{"ui_id": "default", "content_id": "d7"}]},
                token=token,
            ).status_code
            == 200
        )
        res = requests.delete(
            _content(settings_server, "default", "d7"),
            headers={"Authorization": f"Bearer {token}", "If-Match": tag},
        )
        assert res.status_code == 409
        _patch(_ui(settings_server), {"content_refs": []}, token=token)

    def test_a_settings_record_honours_it_too(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _patch(_ui(settings_server, "cond"), {"site_title": "One"}, token=token)
        stale = _get(_ui(settings_server, "cond")).headers["ETag"]
        _patch(_ui(settings_server, "cond"), {"site_title": "Two"}, token=token)
        res = requests.delete(
            _ui(settings_server, "cond"),
            headers={"Authorization": f"Bearer {token}", "If-Match": stale},
        )
        assert res.status_code == 412
        assert _get(_ui(settings_server, "cond")).json()["site_title"] == "Two"

        fresh = _get(_ui(settings_server, "cond")).headers["ETag"]
        ok = requests.delete(
            _ui(settings_server, "cond"),
            headers={"Authorization": f"Bearer {token}", "If-Match": fresh},
        )
        assert ok.status_code == 204


class TestDefaultRecordDeleteIsAReset:
    """
    `default` always has a representation, so DELETE cannot mean
    "it stops existing"
    """

    def test_deleting_an_override_restores_the_built_in_defaults(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _patch(_ui(settings_server), {"site_title": "Customised"}, token=token)
        assert _get(_ui(settings_server)).json()["site_title"] == "Customised"

        assert _delete(_ui(settings_server), token=token).status_code == 204
        after = _get(_ui(settings_server))
        assert after.status_code == 200
        assert after.json()["site_title"] == "Freva"

    def test_it_is_idempotent(self, settings_server: str, auth: Dict[str, Any]) -> None:
        token = _admin(auth)
        _patch(_ui(settings_server), {"site_title": "Customised"}, token=token)
        assert _delete(_ui(settings_server), token=token).status_code == 204
        # no override left; a reset is still a reset
        assert _delete(_ui(settings_server), token=token).status_code == 204
        assert _get(_ui(settings_server)).status_code == 200

    def test_star_matches_the_synthesised_representation(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _delete(_ui(settings_server), token=token)  # ensure no override
        res = requests.delete(
            _ui(settings_server),
            headers={"Authorization": f"Bearer {token}", "If-Match": "*"},
        )
        assert res.status_code == 204

    def test_a_stale_tag_still_blocks_the_reset(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _patch(_ui(settings_server), {"site_title": "One"}, token=token)
        stale = _get(_ui(settings_server)).headers["ETag"]
        _patch(_ui(settings_server), {"site_title": "Two"}, token=token)
        res = requests.delete(
            _ui(settings_server),
            headers={"Authorization": f"Bearer {token}", "If-Match": stale},
        )
        assert res.status_code == 412
        assert _get(_ui(settings_server)).json()["site_title"] == "Two"
        _delete(_ui(settings_server), token=token)

    def test_a_named_record_still_404s_when_absent(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _delete(_ui(settings_server, "no-such-record"), token=_admin(auth))
        assert res.status_code == 404


class TestSandboxRevocation:
    """The /document endpoint is a revocation boundary."""

    def test_a_deleted_sandbox_document_is_gone_at_once(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "box",
            format="sandbox-html",
            source="<script>go()</script>",
        )
        url = _content(settings_server, "default", "box") + "/document"
        assert "go()" in _get(url).text
        assert (
            _delete(
                _content(settings_server, "default", "box"), token=token
            ).status_code
            == 204
        )
        # no read-TTL grace period for executable content
        assert _get(url).status_code == 404

    def test_the_document_response_is_never_shared_cached(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "box2",
            format="sandbox-html",
            source="<p>x</p>",
        )
        res = _get(_content(settings_server, "default", "box2") + "/document")
        assert res.headers["Cache-Control"] == "private, no-store"


class TestAcceptedDanglingReferenceWindow:
    """An *accepted* property rather than one that is closed.

    A ui PATCH validates that content exists, a concurrent DELETE sees no
    committed reference and removes it, and the PATCH then commits its
    reference. Both requests are ordinary and both succeed. Closing this needs a
    cross-document transaction, which needs a replica set - a deployment
    requirement this project does not impose. The audit endpoint is the
    documented compensating control, not a fix.
    """

    def test_the_audit_endpoint_reports_a_dangling_reference(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "doomed", format="markdown", source="# a"
        )
        _patch(
            _ui(settings_server),
            {"content_refs": [{"ui_id": "default", "content_id": "doomed"}]},
            token=token,
        )
        # force=true is the same end state the race produces
        assert (
            _delete(
                _content(settings_server, "default", "doomed") + "?force=true",
                token=token,
            ).status_code
            == 204
        )
        audit = _get(
            f"{BASE.format(server=settings_server)}/ui/contents/audit",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert audit.status_code == 200
        report = audit.json()
        assert report["complete"] is True
        assert report["page_consistent"] is False
        assert any("doomed" in problem for problem in report["problems"])
        _patch(_ui(settings_server), {"content_refs": []}, token=token)

    def test_the_reference_guard_still_catches_the_ordinary_case(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "kept", format="markdown", source="# a"
        )
        _patch(
            _ui(settings_server),
            {"content_refs": [{"ui_id": "default", "content_id": "kept"}]},
            token=token,
        )
        assert (
            _delete(
                _content(settings_server, "default", "kept"), token=token
            ).status_code
            == 409
        )
        _patch(_ui(settings_server), {"content_refs": []}, token=token)


class TestMetadataPatchDoesNotMigrate:
    """A title edit is not a renderer migration."""

    def test_a_title_edit_leaves_a_stale_document_stale(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "aged", format="markdown", source="# a"
        )
        config = ServerConfig()
        with _mongo() as client:
            contents = client[config.mongo_db]["ui_contents"]
            contents.update_one(
                {"_id": "default:aged"},
                {"$set": {"renderer_version": "0+ancient"}},
            )
            reset_caches()
            before = _get(_content(settings_server, "default", "aged")).json()
            assert before["is_stale"] is True

            renamed = _seed_content(
                settings_server, token, "default", "aged", title="Renamed"
            )
            assert renamed.status_code == 200, renamed.text
            reset_caches()
            after = _get(_content(settings_server, "default", "aged")).json()
            assert after["title"] == "Renamed"
            assert after["is_stale"] is True
            assert contents.find_one({"_id": "default:aged"})["renderer_version"] == (
                "0+ancient"
            )

            requests.post(
                f"{BASE.format(server=settings_server)}/ui/contents/rebuild",
                headers={"Authorization": f"Bearer {token}"},
            )
            reset_caches()
            rebuilt = _get(_content(settings_server, "default", "aged")).json()
            assert rebuilt["is_stale"] is False
            assert rebuilt["title"] == "Renamed"

    def test_resending_an_unchanged_source_does_not_bump_the_rendering(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        # an editor's full save round-trips every field
        token = _admin(auth)
        first = _seed_content(
            settings_server, token, "default", "resave", format="markdown", source="# a"
        )
        assert first.status_code == 200
        again = _seed_content(
            settings_server,
            token,
            "default",
            "resave",
            format="markdown",
            source="# a",
            title="New title",
        )
        assert again.status_code == 200
        assert again.json()["rendered_html"] == first.json()["rendered_html"]

    def test_a_real_source_change_still_renders(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "edited", format="markdown", source="# a"
        )
        changed = _seed_content(
            settings_server,
            token,
            "default",
            "edited",
            format="markdown",
            source="# b",
        )
        assert "b" in changed.json()["rendered_html"]
        assert changed.json()["is_stale"] is False


class TestAnnouncementWindowsOverHttp:
    def test_a_bad_timestamp_is_refused(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"announcements": [{"id": "a", "message": "x", "starts_at": "soon"}]},
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_a_reverse_window_is_refused(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {
                "announcements": [
                    {
                        "id": "a",
                        "message": "x",
                        "starts_at": "2026-05-01T00:00:00Z",
                        "ends_at": "2020-01-01T00:00:00Z",
                    }
                ]
            },
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_a_valid_window_round_trips_in_one_spelling(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        res = _patch(
            _ui(settings_server),
            {
                "announcements": [
                    {
                        "id": "a",
                        "message": "x",
                        "starts_at": "2026-05-01T09:00:00Z",
                        "ends_at": "2026-06-01T09:00:00Z",
                    }
                ]
            },
            token=token,
        )
        assert res.status_code == 200, res.text
        stored = _get(_ui(settings_server)).json()["announcements"][0]
        assert stored["starts_at"] == "2026-05-01T09:00:00+00:00"
        _patch(_ui(settings_server), {"announcements": []}, token=token)


class TestSearchBlockTarget:
    def test_a_search_block_may_not_target_a_content_route(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {
                        "kind": "content",
                        "id": "c1",
                        "path": "/c",
                        "ui_id": "default",
                        "content_id": "a",
                    }
                ],
                "landing_blocks": [{"block": "search", "target_route_id": "c1"}],
            },
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_a_route_for_a_disabled_feature_is_still_accepted(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        res = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {"kind": "feature", "id": "s1", "path": "/s", "feature": "stac"}
                ],
                "features": {"stac": {"enabled": False}},
            },
            token=token,
        )
        assert res.status_code == 200, res.text
        _patch(_ui(settings_server), {"routes": [], "navigation": []}, token=token)


class TestStoredDocumentHardening:
    def test_a_metadata_patch_never_serves_raw_stored_html(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        """The response, an immediate GET and the cached copy must all be
        sanitised. Deliberately no reset_caches() after the PATCH"""
        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches
        from freva_rest.settings_api.renderers import rendered_hash

        token = _admin(auth)
        forged = "<p>ok</p><script>alert(1)</script>"
        _seed_content(
            settings_server,
            token,
            "default",
            "forgedmeta",
            format="html-fragment",
            source="<p>ok</p>",
        )
        config = ServerConfig()
        with _mongo() as client:
            client[config.mongo_db]["ui_contents"].update_one(
                {"_id": "default:forgedmeta"},
                {
                    "$set": {
                        "rendered_html": forged,
                        "rendered_hash": rendered_hash(forged),
                    }
                },
            )
            reset_caches()
            written = _seed_content(
                settings_server, token, "default", "forgedmeta", title="Renamed"
            )
            assert written.status_code == 200, written.text
            assert "<script>" not in written.text
            assert "<p>ok</p>" in written.json()["rendered_html"]

            served = _get(_content(settings_server, "default", "forgedmeta"))
            assert "<script>" not in served.text
            assert served.headers["ETag"] == written.headers["ETag"]

            stored = client[config.mongo_db]["ui_contents"].find_one(
                {"_id": "default:forgedmeta"}
            )
            assert stored["rendered_html"] == forged  # raw state preserved

    def test_a_document_with_no_source_is_repaired_not_erased(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        first = _seed_content(
            settings_server,
            token,
            "default",
            "nosource",
            format="markdown",
            source="# valuable",
        )
        rendered = first.json()["rendered_html"]
        config = ServerConfig()
        with _mongo() as client:
            contents = client[config.mongo_db]["ui_contents"]
            contents.update_one({"_id": "default:nosource"}, {"$unset": {"source": ""}})
            reset_caches()

            refused = _seed_content(
                settings_server, token, "default", "nosource", title="Renamed"
            )
            assert refused.status_code == 422, refused.text
            assert "source" in refused.text
            assert (
                contents.find_one({"_id": "default:nosource"})["rendered_html"]
                == rendered
            )

            rebuilt = requests.post(
                f"{BASE.format(server=settings_server)}/ui/contents/rebuild",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert rebuilt.status_code == 200
            assert rebuilt.json()["failed"] >= 1
            assert (
                contents.find_one({"_id": "default:nosource"})["rendered_html"]
                == rendered
            )

            repaired = _seed_content(
                settings_server,
                token,
                "default",
                "nosource",
                format="markdown",
                source="# back",
            )
            assert repaired.status_code == 200
            assert "back" in repaired.json()["rendered_html"]

    def test_a_referenced_document_reports_409_even_with_a_stale_tag(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        """The discriminating sequence"""
        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "reforder",
            format="markdown",
            source="# a",
        )
        stale = _get(_content(settings_server, "default", "reforder")).headers["ETag"]
        # the client's tag goes stale...
        assert (
            _seed_content(
                settings_server, token, "default", "reforder", title="Moved on"
            ).status_code
            == 200
        )
        # ...and the content becomes referenced
        assert (
            _patch(
                _ui(settings_server),
                {"content_refs": [{"ui_id": "default", "content_id": "reforder"}]},
                token=token,
            ).status_code
            == 200
        )
        res = requests.delete(
            _content(settings_server, "default", "reforder"),
            headers={"Authorization": f"Bearer {token}", "If-Match": stale},
        )
        assert res.status_code == 409, res.status_code
        assert "referenced" in res.text
        _patch(_ui(settings_server), {"content_refs": []}, token=token)

    def test_a_malformed_sandbox_source_is_refused_on_read(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        """The read path must not stringify a damaged stored source"""
        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "brokenbox",
            format="sandbox-html",
            source="<p>fine</p>",
        )
        url = _content(settings_server, "default", "brokenbox") + "/document"
        assert _get(url).status_code == 200

        config = ServerConfig()
        with _mongo() as client:
            contents = client[config.mongo_db]["ui_contents"]
            for update in (
                {"$unset": {"source": ""}},
                {"$set": {"source": None}},
                {"$set": {"source": ["<b>x</b>"]}},
                {"$set": {"source": {"a": "<script>go()</script>"}}},
            ):
                contents.update_one({"_id": "default:brokenbox"}, update)
                reset_caches()
                res = _get(url)
                assert res.status_code == 422, (update, res.status_code)
                assert "<b>" not in res.text
                assert "<script>" not in res.text

            # and a repair through PATCH brings it back
            repaired = _seed_content(
                settings_server,
                token,
                "default",
                "brokenbox",
                format="sandbox-html",
                source="<p>repaired</p>",
            )
            assert repaired.status_code == 200, repaired.text
            reset_caches()
            served = _get(url)
            assert served.status_code == 200
            assert served.text == "<p>repaired</p>"

    def test_an_oversized_sandbox_document_is_refused_on_read(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import (
            MAX_SANDBOX_SOURCE_BYTES,
            reset_caches,
        )

        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "bigbox",
            format="sandbox-html",
            source="<p>small</p>",
        )
        config = ServerConfig()
        with _mongo() as client:
            client[config.mongo_db]["ui_contents"].update_one(
                {"_id": "default:bigbox"},
                {"$set": {"source": "x" * (MAX_SANDBOX_SOURCE_BYTES + 1)}},
            )
            reset_caches()
            res = _get(_content(settings_server, "default", "bigbox") + "/document")
            assert res.status_code == 422

    def test_the_public_content_shape_does_not_publish_dependency_versions(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        from freva_rest.settings_api.sanitizer import RENDERER_VERSION

        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "gen", format="markdown", source="# a"
        )
        public = _get(_content(settings_server, "default", "gen"))
        assert public.json()["renderer_version"] == RENDERER_VERSION
        assert "nh3=" not in public.text

        admin = _get(
            _content(settings_server, "default", "gen") + "?include_source=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert "nh3=" in admin.json()["renderer_fingerprint"]


class TestUrlAndPathHardening:
    def test_obfuscated_javascript_url_rejected(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {"institution_url": "java\nscript:alert(1)"},
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_protocol_relative_route_path_rejected(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server),
            {
                "routes": [
                    {
                        "kind": "content",
                        "id": "p",
                        "path": "//evil",
                        "ui_id": "default",
                        "content_id": "c",
                    }
                ]
            },
            token=_admin(auth),
        )
        assert res.status_code == 422

    def test_trailing_newline_record_id_rejected(self, settings_server: str) -> None:
        res = _get(f"{BASE.format(server=settings_server)}/ui/default%0A")
        assert res.status_code == 422


class TestRstTitleOverHttp:
    def test_rendered_page_keeps_its_heading(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "rstpage",
            format="rst",
            source="Page Title\n==========\n\nBody text.\n",
        )
        assert res.status_code == 200
        html = res.json()["rendered_html"]
        assert "<h1>" in html and "Page Title" in html


class TestPreconditionsMergesAndAudit:
    def test_if_match_star_works_on_synthesised_default(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        assert _get(_ui(settings_server)).status_code == 200
        res = _patch(
            _ui(settings_server),
            {"site_title": "First"},
            token=_admin(auth),
            headers={"If-Match": "*"},
        )
        assert res.status_code == 200, res.text
        assert res.json()["site_title"] == "First"

    def test_if_match_star_still_412s_on_a_named_record(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _patch(
            _ui(settings_server, "neverwritten"),
            {"site_title": "X"},
            token=_admin(auth),
            headers={"If-Match": "*"},
        )
        assert res.status_code == 412

    def test_header_show_null_resets(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        url = _ui(settings_server)
        _patch(url, {"header": {"show": False}}, token=token)
        res = _patch(url, {"header": {"show": None}}, token=token)
        assert res.status_code == 200, res.text
        assert res.json()["header"]["show"] is True

    def test_partial_content_ref_keeps_ui_id(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "_shared", "old", format="markdown", source="a"
        )
        _seed_content(
            settings_server, token, "_shared", "new", format="markdown", source="b"
        )
        url = _ui(settings_server)
        _patch(
            url,
            {"header": {"content": {"ui_id": "_shared", "content_id": "old"}}},
            token=token,
        )
        res = _patch(url, {"header": {"content": {"content_id": "new"}}}, token=token)
        assert res.status_code == 200, res.text
        assert res.json()["header"]["content"] == {
            "ui_id": "_shared",
            "content_id": "new",
        }

    def test_infinity_in_public_extensions_rejected(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = requests.patch(
            _ui(settings_server),
            data='{"public_extensions": {"a": 1e999}}',
            headers={
                "Authorization": f"Bearer {_admin(auth)}",
                "Content-Type": "application/json",
            },
        )
        assert res.status_code == 422
        assert _get(_ui(settings_server)).json() is not None

    def test_metadata_patch_keeps_a_versionless_document_stale(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:

        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "legacy", format="markdown", source="# a"
        )
        config = ServerConfig()
        with _mongo() as client:
            client[config.mongo_db]["ui_contents"].update_one(
                {"_id": "default:legacy"}, {"$unset": {"renderer_version": ""}}
            )
            reset_caches()
            res = _seed_content(
                settings_server, token, "default", "legacy", title="New"
            )
            assert res.status_code == 200
            assert res.json()["is_stale"] is True

    def test_rebuild_picks_up_a_document_without_a_revision(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:

        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "norev", format="markdown", source="# a"
        )
        config = ServerConfig()
        with _mongo() as client:
            # a legacy document: no revision field at all, stale renderer version
            client[config.mongo_db]["ui_contents"].update_one(
                {"_id": "default:norev"},
                {"$unset": {"revision": ""}, "$set": {"renderer_version": "0"}},
            )
            reset_caches()
            res = requests.post(
                f"{BASE.format(server=settings_server)}/ui/contents/rebuild",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert res.status_code == 200, res.text
            assert res.json()["rebuilt"] == 1, res.json()
            assert res.json()["skipped"] == 0

    def test_audit_reports_a_clean_deployment(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = requests.get(
            f"{BASE.format(server=settings_server)}/ui/contents/audit",
            headers={"Authorization": f"Bearer {_admin(auth)}"},
        )
        assert res.status_code == 200, res.text
        assert res.json() == {
            "page_consistent": True,
            "problems": [],
            "complete": True,
        }

    def test_audit_requires_admin(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        url = f"{BASE.format(server=settings_server)}/ui/contents/audit"
        assert requests.get(url).status_code in (401, 403)
        assert requests.get(url, headers=_tok(auth, "user")).status_code == 403

    def test_audit_finds_a_reference_broken_out_of_band(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:

        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "page", format="markdown", source="# a"
        )
        _patch(
            _ui(settings_server),
            {
                "routes": [
                    {
                        "kind": "content",
                        "id": "page",
                        "path": "/page",
                        "ui_id": "default",
                        "content_id": "page",
                    }
                ]
            },
            token=token,
        )
        # simulate the residue of the non-atomic window: flip the format straight
        # in mongo, as a lost race would
        config = ServerConfig()
        with _mongo() as client:
            client[config.mongo_db]["ui_contents"].update_one(
                {"_id": "default:page"}, {"$set": {"format": "sandbox-html"}}
            )
            reset_caches()
            res = requests.get(
                f"{BASE.format(server=settings_server)}/ui/contents/audit",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert res.status_code == 200
            assert res.json()["page_consistent"] is False
            assert any("sandbox-html" in p for p in res.json()["problems"])

    def test_public_cache_header_has_no_stale_while_revalidate(
        self, settings_server: str
    ) -> None:
        cache = _get(_ui(settings_server)).headers["Cache-Control"]
        assert "stale-while-revalidate" not in cache


class TestRouteTableFormatClassAndRepair:
    ROUTE_COUNT = 10

    def test_module_imports_and_all_routes_register(self, settings_server: str) -> None:
        """A missing name in a function annotation compiles fine and then raises
        NameError at import time, which compileall does not catch."""
        from freva_rest.rest import app

        registered = {
            (sorted(r.methods)[0], r.path)
            for r in app.routes
            if hasattr(r, "methods") and "/settings" in r.path
        }
        for method, path in [
            ("GET", "/api/freva-nextgen/settings/{resource}/_schema"),
            ("GET", "/api/freva-nextgen/settings/{resource}/{record_id}"),
            ("PATCH", "/api/freva-nextgen/settings/{resource}/{record_id}"),
            ("DELETE", "/api/freva-nextgen/settings/{resource}/{record_id}"),
            ("GET", "/api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}"),
            ("PATCH", "/api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}"),
            ("DELETE", "/api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}"),
            ("GET", "/api/freva-nextgen/settings/ui/contents/audit"),
            ("POST", "/api/freva-nextgen/settings/ui/contents/rebuild"),
            (
                "GET",
                "/api/freva-nextgen/settings/ui/{ui_id}/contents/"
                "{content_id}/document",
            ),
        ]:
            assert (method, path) in registered, f"{method} {path} is not registered"
        assert len(registered) == self.ROUTE_COUNT, sorted(registered)

    def test_openapi_generates(self, settings_server: str) -> None:
        res = _get(OPENAPI.format(server=settings_server))
        assert res.status_code == 200
        assert "/api/freva-nextgen/settings/ui/contents/audit" in res.json()["paths"]

    def test_format_class_is_immutable_even_when_unreferenced(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        # the format class is immutable outright, so the guard reads nothing
        # from the ui records
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "loose", format="markdown", source="# a"
        )
        res = _seed_content(
            settings_server,
            token,
            "default",
            "loose",
            format="sandbox-html",
            source="<html></html>",
        )
        assert res.status_code == 409, res.text
        assert _get(_content(settings_server, "default", "loose")).json()["format"] == (
            "markdown"
        )

    def test_force_still_migrates_the_class(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "mig", format="markdown", source="# a"
        )
        res = _patch(
            _content(settings_server, "default", "mig") + "?force=true",
            {"format": "sandbox-html", "source": "<html></html>"},
            token=token,
        )
        assert res.status_code == 200, res.text
        assert _get(_content(settings_server, "default", "mig")).json()["is_sandbox"]

    def test_creating_content_is_not_blocked(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "brandnew",
            format="sandbox-html",
            source="<html></html>",
        )
        assert res.status_code == 200, res.text

    def test_audit_flags_an_unusable_format(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:

        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "page", format="markdown", source="# a"
        )
        _patch(
            _ui(settings_server),
            {
                "routes": [
                    {
                        "kind": "content",
                        "id": "page",
                        "path": "/page",
                        "ui_id": "default",
                        "content_id": "page",
                    }
                ]
            },
            token=token,
        )
        config = ServerConfig()
        with _mongo() as client:
            # not sandbox-html, but not renderable either
            client[config.mongo_db]["ui_contents"].update_one(
                {"_id": "default:page"}, {"$set": {"format": "bogus"}}
            )
            reset_caches()
            res = requests.get(
                f"{BASE.format(server=settings_server)}/ui/contents/audit",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert res.status_code == 200
            assert res.json()["page_consistent"] is False
            assert any("unusable format" in p for p in res.json()["problems"])

    def test_a_malformed_revision_can_be_repaired_by_a_patch(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:

        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        _patch(_ui(settings_server), {"site_title": "Before"}, token=token)
        config = ServerConfig()
        with _mongo() as client:
            client[config.mongo_db]["settings"].update_one(
                {"_id": "ui:default"}, {"$set": {"revision": "oops"}}
            )
            reset_caches()
            res = _patch(_ui(settings_server), {"site_title": "After"}, token=token)
            assert res.status_code == 200, res.text
            assert res.json()["site_title"] == "After"
            stored = client[config.mongo_db]["settings"].find_one({"_id": "ui:default"})
            assert stored["revision"] == 1  # normalised on the way out

    def test_rebuild_repairs_a_malformed_revision(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:

        from freva_rest.config import ServerConfig
        from freva_rest.settings_api.core import reset_caches

        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "badrev", format="markdown", source="# a"
        )
        config = ServerConfig()
        with _mongo() as client:
            client[config.mongo_db]["ui_contents"].update_one(
                {"_id": "default:badrev"},
                {"$set": {"revision": "oops", "renderer_version": "0"}},
            )
            reset_caches()
            res = requests.post(
                f"{BASE.format(server=settings_server)}/ui/contents/rebuild",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert res.status_code == 200, res.text
            assert res.json()["rebuilt"] == 1, res.json()
            stored = client[config.mongo_db]["ui_contents"].find_one(
                {"_id": "default:badrev"}
            )
            assert stored["revision"] == 1

    def test_cache_control_is_the_documented_policy(self, settings_server: str) -> None:
        assert _get(_ui(settings_server)).headers["Cache-Control"] == (
            "public, max-age=30"
        )


class TestFormatClassRace:
    """
    The format-class check must hold under real concurrency.
    """

    def test_two_concurrent_creates_cannot_cross_the_class_boundary(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        import asyncio

        from fastapi import HTTPException
        from pymongo import AsyncMongoClient

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        reset_caches()
        config = ServerConfig()

        async def _run() -> Any:
            # a client created on, and closed from, this loop
            client: Any = AsyncMongoClient(config.mongo_url)
            try:

                class _Config:
                    mongo_collection_ui_contents = client[config.mongo_db][
                        "ui_contents"
                    ]

                store = ContentStore(_Config(), "default", "raced")
                collection = _Config.mongo_collection_ui_contents
                both_read = asyncio.Event()
                seen_absent = 0
                real_find_one = collection.find_one

                async def _barriered_find_one(*args: Any, **kwargs: Any) -> Any:
                    nonlocal seen_absent
                    found = await real_find_one(*args, **kwargs)
                    if found is None and not both_read.is_set():
                        seen_absent += 1
                        if seen_absent >= 2:
                            both_read.set()
                        else:
                            await asyncio.wait_for(both_read.wait(), timeout=10)
                    return found

                collection.find_one = _barriered_find_one  # type: ignore[method-assign]
                try:
                    return await asyncio.gather(
                        store.patch(
                            ContentSource.model_validate(
                                {"format": "markdown", "source": "# a"}
                            )
                        ),
                        store.patch(
                            ContentSource.model_validate(
                                {"format": "sandbox-html", "source": "<html></html>"}
                            )
                        ),
                        return_exceptions=True,
                    )
                finally:
                    collection.find_one = real_find_one  # type: ignore[method-assign]
            finally:
                await client.close()

        results = asyncio.run(_run())
        reset_caches()
        stored = _get(_content(settings_server, "default", "raced"))
        assert stored.status_code == 200, stored.text
        assert stored.json()["format"] in ("markdown", "sandbox-html")
        refused = [
            r
            for r in results
            if isinstance(r, HTTPException) and r.status_code in (409, 412)
        ]
        succeeded = [r for r in results if isinstance(r, tuple)]
        assert len(succeeded) == 1, results
        assert len(refused) == 1, results

    def test_sequential_create_then_cross_is_refused(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "seq", format="markdown", source="# a"
        )
        res = _seed_content(
            settings_server,
            token,
            "default",
            "seq",
            format="sandbox-html",
            source="<html></html>",
        )
        assert res.status_code == 409, res.text


class TestAbaProtection:
    """Delete-and-recreate resets the revision counter, so a revision-only CAS
    lets a stale writer overwrite a whole new generation of the document."""

    def test_stale_writer_cannot_overwrite_a_recreated_document(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:

        from freva_rest.settings_api.core import CAS_TOKEN_FIELD, reset_caches

        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "aba", format="markdown", source="# one"
        )
        config = ServerConfig()
        with _mongo() as client:
            collection = client[config.mongo_db]["ui_contents"]
            first = collection.find_one({"_id": "default:aba"})
            assert first[CAS_TOKEN_FIELD]

            _delete(_content(settings_server, "default", "aba"), token=token)
            _seed_content(
                settings_server,
                token,
                "default",
                "aba",
                format="markdown",
                source="# two",
            )
            second = collection.find_one({"_id": "default:aba"})
            # same revision as the first generation, different identity
            assert second["revision"] == first["revision"]
            assert second[CAS_TOKEN_FIELD] != first[CAS_TOKEN_FIELD]

            # a stale writer holding the first generation's CAS state matches nothing
            stale = collection.replace_one(
                {"_id": "default:aba", CAS_TOKEN_FIELD: first[CAS_TOKEN_FIELD]},
                dict(first, source="# stale"),
            )
            assert stale.matched_count == 0
            reset_caches()
            current = requests.get(
                _content(settings_server, "default", "aba") + "?include_source=true",
                headers={"Authorization": f"Bearer {token}"},
            ).json()
            assert current["source"] == "# two"


class TestConditionalDelete:
    """
    A delete that has work to do before deleting must not remove a generation
    it never validated.
    """

    def test_delete_endpoint_409s_when_the_document_moves_under_it(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        import mock

        from freva_rest.settings_api import endpoints as ep
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD, reset_caches

        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "delrace", format="markdown", source="1"
        )
        config = ServerConfig()
        with _mongo() as client:
            contents = client[config.mongo_db]["ui_contents"]
            first = contents.find_one({"_id": "default:delrace"})

            original = ep._referring_uis

            async def _recreate_then_scan(ui_id: str, content_id: str) -> Any:
                # The endpoint captures cas_state BEFORE this scan and deletes AFTER
                # it, so this is exactly the window at risk. Replacing the document
                # here makes the delete's captured identity stale.
                if content_id == "delrace":
                    contents.delete_one({"_id": "default:delrace"})
                    contents.insert_one(
                        dict(
                            first,
                            source="2",
                            revision=1,
                            **{CAS_TOKEN_FIELD: "a-different-generation"},
                        )
                    )
                return await original(ui_id, content_id)

            with mock.patch.object(ep, "_referring_uis", _recreate_then_scan):
                res = _delete(
                    _content(settings_server, "default", "delrace"), token=token
                )

            assert res.status_code == 409, res.text
            assert "changed" in res.text.lower()
            # the generation that appeared mid-delete is still there
            reset_caches()
            current = requests.get(
                _content(settings_server, "default", "delrace")
                + "?include_source=true",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert current.status_code == 200, current.text
            assert current.json()["source"] == "2"

    def test_ordinary_delete_still_works(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server, token, "default", "plain", format="markdown", source="1"
        )
        res = _delete(_content(settings_server, "default", "plain"), token=token)
        assert res.status_code == 204
        assert _get(_content(settings_server, "default", "plain")).status_code == 404

    def test_delete_of_a_missing_document_is_404(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        res = _delete(
            _content(settings_server, "default", "neverexisted"), token=_admin(auth)
        )
        assert res.status_code == 404

    def test_settings_record_delete_still_works(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _patch(_ui(settings_server, "doomed"), {"site_title": "A"}, token=token)
        assert _delete(_ui(settings_server, "doomed"), token=token).status_code == 204
        assert _get(_ui(settings_server, "doomed")).status_code == 404


class TestContentReadIsAdvertisedAsPublic:

    PATH = "/api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}"

    def _operation(self, settings_server: str) -> Dict[str, Any]:
        spec = _get(OPENAPI.format(server=settings_server)).json()
        return dict(spec["paths"][self.PATH]["get"])

    def test_the_anonymous_alternative_is_present(self, settings_server: str) -> None:
        security = self._operation(settings_server).get("security")
        assert security is not None, "the operation declares no security at all"
        assert {} in security, security

    def test_the_bearer_alternative_is_retained(self, settings_server: str) -> None:
        security = self._operation(settings_server)["security"]
        named = [requirement for requirement in security if requirement]
        assert named, security
        # whatever the deployment's scheme is called, it must be a declared one
        schemes = _get(OPENAPI.format(server=settings_server)).json()["components"][
            "securitySchemes"
        ]
        for requirement in named:
            for scheme in requirement:
                assert scheme in schemes, (scheme, list(schemes))

    def test_a_write_on_the_same_path_stays_mandatory(
        self, settings_server: str
    ) -> None:
        spec = _get(OPENAPI.format(server=settings_server)).json()
        patch = spec["paths"][self.PATH]["patch"].get("security")
        assert patch, patch
        assert {} not in patch, patch

    def test_a_plain_get_without_a_token_returns_the_public_shape(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "pubread",
            format="markdown",
            source="# Secret",
        )
        res = _get(_content(settings_server, "default", "pubread"))
        assert res.status_code == 200
        body = res.json()
        assert "rendered_html" in body
        assert "source" not in body

    def test_include_source_without_a_token_is_refused(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "pubread",
            format="markdown",
            source="# Secret",
        )
        url = _content(settings_server, "default", "pubread") + "?include_source=true"
        assert _get(url).status_code in (401, 403)

    def test_include_source_as_a_non_admin_is_403(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        _seed_content(
            settings_server,
            _admin(auth),
            "default",
            "pubread",
            format="markdown",
            source="# Secret",
        )
        url = _content(settings_server, "default", "pubread") + "?include_source=true"
        res = _get(url, headers=_tok(auth, "user"))
        assert res.status_code == 403, res.text

    def test_include_source_as_an_admin_returns_the_admin_shape(
        self, settings_server: str, auth: Dict[str, Any]
    ) -> None:
        token = _admin(auth)
        _seed_content(
            settings_server,
            token,
            "default",
            "pubread",
            format="markdown",
            source="# Secret",
        )
        url = _content(settings_server, "default", "pubread") + "?include_source=true"
        res = _get(url, headers={"Authorization": f"Bearer {token}"})
        assert res.status_code == 200
        assert res.json()["source"] == "# Secret"
