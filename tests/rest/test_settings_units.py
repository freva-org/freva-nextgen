"""
Unit tests for the settings rendering, sanitising and schema layers.
"""

import contextlib
import copy

import pytest
from pydantic import ValidationError

from freva_rest.settings_api import renderers, sanitizer
from freva_rest.settings_api.schema import (
    ContentAdmin,
    ContentPublic,
    ContentSource,
    UiConfig,
    UiConfigUpdate,
)


def _executable(html: str) -> bool:
    """True if the html still carries something that can run in a browser."""
    low = html.lower()
    return any(
        token in low
        for token in (
            "<script",
            "onerror=",
            "onclick=",
            "onload=",
            "javascript:",
            "<iframe",
            "<object",
            "<embed",
        )
    )


class TestSanitizer:
    def test_script_removed(self) -> None:
        assert "<script" not in sanitizer.sanitize_html("<script>x()</script><p>ok</p>")

    def test_event_handler_removed(self) -> None:
        assert "onerror" not in sanitizer.sanitize_html('<img src=x onerror="steal()">')

    def test_dangerous_url_schemes_removed(self) -> None:
        for scheme in (
            "javascript:alert(1)",
            "data:text/html,x",
            "vbscript:x",
            "file:///etc/passwd",
        ):
            out = sanitizer.sanitize_html(f'<a href="{scheme}">x</a>')
            assert scheme.split(":")[0] not in out.lower() or "href" not in out.lower()

    def test_style_content_removed(self) -> None:
        assert "expression" not in sanitizer.sanitize_html(
            "<style>body{x:expression(evil())}</style>hi"
        )

    def test_target_blank_gets_noopener(self) -> None:
        out = sanitizer.sanitize_html('<a href="https://x.de" target="_blank">l</a>')
        assert "noopener" in out

    def test_safe_markup_kept(self) -> None:
        out = sanitizer.sanitize_html("<p>Hello <strong>world</strong></p>")
        assert "<strong>world</strong>" in out

    def test_table_kept(self) -> None:
        out = sanitizer.sanitize_html("<table><tr><td>c</td></tr></table>")
        assert "<td>c</td>" in out


class TestRenderers:
    def test_markdown_renders(self) -> None:
        out = renderers.render("# Hi\n\n**bold**", "markdown")
        assert "<h1>Hi</h1>" in out and "<strong>bold</strong>" in out

    def test_markdown_raw_html_neutralised(self) -> None:
        assert not _executable(
            renderers.render("# Hi\n<script>evil()</script>", "markdown")
        )

    def test_markdown_js_link_neutralised(self) -> None:
        assert not _executable(renderers.render("[x](javascript:alert(1))", "markdown"))

    def test_rst_renders(self) -> None:
        out = renderers.render("Title\n=====\n\nSome **text**.", "rst")
        assert "text" in out.lower()

    def test_rst_raw_directive_blocked(self) -> None:
        out = renderers.render(
            ".. raw:: html\n\n   <script>evil()</script>\n\nText", "rst"
        )
        assert not _executable(out)

    def test_rst_include_directive_blocked(self) -> None:
        out = renderers.render(".. include:: /etc/passwd\n\nText", "rst")
        assert "root:" not in out

    def test_html_sanitised(self) -> None:
        assert not _executable(
            renderers.render(
                '<p onclick="e()">t</p><img src=x onerror="s()">', "html-fragment"
            )
        )

    def test_oversize_source_raises(self) -> None:
        with pytest.raises(ValueError):
            renderers.render("x" * (renderers.MAX_SOURCE_BYTES + 1), "markdown")

    def test_source_hash_is_format_sensitive(self) -> None:
        assert renderers.source_hash("x", "markdown") != renderers.source_hash(
            "x", "rst"
        )

    def test_source_hash_is_stable(self) -> None:
        assert renderers.source_hash("abc", "markdown") == renderers.source_hash(
            "abc", "markdown"
        )

    def test_sandbox_html_not_renderable(self) -> None:
        assert "sandbox-html" not in renderers.RENDERABLE_FORMATS


class TestUiConfigSchema:
    def test_defaults(self) -> None:
        c = UiConfig()
        assert c.site_title == "Freva"
        assert c.features.databrowser.enabled is True

    def test_routes_typed_union(self) -> None:
        c = UiConfig(
            routes=[
                {"kind": "landing", "id": "home"},
                {
                    "kind": "feature",
                    "id": "db",
                    "path": "/databrowser",
                    "feature": "databrowser",
                },
                {
                    "kind": "content",
                    "id": "about",
                    "path": "/about",
                    "ui_id": "default",
                    "content_id": "about",
                },
                {"kind": "external", "id": "dkrz", "url": "https://dkrz.de"},
            ],
            navigation=[
                {"route_id": "home", "label": "Home"},
                {"route_id": "db", "label": "Search"},
            ],
        )
        assert [type(r).__name__ for r in c.routes] == [
            "LandingRoute",
            "FeatureRoute",
            "ContentRoute",
            "ExternalRoute",
        ]
        assert c.routes[0].path == "/"
        assert c.navigation[0].route_id == "home"

    def test_routes_rejects_untyped_entry(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(routes=[{"id": "x", "path": "/x"}])

    def test_feature_route_must_be_allowlisted(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(
                routes=[
                    {
                        "kind": "feature",
                        "id": "x",
                        "path": "/x",
                        "feature": "notafeature",
                    }
                ]
            )

    def test_duplicate_route_id_rejected(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(
                routes=[
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
            )

    def test_at_most_one_landing_route(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(
                routes=[
                    {"kind": "landing", "id": "a"},
                    {"kind": "landing", "id": "b", "path": "/b"},
                ]
            )

    def test_navigation_must_reference_existing_route(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(navigation=[{"route_id": "ghost", "label": "X"}])

    def test_navigation_route_unique(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(
                routes=[{"kind": "landing", "id": "home"}],
                navigation=[
                    {"route_id": "home", "label": "A"},
                    {"route_id": "home", "label": "B"},
                ],
            )

    def test_external_route_url_scheme_checked(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(
                routes=[
                    {
                        "kind": "external",
                        "id": "x",
                        "url": "javascript:alert(1)",
                    }
                ]
            )

    def test_theme_token_name_validated(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(extra_colors={"bad token!": "#ffffff"})
        UiConfig(extra_colors={"accent-2": "#ffffff"})

    def test_route_count_bounded(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(
                routes=[
                    {
                        "kind": "content",
                        "id": f"i{n}",
                        "path": f"/p{n}",
                        "ui_id": "default",
                        "content_id": "a",
                    }
                    for n in range(51)
                ]
            )

    def test_manifest_has_schema_version(self) -> None:
        assert UiConfig().model_dump(by_alias=True)["schemaVersion"] == 1

    def test_colour_must_be_hex(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(main_color="red")

    def test_content_ref_explicit(self) -> None:
        c = UiConfig(header={"content": {"ui_id": "_shared", "content_id": "banner"}})
        assert c.header.content.ui_id == "_shared"

    def test_widget_hints_present(self) -> None:
        props = UiConfig.model_json_schema()["properties"]
        assert props["main_color"].get("x-widget") == "colour"
        assert props["homepage_text"].get("x-widget") == "textarea"


class TestContentSchema:
    def test_public_hides_source(self) -> None:
        assert "source" not in ContentPublic.model_fields

    def test_admin_exposes_source(self) -> None:
        assert "source" in ContentAdmin.model_fields

    def test_content_source_all_optional(self) -> None:
        # a metadata-only patch is valid
        ContentSource(title="just a title")


class TestUrlValidation:
    def test_protocol_relative_rejected(self) -> None:
        from freva_rest.settings_api.field_types import _check_url

        for bad in ("//evil.example", " //evil", "/\\evil", "\\\\evil"):
            with pytest.raises(ValueError):
                _check_url(bad)

    def test_dangerous_schemes_rejected(self) -> None:
        from freva_rest.settings_api.field_types import _check_url

        for bad in (
            "javascript:alert(1)",
            "data:text/html,x",
            "vbscript:x",
            "file:///etc/passwd",
        ):
            with pytest.raises(ValueError):
                _check_url(bad)

    def test_safe_urls_accepted(self) -> None:
        from freva_rest.settings_api.field_types import _check_url

        for ok in ("https://x.de", "http://x.de", "/relative", "#frag", "page"):
            assert _check_url(ok) == ok


class TestTypedStructures:
    def test_features_closed(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(features={"unknown_feature": {"x": 1}})

    def test_landing_block_union(self) -> None:
        c = UiConfig(
            routes=[
                {
                    "kind": "feature",
                    "id": "db",
                    "path": "/db",
                    "feature": "databrowser",
                }
            ],
            landing_blocks=[
                {"block": "hero", "heading": "Hi"},
                {"block": "search", "target_route_id": "db", "flavour": "cmip6"},
            ],
        )
        assert [type(b).__name__ for b in c.landing_blocks] == [
            "HeroBlock",
            "SearchBlock",
        ]

    def test_hero_cta_url_scheme_checked(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(landing_blocks=[{"block": "hero", "cta_url": "javascript:x"}])

    def test_content_stale_default(self) -> None:
        assert "is_stale" in ContentPublic.model_fields


class TestSanitizerProtocolRelative:
    def test_protocol_relative_href_stripped(self) -> None:
        out = sanitizer.sanitize_html('<a href="//evil.example">x</a>')
        assert "//evil" not in out

    def test_protocol_relative_src_stripped(self) -> None:
        out = sanitizer.sanitize_html('<img src="//evil.example/t.gif">')
        assert "//evil" not in out

    def test_backslash_protocol_relative_stripped(self) -> None:
        out = sanitizer.sanitize_html('<a href="/\\evil.example">x</a>')
        assert "evil.example" not in out

    def test_relative_href_survives(self) -> None:
        out = sanitizer.sanitize_html('<a href="/page">x</a>')
        assert 'href="/page"' in out

    def test_https_href_survives(self) -> None:
        out = sanitizer.sanitize_html('<a href="https://ok.de">x</a>')
        assert "https://ok.de" in out


class TestManifestStructures:
    def test_sandbox_route(self) -> None:
        c = UiConfig(
            routes=[
                {
                    "kind": "sandbox",
                    "id": "wx",
                    "path": "/w",
                    "ui_id": "default",
                    "content_id": "w",
                }
            ]
        )
        assert type(c.routes[0]).__name__ == "SandboxRoute"

    def test_landing_links_and_feature_link(self) -> None:
        c = UiConfig(
            routes=[
                {
                    "kind": "feature",
                    "id": "db",
                    "path": "/db",
                    "feature": "databrowser",
                }
            ],
            landing_blocks=[
                {"block": "links", "links": [{"title": "D", "url": "https://d.de"}]},
                {"block": "feature-link", "route_id": "db", "label": "S"},
            ],
        )
        assert [type(b).__name__ for b in c.landing_blocks] == [
            "LinksBlock",
            "FeatureLinkBlock",
        ]

    def test_block_ref_to_unknown_route_rejected(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(landing_blocks=[{"block": "search", "target_route_id": "ghost"}])

    def test_branding_can_be_disabled(self) -> None:
        c = UiConfig(branding_enabled=False)
        assert c.branding_enabled is False

    def test_search_block_inherits_databrowser_defaults(self) -> None:
        c = UiConfig(
            routes=[
                {
                    "kind": "feature",
                    "id": "db",
                    "path": "/db",
                    "feature": "databrowser",
                }
            ],
            landing_blocks=[
                {"block": "search", "placeholder": "Find", "target_route_id": "db"}
            ],
        )
        block = c.landing_blocks[0]
        assert block.placeholder == "Find"
        assert block.flavour is None  # None = inherit features.databrowser
        assert c.features.databrowser.default_flavour == "freva"

    def test_no_duplicate_databrowser_defaults(self) -> None:
        # single source: features.databrowser; no top-level duplicates
        assert "default_flavour" not in UiConfig.model_fields
        assert "fixed_facets" not in UiConfig.model_fields
        assert "landing_search_flavour" not in UiConfig.model_fields

    def test_footer_groups_and_legal_links(self) -> None:
        c = UiConfig(
            footer={
                "groups": [
                    {
                        "title": "About",
                        "links": [{"label": "Team", "url": "https://t.de"}],
                    }
                ],
                "legal_links": [{"label": "Imprint", "url": "https://i.de"}],
            }
        )
        assert c.footer.groups[0].title == "About"
        assert c.footer.legal_links[0].label == "Imprint"


class TestUrlControlCharacters:
    """A browser strips tab/CR/LF before parsing a url's scheme, so a raw-string
    scheme regex sees 'java\\nscript' and lets an executable link through.
    https://url.spec.whatwg.org/#concept-basic-url-parser"""

    @pytest.mark.parametrize(
        "url",
        [
            "java\nscript:alert(1)",
            "java\tscript:alert(1)",
            "jav\ra\nscript:alert(1)",
            "java\x00script:alert(1)",
            "\x01https://ok.de",
        ],
    )
    def test_obfuscated_scheme_rejected(self, url: str) -> None:
        with pytest.raises(ValidationError):
            UiConfig(institution_url=url)

    def test_plain_urls_still_accepted(self) -> None:
        config = UiConfig(institution_url="https://dkrz.de/a-b_c?q=1#f")
        assert config.institution_url == "https://dkrz.de/a-b_c?q=1#f"

    def test_relative_url_still_accepted(self) -> None:
        assert UiConfig(docs_url="/docs/index.html").docs_url == "/docs/index.html"


class TestRoutePathIsInternal:
    """'//evil' is a protocol-relative url a browser resolves as 'https://evil/',
    which would turn an internal route into an off-site link."""

    @pytest.mark.parametrize("path", ["//evil", "//", "//a/b"])
    def test_protocol_relative_path_rejected(self, path: str) -> None:
        with pytest.raises(ValidationError):
            UiConfig(
                routes=[
                    {
                        "kind": "content",
                        "id": "p",
                        "path": path,
                        "ui_id": "default",
                        "content_id": "c",
                    }
                ]
            )

    @pytest.mark.parametrize("path", ["/", "/about", "/a/b/c", "/a-b_c"])
    def test_ordinary_paths_accepted(self, path: str) -> None:
        config = UiConfig(
            routes=[
                {
                    "kind": "content",
                    "id": "p",
                    "path": path,
                    "ui_id": "default",
                    "content_id": "c",
                }
            ]
        )
        assert config.routes[0].path == path


class TestThemeTokenFullmatch:
    """Python's '$' also matches before a trailing newline, so a token that
    becomes a css custom property name needs fullmatch, not match."""

    def test_trailing_newline_token_rejected(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(extra_colors={"brand\n": "#111111"})

    def test_ordinary_token_accepted(self) -> None:
        assert UiConfig(extra_colors={"brand-2": "#111111"}).extra_colors["brand-2"]


class TestSharedUiIdIsReserved:
    """The underscore prefix is reserved for '_shared' alone, not a general
    namespace."""

    def test_shared_accepted(self) -> None:
        config = UiConfig(content_refs=[{"ui_id": "_shared", "content_id": "policy"}])
        assert config.content_refs[0].ui_id == "_shared"

    @pytest.mark.parametrize("ui_id", ["_staging", "_x", "_"])
    def test_other_underscore_ids_rejected(self, ui_id: str) -> None:
        with pytest.raises(ValidationError):
            UiConfig(content_refs=[{"ui_id": ui_id, "content_id": "policy"}])


class TestRstKeepsDocumentTitle:
    """docutils' doctitle_xform promotes a lone top-level title out of the body,
    so it stays off."""

    def test_title_survives(self) -> None:
        html = renderers.render("Page Title\n==========\n\ntext\n", "rst")
        assert "Page Title" in html
        assert "<h1>" in html

    def test_nested_headings_are_ordered(self) -> None:
        html = renderers.render(
            "Top\n===\n\na\n\nSub\n---\n\nb\n",
            "rst",
        )
        assert html.index("<h1>") < html.index("<h2>")

    def test_section_ids_are_not_emitted(self) -> None:
        # author-controlled ids in the main DOM invite element clobbering
        html = renderers.render("Page Title\n==========\n\ntext\n", "rst")
        assert "id=" not in html

    def test_rst_lockdown_still_holds(self) -> None:
        assert renderers.render(".. raw:: html\n\n   <script>x</script>\n", "rst") == ""
        assert renderers.render(".. include:: /etc/passwd\n", "rst") == ""


class TestNestedPatchMerge:
    """Patching one sub-field of a nested object leaves every sibling as stored,
    rather than replacing the object wholesale."""

    @staticmethod
    def _store():
        from freva_rest.settings_api.core import SettingsStore
        from freva_rest.settings_api.registry import REGISTRY

        entry = REGISTRY["ui"]
        return SettingsStore(
            None, "ui", "default", entry.model, entry.update_model, entry.open_maps
        )

    @staticmethod
    def _stored():
        return {
            "features": {
                "databrowser": {
                    "enabled": True,
                    "default_flavour": "cmip6",
                    "fixed_facets": {"project": ["a"]},
                },
                "stac": {"enabled": True},
            },
            "extra_colors": {"brand": "#111111", "accent": "#222222"},
            "header": {"show": True, "links": [{"label": "L", "url": "https://x.de"}]},
        }

    def _merge(self, body):
        from freva_rest.settings_api.schema import UiConfigUpdate

        merged = self._store()._merge(
            self._stored(), UiConfigUpdate.model_validate(body)
        )
        UiConfig(**merged)  # the candidate must still be a valid manifest
        return merged

    def test_patching_stac_keeps_databrowser(self) -> None:
        features = self._merge({"features": {"stac": {"enabled": False}}})["features"]
        assert features["stac"]["enabled"] is False
        assert features["databrowser"]["default_flavour"] == "cmip6"
        assert features["databrowser"]["fixed_facets"] == {"project": ["a"]}

    def test_patching_databrowser_keeps_stac(self) -> None:
        features = self._merge(
            {"features": {"databrowser": {"default_flavour": "cmip5"}}}
        )["features"]
        assert features["stac"]["enabled"] is True
        assert features["databrowser"]["fixed_facets"] == {"project": ["a"]}

    def test_nested_open_map_merges_key_by_key(self) -> None:
        facets = self._merge(
            {"features": {"databrowser": {"fixed_facets": {"experiment": ["b"]}}}}
        )["features"]["databrowser"]["fixed_facets"]
        assert facets == {"project": ["a"], "experiment": ["b"]}

    def test_nested_open_map_null_deletes_one_key(self) -> None:
        facets = self._merge(
            {"features": {"databrowser": {"fixed_facets": {"project": None}}}}
        )["features"]["databrowser"]["fixed_facets"]
        assert facets == {}

    def test_nested_open_map_empty_clears(self) -> None:
        merged = self._merge({"features": {"databrowser": {"fixed_facets": {}}}})
        assert merged["features"]["databrowser"]["fixed_facets"] == {}
        assert merged["features"]["databrowser"]["default_flavour"] == "cmip6"

    def test_null_resets_whole_nested_object(self) -> None:
        assert "features" not in self._merge({"features": None})

    def test_empty_nested_object_is_a_no_op(self) -> None:
        assert self._merge({"features": {}})["features"] == self._stored()["features"]

    def test_partial_header_keeps_links(self) -> None:
        header = self._merge({"header": {"show": False}})["header"]
        assert header["show"] is False
        assert header["links"] == [{"label": "L", "url": "https://x.de"}]

    def test_top_level_open_maps_unchanged(self) -> None:
        assert self._merge({"extra_colors": {"accent": "#333333"}})["extra_colors"] == {
            "brand": "#111111",
            "accent": "#333333",
        }


class TestIfMatchAsterisk:
    """RFC 9110 13.1.1: 'If-Match: *' means 'if the resource currently exists'."""

    def test_asterisk_fails_when_absent(self) -> None:
        from freva_rest.settings_api.core import etag_of, strong_if_match

        assert strong_if_match("*", etag_of(b"{}"), exists=False) is False

    def test_asterisk_passes_when_present(self) -> None:
        from freva_rest.settings_api.core import etag_of, strong_if_match

        assert strong_if_match("*", etag_of(b"{}"), exists=True) is True


class TestContentReadDegrades:
    """A malformed stored document must degrade, not 500 - a 500 would also take
    out the admin view needed to repair it."""

    @staticmethod
    def _store(doc):
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches

        class _Collection:
            async def find_one(self, key, projection=None):
                return doc

        class _Config:
            mongo_collection_ui_contents = _Collection()

        reset_caches()
        store = ContentStore(
            _Config(), doc.get("ui_id", "a"), doc.get("content_id", "b")
        )
        return (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(store.get_public())
        )

    @pytest.mark.parametrize(
        "doc",
        [
            {"_id": "a:b", "ui_id": "a", "content_id": "b", "title": "x"},
            {"_id": "a:b", "ui_id": "a", "content_id": "b", "format": "bogus"},
            {"_id": "a:b", "content_id": "b", "format": "markdown"},
            {
                "_id": "a:b",
                "ui_id": "a",
                "content_id": "b",
                "format": "markdown",
                "revision": "oops",
            },
        ],
    )
    def test_malformed_document_does_not_raise(self, doc) -> None:
        body, etag = self._store(doc)
        assert body and etag.startswith('"')

    def test_unusable_format_is_reported_stale_and_empty(self) -> None:
        import json

        body, _ = self._store({"_id": "a:b", "ui_id": "a", "content_id": "b"})
        payload = json.loads(body)
        assert payload["is_stale"] is True
        assert payload["rendered_html"] == ""


class TestCacheIsByteBounded:
    """TTLCache counts entries unless it is given getsizeof, and a single content
    document can carry three quarters of a megabyte."""

    def test_caches_measure_bytes(self) -> None:
        from freva_rest.settings_api import core

        for cache in (core._read_cache, core._lkg_cache, core._body_cache):
            assert cache.maxsize == core._CACHE_BYTES

    def test_a_big_document_costs_its_size(self) -> None:
        from freva_rest.settings_api import core

        small = core._sizeof_doc({"rendered_html": "x" * 10})
        big = core._sizeof_doc({"rendered_html": "x" * 500_000})
        assert big - small >= 499_000

    def test_oversized_entry_is_skipped_not_fatal(self) -> None:
        from freva_rest.settings_api import core

        core.reset_caches()
        core._cache_put("k", {"rendered_html": "x" * (core._CACHE_BYTES + 1)})
        assert core._cache_get("k") is None  # skipped, and no exception

    def test_read_ttl_is_short_enough_to_bound_worker_disagreement(self) -> None:
        from freva_rest.settings_api import core

        assert core._READ_TTL <= 5


class TestAdminClaimFallback:
    """The flat 'roles' claim must not be a fallback for a claim path the
    operator nominated."""

    @staticmethod
    def _config(claims):
        from freva_rest.config import ServerConfig

        return ServerConfig.model_construct(admin_token_claims=claims)

    @staticmethod
    def _token(**claims):
        from pydantic import BaseModel, ConfigDict

        class _Token(BaseModel):
            model_config = ConfigDict(extra="allow")

        return _Token.model_validate(claims)

    def test_roles_does_not_satisfy_a_groups_filter(self) -> None:
        from freva_rest.config import ServerConfig

        config = self._config({"groups": ["^settings-admin$"]})
        token = self._token(sub="u", roles=["settings-admin"])
        assert ServerConfig.is_admin_user(config, token) is False

    def test_matching_groups_still_authorises(self) -> None:
        from freva_rest.config import ServerConfig

        config = self._config({"groups": ["^settings-admin$"]})
        token = self._token(sub="u", groups=["settings-admin"])
        assert ServerConfig.is_admin_user(config, token) is True

    def test_legacy_list_form_still_uses_flat_roles(self) -> None:
        from freva_rest.config import ServerConfig

        config = self._config(["^admin$"])
        token = self._token(sub="u", roles=["admin"])
        assert ServerConfig.is_admin_user(config, token) is True

    def test_explicit_roles_path_still_uses_flat_roles(self) -> None:
        from freva_rest.config import ServerConfig

        config = self._config({"roles": ["^admin$"]})
        token = self._token(sub="u", roles=["admin"])
        assert ServerConfig.is_admin_user(config, token) is True

    def test_nested_path_still_works(self) -> None:
        from freva_rest.config import ServerConfig

        config = self._config({"resource_access.rm.roles": ["^admin$"]})
        token = self._token(sub="u", resource_access={"rm": {"roles": ["admin"]}})
        assert ServerConfig.is_admin_user(config, token) is True


class TestAdminClaimMatchesWholeValues:
    """
    An admin rule matches a role, not a substring of one.
    """

    _CONFIG = {"roles": ["admin"]}

    @staticmethod
    def _decides(patterns, value):
        from pydantic import BaseModel, ConfigDict

        from freva_rest.config import ServerConfig

        class _Token(BaseModel):
            model_config = ConfigDict(extra="allow")

        config = ServerConfig.model_construct(admin_token_claims={"roles": patterns})
        token = _Token.model_validate({"sub": "u", "roles": [value]})
        return ServerConfig.is_admin_user(config, token)

    def test_the_exact_role_still_grants_admin(self) -> None:
        assert self._decides(["admin"], "admin") is True

    @pytest.mark.parametrize(
        "role",
        [
            "non-admin",
            "grafana-admin",
            "admin-readonly",
            "notadminx",
            "admin\n",
            "Admin",
            " admin",
            "admin ",
        ],
    )
    def test_a_role_that_merely_contains_it_does_not(self, role: str) -> None:
        assert self._decides(["admin"], role) is False, role

    def test_a_trailing_newline_is_not_an_admin(self) -> None:
        assert self._decides(["^admin$"], "admin\n") is False

    def test_anchored_patterns_are_unchanged(self) -> None:
        assert self._decides(["^admin$"], "admin") is True
        assert self._decides(["^admin$"], "non-admin") is False

    @pytest.mark.parametrize(
        "pattern,role,expected",
        [
            (".*admin.*", "grafana-admin", True),
            (".*admin.*", "admin", True),
            ("admin.*", "admin-readonly", True),
            ("admin.*", "non-admin", False),
            ("(admin|superuser)", "superuser", True),
        ],
    )
    def test_substring_behaviour_is_available_when_asked_for(
        self, pattern: str, role: str, expected: bool
    ) -> None:
        assert self._decides([pattern], role) is expected

    def test_one_matching_pattern_out_of_several_is_enough(self) -> None:
        assert self._decides(["^superuser$", "^admin$"], "admin") is True


class TestHeaderFooterPatchShapes:
    """header/footer patches accept null-to-reset and partial content
    references, merging them rather than rejecting them."""

    @staticmethod
    def _merge(stored, body):
        from freva_rest.settings_api.core import SettingsStore
        from freva_rest.settings_api.registry import REGISTRY
        from freva_rest.settings_api.schema import UiConfigUpdate

        entry = REGISTRY["ui"]
        store = SettingsStore(
            None, "ui", "default", entry.model, entry.update_model, entry.open_maps
        )
        merged = store._merge(stored, UiConfigUpdate.model_validate(body))
        UiConfig(**merged)
        return merged

    def test_show_null_restores_the_default(self) -> None:
        merged = self._merge({"header": {"show": False}}, {"header": {"show": None}})
        assert "show" not in merged["header"]
        assert UiConfig(**merged).header.show is True

    def test_partial_content_ref_keeps_ui_id(self) -> None:
        merged = self._merge(
            {"header": {"content": {"ui_id": "_shared", "content_id": "old"}}},
            {"header": {"content": {"content_id": "new"}}},
        )
        assert merged["header"]["content"] == {"ui_id": "_shared", "content_id": "new"}

    def test_footer_partial_patch_keeps_groups(self) -> None:
        stored = {
            "footer": {
                "show": True,
                "groups": [{"title": "About", "links": []}],
            }
        }
        merged = self._merge(stored, {"footer": {"show": False}})
        assert merged["footer"]["groups"] == [{"title": "About", "links": []}]

    def test_half_formed_reference_still_refused(self) -> None:
        # nothing stored to merge with, so the pair stays incomplete
        with pytest.raises(ValidationError):
            self._merge({}, {"header": {"content": {"content_id": "new"}}})


class TestRecursiveByteSizing:
    """The sizer walks nested containers and counts utf-8 bytes, not characters,
    so the byte budget bounds the documents worth bounding."""

    def test_nested_list_is_charged_realistically(self) -> None:
        from freva_rest.settings_api.core import _sizeof_doc, serialise

        routes = [
            {
                "kind": "content",
                "id": f"r{i}",
                "path": f"/p{i}",
                "ui_id": "default",
                "content_id": "c",
            }
            for i in range(200)
        ]
        doc = {"routes": routes}
        assert _sizeof_doc(doc) >= len(serialise(doc))

    def test_non_ascii_counted_as_utf8_bytes(self) -> None:
        from freva_rest.settings_api.core import _sizeof_doc

        doc = {"rendered_html": "é" * 10_000}
        assert _sizeof_doc(doc) >= len(doc["rendered_html"].encode("utf-8"))

    def test_deeply_nested_dict_is_walked(self) -> None:
        from freva_rest.settings_api.core import _sizeof_doc

        doc = {"a": {"b": {"c": {"d": "x" * 50_000}}}}
        assert _sizeof_doc(doc) >= 50_000


class TestNonFiniteExtensions:
    """inf/nan would serialise as the bare tokens Infinity/NaN, which
    JSON.parse rejects - one admin edit could break every client."""

    @pytest.mark.parametrize("value", [1e999, float("inf"), float("-inf")])
    def test_infinities_rejected(self, value: float) -> None:
        with pytest.raises(ValidationError):
            UiConfig(public_extensions={"a": value})

    def test_nan_rejected(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(public_extensions={"a": float("nan")})

    def test_json_literal_1e999_is_rejected(self) -> None:
        import json

        payload = json.loads('{"public_extensions": {"a": 1e999}}')
        assert payload["public_extensions"]["a"] == float("inf")
        with pytest.raises(ValidationError):
            UiConfig(**payload)

    def test_ordinary_floats_accepted(self) -> None:
        assert UiConfig(public_extensions={"a": 1.5}).public_extensions["a"] == 1.5

    def test_serialise_refuses_non_finite(self) -> None:
        from freva_rest.settings_api.core import serialise

        with pytest.raises(ValueError):
            serialise({"a": float("inf")})

    def test_safe_body_degrades_instead_of_emitting_bad_json(self) -> None:
        import json

        from freva_rest.settings_api.core import SettingsStore
        from freva_rest.settings_api.registry import REGISTRY

        entry = REGISTRY["ui"]
        store = SettingsStore(
            None, "ui", "default", entry.model, entry.update_model, entry.open_maps
        )
        body = store._safe_body({"public_extensions": {"a": float("inf")}})
        assert json.loads(body)


class TestStaleVersionPreserved:
    """A metadata-only patch must not stamp legacy html as current."""

    def test_missing_version_stays_missing_on_reuse(self) -> None:
        from freva_rest.settings_api.core import _as_str

        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        assert _as_str({}.get("renderer_version")) != RENDERER_FINGERPRINT

    def test_document_without_version_reads_as_stale(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import ContentStore, reset_caches

        doc = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "rendered_html": "<p>legacy</p>",
        }

        class _Collection:
            async def find_one(self, key, projection=None):
                return doc

        class _Config:
            mongo_collection_ui_contents = _Collection()

        reset_caches()
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(ContentStore(_Config(), "a", "b").get_public())
        )
        assert json.loads(body)["is_stale"] is True


class TestSandboxDocumentDefensive:
    """get_document must not 500 on a document with no format."""

    @staticmethod
    def _get(doc):
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches

        class _Collection:
            async def find_one(self, key, projection=None):
                return doc

        class _Config:
            mongo_collection_ui_contents = _Collection()

        reset_caches()
        return (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(ContentStore(_Config(), "a", "b").get_document())
        )

    def test_missing_format_is_404_not_500(self) -> None:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as caught:
            self._get({"_id": "a:b", "ui_id": "a", "content_id": "b"})
        assert caught.value.status_code == 404

    def test_sandbox_document_still_served(self) -> None:
        doc = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "sandbox-html",
            "source": "<html></html>",
        }
        assert self._get(doc) == "<html></html>"


class TestMalformedCombinations:
    """An invalid format combined with an invalid revision still normalises."""

    def test_bad_format_and_bad_revision(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import ContentStore, reset_caches

        doc = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "bogus",
            "revision": "oops",
        }

        class _Collection:
            async def find_one(self, key, projection=None):
                return doc

        class _Config:
            mongo_collection_ui_contents = _Collection()

        reset_caches()
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(ContentStore(_Config(), "a", "b").get_public())
        )
        assert json.loads(body)["revision"] == 0


class TestIfMatchAgainstSynthesisedDefault:
    """The default record's GET returns 200 before anything is stored, so under
    HTTP semantics the resource exists and If-Match: * must succeed."""

    @staticmethod
    def _store(record_id):
        from freva_rest.settings_api.core import SettingsStore
        from freva_rest.settings_api.registry import REGISTRY

        entry = REGISTRY["ui"]
        return SettingsStore(
            None, "ui", record_id, entry.model, entry.update_model, entry.open_maps
        )

    def test_default_record_synthesises(self) -> None:
        assert self._store("default").synthesises_default is True

    def test_named_record_does_not(self) -> None:
        assert self._store("waterpark").synthesises_default is False


class TestModelExtraIsNotANamespace:
    """model_dump() flattens pydantic extras, so claims['model_extra'] is a
    user-controlled claim, not the extras container."""

    @staticmethod
    def _token(**claims):
        from pydantic import BaseModel, ConfigDict

        class _Token(BaseModel):
            model_config = ConfigDict(extra="allow")

        return _Token.model_validate(claims)

    def test_claim_named_model_extra_does_not_grant_admin(self) -> None:
        from freva_rest.config import ServerConfig

        config = ServerConfig.model_construct(admin_token_claims={"roles": ["^admin$"]})
        token = self._token(sub="u", model_extra={"roles": ["admin"]})
        assert ServerConfig.is_admin_user(config, token) is False

    def test_claim_named_model_extra_does_not_grant_via_legacy_list(self) -> None:
        from freva_rest.config import ServerConfig

        config = ServerConfig.model_construct(admin_token_claims=["^admin$"])
        token = self._token(sub="u", model_extra={"roles": ["admin"]})
        assert ServerConfig.is_admin_user(config, token) is False

    def test_a_real_roles_claim_still_grants_admin(self) -> None:
        from freva_rest.config import ServerConfig

        config = ServerConfig.model_construct(admin_token_claims={"roles": ["^admin$"]})
        assert (
            ServerConfig.is_admin_user(config, self._token(sub="u", roles=["admin"]))
            is True
        )

    def test_scalar_roles_claim_does_not_crash(self) -> None:
        from freva_rest.config import ServerConfig

        config = ServerConfig.model_construct(admin_token_claims=["^admin$"])
        assert (
            ServerConfig.is_admin_user(config, self._token(sub="u", roles="admin"))
            is True
        )


class TestFormatClassification:
    """'not sandbox-html' is not the same as 'renderable'"""

    @staticmethod
    def _mismatch(fmt, expect):
        from freva_rest.settings_api.core import format_mismatch

        return format_mismatch("default", "page", fmt, expect)

    @pytest.mark.parametrize("fmt", ["bogus", None, "", 7])
    def test_unusable_format_fails_a_rendered_reference(self, fmt) -> None:
        assert self._mismatch(fmt, "rendered")

    @pytest.mark.parametrize("fmt", ["bogus", None, ""])
    def test_unusable_format_fails_a_sandbox_reference(self, fmt) -> None:
        assert self._mismatch(fmt, "sandbox")

    @pytest.mark.parametrize("fmt", ["markdown", "rst", "html-fragment"])
    def test_renderable_formats_satisfy_a_rendered_reference(self, fmt) -> None:
        assert self._mismatch(fmt, "rendered") == []

    def test_sandbox_satisfies_a_sandbox_reference(self) -> None:
        assert self._mismatch("sandbox-html", "sandbox") == []

    def test_sandbox_fails_a_rendered_reference(self) -> None:
        assert "cannot be inlined" in self._mismatch("sandbox-html", "rendered")[0]

    def test_existence_only_reference_ignores_format(self) -> None:
        assert self._mismatch("bogus", None) == []


class TestMalformedRevisionIsRepairable:
    """A record with revision='oops' must still be matchable by the CAS filter,
    or no write can ever repair it."""

    @staticmethod
    def _predicate(stored):
        from freva_rest.settings_api.core import _cas_predicate

        return _cas_predicate({"_id": "ui:default"}, stored)

    def test_malformed_revision_is_matched_literally(self) -> None:
        predicate = self._predicate("oops")
        assert _FakeCollection._matches(
            {"_id": "ui:default", "revision": "oops"}, predicate
        )
        assert not _FakeCollection._matches(
            {"_id": "ui:default", "revision": 1}, predicate
        )
        assert not _FakeCollection._matches({"_id": "ui:default"}, predicate)

    def test_absent_revision_uses_the_exists_clause(self) -> None:
        from freva_rest.settings_api.core import MISSING

        predicate = self._predicate(MISSING)
        assert _FakeCollection._matches({"_id": "ui:default"}, predicate)
        assert not _FakeCollection._matches(
            {"_id": "ui:default", "revision": None}, predicate
        )

    def test_explicit_zero_is_not_the_same_as_absent(self) -> None:
        from freva_rest.settings_api.core import MISSING

        assert _FakeCollection._matches(
            {"_id": "ui:default", "revision": 0}, self._predicate(0)
        )
        assert not _FakeCollection._matches({"_id": "ui:default"}, self._predicate(0))
        assert _FakeCollection._matches({"_id": "ui:default"}, self._predicate(MISSING))

    def test_normal_revision_matched_exactly(self) -> None:
        predicate = self._predicate(7)
        assert _FakeCollection._matches({"_id": "ui:default", "revision": 7}, predicate)
        assert not _FakeCollection._matches(
            {"_id": "ui:default", "revision": 8}, predicate
        )

    def test_replacement_revision_is_always_normalised(self) -> None:
        from freva_rest.settings_api.core import _as_int

        assert _as_int("oops") + 1 == 1


class TestIntegerCacheSizing:
    """public_extensions accepts arbitrary-precision ints, so a flat per-scalar
    charge would be bypassable."""

    def test_huge_int_is_charged_its_decimal_width(self) -> None:
        from freva_rest.settings_api.core import _sizeof_doc, serialise

        doc = {"public_extensions": {"a": 10**4000}}
        assert _sizeof_doc(doc) >= len(serialise(doc))

    def test_bool_is_not_charged_as_an_int(self) -> None:
        from freva_rest.settings_api.core import _sizeof_doc

        assert _sizeof_doc({"a": True}) == _sizeof_doc({"a": None})

    def test_ordinary_int_is_cheap(self) -> None:
        from freva_rest.settings_api.core import _sizeof_doc

        assert _sizeof_doc({"a": 42}) < 200


class TestRebuildIsolatesFailures:
    """One damaged record must fail alone, not abort the whole rebuild."""

    def test_unrenderable_record_fails_alone(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import rebuild_stale_content

        docs = [
            {
                "_id": "a:bad",
                "ui_id": "a",
                "content_id": "bad",
                "format": "markdown",
                "source": "x" * (256 * 1024 + 1),
                "renderer_version": "0",
            },
            {
                "_id": "a:good",
                "ui_id": "a",
                "content_id": "good",
                "format": "markdown",
                "source": "# ok",
                "renderer_version": "0",
                "revision": 1,
            },
        ]

        collection = _FakeCollection()
        for entry in docs:
            collection.docs[entry["_id"]] = entry
        _Config = _FakeConfig(collection)

        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_Config))
        )
        assert report["examined"] == 2
        assert report["rebuilt"] == 1
        assert report["failed"] == 1


def _bson_type_name(value):
    """The subset of mongo's `$type` aliases these tests need."""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    if value is None:
        return "null"
    return type(value).__name__


def _same_bson_type(left, right) -> bool:
    return _bson_type_name(left) == _bson_type_name(right)


class _FakeCollection:
    """An in-memory stand-in for the mongo collection, faithful on the two
    behaviours the CAS logic depends on"""

    def __init__(self) -> None:
        self.docs: dict = {}
        self.on_read = None

    @staticmethod
    def _matches(doc, predicate) -> bool:
        """
        Mongo's matching rules for the operators this code actually uses.
        """
        for key, want in predicate.items():
            if key == "$or":
                if not any(_FakeCollection._matches(doc, c) for c in want):
                    return False
                continue
            if key == "$and":
                if not all(_FakeCollection._matches(doc, c) for c in want):
                    return False
                continue
            if key == "$expr":
                if not _FakeCollection._expr(doc, want):
                    return False
                continue
            if isinstance(want, dict) and any(k.startswith("$") for k in want):
                if "$exists" in want and (key in doc) != want["$exists"]:
                    return False
                if "$eq" in want:
                    if key not in doc:
                        return False
                    stored = doc[key]
                    target = want["$eq"]
                    if stored != target and not (
                        isinstance(stored, list) and target in stored
                    ):
                        return False
                continue
            if want is None:
                if doc.get(key) is not None:
                    return False
                continue
            if key not in doc or doc[key] != want:
                return False
        return True

    MISSING_OPERAND = object()

    @classmethod
    def _bson_type(cls, value) -> str:
        """
        The name `$type` would return for this value.
        """
        if value is cls.MISSING_OPERAND:
            return "missing"
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int" if -(2**31) <= value < 2**31 else "long"
        if isinstance(value, float):
            return "double"
        if isinstance(value, str):
            return "string"
        if isinstance(value, list):
            return "array"
        if isinstance(value, dict):
            return "object"
        return "unknown"

    @classmethod
    def _mongo_eq(cls, left, right) -> bool:
        """
        Aggregation `$eq` as mongo actually defines it.
        """
        if left is cls.MISSING_OPERAND or right is cls.MISSING_OPERAND:
            return False
        if isinstance(left, bool) != isinstance(right, bool):
            return False
        if isinstance(left, bool):
            return left is right
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return left == right  # numeric types are mutually comparable
        if type(left) is not type(right):
            return False
        return bool(left == right)

    @classmethod
    def _expr(cls, doc, expression) -> bool:
        """
        Aggregation-expression evaluation for the subset used here.
        """

        def _resolve(operand):
            if isinstance(operand, dict) and "$literal" in operand:
                return operand["$literal"]
            if isinstance(operand, dict) and "$type" in operand:
                return cls._bson_type(_resolve(operand["$type"]))
            if isinstance(operand, str) and operand.startswith("$"):
                return doc.get(operand[1:], cls.MISSING_OPERAND)
            return operand

        if "$and" in expression:
            return all(cls._expr(doc, part) for part in expression["$and"])
        assert set(expression) == {"$eq"}, expression
        left, right = (_resolve(o) for o in expression["$eq"])
        return cls._mongo_eq(left, right)

    async def find_one(self, key, projection=None):
        doc = self.docs.get(key["_id"])
        if self.on_read is not None:
            self.on_read()
        return dict(doc) if doc else None

    async def insert_one(self, doc):
        from pymongo.errors import DuplicateKeyError

        if doc["_id"] in self.docs:
            raise DuplicateKeyError("duplicate _id")
        self.docs[doc["_id"]] = dict(doc)

    async def replace_one(self, predicate, doc):
        existing = self.docs.get(predicate["_id"])

        class _Result:
            matched_count = 0

        if existing is not None and self._matches(existing, predicate):
            self.docs[predicate["_id"]] = dict(doc)
            _Result.matched_count = 1
        return _Result()

    async def delete_one(self, predicate):
        existing = self.docs.get(predicate["_id"])

        class _Result:
            deleted_count = 0

        if existing is not None and self._matches(existing, predicate):
            del self.docs[predicate["_id"]]
            _Result.deleted_count = 1
        return _Result()

    async def update_one(self, predicate, update):
        existing = self.docs.get(predicate["_id"])

        class _Result:
            matched_count = 0

        if existing is not None and self._matches(existing, predicate):
            existing.update(update.get("$set", {}))
            _Result.matched_count = 1
        return _Result()

    async def count_documents(self, selector=None, *a, **k):
        return len(self.find(selector).__dict__["_rows"])

    def find(self, selector=None, projection=None, *a, **k):
        """
        A cursor faithful on what the scans depend on: `$type`, `$not`,
        `$regex`, `$in`, a **type-bracketed** `$gt`, and `.sort()`.
        """
        docs = [dict(d) for d in self.docs.values()]
        if isinstance(selector, dict):
            for clause in selector.get("$and", []):
                kept = self.find(clause).__dict__["_rows"]
                keys = [c.get("_id") for c in kept]
                docs = [d for d in docs if d.get("_id") in keys]
            bound = selector.get("_id") or {}
            if isinstance(bound, dict) and "$type" in bound:
                docs = [
                    d for d in docs if _bson_type_name(d.get("_id")) == bound["$type"]
                ]
            if isinstance(bound, dict) and "$not" in bound:
                excluded = self.find({"_id": bound["$not"]}).__dict__["_rows"]
                skip = [e.get("_id") for e in excluded]
                docs = [d for d in docs if d.get("_id") not in skip]
            if isinstance(bound, dict) and "$gt" in bound:
                docs = [
                    d
                    for d in docs
                    if _same_bson_type(d.get("_id"), bound["$gt"])
                    and d.get("_id") > bound["$gt"]
                ]
            if isinstance(bound, dict) and "$in" in bound:
                docs = [d for d in docs if d.get("_id") in bound["$in"]]
            if isinstance(bound, dict) and "$regex" in bound:
                import re as _re

                docs = [
                    d
                    for d in docs
                    if isinstance(d.get("_id"), str)
                    and _re.search(bound["$regex"], d["_id"])
                ]

        class _Cursor:
            def __init__(self, rows):
                self._rows = rows

            def sort(self, key, direction=1):
                # within a type, as mongo does
                return _Cursor(
                    sorted(
                        self._rows,
                        key=lambda d: (_bson_type_name(d.get(key)), str(d.get(key))),
                        reverse=direction < 0,
                    )
                )

            def __aiter__(self):
                rows = list(self._rows)

                async def gen():
                    for doc in rows:
                        yield doc

                return gen()

        return _Cursor(docs)


class _FakeConfig:
    def __init__(self, collection) -> None:
        self._collection = collection

    @property
    def mongo_collection_ui_contents(self):
        return self._collection


class TestFormatClassIsCasProtected:
    """The class check has to run against the document the write itself will
    match, or two concurrent creates defeat it with no force flag at all."""

    @staticmethod
    def _store(collection):
        from freva_rest.settings_api.core import ContentStore, reset_caches

        reset_caches()
        return ContentStore(_FakeConfig(collection), "default", "p")

    def test_interleaved_creates_cannot_cross_the_boundary(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        state = {"fired": False}

        def _interleave():
            if state["fired"]:
                return
            state["fired"] = True
            collection.docs["default:p"] = {
                "_id": "default:p",
                "ui_id": "default",
                "content_id": "p",
                "format": "markdown",
                "source": "# a",
                "rendered_html": "<h1>a</h1>",
                "renderer_version": "0",
                "revision": 1,
            }

        collection.on_read = _interleave
        with pytest.raises(HTTPException) as caught:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                store.patch(
                    ContentSource.model_validate(
                        {"format": "sandbox-html", "source": "<html></html>"}
                    )
                )
            )
        assert caught.value.status_code == 409
        assert collection.docs["default:p"]["format"] == "markdown"

    def test_force_still_crosses_the_boundary(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["default:p"] = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "revision": 1,
        }
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.patch(
                ContentSource.model_validate(
                    {"format": "sandbox-html", "source": "<html></html>"}
                ),
                force=True,
            )
        )
        assert collection.docs["default:p"]["format"] == "sandbox-html"

    def test_a_plain_create_is_unaffected(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.patch(
                ContentSource.model_validate(
                    {"format": "sandbox-html", "source": "<html></html>"}
                )
            )
        )
        assert collection.docs["default:p"]["format"] == "sandbox-html"


class TestStoredNullRevision:
    """An absent field and a field stored as null need different filters."""

    @staticmethod
    def _predicate(stored):
        from freva_rest.settings_api.core import _cas_predicate

        return _cas_predicate({"_id": "x"}, stored)

    def test_missing_sentinel_uses_the_exists_clause(self) -> None:
        from freva_rest.settings_api.core import MISSING

        assert _FakeCollection._matches({"_id": "x"}, self._predicate(MISSING))

    def test_stored_null_is_matched_literally(self) -> None:
        predicate = self._predicate(None)
        assert _FakeCollection._matches({"_id": "x", "revision": None}, predicate)
        # neither an absent revision nor a legacy document with a token matches
        assert not _FakeCollection._matches({"_id": "x"}, predicate)
        assert not _FakeCollection._matches(
            {"_id": "x", "revision": None, "cas_token": "t"}, predicate
        )

    def test_a_null_revision_document_can_be_written(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["default:p"] = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "source": "# a",
            "revision": None,
        }
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.patch(ContentSource.model_validate({"title": "New"}))
        )
        assert collection.docs["default:p"]["revision"] == 1
        assert collection.docs["default:p"]["title"] == "New"


class TestUnhashableFormats:
    """
    `[] in frozenset(...)` raises TypeError, in the branches whose whole job
    is to survive a malformed stored document.
    """

    @pytest.mark.parametrize("fmt", [[], {}, set()])
    def test_format_mismatch_survives(self, fmt) -> None:
        from freva_rest.settings_api.core import format_mismatch

        assert format_mismatch("d", "p", fmt, "rendered")

    @pytest.mark.parametrize("fmt", [[], {}])
    def test_public_read_survives(self, fmt) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import ContentStore, reset_caches

        collection = _FakeCollection()
        collection.docs["default:p"] = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": fmt,
        }
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(store.get_public())
        )
        assert json.loads(body)["is_stale"] is True

    @pytest.mark.parametrize("fmt", [[], {}])
    def test_rebuild_survives(self, fmt) -> None:
        import asyncio

        from freva_rest.settings_api.core import rebuild_stale_content

        collection = _FakeCollection()
        collection.docs["a:bad"] = {
            "_id": "a:bad",
            "ui_id": "a",
            "content_id": "bad",
            "format": fmt,
            "source": "x",
        }
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["failed"] == 1


class TestRebuildDoesNotRewriteContent:
    """A non-string source must be counted as failed and left untouched, not
    coerced through str() and stamped renderer-current."""

    @pytest.mark.parametrize("source", [{"a": 1}, [1, 2], 12345, None])
    def test_non_string_source_is_left_alone(self, source) -> None:
        import asyncio

        from freva_rest.settings_api.core import rebuild_stale_content

        collection = _FakeCollection()
        collection.docs["a:bad"] = {
            "_id": "a:bad",
            "ui_id": "a",
            "content_id": "bad",
            "format": "markdown",
            "source": source,
            "source_hash": "old",
            "renderer_version": "0",
            "revision": 1,
        }
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["failed"] == 1
        assert report["rebuilt"] == 0
        stored = collection.docs["a:bad"]
        assert stored["renderer_version"] == "0"  # untouched
        assert stored["source"] == source
        assert stored["source_hash"] == "old"

    def test_a_good_record_alongside_it_is_still_rebuilt(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import rebuild_stale_content
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        collection = _FakeCollection()
        collection.docs["a:bad"] = {
            "_id": "a:bad",
            "ui_id": "a",
            "content_id": "bad",
            "format": "markdown",
            "source": {"not": "a string"},
            "renderer_version": "0",
        }
        collection.docs["a:good"] = {
            "_id": "a:good",
            "ui_id": "a",
            "content_id": "good",
            "format": "markdown",
            "source": "# ok",
            "renderer_version": "0",
            "revision": 1,
        }
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report == {
            "examined": 2,
            "rebuilt": 1,
            "failed": 1,
            "skipped": 0,
            "truncated": 0,
            "malformed_ids": 0,
        }
        assert collection.docs["a:good"]["renderer_version"] == RENDERER_FINGERPRINT


class TestAbaTokenClosesDeleteRecreate:
    """A revision counter cannot express generation: delete-and-recreate resets
    it, so a stale writer's revision-only predicate matches the new document."""

    def test_token_and_revision_together_identify_the_generation(self) -> None:
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            _cas_predicate,
        )

        predicate = _cas_predicate({"_id": "x"}, 1, "tok-a")
        assert _FakeCollection._matches(
            {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: "tok-a"}, predicate
        )
        assert not _FakeCollection._matches(
            {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: "tok-b"}, predicate
        )
        # the revision is compared too - a token alone is not enough, because an
        # out-of-band edit can advance the revision while keeping the token
        assert not _FakeCollection._matches(
            {"_id": "x", "revision": 99, CAS_TOKEN_FIELD: "tok-a"}, predicate
        )

    def test_legacy_document_pins_the_absence_of_a_token(self) -> None:
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            MISSING,
            _cas_predicate,
        )

        legacy = {"_id": "x", "revision": 1}
        stored_null = {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: None}
        assert _FakeCollection._matches(
            legacy, _cas_predicate({"_id": "x"}, 1, MISSING)
        )
        assert not _FakeCollection._matches(
            stored_null, _cas_predicate({"_id": "x"}, 1, MISSING)
        )
        assert _FakeCollection._matches(
            stored_null, _cas_predicate({"_id": "x"}, 1, None)
        )
        assert not _FakeCollection._matches(
            legacy, _cas_predicate({"_id": "x"}, 1, None)
        )

    def test_stale_writer_does_not_match_a_recreated_document(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            ContentStore,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# one"})
            )
        )
        first = dict(collection.docs["default:p"])

        # delete and recreate: same revision, different generation
        del collection.docs["default:p"]
        reset_caches()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# two"})
            )
        )
        second = collection.docs["default:p"]
        assert second["revision"] == first["revision"]
        assert second[CAS_TOKEN_FIELD] != first[CAS_TOKEN_FIELD]

        # the stale generation's predicate must match nothing
        result = loop.run_until_complete(
            collection.replace_one(
                {"_id": "default:p", CAS_TOKEN_FIELD: first[CAS_TOKEN_FIELD]},
                dict(first, source="# stale"),
            )
        )
        assert result.matched_count == 0
        assert collection.docs["default:p"]["source"] == "# two"

    def test_rebuild_carries_a_fresh_token(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            rebuild_stale_content,
        )

        collection = _FakeCollection()
        collection.docs["a:b"] = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "source": "# ok",
            "renderer_version": "0",
            "revision": 1,
            CAS_TOKEN_FIELD: "old-token",
        }
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["rebuilt"] == 1
        assert collection.docs["a:b"][CAS_TOKEN_FIELD] != "old-token"

    def test_the_token_never_reaches_the_settings_model(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD, SettingsStore
        from freva_rest.settings_api.registry import REGISTRY

        entry = REGISTRY["ui"]
        store = SettingsStore(
            None, "ui", "default", entry.model, entry.update_model, entry.open_maps
        )
        overrides = store._overrides(
            {"site_title": "X", CAS_TOKEN_FIELD: "tok", "revision": 3}
        )
        assert overrides == {"site_title": "X"}


class TestRevisionNormalisationIsBounded:
    def test_infinity_does_not_raise(self) -> None:
        from freva_rest.settings_api.core import _as_int

        assert _as_int(float("inf")) == 0
        assert _as_int(float("-inf")) == 0
        assert _as_int(float("nan")) == 0

    def test_out_of_range_int_is_reset(self) -> None:
        from freva_rest.settings_api.core import BSON_INT64_MAX, _as_int

        assert _as_int("9" * 30) == 0
        assert _as_int(BSON_INT64_MAX) == 0  # +1 would overflow on write
        assert _as_int(BSON_INT64_MAX - 1) == BSON_INT64_MAX - 1

    def test_ordinary_values_unchanged(self) -> None:
        from freva_rest.settings_api.core import _as_int

        assert _as_int(7) == 7
        assert _as_int("7") == 7


class TestMalformedStoredSource:
    """An inherited non-string source is refused, not turned into a 500 and not
    silently rewritten to ''."""

    @staticmethod
    def _patch_with(stored_source, body):
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["default:p"] = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "source": stored_source,
            "source_hash": "old",
            "rendered_html": "<p>old</p>",
            "renderer_version": "0",
            "revision": 1,
        }
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        return (
            collection,
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(store.patch(ContentSource.model_validate(body))),
        )

    @pytest.mark.parametrize("stored", [{"a": 1}, [1], 0, [], {}, None])
    def test_metadata_patch_on_a_malformed_source_is_422(self, stored) -> None:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as caught:
            self._patch_with(stored, {"title": "New"})
        assert caught.value.status_code == 422
        assert "source" in str(caught.value.detail).lower()

    def test_an_explicit_replacement_source_repairs_it(self) -> None:
        collection, _ = self._patch_with({"a": 1}, {"source": "# fixed"})
        stored = collection.docs["default:p"]
        assert stored["source"] == "# fixed"
        assert stored["source_hash"] != "old"
        assert "fixed" in stored["rendered_html"]

    def test_an_explicit_null_source_clears_it(self) -> None:
        collection, _ = self._patch_with("# a", {"source": None})
        assert collection.docs["default:p"]["source"] == ""


class TestNonFiniteCacheTtl:
    def test_infinite_ttl_falls_back_to_the_default(self, monkeypatch) -> None:
        from freva_rest.settings_api.core import _positive_float

        monkeypatch.setenv("X_TTL", "inf")
        assert _positive_float("X_TTL", 2.0) == 2.0
        monkeypatch.setenv("X_TTL", "nan")
        assert _positive_float("X_TTL", 2.0) == 2.0
        monkeypatch.setenv("X_TTL", "-1")
        assert _positive_float("X_TTL", 2.0) == 2.0

    def test_a_real_value_is_honoured(self, monkeypatch) -> None:
        from freva_rest.settings_api.core import _positive_float

        monkeypatch.setenv("X_TTL", "5.5")
        assert _positive_float("X_TTL", 2.0) == 5.5


class TestConditionalDeleteUnit:
    """delete_one by _id alone removes whatever generation happens to be there
    when a slow delete finally gets around to it."""

    @staticmethod
    def _store(collection):
        from freva_rest.settings_api.core import ContentStore, reset_caches

        reset_caches()
        return ContentStore(_FakeConfig(collection), "default", "p")

    def _seed(self, collection, source):
        import asyncio

        from freva_rest.settings_api.schema import ContentSource

        store = self._store(collection)
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": source})
            )
        )
        return store

    def test_stale_delete_does_not_remove_a_new_generation(self) -> None:
        import asyncio

        collection = _FakeCollection()
        store = self._seed(collection, "# one")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        expected = loop.run_until_complete(store.cas_state())

        # the document is deleted and recreated while the delete is "thinking"
        del collection.docs["default:p"]
        self._seed(collection, "# two")

        outcome = loop.run_until_complete(store.delete(expected=expected))
        assert outcome == "changed"
        assert collection.docs["default:p"]["source"] == "# two"

    def test_matching_delete_succeeds(self) -> None:
        import asyncio

        collection = _FakeCollection()
        store = self._seed(collection, "# one")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        expected = loop.run_until_complete(store.cas_state())
        assert loop.run_until_complete(store.delete(expected=expected)) == "deleted"
        assert "default:p" not in collection.docs

    def test_delete_of_a_missing_document_reports_missing(self) -> None:
        import asyncio

        collection = _FakeCollection()
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        assert loop.run_until_complete(store.cas_state()) is None
        assert loop.run_until_complete(store.delete()) == "missing"

    def test_unconditional_delete_still_available(self) -> None:
        import asyncio

        collection = _FakeCollection()
        self._seed(collection, "# one")
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        assert loop.run_until_complete(store.delete()) == "deleted"


class TestSettingsStoreConditionalDelete:
    """
    SettingsStore.delete has the same CAS contract as ContentStore.delete.
    """

    class _SettingsConfig:
        def __init__(self, collection) -> None:
            self._collection = collection

        @property
        def mongo_collection_settings(self):
            return self._collection

    @staticmethod
    def _store(collection):
        from freva_rest.settings_api.core import SettingsStore, reset_caches
        from freva_rest.settings_api.registry import REGISTRY

        reset_caches()
        entry = REGISTRY["ui"]
        return SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "doomed",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )

    def _write(self, collection, title):
        import asyncio

        from freva_rest.settings_api.schema import UiConfigUpdate

        store = self._store(collection)
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.patch(UiConfigUpdate.model_validate({"site_title": title}))
        )
        return store

    def test_stale_delete_does_not_remove_a_newer_record(self) -> None:
        import asyncio

        collection = _FakeCollection()
        store = self._write(collection, "A")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        expected = loop.run_until_complete(store.cas_state())
        self._write(collection, "B")
        assert loop.run_until_complete(store.delete(expected=expected)) == "changed"
        assert collection.docs["ui:doomed"]["site_title"] == "B"

    def test_matching_delete_succeeds(self) -> None:
        import asyncio

        collection = _FakeCollection()
        store = self._write(collection, "A")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        expected = loop.run_until_complete(store.cas_state())
        assert loop.run_until_complete(store.delete(expected=expected)) == "deleted"
        assert "ui:doomed" not in collection.docs

    def test_missing_record_reports_missing(self) -> None:
        import asyncio

        collection = _FakeCollection()
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        assert loop.run_until_complete(store.cas_state()) is None
        assert loop.run_until_complete(store.delete()) == "missing"


class TestIfMatchRequiresExistence:
    """RFC 9110 13.1.1: If-Match fails when the resource has no current
    representation - for every value, not only '*'."""

    def test_exact_etag_fails_when_the_resource_is_absent(self) -> None:
        from freva_rest.settings_api.core import etag_of, strong_if_match

        tag = etag_of(b'{"site_title":"Freva"}')
        assert strong_if_match(tag, tag, exists=False) is False

    def test_star_fails_when_absent(self) -> None:
        from freva_rest.settings_api.core import etag_of, strong_if_match

        assert strong_if_match("*", etag_of(b"{}"), exists=False) is False

    def test_list_of_tags_fails_when_absent(self) -> None:
        from freva_rest.settings_api.core import etag_of, strong_if_match

        tag = etag_of(b"{}")
        assert strong_if_match(f'"other", {tag}', tag, exists=False) is False

    def test_matching_tag_still_passes_when_present(self) -> None:
        from freva_rest.settings_api.core import etag_of, strong_if_match

        tag = etag_of(b"{}")
        assert strong_if_match(tag, tag, exists=True) is True

    def test_a_named_record_cannot_be_created_with_the_default_etag(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import (
            SettingsStore,
            etag_of,
            reset_caches,
        )
        from freva_rest.settings_api.registry import REGISTRY
        from freva_rest.settings_api.schema import UiConfigUpdate

        entry = REGISTRY["ui"]
        collection = _FakeCollection()

        class _Config:
            mongo_collection_settings = collection

        reset_caches()
        default_store = SettingsStore(
            _Config(), "ui", "default", entry.model, entry.update_model, entry.open_maps
        )
        loop = asyncio.get_event_loop_policy().new_event_loop()
        _, default_etag = loop.run_until_complete(default_store.get(allow_default=True))

        named = SettingsStore(
            _Config(),
            "ui",
            "waterpark",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )
        with pytest.raises(HTTPException) as caught:
            loop.run_until_complete(
                named.patch(
                    UiConfigUpdate.model_validate({"site_title": "X"}),
                    if_match=default_etag,
                )
            )
        assert caught.value.status_code == 412
        assert "ui:waterpark" not in collection.docs
        assert etag_of(b"") != default_etag  # sanity: a real tag was used


class TestMalformedCasToken:
    """A stored null / empty / non-string token must stay matchable, or the
    document is unpatchable, unrebuildable and undeletable at once."""

    @pytest.mark.parametrize("token", [None, "", 0, 123, [], {}])
    def test_present_but_malformed_token_is_matched_literally(self, token) -> None:
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            _cas_predicate,
        )

        predicate = _cas_predicate({"_id": "x"}, 1, token)
        assert _FakeCollection._matches(
            {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: token}, predicate
        )
        assert not _FakeCollection._matches({"_id": "x", "revision": 1}, predicate)

    def test_absent_token_still_pins_absence(self) -> None:
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            MISSING,
            _cas_predicate,
        )

        predicate = _cas_predicate({"_id": "x"}, 1, MISSING)
        assert _FakeCollection._matches({"_id": "x", "revision": 1}, predicate)
        assert not _FakeCollection._matches(
            {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: "t"}, predicate
        )

    @pytest.mark.parametrize("token", [None, "", 123])
    def test_a_document_with_a_bad_token_can_be_patched(self, token) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            ContentStore,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["default:p"] = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "source": "# a",
            "revision": 1,
            CAS_TOKEN_FIELD: token,
        }
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.patch(ContentSource.model_validate({"title": "New"}))
        )
        assert collection.docs["default:p"]["title"] == "New"
        assert isinstance(collection.docs["default:p"][CAS_TOKEN_FIELD], str)

    def test_a_document_with_a_bad_token_can_be_deleted(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            ContentStore,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["default:p"] = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "revision": 1,
            CAS_TOKEN_FIELD: None,
        }
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        expected = loop.run_until_complete(store.cas_state())
        assert loop.run_until_complete(store.delete(expected=expected)) == "deleted"

    def test_a_document_with_a_bad_token_is_rebuilt(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            rebuild_stale_content,
        )

        collection = _FakeCollection()
        collection.docs["a:b"] = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "source": "# ok",
            "renderer_version": "0",
            "revision": 1,
            CAS_TOKEN_FIELD: None,
        }
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["rebuilt"] == 1, report


class TestRebuildIdentityFailures:
    """Identity, update and cache invalidation all sit inside the per-record
    boundary, so a KeyError after a partial write fails one record only."""

    def test_missing_identity_fails_one_record_only(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import rebuild_stale_content
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        collection = _FakeCollection()
        collection.docs["a:noident"] = {
            "_id": "a:noident",
            "format": "markdown",
            "source": "# fine",
            "renderer_version": "0",
            "revision": 1,
        }
        collection.docs["a:good"] = {
            "_id": "a:good",
            "ui_id": "a",
            "content_id": "good",
            "format": "markdown",
            "source": "# ok",
            "renderer_version": "0",
            "revision": 1,
        }
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report == {
            "examined": 2,
            "rebuilt": 1,
            "failed": 1,
            "skipped": 0,
            "truncated": 0,
            "malformed_ids": 0,
        }
        assert collection.docs["a:good"]["renderer_version"] == RENDERER_FINGERPRINT
        assert collection.docs["a:noident"]["renderer_version"] == "0"

    @pytest.mark.parametrize("bad", [{"ui_id": 5}, {"content_id": None}, {}])
    def test_various_identity_shapes_are_counted_not_raised(self, bad) -> None:
        import asyncio

        from freva_rest.settings_api.core import rebuild_stale_content

        collection = _FakeCollection()
        collection.docs["a:b"] = dict(
            {
                "_id": "a:b",
                "format": "markdown",
                "source": "# fine",
                "renderer_version": "0",
            },
            **bad,
        )
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["failed"] == 1
        assert report["rebuilt"] == 0


class TestMalformedOpenMapRepair:
    """A partial patch onto a damaged open map must repair it, not 500."""

    @staticmethod
    def _merge(stored, body):
        from freva_rest.settings_api.core import SettingsStore
        from freva_rest.settings_api.registry import REGISTRY
        from freva_rest.settings_api.schema import UiConfigUpdate

        entry = REGISTRY["ui"]
        store = SettingsStore(
            None, "ui", "default", entry.model, entry.update_model, entry.open_maps
        )
        return store._merge(stored, UiConfigUpdate.model_validate(body))

    @pytest.mark.parametrize("stored", ["abc", 5, ["a"], True, 1.5])
    def test_top_level_open_map_is_replaced_not_fatal(self, stored) -> None:
        merged = self._merge(
            {"extra_colors": stored}, {"extra_colors": {"brand": "#111111"}}
        )
        assert merged["extra_colors"] == {"brand": "#111111"}
        UiConfig(**merged)

    @pytest.mark.parametrize("stored", ["abc", 5, ["a"]])
    def test_public_extensions_too(self, stored) -> None:
        merged = self._merge(
            {"public_extensions": stored}, {"public_extensions": {"a": "1"}}
        )
        assert merged["public_extensions"] == {"a": "1"}

    @pytest.mark.parametrize("stored", ["abc", 5, ["a"]])
    def test_nested_fixed_facets_too(self, stored) -> None:
        merged = self._merge(
            {"features": {"databrowser": {"fixed_facets": stored}}},
            {"features": {"databrowser": {"fixed_facets": {"project": ["a"]}}}},
        )
        assert merged["features"]["databrowser"]["fixed_facets"] == {"project": ["a"]}

    def test_a_healthy_map_still_merges(self) -> None:
        merged = self._merge(
            {"extra_colors": {"brand": "#111111"}},
            {"extra_colors": {"accent": "#222222"}},
        )
        assert merged["extra_colors"] == {"brand": "#111111", "accent": "#222222"}


class TestRebuildSurfacesOutages:
    """A database outage is not a per-record failure. Counting it as one would
    let the rebuild endpoint answer 200 with a tally while mongo is
    unreachable."""

    class _FlakyCollection(_FakeCollection):
        async def update_one(self, predicate, update):
            from pymongo.errors import PyMongoError

            raise PyMongoError("connection lost")

    def _stale_doc(self):
        return {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "source": "# ok",
            "renderer_version": "0",
            "revision": 1,
        }

    def test_pymongo_error_propagates(self) -> None:
        import asyncio

        from pymongo.errors import PyMongoError

        from freva_rest.settings_api.core import rebuild_stale_content

        collection = self._FlakyCollection()
        collection.docs["a:b"] = self._stale_doc()
        with pytest.raises(PyMongoError):
            asyncio.run(rebuild_stale_content(_FakeConfig(collection)))

    def test_document_damage_is_still_counted_not_raised(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import rebuild_stale_content

        collection = _FakeCollection()
        collection.docs["a:b"] = dict(self._stale_doc(), source={"not": "a string"})
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["failed"] == 1


class TestCasPredicateUsesExplicitOperators:
    """A bare value is not an equality match in mongo, and a dict value is read
    as an operator expression."""

    @staticmethod
    def _predicate(revision, token):
        from freva_rest.settings_api.core import _cas_predicate

        return _cas_predicate({"_id": "x"}, revision, token)

    def test_null_token_does_not_match_a_missing_field(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        predicate = self._predicate(1, None)
        legacy = {"_id": "x", "revision": 1}  # no token field at all
        stored_null = {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: None}
        assert not _FakeCollection._matches(legacy, predicate)
        assert _FakeCollection._matches(stored_null, predicate)

    def test_a_bare_none_predicate_would_have_matched_both(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        # the naive shape, kept as a witness to why it is wrong
        naive = {"_id": "x", CAS_TOKEN_FIELD: None}
        legacy = {"_id": "x"}
        stored_null = {"_id": "x", CAS_TOKEN_FIELD: None}
        assert _FakeCollection._matches(legacy, naive)
        assert _FakeCollection._matches(stored_null, naive)

    def test_dict_token_is_not_read_as_an_operator(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        # a stored {"$ne": "x"} must be compared as a *value*; read as an
        # operator it would match almost every document
        predicate = self._predicate(1, {"$ne": "x"})
        other = {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: "something-else"}
        assert not _FakeCollection._matches(other, predicate)
        same = {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: {"$ne": "x"}}
        assert _FakeCollection._matches(same, predicate)

    def test_explicit_zero_revision_does_not_match_an_absent_one(self) -> None:
        predicate = self._predicate(
            0, __import__("freva_rest.settings_api.core", fromlist=["MISSING"]).MISSING
        )
        assert not _FakeCollection._matches({"_id": "x"}, predicate)
        assert _FakeCollection._matches({"_id": "x", "revision": 0}, predicate)

    def test_absent_revision_does_not_match_an_explicit_zero(self) -> None:
        from freva_rest.settings_api.core import MISSING

        predicate = self._predicate(MISSING, MISSING)
        assert _FakeCollection._matches({"_id": "x"}, predicate)
        assert not _FakeCollection._matches({"_id": "x", "revision": 0}, predicate)

    def test_stale_writer_still_cannot_match_a_recreated_generation(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        predicate = self._predicate(1, "generation-one")
        recreated = {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: "generation-two"}
        assert not _FakeCollection._matches(recreated, predicate)


class TestCasComparisonIsTypeExact:
    """Query `$eq` descends into arrays, so an expected "x" would match a stored
    ["x"] - a narrow ABA hole of the same kind."""

    @staticmethod
    def _predicate(token):
        from freva_rest.settings_api.core import _cas_predicate

        return _cas_predicate({"_id": "x"}, 1, token)

    def test_scalar_does_not_match_an_array_containing_it(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        predicate = self._predicate("x")
        assert not _FakeCollection._matches(
            {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: ["x"]}, predicate
        )
        assert _FakeCollection._matches(
            {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: "x"}, predicate
        )

    def test_query_eq_would_have_matched_it(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        naive = {"_id": "x", CAS_TOKEN_FIELD: {"$eq": "x"}}
        assert _FakeCollection._matches(
            {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: ["x"]}, naive
        )

    def test_array_token_matches_only_the_same_array(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        predicate = self._predicate(["x"])
        assert _FakeCollection._matches(
            {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: ["x"]}, predicate
        )
        assert not _FakeCollection._matches(
            {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: "x"}, predicate
        )

    @pytest.mark.parametrize(
        "expected,stored",
        [(1, True), (True, 1), (1, 1.0), (0, False), ("1", 1)],
    )
    def test_comparison_is_type_strict(self, expected, stored) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        predicate = self._predicate(expected)
        assert not _FakeCollection._matches(
            {"_id": "x", CAS_TOKEN_FIELD: stored}, predicate
        )

    def test_a_real_write_round_trips(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        reset_caches()
        loop.run_until_complete(
            store.patch(ContentSource.model_validate({"title": "second write"}))
        )
        assert collection.docs["default:p"]["title"] == "second write"
        assert collection.docs["default:p"]["revision"] == 2


class TestRebuildCannotRepairAnId:
    """An _id is immutable in mongo. Synthesising one produces a filter that
    cannot match the document it came from, so it would report 'skipped'
    forever."""

    @staticmethod
    def _rebuild(doc):
        import asyncio

        from freva_rest.settings_api.core import rebuild_stale_content

        collection = _FakeCollection()
        raw = doc.get("_id")
        collection.docs[raw if isinstance(raw, str) and raw else "<unkeyed>"] = doc
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        return collection, report

    def _base(self, **over):
        return dict(
            {
                "ui_id": "a",
                "content_id": "b",
                "format": "markdown",
                "source": "# ok",
                "renderer_version": "0",
                "revision": 1,
            },
            **over,
        )

    @pytest.mark.parametrize("bad_id", [""])
    def test_an_unusable_string_id_is_a_failure_not_a_skip(self, bad_id) -> None:
        _, report = self._rebuild(self._base(_id=bad_id))
        assert report["failed"] == 1, report
        assert report["skipped"] == 0, report
        assert report["rebuilt"] == 0, report

    @pytest.mark.parametrize("bad_id", [None, 12345, {"x": 1}, ["a:b"]])
    def test_a_non_string_id_is_counted_in_its_own_domain(self, bad_id) -> None:
        _, report = self._rebuild(self._base(_id=bad_id))
        assert report["malformed_ids"] == 1, report
        assert report["examined"] == 0, report
        assert report["failed"] == 0, report
        assert report["failed"] <= report["examined"], report

    def test_id_disagreeing_with_the_natural_key_is_a_failure(self) -> None:
        _, report = self._rebuild(self._base(_id="somethingelse"))
        assert report["failed"] == 1, report
        assert report["skipped"] == 0, report

    def test_a_correct_id_still_rebuilds(self) -> None:
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        collection, report = self._rebuild(self._base(_id="a:b"))
        assert report["rebuilt"] == 1, report
        assert collection.docs["a:b"]["renderer_version"] == RENDERER_FINGERPRINT

    def test_a_broken_id_does_not_stop_the_run(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import rebuild_stale_content

        collection = _FakeCollection()
        collection.docs["bad"] = self._base(_id=None)
        collection.docs["a:good"] = dict(
            self._base(_id="a:good"), ui_id="a", content_id="good"
        )
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report == {
            "examined": 1,
            "rebuilt": 1,
            "failed": 0,
            "skipped": 0,
            "truncated": 0,
            "malformed_ids": 1,
        }


class TestContentReadShapesAreDisjoint:
    """The documented `oneOf` requires exactly one branch to match."""

    @staticmethod
    def _payloads():
        public = ContentPublic(
            ui_id="a", content_id="b", format="markdown"
        ).model_dump()
        admin = ContentAdmin(
            ui_id="a", content_id="b", format="markdown", source="x", source_hash="h"
        ).model_dump()
        return public, admin

    def test_admin_fields_are_required(self) -> None:
        required = set(ContentAdmin.model_json_schema()["required"])
        assert {"source", "source_hash"} <= required

    def test_public_forbids_extras(self) -> None:
        assert ContentPublic.model_json_schema()["additionalProperties"] is False

    def test_a_public_payload_is_not_a_valid_admin_payload(self) -> None:
        public, _ = self._payloads()
        with pytest.raises(ValidationError):
            ContentAdmin.model_validate(public)

    def test_an_admin_payload_is_not_a_valid_public_payload(self) -> None:
        _, admin = self._payloads()
        with pytest.raises(ValidationError):
            ContentPublic.model_validate(admin)

    def test_each_payload_matches_exactly_one_branch(self) -> None:
        jsonschema = pytest.importorskip("jsonschema")
        public, admin = self._payloads()
        schemas = {
            "ContentPublic": ContentPublic.model_json_schema(),
            "ContentAdmin": ContentAdmin.model_json_schema(),
        }
        for payload in (public, admin):
            hits = [
                name
                for name, schema in schemas.items()
                if jsonschema.Draft202012Validator(schema).is_valid(payload)
            ]
            assert len(hits) == 1, (payload, hits)

    def test_the_admin_read_still_builds(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import ContentStore, reset_caches

        collection = _FakeCollection()
        collection.docs["default:p"] = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "source": "# a",
            "source_hash": "h",
            "rendered_html": "<h1>a</h1>",
            "renderer_version": "0",
            "revision": 1,
        }
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(store.get_admin())
        )
        payload = json.loads(body)
        assert payload["source"] == "# a"
        assert payload["source_hash"] == "h"


class TestCasNumericTypeExactness:
    """Mongo's numeric BSON types compare equal to one another, so aggregation
    `$eq` alone does not distinguish 1 from 1.0"""

    @staticmethod
    def _predicate(token):
        from freva_rest.settings_api.core import _cas_predicate

        return _cas_predicate({"_id": "x"}, 1, token)

    @staticmethod
    def _doc(token):
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        return {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: token}

    def test_eq_alone_would_conflate_int_and_double(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        eq_only = {
            "_id": "x",
            "$expr": {"$eq": [f"${CAS_TOKEN_FIELD}", {"$literal": 1}]},
        }
        assert _FakeCollection._matches(self._doc(1.0), eq_only)

    @pytest.mark.parametrize("expected,stored", [(1, 1.0), (1.0, 1), (2**40, 2.0**40)])
    def test_type_clause_separates_numeric_types(self, expected, stored) -> None:
        assert not _FakeCollection._matches(
            self._doc(stored), self._predicate(expected)
        )

    @pytest.mark.parametrize("value", [1, 1.0, 2**40, "x", None, ["x"], {"k": "v"}])
    def test_a_value_always_matches_itself(self, value) -> None:
        assert _FakeCollection._matches(self._doc(value), self._predicate(value))

    @pytest.mark.parametrize("expected,stored", [(1, True), (True, 1), (0, False)])
    def test_bool_is_its_own_type(self, expected, stored) -> None:
        assert not _FakeCollection._matches(
            self._doc(stored), self._predicate(expected)
        )

    def test_int32_and_int64_are_distinguished(self) -> None:
        assert not _FakeCollection._matches(self._doc(2**40), self._predicate(1))

    def test_api_issued_tokens_are_strings(self) -> None:
        from freva_rest.settings_api.core import _new_cas_token

        token = _new_cas_token()
        assert isinstance(token, str) and len(token) == 32
        assert _FakeCollection._matches(self._doc(token), self._predicate(token))


class TestStaleReadCannotRepopulateCaches:
    """A read is not atomic. If a write commits while a read is in flight, the
    read must not then store what it fetched"""

    class _RacingCollection(_FakeCollection):
        """Commits a write *during* the read, between query and cache put."""

        def __init__(self, on_read) -> None:
            super().__init__()
            self._on_read = on_read

        async def find_one(self, key, projection=None):
            doc = await super().find_one(key, projection)
            self._on_read()
            return doc

    def _setup(self, cache_key, new_doc):
        from freva_rest.settings_api.core import (
            _cache_invalidate,
            reset_caches,
        )

        reset_caches()
        fired = {"done": False}

        def _commit_a_write():
            if fired["done"]:
                return
            fired["done"] = True
            collection.docs["default:p"] = new_doc
            _cache_invalidate(cache_key)

        collection = self._RacingCollection(_commit_a_write)
        return collection

    def test_public_read_does_not_cache_a_superseded_document(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import (
            ContentStore,
            _body_get,
            _cache_get,
            _last_known_good,
        )
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        old = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "rendered_html": "<p>old</p>",
            "rendered_hash": rendered_hash("<p>old</p>"),
            "renderer_version": RENDERER_FINGERPRINT,
            "revision": 1,
        }
        new = dict(
            old,
            rendered_html="<p>new</p>",
            rendered_hash=rendered_hash("<p>new</p>"),
            revision=2,
        )
        collection = self._setup("content:default:p", new)
        collection.docs["default:p"] = old

        store = ContentStore(_FakeConfig(collection), "default", "p")
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(store.get_public())
        )
        assert "old" in json.loads(body)["rendered_html"]
        assert _cache_get("content:default:p") is None
        assert _body_get("content:default:p") is None
        assert _last_known_good("content:default:p") is None

    def test_the_next_read_sees_the_new_document(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import ContentStore
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        old = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "rendered_html": "<p>old</p>",
            "rendered_hash": rendered_hash("<p>old</p>"),
            "renderer_version": RENDERER_FINGERPRINT,
            "revision": 1,
        }
        new = dict(
            old,
            rendered_html="<p>new</p>",
            rendered_hash=rendered_hash("<p>new</p>"),
            revision=2,
        )
        collection = self._setup("content:default:p", new)
        collection.docs["default:p"] = old
        store = ContentStore(_FakeConfig(collection), "default", "p")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(store.get_public())
        body, _ = loop.run_until_complete(store.get_public())
        assert "new" in json.loads(body)["rendered_html"]

    def test_settings_read_does_not_cache_a_superseded_record(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            SettingsStore,
            _cache_get,
            _cache_invalidate,
            _last_known_good,
            reset_caches,
        )
        from freva_rest.settings_api.registry import REGISTRY

        reset_caches()
        entry = REGISTRY["ui"]
        fired = {"done": False}

        class _Racing(_FakeCollection):
            async def find_one(self, key, projection=None):
                doc = await super().find_one(key, projection)
                if not fired["done"]:
                    fired["done"] = True
                    _cache_invalidate("settings:ui:default")
                return doc

        collection = _Racing()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "site_title": "Old",
            "revision": 1,
        }

        class _Config:
            mongo_collection_settings = collection

        store = SettingsStore(
            _Config(), "ui", "default", entry.model, entry.update_model, entry.open_maps
        )
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(store.get())
        assert _cache_get("settings:ui:default") is None
        assert _last_known_good("settings:ui:default") is None

    def test_an_uncontended_read_still_caches(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            ContentStore,
            _body_get,
            _cache_get,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["default:p"] = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "rendered_html": "<p>x</p>",
            "renderer_version": "0",
            "revision": 1,
        }
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.get_public()
        )
        assert _cache_get("content:default:p") is not None
        assert _body_get("content:default:p") is not None

    def test_a_write_still_seeds_the_cache(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            ContentStore,
            _cache_get,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        assert _cache_get("content:default:p") is not None

    def test_epoch_only_grows_on_invalidation(self) -> None:
        from freva_rest.settings_api.core import (
            _cache_invalidate,
            _current_epoch,
            reset_caches,
        )

        reset_caches()
        start = _current_epoch()
        _cache_invalidate("k")
        assert _current_epoch() == start + 1
        _cache_invalidate("k", keep_lkg=True)
        assert _current_epoch() == start + 2

    def test_the_epoch_is_a_single_int_not_a_per_key_map(self) -> None:
        from freva_rest.settings_api import core

        assert isinstance(core._current_epoch(), int)
        assert not hasattr(core, "_generations")

    def test_a_write_to_one_key_drops_an_in_flight_fill_for_another(self) -> None:
        from freva_rest.settings_api.core import (
            _cache_get,
            _cache_invalidate,
            _cache_put,
            _current_epoch,
            reset_caches,
        )

        reset_caches()
        seen = _current_epoch()
        _cache_invalidate("some:other:key")
        _cache_put("unrelated:key", {"a": 1}, seen)
        assert _cache_get("unrelated:key") is None


class TestEveryReadOriginIsGuarded:
    """
    get_admin, get_document and exists never repopulate the cache, and
    `_read` resolves its own snapshot when a caller omits one, so no read
    origin can reinstate a stale generation.
    """

    OLD = {
        "_id": "default:p",
        "ui_id": "default",
        "content_id": "p",
        "format": "sandbox-html",
        "source": "<html>old</html>",
        "source_hash": "h-old",
        "rendered_html": "",
        "renderer_version": "0",
        "revision": 1,
    }

    def _racing_store(self):
        from freva_rest.settings_api.core import (
            ContentStore,
            _cache_invalidate,
            reset_caches,
        )

        reset_caches()
        fired = {"done": False}
        new = dict(self.OLD, source="<html>new</html>", revision=2)

        class _Racing(_FakeCollection):
            async def find_one(self, key, projection=None):
                doc = await _FakeCollection.find_one(self, key, projection)
                if not fired["done"]:
                    fired["done"] = True
                    self.docs["default:p"] = new
                    _cache_invalidate("content:default:p")
                return doc

        collection = _Racing()
        collection.docs["default:p"] = dict(self.OLD)
        return collection, ContentStore(_FakeConfig(collection), "default", "p")

    @staticmethod
    def _caches_empty() -> bool:
        from freva_rest.settings_api.core import (
            _body_get,
            _cache_get,
            _last_known_good,
        )

        key = "content:default:p"
        return (
            _cache_get(key) is None
            and _body_get(key) is None
            and _last_known_good(key) is None
        )

    def test_get_admin_does_not_repopulate(self) -> None:
        import asyncio

        _, store = self._racing_store()
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.get_admin()
        )
        assert self._caches_empty()

    def test_get_document_does_not_repopulate(self) -> None:
        import asyncio

        _, store = self._racing_store()
        source = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(store.get_document())
        )
        assert source == "<html>old</html>"  # what was current when fetched
        assert self._caches_empty()

    def test_exists_does_not_repopulate(self) -> None:
        import asyncio

        _, store = self._racing_store()
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store._read()
        )
        assert self._caches_empty()

    def test_the_snapshot_is_taken_even_when_the_caller_omits_it(self) -> None:
        import asyncio
        import inspect

        from freva_rest.settings_api.core import ContentStore

        source = inspect.getsource(ContentStore._read_uncached)
        assert "if seen is None:" in source
        assert "_current_epoch()" in source

        collection = _FakeCollection()
        collection.docs["default:p"] = dict(self.OLD)
        from freva_rest.settings_api.core import _cache_get, reset_caches

        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.get_admin()
        )
        assert _cache_get("content:default:p") is not None


class TestReuseRequiresAConsistentDocument:
    """
    A metadata-only PATCH must not stamp a fresh source_hash onto html that
    was rendered from a different source.
    """

    @staticmethod
    def _drifted():
        from freva_rest.settings_api.core import source_hash
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        return {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "source": "# new source",
            "source_hash": source_hash("# old source", "markdown"),
            "rendered_html": "<h1>old source</h1>",
            "rendered_hash": rendered_hash("<h1>old source</h1>"),
            "renderer_version": RENDERER_FINGERPRINT,
            "revision": 1,
        }

    def _patch_title(self, doc):
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["default:p"] = doc
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.patch(ContentSource.model_validate({"title": "New title"}))
        )
        return collection.docs["default:p"]

    def test_a_drifted_document_is_preserved_not_re_rendered(self) -> None:
        before = self._drifted()
        stored = self._patch_title(dict(before))
        for field in (
            "rendered_html",
            "rendered_hash",
            "source_hash",
            "renderer_version",
            "source",
        ):
            assert stored[field] == before[field], field
        assert stored["title"] == "New title"

    def test_the_drift_is_not_laundered_into_consistency(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild, source_hash

        stored = self._patch_title(self._drifted())
        assert stored["source_hash"] != source_hash(stored["source"], stored["format"])
        assert needs_rebuild(stored) is True

    def test_the_renderer_is_never_called(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        calls = {"n": 0}
        real = core.render

        def _counting(source, fmt):
            calls["n"] += 1
            return real(source, fmt)

        collection = _FakeCollection()
        collection.docs["default:p"] = self._drifted()
        reset_caches()
        core.render = _counting
        try:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                ContentStore(_FakeConfig(collection), "default", "p").patch(
                    ContentSource.model_validate({"title": "New title"})
                )
            )
        finally:
            core.render = real
        assert calls["n"] == 0

    def test_a_consistent_document_still_reuses_its_html(self) -> None:
        from freva_rest.settings_api.core import source_hash
        from freva_rest.settings_api.renderers import rendered_hash

        marker = "<h1>a marker only reuse can preserve</h1>"
        consistent = dict(
            self._drifted(),
            source="# same",
            source_hash=source_hash("# same", "markdown"),
            rendered_html=marker,
            rendered_hash=rendered_hash(marker),
        )
        stored = self._patch_title(consistent)
        assert "a marker only reuse can preserve" in stored["rendered_html"]
        assert stored["title"] == "New title"

    def test_non_string_rendered_html_is_preserved_and_stays_stale(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild, source_hash

        broken = dict(
            self._drifted(),
            source="# same",
            source_hash=source_hash("# same", "markdown"),
            rendered_html={"not": "a string"},
        )
        stored = self._patch_title(broken)
        assert stored["rendered_html"] == {"not": "a string"}
        assert needs_rebuild(stored) is True

    def test_rebuild_rewrites_the_hash(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            source_hash,
        )

        collection = _FakeCollection()
        collection.docs["default:p"] = dict(self._drifted(), renderer_version="0")
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["rebuilt"] == 1
        stored = collection.docs["default:p"]
        assert stored["source_hash"] == source_hash(stored["source"], stored["format"])
        assert "new source" in stored["rendered_html"]


class TestCasComparesBothFields:
    """An out-of-band edit can advance the revision while keeping the token."""

    @staticmethod
    def _predicate():
        from freva_rest.settings_api.core import _cas_predicate

        return _cas_predicate({"_id": "x"}, 1, "tok")

    def test_same_token_newer_revision_does_not_match(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        assert not _FakeCollection._matches(
            {"_id": "x", "revision": 2, CAS_TOKEN_FIELD: "tok"}, self._predicate()
        )

    def test_same_revision_different_token_does_not_match(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        assert not _FakeCollection._matches(
            {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: "other"}, self._predicate()
        )

    def test_both_matching_matches(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        assert _FakeCollection._matches(
            {"_id": "x", "revision": 1, CAS_TOKEN_FIELD: "tok"}, self._predicate()
        )

    def test_a_stale_patch_against_a_bumped_revision_is_refused(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            ContentStore,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        base = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "source": "# a",
            "revision": 1,
            CAS_TOKEN_FIELD: "unchanged-token",
        }
        collection.docs["default:p"] = dict(base)
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")

        bumps = {"n": 1}
        real_find_one = collection.find_one

        async def _bump_revision_mid_write(key, projection=None):
            doc = await real_find_one(key, projection)
            bumps["n"] += 1
            collection.docs["default:p"] = dict(base, revision=bumps["n"], source="# b")
            return doc

        collection.find_one = _bump_revision_mid_write
        with pytest.raises(HTTPException) as caught:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                store.patch(ContentSource.model_validate({"title": "T"}))
            )
        assert caught.value.status_code == 409
        assert collection.docs["default:p"]["source"] == "# b"


class TestBsonEncodableValues:
    """Values the models accept must be values mongo can actually store."""

    @pytest.mark.parametrize("value", [2**63, -(2**63) - 1, 2**70])
    def test_out_of_range_integers_rejected(self, value) -> None:
        with pytest.raises(ValidationError):
            UiConfig(public_extensions={"a": value})

    @pytest.mark.parametrize("value", [2**63 - 1, -(2**63), 0, 42])
    def test_in_range_integers_accepted(self, value) -> None:
        assert UiConfig(public_extensions={"a": value}).public_extensions["a"] == value

    def test_the_bound_matches_what_bson_can_encode(self) -> None:
        bson = pytest.importorskip("bson")
        from freva_rest.settings_api.field_types import BSON_INT64_MAX

        bson.encode({"a": BSON_INT64_MAX})
        with pytest.raises(OverflowError):
            bson.encode({"a": BSON_INT64_MAX + 1})

    @pytest.mark.parametrize(
        "key", ["bad\x00key", "with\nnewline", "with\ttab", "dotted.key", "$operator"]
    )
    def test_invalid_map_keys_rejected(self, key) -> None:
        with pytest.raises(ValidationError):
            UiConfig(public_extensions={key: "v"})

    @pytest.mark.parametrize("key", ["plain", "with-dash", "with_underscore", "MiXed1"])
    def test_ordinary_map_keys_accepted(self, key) -> None:
        assert key in UiConfig(public_extensions={key: "v"}).public_extensions

    def test_fixed_facets_keys_are_constrained_too(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(features={"databrowser": {"fixed_facets": {"bad\x00key": ["v"]}}})

    def test_a_valid_config_round_trips_through_bson(self) -> None:
        bson = pytest.importorskip("bson")

        config = UiConfig(
            public_extensions={"count": 2**63 - 1, "flag": True, "name": "x"},
            features={"databrowser": {"fixed_facets": {"project": ["a"]}}},
        )
        bson.encode(config.model_dump(by_alias=True))


class TestStalenessIsOnePredicate:
    """is_stale on a read and 'rebuild this' must agree, or a document can be
    inconsistent, report itself healthy, and be skipped forever."""

    @staticmethod
    def _doc(**over):
        from freva_rest.settings_api.core import source_hash
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        return dict(
            {
                "_id": "a:b",
                "ui_id": "a",
                "content_id": "b",
                "format": "markdown",
                "source": "# a",
                "source_hash": source_hash("# a", "markdown"),
                "rendered_html": "<h1>a</h1>",
                "rendered_hash": rendered_hash("<h1>a</h1>"),
                "renderer_version": RENDERER_FINGERPRINT,
                "revision": 1,
            },
            **over,
        )

    def test_a_consistent_document_is_not_stale(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild

        assert needs_rebuild(self._doc()) is False

    @pytest.mark.parametrize(
        "over",
        [
            {"renderer_version": "0"},
            {"source_hash": "drifted"},
            {"rendered_html": {"not": "a string"}},
            {"source": {"not": "a string"}},
        ],
    )
    def test_inconsistent_documents_are_stale(self, over) -> None:
        from freva_rest.settings_api.core import needs_rebuild

        assert needs_rebuild(self._doc(**over)) is True

    def test_sandbox_content_is_never_stale(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild

        assert needs_rebuild(self._doc(format="sandbox-html")) is False

    def test_the_read_reports_what_the_rebuild_will_act_on(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import (
            ContentStore,
            rebuild_stale_content,
            reset_caches,
        )

        doc = self._doc(source_hash="drifted")
        collection = _FakeCollection()
        collection.docs["a:b"] = dict(doc)
        reset_caches()
        loop = asyncio.get_event_loop_policy().new_event_loop()
        body, _ = loop.run_until_complete(
            ContentStore(_FakeConfig(collection), "a", "b").get_public()
        )
        assert json.loads(body)["is_stale"] is True

        report = loop.run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        assert report["rebuilt"] == 1, report

    def test_after_the_rebuild_it_is_no_longer_stale(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            needs_rebuild,
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["a:b"] = self._doc(source_hash="drifted")
        reset_caches()
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            rebuild_stale_content(_FakeConfig(collection))
        )
        assert needs_rebuild(collection.docs["a:b"]) is False


class TestEmptyNestedPatchIsANoOp:
    """An empty object for a nested model must not materialise the container."""

    @staticmethod
    def _merge(stored, body):
        from freva_rest.settings_api.core import SettingsStore
        from freva_rest.settings_api.registry import REGISTRY
        from freva_rest.settings_api.schema import UiConfigUpdate

        entry = REGISTRY["ui"]
        store = SettingsStore(
            None, "ui", "default", entry.model, entry.update_model, entry.open_maps
        )
        merged = store._merge(stored, UiConfigUpdate.model_validate(body))
        UiConfig(**merged)
        return merged

    def test_empty_content_ref_does_not_create_one(self) -> None:
        merged = self._merge({}, {"header": {"content": {}}})
        assert "content" not in merged.get("header", {})

    def test_empty_nested_object_leaves_the_document_alone(self) -> None:
        stored = {"header": {"show": False}}
        assert self._merge(stored, {"header": {}})["header"] == {"show": False}

    def test_empty_features_is_still_a_no_op(self) -> None:
        stored = {"features": {"stac": {"enabled": True}}}
        merged = self._merge(stored, {"features": {}})
        assert merged["features"] == {"stac": {"enabled": True}}

    def test_an_existing_content_ref_survives_an_empty_patch(self) -> None:
        stored = {"header": {"content": {"ui_id": "_shared", "content_id": "p"}}}
        merged = self._merge(stored, {"header": {"content": {}}})
        assert merged["header"]["content"] == {"ui_id": "_shared", "content_id": "p"}

    def test_open_maps_keep_their_clear_semantics(self) -> None:
        merged = self._merge({"extra_colors": {"a": "#111111"}}, {"extra_colors": {}})
        assert merged["extra_colors"] == {}


class TestFailedDeleteKeepsLastKnownGood:
    """A delete that matched nothing removed nothing, so it must not strip
    readers of their outage fallback."""

    @staticmethod
    def _seeded():
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        return collection, store, loop

    def test_changed_delete_preserves_lkg(self) -> None:
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            _last_known_good,
        )

        collection, store, loop = self._seeded()
        expected = loop.run_until_complete(store.cas_state())
        collection.docs["default:p"][CAS_TOKEN_FIELD] = "a-newer-generation"
        assert loop.run_until_complete(store.delete(expected=expected)) == "changed"
        assert _last_known_good("content:default:p") is not None

    def test_successful_delete_clears_lkg(self) -> None:
        from freva_rest.settings_api.core import _last_known_good

        _, store, loop = self._seeded()
        expected = loop.run_until_complete(store.cas_state())
        assert loop.run_until_complete(store.delete(expected=expected)) == "deleted"
        assert _last_known_good("content:default:p") is None

    def test_missing_delete_clears_lkg(self) -> None:
        from freva_rest.settings_api.core import _last_known_good

        collection, store, loop = self._seeded()
        expected = loop.run_until_complete(store.cas_state())
        del collection.docs["default:p"]
        assert loop.run_until_complete(store.delete(expected=expected)) == "missing"
        assert _last_known_good("content:default:p") is None


class TestWarningLatchIsBounded:
    def test_it_is_not_an_unbounded_set(self) -> None:
        from cachetools import TTLCache

        from freva_rest.settings_api import core

        assert isinstance(core._WARNED, TTLCache)
        assert core._WARNED.maxsize <= 4096

    def test_many_distinct_ids_do_not_grow_it_without_limit(self) -> None:
        from freva_rest.settings_api.core import _warn_once, reset_caches

        reset_caches()
        for index in range(5000):
            _warn_once(f"content:ui:doc-{index}:format", "broken %s", index)
        from freva_rest.settings_api import core

        assert len(core._WARNED) <= core._WARNED.maxsize

    def test_a_repair_forgets_that_documents_latch(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            ContentStore,
            _warn_once,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        reset_caches()
        _warn_once("content:default:p:format", "broken")
        assert "content:default:p:format" in core._WARNED

        collection = _FakeCollection()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        assert "content:default:p:format" not in core._WARNED


class TestRenderedHtmlHasItsOwnDigest:
    """
    `source_hash` covers the source and the format, which says nothing
    about the html stored beside them.
    """

    @staticmethod
    def _forged(**over):
        """Current metadata in every respect, plus hand-written html."""
        from freva_rest.settings_api.core import source_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        return dict(
            {
                "_id": "a:b",
                "ui_id": "a",
                "content_id": "b",
                "format": "markdown",
                "title": "t",
                "source": "# a",
                "source_hash": source_hash("# a", "markdown"),
                "rendered_html": "<script>alert(1)</script>",
                "renderer_version": RENDERER_FINGERPRINT,
                "revision": 1,
            },
            **over,
        )

    def test_the_digest_is_integrity_only_not_freshness(self) -> None:
        """The digest covers the html alone. Mixing in a renderer identity would
        conflate integrity with freshness, so a digest written by an older
        renderer would fail *integrity* and a title edit would re-render."""
        import hashlib

        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import (
            RENDERER_FINGERPRINT,
            RENDERER_VERSION,
        )

        html = "<h1>a</h1>"
        expected = hashlib.sha256()
        expected.update(b"rendered-html\x00")
        expected.update(html.encode("utf-8"))
        assert rendered_hash(html) == expected.hexdigest()

        # neither renderer identity is an input, so the digest a document was
        # written with keeps verifying across a renderer upgrade
        for identity in (RENDERER_VERSION, RENDERER_FINGERPRINT):
            mixed = hashlib.sha256()
            mixed.update(identity.encode("utf-8"))
            mixed.update(b"\x00")
            mixed.update(html.encode("utf-8"))
            assert rendered_hash(html) != mixed.hexdigest()

    def test_freshness_is_answered_by_the_stored_renderer_identity(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        html = "<h1>a</h1>"
        # integrity intact, renderer behind -> stale, and stale for that reason
        old = self._forged(rendered_html=html, rendered_hash=rendered_hash(html))
        assert needs_rebuild(dict(old, renderer_version="0+ancient")) is True
        assert needs_rebuild(dict(old, renderer_version=RENDERER_FINGERPRINT)) is False

    def test_forged_html_is_reported_stale(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild

        assert needs_rebuild(self._forged()) is True

    def test_a_document_without_the_digest_is_stale(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild
        from freva_rest.settings_api.renderers import render

        doc = self._forged(rendered_html=render("# a", "markdown"))
        assert "rendered_hash" not in doc
        assert needs_rebuild(doc) is True

    def test_forged_html_is_reported_stale_on_a_read(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import ContentStore, reset_caches

        collection = _FakeCollection()
        collection.docs["a:b"] = self._forged()
        reset_caches()
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(
                ContentStore(_FakeConfig(collection), "a", "b").get_public()
            )
        )
        payload = json.loads(body)
        assert payload["is_stale"] is True
        # what it does *not* contain is TestStoredHtmlIsSanitizedOnTheWayOut's
        # subject; the digest's job is only to make the document stale
        assert "script" not in body.decode()

    def test_a_metadata_patch_leaves_forged_html_stale_not_repaired(self) -> None:
        """A metadata patch is not allowed to repair a rendering any more than
        it is allowed to migrate one - both are the rebuild's job. What protects
        the reader is that the forged html never reaches a browser
        unsanitized."""
        import asyncio
        import json

        from freva_rest.settings_api.core import (
            ContentStore,
            needs_rebuild,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["a:b"] = self._forged()
        reset_caches()
        loop = asyncio.get_event_loop_policy().new_event_loop()
        store = ContentStore(_FakeConfig(collection), "a", "b")
        loop.run_until_complete(
            store.patch(ContentSource.model_validate({"title": "renamed"}))
        )
        stored = collection.docs["a:b"]
        assert stored["title"] == "renamed"
        assert stored["rendered_html"] == self._forged()["rendered_html"]
        assert needs_rebuild(stored) is True

        reset_caches()
        body, _ = loop.run_until_complete(store.get_public())
        payload = json.loads(body)
        assert "<script>" not in payload["rendered_html"]
        assert payload["is_stale"] is True

    def test_the_rebuild_picks_it_up(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            needs_rebuild,
            rebuild_stale_content,
            reset_caches,
        )
        from freva_rest.settings_api.renderers import rendered_hash

        collection = _FakeCollection()
        collection.docs["a:b"] = self._forged()
        reset_caches()
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["rebuilt"] == 1, report
        stored = collection.docs["a:b"]
        assert "<script>" not in stored["rendered_html"]
        assert stored["rendered_hash"] == rendered_hash(stored["rendered_html"])
        assert needs_rebuild(stored) is False

    def test_a_write_stamps_the_digest(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            ContentStore(_FakeConfig(collection), "a", "b").patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        stored = collection.docs["a:b"]
        assert stored["rendered_hash"] == rendered_hash(stored["rendered_html"])


class TestUnencodableStringsAreRefused:
    """
    `json.loads` accepts a lone surrogate; no encoder can serialise one.
    That has to be a 422, not a 500 raised out of the middle of a write.
    """

    LONE = "\ud800"

    def test_a_plain_string_is_refused(self) -> None:
        from fastapi import HTTPException

        from freva_rest.settings_api.core import _ensure_encodable

        with pytest.raises(HTTPException) as caught:
            _ensure_encodable(self.LONE, "site_title")
        assert caught.value.status_code == 422
        assert "site_title" in caught.value.detail

    def test_it_descends_into_containers_and_keys(self) -> None:
        from fastapi import HTTPException

        from freva_rest.settings_api.core import _ensure_encodable

        for value in (
            {"a": {"b": [1, self.LONE]}},
            {"a": [{"b": self.LONE}]},
            {self.LONE: "x"},
            [[self.LONE]],
        ):
            with pytest.raises(HTTPException) as caught:
                _ensure_encodable(value)
            assert caught.value.status_code == 422

    def test_encodable_documents_pass(self) -> None:
        from freva_rest.settings_api.core import _ensure_encodable

        _ensure_encodable({"a": ["\U0001f600", {"b": "ä"}], "c": 1, "d": None})

    def test_the_schema_still_lets_one_through_the_extension_union(self) -> None:
        from freva_rest.settings_api.schema import UiConfigUpdate

        with pytest.raises(ValidationError):
            UiConfigUpdate.model_validate({"site_title": self.LONE})
        accepted = UiConfigUpdate.model_validate(
            {"public_extensions": {"a": [self.LONE]}}
        )
        assert accepted.public_extensions == {"a": [self.LONE]}

    def test_a_settings_patch_carrying_one_is_a_422(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import SettingsStore, reset_caches
        from freva_rest.settings_api.registry import REGISTRY
        from freva_rest.settings_api.schema import UiConfigUpdate

        collection = _FakeCollection()
        reset_caches()
        entry = REGISTRY["ui"]
        store = SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )
        with pytest.raises(HTTPException) as caught:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                store.patch(
                    UiConfigUpdate.model_validate(
                        {"public_extensions": {"a": [self.LONE]}}
                    )
                )
            )
        assert caught.value.status_code == 422
        assert not collection.docs

    def test_an_inherited_unencodable_source_is_a_422(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["a:b"] = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "source": self.LONE,
            "rendered_html": "",
            "revision": 1,
        }
        reset_caches()
        with pytest.raises(HTTPException) as caught:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                ContentStore(_FakeConfig(collection), "a", "b").patch(
                    ContentSource.model_validate({"title": "renamed"})
                )
            )
        assert caught.value.status_code == 422
        assert collection.docs["a:b"]["source"] == self.LONE


class TestFailedDeleteStateLookupStillInvalidates:
    """The delete's follow-up 'is it still there?' query can fail. When it does
    the caller gets a 503 - but the cached copy it just failed to remove must
    not survive as if nothing had been attempted."""

    class _BrokenLookup(_FakeCollection):
        """delete_one matches nothing, then the state query fails."""

        def __init__(self) -> None:
            super().__init__()
            self.lookups = 0

        async def find_one(self, predicate, projection=None):
            from pymongo.errors import PyMongoError

            if projection == {"_id": 1}:
                self.lookups += 1
                raise PyMongoError("connection lost")
            return await super().find_one(predicate, projection)

    def _seeded(self):
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = self._BrokenLookup()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        return collection, store, loop

    def test_a_failing_lookup_still_drops_the_fresh_caches(self) -> None:
        from fastapi import HTTPException

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            _body_get,
            _cache_get,
            _last_known_good,
        )

        collection, store, loop = self._seeded()
        expected = loop.run_until_complete(store.cas_state())
        loop.run_until_complete(store.get_public())  # populate both caches
        assert _cache_get("content:default:p") is not None
        assert _body_get("content:default:p") is not None

        collection.docs["default:p"][CAS_TOKEN_FIELD] = "a-newer-generation"
        with pytest.raises(HTTPException) as caught:
            loop.run_until_complete(store.delete(expected=expected))
        assert caught.value.status_code == 503
        assert collection.lookups == 1
        assert _cache_get("content:default:p") is None
        assert _body_get("content:default:p") is None
        assert _last_known_good("content:default:p") is not None
        assert "default:p" in collection.docs


class TestWarningLatchIsRecordScoped:
    """
    A latch belongs to one document. Clearing it by bare prefix takes
    neighbouring documents with it
    """

    def test_a_neighbour_with_a_longer_id_keeps_its_latch(self) -> None:
        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            _warn_forget,
            _warn_once,
            reset_caches,
        )

        reset_caches()
        _warn_once("content:default:home:format", "broken")
        _warn_once("content:default:homepage:format", "broken")
        _warn_forget("content:default:home")
        assert "content:default:home:format" not in core._WARNED
        assert "content:default:homepage:format" in core._WARNED

    def test_a_content_repair_leaves_other_records_alone(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            ContentStore,
            _warn_once,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        reset_caches()
        _warn_once("content:default:home:format", "broken")
        _warn_once("content:default:homepage:format", "broken")
        collection = _FakeCollection()
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            ContentStore(_FakeConfig(collection), "default", "home").patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        assert "content:default:home:format" not in core._WARNED
        assert "content:default:homepage:format" in core._WARNED

    def test_a_settings_write_does_not_clear_every_latch(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            SettingsStore,
            _warn_once,
            reset_caches,
        )
        from freva_rest.settings_api.registry import REGISTRY
        from freva_rest.settings_api.schema import UiConfigUpdate

        collection = _FakeCollection()
        reset_caches()
        _warn_once("content:default:home:format", "broken")
        _warn_once("settings:ui:default:revision", "broken")
        entry = REGISTRY["ui"]
        store = SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.patch(UiConfigUpdate.model_validate({"site_title": "A"}))
        )
        assert "settings:ui:default:revision" not in core._WARNED
        assert "content:default:home:format" in core._WARNED


class TestStoredHtmlIsSanitizedOnTheWayOut:
    """
    `rendered_hash` is an unkeyed digest over public inputs, so a mongo
    writer can compute a valid one for any markup.
    """

    HOSTILE = '<p>ok</p><script>alert(1)</script><img src="x" onerror="alert(1)">'

    @staticmethod
    def _doc(html, *, stamp_digest, format="html-fragment", source="# a", **over):
        from freva_rest.settings_api.core import source_hash
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        doc = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": format,
            "title": "t",
            "source": source,
            "source_hash": source_hash(source, format),
            "rendered_html": html,
            "renderer_version": RENDERER_FINGERPRINT,
            "revision": 1,
        }
        if stamp_digest:
            doc["rendered_hash"] = rendered_hash(html)
        doc.update(over)
        return doc

    @staticmethod
    def _read(doc):
        import asyncio
        import json

        from freva_rest.settings_api.core import ContentStore, reset_caches

        collection = _FakeCollection()
        collection.docs["a:b"] = doc
        reset_caches()
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(
                ContentStore(_FakeConfig(collection), "a", "b").get_public()
            )
        )
        return json.loads(body)

    def test_the_digest_is_forgeable_by_anyone_who_can_write(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild
        from freva_rest.settings_api.renderers import rendered_hash

        doc = self._doc(self.HOSTILE, stamp_digest=True)
        assert doc["rendered_hash"] == rendered_hash(self.HOSTILE)
        assert needs_rebuild(doc) is False

    def test_hostile_html_with_a_valid_digest_is_still_sanitized(self) -> None:
        payload = self._read(self._doc(self.HOSTILE, stamp_digest=True))
        html = payload["rendered_html"]
        assert "<script>" not in html and "alert(1)" not in html
        assert "onerror" not in html
        assert "<p>ok</p>" in html

    def test_hostile_html_without_a_digest_is_also_sanitized(self) -> None:
        payload = self._read(self._doc(self.HOSTILE, stamp_digest=False))
        assert "<script>" not in payload["rendered_html"]
        assert payload["is_stale"] is True

    def test_a_pair_copied_from_another_record_verifies_but_is_sanitized(
        self,
    ) -> None:
        from freva_rest.settings_api.core import needs_rebuild

        borrowed = self._doc(self.HOSTILE, stamp_digest=True, ui_id="a", content_id="b")
        assert needs_rebuild(borrowed) is False
        assert "<script>" not in self._read(borrowed)["rendered_html"]

    def test_legitimate_html_is_returned_byte_identical(self) -> None:
        from freva_rest.settings_api.renderers import render

        for source, fmt in (
            (
                "# Title\n\n*text* and a [link](https://x.example)\n\n- a\n- b\n",
                "markdown",
            ),
            ("Title\n=====\n\nBody with `literal`.\n", "rst"),
            ('<p>hi <a href="https://x.example">there</a></p>', "html-fragment"),
        ):
            html = render(source, fmt)
            doc = self._doc(html, stamp_digest=True, format=fmt, source=source)
            payload = self._read(doc)
            assert payload["rendered_html"] == html, fmt
            assert payload["is_stale"] is False, fmt

    def test_a_document_predating_the_digest_still_serves_its_html(self) -> None:
        from freva_rest.settings_api.renderers import render

        html = render("# Title\n\ntext\n", "markdown")
        payload = self._read(self._doc(html, stamp_digest=False, format="markdown"))
        assert payload["rendered_html"] == html
        assert payload["is_stale"] is True

    def test_a_non_string_html_field_is_served_as_empty(self) -> None:
        payload = self._read(self._doc({"not": "a string"}, stamp_digest=False))
        assert payload["rendered_html"] == ""

    def test_sandbox_html_is_not_served_through_the_public_read_at_all(self) -> None:
        payload = self._read(
            self._doc(
                "<script>alert(1)</script>", stamp_digest=True, format="sandbox-html"
            )
        )
        assert payload["rendered_html"] == ""
        assert payload["is_sandbox"] is True


class TestWriteSideReadsDoNotPopulateCaches:
    """
    A write-side read is a CAS candidate. When the CAS misses, that snapshot
    is known-stale
    """

    @staticmethod
    def _always_stale(collection, base):
        """Move the document on before every CAS, so all retries miss."""
        real_find_one = collection.find_one
        bumps = {"n": 1}

        async def _bump(key, projection=None):
            doc = await real_find_one(key, projection)
            bumps["n"] += 1
            collection.docs["default:p"] = dict(
                base, revision=bumps["n"], source="# moved-on"
            )
            return doc

        collection.find_one = _bump

    def test_an_exhausted_cas_leaves_no_snapshot_behind(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            ContentStore,
            _body_get,
            _cache_get,
            _last_known_good,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        base = {
            "_id": "default:p",
            "ui_id": "default",
            "content_id": "p",
            "format": "markdown",
            "source": "# a",
            "revision": 1,
            CAS_TOKEN_FIELD: "unchanged-token",
        }
        collection.docs["default:p"] = dict(base)
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        self._always_stale(collection, base)

        with pytest.raises(HTTPException) as caught:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                store.patch(ContentSource.model_validate({"title": "T"}))
            )
        assert caught.value.status_code == 409
        assert _cache_get("content:default:p") is None
        assert _body_get("content:default:p") is None
        assert _last_known_good("content:default:p") is None

    def test_the_same_holds_for_a_settings_record(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            SettingsStore,
            _cache_get,
            _last_known_good,
            reset_caches,
        )
        from freva_rest.settings_api.registry import REGISTRY
        from freva_rest.settings_api.schema import UiConfigUpdate

        collection = _FakeCollection()
        base = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "revision": 1,
            CAS_TOKEN_FIELD: "unchanged-token",
        }
        collection.docs["ui:default"] = dict(base)
        reset_caches()
        entry = REGISTRY["ui"]
        store = SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )
        real_find_one = collection.find_one
        bumps = {"n": 1}

        async def _bump(key, projection=None):
            doc = await real_find_one(key, projection)
            bumps["n"] += 1
            collection.docs["ui:default"] = dict(base, revision=bumps["n"])
            return doc

        collection.find_one = _bump
        with pytest.raises(HTTPException) as caught:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                store.patch(UiConfigUpdate.model_validate({"site_title": "T"}))
            )
        assert caught.value.status_code == 409
        assert _cache_get("settings:ui:default") is None
        assert _last_known_good("settings:ui:default") is None

    def test_a_successful_write_still_caches_what_it_stored(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            ContentStore,
            _cache_get,
            _last_known_good,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            ContentStore(_FakeConfig(collection), "default", "p").patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        cached = _cache_get("content:default:p")
        assert cached is not None and cached["source"] == "# a"
        assert _last_known_good("content:default:p") is not None

    def test_a_skipped_rebuild_invalidates_the_generation_it_read(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            ContentStore,
            _body_get,
            _cache_get,
            _last_known_good,
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["a:b"] = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "source": "# a",
            "source_hash": "drifted",
            "rendered_html": "<h1>a</h1>",
            "renderer_version": "0",
            "revision": 1,
            CAS_TOKEN_FIELD: "generation-1",
        }
        reset_caches()
        loop = asyncio.get_event_loop_policy().new_event_loop()
        store = ContentStore(_FakeConfig(collection), "a", "b")
        loop.run_until_complete(store.get_public())
        assert _cache_get("content:a:b") is not None

        real_find = collection.find

        def _find_then_move(*a, **k):
            cursor = real_find(*a, **k)
            collection.docs["a:b"][CAS_TOKEN_FIELD] = "generation-2"
            return cursor

        collection.find = _find_then_move
        report = loop.run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        assert report["skipped"] == 1 and report["rebuilt"] == 0, report
        assert _cache_get("content:a:b") is None
        assert _body_get("content:a:b") is None
        assert _last_known_good("content:a:b") is not None


class TestRebuildResetsTheWarningLatch:
    """A repair is what makes the next warning meaningful again."""

    def test_a_rebuilt_document_can_warn_again(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            _warn_once,
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["a:b"] = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "source": "# a",
            "source_hash": "drifted",
            "rendered_html": "<h1>a</h1>",
            "renderer_version": "0",
            "revision": 1,
        }
        reset_caches()
        _warn_once("content:a:b:untrusted-html", "broken")
        _warn_once("content:a:bb:untrusted-html", "a different document")
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["rebuilt"] == 1, report
        assert "content:a:b:untrusted-html" not in core._WARNED
        assert "content:a:bb:untrusted-html" in core._WARNED


class TestSanitizerIsNotIdempotent:
    """
    Sanitising is not idempotent, and only a counter-example establishes
    that
    """

    PATHOLOGICAL = "<a><ul><div><table></ul></div><a><caption>" * 3799

    def test_a_second_pass_changes_and_grows_the_output(self) -> None:
        from freva_rest.settings_api.sanitizer import sanitize_html

        once = sanitize_html(self.PATHOLOGICAL)
        twice = sanitize_html(once)
        assert twice != once
        assert len(twice.encode()) > len(once.encode())

    def test_the_single_pass_output_fits_the_cap_but_the_real_one_does_not(
        self,
    ) -> None:

        from freva_rest.settings_api.renderers import MAX_RENDERED_BYTES
        from freva_rest.settings_api.sanitizer import (
            sanitize_html,
            stable_sanitize,
        )

        once = len(sanitize_html(self.PATHOLOGICAL).encode())
        settled = len(stable_sanitize(self.PATHOLOGICAL).encode())
        assert once <= MAX_RENDERED_BYTES < settled

    def test_the_write_path_now_refuses_it(self) -> None:
        from freva_rest.settings_api.renderers import render

        with pytest.raises(ValueError, match="larger than"):
            render(self.PATHOLOGICAL, "html-fragment")

    def test_stable_sanitize_returns_an_actual_fixed_point(self) -> None:
        from freva_rest.settings_api.sanitizer import (
            sanitize_html,
            stable_sanitize,
        )

        for source in (
            self.PATHOLOGICAL[:410],
            "<div><p><table><ul></p></table>",
            "<a href='https://x.example'><ul><li>x</a></ul>",
            "<p>plain</p>",
        ):
            settled = stable_sanitize(source)
            assert sanitize_html(settled) == settled, source[:40]

    @pytest.mark.parametrize(
        "source,fmt",
        [
            ("# Title\n\n*text*\n\n- a\n- b\n", "markdown"),
            ("Title\n=====\n\nBody.\n", "rst"),
            ("<div><p><table><ul></p></table>", "html-fragment"),
            ("<p>ok</p><script>alert(1)</script>", "html-fragment"),
        ],
    )
    def test_everything_render_stores_is_a_fixed_point(self, source, fmt) -> None:
        from freva_rest.settings_api.renderers import render
        from freva_rest.settings_api.sanitizer import sanitize_html

        html = render(source, fmt)
        assert sanitize_html(html) == html

    def test_markup_that_will_not_settle_is_refused_not_stored(self) -> None:
        from freva_rest.settings_api import sanitizer

        calls = {"n": 0}

        def _never_settles(html: str) -> str:
            calls["n"] += 1
            return html + "x"

        real = sanitizer.sanitize_html
        sanitizer.sanitize_html = _never_settles
        try:
            with pytest.raises(ValueError, match="does not stabilise"):
                sanitizer.stable_sanitize("<p>a</p>")
        finally:
            sanitizer.sanitize_html = real
        assert calls["n"] == sanitizer.MAX_SANITIZE_PASSES


class TestReadPathSanitizationIsBoundedAndCached:
    """
    Sanitizing on read is only defensible if it is bounded, off the loop,
    and paid once per document rather than once per request.
    """

    @staticmethod
    def _store(doc, collection=None):
        from freva_rest.settings_api.core import ContentStore, reset_caches

        collection = collection or _FakeCollection()
        collection.docs["a:b"] = doc
        reset_caches()
        return ContentStore(_FakeConfig(collection), "a", "b"), collection

    @staticmethod
    def _doc(html, **over):
        from freva_rest.settings_api.core import source_hash
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        return dict(
            {
                "_id": "a:b",
                "ui_id": "a",
                "content_id": "b",
                "format": "html-fragment",
                "title": "t",
                "source": "<p>ok</p>",
                "source_hash": source_hash("<p>ok</p>", "html-fragment"),
                "rendered_html": html,
                "rendered_hash": rendered_hash(html),
                "renderer_version": RENDERER_FINGERPRINT,
                "revision": 1,
            },
            **over,
        )

    def test_oversized_stored_html_is_not_sanitized_at_all(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api import core
        from freva_rest.settings_api.renderers import MAX_RENDERED_BYTES

        calls = {"n": 0}
        real = core.sanitize_html

        def _counting(html: str) -> str:
            calls["n"] += 1
            return real(html)

        core.sanitize_html = _counting
        try:
            store, _ = self._store(self._doc("<a>x</a>" * (MAX_RENDERED_BYTES // 4)))
            body, _ = (
                asyncio.get_event_loop_policy()
                .new_event_loop()
                .run_until_complete(store.get_public())
            )
        finally:
            core.sanitize_html = real
        assert calls["n"] == 0
        payload = json.loads(body)
        assert payload["rendered_html"] == ""
        assert payload["is_stale"] is True

    def test_html_that_expands_past_the_ceiling_is_dropped(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.renderers import MAX_RENDERED_BYTES
        from freva_rest.settings_api.sanitizer import sanitize_html

        stored = sanitize_html(TestSanitizerIsNotIdempotent.PATHOLOGICAL)
        assert len(stored.encode()) <= MAX_RENDERED_BYTES
        store, _ = self._store(self._doc(stored))
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(store.get_public())
        )
        assert json.loads(body)["rendered_html"] == ""

    def test_it_runs_once_per_document_not_once_per_read(self) -> None:
        import asyncio

        from freva_rest.settings_api import core

        calls = {"n": 0}
        real = core.sanitize_html

        def _counting(html: str) -> str:
            calls["n"] += 1
            return real(html)

        core.sanitize_html = _counting
        try:
            store, _ = self._store(self._doc("<p>ok</p>"))
            loop = asyncio.get_event_loop_policy().new_event_loop()
            for _ in range(3):
                loop.run_until_complete(store.get_public())
            public_only = calls["n"]
            for _ in range(3):
                loop.run_until_complete(store.get_admin())
        finally:
            core.sanitize_html = real
        assert public_only == 1
        assert calls["n"] == 1

    def test_the_sanitized_form_is_what_gets_cached(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import _cache_get

        store, _ = self._store(self._doc("<p>ok</p><script>alert(1)</script>"))
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.get_public()
        )
        cached = _cache_get("content:a:b")
        assert cached is not None
        assert "<script>" not in cached["rendered_html"]

    def test_a_write_side_read_keeps_the_raw_html(self) -> None:
        # the CAS predicate and the metadata-reuse check compare against what is
        # actually stored; sanitizing there would make them lie
        import asyncio

        from freva_rest.settings_api.core import reset_caches

        hostile = "<p>ok</p><script>alert(1)</script>"
        store, _ = self._store(self._doc(hostile))
        reset_caches()
        raw = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(store._read_uncached(for_write=True))
        )
        assert raw is not None and raw["rendered_html"] == hostile

    def test_it_does_not_block_the_event_loop(self) -> None:
        import asyncio
        import time

        from freva_rest.settings_api.renderers import MAX_RENDERED_BYTES

        store, _ = self._store(self._doc("<a>x</a>" * (MAX_RENDERED_BYTES // 16)))

        async def main():
            ticks = {"n": 0}
            running = True

            async def heartbeat():
                while running:
                    ticks["n"] += 1
                    await asyncio.sleep(0.001)

            beat = asyncio.create_task(heartbeat())
            await asyncio.sleep(0.01)
            ticks["n"] = 0
            start = time.perf_counter()
            await store.get_public()
            elapsed = time.perf_counter() - start
            running = False
            beat.cancel()
            return ticks["n"], elapsed

        ticks, elapsed = (
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(main())
        )
        assert ticks > 0, f"loop was blocked for {elapsed:.3f}s"


class TestWarningLatchesAreGenerationScoped:
    """A latch must not outlive the generation it described - otherwise a
    repair, or any write by another worker, buys an hour of silence for whatever
    is written next."""

    def test_a_new_generation_reports_itself(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD, _doc_latch

        first = {"ui_id": "a", "content_id": "b", CAS_TOKEN_FIELD: "gen-1"}
        second = {"ui_id": "a", "content_id": "b", CAS_TOKEN_FIELD: "gen-2"}
        assert _doc_latch("content:a:b", first, "untrusted-html") != _doc_latch(
            "content:a:b", second, "untrusted-html"
        )

    def test_the_record_prefix_still_comes_first(self) -> None:
        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            _doc_latch,
            _warn_forget,
            _warn_once,
            reset_caches,
        )

        reset_caches()
        mine = _doc_latch("content:a:b", {CAS_TOKEN_FIELD: "g1"}, "x")
        also_mine = _doc_latch("content:a:b", {CAS_TOKEN_FIELD: "g2"}, "x")
        neighbour = _doc_latch("content:a:bb", {CAS_TOKEN_FIELD: "g1"}, "x")
        for key in (mine, also_mine, neighbour):
            _warn_once(key, "broken")
        _warn_forget("content:a:b")
        assert mine not in core._WARNED and also_mine not in core._WARNED
        assert neighbour in core._WARNED

    def test_a_document_rewritten_out_of_band_warns_again(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            ContentStore,
            reset_caches,
        )

        collection = _FakeCollection()
        doc = TestReadPathSanitizationIsBoundedAndCached._doc(
            "<p>ok</p><script>alert(1)</script>", **{CAS_TOKEN_FIELD: "gen-1"}
        )
        collection.docs["a:b"] = doc
        reset_caches()
        loop = asyncio.get_event_loop_policy().new_event_loop()
        store = ContentStore(_FakeConfig(collection), "a", "b")
        loop.run_until_complete(store.get_public())
        first = [k for k in core._WARNED if "untrusted-html" in k]
        assert len(first) == 1

        collection.docs["a:b"] = TestReadPathSanitizationIsBoundedAndCached._doc(
            "<p>other</p><script>alert(2)</script>", **{CAS_TOKEN_FIELD: "gen-2"}
        )
        reset_caches()
        core._WARNED[first[0]] = True
        loop.run_until_complete(store.get_public())
        assert len([k for k in core._WARNED if "untrusted-html" in k]) == 2

    def test_a_skipped_rebuild_drops_the_latches_it_no_longer_describes(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            _warn_once,
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["a:b"] = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "source": "# a",
            "source_hash": "drifted",
            "rendered_html": "<h1>a</h1>",
            "renderer_version": "0",
            "revision": 1,
            CAS_TOKEN_FIELD: "generation-1",
        }
        reset_caches()
        _warn_once("content:a:b:generation-1:untrusted-html", "broken")
        _warn_once("content:a:bb:generation-1:untrusted-html", "a neighbour")

        real_find = collection.find

        def _find_then_move(*a, **k):
            cursor = real_find(*a, **k)
            collection.docs["a:b"][CAS_TOKEN_FIELD] = "generation-2"
            return cursor

        collection.find = _find_then_move
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["skipped"] == 1, report
        assert "content:a:b:generation-1:untrusted-html" not in core._WARNED
        assert "content:a:bb:generation-1:untrusted-html" in core._WARNED


class TestWarningsSayWhatActuallyHappened:
    def test_warn_once_does_not_append_a_remedy_of_its_own(self, caplog) -> None:
        from freva_rest.settings_api.core import _warn_once, reset_caches

        reset_caches()
        with caplog.at_level("ERROR"):
            _warn_once("k", "Serving the sanitised form instead.")
        assert "Serving defaults instead" not in caplog.text
        assert caplog.text.count("Serving the sanitised form instead.") == 1

    def test_the_settings_callers_kept_the_line_that_was_true_for_them(
        self, caplog
    ) -> None:
        from freva_rest.settings_api.core import SettingsStore, reset_caches
        from freva_rest.settings_api.registry import REGISTRY

        reset_caches()
        entry = REGISTRY["ui"]
        store = SettingsStore(
            None, "ui", "default", entry.model, entry.update_model, entry.open_maps
        )
        with caplog.at_level("ERROR"):
            store._safe_body({"site_title": object()})
        assert "Serving defaults instead" in caplog.text

    def test_the_content_warning_describes_serving_not_defaults(self, caplog) -> None:
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches

        collection = _FakeCollection()
        collection.docs["a:b"] = TestReadPathSanitizationIsBoundedAndCached._doc(
            "<p>ok</p><script>alert(1)</script>"
        )
        reset_caches()
        with caplog.at_level("ERROR"):
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                ContentStore(_FakeConfig(collection), "a", "b").get_public()
            )
        assert "Serving the sanitised form" in caplog.text
        assert "Serving defaults instead" not in caplog.text


class TestDeletePreconditionsShareTheCasSnapshot:
    """
    The internal CAS only covers changes made *during* the delete.
    """

    @staticmethod
    def _content(collection=None):
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = collection or _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        return collection, store, loop

    def test_the_snapshot_carries_the_document_and_the_identity(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        collection, store, loop = self._content()
        doc, expected = loop.run_until_complete(store.cas_snapshot())
        assert doc["_id"] == "default:p"
        assert expected["revision"] == collection.docs["default:p"]["revision"]
        assert (
            expected[CAS_TOKEN_FIELD] == collection.docs["default:p"][CAS_TOKEN_FIELD]
        )

    def test_cas_state_still_answers_the_identity_alone(self) -> None:
        _, store, loop = self._content()
        doc, expected = loop.run_until_complete(store.cas_snapshot())
        assert loop.run_until_complete(store.cas_state()) == expected

    def test_content_offers_both_tags_a_client_can_hold(self) -> None:
        import json

        from freva_rest.settings_api.core import etag_of, serialise

        _, store, loop = self._content()
        doc, _ = loop.run_until_complete(store.cas_snapshot())
        public_tag, admin_tag = loop.run_until_complete(store.read_etags(doc))
        assert public_tag != admin_tag
        body, served = loop.run_until_complete(store.get_public())
        assert served == public_tag
        admin_body, admin_served = loop.run_until_complete(store.get_admin())
        assert admin_served == admin_tag
        assert "source" in json.loads(admin_body)
        assert etag_of(serialise(json.loads(body))) == public_tag

    def test_the_public_tag_is_computed_from_the_sanitized_view(self) -> None:
        collection, store, loop = self._content()
        collection.docs["default:p"][
            "rendered_html"
        ] = "<p>ok</p><script>alert(1)</script>"
        from freva_rest.settings_api.core import reset_caches

        reset_caches()
        doc, _ = loop.run_until_complete(store.cas_snapshot())
        public_tag, _ = loop.run_until_complete(store.read_etags(doc))
        reset_caches()
        _, served = loop.run_until_complete(store.get_public())
        assert public_tag == served

    def test_a_settings_record_offers_the_tag_its_get_hands_out(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import SettingsStore, reset_caches
        from freva_rest.settings_api.registry import REGISTRY
        from freva_rest.settings_api.schema import UiConfigUpdate

        collection = _FakeCollection()
        reset_caches()
        entry = REGISTRY["ui"]
        store = SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(UiConfigUpdate.model_validate({"site_title": "A"}))
        )
        doc, _ = loop.run_until_complete(store.cas_snapshot())
        (tag,) = store.etags_for(doc)
        reset_caches()
        _, served = loop.run_until_complete(store.get())
        assert tag == served

    def test_the_synthesised_default_has_a_tag_even_with_no_document(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import SettingsStore, reset_caches
        from freva_rest.settings_api.registry import REGISTRY

        collection = _FakeCollection()
        reset_caches()
        entry = REGISTRY["ui"]
        store = SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )
        loop = asyncio.get_event_loop_policy().new_event_loop()
        assert loop.run_until_complete(store.cas_snapshot()) is None
        (tag,) = store.etags_for({})
        _, served = loop.run_until_complete(store.get())
        assert tag == served
    def test_a_stale_tag_no_longer_matches_after_a_write(self) -> None:
        from freva_rest.settings_api.core import reset_caches

        collection, store, loop = self._content()
        doc, _ = loop.run_until_complete(store.cas_snapshot())
        stale_public, _ = loop.run_until_complete(store.read_etags(doc))
        from freva_rest.settings_api.schema import ContentSource

        loop.run_until_complete(
            store.patch(ContentSource.model_validate({"title": "moved on"}))
        )
        reset_caches()
        fresh_doc, _ = loop.run_until_complete(store.cas_snapshot())
        fresh_public, _ = loop.run_until_complete(store.read_etags(fresh_doc))
        assert stale_public != fresh_public


class TestPreconditionHelper:
    @staticmethod
    def _check(header, tags):
        from freva_rest.settings_api.core import check_if_match

        check_if_match(header, tags, "thing")

    def test_no_header_is_unconditional(self) -> None:
        self._check(None, ('"a"',))

    def test_a_matching_tag_passes(self) -> None:
        self._check('"a"', ('"a"',))

    def test_either_of_two_tags_passes(self) -> None:
        self._check('"b"', ('"a"', '"b"'))

    def test_a_stale_tag_is_a_412(self) -> None:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as caught:
            self._check('"old"', ('"new"',))
        assert caught.value.status_code == 412

    def test_a_weak_tag_never_matches(self) -> None:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as caught:
            self._check('W/"a"', ('"a"',))
        assert caught.value.status_code == 412

    def test_star_matches_an_existing_representation(self) -> None:
        self._check("*", ('"a"',))


class TestSandboxDeletionIsARevocationBoundary:
    """
    Executable content that has been deleted must stop executing;
    that is not a cache-freshness question.
    """

    @staticmethod
    def _seeded(collection=None):
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = collection or _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "s")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate(
                    {"format": "sandbox-html", "source": "<script>go()</script>"}
                )
            )
        )
        return collection, store, loop

    def test_a_deleted_document_stops_being_served_immediately(self) -> None:
        from fastapi import HTTPException

        collection, store, loop = self._seeded()
        assert "go()" in loop.run_until_complete(store.get_document())
        del collection.docs["default:s"]
        with pytest.raises(HTTPException) as caught:
            loop.run_until_complete(store.get_document())
        assert caught.value.status_code == 404

    def test_it_never_serves_from_last_known_good(self) -> None:
        from fastapi import HTTPException
        from pymongo.errors import PyMongoError

        collection, store, loop = self._seeded()
        loop.run_until_complete(store.get_document())  # warm everything

        async def _down(*a, **k):
            raise PyMongoError("down")

        collection.find_one = _down
        with pytest.raises(HTTPException) as caught:
            loop.run_until_complete(store.get_document())
        assert caught.value.status_code == 503

    def test_an_ordinary_read_still_degrades_gracefully(self) -> None:
        import asyncio
        import json

        from pymongo.errors import PyMongoError

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "page")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate(
                    {"format": "markdown", "source": "# hello"}
                )
            )
        )
        loop.run_until_complete(store.get_public())

        async def _down(*a, **k):
            raise PyMongoError("down")

        collection.find_one = _down
        from freva_rest.settings_api.core import _body_cache, _read_cache

        _read_cache.pop("content:default:page", None)
        _body_cache.pop("content:default:page", None)
        body, _ = loop.run_until_complete(store.get_public())
        assert "hello" in json.loads(body)["rendered_html"]

    def test_the_document_read_populates_no_cache(self) -> None:
        from freva_rest.settings_api.core import _cache_get, reset_caches

        collection, store, loop = self._seeded()
        reset_caches()
        loop.run_until_complete(store.get_document())
        assert _cache_get("content:default:s") is None


class TestKnownAbsentDefaultSurvivesAnOutage:
    """
    `Mongo says there is no override` and `I have never asked` are
    different states, and only the first licenses synthesising a default.
    """

    @staticmethod
    def _store(collection):
        from freva_rest.settings_api.core import SettingsStore, reset_caches
        from freva_rest.settings_api.registry import REGISTRY

        reset_caches()
        entry = REGISTRY["ui"]
        return SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )

    @staticmethod
    def _break(collection):
        from pymongo.errors import PyMongoError

        async def _down(*a, **k):
            raise PyMongoError("down")

        collection.find_one = _down

    def test_a_confirmed_absence_is_recorded(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import _is_absent, _last_known_good

        collection = _FakeCollection()
        store = self._store(collection)
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(store.get())
        assert _is_absent(_last_known_good("settings:ui:default"))

    def test_the_default_stays_available_when_mongo_goes_away(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import _body_cache

        collection = _FakeCollection()
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        body, _ = loop.run_until_complete(store.get())
        _body_cache.pop("settings:ui:default", None)
        self._break(collection)
        again, _ = loop.run_until_complete(store.get())
        assert json.loads(again)["site_title"] == json.loads(body)["site_title"]

    def test_it_is_still_a_503_if_nothing_was_ever_established(self) -> None:
        import asyncio

        from fastapi import HTTPException

        collection = _FakeCollection()
        store = self._store(collection)
        self._break(collection)
        with pytest.raises(HTTPException) as caught:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                store.get()
            )
        assert caught.value.status_code == 503

    def test_a_named_missing_record_stays_a_404_not_a_default(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import (
            SettingsStore,
            _body_cache,
            reset_caches,
        )
        from freva_rest.settings_api.registry import REGISTRY

        collection = _FakeCollection()
        reset_caches()
        entry = REGISTRY["ui"]
        store = SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "waterpark",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )
        loop = asyncio.get_event_loop_policy().new_event_loop()
        for _ in range(2):
            with pytest.raises(HTTPException) as caught:
                loop.run_until_complete(store.get(allow_default=False))
            assert caught.value.status_code == 404
            _body_cache.pop("settings:ui:waterpark", None)
            self._break(collection)
        assert caught.value.status_code == 404

    def test_a_tombstone_never_leaks_into_a_representation(self) -> None:
        import asyncio
        import json

        collection = _FakeCollection()
        store = self._store(collection)
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(store.get())
        )
        assert json.loads(body) == UiConfig().model_dump(by_alias=True)

    def test_a_write_clears_the_tombstone(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.schema import UiConfigUpdate

        collection = _FakeCollection()
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(store.get())
        loop.run_until_complete(
            store.patch(UiConfigUpdate.model_validate({"site_title": "Written"}))
        )
        body, _ = loop.run_until_complete(store.get())
        assert json.loads(body)["site_title"] == "Written"


class TestAnnouncementWindows:
    @staticmethod
    def _announce(**over):
        return dict({"id": "a", "message": "hi"}, **over)

    @pytest.mark.parametrize(
        "value", ["not a date", "2026-13-01T00:00:00Z", "", "2026-05-01"]
    )
    def test_non_rfc3339_is_refused(self, value) -> None:
        with pytest.raises(ValidationError):
            UiConfig(announcements=[self._announce(starts_at=value)])

    def test_a_naive_timestamp_is_refused(self) -> None:
        with pytest.raises(ValidationError) as caught:
            UiConfig(announcements=[self._announce(starts_at="2026-05-01T00:00:00")])
        assert "offset" in str(caught.value)

    def test_a_reverse_interval_is_refused(self) -> None:
        with pytest.raises(ValidationError) as caught:
            UiConfig(
                announcements=[
                    self._announce(
                        starts_at="2026-05-01T00:00:00Z", ends_at="2020-01-01T00:00:00Z"
                    )
                ]
            )
        assert "before it starts" in str(caught.value)

    def test_the_comparison_is_on_instants_not_strings(self) -> None:
        UiConfig(
            announcements=[
                self._announce(
                    starts_at="2026-01-01T00:00:00+02:00",
                    ends_at="2026-01-01T01:00:00+00:00",
                )
            ]
        )

    def test_one_bound_alone_is_fine(self) -> None:
        UiConfig(announcements=[self._announce(starts_at="2026-05-01T00:00:00Z")])
        UiConfig(announcements=[self._announce(ends_at="2026-05-01T00:00:00Z")])

    def test_z_is_normalised_to_one_spelling(self) -> None:
        stored = UiConfig(
            announcements=[self._announce(starts_at="2026-05-01T09:00:00Z")]
        ).announcements[0]
        assert stored.starts_at == "2026-05-01T09:00:00+00:00"

    def test_openapi_advertises_date_time(self) -> None:
        from freva_rest.settings_api.schema import Announcement

        prop = Announcement.model_json_schema()["properties"]["starts_at"]
        assert any(
            option.get("format") == "date-time" for option in prop.get("anyOf", [])
        )


class TestManifestCrossFieldRules:
    """One of the two accepted states is an error, the other a documented
    feature."""

    def test_a_search_block_must_target_the_databrowser(self) -> None:
        with pytest.raises(ValidationError) as caught:
            UiConfig(
                routes=[
                    {
                        "kind": "content",
                        "id": "c1",
                        "path": "/c",
                        "ui_id": "default",
                        "content_id": "a",
                    }
                ],
                landing_blocks=[{"block": "search", "target_route_id": "c1"}],
            )
        assert "must target a feature route" in str(caught.value)

    def test_another_feature_is_not_close_enough(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(
                routes=[
                    {"kind": "feature", "id": "s", "path": "/s", "feature": "stac"}
                ],
                landing_blocks=[{"block": "search", "target_route_id": "s"}],
            )

    def test_the_databrowser_route_is_accepted(self) -> None:
        UiConfig(
            routes=[
                {"kind": "feature", "id": "db", "path": "/s", "feature": "databrowser"}
            ],
            landing_blocks=[{"block": "search", "target_route_id": "db"}],
        )

    def test_a_route_for_a_disabled_feature_stays_legal(self) -> None:
        # deliberate: this is how a rollout is staged. The client obligation -
        # do not expose it while disabled - is documented, not enforced here.
        config = UiConfig(
            routes=[{"kind": "feature", "id": "s", "path": "/s", "feature": "stac"}],
            features={"stac": {"enabled": False}},
        )
        assert config.features.stac.enabled is False

    def test_an_unknown_target_is_still_reported_as_unknown(self) -> None:
        with pytest.raises(ValidationError) as caught:
            UiConfig(landing_blocks=[{"block": "search", "target_route_id": "nope"}])
        assert "unknown route ids" in str(caught.value)


class TestFormSchemaIsPatchable:
    def test_the_read_schema_marks_the_unpatchable_field(self) -> None:
        prop = UiConfig.model_json_schema(by_alias=True)["properties"]["schemaVersion"]
        assert prop.get("readOnly") is True

    def test_the_update_schema_does_not_contain_it_at_all(self) -> None:
        from freva_rest.settings_api.schema import UiConfigUpdate

        assert "schemaVersion" not in UiConfigUpdate.model_json_schema()["properties"]
        with pytest.raises(ValidationError):
            UiConfigUpdate.model_validate({"schemaVersion": 1})

    def test_every_update_field_is_actually_patchable(self) -> None:
        from freva_rest.settings_api.schema import UiConfigUpdate

        for name in UiConfigUpdate.model_json_schema()["properties"]:
            UiConfigUpdate.model_validate({name: None})


class TestRendererIdentityTracksTheDeployment:
    def test_the_fingerprint_names_every_rendering_dependency(self) -> None:
        from freva_rest.settings_api.sanitizer import (
            RENDERER_FINGERPRINT,
            RENDERER_VERSION,
            RENDERING_DEPENDENCIES,
        )

        assert RENDERER_FINGERPRINT.startswith(RENDERER_VERSION + "+")
        for package in RENDERING_DEPENDENCIES:
            assert f"{package}=" in RENDERER_FINGERPRINT

    def test_it_is_deterministic(self) -> None:
        from freva_rest.settings_api.sanitizer import _dependency_versions

        assert _dependency_versions() == _dependency_versions()

    def test_an_upgrade_inside_the_allowed_range_changes_it(self) -> None:

        from importlib import metadata

        from freva_rest.settings_api import sanitizer

        real = metadata.version

        def _bumped(package: str) -> str:
            return "99.9.9" if package == "nh3" else real(package)

        metadata.version = _bumped
        try:
            assert sanitizer._dependency_versions() != sanitizer.RENDERER_FINGERPRINT
            assert "nh3=99.9.9" in sanitizer._dependency_versions()
        finally:
            metadata.version = real

    def test_a_missing_dependency_is_named_not_crashed_on(self) -> None:
        from importlib import metadata

        from freva_rest.settings_api import sanitizer

        real = metadata.version

        def _absent(package: str) -> str:
            if package == "mistune":
                raise metadata.PackageNotFoundError(package)
            return real(package)

        metadata.version = _absent
        try:
            assert "mistune=absent" in sanitizer._dependency_versions()
        finally:
            metadata.version = real

    def test_a_document_at_the_old_identity_is_stale(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild, source_hash
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        html = "<h1>a</h1>"
        doc = {
            "format": "markdown",
            "source": "# a",
            "source_hash": source_hash("# a", "markdown"),
            "rendered_html": html,
            "rendered_hash": rendered_hash(html),
            "renderer_version": RENDERER_FINGERPRINT,
        }
        assert needs_rebuild(doc) is False
        assert needs_rebuild(dict(doc, renderer_version="2+nh3=0.0.1")) is True


class TestEffectiveChangeDrivesRendering:
    """
    What decides whether the renderer runs is the effective source and
    format, not which keys the request happened to carry.
    """

    @staticmethod
    def _doc(**over):
        from freva_rest.settings_api.core import source_hash
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        html = "<h1>a</h1>"
        return dict(
            {
                "_id": "a:b",
                "ui_id": "a",
                "content_id": "b",
                "format": "markdown",
                "source": "# a",
                "source_hash": source_hash("# a", "markdown"),
                "rendered_html": html,
                "rendered_hash": rendered_hash(html),
                "renderer_version": RENDERER_FINGERPRINT,
                "title": "t",
                "revision": 1,
            },
            **over,
        )

    @staticmethod
    def _patch(doc, body):
        """Apply a patch, counting renderer calls."""
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["a:b"] = doc
        reset_caches()
        calls = {"n": 0}
        real = core.render

        def _counting(source, fmt):
            calls["n"] += 1
            return real(source, fmt)

        core.render = _counting
        try:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                ContentStore(_FakeConfig(collection), "a", "b").patch(
                    ContentSource.model_validate(body)
                )
            )
        finally:
            core.render = real
        return collection.docs["a:b"], calls["n"]

    def test_resending_the_same_source_and_format_renders_nothing(self) -> None:
        before = self._doc()
        stored, calls = self._patch(
            dict(before),
            {"format": "markdown", "source": "# a", "title": "Renamed"},
        )
        assert calls == 0
        assert stored["title"] == "Renamed"
        for field in ("rendered_html", "rendered_hash", "renderer_version"):
            assert stored[field] == before[field], field

    def test_a_changed_source_still_renders(self) -> None:
        stored, calls = self._patch(self._doc(), {"source": "# b"})
        assert calls == 1
        assert "b" in stored["rendered_html"]

    def test_a_changed_format_still_renders(self) -> None:
        stored, calls = self._patch(
            self._doc(), {"format": "html-fragment", "source": "<p>x</p>"}
        )
        assert calls == 1

    def test_a_stale_document_is_not_migrated_by_a_title_edit(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild

        before = self._doc(renderer_version="0+ancient")
        assert needs_rebuild(before) is True
        stored, calls = self._patch(dict(before), {"title": "Renamed"})
        assert calls == 0
        assert stored["renderer_version"] == "0+ancient"
        assert needs_rebuild(stored) is True
        assert stored["title"] == "Renamed"

    def test_a_legacy_document_without_a_digest_is_left_legacy(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild

        before = self._doc()
        before.pop("rendered_hash")
        stored, calls = self._patch(dict(before), {"title": "Renamed"})
        assert calls == 0
        assert "rendered_hash" not in stored
        assert needs_rebuild(stored) is True

    def test_a_metadata_edit_works_when_the_source_no_longer_renders(self) -> None:
        from freva_rest.settings_api.core import source_hash
        from freva_rest.settings_api.renderers import MAX_SOURCE_BYTES

        huge = "x" * (MAX_SOURCE_BYTES + 10)
        before = self._doc(source=huge, source_hash=source_hash(huge, "markdown"))
        stored, calls = self._patch(dict(before), {"title": "Renamed"})
        assert calls == 0
        assert stored["title"] == "Renamed"

    def test_the_same_source_after_an_out_of_band_edit_is_a_change(self) -> None:
        stored, calls = self._patch(self._doc(source="# something else"), {})
        assert calls == 0
        stored, calls = self._patch(self._doc(), {"source": "# something else"})
        assert calls == 1

    def test_the_rebuild_is_what_migrates(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            needs_rebuild,
            rebuild_stale_content,
            reset_caches,
        )
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        collection = _FakeCollection()
        collection.docs["a:b"] = self._doc(renderer_version="0+ancient")
        reset_caches()
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["rebuilt"] == 1, report
        stored = collection.docs["a:b"]
        assert stored["renderer_version"] == RENDERER_FINGERPRINT
        assert needs_rebuild(stored) is False


class TestWritePathCannotServeRawHtml:
    """
    A metadata-only PATCH preserves the stored rendering *in mongo*, and
    that rendering may be html written straight into the database.
    """

    FORGED = "<p>ok</p><script>alert(1)</script>"

    @staticmethod
    def _seeded(html):
        from freva_rest.settings_api.core import (
            ContentStore,
            reset_caches,
            source_hash,
        )
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        collection = _FakeCollection()
        collection.docs["a:b"] = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "html-fragment",
            "source": "<p>ok</p>",
            "source_hash": source_hash("<p>ok</p>", "html-fragment"),
            "rendered_html": html,
            "rendered_hash": rendered_hash(html),
            "renderer_version": RENDERER_FINGERPRINT,
            "title": "t",
            "revision": 1,
        }
        reset_caches()
        return collection, ContentStore(_FakeConfig(collection), "a", "b")

    def _patch(self, store):
        import asyncio

        from freva_rest.settings_api.schema import ContentSource

        loop = asyncio.get_event_loop_policy().new_event_loop()
        return loop, loop.run_until_complete(
            store.patch(ContentSource.model_validate({"title": "renamed"}))
        )

    def test_the_patch_response_itself_is_sanitized(self) -> None:
        _, store = self._seeded(self.FORGED)
        _, (body, _) = self._patch(store)
        assert "<script>" not in body.decode()
        assert "<p>ok</p>" in body.decode()

    def test_the_immediate_cache_hit_get_is_sanitized(self) -> None:
        _, store = self._seeded(self.FORGED)
        loop, _ = self._patch(store)
        body, _ = loop.run_until_complete(store.get_public())
        assert "<script>" not in body.decode()

    def test_the_outage_fallback_is_sanitized(self) -> None:
        from freva_rest.settings_api.core import _last_known_good

        _, store = self._seeded(self.FORGED)
        self._patch(store)
        good = _last_known_good("content:a:b")
        assert good is not None
        assert "<script>" not in good["rendered_html"]

    def test_the_hot_cache_is_sanitized(self) -> None:
        from freva_rest.settings_api.core import _cache_get

        _, store = self._seeded(self.FORGED)
        self._patch(store)
        cached = _cache_get("content:a:b")
        assert cached is not None
        assert "<script>" not in cached["rendered_html"]

    def test_mongo_still_holds_the_raw_state_for_the_rebuild(self) -> None:
        collection, store = self._seeded(self.FORGED)
        self._patch(store)
        assert collection.docs["a:b"]["rendered_html"] == self.FORGED

    def test_the_write_etag_equals_the_next_read_etag(self) -> None:
        _, store = self._seeded(self.FORGED)
        loop, (_, written) = self._patch(store)
        _, read = loop.run_until_complete(store.get_public())
        assert written == read

    def test_oversized_stored_html_is_bounded_on_the_write_path_too(self) -> None:
        from freva_rest.settings_api.renderers import MAX_RENDERED_BYTES

        _, store = self._seeded("<a>x</a>" * (MAX_RENDERED_BYTES // 4))
        _, (body, _) = self._patch(store)
        assert len(body) < MAX_RENDERED_BYTES


class TestMissingSourceIsNeverRenderedAsEmpty:
    """
    An absent source must stay distinguishable from an empty one.
    """

    @staticmethod
    def _damaged():
        from freva_rest.settings_api.core import source_hash
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        html = "<h1>valuable</h1>"
        return {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "source_hash": source_hash("# valuable", "markdown"),
            "rendered_html": html,
            "rendered_hash": rendered_hash(html),
            "renderer_version": RENDERER_FINGERPRINT,
            "title": "t",
            "revision": 1,
        }

    def _patch(self, body):
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["a:b"] = self._damaged()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "a", "b")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        try:
            loop.run_until_complete(store.patch(ContentSource.model_validate(body)))
            return collection, None
        except HTTPException as error:
            return collection, error

    def test_a_metadata_patch_is_refused_not_applied(self) -> None:
        collection, error = self._patch({"title": "renamed"})
        assert error is not None and error.status_code == 422
        assert "no 'source'" in error.detail
        assert collection.docs["a:b"]["rendered_html"] == "<h1>valuable</h1>"
        assert collection.docs["a:b"]["title"] == "t"

    def test_an_empty_patch_is_refused_too(self) -> None:
        collection, error = self._patch({})
        assert error is not None and error.status_code == 422
        assert collection.docs["a:b"]["rendered_html"] == "<h1>valuable</h1>"

    def test_an_explicit_source_repairs_it(self) -> None:
        collection, error = self._patch({"format": "markdown", "source": "# back"})
        assert error is None
        assert "back" in collection.docs["a:b"]["rendered_html"]
        assert collection.docs["a:b"]["source"] == "# back"

    def test_an_intentional_empty_string_is_still_a_real_source(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "a", "b")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": ""})
            )
        )
        assert collection.docs["a:b"]["source"] == ""
        loop.run_until_complete(
            store.patch(ContentSource.model_validate({"title": "fine"}))
        )
        assert collection.docs["a:b"]["title"] == "fine"

    def test_the_rebuild_counts_it_failed_and_changes_nothing(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["a:b"] = self._damaged()
        before = dict(collection.docs["a:b"])
        reset_caches()
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["failed"] == 1 and report["rebuilt"] == 0
        assert collection.docs["a:b"] == before


class TestDeleteGuardOrder:
    """
    A referenced document is refused whatever ETag the client holds, so that
    check has to come first
    """

    def _call(self, if_match, referrers, monkeypatch):
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api import endpoints
        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )

        async def _referring(ui_id, content_id):
            return list(referrers)

        monkeypatch.setattr(endpoints, "_content_store", lambda u, c: store)
        monkeypatch.setattr(endpoints, "_referring_uis", _referring)
        monkeypatch.setattr(endpoints, "_require_admin", lambda user: None)
        try:
            loop.run_until_complete(
                endpoints.delete_content(
                    ui_id="default",
                    content_id="p",
                    force=False,
                    if_match=if_match,
                    current_user=None,
                )
            )
            return None
        except HTTPException as error:
            return error

    def test_stale_tag_and_a_reference_reports_the_reference(self, monkeypatch) -> None:
        error = self._call('"a-stale-tag"', ["ui/default"], monkeypatch)
        assert error is not None and error.status_code == 409
        assert "referenced" in error.detail

    def test_a_stale_tag_alone_is_still_412(self, monkeypatch) -> None:
        error = self._call('"a-stale-tag"', [], monkeypatch)
        assert error is not None and error.status_code == 412

    def test_a_reference_alone_is_still_409(self, monkeypatch) -> None:
        error = self._call(None, ["ui/default"], monkeypatch)
        assert error is not None and error.status_code == 409

    def test_neither_deletes(self, monkeypatch) -> None:
        assert self._call(None, [], monkeypatch) is None


class TestSnapshotReconcilesLocalCaches:
    """A snapshot that bypasses the cache still *proves* something about it."""

    @staticmethod
    def _store(collection):
        from freva_rest.settings_api.core import ContentStore, reset_caches

        reset_caches()
        return ContentStore(_FakeConfig(collection), "default", "p")

    def _seed(self):
        import asyncio

        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        return collection, store, loop

    def test_a_superseded_cached_copy_is_dropped(self) -> None:
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            _body_get,
            _cache_get,
            _last_known_good,
        )

        collection, store, loop = self._seed()
        loop.run_until_complete(store.get_public())
        assert _cache_get("content:default:p") is not None

        collection.docs["default:p"][CAS_TOKEN_FIELD] = "generation-2"
        collection.docs["default:p"]["revision"] = 99
        loop.run_until_complete(store.cas_snapshot())

        assert _cache_get("content:default:p") is None
        assert _body_get("content:default:p") is None

        assert _last_known_good("content:default:p") is None

    def test_a_matching_snapshot_leaves_the_documents_alone(self) -> None:
        from freva_rest.settings_api.core import _body_get, _cache_get

        _, store, loop = self._seed()
        loop.run_until_complete(store.get_public())
        loop.run_until_complete(store.cas_snapshot())
        assert _cache_get("content:default:p") is not None

        assert _body_get("content:default:p") is None

    def test_a_cached_document_that_mongo_no_longer_has_is_dropped(self) -> None:
        from freva_rest.settings_api.core import _cache_get

        collection, store, loop = self._seed()
        loop.run_until_complete(store.get_public())
        del collection.docs["default:p"]
        loop.run_until_complete(store.cas_snapshot())
        assert _cache_get("content:default:p") is None

    def test_the_settings_store_reconciles_too(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            SettingsStore,
            _cache_get,
            reset_caches,
        )
        from freva_rest.settings_api.registry import REGISTRY
        from freva_rest.settings_api.schema import UiConfigUpdate

        collection = _FakeCollection()
        reset_caches()
        entry = REGISTRY["ui"]
        store = SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(UiConfigUpdate.model_validate({"site_title": "A"}))
        )
        loop.run_until_complete(store.get())
        collection.docs["ui:default"][CAS_TOKEN_FIELD] = "generation-2"
        loop.run_until_complete(store.cas_snapshot())
        assert _cache_get("settings:ui:default") is None


class TestResetEstablishesKnownAbsence:
    """After a reset, the very next thing that happens is usually a read."""

    @staticmethod
    def _store(collection):
        from freva_rest.settings_api.core import SettingsStore, reset_caches
        from freva_rest.settings_api.registry import REGISTRY

        reset_caches()
        entry = REGISTRY["ui"]
        return SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )

    def test_note_absent_seeds_the_tombstone(self) -> None:
        from freva_rest.settings_api.core import _is_absent, _last_known_good

        store = self._store(_FakeCollection())
        store.note_absent()
        assert _is_absent(_last_known_good("settings:ui:default"))

    def test_it_clears_an_obsolete_body(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import _body_get
        from freva_rest.settings_api.schema import UiConfigUpdate

        collection = _FakeCollection()
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(UiConfigUpdate.model_validate({"site_title": "Old"}))
        )
        loop.run_until_complete(store.get())
        assert _body_get("settings:ui:default") is not None
        store.note_absent()
        assert _body_get("settings:ui:default") is None

    def test_the_default_survives_an_outage_right_after_a_reset(self) -> None:
        import asyncio
        import json

        from pymongo.errors import PyMongoError

        collection = _FakeCollection()
        store = self._store(collection)
        store.note_absent()

        async def _down(*a, **k):
            raise PyMongoError("down")

        collection.find_one = _down
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(store.get())
        )
        assert json.loads(body)["site_title"] == "Freva"


class TestTombstoneCannotBeForged:
    """An in-band marker key is a marker an out-of-band document can carry."""

    def test_it_is_a_private_type_not_a_field(self) -> None:
        from freva_rest.settings_api.core import _absent_marker, _is_absent

        assert _is_absent(_absent_marker())
        assert _is_absent({"__settings_absent__": True}) is False
        assert _is_absent({}) is False
        assert _is_absent(None) is False

    def test_a_document_carrying_the_old_marker_field_is_a_document(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import SettingsStore, reset_caches
        from freva_rest.settings_api.registry import REGISTRY

        collection = _FakeCollection()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "revision": 1,
            "__settings_absent__": True,
            "site_title": "Still Here",
        }
        reset_caches()
        entry = REGISTRY["ui"]
        store = SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )
        loop = asyncio.get_event_loop_policy().new_event_loop()
        for _ in range(2):
            body, _etag = loop.run_until_complete(store.get())
            assert json.loads(body)["site_title"] == "Still Here"
            from freva_rest.settings_api.core import _body_cache

            _body_cache.pop("settings:ui:default", None)

    def test_a_copy_of_a_tombstone_is_still_a_tombstone(self) -> None:
        from freva_rest.settings_api.core import (
            _absent_marker,
            _copy_doc,
            _is_absent,
        )

        assert _is_absent(_copy_doc(_absent_marker()))


class TestStrictRfc3339Grammar:
    """`fromisoformat` is an ISO 8601 parser, and a version-dependent one."""

    @pytest.mark.parametrize(
        "value",
        [
            "2026-W18-1T00:00:00+00:00",  # week date
            "20260501T090000+0000",  # compact
            "2026-05-01 09:00:00+00:00",  # space separator
            "2026-05-01x09:00:00+00:00",  # any separator at all
            "2026-05-01T09:00:00,5+00:00",  # comma fraction
            "2026-05-01T09+00:00",  # hour only
            "2026-05-01T09:00+00:00",  # no seconds
            "2026-05-01T09:00:00+00:00:30",  # offset with seconds
            "  2026-05-01T09:00:00+00:00  ",  # surrounding whitespace
        ],
    )
    def test_iso8601_that_is_not_rfc3339_is_refused(self, value) -> None:
        with pytest.raises(ValidationError):
            UiConfig(announcements=[{"id": "a", "message": "x", "starts_at": value}])

    def test_a_long_fraction_cannot_shrink_past_the_length_bound(self) -> None:
        # normalisation shortens it, and max_length is evaluated after the
        # validator - so the raw string has to be bounded before parsing
        value = "2026-05-01T09:00:00." + "0" * 400 + "+00:00"
        with pytest.raises(ValidationError):
            UiConfig(announcements=[{"id": "a", "message": "x", "starts_at": value}])

    def test_a_grammatical_but_impossible_date_is_refused(self) -> None:
        for value in ("2026-13-01T00:00:00Z", "2026-02-31T00:00:00Z"):
            with pytest.raises(ValidationError):
                UiConfig(
                    announcements=[{"id": "a", "message": "x", "starts_at": value}]
                )

    @pytest.mark.parametrize(
        "value",
        [
            "2026-05-01T09:00:00Z",
            "2026-05-01t09:00:00z",
            "2026-05-01T09:00:00+02:00",
            "2026-05-01T09:00:00-05:00",
            "2026-05-01T09:00:00.123+00:00",
        ],
    )
    def test_real_rfc3339_is_accepted(self, value) -> None:
        UiConfig(announcements=[{"id": "a", "message": "x", "starts_at": value}])


class TestApiOwnedAndRouteIdentity:
    """What the api owns, and whose identity a response reports."""

    def test_a_stored_schema_version_is_not_authoritative(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import SettingsStore, reset_caches
        from freva_rest.settings_api.registry import REGISTRY
        from freva_rest.settings_api.schema import (
            SCHEMA_VERSION,
            UiConfigUpdate,
        )

        collection = _FakeCollection()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "revision": 1,
            "schema_version": 999,
            "site_title": "X",
        }
        reset_caches()
        entry = REGISTRY["ui"]
        store = SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )
        loop = asyncio.get_event_loop_policy().new_event_loop()
        body, _ = loop.run_until_complete(store.get())
        assert json.loads(body)["schemaVersion"] == SCHEMA_VERSION
        loop.run_until_complete(
            store.patch(UiConfigUpdate.model_validate({"site_title": "Y"}))
        )
        assert "schema_version" not in collection.docs["ui:default"]

    def test_a_response_reports_the_identity_that_was_asked_for(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import (
            ContentStore,
            reset_caches,
            source_hash,
        )
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        html = "<h1>a</h1>"
        collection = _FakeCollection()
        collection.docs["a:b"] = {
            "_id": "a:b",
            # redundant copies, wrong
            "ui_id": "somewhere-else",
            "content_id": "something-else",
            "format": "markdown",
            "source": "# a",
            "source_hash": source_hash("# a", "markdown"),
            "rendered_html": html,
            "rendered_hash": rendered_hash(html),
            "renderer_version": RENDERER_FINGERPRINT,
            "revision": 1,
        }
        reset_caches()
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(
                ContentStore(_FakeConfig(collection), "a", "b").get_public()
            )
        )
        payload = json.loads(body)
        assert payload["ui_id"] == "a"
        assert payload["content_id"] == "b"

    def test_an_absent_identity_does_not_become_the_string_none(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import (
            ContentStore,
            reset_caches,
            source_hash,
        )
        from freva_rest.settings_api.renderers import rendered_hash

        collection = _FakeCollection()
        collection.docs["a:b"] = {
            "_id": "a:b",
            "format": "markdown",
            "source": "# a",
            "source_hash": source_hash("# a", "markdown"),
            "rendered_html": "<h1>a</h1>",
            "rendered_hash": rendered_hash("<h1>a</h1>"),
            "revision": 1,
        }
        reset_caches()
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(
                ContentStore(_FakeConfig(collection), "a", "b").get_public()
            )
        )
        assert json.loads(body)["ui_id"] == "a"

    def test_a_current_document_with_a_broken_identity_is_reported(self) -> None:
        # needs_rebuild only looks at the rendering, so a current rendering must
        # not let a broken identity go unreported
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
            source_hash,
        )
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        html = "<h1>a</h1>"
        collection = _FakeCollection()
        collection.docs["a:b"] = {
            "_id": "a:b",
            "ui_id": None,
            "content_id": "b",
            "format": "markdown",
            "source": "# a",
            "source_hash": source_hash("# a", "markdown"),
            "rendered_html": html,
            "rendered_hash": rendered_hash(html),
            "renderer_version": RENDERER_FINGERPRINT,
            "revision": 1,
        }
        reset_caches()
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["failed"] == 1 and report["skipped"] == 0


class TestPublicResponseDoesNotPublishTheStack:
    def test_the_public_field_carries_the_generation_only(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.sanitizer import (
            RENDERER_FINGERPRINT,
            RENDERER_VERSION,
        )
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "a", "b")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        body, _ = loop.run_until_complete(store.get_public())
        payload = json.loads(body)
        assert payload["renderer_version"] == RENDERER_VERSION
        assert "nh3=" not in body.decode()
        # the stored identity is still the full fingerprint
        assert collection.docs["a:b"]["renderer_version"] == RENDERER_FINGERPRINT

    def test_an_admin_read_still_gets_the_exact_stack(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "a", "b")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        body, _ = loop.run_until_complete(store.get_admin())
        assert json.loads(body)["renderer_fingerprint"] == RENDERER_FINGERPRINT


class TestRemainingHardening:
    def test_the_rebuild_report_says_when_it_stopped_early(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        for index in range(5):
            collection.docs[f"a:{index}"] = {
                "_id": f"a:{index}",
                "ui_id": "a",
                "content_id": str(index),
                "format": "markdown",
                "source": "# a",
                "source_hash": "drifted",
                "rendered_html": "<h1>a</h1>",
                "renderer_version": "0",
                "revision": 1,
            }
        reset_caches()
        original = core.MAX_SCAN_DOCUMENTS
        core.MAX_SCAN_DOCUMENTS = 2
        try:
            report = (
                asyncio.get_event_loop_policy()
                .new_event_loop()
                .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
            )
        finally:
            core.MAX_SCAN_DOCUMENTS = original
        assert report["examined"] == 2
        assert report["truncated"] == 1

    def test_a_metadata_only_sandbox_patch_preserves_its_metadata(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["a:s"] = {
            "_id": "a:s",
            "ui_id": "a",
            "content_id": "s",
            "format": "sandbox-html",
            "source": "<script>go()</script>",
            "source_hash": "an-older-hash",
            "rendered_html": "",
            "rendered_hash": "an-older-digest",
            "renderer_version": "0+ancient",
            "title": "t",
            "revision": 1,
        }
        reset_caches()
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            ContentStore(_FakeConfig(collection), "a", "s").patch(
                ContentSource.model_validate({"title": "renamed"})
            )
        )
        stored = collection.docs["a:s"]
        assert stored["title"] == "renamed"
        assert stored["renderer_version"] == "0+ancient"
        assert stored["source_hash"] == "an-older-hash"

    def test_a_missing_content_document_is_negatively_cached(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import ContentStore, reset_caches

        collection = _FakeCollection()
        reads = {"n": 0}
        real = collection.find_one

        async def _counting(key, projection=None):
            reads["n"] += 1
            return await real(key, projection)

        collection.find_one = _counting
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "a", "nope")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        for _ in range(3):
            with pytest.raises(HTTPException) as caught:
                loop.run_until_complete(store.get_public())
            assert caught.value.status_code == 404
        assert reads["n"] == 1

    def test_a_known_missing_document_stays_a_404_during_an_outage(self) -> None:
        import asyncio

        from fastapi import HTTPException
        from pymongo.errors import PyMongoError

        from freva_rest.settings_api.core import (
            ContentStore,
            _body_cache,
            _read_cache,
            reset_caches,
        )

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "a", "nope")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        with pytest.raises(HTTPException):
            loop.run_until_complete(store.get_public())

        async def _down(*a, **k):
            raise PyMongoError("down")

        collection.find_one = _down
        _read_cache.pop("content:a:nope", None)
        _body_cache.pop("content:a:nope", None)
        with pytest.raises(HTTPException) as caught:
            loop.run_until_complete(store.get_public())
        assert caught.value.status_code == 404  # not a 503

    def test_an_oversized_sandbox_document_is_refused_on_read(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import (
            MAX_SANDBOX_SOURCE_BYTES,
            ContentStore,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["a:s"] = {
            "_id": "a:s",
            "ui_id": "a",
            "content_id": "s",
            "format": "sandbox-html",
            "source": "x" * (MAX_SANDBOX_SOURCE_BYTES + 1),
            "revision": 1,
        }
        reset_caches()
        with pytest.raises(HTTPException) as caught:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                ContentStore(_FakeConfig(collection), "a", "s").get_document()
            )
        assert caught.value.status_code == 422

    @pytest.mark.parametrize("url", ["/admin", "about", "//evil.example", "?x=1"])
    def test_an_external_route_must_be_absolute(self, url) -> None:
        with pytest.raises(ValidationError):
            UiConfig(routes=[{"kind": "external", "id": "e", "url": url}])

    def test_an_absolute_external_route_is_fine(self) -> None:
        UiConfig(routes=[{"kind": "external", "id": "e", "url": "https://x.example/a"}])

    @pytest.mark.parametrize(
        "value,expected", [(1.9, 0), (-3, 0), (True, 0), ("1.9", 0), ("12", 12), (7, 7)]
    )
    def test_revision_repair_is_lossless_or_zero(self, value, expected) -> None:
        from freva_rest.settings_api.core import _as_int

        assert _as_int(value) == expected

    def test_a_del_character_is_not_a_valid_map_key(self) -> None:
        with pytest.raises(ValidationError):
            UiConfig(public_extensions={"bad\x7fkey": "x"})

    def test_plain_text_is_actually_single_line(self) -> None:
        from pydantic import BaseModel

        from freva_rest.settings_api.field_types import PlainText

        class _Model(BaseModel):
            value: PlainText

        _Model(value="one line")
        for bad in ("two\nlines", "carriage\rreturn"):
            with pytest.raises(ValidationError):
                _Model(value=bad)


class TestMissingSourceIsNeverCurrent:
    """A document with no `source` is never current, even when its stored
    `source_hash` agrees with `source_hash("", fmt)`: `needs_rebuild` must
    not default an absent source to `""` and report such a document
    healthy."""

    @staticmethod
    def _looks_current():
        from freva_rest.settings_api.core import source_hash
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        html = "<h1>valuable</h1>"
        return {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "source_hash": source_hash("", "markdown"),
            "rendered_html": html,
            "rendered_hash": rendered_hash(html),
            "renderer_version": RENDERER_FINGERPRINT,
            "revision": 1,
        }

    def test_the_predicate_itself_reports_it(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild

        assert needs_rebuild(self._looks_current()) is True

    def test_a_read_reports_it_stale(self) -> None:
        import asyncio
        import json

        from freva_rest.settings_api.core import ContentStore, reset_caches

        collection = _FakeCollection()
        collection.docs["a:b"] = self._looks_current()
        reset_caches()
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(
                ContentStore(_FakeConfig(collection), "a", "b").get_public()
            )
        )
        assert json.loads(body)["is_stale"] is True

    def test_the_rebuild_counts_it_failed_and_leaves_it_alone(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["a:b"] = self._looks_current()
        before = dict(collection.docs["a:b"])
        reset_caches()
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["examined"] == 1
        assert report["failed"] == 1
        assert report["rebuilt"] == 0
        assert collection.docs["a:b"] == before

    def test_a_non_string_source_is_equally_never_current(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild

        for source in ({"a": 1}, [], 0, None):
            assert needs_rebuild(dict(self._looks_current(), source=source)) is True


class TestReadAndRebuildAgreeOnDrift:
    """
    The read and the rebuild must judge staleness from the same document.
    """

    @staticmethod
    def _drifting():
        from freva_rest.settings_api.core import source_hash
        from freva_rest.settings_api.renderers import rendered_hash
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        forged = "<p>ok</p><script>alert(1)</script>"
        return {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "html-fragment",
            "source": "<p>ok</p>",
            "source_hash": source_hash("<p>ok</p>", "html-fragment"),
            "rendered_html": forged,
            "rendered_hash": rendered_hash(forged),
            "renderer_version": RENDERER_FINGERPRINT,
            "revision": 1,
        }

    def _read(self, collection):
        import asyncio
        import json

        from freva_rest.settings_api.core import ContentStore, reset_caches

        reset_caches()
        body, _ = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(
                ContentStore(_FakeConfig(collection), "a", "b").get_public()
            )
        )
        return json.loads(body)

    def test_the_read_reports_stale(self) -> None:
        collection = _FakeCollection()
        collection.docs["a:b"] = self._drifting()
        payload = self._read(collection)
        assert payload["is_stale"] is True
        assert "<script>" not in payload["rendered_html"]

    def test_the_rebuild_acts_on_what_the_read_reported(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["a:b"] = self._drifting()
        reset_caches()
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["rebuilt"] == 1, report
        assert "<script>" not in collection.docs["a:b"]["rendered_html"]

    def test_and_afterwards_the_read_agrees_it_is_current(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["a:b"] = self._drifting()
        reset_caches()
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            rebuild_stale_content(_FakeConfig(collection))
        )
        assert self._read(collection)["is_stale"] is False

    def test_a_clean_document_is_not_dragged_into_a_rebuild(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            ContentStore,
            rebuild_stale_content,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            ContentStore(_FakeConfig(collection), "a", "b").patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        report = loop.run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        assert report["rebuilt"] == 0 and report["failed"] == 0

    def test_the_verdict_never_reaches_a_response(self) -> None:
        from freva_rest.settings_api.core import STALE_VERDICT_KEY

        collection = _FakeCollection()
        collection.docs["a:b"] = self._drifting()
        assert STALE_VERDICT_KEY not in self._read(collection)


class TestReconciliationCoversEveryLocalEntry:
    """The read cache expires in two seconds"""

    def _seeded(self):
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        loop.run_until_complete(store.get_public())
        return collection, store, loop

    def test_the_expired_read_cache_does_not_end_the_check(self) -> None:
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            _body_get,
            _last_known_good,
            _read_cache,
        )

        collection, store, loop = self._seeded()
        collection.docs["default:p"][CAS_TOKEN_FIELD] = "generation-2"
        _read_cache.pop("content:default:p", None)
        loop.run_until_complete(store.cas_snapshot())
        assert _body_get("content:default:p") is None
        assert _last_known_good("content:default:p") is None

    def test_a_stale_lkg_alone_is_enough_to_invalidate(self) -> None:
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            _body_cache,
            _last_known_good,
            _read_cache,
        )

        collection, store, loop = self._seeded()
        collection.docs["default:p"][CAS_TOKEN_FIELD] = "generation-2"
        _read_cache.pop("content:default:p", None)
        _body_cache.pop("content:default:p", None)
        loop.run_until_complete(store.cas_snapshot())
        assert _last_known_good("content:default:p") is None

    def test_a_current_snapshot_keeps_the_documents_but_not_the_body(self) -> None:
        from freva_rest.settings_api.core import _body_get, _last_known_good

        _, store, loop = self._seeded()
        loop.run_until_complete(store.cas_snapshot())
        assert _last_known_good("content:default:p") is not None
        assert _body_get("content:default:p") is None


class TestScansAreResumable:
    """
    A cap that always restarts from the beginning of an unsorted collection
    is not a batch mechanism.
    """

    @staticmethod
    def _stale(index):
        return {
            "_id": f"a:{index:03d}",
            "ui_id": "a",
            "content_id": f"{index:03d}",
            "format": "markdown",
            "source": "# a",
            "source_hash": "drifted",
            "rendered_html": "<h1>a</h1>",
            "renderer_version": "0",
            "revision": 1,
        }

    def test_a_second_pass_reaches_documents_the_first_could_not(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            needs_rebuild,
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        for index in range(5):
            collection.docs[f"a:{index:03d}"] = self._stale(index)
        reset_caches()
        loop = asyncio.get_event_loop_policy().new_event_loop()
        original = core.MAX_SCAN_DOCUMENTS
        core.MAX_SCAN_DOCUMENTS = 2
        try:
            seen = []
            after = None
            for _ in range(5):
                report = loop.run_until_complete(
                    rebuild_stale_content(_FakeConfig(collection), after=after)
                )
                seen.append(report["examined"])
                after = report.get("next_after")
                if not report["truncated"]:
                    break
        finally:
            core.MAX_SCAN_DOCUMENTS = original
        assert sum(seen) == 5, seen
        assert all(not needs_rebuild(doc) for doc in collection.docs.values())

    def test_the_cursor_is_reported_only_when_it_stopped_early(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs["a:000"] = self._stale(0)
        reset_caches()
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["truncated"] == 0
        assert "next_after" not in report
        assert core.MAX_SCAN_DOCUMENTS > 1

    def test_the_audit_is_bounded_and_resumable_too(self, monkeypatch) -> None:
        import asyncio

        from freva_rest.settings_api import core, endpoints

        settings = _FakeCollection()
        for index in range(4):
            settings.docs[f"ui:{index:03d}"] = {
                "_id": f"ui:{index:03d}",
                "resource_name": "ui",
                "record_id": f"{index:03d}",
                "content_refs": [{"ui_id": "a", "content_id": "gone"}],
            }

        class _Config:
            mongo_collection_settings = settings
            mongo_collection_ui_contents = _FakeCollection()

        monkeypatch.setattr(endpoints, "server_config", _Config())
        monkeypatch.setattr(core, "MAX_SCAN_DOCUMENTS", 2)
        monkeypatch.setattr(endpoints, "MAX_SCAN_DOCUMENTS", 2)
        loop = asyncio.get_event_loop_policy().new_event_loop()

        problems, after, capped = loop.run_until_complete(endpoints.audit_references())
        assert len(problems) == 2 and after == "ui:001" and capped is False
        more, again, _ = loop.run_until_complete(
            endpoints.audit_references(after=after)
        )
        assert len(more) == 2
        assert again is None or again != after


class TestErrorsAreNotCacheable:
    """
    Every settings error carries the documented no-store cache policy.
    """

    def test_every_settings_error_carries_no_store(self) -> None:
        from freva_rest.settings_api.core import SettingsError

        for status in (404, 409, 412, 422, 503):
            assert (
                SettingsError(status_code=status, detail="x").headers["Cache-Control"]
                == "private, no-store"
            )

    def test_it_is_an_httpexception_so_fastapi_still_handles_it(self) -> None:
        from fastapi import HTTPException

        from freva_rest.settings_api.core import SettingsError

        assert isinstance(SettingsError(status_code=404, detail="x"), HTTPException)

    def test_extra_headers_are_kept(self) -> None:
        from freva_rest.settings_api.core import SettingsError

        headers = SettingsError(
            status_code=412, detail="x", headers={"ETag": '"a"'}
        ).headers
        assert headers["ETag"] == '"a"'
        assert headers["Cache-Control"] == "private, no-store"

    def test_a_missing_content_404_is_raised_with_the_policy(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import ContentStore, reset_caches

        reset_caches()
        with pytest.raises(HTTPException) as caught:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                ContentStore(_FakeConfig(_FakeCollection()), "a", "nope").get_public()
            )
        assert caught.value.status_code == 404
        assert caught.value.headers["Cache-Control"] == "private, no-store"


class TestUpdateSchemaKeepsFormMetadata:
    """
    Asserted as parity rather than field by field, so a new field cannot
    quietly lose its hint.
    """

    @staticmethod
    def _properties():
        from freva_rest.settings_api.schema import UiConfigUpdate

        read = UiConfig.model_json_schema(by_alias=True)["properties"]
        update = UiConfigUpdate.model_json_schema(by_alias=True)["properties"]
        return read, update

    def test_every_shared_field_carries_the_same_widget(self) -> None:
        read, update = self._properties()
        drifted = {
            name: (spec["x-widget"], update[name].get("x-widget"))
            for name, spec in read.items()
            if "x-widget" in spec
            and name in update
            and update[name].get("x-widget") != spec["x-widget"]
        }
        assert drifted == {}

    def test_the_hint_is_on_the_property_not_buried_in_an_anyof(self) -> None:
        _, update = self._properties()
        for name in ("main_color", "homepage_text", "institution_url"):
            assert update[name].get("x-widget"), name

    @pytest.mark.parametrize(
        "name,widget",
        [
            ("public_extensions", "keyvalue"),
            ("institution_url", "url"),
            ("institution_logo", "url"),
            ("favicon", "url"),
            ("docs_url", "url"),
            ("terms_url", "url"),
            ("privacy_url", "url"),
            ("main_color", "colour"),
            ("border_color", "colour"),
            ("hover_color", "colour"),
            ("homepage_text", "textarea"),
            ("extra_colors", "colourmap"),
            ("routes", "routes"),
            ("navigation", "navigation"),
            ("landing_blocks", "blocks"),
            ("announcements", "announcements"),
        ],
    )
    def test_named_fields_carry_their_widget(self, name, widget) -> None:
        _, update = self._properties()
        assert update[name].get("x-widget") == widget


class TestExternalUrlIsParsedNotPrefixMatched:
    @pytest.mark.parametrize(
        "url",
        [
            "https://",
            "https:///path",
            "http://?x=1",
            "https://#frag",
            "https:// x",
            "http:// ",
            "/admin",
            "about",
            "//evil.example",
            "ftp://x.example",
        ],
    )
    def test_a_prefix_is_not_a_url(self, url) -> None:
        with pytest.raises(ValidationError):
            UiConfig(routes=[{"kind": "external", "id": "e", "url": url}])

    @pytest.mark.parametrize(
        "url",
        [
            "https://x.example",
            "http://x.example/page",
            "https://x.example:8443/a?b=1#c",
            "https://user@x.example/p",
        ],
    )
    def test_a_real_url_is_accepted(self, url) -> None:
        UiConfig(routes=[{"kind": "external", "id": "e", "url": url}])


class TestAuditSpeaksOnlyForItsPage:
    """
    A bounded scan cannot describe a deployment, and this endpoint is the
    compensating control for the accepted PATCH/DELETE race
    """

    @staticmethod
    def _deployment(monkeypatch, limit=1):
        from freva_rest.settings_api import core, endpoints

        settings = _FakeCollection()
        settings.docs["ui:000"] = {
            "_id": "ui:000",
            "resource_name": "ui",
            "record_id": "000",
        }
        settings.docs["ui:001"] = {
            "_id": "ui:001",
            "resource_name": "ui",
            "record_id": "001",
            "content_refs": [{"ui_id": "a", "content_id": "gone"}],
        }

        class _Config:
            mongo_collection_settings = settings
            mongo_collection_ui_contents = _FakeCollection()

            def is_admin_user(self, user):
                return True

            admin_token_claims = {"roles": ["admin"]}

        monkeypatch.setattr(endpoints, "server_config", _Config())
        monkeypatch.setattr(core, "MAX_SCAN_DOCUMENTS", limit)
        monkeypatch.setattr(endpoints, "MAX_SCAN_DOCUMENTS", limit)
        return endpoints

    def test_a_truncated_page_does_not_claim_the_deployment(self, monkeypatch) -> None:
        import asyncio

        endpoints = self._deployment(monkeypatch)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        page = loop.run_until_complete(
            endpoints.audit_content_references(after=None, current_user=None)
        )
        assert page["page_consistent"] is True
        assert page["complete"] is False
        assert "next_after" in page

    def test_the_field_that_over_claimed_is_gone(self, monkeypatch) -> None:
        import asyncio

        endpoints = self._deployment(monkeypatch)
        page = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(
                endpoints.audit_content_references(after=None, current_user=None)
            )
        )
        assert "consistent" not in page

    def test_following_the_cursor_finds_the_problem(self, monkeypatch) -> None:
        import asyncio

        endpoints = self._deployment(monkeypatch)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        pages = []
        after = None
        while True:
            page = loop.run_until_complete(
                endpoints.audit_content_references(after=after, current_user=None)
            )
            pages.append(page)
            if page["complete"]:
                break
            after = page["next_after"]
        assert any(page["problems"] for page in pages)
        assert not all(page["page_consistent"] for page in pages)

    def test_an_unbounded_scan_reports_complete(self, monkeypatch) -> None:
        import asyncio

        endpoints = self._deployment(monkeypatch, limit=1000)
        page = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(
                endpoints.audit_content_references(after=None, current_user=None)
            )
        )
        assert page["complete"] is True
        assert page["page_consistent"] is False
        assert "next_after" not in page


class TestTheSerialisedBodyIsNeverAssumedCurrent:
    """
    The document caches carry `(revision, cas_token)`; the body cache
    carries bytes and an etag. There is nothing to compare, so it can only be
    assumed current
    """

    def _seeded(self):
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        return collection, store, loop

    def test_an_obsolete_body_cannot_outlive_an_authoritative_snapshot(self) -> None:
        import json

        from freva_rest.settings_api.core import _body_cache, _body_get

        collection, store, loop = self._seeded()
        loop.run_until_complete(store.get_public())
        from freva_rest.settings_api.schema import ContentSource

        loop.run_until_complete(
            store.patch(ContentSource.model_validate({"source": "# b"}))
        )
        loop.run_until_complete(store.get_public())
        _body_cache["content:default:p"] = (
            json.dumps({"revision": 1}).encode(),
            '"stale"',
        )
        loop.run_until_complete(store.cas_snapshot())
        assert _body_get("content:default:p") is None

    def test_the_next_read_serves_the_current_revision(self) -> None:
        import json

        from freva_rest.settings_api.core import _body_cache
        from freva_rest.settings_api.schema import ContentSource

        collection, store, loop = self._seeded()
        loop.run_until_complete(store.get_public())
        loop.run_until_complete(
            store.patch(ContentSource.model_validate({"source": "# b"}))
        )
        loop.run_until_complete(store.get_public())
        _body_cache["content:default:p"] = (
            json.dumps({"revision": 1}).encode(),
            '"stale"',
        )
        loop.run_until_complete(store.cas_snapshot())
        body, _ = loop.run_until_complete(store.get_public())
        assert json.loads(body)["revision"] == 2

    def test_the_document_caches_are_still_compared_not_dropped(self) -> None:
        from freva_rest.settings_api.core import _cache_get, _last_known_good

        _, store, loop = self._seeded()
        loop.run_until_complete(store.get_public())
        loop.run_until_complete(store.cas_snapshot())
        assert _cache_get("content:default:p") is not None
        assert _last_known_good("content:default:p") is not None


class TestCursorsRespectBsonTypeBracketing:
    """Mongo range queries compare within a single BSON type, so a string
    cursor cannot traverse numeric or ObjectId ids at all."""

    @staticmethod
    def _stale(doc_id, content_id):
        return {
            "_id": doc_id,
            "ui_id": "a",
            "content_id": content_id,
            "format": "markdown",
            "source": "# a",
            "source_hash": "drifted",
            "rendered_html": "<h1>a</h1>",
            "renderer_version": "0",
            "revision": 1,
        }

    def test_a_non_string_id_is_reported_rather_than_unreachable(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs[7] = self._stale(7, "numeric")
        collection.docs["a:z"] = self._stale("a:z", "z")
        reset_caches()
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["rebuilt"] == 1
        assert report["malformed_ids"] == 1
        assert report["failed"] == 0
        assert report["truncated"] == 0

    def test_an_empty_string_cursor_does_not_restart_the_scan(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs[""] = self._stale("", "empty")
        collection.docs["a:z"] = self._stale("a:z", "z")
        reset_caches()
        loop = asyncio.get_event_loop_policy().new_event_loop()
        original = core.MAX_SCAN_DOCUMENTS
        core.MAX_SCAN_DOCUMENTS = 1
        try:
            first = loop.run_until_complete(
                rebuild_stale_content(_FakeConfig(collection))
            )
            assert first["next_after"] == ""
            second = loop.run_until_complete(
                rebuild_stale_content(
                    _FakeConfig(collection), after=first["next_after"]
                )
            )
        finally:
            core.MAX_SCAN_DOCUMENTS = original
        assert second["examined"] == 1
        assert "a:z" in str(collection.docs["a:z"]["_id"])
        from freva_rest.settings_api.core import needs_rebuild

        assert needs_rebuild(collection.docs["a:z"]) is False

    def test_a_truncated_pass_always_yields_a_usable_cursor(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        collection.docs[7] = self._stale(7, "numeric")
        for index in range(3):
            collection.docs[f"a:{index}"] = self._stale(f"a:{index}", str(index))
        reset_caches()
        loop = asyncio.get_event_loop_policy().new_event_loop()
        original = core.MAX_SCAN_DOCUMENTS
        core.MAX_SCAN_DOCUMENTS = 1
        try:
            report = loop.run_until_complete(
                rebuild_stale_content(_FakeConfig(collection))
            )
            assert report["truncated"] == 1
            assert isinstance(report["next_after"], str)
        finally:
            core.MAX_SCAN_DOCUMENTS = original

    def test_the_fake_type_brackets_like_mongo(self) -> None:
        collection = _FakeCollection()
        collection.docs[7] = {"_id": 7}
        collection.docs["a"] = {"_id": "a"}
        rows = collection.find({"_id": {"$gt": "0"}}).__dict__["_rows"]
        assert [row["_id"] for row in rows] == ["a"]


class TestExternalUrlAuthorityIsValidated:
    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com:bad/x",
            "https://example.com:99999/x",
            "https://./x",
            "https://-a-.com/x",
            "https://a..b/x",
            "https://a-.com/x",
            "https://.com/x",
        ],
    )
    def test_a_malformed_authority_is_refused(self, url) -> None:
        with pytest.raises(ValidationError):
            UiConfig(routes=[{"kind": "external", "id": "e", "url": url}])

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/ok",
            "https://x.example:8443/a?b=1#c",
            "https://192.0.2.1/x",
            "https://[2001:db8::1]:8443/x",
            "https://sub.domain.example.org./trailing-dot",
            "https://user@x.example/p",
        ],
    )
    def test_a_valid_authority_is_accepted(self, url) -> None:
        UiConfig(routes=[{"kind": "external", "id": "e", "url": url}])


class TestCarriedVerdictIsNotRecomputed:
    """`dict.get(key, expensive())` evaluates the fallback even when the key
    is present, so a read would re-hash the source and the html to reach a
    verdict already sitting in front of it."""

    def test_the_fallback_is_not_evaluated_when_the_key_is_present(self) -> None:
        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            STALE_VERDICT_KEY,
            ContentStore,
        )

        calls = {"n": 0}
        real = core.needs_rebuild

        def _counting(doc):
            calls["n"] += 1
            return real(doc)

        store = ContentStore(_FakeConfig(_FakeCollection()), "a", "b")
        view = {
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "rendered_html": "<h1>a</h1>",
            STALE_VERDICT_KEY: True,
        }
        core.needs_rebuild = _counting
        try:
            public = store._public(view)
        finally:
            core.needs_rebuild = real
        assert public.is_stale is True
        assert calls["n"] == 0

    def test_it_still_falls_back_when_the_key_is_absent(self) -> None:
        from freva_rest.settings_api.core import ContentStore

        store = ContentStore(_FakeConfig(_FakeCollection()), "a", "b")
        public = store._public(
            {
                "ui_id": "a",
                "content_id": "b",
                "format": "markdown",
                "rendered_html": "<h1>a</h1>",
            }
        )
        assert public.is_stale is True


class TestReconciliationStopsAnInFlightRead:
    """
    Reconciling what is *currently* cached says nothing about a read already
    in flight.
    """

    @staticmethod
    def _orchestrate(mutate):
        """
        Run a read that pauses mid-query while `mutate` happens, then let it
        finish
        """
        import asyncio
        import json

        from freva_rest.settings_api.core import (
            ContentStore,
            _body_get,
            _cache_get,
            _last_known_good,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        async def main():
            collection = _FakeCollection()
            reset_caches()
            store = ContentStore(_FakeConfig(collection), "default", "p")
            await store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
            reset_caches()

            paused = asyncio.Event()
            resume = asyncio.Event()
            real_find_one = collection.find_one

            async def _pause_mid_read(key, projection=None):
                doc = await real_find_one(key, projection)
                paused.set()
                await resume.wait()
                return doc

            collection.find_one = _pause_mid_read
            reader = asyncio.create_task(store.get_public())
            await paused.wait()
            collection.find_one = real_find_one

            await mutate(collection, store)

            resume.set()
            body, _etag = await reader
            return (
                json.loads(body)["revision"],
                _cache_get("content:default:p"),
                _body_get("content:default:p"),
                _last_known_good("content:default:p"),
            )

        return asyncio.run(main())

    def test_a_read_overlapping_a_snapshot_cannot_cache_its_result(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        async def _mutate(collection, store):
            collection.docs["default:p"]["revision"] = 2
            collection.docs["default:p"][CAS_TOKEN_FIELD] = "generation-2"
            await store.cas_snapshot()

        served, cached, body, good = self._orchestrate(_mutate)

        assert served == 1
        assert cached is None
        assert body is None
        assert good is None

    def test_the_snapshot_agreeing_does_not_license_a_stale_fill_either(
        self,
    ) -> None:

        async def _mutate(collection, store):
            await store.cas_snapshot()

        served, cached, body, good = self._orchestrate(_mutate)
        assert served == 1
        assert cached is None and body is None and good is None

    def test_an_ordinary_read_with_no_snapshot_still_caches(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            ContentStore,
            _cache_get,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        store = ContentStore(_FakeConfig(collection), "default", "p")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        reset_caches()
        loop.run_until_complete(store.get_public())
        assert _cache_get("content:default:p") is not None


class TestMalformedIdsAreCountedExactly:
    """Bounding a scan without a continuation, and then adding the partial
    result to `failed`, is worse than not reporting at all."""

    @staticmethod
    def _collection(numeric_ids):
        collection = _FakeCollection()
        for index in range(numeric_ids):
            collection.docs[index] = {
                "_id": index,
                "ui_id": "a",
                "content_id": str(index),
                "format": "markdown",
                "source": "# a",
                "renderer_version": "0",
            }
        return collection

    def test_the_count_is_exact_regardless_of_the_scan_bound(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = self._collection(3)
        reset_caches()
        original = core.MAX_SCAN_DOCUMENTS
        core.MAX_SCAN_DOCUMENTS = 2
        try:
            report = (
                asyncio.get_event_loop_policy()
                .new_event_loop()
                .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
            )
        finally:
            core.MAX_SCAN_DOCUMENTS = original
        assert report["malformed_ids"] == 3

    def test_the_counts_stay_coherent(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = self._collection(3)
        reset_caches()
        report = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        )
        assert report["failed"] <= report["examined"]
        assert report["examined"] == 0
        assert report["malformed_ids"] == 3

    def test_a_clean_collection_reports_zero(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            ContentStore,
            rebuild_stale_content,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        reset_caches()
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            ContentStore(_FakeConfig(collection), "a", "b").patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        report = loop.run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
        assert report["malformed_ids"] == 0

    def test_a_truncated_pass_does_not_report_the_other_domain_yet(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = self._collection(2)
        for index in range(3):
            collection.docs[f"a:{index}"] = {
                "_id": f"a:{index}",
                "ui_id": "a",
                "content_id": str(index),
                "format": "markdown",
                "source": "# a",
                "source_hash": "drifted",
                "rendered_html": "<h1>a</h1>",
                "renderer_version": "0",
                "revision": 1,
            }
        reset_caches()
        original = core.MAX_SCAN_DOCUMENTS
        core.MAX_SCAN_DOCUMENTS = 1
        try:
            report = (
                asyncio.get_event_loop_policy()
                .new_event_loop()
                .run_until_complete(rebuild_stale_content(_FakeConfig(collection)))
            )
        finally:
            core.MAX_SCAN_DOCUMENTS = original
        assert report["truncated"] == 1
        assert "malformed_ids" not in report


class TestExternalUrlUsesABrowserGrammar:
    """A hostname regex cannot tell an invalid IPv4 literal from a DNS name,
    and does not speak IPv6 at all."""

    @pytest.mark.parametrize(
        "url",
        [
            "https://999.999.999.999/x",
            "https://256.0.0.1/x",
            "https://[v1.foo]/x",
            "https://[2001:db8::1%25eth0]/x",
        ],
    )
    def test_browser_invalid_authorities_are_refused(self, url) -> None:
        with pytest.raises(ValidationError):
            UiConfig(routes=[{"kind": "external", "id": "e", "url": url}])

    @pytest.mark.parametrize(
        "url", ["https://./x", "https://-a-.com/x", "https://a..b/x", "https:///path"]
    )
    def test_dns_shapes_the_parser_tolerates_are_still_refused(self, url) -> None:
        with pytest.raises(ValidationError):
            UiConfig(routes=[{"kind": "external", "id": "e", "url": url}])

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/ok",
            "http://x.example",
            "https://x.example:8443/a?b=1#c",
            "https://user@x.example/p",
            "https://192.0.2.1/x",
            "https://[2001:db8::1]:8443/x",
            "https://sub.domain.example.org./trailing-dot",
        ],
    )
    def test_the_intended_fixtures_all_pass(self, url) -> None:
        UiConfig(routes=[{"kind": "external", "id": "e", "url": url}])

    def test_the_stored_value_is_the_string_that_was_sent(self) -> None:
        url = "https://x.example:8443/a?b=1#c"
        config = UiConfig(routes=[{"kind": "external", "id": "e", "url": url}])
        assert config.routes[0].url == url


class TestSingleFlightIsScopedToAGeneration:

    @staticmethod
    def _three_party():
        """old reader pauses -> snapshot bumps the epoch -> late reader starts ->
        old reader resumes. Returns what each party saw."""
        import asyncio
        import json

        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            ContentStore,
            _body_get,
            _cache_get,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        async def main():
            collection = _FakeCollection()
            reset_caches()
            store = ContentStore(_FakeConfig(collection), "default", "p")
            await store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
            reset_caches()

            paused = asyncio.Event()
            resume = asyncio.Event()
            real_find_one = collection.find_one

            async def _pause_mid_read(key, projection=None):
                doc = await real_find_one(key, projection)
                paused.set()
                await resume.wait()
                return doc

            collection.find_one = _pause_mid_read
            old_reader = asyncio.create_task(store.get_public())
            await paused.wait()
            collection.find_one = real_find_one

            collection.docs["default:p"]["revision"] = 2
            collection.docs["default:p"][CAS_TOKEN_FIELD] = "generation-2"
            snapshot = await store.cas_snapshot()

            late_reader = asyncio.create_task(store.get_public())
            await asyncio.sleep(0)
            resume.set()

            old_body, _ = await old_reader
            late_body, _ = await late_reader
            following, _ = await store.get_public()
            cached = _cache_get("content:default:p")
            body = _body_get("content:default:p")
            return {
                "snapshot": snapshot[1]["revision"],
                "old": json.loads(old_body)["revision"],
                "late": json.loads(late_body)["revision"],
                "following": json.loads(following)["revision"],
                "document_cache": (cached or {}).get("revision"),
                "body_cache": json.loads(body[0])["revision"] if body else None,
            }

        return asyncio.run(main())

    def test_a_late_reader_does_not_join_a_superseded_flight(self) -> None:
        seen = self._three_party()
        assert seen["snapshot"] == 2
        assert seen["old"] == 1
        assert seen["late"] == 2

    def test_no_cache_is_left_holding_the_superseded_generation(self) -> None:
        seen = self._three_party()
        assert seen["following"] == 2
        assert seen["document_cache"] == 2
        assert seen["body_cache"] == 2

    def test_readers_in_the_same_generation_still_coalesce(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        async def main():
            collection = _FakeCollection()
            reset_caches()
            store = ContentStore(_FakeConfig(collection), "default", "p")
            await store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
            reset_caches()

            queries = {"n": 0}
            paused = asyncio.Event()
            resume = asyncio.Event()
            real_find_one = collection.find_one

            async def _counting(key, projection=None):
                queries["n"] += 1
                doc = await real_find_one(key, projection)
                paused.set()
                await resume.wait()
                return doc

            collection.find_one = _counting
            first = asyncio.create_task(store.get_public())
            await paused.wait()
            second = asyncio.create_task(store.get_public())
            await asyncio.sleep(0)
            resume.set()
            await first
            await second
            return queries["n"]

        assert asyncio.run(main()) == 1

    def test_the_flight_key_carries_the_epoch(self) -> None:
        from freva_rest.settings_api.core import _current_epoch, _single_flight

        async def main():
            from freva_rest.settings_api import core

            seen = []

            async def _work():
                seen.append(sorted(core._INFLIGHT))
                return None

            await _single_flight(("content:a:b", _current_epoch()), _work)
            return seen[0]

        import asyncio

        key = asyncio.run(main())[0]
        assert isinstance(key, tuple) and len(key) == 2
        assert key[0] == "content:a:b" and isinstance(key[1], int)

    def test_a_read_after_a_delete_does_not_join_a_pre_delete_flight(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import (
            ContentStore,
            _body_get,
            reset_caches,
        )
        from freva_rest.settings_api.schema import ContentSource

        async def main():
            collection = _FakeCollection()
            reset_caches()
            store = ContentStore(_FakeConfig(collection), "default", "p")
            await store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
            reset_caches()

            paused = asyncio.Event()
            resume = asyncio.Event()
            real_find_one = collection.find_one

            async def _pause_mid_read(key, projection=None):
                doc = await real_find_one(key, projection)
                paused.set()
                await resume.wait()
                return doc

            collection.find_one = _pause_mid_read
            old_reader = asyncio.create_task(store.get_public())
            await paused.wait()
            collection.find_one = real_find_one

            expected = await store.cas_state()
            assert await store.delete(expected=expected) == "deleted"

            late_reader = asyncio.create_task(store.get_public())
            await asyncio.sleep(0)
            resume.set()
            await old_reader
            outcome = None
            try:
                await late_reader
            except HTTPException as error:
                outcome = error.status_code
            return outcome, _body_get("content:default:p")

        status, body = asyncio.run(main())
        assert status == 404
        assert body is None


class TestSingleFlightSurvivesLeaderCancellation:
    """
    The shared work must not belong to whichever request arrived first.
    """

    @staticmethod
    def _run(cancel, fail=False):
        """
        Start a leader and a same-generation waiter, cancel one, and report
        what each received, how often the work ran, and what is left behind.
        """
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import _current_epoch, _single_flight

        async def main():
            core._INFLIGHT.clear()
            runs = {"n": 0}
            started = asyncio.Event()

            async def work():
                runs["n"] += 1
                started.set()
                await asyncio.sleep(0.01)
                if fail:
                    raise RuntimeError("boom")
                return {"revision": 1}

            key = ("content:a:b", _current_epoch())
            leader = asyncio.create_task(_single_flight(key, work))
            await started.wait()
            waiter = asyncio.create_task(_single_flight(key, work))
            await asyncio.sleep(0)
            if cancel == "leader":
                leader.cancel()
            elif cancel == "waiter":
                waiter.cancel()

            outcome = {}
            for name, task in (("leader", leader), ("waiter", waiter)):
                try:
                    outcome[name] = await task
                except asyncio.CancelledError:
                    outcome[name] = "cancelled"
                except RuntimeError as error:
                    outcome[name] = f"error:{error}"
            await asyncio.sleep(0)
            return outcome, runs["n"], len(core._INFLIGHT)

        return asyncio.run(main())

    def test_a_cancelled_leader_does_not_cancel_the_waiter(self) -> None:
        outcome, runs, inflight = self._run("leader")
        assert outcome["leader"] == "cancelled"
        assert outcome["waiter"] == {"revision": 1}
        assert runs == 1
        assert inflight == 0

    def test_a_cancelled_waiter_does_not_cancel_the_work(self) -> None:
        outcome, runs, inflight = self._run("waiter")
        assert outcome["waiter"] == "cancelled"
        assert outcome["leader"] == {"revision": 1}
        assert runs == 1
        assert inflight == 0

    def test_the_work_runs_once_when_nobody_is_cancelled(self) -> None:
        outcome, runs, inflight = self._run(None)
        assert outcome["leader"] == outcome["waiter"] == {"revision": 1}
        assert runs == 1
        assert inflight == 0

    def test_a_real_failure_still_reaches_every_caller(self) -> None:
        outcome, runs, inflight = self._run(None, fail=True)
        assert outcome["leader"] == "error:boom"
        assert outcome["waiter"] == "error:boom"
        assert runs == 1
        assert inflight == 0

    def test_a_failure_reaches_the_waiter_even_if_the_leader_left(self) -> None:
        outcome, runs, inflight = self._run("leader", fail=True)
        assert outcome["leader"] == "cancelled"
        assert outcome["waiter"] == "error:boom"
        assert inflight == 0

    def test_the_entry_is_cleaned_when_the_work_completes(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import _current_epoch, _single_flight

        async def main():
            core._INFLIGHT.clear()

            async def work():
                await asyncio.sleep(0)
                return {"revision": 1}

            key = ("content:a:b", _current_epoch())
            await _single_flight(key, work)
            await asyncio.sleep(0)
            return len(core._INFLIGHT)

        assert asyncio.run(main()) == 0


class TestSandboxSourceMustBeText:
    """
    The read path must not run `_as_str` here: it invents content, turning
    a missing source into an empty 200 and a list or dict into its python repr
    """

    @staticmethod
    def _document(**over):
        return dict(
            {
                "_id": "a:s",
                "ui_id": "a",
                "content_id": "s",
                "format": "sandbox-html",
                "revision": 1,
            },
            **over,
        )

    def _get(self, doc):
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import ContentStore, reset_caches

        collection = _FakeCollection()
        collection.docs["a:s"] = doc
        reset_caches()
        try:
            return (
                asyncio.get_event_loop_policy()
                .new_event_loop()
                .run_until_complete(
                    ContentStore(_FakeConfig(collection), "a", "s").get_document()
                ),
                None,
            )
        except HTTPException as error:
            return None, error

    def test_a_missing_source_is_refused(self) -> None:
        served, error = self._get(self._document())
        assert served is None
        assert error is not None and error.status_code == 422

    @pytest.mark.parametrize(
        "source", [None, ["<b>x</b>"], {"a": "<script>x</script>"}, 7, True]
    )
    def test_a_non_string_source_is_refused(self, source) -> None:
        served, error = self._get(self._document(source=source))
        assert served is None
        assert error is not None and error.status_code == 422
        assert "<b>" not in str(error.detail)
        assert "<script>" not in str(error.detail)

    def test_a_valid_source_is_returned_byte_for_byte(self) -> None:
        payload = "<p>hi</p><script>go()</script>\nä"
        served, error = self._get(self._document(source=payload))
        assert error is None
        assert served == payload

    def test_the_size_ceiling_still_runs_after_the_type_check(self) -> None:
        from freva_rest.settings_api.core import MAX_SANDBOX_SOURCE_BYTES

        served, error = self._get(
            self._document(source="x" * (MAX_SANDBOX_SOURCE_BYTES + 1))
        )
        assert served is None
        assert error is not None and error.status_code == 422
        assert "larger than" in str(error.detail)

    def test_the_refusal_is_warned_once_per_generation(self) -> None:
        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD

        self._get(self._document(source=None, **{CAS_TOKEN_FIELD: "gen-1"}))
        assert any("malformed-sandbox-source" in key for key in core._WARNED)

    def test_it_agrees_with_the_write_path(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["a:s"] = self._document(source={"not": "text"})
        reset_caches()
        with pytest.raises(HTTPException) as caught:
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
                ContentStore(_FakeConfig(collection), "a", "s").patch(
                    ContentSource.model_validate({"title": "renamed"})
                )
            )
        assert caught.value.status_code == 422


class TestAbandonedFlightsDoNotLinger:
    """
    A flight nobody is waiting for has no reason to keep a slot that later
    arrivals would join.
    """

    @staticmethod
    def _abandon(callers=2):
        """Start `callers` waiters on work that blocks forever, cancel them all,
        and report the state *without* ever releasing the work."""
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import _current_epoch, _single_flight

        async def main():
            core._INFLIGHT.clear()
            blocked = asyncio.Event()
            started = asyncio.Event()
            runs = {"n": 0}

            async def work():
                runs["n"] += 1
                started.set()
                await blocked.wait()
                return {"revision": 1}

            key = ("content:a:b", _current_epoch())
            tasks = []
            first = asyncio.create_task(_single_flight(key, work))
            tasks.append(first)
            await started.wait()
            for _ in range(callers - 1):
                tasks.append(asyncio.create_task(_single_flight(key, work)))
            await asyncio.sleep(0)

            flight = core._INFLIGHT.get(key)
            for task in tasks:
                task.cancel()
            for task in tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            await asyncio.sleep(0)
            return {
                "entry_present": key in core._INFLIGHT,
                "work_cancelled": flight.task.cancelled() if flight else None,
                "runs": runs["n"],
                "blocked_still_unset": not blocked.is_set(),
            }

        return asyncio.run(main())

    def test_the_entry_goes_as_soon_as_the_last_caller_leaves(self) -> None:
        state = self._abandon()
        assert state["blocked_still_unset"] is True
        assert state["entry_present"] is False

    def test_the_abandoned_work_is_cancelled_not_left_running(self) -> None:
        state = self._abandon()
        assert state["work_cancelled"] is True

    def test_one_caller_leaving_is_the_same_case(self) -> None:
        state = self._abandon(callers=1)
        assert state["entry_present"] is False
        assert state["work_cancelled"] is True

    def test_a_later_request_starts_a_fresh_flight(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import _current_epoch, _single_flight

        async def main():
            core._INFLIGHT.clear()
            blocked = asyncio.Event()
            started = asyncio.Event()
            runs = {"n": 0}

            async def work():
                runs["n"] += 1
                started.set()
                await blocked.wait()
                return {"revision": runs["n"]}

            key = ("content:a:b", _current_epoch())
            abandoned = asyncio.create_task(_single_flight(key, work))
            await started.wait()
            abandoned.cancel()
            try:
                await abandoned
            except asyncio.CancelledError:
                pass
            await asyncio.sleep(0)

            started.clear()
            blocked.set()
            result = await _single_flight(key, work)
            return result, runs["n"]

        result, runs = asyncio.run(main())
        assert result == {"revision": 2}
        assert runs == 2

    def test_a_caller_leaving_while_others_remain_changes_nothing(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import _current_epoch, _single_flight

        async def main():
            core._INFLIGHT.clear()
            release = asyncio.Event()
            started = asyncio.Event()

            async def work():
                started.set()
                await release.wait()
                return {"revision": 1}

            key = ("content:a:b", _current_epoch())
            stayer = asyncio.create_task(_single_flight(key, work))
            await started.wait()
            leaver = asyncio.create_task(_single_flight(key, work))
            await asyncio.sleep(0)
            leaver.cancel()
            try:
                await leaver
            except asyncio.CancelledError:
                pass
            await asyncio.sleep(0)
            present_after_one_left = key in core._INFLIGHT
            release.set()
            result = await stayer
            return present_after_one_left, result

        present, result = asyncio.run(main())
        assert present is True
        assert result == {"revision": 1}

    def test_the_cleanup_cannot_evict_a_replacement_flight(self) -> None:
        import asyncio

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import _Flight, _forget_flight

        async def main():
            core._INFLIGHT.clear()
            key = ("content:a:b", 1)

            async def _noop():
                return None

            first_task = asyncio.ensure_future(_noop())
            second_task = asyncio.ensure_future(_noop())
            await first_task
            await second_task
            first, second = _Flight(first_task), _Flight(second_task)
            core._INFLIGHT[key] = second
            _forget_flight(key, first, first_task)
            survived = core._INFLIGHT.get(key) is second
            _forget_flight(key, second, second_task)
            return survived, key in core._INFLIGHT

        survived, left = asyncio.run(main())
        assert survived is True
        assert left is False


class TestNonFiniteNumbersAreRefusedNotCrashed:
    """
    A body carrying `inf` or `NaN` must be a 422, not a 500.
    """

    BODY = '{"public_extensions": {"a": 1e999}}'
    JSON = {"Content-Type": "application/json"}

    @staticmethod
    def _app(*, guarded: bool):
        """The real update model behind a route with and without the guard."""
        from fastapi import Depends, FastAPI
        from fastapi.testclient import TestClient

        from freva_rest.settings_api.endpoints import (
            reject_unserialisable_body,
        )
        from freva_rest.settings_api.registry import REGISTRY

        update_model = REGISTRY["ui"].update_model
        app = FastAPI()
        guard = [Depends(reject_unserialisable_body)] if guarded else []

        @app.patch("/r", dependencies=guard)
        async def route(
            payload: update_model,
        ) -> dict:
            return {"ok": True}

        return TestClient(app, raise_server_exceptions=False)

    def test_the_unguarded_route_really_does_return_500(self) -> None:
        res = self._app(guarded=False).patch("/r", content=self.BODY, headers=self.JSON)
        assert res.status_code == 500

    def test_the_guarded_route_returns_422(self) -> None:
        res = self._app(guarded=True).patch("/r", content=self.BODY, headers=self.JSON)
        assert res.status_code == 422
        assert "finite" in res.text

    def test_the_refusal_is_not_cacheable(self) -> None:
        res = self._app(guarded=True).patch("/r", content=self.BODY, headers=self.JSON)
        assert res.headers["Cache-Control"] == "private, no-store"

    @pytest.mark.parametrize(
        "body",
        [
            '{"public_extensions": {"a": 1e999}}',
            '{"public_extensions": {"a": -1e999}}',
            '{"public_extensions": {"a": NaN}}',
            '{"public_extensions": {"a": Infinity}}',
            '{"public_extensions": {"a": -Infinity}}',
            '{"public_extensions": {"a": [1e999]}}',
            '{"nested": {"deeper": [{"x": 1e999}]}}',
        ],
    )
    def test_every_spelling_is_caught(self, body: str) -> None:
        res = self._app(guarded=True).patch("/r", content=body, headers=self.JSON)
        assert res.status_code == 422, body

    @pytest.mark.parametrize(
        "body",
        [
            '{"public_extensions": {"a": 1.5}}',
            '{"public_extensions": {"a": -0.0}}',
            '{"public_extensions": {"a": 1e308}}',
            '{"public_extensions": {"a": "1e999"}}',
            '{"public_extensions": {}}',
            "{}",
        ],
    )
    def test_finite_bodies_are_untouched(self, body: str) -> None:
        res = self._app(guarded=True).patch("/r", content=body, headers=self.JSON)
        assert res.status_code == 200, res.text

    def test_a_body_that_is_not_json_still_gets_the_normal_error(self) -> None:
        res = self._app(guarded=True).patch(
            "/r", content="not json at all", headers=self.JSON
        )
        assert res.status_code == 422
        assert "finite" not in res.text

    def test_an_empty_body_is_not_the_guards_problem(self) -> None:
        res = self._app(guarded=True).patch("/r", content="", headers=self.JSON)
        assert res.status_code == 422
        assert "finite" not in res.text

    def test_both_write_routes_carry_the_guard_after_authentication(self) -> None:
        import freva_rest.settings_api.endpoints as endpoints
        from freva_rest.rest import app

        guarded = 0
        for route in app.routes:
            if "PATCH" not in getattr(route, "methods", ()) or (
                "/settings" not in getattr(route, "path", "")
            ):
                continue
            calls = [dep.call for dep in route.dependant.dependencies]
            assert endpoints.reject_unserialisable_body in calls, route.path
            assert calls.index(endpoints.reject_unserialisable_body) > 0, route.path
            guarded += 1
        assert guarded == 2


class TestSettingsMongoOperationsAreBounded:
    """
    `serverSelectionTimeoutMS` bounds *finding* a server, not talking to
    one.
    """

    def test_the_knob_exists_and_is_positive(self) -> None:
        import math

        from freva_rest.settings_api import core

        assert core.MONGO_OP_TIMEOUT > 0
        assert math.isfinite(core.MONGO_OP_TIMEOUT)

    def test_it_is_read_from_the_environment(self) -> None:
        import os
        from unittest import mock

        from freva_rest.settings_api.core import _positive_float

        with mock.patch.dict(os.environ, {"API_SETTINGS_MONGO_TIMEOUT": "0.25"}):
            assert _positive_float("API_SETTINGS_MONGO_TIMEOUT", 5.0) == 0.25
        with mock.patch.dict(os.environ, {"API_SETTINGS_MONGO_TIMEOUT": "nonsense"}):
            assert _positive_float("API_SETTINGS_MONGO_TIMEOUT", 5.0) == 5.0

    def test_the_deadline_is_actually_set_while_the_call_runs(self) -> None:
        import asyncio

        import pymongo._csot as csot

        from freva_rest.settings_api.core import mongo_timeout

        async def main() -> tuple:
            outside = csot.get_timeout()
            with mongo_timeout(1.5):
                await asyncio.sleep(0)
                inside = csot.get_timeout()
            return outside, inside

        outside, inside = asyncio.run(main())
        assert outside is None
        assert inside is not None

    def test_it_does_not_leak_past_the_block(self) -> None:
        import asyncio

        import pymongo._csot as csot

        from freva_rest.settings_api.core import mongo_timeout

        async def main() -> object:
            with mongo_timeout(1.5):
                pass
            return csot.get_timeout()

        assert asyncio.run(main()) is None


class TestMongoTimeoutsReachTheOutagePath:
    """
    A timeout has to land where an outage lands.
    """

    TIMEOUTS = ("ExecutionTimeout", "NetworkTimeout", "ServerSelectionTimeoutError")

    @staticmethod
    def _store(collection):
        from freva_rest.settings_api.core import SettingsStore, reset_caches
        from freva_rest.settings_api.registry import REGISTRY

        reset_caches()
        entry = REGISTRY["ui"]
        return SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )

    @staticmethod
    def _time_out_with(collection, name):
        import pymongo.errors

        error = getattr(pymongo.errors, name)

        async def _slow(*a, **k):
            raise error("timed out")

        collection.find_one = _slow

    @pytest.mark.parametrize("name", TIMEOUTS)
    def test_every_timeout_error_is_a_pymongo_error(self, name: str) -> None:
        import pymongo.errors

        assert issubclass(getattr(pymongo.errors, name), pymongo.errors.PyMongoError)

    @pytest.mark.parametrize("name", TIMEOUTS)
    def test_a_timed_out_read_serves_last_known_good(self, name: str) -> None:
        import asyncio
        import json

        collection = _FakeCollection()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "site_title": "From mongo",
            "revision": 1,
        }
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        body, _ = loop.run_until_complete(store.get())
        assert json.loads(body)["site_title"] == "From mongo"

        from freva_rest.settings_api import core

        core._read_cache.clear()
        core._body_cache.clear()
        self._time_out_with(collection, name)
        body, _ = loop.run_until_complete(store.get())
        assert json.loads(body)["site_title"] == "From mongo", name

    @pytest.mark.parametrize("name", TIMEOUTS)
    def test_a_timed_out_read_with_no_last_known_good_is_503(self, name: str) -> None:
        import asyncio

        from fastapi import HTTPException

        collection = _FakeCollection()
        store = self._store(collection)
        self._time_out_with(collection, name)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        with pytest.raises(HTTPException) as raised:
            loop.run_until_complete(store.get(allow_default=False))
        assert raised.value.status_code == 503, name

    @pytest.mark.parametrize("name", TIMEOUTS)
    def test_a_timed_out_write_is_503_and_never_last_known_good(
        self, name: str
    ) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.schema import UiConfigUpdate

        collection = _FakeCollection()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "site_title": "From mongo",
            "revision": 1,
        }
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(store.get())
        self._time_out_with(collection, name)
        with pytest.raises(HTTPException) as raised:
            loop.run_until_complete(
                store.patch(UiConfigUpdate.model_validate({"site_title": "new"}))
            )
        assert raised.value.status_code == 503, name

    def test_a_timed_out_scan_surfaces_rather_than_reporting_success(self) -> None:
        import asyncio

        import pymongo.errors

        from freva_rest.settings_api.core import bounded_cursor

        class _Cursor:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise pymongo.errors.NetworkTimeout("timed out mid-scan")

        async def main() -> None:
            async for _ in bounded_cursor(_Cursor()):
                pass

        with pytest.raises(pymongo.errors.PyMongoError):
            asyncio.run(main())

    def test_a_healthy_scan_still_yields_every_document(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import bounded_cursor

        class _Cursor:
            def __init__(self, docs):
                self._docs = list(docs)

            def __aiter__(self):
                return self

            async def __anext__(self):
                if not self._docs:
                    raise StopAsyncIteration
                return self._docs.pop(0)

        async def main() -> list:
            return [doc async for doc in bounded_cursor(_Cursor([1, 2, 3]))]

        assert asyncio.run(main()) == [1, 2, 3]

    def test_the_scan_does_not_charge_per_document_work_to_the_budget(self) -> None:
        import asyncio

        import pymongo._csot as csot

        from freva_rest.settings_api.core import bounded_cursor

        class _Cursor:
            def __init__(self):
                self._left = 2

            def __aiter__(self):
                return self

            async def __anext__(self):
                if not self._left:
                    raise StopAsyncIteration
                self._left -= 1
                return {"deadline": csot.get_timeout()}

        async def main() -> list:
            seen = []
            async for doc in bounded_cursor(_Cursor()):
                seen.append((doc["deadline"], csot.get_timeout()))
            return seen

        for during_fetch, between_documents in asyncio.run(main()):
            assert during_fetch is not None
            assert between_documents is None


class TestTombstoneCannotOverwriteAConcurrentWrite:
    """
    The DELETE that observes "no override" must not still be believed once a
    PATCH has landed in the meantime.
    """

    @staticmethod
    def _store(collection):
        from freva_rest.settings_api.core import SettingsStore
        from freva_rest.settings_api.registry import REGISTRY

        entry = REGISTRY["ui"]
        return SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )

    def _run(self):
        """DELETE observes absence, pauses, a PATCH commits, DELETE resumes."""
        import asyncio

        from freva_rest.settings_api.core import reset_caches
        from freva_rest.settings_api.schema import UiConfigUpdate

        reset_caches()
        collection = _FakeCollection()
        delete_store = self._store(collection)
        patch_store = self._store(collection)
        paused = asyncio.Event()
        resume = asyncio.Event()
        real_find_one = collection.find_one

        async def _find_one_then_pause(*a, **kw):
            result = await real_find_one(*a, **kw)
            collection.find_one = real_find_one
            paused.set()
            await resume.wait()
            return result

        async def main():
            collection.find_one = _find_one_then_pause

            async def deleting():
                snapshot = await delete_store.cas_snapshot()
                assert snapshot is None
                delete_store.note_absent()

            task = asyncio.ensure_future(deleting())
            await paused.wait()
            await patch_store.patch(
                UiConfigUpdate.model_validate({"site_title": "Written mid-delete"})
            )
            resume.set()
            await task
            return collection

        loop = asyncio.get_event_loop_policy().new_event_loop()
        return loop, loop.run_until_complete(main())

    def test_mongo_still_holds_the_written_document(self) -> None:
        _, collection = self._run()
        assert collection.docs["ui:default"]["site_title"] == "Written mid-delete"

    def test_the_tombstone_did_not_replace_the_cached_document(self) -> None:
        from freva_rest.settings_api.core import _cache_get, _is_absent

        self._run()
        cached = _cache_get("settings:ui:default")
        assert cached is not None, "the write's cache entry was dropped"
        assert not _is_absent(cached)
        assert cached["site_title"] == "Written mid-delete"

    def test_the_last_known_good_copy_is_the_new_document(self) -> None:
        from freva_rest.settings_api.core import _is_absent, _last_known_good

        self._run()
        good = _last_known_good("settings:ui:default")
        assert good is not None
        assert not _is_absent(good)
        assert good["site_title"] == "Written mid-delete"

    def test_the_next_read_serves_the_new_document(self) -> None:
        import json

        loop, collection = self._run()
        body, _ = loop.run_until_complete(self._store(collection).get())
        assert json.loads(body)["site_title"] == "Written mid-delete"

    def test_a_later_outage_does_not_synthesise_defaults_over_the_override(
        self,
    ) -> None:
        import json

        from pymongo.errors import PyMongoError

        from freva_rest.settings_api import core

        loop, collection = self._run()

        async def _down(*a, **kw):
            raise PyMongoError("mongo is unreachable")

        collection.find_one = _down
        core._read_cache.clear()
        core._body_cache.clear()
        body, _ = loop.run_until_complete(self._store(collection).get())
        assert json.loads(body)["site_title"] == "Written mid-delete"

    def test_the_uncontended_reset_still_records_the_absence(self) -> None:
        import asyncio
        import json

        from pymongo.errors import PyMongoError

        from freva_rest.settings_api.core import (
            _is_absent,
            _last_known_good,
            reset_caches,
        )

        reset_caches()
        collection = _FakeCollection()
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        assert loop.run_until_complete(store.cas_snapshot()) is None
        store.note_absent()
        assert _is_absent(_last_known_good("settings:ui:default"))

        async def _down(*a, **kw):
            raise PyMongoError("mongo is unreachable")

        collection.find_one = _down
        body, _ = loop.run_until_complete(store.get())
        assert json.loads(body)["site_title"] == "Freva"

    def test_a_successful_reset_seeds_the_absence_without_a_second_snapshot(
        self,
    ) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            _is_absent,
            _last_known_good,
            reset_caches,
        )
        from freva_rest.settings_api.schema import UiConfigUpdate

        reset_caches()
        collection = _FakeCollection()
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(UiConfigUpdate.model_validate({"site_title": "Gone soon"}))
        )
        snapshot = loop.run_until_complete(store.cas_snapshot())
        assert snapshot is not None
        outcome = loop.run_until_complete(
            store.delete(expected=snapshot[1], seed_absent=True)
        )
        assert outcome == "deleted"
        assert _is_absent(_last_known_good("settings:ui:default"))

    def test_a_named_record_delete_leaves_no_tombstone(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            SettingsStore,
            _last_known_good,
            reset_caches,
        )
        from freva_rest.settings_api.registry import REGISTRY
        from freva_rest.settings_api.schema import UiConfigUpdate

        reset_caches()
        collection = _FakeCollection()
        entry = REGISTRY["ui"]
        store = SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "named",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(UiConfigUpdate.model_validate({"site_title": "N"}))
        )
        snapshot = loop.run_until_complete(store.cas_snapshot())
        assert snapshot is not None
        loop.run_until_complete(store.delete(expected=snapshot[1], seed_absent=False))
        assert _last_known_good("settings:ui:named") is None


class TestSettingsReadsCoalesce:
    """The settings record is read on every page load, so without coalescing a
    cold cache means one mongo query per concurrent request"""

    @staticmethod
    def _store(collection):
        from freva_rest.settings_api.core import SettingsStore, reset_caches
        from freva_rest.settings_api.registry import REGISTRY

        reset_caches()
        entry = REGISTRY["ui"]
        return SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            "default",
            entry.model,
            entry.update_model,
            entry.open_maps,
        )

    @staticmethod
    def _counting(collection, gate=None):
        """Replace find_one with a counting, optionally blocking, version."""
        calls = []
        real = collection.find_one

        async def _counted(*a, **kw):
            calls.append(1)
            if gate is not None:
                await gate.wait()
            return await real(*a, **kw)

        collection.find_one = _counted
        return calls

    def test_concurrent_misses_run_one_query(self) -> None:
        import asyncio

        collection = _FakeCollection()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "site_title": "Shared",
            "revision": 1,
        }
        store = self._store(collection)

        async def main():
            gate = asyncio.Event()
            calls = self._counting(collection, gate)
            readers = [asyncio.ensure_future(store._read()) for _ in range(8)]
            await asyncio.sleep(0)
            gate.set()
            results = await asyncio.gather(*readers)
            return calls, results

        calls, results = asyncio.run(main())
        assert len(calls) == 1, f"{len(calls)} queries for 8 concurrent readers"
        assert all(r["site_title"] == "Shared" for r in results)

    def test_readers_that_saw_different_generations_do_not_share(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import _current_epoch

        collection = _FakeCollection()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "site_title": "Shared",
            "revision": 1,
        }
        store = self._store(collection)

        async def main():
            gate = asyncio.Event()
            calls = self._counting(collection, gate)
            first = asyncio.ensure_future(store._read(seen=_current_epoch()))
            second = asyncio.ensure_future(store._read(seen=_current_epoch() + 1))
            await asyncio.sleep(0)
            gate.set()
            await asyncio.gather(first, second)
            return calls

        assert len(asyncio.run(main())) == 2

    def test_the_flight_key_is_namespaced_to_this_store(self) -> None:
        from freva_rest.settings_api.core import ContentStore

        settings_key = self._store(_FakeCollection())._cache_key
        content_key = ContentStore(
            _FakeConfig(_FakeCollection()), "ui", "default"
        )._cache_key
        assert settings_key != content_key
        assert settings_key.startswith("settings:")

    def test_cancelling_one_reader_does_not_disturb_the_others(self) -> None:
        import asyncio

        collection = _FakeCollection()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "site_title": "Survives",
            "revision": 1,
        }
        store = self._store(collection)

        async def main():
            gate = asyncio.Event()
            self._counting(collection, gate)
            leader = asyncio.ensure_future(store._read())
            waiter = asyncio.ensure_future(store._read())
            await asyncio.sleep(0)
            leader.cancel()
            gate.set()
            return await waiter

        assert asyncio.run(main())["site_title"] == "Survives"

    def test_a_failure_is_not_memoised(self) -> None:
        import asyncio

        from pymongo.errors import PyMongoError

        collection = _FakeCollection()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "site_title": "Recovered",
            "revision": 1,
        }
        store = self._store(collection)
        real = collection.find_one

        async def _broken(*a, **kw):
            raise PyMongoError("down")

        async def main():
            collection.find_one = _broken
            with pytest.raises(Exception):
                await store._read()
            collection.find_one = real
            return await store._read()

        assert asyncio.run(main())["site_title"] == "Recovered"

    def test_a_failure_reaches_every_waiter(self) -> None:
        import asyncio

        from pymongo.errors import PyMongoError

        collection = _FakeCollection()
        store = self._store(collection)

        async def main():
            gate = asyncio.Event()

            async def _broken(*a, **kw):
                await gate.wait()
                raise PyMongoError("down")

            collection.find_one = _broken
            readers = [asyncio.ensure_future(store._read()) for _ in range(3)]
            await asyncio.sleep(0)
            gate.set()
            return await asyncio.gather(*readers, return_exceptions=True)

        outcomes = asyncio.run(main())
        assert len(outcomes) == 3
        assert all(isinstance(o, Exception) for o in outcomes)

    def test_the_entry_does_not_outlive_the_read(self) -> None:
        import asyncio

        from freva_rest.settings_api import core

        collection = _FakeCollection()
        store = self._store(collection)

        async def main():
            await store._read()
            return dict(core._INFLIGHT)

        assert asyncio.run(main()) == {}

    def test_a_cached_read_never_starts_a_flight(self) -> None:
        import asyncio

        from freva_rest.settings_api import core

        collection = _FakeCollection()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "site_title": "Cached",
            "revision": 1,
        }
        store = self._store(collection)

        async def main():
            await store._read()
            calls = self._counting(collection)
            entered = []
            real_flight = core._single_flight

            async def _spy(key, work):
                entered.append(key)
                return await real_flight(key, work)

            core._single_flight = _spy
            try:
                again = await store._read()
            finally:
                core._single_flight = real_flight
            return calls, entered, again

        calls, entered, again = asyncio.run(main())
        assert calls == []
        assert entered == []
        assert again["site_title"] == "Cached"


class TestValidatorParityBetweenReadAndUpdate:
    """
    A validator that runs on the read model and not on the update model is
    the wrong way round: the update model is the one that sees hostile input.
    """

    @staticmethod
    def _validated_fields(model, name):
        """The field names a named field_validator is attached to."""
        fields = set()
        for field, decorators in model.__pydantic_decorators__.field_validators.items():
            if field == name or getattr(decorators.func, "__name__", "") == name:
                fields.update(decorators.info.fields)
        return fields

    def test_the_url_validator_covers_the_same_fields_on_both_models(self) -> None:
        from freva_rest.settings_api.schema import UiConfig, UiConfigUpdate

        read = self._validated_fields(UiConfig, "_url_scheme")
        update = self._validated_fields(UiConfigUpdate, "_url_scheme")
        assert read, "the read model's validator was not found"
        assert read == update, f"only on the read model: {sorted(read - update)}"

    def test_every_url_typed_field_is_actually_validated(self) -> None:
        from freva_rest.settings_api.schema import UiConfig

        url_fields = {
            name
            for name, spec in UiConfig.model_json_schema(by_alias=True)[
                "properties"
            ].items()
            if spec.get("x-widget") == "url"
        }
        assert url_fields <= self._validated_fields(UiConfig, "_url_scheme")

    @pytest.mark.parametrize(
        "field",
        [
            "institution_url",
            "institution_logo",
            "favicon",
            "docs_url",
            "terms_url",
            "privacy_url",
        ],
    )
    @pytest.mark.parametrize(
        "hostile",
        ["javascript:alert(1)", "data:text/html,<script>alert(1)</script>"],
    )
    def test_the_update_model_refuses_a_dangerous_scheme(
        self, field: str, hostile: str
    ) -> None:
        from freva_rest.settings_api.schema import UiConfigUpdate

        with pytest.raises(ValidationError):
            UiConfigUpdate.model_validate({field: hostile})

    @pytest.mark.parametrize(
        "field",
        [
            "institution_url",
            "institution_logo",
            "favicon",
            "docs_url",
            "terms_url",
            "privacy_url",
        ],
    )
    def test_the_update_model_still_accepts_a_real_url_and_a_reset(
        self, field: str
    ) -> None:
        from freva_rest.settings_api.schema import UiConfigUpdate

        assert (
            getattr(
                UiConfigUpdate.model_validate({field: "https://x.example/a"}), field
            )
            == "https://x.example/a"
        )
        assert getattr(UiConfigUpdate.model_validate({field: None}), field) is None


class TestDeleteCannotTombstoneAConcurrentInsert:
    """
    The absence marker written by `delete(seed_absent=True)` must not
    outlive a write that landed while the delete was in flight.
    """

    NEW = "Inserted mid-delete"

    @staticmethod
    def _store(collection, record_id="default"):
        from freva_rest.settings_api.core import SettingsStore
        from freva_rest.settings_api.registry import REGISTRY

        entry = REGISTRY["ui"]
        return SettingsStore(
            TestSettingsStoreConditionalDelete._SettingsConfig(collection),
            "ui",
            record_id,
            entry.model,
            entry.update_model,
            entry.open_maps,
        )

    def _race(self, pause_on):
        """
        Run the delete, pausing inside `pause_on`; PATCH lands meanwhile.
        """
        import asyncio

        from freva_rest.settings_api.core import reset_caches
        from freva_rest.settings_api.schema import UiConfigUpdate

        reset_caches()
        collection = _FakeCollection()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "site_title": "Original",
            "revision": 1,
        }
        delete_store = self._store(collection)
        patch_store = self._store(collection)
        paused = asyncio.Event()
        resume = asyncio.Event()

        def _pausing(name):
            real = getattr(collection, name)

            async def _wrapped(*a, **kw):
                result = await real(*a, **kw)
                setattr(collection, name, real)
                paused.set()
                await resume.wait()
                return result

            setattr(collection, name, _wrapped)

        async def main():
            if pause_on == "find_one":
                expected = {"revision": 999, "__cas__": "stale"}
                collection.docs.pop("ui:default")
            else:
                snapshot = await delete_store.cas_snapshot()
                assert snapshot is not None
                expected = snapshot[1]
            _pausing(pause_on)

            async def deleting():
                return await delete_store.delete(expected=expected, seed_absent=True)

            task = asyncio.ensure_future(deleting())
            await paused.wait()
            await patch_store.patch(
                UiConfigUpdate.model_validate({"site_title": self.NEW})
            )
            resume.set()
            outcome = await task
            return outcome, collection

        loop = asyncio.get_event_loop_policy().new_event_loop()
        return (loop,) + loop.run_until_complete(main())

    @pytest.mark.parametrize("pause_on", ["delete_one", "find_one"])
    def test_mongo_holds_the_new_generation(self, pause_on: str) -> None:
        _, _outcome, collection = self._race(pause_on)
        assert collection.docs["ui:default"]["site_title"] == self.NEW

    @pytest.mark.parametrize("pause_on", ["delete_one", "find_one"])
    def test_the_read_cache_holds_no_tombstone(self, pause_on: str) -> None:
        from freva_rest.settings_api.core import _cache_get, _is_absent

        self._race(pause_on)
        cached = _cache_get("settings:ui:default")
        assert cached is None or not _is_absent(cached), pause_on

    @pytest.mark.parametrize("pause_on", ["delete_one", "find_one"])
    def test_the_last_known_good_cache_holds_no_tombstone(self, pause_on: str) -> None:
        from freva_rest.settings_api.core import _is_absent, _last_known_good

        self._race(pause_on)
        good = _last_known_good("settings:ui:default")
        assert good is None or not _is_absent(good), pause_on

    @pytest.mark.parametrize("pause_on", ["delete_one", "find_one"])
    def test_the_next_get_returns_the_new_generation(self, pause_on: str) -> None:
        import json

        loop, _outcome, collection = self._race(pause_on)
        body, _ = loop.run_until_complete(self._store(collection).get())
        assert json.loads(body)["site_title"] == self.NEW, pause_on

    @pytest.mark.parametrize("pause_on", ["delete_one", "find_one"])
    def test_a_later_outage_serves_the_override_not_defaults(
        self, pause_on: str
    ) -> None:
        import json

        from pymongo.errors import PyMongoError

        from freva_rest.settings_api import core

        loop, _outcome, collection = self._race(pause_on)
        loop.run_until_complete(self._store(collection).get())
        core._read_cache.clear()
        core._body_cache.clear()

        async def _down(*a, **kw):
            raise PyMongoError("mongo is unreachable")

        collection.find_one = _down
        body, _ = loop.run_until_complete(self._store(collection).get())
        assert json.loads(body)["site_title"] == self.NEW, pause_on

    def test_an_unconditional_delete_reporting_missing_is_guarded_too(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            _is_absent,
            _last_known_good,
            reset_caches,
        )
        from freva_rest.settings_api.schema import UiConfigUpdate

        reset_caches()
        collection = _FakeCollection()  # nothing stored: the delete finds nothing
        delete_store = self._store(collection)
        patch_store = self._store(collection)
        paused = asyncio.Event()
        resume = asyncio.Event()
        real = collection.delete_one

        async def _pausing(*a, **kw):
            result = await real(*a, **kw)
            collection.delete_one = real
            paused.set()
            await resume.wait()
            return result

        async def main():
            collection.delete_one = _pausing
            task = asyncio.ensure_future(
                delete_store.delete(expected=None, seed_absent=True)
            )
            await paused.wait()
            await patch_store.patch(
                UiConfigUpdate.model_validate({"site_title": self.NEW})
            )
            resume.set()
            return await task

        outcome = (
            asyncio.get_event_loop_policy().new_event_loop().run_until_complete(main())
        )
        assert outcome == "missing"
        good = _last_known_good("settings:ui:default")
        assert good is None or not _is_absent(good)

    @pytest.mark.parametrize("pause_on", ["delete_one", "find_one"])
    def test_the_uncontended_delete_still_seeds_the_absence(
        self, pause_on: str
    ) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            _is_absent,
            _last_known_good,
            reset_caches,
        )
        from freva_rest.settings_api.schema import UiConfigUpdate

        reset_caches()
        collection = _FakeCollection()
        store = self._store(collection)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        if pause_on == "find_one":
            expected = {"revision": 999, "__cas__": "stale"}
        else:
            loop.run_until_complete(
                store.patch(UiConfigUpdate.model_validate({"site_title": "Gone soon"}))
            )
            snapshot = loop.run_until_complete(store.cas_snapshot())
            assert snapshot is not None
            expected = snapshot[1]
        loop.run_until_complete(store.delete(expected=expected, seed_absent=True))
        assert _is_absent(_last_known_good("settings:ui:default")), pause_on

    def test_an_unconditional_delete_with_no_contention_seeds_it(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            _is_absent,
            _last_known_good,
            reset_caches,
        )

        reset_caches()
        store = self._store(_FakeCollection())
        outcome = (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(store.delete(expected=None, seed_absent=True))
        )
        assert outcome == "missing"
        assert _is_absent(_last_known_good("settings:ui:default"))

    def test_a_delete_never_seeds_when_not_asked(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import _last_known_good, reset_caches

        reset_caches()
        store = self._store(_FakeCollection(), record_id="named")
        asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
            store.delete(expected=None, seed_absent=False)
        )
        assert _last_known_good("settings:ui:named") is None


class TestTheRebuildWriteIsBounded:
    """
    The rebuild's `update_one` is the one write in the feature that runs
    inside a loop.
    """

    @staticmethod
    def _config(collection):
        class _Config:
            mongo_collection_ui_contents = collection

        return _Config()

    @staticmethod
    def _stale_document():
        return {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "source": "# a",
            "rendered_html": "<p>old</p>",
            "renderer_version": "0+ancient",
            "revision": 1,
        }

    def test_the_write_runs_under_the_operation_deadline(self) -> None:
        import asyncio

        import pymongo._csot as csot

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        reset_caches()
        collection = _FakeCollection()
        collection.docs["a:b"] = self._stale_document()
        seen = []
        real = collection.update_one

        async def _watched(*a, **kw):
            seen.append(csot.get_timeout())
            return await real(*a, **kw)

        collection.update_one = _watched
        report = asyncio.run(rebuild_stale_content(self._config(collection)))
        assert report["rebuilt"] == 1
        assert seen and all(t is not None for t in seen), seen

    @pytest.mark.parametrize(
        "error_name", ["ExecutionTimeout", "NetworkTimeout", "PyMongoError"]
    )
    def test_a_write_failure_propagates_rather_than_being_counted(
        self, error_name: str
    ) -> None:
        import asyncio

        import pymongo.errors

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        reset_caches()
        collection = _FakeCollection()
        collection.docs["a:b"] = self._stale_document()
        error = getattr(pymongo.errors, error_name)

        async def _failing(*a, **kw):
            raise error("write timed out")

        collection.update_one = _failing
        with pytest.raises(pymongo.errors.PyMongoError):
            asyncio.run(rebuild_stale_content(self._config(collection)))

    @pytest.mark.parametrize(
        "error_name", ["ExecutionTimeout", "NetworkTimeout", "PyMongoError"]
    )
    def test_the_endpoint_maps_it_to_503(self, error_name: str) -> None:
        import asyncio

        import pymongo.errors
        from fastapi import HTTPException

        from freva_rest.settings_api import endpoints
        from freva_rest.settings_api.core import reset_caches

        reset_caches()
        collection = _FakeCollection()
        collection.docs["a:b"] = self._stale_document()
        error = getattr(pymongo.errors, error_name)

        async def _failing(*a, **kw):
            raise error("write timed out")

        collection.update_one = _failing

        class _Config:
            mongo_collection_ui_contents = collection

        class _Admin:
            def model_dump(self):
                return {"roles": ["admin"]}

        original_config = endpoints.server_config
        original_admin = endpoints._require_admin
        endpoints.server_config = _Config()
        endpoints._require_admin = lambda _user: None
        try:
            with pytest.raises(HTTPException) as raised:
                asyncio.run(
                    endpoints.rebuild_content(after=None, current_user=_Admin())
                )
        finally:
            endpoints.server_config = original_config
            endpoints._require_admin = original_admin
        assert raised.value.status_code == 503, error_name

    def test_a_damaged_document_is_still_counted_not_raised(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        reset_caches()
        collection = _FakeCollection()
        broken = self._stale_document()
        broken["source"] = {"not": "a string"}
        collection.docs["a:b"] = broken
        report = asyncio.run(rebuild_stale_content(self._config(collection)))
        assert report["failed"] == 1
        assert report["rebuilt"] == 0


class TestMarkdownPluginsRenderAndSurviveSanitisation:
    """
    Golden output for the four enabled mistune plugins.
    """

    @staticmethod
    def _render(source: str) -> str:
        from freva_rest.settings_api.renderers import render

        return render(source, "markdown")

    def test_the_enabled_set_is_exactly_what_was_agreed(self) -> None:
        from freva_rest.settings_api.renderers import MARKDOWN_PLUGINS

        assert MARKDOWN_PLUGINS == ("table", "url", "strikethrough", "def_list")

    def test_a_table_renders(self) -> None:
        html = self._render("| a | b |\n|---|:-:|\n| 1 | 2 |\n")
        for tag in ("<table>", "<thead>", "<tbody>", "<th>a</th>", "<td>1</td>"):
            assert tag in html, html

    def test_table_alignment_style_is_dropped_not_carried(self) -> None:
        html = self._render("| a |\n|:-:|\n| 1 |\n")
        assert "style" not in html
        assert "text-align" not in html

    def test_a_bare_url_becomes_a_link_with_the_forced_rel(self) -> None:
        html = self._render("see https://example.org/x for more\n")
        assert '<a href="https://example.org/x"' in html
        assert 'rel="noopener noreferrer nofollow"' in html

    def test_an_autolinked_url_cannot_smuggle_a_scheme(self) -> None:
        html = self._render("javascript:alert(1) and vbscript:x\n")
        assert "javascript:" not in html.lower() or "<a" not in html

    def test_strikethrough_renders_as_del(self) -> None:
        assert "<del>gone</del>" in self._render("~~gone~~\n")

    def test_a_definition_list_renders(self) -> None:
        html = self._render("Term\n:   Definition text\n")
        assert "<dl>" in html and "<dt>Term</dt>" in html
        assert "<dd>Definition text</dd>" in html

    def test_raw_markdown_html_is_still_escaped(self) -> None:
        html = self._render("<script>alert(1)</script>\n<img src=x onerror=alert(1)>\n")
        assert "<script" not in html
        assert "<img" not in html
        assert "onerror=" not in html.replace("onerror=alert(1)&gt;", "")
        assert "&lt;script&gt;" in html
        assert "&lt;img src=x onerror=alert(1)&gt;" in html

    def test_the_output_is_already_a_sanitiser_fixed_point(self) -> None:
        from freva_rest.settings_api.sanitizer import stable_sanitize

        source = (
            "| a | b |\n|---|:-:|\n| 1 | 2 |\n\n"
            "~~gone~~ and https://example.org/x\n\n"
            "Term\n:   Definition\n\n"
            "```python\nx = 1\n```\n"
        )
        html = self._render(source)
        assert stable_sanitize(html) == html


class TestServerSideSyntaxHighlighting:
    """
    Fenced code is highlighted on the server; the portal ships styles, not a
    highlighter.
    """

    @staticmethod
    def _render(source: str) -> str:
        from freva_rest.settings_api.renderers import render

        return render(source, "markdown")

    def test_a_recognised_language_is_highlighted(self) -> None:
        html = self._render("```python\nx = 1\n```\n")
        assert 'class="language-python"' in html
        assert '<span class="n">x</span>' in html
        assert '<span class="mi">1</span>' in html

    @pytest.mark.parametrize(
        "fence,canonical",
        [("py", "python"), ("PY", "python"), ("Python3", "python"), ("sh", "bash")],
    )
    def test_an_alias_normalises_to_its_canonical_name(
        self, fence: str, canonical: str
    ) -> None:
        html = self._render(f"```{fence}\nx = 1\n```\n")
        assert f'class="language-{canonical}"' in html, html

    def test_an_unknown_language_falls_back_to_plain_escaped_code(self) -> None:
        html = self._render("```klingon\nx = 1 & 2 < 3\n```\n")
        assert "<pre><code>" in html
        assert "language-" not in html
        assert "&amp;" in html and "&lt;" in html

    def test_a_bare_fence_is_plain_escaped_code(self) -> None:
        html = self._render("```\n<b>not bold</b>\n```\n")
        assert "&lt;b&gt;" in html
        assert "<b>" not in html

    @pytest.mark.parametrize(
        "info",
        [
            'py" onload="alert(1)',
            "python><script>alert(1)</script>",
            'python" class="evil',
            "../../etc/passwd",
        ],
    )
    def test_malicious_fence_info_never_reaches_an_attribute(self, info: str) -> None:
        html = self._render(f"```{info}\nx = 1\n```\n")
        assert "onload" not in html
        assert "<script" not in html
        assert "evil" not in html
        assert "passwd" not in html.replace("&#", "")

    def test_an_oversized_block_is_not_highlighted_but_still_renders(self) -> None:
        from freva_rest.settings_api.renderers import MAX_HIGHLIGHT_BLOCK_BYTES

        code = "x = 1\n" * (MAX_HIGHLIGHT_BLOCK_BYTES // 6 + 100)
        html = self._render(f"```python\n{code}```\n")
        assert '<span class="n">' not in html
        assert "x = 1" in html

    def test_highlighted_spans_survive_sanitisation_unchanged(self) -> None:
        from freva_rest.settings_api.sanitizer import stable_sanitize

        html = self._render("```python\ndef f(x):\n    return x + 1\n```\n")
        assert stable_sanitize(html) == html
        assert '<span class="k">def</span>' in html

    def test_highlighting_cannot_introduce_markup_of_its_own(self) -> None:
        html = self._render('```python\nx = "<script>alert(1)</script>"\n```\n')
        assert "<script" not in html
        assert "&lt;script&gt;" in html

    def test_expansion_still_trips_the_rendered_size_ceiling(self) -> None:
        from freva_rest.settings_api.renderers import (
            MAX_HIGHLIGHT_BLOCK_BYTES,
            MAX_RENDERED_BYTES,
            MAX_SOURCE_BYTES,
            render,
        )

        block = "x=1;y=2;z=3;a=[1,2,3];b={'k':'v'}\n" * 90
        assert len(block.encode()) <= MAX_HIGHLIGHT_BLOCK_BYTES
        fenced = "```python\n" + block + "```\n\n"
        source = fenced * (MAX_SOURCE_BYTES // len(fenced.encode()))
        assert len(source.encode()) <= MAX_SOURCE_BYTES
        with pytest.raises(ValueError) as raised:
            render(source, "markdown")
        assert str(MAX_RENDERED_BYTES) in str(raised.value)

    def test_the_budget_still_applied_on_the_way_to_that_ceiling(self) -> None:
        from freva_rest.settings_api.renderers import (
            MAX_HIGHLIGHT_DOCUMENT_BYTES,
            _render_markdown,
        )

        block = "x=1;y=2;z=3;a=[1,2,3];b={'k':'v'}\n" * 90
        fenced = "```python\n" + block + "```\n\n"
        html = _render_markdown(fenced * 40)  # before the size check
        blocks = html.split("<pre>")[1:]
        highlighted = [b for b in blocks if "<span" in b]
        assert len(highlighted) == MAX_HIGHLIGHT_DOCUMENT_BYTES // len(block.encode())
        assert len(highlighted) < len(blocks)

    def test_the_language_map_resolves_to_real_lexers(self) -> None:
        import pygments.lexers
        import pygments.util

        from freva_rest.settings_api.renderers import HIGHLIGHT_LANGUAGES

        for alias, canonical in sorted(HIGHLIGHT_LANGUAGES.items()):
            try:
                pygments.lexers.get_lexer_by_name(canonical)
            except pygments.util.ClassNotFound:
                pytest.fail(f"{alias} -> {canonical} is not a pygments lexer")

    def test_an_unknown_lexer_is_caught_narrowly(self) -> None:
        import ast
        import inspect
        import textwrap

        from freva_rest.settings_api import renderers

        tree = ast.parse(textwrap.dedent(inspect.getsource(renderers._highlight)))
        caught = [
            ast.unparse(handler.type)
            for handler in ast.walk(tree)
            if isinstance(handler, ast.ExceptHandler) and handler.type is not None
        ]
        assert "util.ClassNotFound" in caught, caught
        assert "Exception" not in caught
        assert "BaseException" not in caught


class TestRstCodeBlockAndMath:
    """
    rst has html math; its code blocks are *not* highlighted.
    """

    @staticmethod
    def _render(source: str) -> str:
        from freva_rest.settings_api.renderers import render

        return render(source, "rst")

    def test_a_code_block_is_plain_escaped_code(self) -> None:
        html = self._render(".. code-block:: python\n\n   x = 1 & 2 < 3\n")
        assert "<span" not in html
        assert "&amp;" in html and "&lt;" in html
        assert "x = 1" in html

    def test_the_docutils_highlighter_is_switched_off(self) -> None:
        from freva_rest.settings_api.renderers import (
            _rst_settings,
            _rst_writer,
        )

        assert _rst_settings(_rst_writer()).syntax_highlight == "none"

    @pytest.mark.parametrize(
        "language",
        [
            "perl",
            "brainfuck",
            "definitely-not-a-language",
            'py"><script>alert(1)</script>',
            "python",
        ],
    )
    def test_no_language_reaches_pygments_through_rst(self, language: str) -> None:
        from unittest import mock

        import pygments

        with mock.patch.object(
            pygments, "highlight", side_effect=AssertionError("pygments was called")
        ):
            html = self._render(f".. code-block:: {language}\n\n   x = 1\n")
        assert "<span" not in html

    def test_a_large_rst_code_block_is_cheap(self) -> None:
        html = self._render(".. code-block:: make\n\n   " + ("a" * 40000) + "\n")
        assert "<span" not in html

    def test_hostile_language_text_is_normalised_out_of_the_class(self) -> None:
        html = self._render(
            '.. code-block:: py"><script>alert(1)</script>\n\n   x = 1\n'
        )
        assert "script" not in html
        assert "alert" not in html
        assert 'class="code literal-block"' in html

    def test_an_ordinary_language_still_names_itself(self) -> None:
        html = self._render(".. code-block:: python\n\n   x = 1\n")
        assert "python" in html

    def test_inline_math_renders_with_its_classes(self) -> None:
        html = self._render("Inline :math:`a^2 + b^2 = c^2` here.\n")
        assert 'class="formula"' in html
        assert "<i>a</i>" in html
        assert "<sup>2</sup>" in html

    def test_a_fraction_renders(self) -> None:
        html = self._render(".. math::\n\n   \\frac{x}{y}\n")
        assert 'class="fraction"' in html
        assert 'class="numerator"' in html and 'class="denominator"' in html

    def test_sum_and_integral_limits_render(self) -> None:
        html = self._render(".. math::\n\n   \\sum_{i=1}^{n} x_i = \\int_0^1 f(x) dx\n")
        assert 'class="limits"' in html
        assert 'class="limit"' in html
        assert "∑" in html and "∫" in html

    @pytest.mark.parametrize(
        "required", ["formula", "limits", "limit", "fraction", "bigoperator"]
    )
    def test_the_required_classes_survive_stable_sanitize(self, required: str) -> None:
        from freva_rest.settings_api.sanitizer import stable_sanitize

        html = self._render(
            ".. math::\n\n   \\sum_{i=1}^{n} \\frac{x_i}{n} = \\int_0^1 f(x) dx\n"
        )
        assert f'class="{required}"' in html or f'{required}"' in html, required
        assert stable_sanitize(html) == html

    def test_math_adds_no_script_and_no_mathml(self) -> None:
        html = self._render("Inline :math:`a^2` and\n\n.. math::\n\n   \\int_0^1 x\n")
        for forbidden in ("<script", "<math", "MathJax", "style="):
            assert forbidden not in html, forbidden

    def test_raw_directives_are_still_refused(self) -> None:
        html = self._render(".. raw:: html\n\n   <script>alert(1)</script>\n")
        assert "<script" not in html
        assert "alert(1)" not in html


class TestRendererIdentity:
    """The identity is the hand-maintained generation plus the installed
    rendering dependencies, and only the generation is public."""

    def test_the_generation_is_pinned(self) -> None:
        from freva_rest.settings_api.sanitizer import RENDERER_VERSION
        assert RENDERER_VERSION == "1"

    def test_pygments_is_part_of_the_identity(self) -> None:
        from freva_rest.settings_api.sanitizer import (
            RENDERER_FINGERPRINT,
            RENDERING_DEPENDENCIES,
        )

        assert "pygments" in RENDERING_DEPENDENCIES
        assert "pygments=" in RENDERER_FINGERPRINT

    def test_the_public_generation_is_still_only_the_generation(self) -> None:
        from freva_rest.settings_api.sanitizer import (
            RENDERER_FINGERPRINT,
            renderer_generation,
        )

        public = renderer_generation(RENDERER_FINGERPRINT)
        assert public == "1"
        assert "pygments" not in public

    @pytest.mark.parametrize("other", ["0", "2"])
    def test_a_document_from_another_generation_is_stale(self, other: str) -> None:
        """The comparison is equality, so a generation on either side of the
        current one is stale."""
        from freva_rest.settings_api.core import needs_rebuild
        from freva_rest.settings_api.renderers import rendered_hash, source_hash
        from freva_rest.settings_api.sanitizer import (
            RENDERER_FINGERPRINT,
            RENDERER_VERSION,
        )

        html = "<p>old</p>"
        assert RENDERER_VERSION != other
        assert RENDERER_FINGERPRINT.startswith(RENDERER_VERSION + "+")
        dependencies = RENDERER_FINGERPRINT[len(RENDERER_VERSION) + 1 :]
        document = {
            "format": "markdown",
            "source": "old",
            "source_hash": source_hash("old", "markdown"),
            "rendered_html": html,
            "rendered_hash": rendered_hash(html),
            "renderer_version": f"{other}+{dependencies}",
        }
        assert needs_rebuild(document) is True

    def test_a_fingerprint_from_a_different_build_is_stale(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild
        from freva_rest.settings_api.renderers import (
            rendered_hash,
            source_hash,
        )

        html = "<p>old</p>"
        document = {
            "format": "markdown",
            "source": "old",
            "source_hash": source_hash("old", "markdown"),
            "rendered_html": html,
            "rendered_hash": rendered_hash(html),
            "renderer_version": "0+docutils=0.19+mistune=3.0.0+nh3=0.2.15",
        }
        assert needs_rebuild(document) is True

    def test_a_pygments_upgrade_alone_would_mark_documents_stale(self) -> None:
        from freva_rest.settings_api.core import needs_rebuild
        from freva_rest.settings_api.renderers import (
            rendered_hash,
            source_hash,
        )
        from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

        html = "<p>old</p>"
        as_if_older_pygments = RENDERER_FINGERPRINT.replace("pygments=", "pygments=0.")
        assert as_if_older_pygments != RENDERER_FINGERPRINT
        document = {
            "format": "markdown",
            "source": "old",
            "source_hash": source_hash("old", "markdown"),
            "rendered_html": html,
            "rendered_hash": rendered_hash(html),
            "renderer_version": as_if_older_pygments,
        }
        assert needs_rebuild(document) is True


class TestMalformedRstCannotEndTheProcess:
    """`.. math:: \\frac{` makes docutils raise `SystemExit(1)` out of a
    request handler, with a bug report on stderr. A failure to render *content*
    is a 422, not an exit."""

    BROKEN = ".. math::\n\n   \\frac{\n"

    def test_render_raises_value_error_not_system_exit(self) -> None:
        from freva_rest.settings_api.renderers import render

        with pytest.raises(ValueError):
            render(self.BROKEN, "rst")

    def test_it_is_not_a_system_exit_under_any_name(self) -> None:
        from freva_rest.settings_api.renderers import render

        try:
            render(self.BROKEN, "rst")
        except BaseException as error:
            assert not isinstance(error, SystemExit), error
            assert isinstance(error, ValueError), type(error)
        else:
            pytest.fail("expected a ValueError")

    def test_no_docutils_bug_report_reaches_stderr(self) -> None:
        import contextlib
        import io

        from freva_rest.settings_api.renderers import render

        captured = io.StringIO()
        with contextlib.redirect_stderr(captured):
            with pytest.raises(ValueError):
                render(self.BROKEN, "rst")
        text = captured.getvalue()
        for marker in (
            "Please report errors",
            "Exiting due to error",
            "docutils-users@lists.sourceforge.net",
            'Use "--traceback"',
        ):
            assert marker not in text, f"{marker!r} still on stderr:\n{text}"

    def test_shutdown_signals_are_not_swallowed(self) -> None:
        import ast
        import inspect
        import textwrap

        from freva_rest.settings_api import renderers

        tree = ast.parse(textwrap.dedent(inspect.getsource(renderers._render_rst)))
        caught = {
            ast.unparse(handler.type)
            for handler in ast.walk(tree)
            if isinstance(handler, ast.ExceptHandler) and handler.type is not None
        }
        assert caught == {"SystemExit", "Exception"}, caught

    def test_traceback_is_enabled_on_the_settings(self) -> None:
        from freva_rest.settings_api.renderers import (
            _rst_settings,
            _rst_writer,
        )

        assert _rst_settings(_rst_writer()).traceback is True

    def test_a_valid_document_still_renders(self) -> None:
        from freva_rest.settings_api.renderers import render

        assert "<p>" in render("Just a paragraph.\n", "rst")

    def test_the_write_path_refuses_and_leaves_the_document_alone(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.schema import ContentSource

        reset_caches()
        collection = _FakeCollection()
        store = ContentStore(_FakeConfig(collection), "a", "b")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "rst", "source": "Good.\n"})
            )
        )
        before = dict(collection.docs["a:b"])
        with pytest.raises(HTTPException) as raised:
            loop.run_until_complete(
                store.patch(
                    ContentSource.model_validate(
                        {"format": "rst", "source": self.BROKEN}
                    )
                )
            )
        assert raised.value.status_code == 422
        assert collection.docs["a:b"] == before

    def test_the_rebuild_counts_it_failed_and_carries_on(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )
        from freva_rest.settings_api.renderers import (
            rendered_hash,
            source_hash,
        )

        reset_caches()
        collection = _FakeCollection()
        for name, source in (("a:broken", self.BROKEN), ("b:good", "Fine.\n")):
            ui_id, content_id = name.split(":")
            collection.docs[name] = {
                "_id": name,
                "ui_id": ui_id,
                "content_id": content_id,
                "format": "rst",
                "source": source,
                "source_hash": source_hash(source, "rst"),
                "rendered_html": "<p>stale</p>",
                "rendered_hash": rendered_hash("<p>stale</p>"),
                "renderer_version": "0+ancient",
                "revision": 1,
            }

        class _Config:
            mongo_collection_ui_contents = collection

        report = asyncio.run(rebuild_stale_content(_Config()))
        assert report["examined"] == 2
        assert report["failed"] == 1
        assert report["rebuilt"] == 1
        assert collection.docs["b:good"]["renderer_version"] != "0+ancient"
        assert collection.docs["a:broken"]["renderer_version"] == "0+ancient"
        assert collection.docs["a:broken"]["source"] == self.BROKEN

    def test_pagination_advances_past_it_rather_than_looping(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )
        from freva_rest.settings_api.renderers import (
            rendered_hash,
            source_hash,
        )

        reset_caches()
        collection = _FakeCollection()
        for index in range(3):
            name = f"ui:{index}"
            collection.docs[name] = {
                "_id": name,
                "ui_id": "ui",
                "content_id": str(index),
                "format": "rst",
                "source": self.BROKEN if index == 0 else "Fine.\n",
                "source_hash": source_hash(
                    self.BROKEN if index == 0 else "Fine.\n", "rst"
                ),
                "rendered_html": "<p>stale</p>",
                "rendered_hash": rendered_hash("<p>stale</p>"),
                "renderer_version": "0+ancient",
                "revision": 1,
            }

        class _Config:
            mongo_collection_ui_contents = collection

        import freva_rest.settings_api.core as core

        original = core.MAX_SCAN_DOCUMENTS
        core.MAX_SCAN_DOCUMENTS = 1
        try:
            first = asyncio.run(rebuild_stale_content(_Config()))
            assert first["truncated"] == 1
            assert first["next_after"] == "ui:0"
            second = asyncio.run(rebuild_stale_content(_Config(), after="ui:0"))
        finally:
            core.MAX_SCAN_DOCUMENTS = original
        assert second["next_after"] == "ui:1"


class TestHighlightingIsGenuinelyBounded:
    """A per-block cap alone is not a bound, and a timeout around
    `asyncio.to_thread` is not one either - cancelling the await does not stop
    the worker thread. The only real bound is on what the thread is handed."""

    @staticmethod
    def _render(source: str) -> str:
        from freva_rest.settings_api.renderers import render

        return render(source, "markdown")

    def test_the_caps_are_the_measured_ones(self) -> None:
        from freva_rest.settings_api import renderers

        assert renderers.MAX_HIGHLIGHT_BLOCK_BYTES == 4 * 1024
        assert renderers.EXPENSIVE_HIGHLIGHT_BLOCK_BYTES == 1 * 1024
        assert renderers.MAX_HIGHLIGHT_DOCUMENT_BYTES == 16 * 1024
        assert not hasattr(renderers, "MAX_HIGHLIGHT_BYTES")

    def test_the_expensive_set_is_what_measurement_found(self) -> None:
        from freva_rest.settings_api.renderers import EXPENSIVE_LEXERS

        assert EXPENSIVE_LEXERS == frozenset({"console", "ini", "make", "r"})

    def test_every_expensive_lexer_is_reachable_from_the_map(self) -> None:
        from freva_rest.settings_api.renderers import (
            EXPENSIVE_LEXERS,
            HIGHLIGHT_LANGUAGES,
        )

        assert EXPENSIVE_LEXERS <= set(HIGHLIGHT_LANGUAGES.values())

    def test_a_block_over_the_ordinary_cap_falls_back(self) -> None:
        from freva_rest.settings_api.renderers import MAX_HIGHLIGHT_BLOCK_BYTES

        code = "x = 1\n" * (MAX_HIGHLIGHT_BLOCK_BYTES // 6 + 50)
        html = self._render(f"```python\n{code}```\n")
        assert "<span" not in html
        assert "x = 1" in html

    def test_a_block_just_under_the_ordinary_cap_is_highlighted(self) -> None:
        from freva_rest.settings_api.renderers import MAX_HIGHLIGHT_BLOCK_BYTES

        code = "x = 1\n" * ((MAX_HIGHLIGHT_BLOCK_BYTES - 100) // 6)
        html = self._render(f"```python\n{code}```\n")
        assert '<span class="n">x</span>' in html

    @pytest.mark.parametrize(
        "alias", ["make", "makefile", "console", "r", "ini", "cfg"]
    )
    def test_an_expensive_lexer_gets_the_smaller_cap(self, alias: str) -> None:
        from freva_rest.settings_api.renderers import (
            EXPENSIVE_HIGHLIGHT_BLOCK_BYTES,
            MAX_HIGHLIGHT_BLOCK_BYTES,
        )

        size = (EXPENSIVE_HIGHLIGHT_BLOCK_BYTES + MAX_HIGHLIGHT_BLOCK_BYTES) // 2
        code = "a\n" * (size // 2)
        assert "<span" not in self._render(f"```{alias}\n{code}```\n"), alias
        assert '<span class="n">' in self._render(f"```python\n{code}```\n")

    def test_an_expensive_lexer_under_its_cap_still_highlights(self) -> None:
        html = self._render("```make\nall:\n\tgcc -o x x.c\n```\n")
        assert "<span" in html

    def test_many_legal_blocks_cannot_bypass_the_document_budget(self) -> None:
        from freva_rest.settings_api.renderers import (
            MAX_HIGHLIGHT_DOCUMENT_BYTES,
        )

        block = "x = 1\n" * 300
        count = MAX_HIGHLIGHT_DOCUMENT_BYTES // len(block.encode()) + 3
        html = self._render(("```python\n" + block + "```\n\n") * count)
        blocks = html.split("<pre>")[1:]
        assert len(blocks) == count
        highlighted = [b for b in blocks if "<span" in b]
        plain = [b for b in blocks if "<span" not in b]
        assert highlighted, "nothing was highlighted at all"
        assert plain, "the budget never ran out"
        assert len(highlighted) == MAX_HIGHLIGHT_DOCUMENT_BYTES // len(block.encode())
        assert 'class="language-python"' in plain[0]

    def test_the_budget_is_per_render_not_per_process(self) -> None:
        from freva_rest.settings_api.renderers import (
            MAX_HIGHLIGHT_DOCUMENT_BYTES,
        )

        block = "x = 1\n" * 300
        count = MAX_HIGHLIGHT_DOCUMENT_BYTES // len(block.encode()) + 3
        self._render(("```python\n" + block + "```\n\n") * count)
        assert '<span class="n">x</span>' in self._render("```python\nx = 1\n```\n")

    def test_concurrent_renders_do_not_share_a_budget(self) -> None:
        import concurrent.futures

        from freva_rest.settings_api.renderers import (
            MAX_HIGHLIGHT_DOCUMENT_BYTES,
        )

        block = "x = 1\n" * 300
        count = MAX_HIGHLIGHT_DOCUMENT_BYTES // len(block.encode()) + 3
        heavy = ("```python\n" + block + "```\n\n") * count
        small = "```python\nx = 1\n```\n"
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(self._render, heavy) for _ in range(2)]
            futures += [pool.submit(self._render, small) for _ in range(4)]
            results = [f.result() for f in futures]
        for html in results[2:]:
            assert '<span class="n">x</span>' in html

    def test_the_budget_object_refuses_what_it_cannot_afford(self) -> None:
        from freva_rest.settings_api.renderers import (
            MIN_HIGHLIGHT_CHARGE,
            _HighlightBudget,
        )

        budget = _HighlightBudget(total=MIN_HIGHLIGHT_CHARGE * 3)
        assert budget.take(MIN_HIGHLIGHT_CHARGE * 2) is True
        assert budget.take(MIN_HIGHLIGHT_CHARGE * 2) is False
        assert budget.take(MIN_HIGHLIGHT_CHARGE) is True
        assert budget.take(1) is False

    def test_an_oversized_block_does_not_spend_the_budget(self) -> None:
        from freva_rest.settings_api.renderers import MAX_HIGHLIGHT_BLOCK_BYTES

        huge = "x = 1\n" * (MAX_HIGHLIGHT_BLOCK_BYTES // 6 + 50)
        html = self._render(f"```python\n{huge}```\n\n```python\ny = 2\n```\n")
        assert '<span class="n">y</span>' in html


class TestRstMathStateIsPerRender:
    """
    `HTMLTranslator.math_tags` is a class attribute holding lists, and
    `starttag` appends the node's classes to one of them in place.
    """

    PWN = ".. math::\n   :class: pwn\n\n   x^2\n"
    PLAIN = ".. math::\n\n   y^2\n"
    INLINE = "Inline :math:`z^2`\n"

    @staticmethod
    def _render(source: str) -> str:
        from freva_rest.settings_api.renderers import render

        return render(source, "rst")

    def test_ordinary_math_is_clean(self) -> None:
        assert 'class="formula"' in self._render(self.PLAIN)

    def test_the_class_reaches_its_own_document(self) -> None:
        assert "pwn" in self._render(self.PWN)

    def test_a_following_block_is_unaffected(self) -> None:
        self._render(self.PWN)
        html = self._render(self.PLAIN)
        assert "pwn" not in html
        assert 'class="formula"' in html

    def test_following_inline_math_is_unaffected(self) -> None:
        self._render(self.PWN)
        html = self._render(self.INLINE)
        assert "pwn" not in html
        assert 'class="formula"' in html

    def test_two_concurrent_renders_do_not_bleed_into_each_other(self) -> None:
        import concurrent.futures

        other = ".. math::\n   :class: mine\n\n   w^2\n"
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = {
                pool.submit(self._render, source): label
                for label, source in (
                    ("pwn", self.PWN),
                    ("mine", other),
                    ("plain", self.PLAIN),
                    ("plain2", self.PLAIN),
                    ("inline", self.INLINE),
                )
            }
            results = {label: f.result() for f, label in futures.items()}
        assert "pwn" in results["pwn"] and "mine" not in results["pwn"]
        assert "mine" in results["mine"] and "pwn" not in results["mine"]
        for label in ("plain", "plain2", "inline"):
            assert "pwn" not in results[label], label
            assert "mine" not in results[label], label

    def test_repeated_hostile_classes_do_not_grow_shared_state(self) -> None:
        import docutils.writers.html5_polyglot as html5

        before = copy.deepcopy(html5.HTMLTranslator.math_tags)
        for index in range(25):
            self._render(f".. math::\n   :class: grow{index}\n\n   x^2\n")
        assert html5.HTMLTranslator.math_tags == before
        assert html5.HTMLTranslator.math_tags["html"][2] == ["formula"]

    def test_each_render_gets_its_own_translator(self) -> None:
        from freva_rest.settings_api.renderers import _rst_writer

        first, second = _rst_writer(), _rst_writer()
        assert first.translator_class is not second.translator_class
        assert first.translator_class.math_tags is not second.translator_class.math_tags
        first.translator_class.math_tags["html"][2].append("scribble")
        assert second.translator_class.math_tags["html"][2] == ["formula"]


class TestRstIsParsedOnce:
    """A second parse purely to produce a log line would repeat math conversion
    and highlighting over the whole document."""

    def test_the_second_parse_is_gone(self) -> None:
        from freva_rest.settings_api import renderers

        assert not hasattr(renderers, "_rst_diagnostics")

    def test_the_document_is_parsed_once(self) -> None:
        from unittest import mock

        import docutils.parsers.rst as rst_parser

        from freva_rest.settings_api.renderers import render

        source = (
            "Title\n=====\n\nInline :math:`a^2` and\n\n"
            ".. code-block:: python\n\n   x = 1\n\n.. math::\n\n   b^2\n"
        )
        calls = []
        real = rst_parser.Parser.parse

        def _counted(self, *args, **kwargs):
            calls.append(1)
            return real(self, *args, **kwargs)

        with mock.patch.object(rst_parser.Parser, "parse", _counted):
            render(source, "rst")
        assert len(calls) == 1, f"the document was parsed {len(calls)} times"

    def test_math_conversion_runs_once_per_formula(self) -> None:
        from unittest import mock

        import docutils.writers._html_base as html_base

        from freva_rest.settings_api.renderers import render

        source = "Inline :math:`a^2` and\n\n.. math::\n\n   b^2\n"
        real = html_base.math2html.math2html
        calls = []

        def _counted(*args, **kwargs):
            calls.append(1)
            return real(*args, **kwargs)

        with mock.patch.object(html_base.math2html, "math2html", _counted):
            render(source, "rst")
        assert len(calls) == 2, f"{len(calls)} conversions for 2 formulas"

    def test_the_advisory_warning_still_reports_dropped_directives(self) -> None:
        from unittest import mock

        from freva_rest.settings_api import renderers

        with mock.patch.object(renderers.logger, "warning") as warned:
            renderers.render(".. raw:: html\n\n   <b>x</b>\n", "rst")
        assert warned.called
        assert "raw" in str(warned.call_args)

    def test_a_clean_document_warns_about_nothing(self) -> None:
        from unittest import mock

        from freva_rest.settings_api import renderers

        with mock.patch.object(renderers.logger, "warning") as warned:
            renderers.render("Just text.\n", "rst")
        assert not warned.called


class TestRstMathOutputIsSanitizedNotTrusted:
    """
    The rst converter is **not** a safe-output guarantee.
    """

    @staticmethod
    def _render(source: str) -> str:
        from freva_rest.settings_api.renderers import render

        return render(source, "rst")

    def test_mbox_cannot_inject_markup(self) -> None:
        html = self._render(".. math::\n\n   \\mbox{<script>alert(1)</script>}\n")
        assert "<script" not in html
        assert "alert(1)" not in html or "&lt;script&gt;" in html

    def test_mbox_cannot_inject_an_event_attribute(self) -> None:
        html = self._render(".. math::\n\n   \\mbox{<img src=x onerror=alert(1)>}\n")
        assert "onerror" not in html
        assert "alert(1)" not in html
        assert "<img" in html

    @pytest.mark.parametrize(
        "url",
        [
            "javascript:alert(1)",
            "data:text/html,<script>alert(1)</script>",
            "vbscript:x",
        ],
    )
    def test_href_cannot_carry_an_unsafe_scheme(self, url: str) -> None:
        html = self._render(f".. math::\n\n   \\href{{{url}}}{{click}}\n")
        assert "javascript:" not in html.lower()
        assert "vbscript:" not in html.lower()
        assert "data:text/html" not in html.lower()

    def test_href_cannot_carry_a_protocol_relative_url(self) -> None:
        html = self._render(".. math::\n\n   \\href{//evil.example/x}{click}\n")
        assert "//evil.example" not in html

    def test_a_surviving_link_still_carries_the_forced_rel(self) -> None:
        html = self._render(".. math::\n\n   \\href{https://x.example}{click}\n")
        if "<a " in html:
            assert 'rel="noopener noreferrer nofollow"' in html

    def test_math_never_emits_style_id_mathml_or_script(self) -> None:
        html = self._render(
            ".. math::\n   :class: c\n\n   \\sum_{i=1}^{n} \\frac{x_i}{n}\n"
        )
        for forbidden in ("<script", "<math", "style=", " id="):
            assert forbidden not in html, forbidden

    def test_the_output_is_a_sanitiser_fixed_point(self) -> None:
        from freva_rest.settings_api.sanitizer import stable_sanitize

        html = self._render(
            ".. math::\n\n   \\mbox{<b>x</b>} \\href{https://x.example}{y}\n"
        )
        assert stable_sanitize(html) == html


class TestHighlightCallsAreBoundedNotOnlyBytes:
    """
    A byte budget does not bound *calls*.
    """

    @staticmethod
    def _count_calls(source: str) -> tuple:
        from unittest import mock

        from freva_rest.settings_api import renderers

        calls = []
        real = renderers._highlight

        def _counted(code: str, language: str):
            calls.append(language)
            return real(code, language)

        with mock.patch.object(renderers, "_highlight", _counted):
            html = renderers._render_markdown(source)
        return len(calls), html

    def test_the_bound_is_derived_from_the_budget(self) -> None:
        from freva_rest.settings_api import renderers

        assert renderers.MIN_HIGHLIGHT_CHARGE == 256
        assert renderers.MAX_HIGHLIGHT_BLOCKS == 64
        assert renderers.MAX_HIGHLIGHT_BLOCKS == (
            renderers.MAX_HIGHLIGHT_DOCUMENT_BYTES // renderers.MIN_HIGHLIGHT_CHARGE
        )

    def test_empty_fences_cannot_call_pygments_without_limit(self) -> None:
        from freva_rest.settings_api.renderers import MAX_HIGHLIGHT_BLOCKS

        calls, _ = self._count_calls("```python\n```\n\n" * 3000)
        assert calls == MAX_HIGHLIGHT_BLOCKS

    def test_tiny_fences_cannot_either(self) -> None:
        from freva_rest.settings_api.renderers import MAX_HIGHLIGHT_BLOCKS

        calls, _ = self._count_calls("```python\nx\n```\n\n" * 3000)
        assert calls == MAX_HIGHLIGHT_BLOCKS

    def test_the_remainder_renders_as_plain_code(self) -> None:
        from freva_rest.settings_api.renderers import MAX_HIGHLIGHT_BLOCKS

        count = MAX_HIGHLIGHT_BLOCKS + 20
        _, html = self._count_calls("```python\nx\n```\n\n" * count)
        blocks = html.split("<pre>")[1:]
        assert len(blocks) == count
        highlighted = [b for b in blocks if "<span" in b]
        plain = [b for b in blocks if "<span" not in b]
        assert len(highlighted) == MAX_HIGHLIGHT_BLOCKS
        assert len(plain) == 20
        assert 'class="language-python"' in plain[0]
        assert "x" in plain[0]

    def test_a_block_still_charges_its_real_size_when_larger(self) -> None:
        from freva_rest.settings_api.renderers import (
            MAX_HIGHLIGHT_DOCUMENT_BYTES,
            MIN_HIGHLIGHT_CHARGE,
        )

        block = "x = 1\n" * 300
        expected = MAX_HIGHLIGHT_DOCUMENT_BYTES // len(block.encode())
        calls, _ = self._count_calls(("```python\n" + block + "```\n\n") * 40)
        assert calls == expected
        assert expected < MAX_HIGHLIGHT_DOCUMENT_BYTES // MIN_HIGHLIGHT_CHARGE

    def test_the_block_count_is_per_render(self) -> None:
        from freva_rest.settings_api.renderers import MAX_HIGHLIGHT_BLOCKS

        self._count_calls("```python\n```\n\n" * (MAX_HIGHLIGHT_BLOCKS + 10))
        calls, _ = self._count_calls("```python\nx = 1\n```\n")
        assert calls == 1

    def test_the_budget_object_counts_blocks(self) -> None:
        from freva_rest.settings_api.renderers import (
            MAX_HIGHLIGHT_BLOCKS,
            _HighlightBudget,
        )

        budget = _HighlightBudget()
        assert all(budget.take(0) for _ in range(MAX_HIGHLIGHT_BLOCKS))
        assert budget.take(0) is False


class TestMathClassesAreIsolatedWithinOneDocument:
    """
    A fresh translator per render keeps `:class:` from leaking into the
    next *request*. Within one render every formula needs its own list too, or
    the first `:class:` sticks to all of them.
    """

    SOURCE = (
        ".. math::\n   :class: pwn\n\n   x\n\n"
        ".. math::\n\n   y\n\n"
        "Inline :math:`z`\n"
    )

    @staticmethod
    def _render(source: str) -> str:
        from freva_rest.settings_api.renderers import render

        return render(source, "rst")

    def test_only_the_marked_formula_carries_the_class(self) -> None:
        html = self._render(self.SOURCE)
        assert html.count("pwn") == 1, html

    def test_block_to_block_isolation(self) -> None:
        html = self._render(".. math::\n   :class: pwn\n\n   x\n\n.. math::\n\n   y\n")
        first, second = html.split("</div>")[0], html.split("</div>")[1]
        assert "pwn" in first
        assert "pwn" not in second
        assert 'class="formula"' in second

    def test_block_to_inline_isolation(self) -> None:
        html = self._render(".. math::\n   :class: pwn\n\n   x\n\nInline :math:`z`\n")
        inline = html[html.index("<p>") :]
        assert "pwn" not in inline
        assert 'class="formula"' in inline

    def test_two_marked_formulas_do_not_accumulate(self) -> None:
        html = self._render(
            ".. math::\n   :class: one\n\n   x\n\n"
            ".. math::\n   :class: two\n\n   y\n\n"
            ".. math::\n\n   z\n"
        )
        blocks = [b for b in html.split("<div ") if "formula" in b]
        assert len(blocks) == 3
        assert "one" in blocks[0] and "two" not in blocks[0]
        assert "two" in blocks[1] and "one" not in blocks[1]
        assert "one" not in blocks[2] and "two" not in blocks[2]

    def test_the_class_still_reaches_its_own_formula(self) -> None:
        assert "pwn" in self._render(".. math::\n   :class: pwn\n\n   x\n")

    def test_starttag_never_hands_the_shared_list_to_the_base(self) -> None:
        from unittest import mock

        import docutils.writers.html5_polyglot as html5

        from freva_rest.settings_api.renderers import _rst_writer, render

        writer = _rst_writer()
        table = writer.translator_class.math_tags["html"][2]
        received = []
        real = html5.HTMLTranslator.starttag

        def _recording(self, node, tagname, suffix="\n", empty=False, **attributes):
            if "classes" in attributes:
                received.append(attributes["classes"])
            return real(self, node, tagname, suffix, empty, **attributes)

        with mock.patch.object(html5.HTMLTranslator, "starttag", _recording):
            render(".. math::\n   :class: pwn\n\n   x\n\n.. math::\n\n   y\n", "rst")
        formula_lists = [c for c in received if c and c[0] == "formula"]
        assert formula_lists, received
        for handed_over in formula_lists:
            assert handed_over is not table
        assert table == ["formula"]

    def test_the_shared_table_is_still_pristine_afterwards(self) -> None:
        import docutils.writers.html5_polyglot as html5

        self._render(self.SOURCE)
        assert html5.HTMLTranslator.math_tags["html"][2] == ["formula"]


class TestRenderFailuresDoNotLeakInternals:
    """
    `AttributeError: 'NoneType' object has no attribute 'computesize'` must
    not reach editors as a 422 body: it names docutils internals, and there is
    nothing an author can do with it.
    """

    BROKEN = ".. math::\n\n   \\frac{\n"

    def test_the_client_message_is_stable_and_uninformative(self) -> None:
        from freva_rest.settings_api.renderers import (
            RST_FAILURE_DETAIL,
            render,
        )

        with pytest.raises(ValueError) as raised:
            render(self.BROKEN, "rst")
        assert str(raised.value) == RST_FAILURE_DETAIL
        assert RST_FAILURE_DETAIL == (
            "This reStructuredText content could not be rendered."
        )

    @pytest.mark.parametrize(
        "leak", ["AttributeError", "computesize", "NoneType", "docutils", "math2html"]
    )
    def test_no_internal_detail_survives_into_the_message(self, leak: str) -> None:
        from freva_rest.settings_api.renderers import render

        with pytest.raises(ValueError) as raised:
            render(self.BROKEN, "rst")
        assert leak not in str(raised.value)

    def test_the_real_exception_is_logged_server_side(self) -> None:
        from unittest import mock

        from freva_rest.settings_api import renderers

        with mock.patch.object(renderers.logger, "error") as logged:
            with pytest.raises(ValueError):
                renderers.render(self.BROKEN, "rst")
        assert logged.called
        recorded = str(logged.call_args)
        assert "AttributeError" in recorded
        assert "computesize" in recorded

    def test_the_422_body_carries_only_the_stable_message(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.core import ContentStore, reset_caches
        from freva_rest.settings_api.renderers import RST_FAILURE_DETAIL
        from freva_rest.settings_api.schema import ContentSource

        reset_caches()
        collection = _FakeCollection()
        store = ContentStore(_FakeConfig(collection), "a", "b")
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(
            store.patch(
                ContentSource.model_validate({"format": "rst", "source": "Good.\n"})
            )
        )
        before = dict(collection.docs["a:b"])
        with pytest.raises(HTTPException) as raised:
            loop.run_until_complete(
                store.patch(
                    ContentSource.model_validate(
                        {"format": "rst", "source": self.BROKEN}
                    )
                )
            )
        assert raised.value.status_code == 422
        assert raised.value.detail == RST_FAILURE_DETAIL
        assert "computesize" not in str(raised.value.detail)
        assert collection.docs["a:b"] == before

    def test_our_own_messages_are_still_useful(self) -> None:
        from freva_rest.settings_api.renderers import (
            MAX_SOURCE_BYTES,
            RST_FAILURE_DETAIL,
            render,
        )

        with pytest.raises(ValueError) as raised:
            render("x" * (MAX_SOURCE_BYTES + 1), "rst")
        assert str(raised.value) != RST_FAILURE_DETAIL
        assert str(MAX_SOURCE_BYTES) in str(raised.value)

    def test_the_rebuild_still_continues_past_it(self) -> None:
        import asyncio

        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )
        from freva_rest.settings_api.renderers import (
            rendered_hash,
            source_hash,
        )

        reset_caches()
        collection = _FakeCollection()
        for name, source in (("a:broken", self.BROKEN), ("b:good", "Fine.\n")):
            ui_id, content_id = name.split(":")
            collection.docs[name] = {
                "_id": name,
                "ui_id": ui_id,
                "content_id": content_id,
                "format": "rst",
                "source": source,
                "source_hash": source_hash(source, "rst"),
                "rendered_html": "<p>stale</p>",
                "rendered_hash": rendered_hash("<p>stale</p>"),
                "renderer_version": "0+ancient",
                "revision": 1,
            }

        class _Config:
            mongo_collection_ui_contents = collection

        report = asyncio.run(rebuild_stale_content(_Config()))
        assert report["failed"] == 1 and report["rebuilt"] == 1


class TestProtocolRelativeUrlsSurviveNoNormalisation:
    """
    `str.strip()` removes python's whitespace, not the WHATWG "C0 control
    or space" range a browser strips before parsing a url.
    """

    @staticmethod
    def _clean(html: str) -> str:
        from freva_rest.settings_api.sanitizer import sanitize_html

        return sanitize_html(html)

    @pytest.mark.parametrize(
        "href",
        [
            "\x01//evil.example",
            "&#1;//evil.example",
            "\x02\x03//evil.example",
            "&#13;&#1;//evil.example",
            "\x1b//evil.example",
            "/\t/evil.example",
            "/&#9;/evil.example",
            "//evil.example",
            " //evil.example",
            "\\\\evil.example",
            "/\\evil.example",
            "\x01\\\\evil.example",
        ],
    )
    def test_no_spelling_reaches_the_browser_as_an_off_site_url(
        self, href: str
    ) -> None:
        cleaned = self._clean(f'<a href="{href}">x</a>')
        assert "evil.example" not in cleaned, cleaned

    @pytest.mark.parametrize(
        "href",
        ["\x01//evil.example", "&#1;//evil.example", "/\t/evil.example"],
    )
    def test_the_same_through_a_rendered_document(self, href: str) -> None:
        from freva_rest.settings_api.renderers import render

        html = render(f'<a href="{href}">x</a>', "html-fragment")
        assert "evil.example" not in html

    @pytest.mark.parametrize(
        "href",
        [
            "/legit/page",
            "https://ok.example/p",
            "http://ok.example/p",
            "mailto:a@b.example",
            "page.html",
            "#anchor",
            "?q=1",
        ],
    )
    def test_legitimate_urls_are_untouched(self, href: str) -> None:
        cleaned = self._clean(f'<a href="{href}">x</a>')
        assert f'href="{href}"' in cleaned, cleaned

    def test_an_image_source_is_guarded_too(self) -> None:
        cleaned = self._clean('<img src="\x01//evil.example/x.png" alt="a">')
        assert "evil.example" not in cleaned

    def test_c1_controls_are_deliberately_not_stripped(self) -> None:
        from freva_rest.settings_api.sanitizer import _as_a_browser_reads_it

        assert _as_a_browser_reads_it("\x85//evil.example") == "\x85//evil.example"
        assert _as_a_browser_reads_it("\x01//evil.example") == "//evil.example"

    def test_the_normaliser_matches_the_whatwg_range(self) -> None:
        from freva_rest.settings_api.sanitizer import _as_a_browser_reads_it

        for code in range(0x00, 0x21):
            assert _as_a_browser_reads_it(f"{chr(code)}//x") == "//x", hex(code)
        assert _as_a_browser_reads_it("\x21//x") == "\x21//x"


class TestMixedContentReferencesDoNotCrash:
    """
    The same page may legitimately be named twice with different
    expectations
    """

    @staticmethod
    def _sorted(wanted):
        return sorted(set(wanted), key=lambda item: (item[0], item[1], item[2] or ""))

    def test_the_bare_sort_really_does_raise(self) -> None:
        wanted = [("default", "home", None), ("default", "home", "rendered")]
        with pytest.raises(TypeError):
            sorted(set(wanted))

    def test_the_keyed_sort_handles_it(self) -> None:
        wanted = [("default", "home", None), ("default", "home", "rendered")]
        assert self._sorted(wanted) == [
            ("default", "home", None),
            ("default", "home", "rendered"),
        ]

    def test_the_endpoint_uses_a_key(self) -> None:
        import ast
        import inspect
        import textwrap

        from freva_rest.settings_api import endpoints

        source = textwrap.dedent(inspect.getsource(endpoints._make_ref_validator))
        tree = ast.parse(source)
        sorts = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "sorted"
        ]
        assert sorts, "no sorted() call found"
        for call in sorts:
            assert any(
                keyword.arg == "key" for keyword in call.keywords
            ), "sorted() over reference tuples must pass an explicit key"

    def test_both_assertions_are_kept_not_collapsed(self) -> None:
        wanted = [("default", "home", None), ("default", "home", "rendered")]
        assert len(self._sorted(wanted)) == 2

    def test_duplicates_are_still_deduplicated(self) -> None:
        wanted = [("default", "home", None)] * 5 + [("default", "home", "rendered")] * 3
        assert len(self._sorted(wanted)) == 2

    @staticmethod
    async def _validate(manifest, stored_format="markdown"):
        from unittest import mock

        from freva_rest.settings_api import endpoints
        from freva_rest.settings_api.schema import UiConfig

        async def _formats(keys):
            return {key: stored_format for key in keys}

        validator = endpoints._make_ref_validator("ui")
        assert validator is not None
        model = UiConfig.model_validate(manifest)
        with mock.patch.object(endpoints, "_content_formats", _formats):
            await validator(model)

    def test_content_refs_plus_a_content_route_succeeds(self) -> None:
        import asyncio

        asyncio.run(
            self._validate(
                {
                    "content_refs": [{"ui_id": "default", "content_id": "home"}],
                    "routes": [
                        {
                            "kind": "content",
                            "id": "home",
                            "ui_id": "default",
                            "content_id": "home",
                            "path": "/home",
                        }
                    ],
                }
            )
        )

    def test_content_refs_plus_header_and_footer_succeeds(self) -> None:
        import asyncio

        asyncio.run(
            self._validate(
                {
                    "content_refs": [{"ui_id": "default", "content_id": "home"}],
                    "header": {"content": {"ui_id": "default", "content_id": "home"}},
                    "footer": {"content": {"ui_id": "default", "content_id": "home"}},
                }
            )
        )

    def test_rendered_and_sandbox_for_one_id_is_a_controlled_422(self) -> None:
        import asyncio

        from fastapi import HTTPException

        manifest = {
            "content_refs": [{"ui_id": "default", "content_id": "home"}],
            "routes": [
                {
                    "kind": "content",
                    "id": "a",
                    "ui_id": "default",
                    "content_id": "home",
                    "path": "/a",
                },
                {
                    "kind": "sandbox",
                    "id": "b",
                    "ui_id": "default",
                    "content_id": "home",
                    "path": "/b",
                },
            ],
        }
        with pytest.raises(HTTPException) as raised:
            asyncio.run(self._validate(manifest, stored_format="markdown"))
        assert raised.value.status_code == 422
        assert "incompatible" in str(raised.value.detail)


class TestUnserialisableBodiesAreRefusedNotCrashed:
    """
    A lone surrogate is valid json and cannot be encoded as UTF-8.
    """

    JSON = {"Content-Type": "application/json"}

    @staticmethod
    def _app(*, guarded: bool):
        from fastapi import Depends, FastAPI
        from fastapi.testclient import TestClient

        from freva_rest.settings_api.endpoints import (
            reject_unserialisable_body,
        )
        from freva_rest.settings_api.registry import REGISTRY

        app = FastAPI()
        guard = [Depends(reject_unserialisable_body)] if guarded else []

        @app.patch("/ui", dependencies=guard)
        async def ui(
            payload: REGISTRY["ui"].update_model,
        ) -> dict:
            return {"ok": True}

        @app.patch("/content", dependencies=guard)
        async def content(payload: ContentSource) -> dict:  
            return {"ok": True}

        return TestClient(app, raise_server_exceptions=False)

    @pytest.mark.parametrize(
        "route,body",
        [
            ("/ui", '{"site_title": "\\ud800"}'),
            ("/ui", '{"main_color": "\\udfff"}'),
            ("/ui", '{"public_extensions": {"\\ud800": "x"}}'),
            ("/ui", '{"public_extensions": {"a": "\\ud800"}}'),
            ("/ui", '{"public_extensions": {"a": ["ok", "\\ud800"]}}'),
            ("/content", '{"format": "markdown", "source": "\\ud800"}'),
            ("/content", '{"format": "markdown", "title": "\\udfff"}'),
        ],
    )
    def test_the_unguarded_route_really_does_500(self, route: str, body: str) -> None:
        response = self._app(guarded=False).patch(
            route, content=body, headers=self.JSON
        )
        assert response.status_code in (200, 500), response.status_code

    @pytest.mark.parametrize(
        "route,body",
        [
            ("/ui", '{"site_title": "\\ud800"}'),
            ("/ui", '{"main_color": "\\udfff"}'),
            ("/ui", '{"public_extensions": {"\\ud800": "x"}}'),
            ("/ui", '{"public_extensions": {"a": "\\ud800"}}'),
            ("/ui", '{"public_extensions": {"a": ["ok", "\\ud800"]}}'),
            (
                "/ui",
                '{"routes": [{"kind": "external", "path": "/a", "url": "\\ud800"}]}',
            ),
            ("/content", '{"format": "markdown", "source": "\\ud800"}'),
            ("/content", '{"format": "markdown", "title": "\\udfff"}'),
        ],
    )
    def test_the_guarded_route_returns_422(self, route: str, body: str) -> None:
        response = self._app(guarded=True).patch(route, content=body, headers=self.JSON)
        assert response.status_code == 422, response.status_code

    def test_the_message_does_not_echo_the_invalid_string(self) -> None:
        from freva_rest.settings_api.endpoints import UNENCODABLE_DETAIL

        response = self._app(guarded=True).patch(
            "/ui", content='{"site_title": "\\ud800"}', headers=self.JSON
        )
        assert response.json()["detail"] == UNENCODABLE_DETAIL
        assert "\ud800" not in response.text
        assert response.content.decode("utf-8")

    def test_the_refusal_is_not_cacheable(self) -> None:
        response = self._app(guarded=True).patch(
            "/ui", content='{"site_title": "\\ud800"}', headers=self.JSON
        )
        assert response.headers["Cache-Control"] == "private, no-store"

    @pytest.mark.parametrize(
        "body",
        [
            '{"site_title": "\\ud83d\\ude00 emoji"}',
            '{"site_title": "caf\\u00e9"}',
            '{"site_title": "\\u4e2d\\u6587"}',
            '{"public_extensions": {"\\u00e9": "\\ud83d\\ude00"}}',
        ],
    )
    def test_valid_supplementary_characters_still_work(self, body: str) -> None:
        response = self._app(guarded=True).patch("/ui", content=body, headers=self.JSON)
        assert response.status_code == 200, response.text

    def test_malformed_json_is_left_to_fastapi(self) -> None:
        from freva_rest.settings_api.endpoints import UNENCODABLE_DETAIL

        response = self._app(guarded=True).patch(
            "/ui", content="not json", headers=self.JSON
        )
        assert response.status_code == 422
        assert UNENCODABLE_DETAIL not in response.text

    def test_a_deeply_nested_body_does_not_recurse(self) -> None:
        import asyncio

        from freva_rest.settings_api.endpoints import _require_encodable

        document: object = "leaf"
        for _ in range(50_000):
            document = [document]
        asyncio.run(asyncio.sleep(0))
        _require_encodable(document)

    def test_both_write_routes_carry_the_guard(self) -> None:
        from freva_rest.rest import app
        from freva_rest.settings_api import endpoints

        guarded = 0
        for route in app.routes:
            if "PATCH" not in getattr(route, "methods", ()) or (
                "/settings" not in getattr(route, "path", "")
            ):
                continue
            calls = [dep.call for dep in route.dependant.dependencies]
            assert endpoints.reject_unserialisable_body in calls, route.path
            guarded += 1
        assert guarded == 2


class TestAdminIsCheckedBeforeTheBody:
    """401 then 403 then anything about the body. An admin check inside the
    handler runs after body-model validation, letting an authenticated non-admin
    reach validation on a route they were never allowed to call."""

    def test_the_write_routes_depend_on_it(self) -> None:
        from freva_rest.rest import app
        from freva_rest.settings_api import endpoints

        checked = 0
        for route in app.routes:
            methods = getattr(route, "methods", set())
            if "/settings" not in getattr(route, "path", ""):
                continue
            if not methods & {"PATCH", "POST", "DELETE"}:
                continue
            calls = [dep.call for dep in route.dependant.dependencies]
            assert endpoints.require_admin in calls, route.path
            checked += 1
        assert checked == 5

    def test_it_runs_before_the_body_guard(self) -> None:
        from freva_rest.rest import app
        from freva_rest.settings_api import endpoints

        for route in app.routes:
            if "PATCH" not in getattr(route, "methods", ()) or (
                "/settings" not in getattr(route, "path", "")
            ):
                continue
            calls = [dep.call for dep in route.dependant.dependencies]
            assert calls.index(endpoints.require_admin) < calls.index(
                endpoints.reject_unserialisable_body
            ), route.path

    def test_authentication_comes_first_inside_it(self) -> None:
        import inspect

        from freva_rest.settings_api import endpoints

        signature = inspect.signature(endpoints.require_admin)
        assert "current_user" in signature.parameters

    def test_a_non_admin_is_refused_with_neutral_wording(self) -> None:
        from unittest import mock

        from fastapi import HTTPException

        from freva_rest.settings_api import endpoints

        class _Config:
            admin_token_claims = {"roles": ["^admin$"]}

            @staticmethod
            def is_admin_user(_user):
                return False

        with mock.patch.object(endpoints, "server_config", _Config()):
            with pytest.raises(HTTPException) as raised:
                endpoints.require_admin(current_user=object())
        assert raised.value.status_code == 403
        detail = str(raised.value.detail)
        assert detail == "Only administrators may perform this operation."
        assert "change" not in detail

    def test_an_admin_passes_through(self) -> None:
        from unittest import mock

        from freva_rest.settings_api import endpoints

        class _Config:
            admin_token_claims = {"roles": ["^admin$"]}

            @staticmethod
            def is_admin_user(_user):
                return True

        token = object()
        with mock.patch.object(endpoints, "server_config", _Config()):
            assert endpoints.require_admin(current_user=token) is token


class TestRfc3339FractionsAreInterpreterIndependent:
    """The grammar accepts one to nine fractional digits; python 3.10's
    `fromisoformat` accepts only three or six. The project supports 3.10, so
    `2026-05-01T09:00:00.5Z` must not be accepted or refused depending on
    which interpreter the worker happens to run, nor refused with a message
    blaming the calendar."""

    @staticmethod
    def _as_python_310(monkeypatch_target):
        """A `fromisoformat` with python 3.10's fractional-digit rule."""
        import re
        from datetime import datetime as _real

        class _Py310(_real):
            @classmethod
            def fromisoformat(cls, text):
                match = re.search(r"\.(\d+)", text)
                if match and len(match.group(1)) not in (3, 6):
                    raise ValueError(f"Invalid isoformat string: {text!r}")
                return _real.fromisoformat(text)

        return _Py310

    @pytest.mark.parametrize("digits", list(range(1, 10)))
    def test_every_fraction_length_parses_on_this_interpreter(
        self, digits: int
    ) -> None:
        from freva_rest.settings_api.field_types import _parse_rfc3339

        value = "2026-05-01T09:00:00." + "5" * digits + "Z"
        assert _parse_rfc3339(value).startswith("2026-05-01T09:00:00.5")

    @pytest.mark.parametrize("digits", list(range(1, 10)))
    def test_every_fraction_length_parses_under_python_310_rules(
        self, digits: int
    ) -> None:
        from unittest import mock

        from freva_rest.settings_api import field_types

        value = "2026-05-01T09:00:00." + "5" * digits + "Z"
        with mock.patch.object(
            field_types, "datetime", self._as_python_310(field_types)
        ):
            assert field_types._parse_rfc3339(value).endswith("+00:00")

    def test_one_fraction_digit_is_padded_under_python_310_rules(self) -> None:
        from unittest import mock

        from freva_rest.settings_api import field_types

        with mock.patch.object(
            field_types, "datetime", self._as_python_310(field_types)
        ):
            assert (
                field_types._parse_rfc3339("2026-05-01T09:00:00.5Z")
                == "2026-05-01T09:00:00.500000+00:00"
            )

    @pytest.mark.parametrize(
        "fraction,expected",
        [
            ("5", "500000"),
            ("55", "550000"),
            ("555", "555000"),
            ("555555", "555555"),
            ("5555555", "555555"),
            ("555555555", "555555"),
        ],
    )
    def test_padding_and_truncation_match_the_newer_interpreter(
        self, fraction: str, expected: str
    ) -> None:
        from freva_rest.settings_api.field_types import _six_digit_fraction

        rewritten = _six_digit_fraction(f"2026-05-01T09:00:00.{fraction}Z")
        assert rewritten == f"2026-05-01T09:00:00.{expected}Z"

    def test_a_value_with_no_fraction_is_untouched(self) -> None:
        from freva_rest.settings_api.field_types import _six_digit_fraction

        for value in ("2026-05-01T09:00:00Z", "2026-05-01T09:00:00+02:00"):
            assert _six_digit_fraction(value) == value

    def test_the_offset_is_not_mistaken_for_a_fraction(self) -> None:
        from freva_rest.settings_api.field_types import _parse_rfc3339

        assert _parse_rfc3339("2026-05-01T09:00:00.5+02:00") == (
            "2026-05-01T09:00:00.500000+02:00"
        )

    def test_the_input_contract_is_unchanged(self) -> None:
        from freva_rest.settings_api.field_types import _parse_rfc3339

        with pytest.raises(ValueError):
            _parse_rfc3339("2026-05-01T09:00:00.1234567890Z")  # ten digits
        with pytest.raises(ValueError):
            _parse_rfc3339("2026-05-01T09:00:00,5Z")  # comma

    def test_a_calendar_error_is_still_a_calendar_error(self) -> None:
        from freva_rest.settings_api.field_types import _parse_rfc3339

        with pytest.raises(ValueError) as raised:
            _parse_rfc3339("2026-02-31T09:00:00.5Z")
        assert "not a real date" in str(raised.value)

    def test_a_grammar_error_is_still_a_grammar_error(self) -> None:
        from freva_rest.settings_api.field_types import _parse_rfc3339

        with pytest.raises(ValueError) as raised:
            _parse_rfc3339("2026-05-01 09:00:00.5Z")
        assert "RFC 3339" in str(raised.value)


class TestStorageFieldsAreReservedFromModels:
    """`**candidate` would let a future resource model overwrite the
    document's identity or its compare-and-swap state. A model declaring
    `revision` would write its own value into the field the CAS predicate
    matches on, and every later write to that record would fail its own
    predicate."""

    @staticmethod
    def _model(**fields):
        from pydantic import BaseModel, create_model

        return create_model("Candidate", __base__=BaseModel, **fields)

    def test_the_reserved_set_covers_the_storage_fields(self) -> None:
        from freva_rest.settings_api.core import (
            CAS_TOKEN_FIELD,
            RESERVED_STORAGE_FIELDS,
        )

        assert set(RESERVED_STORAGE_FIELDS) == {
            "_id",
            "resource_name",
            "record_id",
            "revision",
            CAS_TOKEN_FIELD,
        }

    @pytest.mark.parametrize(
        "field", ["revision", "cas_token", "record_id", "resource_name"]
    )
    def test_a_model_declaring_one_is_refused(self, field: str) -> None:
        from freva_rest.settings_api.core import check_reserved_fields

        model = self._model(**{field: (int, 0)})
        with pytest.raises(RuntimeError) as raised:
            check_reserved_fields("demo", model)
        assert field in str(raised.value)
        assert "demo" in str(raised.value)

    def test_an_ordinary_model_is_accepted(self) -> None:
        from freva_rest.settings_api.core import check_reserved_fields

        check_reserved_fields("demo", self._model(site_title=(str, "")))

    def test_the_registry_is_checked_at_import(self) -> None:
        import ast
        import inspect

        from freva_rest.settings_api import registry

        source = inspect.getsource(registry)
        assert "check_reserved_fields" in source
        tree = ast.parse(source)
        top_level = [
            node
            for node in tree.body
            if isinstance(node, ast.For)
            and "check_reserved_fields" in ast.unparse(node)
        ]
        assert top_level, "the registry check must run at import"

    def test_the_registered_resources_pass(self) -> None:
        from freva_rest.settings_api.core import check_reserved_fields
        from freva_rest.settings_api.registry import REGISTRY

        for name, resource in REGISTRY.items():
            check_reserved_fields(name, resource.model, resource.update_model)

    def test_identity_is_written_last(self) -> None:
        # defence in depth: even a model that reached the writer could not
        # overwrite the identity, because it is spread first
        import inspect

        from freva_rest.settings_api.core import SettingsStore

        source = inspect.getsource(SettingsStore.patch)
        stored = source[source.index("stored = {") :]
        stored = stored[: stored.index("}")]
        assert stored.index("**candidate") < stored.index('"_id"')
        assert stored.index("**candidate") < stored.index('"revision"')


class TestWarningLatchesUseRequestedIdentity:
    """
    `_doc_latch` takes its identity from the caller.
    """

    def test_two_damaged_documents_get_separate_latches(self) -> None:
        from freva_rest.settings_api.core import _doc_latch

        damaged = {"format": [], "revision": 1}
        first = _doc_latch("content:a:one", damaged, "format")
        second = _doc_latch("content:a:two", damaged, "format")
        assert first != second
        assert first.startswith("content:a:one")
        assert second.startswith("content:a:two")

    def test_a_document_derived_latch_collapses_them(self) -> None:
        damaged = {"format": [], "revision": 1}
        collapsed = (
            f"content:{damaged.get('ui_id')}:{damaged.get('content_id')}:"
            f"{damaged.get('revision')}:format"
        )
        assert collapsed == "content:None:None:1:format"

    def test_both_warnings_are_emitted_independently(self) -> None:
        from unittest import mock

        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            _doc_latch,
            _warn_once,
            reset_caches,
        )

        reset_caches()
        damaged = {"format": [], "revision": 1}
        with mock.patch.object(core.logger, "error") as warned:
            _warn_once(_doc_latch("content:a:one", damaged, "format"), "broken %s", 1)
            _warn_once(_doc_latch("content:a:two", damaged, "format"), "broken %s", 2)
        assert warned.call_count == 2

    def test_repairing_one_clears_only_its_latch(self) -> None:
        from freva_rest.settings_api import core
        from freva_rest.settings_api.core import (
            _doc_latch,
            _warn_forget,
            _warn_once,
            reset_caches,
        )

        reset_caches()
        damaged = {"format": [], "revision": 1}
        one = _doc_latch("content:a:one", damaged, "format")
        two = _doc_latch("content:a:two", damaged, "format")
        _warn_once(one, "broken")
        _warn_once(two, "broken")
        _warn_forget("content:a:one")
        assert one not in core._WARNED
        assert two in core._WARNED

    def test_a_later_malformed_generation_warns_again(self) -> None:
        from freva_rest.settings_api.core import CAS_TOKEN_FIELD, _doc_latch

        first = _doc_latch("content:a:one", {CAS_TOKEN_FIELD: "g1"}, "format")
        second = _doc_latch("content:a:one", {CAS_TOKEN_FIELD: "g2"}, "format")
        assert first != second
        assert first.startswith("content:a:one") and second.startswith("content:a:one")

    def test_the_read_view_takes_the_identity_from_its_caller(self) -> None:
        import inspect

        from freva_rest.settings_api.core import sanitize_read_view

        parameters = list(inspect.signature(sanitize_read_view).parameters)
        assert parameters[0] == "identity"


class TestContentRefsAreBounded:
    """Every manifest collection carries a per-collection bound, and each
    reference is looked up and compatibility-checked inside the compare-and-swap
    retry loop."""

    @staticmethod
    def _refs(count):
        return [{"ui_id": "default", "content_id": "home"} for _ in range(count)]

    def test_the_bound_matches_the_lookup_chunk(self) -> None:
        from freva_rest.settings_api.endpoints import CONTENT_LOOKUP_CHUNK
        from freva_rest.settings_api.schema import MAX_CONTENT_REFS

        assert MAX_CONTENT_REFS == 200
        assert MAX_CONTENT_REFS == CONTENT_LOOKUP_CHUNK

    @pytest.mark.parametrize("model_name", ["UiConfig", "UiConfigUpdate"])
    def test_both_models_enforce_it(self, model_name: str) -> None:
        from freva_rest.settings_api import schema

        model = getattr(schema, model_name)
        with pytest.raises(ValidationError):
            model.model_validate(
                {"content_refs": self._refs(schema.MAX_CONTENT_REFS + 1)}
            )

    @pytest.mark.parametrize("model_name", ["UiConfig", "UiConfigUpdate"])
    def test_the_limit_itself_is_accepted(self, model_name: str) -> None:
        from freva_rest.settings_api import schema

        model = getattr(schema, model_name)
        validated = model.model_validate(
            {"content_refs": self._refs(schema.MAX_CONTENT_REFS)}
        )
        assert len(validated.content_refs) == schema.MAX_CONTENT_REFS

    @pytest.mark.parametrize("model_name", ["UiConfig", "UiConfigUpdate"])
    def test_the_openapi_schema_carries_maxitems(self, model_name: str) -> None:
        from freva_rest.settings_api import schema

        model = getattr(schema, model_name)
        spec = model.model_json_schema(by_alias=True)["properties"]["content_refs"]
        # the update model wraps it in an anyOf for Optional
        text = str(spec)
        assert f"'maxItems': {schema.MAX_CONTENT_REFS}" in text, text

    def test_it_matches_how_the_other_collections_are_bounded(self) -> None:
        from freva_rest.settings_api import schema

        for name in ("MAX_ROUTES", "MAX_LANDING_BLOCKS", "MAX_ANNOUNCEMENTS"):
            assert isinstance(getattr(schema, name), int)
        assert isinstance(schema.MAX_CONTENT_REFS, int)


class TestCreatingWithoutASourceSaysSo:
    """Creating content with no `source` must not report a damaged stored
    document whose rendering was preserved: there is no stored document."""

    @staticmethod
    def _store(collection):
        from freva_rest.settings_api.core import ContentStore, reset_caches

        reset_caches()
        return ContentStore(_FakeConfig(collection), "a", "b")

    def test_creating_without_a_source_is_a_creation_error(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.schema import ContentSource

        store = self._store(_FakeCollection())
        with pytest.raises(HTTPException) as raised:
            asyncio.run(
                store.patch(ContentSource.model_validate({"format": "markdown"}))
            )
        assert raised.value.status_code == 422
        detail = str(raised.value.detail)
        assert detail == (
            "A source is required when creating a content document. "
            "Send 'source' together with 'format'."
        )

    def test_it_does_not_claim_a_stored_document_exists(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.schema import ContentSource

        store = self._store(_FakeCollection())
        with pytest.raises(HTTPException) as raised:
            asyncio.run(
                store.patch(ContentSource.model_validate({"format": "markdown"}))
            )
        detail = str(raised.value.detail)
        for untrue in ("stored document", "left untouched", "repair"):
            assert untrue not in detail, untrue

    def test_nothing_is_written(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        store = self._store(collection)
        with pytest.raises(HTTPException):
            asyncio.run(
                store.patch(ContentSource.model_validate({"format": "markdown"}))
            )
        assert collection.docs == {}

    def test_the_damaged_document_message_is_kept_for_a_patch(self) -> None:
        import asyncio

        from fastapi import HTTPException

        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        collection.docs["a:b"] = {
            "_id": "a:b",
            "ui_id": "a",
            "content_id": "b",
            "format": "markdown",
            "rendered_html": "<p>kept</p>",
            "revision": 1,
        }
        store = self._store(collection)
        with pytest.raises(HTTPException) as raised:
            asyncio.run(store.patch(ContentSource.model_validate({"title": "Renamed"})))
        detail = str(raised.value.detail)
        assert "stored document has no 'source'" in detail
        assert "left untouched" in detail
        assert collection.docs["a:b"]["rendered_html"] == "<p>kept</p>"

    def test_creating_with_a_source_still_works(self) -> None:
        import asyncio

        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        store = self._store(collection)
        asyncio.run(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )
        assert collection.docs["a:b"]["source"] == "# a"

    def test_creating_with_an_empty_source_is_allowed(self) -> None:
        import asyncio

        from freva_rest.settings_api.schema import ContentSource

        collection = _FakeCollection()
        store = self._store(collection)
        asyncio.run(
            store.patch(
                ContentSource.model_validate({"format": "markdown", "source": ""})
            )
        )
        assert collection.docs["a:b"]["source"] == ""


class TestPublicContentReadIsDeclaredOptional:
    """
    The merged content `GET` is public; authentication only selects the
    `include_source=true` variant.
    """

    PATH = "/api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}"

    def test_the_operation_is_listed(self) -> None:
        from freva_rest.settings_api.endpoints import OPTIONAL_AUTH_OPERATIONS

        assert (self.PATH, "get") in OPTIONAL_AUTH_OPERATIONS

    def test_the_transform_prepends_the_anonymous_alternative(self) -> None:
        from freva_rest.settings_api.endpoints import _declare_optional_auth

        schema = {"paths": {self.PATH: {"get": {"security": [{"AnyBearerName": []}]}}}}
        result = _declare_optional_auth(schema)["paths"][self.PATH]["get"]["security"]
        assert result == [{}, {"AnyBearerName": []}]

    def test_the_scheme_name_is_taken_from_the_generated_document(self) -> None:
        import inspect

        from freva_rest.settings_api import endpoints

        source = inspect.getsource(endpoints._declare_optional_auth)
        for guess in ("OAuth2", "Bearer", "HTTPBearer", "oidc"):
            assert guess not in source, guess

    def test_it_is_idempotent(self) -> None:
        from freva_rest.settings_api.endpoints import _declare_optional_auth

        schema = {"paths": {self.PATH: {"get": {"security": [{"S": []}]}}}}
        once = _declare_optional_auth(schema)
        twice = _declare_optional_auth(once)
        assert twice["paths"][self.PATH]["get"]["security"] == [{}, {"S": []}]

    def test_an_operation_with_no_requirement_is_left_alone(self) -> None:
        from freva_rest.settings_api.endpoints import _declare_optional_auth

        schema = {"paths": {self.PATH: {"get": {}}}}
        assert (
            "security" not in _declare_optional_auth(schema)["paths"][self.PATH]["get"]
        )

    def test_a_missing_operation_is_not_an_error(self) -> None:
        from freva_rest.settings_api.endpoints import _declare_optional_auth

        assert _declare_optional_auth({"paths": {}}) == {"paths": {}}

    def test_only_the_listed_operation_is_rewritten(self) -> None:
        from freva_rest.settings_api.endpoints import _declare_optional_auth

        other = "/api/freva-nextgen/settings/{resource}/{record_id}"
        schema = {
            "paths": {
                self.PATH: {"get": {"security": [{"S": []}]}},
                other: {"patch": {"security": [{"S": []}]}},
            }
        }
        result = _declare_optional_auth(schema)["paths"]
        assert result[self.PATH]["get"]["security"] == [{}, {"S": []}]
        assert result[other]["patch"]["security"] == [{"S": []}]

    def test_the_generated_document_agrees_when_a_scheme_is_installed(self) -> None:
        import freva_rest.settings_api.endpoints
        from freva_rest.rest import app

        spec = app.openapi()
        security = spec["paths"][self.PATH]["get"].get("security")
        if not security:
            pytest.skip("no security scheme installed in this harness")
        assert {} in security, security
        assert any(requirement for requirement in security), "bearer alternative lost"

    def test_a_genuinely_required_operation_keeps_only_the_bearer(self) -> None:
        import freva_rest.settings_api.endpoints
        from freva_rest.rest import app

        spec = app.openapi()
        write = spec["paths"][self.PATH].get("patch", {}).get("security")
        if not write:
            pytest.skip("no security scheme installed in this harness")
        assert {} not in write, write

    def test_generating_twice_does_not_accumulate(self) -> None:
        import freva_rest.settings_api.endpoints
        from freva_rest.rest import app

        first = app.openapi()["paths"][self.PATH]["get"].get("security")
        app.openapi_schema = None  # force a regeneration
        second = app.openapi()["paths"][self.PATH]["get"].get("security")
        assert first == second

    def test_it_works_on_a_real_fastapi_security_scheme(self) -> None:
        from typing import Optional as _Optional

        from fastapi import Depends, FastAPI
        from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

        from freva_rest.settings_api.endpoints import _declare_optional_auth

        bearer = HTTPBearer(scheme_name="SomeDeploymentScheme", auto_error=False)
        probe = FastAPI()

        @probe.get(self.PATH)
        async def read(  
            credentials: _Optional[HTTPAuthorizationCredentials] = Depends(bearer),
        ) -> dict:
            return {"ok": True}

        generated = probe.openapi()
        before = generated["paths"][self.PATH]["get"]["security"]
        assert before == [{"SomeDeploymentScheme": []}]
        after = _declare_optional_auth(generated)["paths"][self.PATH]["get"]["security"]
        assert after == [{}, {"SomeDeploymentScheme": []}]



class _EndpointConfig:
    """The slice of ServerConfig the endpoint module actually touches."""

    def __init__(self, settings=None, contents=None, admin=True):
        self._settings = settings if settings is not None else _FakeCollection()
        self._contents = contents if contents is not None else _FakeCollection()
        self.admin_token_claims = {"roles": ["^admin$"]} if admin else None
        self._admin = admin

    @property
    def mongo_collection_settings(self):
        return self._settings

    @property
    def mongo_collection_ui_contents(self):
        return self._contents

    def is_admin_user(self, _user):
        return self._admin


class _Request:
    """Just enough Request for the handlers: a headers mapping."""

    def __init__(self, headers=None):
        self.headers = headers or {}


@contextlib.contextmanager
def _endpoints_with(config):
    from freva_rest.settings_api import core, endpoints

    core.reset_caches()
    original = endpoints.server_config
    endpoints.server_config = config
    try:
        yield endpoints
    finally:
        endpoints.server_config = original
        core.reset_caches()


def _seed_content_doc(collection, ui_id="default", content_id="home", **extra):
    from freva_rest.settings_api.renderers import rendered_hash, source_hash
    from freva_rest.settings_api.sanitizer import RENDERER_FINGERPRINT

    html = extra.pop("rendered_html", "<p>a</p>")
    source = extra.pop("source", "# a")
    fmt = extra.pop("format", "markdown")
    doc = {
        "_id": f"{ui_id}:{content_id}",
        "ui_id": ui_id,
        "content_id": content_id,
        "format": fmt,
        "source": source,
        "source_hash": source_hash(source, fmt),
        "rendered_html": html,
        "rendered_hash": rendered_hash(html),
        "renderer_version": RENDERER_FINGERPRINT,
        "revision": 1,
        "cas_token": "tok",
        "title": "",
        **extra,
    }
    collection.docs[doc["_id"]] = doc
    return doc


class TestEndpointHelpers:
    """The small helpers every route is built from."""

    def test_require_admin_without_a_configured_filter(self) -> None:
        from fastapi import HTTPException

        with _endpoints_with(_EndpointConfig(admin=False)) as endpoints:
            endpoints.server_config.admin_token_claims = None
            with pytest.raises(HTTPException) as raised:
                endpoints._require_admin(object())
        assert raised.value.status_code == 403
        assert "API_ADMIN_TOKEN_CLAIMS" in str(raised.value.detail)

    def test_settings_store_rejects_an_unknown_resource(self) -> None:
        from fastapi import HTTPException

        with _endpoints_with(_EndpointConfig()) as endpoints:
            with pytest.raises(HTTPException) as raised:
                endpoints._settings_store("nope", "default")
        assert raised.value.status_code == 404
        assert "nope" in str(raised.value.detail)

    @pytest.mark.parametrize("record_id", ["Bad", "_leading", "with.dot", "x" * 100])
    def test_settings_store_rejects_a_bad_record_id(self, record_id: str) -> None:
        from fastapi import HTTPException

        with _endpoints_with(_EndpointConfig()) as endpoints:
            with pytest.raises(HTTPException) as raised:
                endpoints._settings_store("ui", record_id)
        assert raised.value.status_code == 422

    def test_settings_store_builds_a_store(self) -> None:
        with _endpoints_with(_EndpointConfig()) as endpoints:
            store = endpoints._settings_store("ui", "default")
        assert store.resource_name == "ui" and store.record_id == "default"

    @pytest.mark.parametrize(
        "ui_id,content_id",
        [("BAD", "home"), ("default", "BAD"), ("_nope", "home")],
    )
    def test_content_store_rejects_bad_ids(self, ui_id: str, content_id: str) -> None:
        from fastapi import HTTPException

        with _endpoints_with(_EndpointConfig()) as endpoints:
            with pytest.raises(HTTPException) as raised:
                endpoints._content_store(ui_id, content_id)
        assert raised.value.status_code == 422

    def test_content_store_builds_a_store(self) -> None:
        with _endpoints_with(_EndpointConfig()) as endpoints:
            store = endpoints._content_store("_shared", "home")
        assert store.ui_id == "_shared" and store.content_id == "home"

    def test_conditional_returns_the_body(self) -> None:
        with _endpoints_with(_EndpointConfig()) as endpoints:
            response = endpoints._conditional(b"{}", '"e"', _Request(), "public")
        assert response.status_code == 200
        assert response.headers["ETag"] == '"e"'
        assert response.headers["Cache-Control"] == "public"

    def test_conditional_returns_304_when_the_tag_matches(self) -> None:
        with _endpoints_with(_EndpointConfig()) as endpoints:
            response = endpoints._conditional(
                b"{}", '"e"', _Request({"if-none-match": '"e"'}), "public"
            )
        assert response.status_code == 304
        assert response.headers["ETag"] == '"e"'

    def test_written_is_never_shared_cached(self) -> None:
        with _endpoints_with(_EndpointConfig()) as endpoints:
            response = endpoints._written(b"{}", '"e"')
        assert response.headers["Cache-Control"] == "private, no-store"

    def test_record_label_uses_the_record_id(self) -> None:
        from freva_rest.settings_api.endpoints import _record_label

        assert _record_label({"record_id": "waterpark"}) == "ui/waterpark"

    @pytest.mark.parametrize("broken", [{}, {"record_id": None}, {"record_id": 7}])
    def test_record_label_survives_a_damaged_record_id(self, broken: dict) -> None:
        from freva_rest.settings_api.endpoints import _record_label

        assert _record_label(broken) == "ui/?"

    def test_a_body_that_is_not_json_returns_early(self) -> None:
        import asyncio

        from freva_rest.settings_api.endpoints import (
            reject_unserialisable_body,
        )

        class _Body:
            async def body(self):
                return b"not json at all"

        asyncio.run(reject_unserialisable_body(_Body()))


class TestSchemaEndpoint:
    def test_an_unknown_resource_is_404(self) -> None:
        import asyncio

        from fastapi import HTTPException

        with _endpoints_with(_EndpointConfig()) as endpoints:
            with pytest.raises(HTTPException) as raised:
                asyncio.run(endpoints.get_settings_schema(_Request(), resource="nope"))
        assert raised.value.status_code == 404

    @pytest.mark.parametrize("variant", ["read", "update"])
    def test_it_serves_the_model_schema(self, variant: str) -> None:
        import asyncio
        import json

        with _endpoints_with(_EndpointConfig()) as endpoints:
            response = asyncio.run(
                endpoints.get_settings_schema(
                    _Request(), resource="ui", variant=variant
                )
            )
        assert response.status_code == 200
        assert "properties" in json.loads(response.body)
        assert response.headers["ETag"].startswith('"')

    def test_the_etag_is_stable_and_conditional(self) -> None:
        import asyncio

        with _endpoints_with(_EndpointConfig()) as endpoints:
            first = asyncio.run(
                endpoints.get_settings_schema(_Request(), resource="ui")
            )
            again = asyncio.run(
                endpoints.get_settings_schema(
                    _Request({"if-none-match": first.headers["ETag"]}), resource="ui"
                )
            )
        assert again.status_code == 304


class TestSettingsRecordEndpoints:
    def test_a_get_of_the_default_synthesises(self) -> None:
        import asyncio
        import json

        with _endpoints_with(_EndpointConfig()) as endpoints:
            response = asyncio.run(
                endpoints.get_settings_record(
                    _Request(), resource="ui", record_id="default"
                )
            )
        assert response.status_code == 200
        assert json.loads(response.body)["site_title"] == "Freva"

    def test_a_patch_writes_and_returns_the_record(self) -> None:
        import asyncio
        import json

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            response = asyncio.run(
                endpoints.patch_settings_record(
                    payload=UiConfigUpdate.model_validate({"site_title": "Set"}),
                    resource="ui",
                    record_id="default",
                    current_user=object(),
                    _wire_safe_body=None,
                    if_match=None,
                )
            )
        assert json.loads(response.body)["site_title"] == "Set"
        assert response.headers["Cache-Control"] == "private, no-store"

    def test_a_patch_the_resource_model_refuses_is_422(self) -> None:
        import asyncio

        from fastapi import HTTPException

        class _Rejecting:
            def model_dump(self, **_kw):
                return {"main_color": "not-a-colour"}

        with _endpoints_with(_EndpointConfig()) as endpoints:
            with pytest.raises(HTTPException) as raised:
                asyncio.run(
                    endpoints.patch_settings_record(
                        payload=_Rejecting(),
                        resource="ui",
                        record_id="default",
                        current_user=object(),
                        _wire_safe_body=None,
                        if_match=None,
                    )
                )
        assert raised.value.status_code == 422


class TestDeleteSettingsRecordEndpoint:
    @staticmethod
    def _delete(endpoints, record_id="default", if_match=None):
        import asyncio

        return asyncio.run(
            endpoints.delete_settings_record(
                resource="ui",
                record_id=record_id,
                current_user=object(),
                if_match=if_match,
            )
        )

    def test_a_missing_named_record_is_404(self) -> None:
        from fastapi import HTTPException

        with _endpoints_with(_EndpointConfig()) as endpoints:
            with pytest.raises(HTTPException) as raised:
                self._delete(endpoints, record_id="named")
        assert raised.value.status_code == 404

    def test_resetting_a_default_with_nothing_stored_is_204(self) -> None:
        with _endpoints_with(_EndpointConfig()) as endpoints:
            response = self._delete(endpoints)
        assert response.status_code == 204

    def test_a_stored_default_is_reset(self) -> None:
        import asyncio

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            asyncio.run(
                endpoints.patch_settings_record(
                    payload=UiConfigUpdate.model_validate({"site_title": "Set"}),
                    resource="ui",
                    record_id="default",
                    current_user=object(),
                    _wire_safe_body=None,
                    if_match=None,
                )
            )
            assert "ui:default" in config.mongo_collection_settings.docs
            response = self._delete(endpoints)
        assert response.status_code == 204
        assert "ui:default" not in config.mongo_collection_settings.docs

    def test_a_stale_if_match_is_412(self) -> None:
        import asyncio

        from fastapi import HTTPException

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            asyncio.run(
                endpoints.patch_settings_record(
                    payload=UiConfigUpdate.model_validate({"site_title": "Set"}),
                    resource="ui",
                    record_id="default",
                    current_user=object(),
                    _wire_safe_body=None,
                    if_match=None,
                )
            )
            with pytest.raises(HTTPException) as raised:
                self._delete(endpoints, if_match='"nonsense"')
        assert raised.value.status_code == 412

    def test_a_concurrent_change_is_409(self) -> None:
        from unittest import mock

        from fastapi import HTTPException

        from freva_rest.settings_api.core import SettingsStore

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed = config.mongo_collection_settings
            _seed.docs["ui:named"] = {
                "_id": "ui:named",
                "resource_name": "ui",
                "record_id": "named",
                "site_title": "x",
                "revision": 1,
                "cas_token": "tok",
            }

            async def _changed(*a, **kw):
                return "changed"

            with mock.patch.object(SettingsStore, "delete", _changed):
                with pytest.raises(HTTPException) as raised:
                    self._delete(endpoints, record_id="named")
        assert raised.value.status_code == 409
        assert "Re-read and retry" in str(raised.value.detail)

    def test_a_named_record_that_vanished_is_404(self) -> None:
        from unittest import mock

        from fastapi import HTTPException

        from freva_rest.settings_api.core import SettingsStore

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            config.mongo_collection_settings.docs["ui:named"] = {
                "_id": "ui:named",
                "resource_name": "ui",
                "record_id": "named",
                "site_title": "x",
                "revision": 1,
                "cas_token": "tok",
            }

            async def _missing(*a, **kw):
                return "missing"

            with mock.patch.object(SettingsStore, "delete", _missing):
                with pytest.raises(HTTPException) as raised:
                    self._delete(endpoints, record_id="named")
        assert raised.value.status_code == 404


class _UnreachableCollection(_FakeCollection):
    """A collection whose reads fail the way pymongo actually fails.

    The cursor *builds* and the error surfaces during iteration, because the
    driver does its network I/O lazily. That distinction is the whole reason the
    fail-closed handlers wrap the `async for` and not just the `find()`: a
    fake that raised eagerly would let a handler that only guarded the call look
    correct while a PyMongoError still escaped as a 500 in production.
    """

    def find(self, *args, **kwargs):
        class _Cursor:
            def sort(self, *_a, **_k):
                return self

            def __aiter__(self):
                async def gen():
                    from pymongo.errors import PyMongoError

                    raise PyMongoError("mongo is unreachable")
                    yield {}

                return gen()

        return _Cursor()

    async def find_one(self, *args, **kwargs):
        from pymongo.errors import PyMongoError

        raise PyMongoError("mongo is unreachable")


class _ScanHookedCollection(_FakeCollection):
    """
    A collection that runs a callback when a scan opens its cursor.
    """

    def __init__(self, on_find=None) -> None:
        super().__init__()
        self.on_find = on_find

    def find(self, *args, **kwargs):
        if self.on_find is not None:
            hook, self.on_find = self.on_find, None
            hook()
        return super().find(*args, **kwargs)


def _ui_record(collection, record_id="default", **fields):
    doc = {
        "_id": f"ui:{record_id}",
        "resource_name": "ui",
        "record_id": record_id,
        "revision": 1,
        "cas_token": "tok",
        **fields,
    }
    collection.docs[doc["_id"]] = doc
    return doc


class TestContentReadEndpoint:
    """The merged content GET: one url, two shapes, and the 403 that keeps the
    admin shape from degrading silently into the public one."""

    @staticmethod
    def _get(endpoints, *, include_source, user, headers=None):
        import asyncio

        return asyncio.run(
            endpoints.get_content(
                request=_Request(headers),
                ui_id="default",
                content_id="home",
                include_source=include_source,
                current_user=user,
            )
        )

    def test_an_anonymous_read_gets_the_public_shape_and_no_source(self) -> None:
        import json

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents, source="# secret")
            response = self._get(endpoints, include_source=False, user=None)
        payload = json.loads(response.body)
        assert response.status_code == 200
        assert "source" not in payload and "source_hash" not in payload
        assert response.headers["Cache-Control"] == endpoints.PUBLIC_CACHE
        assert response.headers["ETag"]

    def test_a_matching_etag_on_the_public_read_is_a_304(self) -> None:
        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents)
            first = self._get(endpoints, include_source=False, user=None)
            again = self._get(
                endpoints,
                include_source=False,
                user=None,
                headers={"if-none-match": first.headers["ETag"]},
            )
        assert again.status_code == 304
        assert again.headers["Cache-Control"] == endpoints.PUBLIC_CACHE

    def test_include_source_without_a_token_is_403(self) -> None:
        from fastapi import HTTPException

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents)
            with pytest.raises(HTTPException) as raised:
                self._get(endpoints, include_source=True, user=None)
        assert raised.value.status_code == 403
        assert "administrator" in str(raised.value.detail)

    def test_include_source_as_an_authenticated_non_admin_is_403(self) -> None:
        from fastapi import HTTPException

        config = _EndpointConfig(admin=False)
        config.admin_token_claims = {"roles": ["^admin$"]}
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents)
            with pytest.raises(HTTPException) as raised:
                self._get(endpoints, include_source=True, user=object())
        assert raised.value.status_code == 403
        assert "perform this operation" in str(raised.value.detail)

    def test_include_source_as_an_admin_carries_the_source_privately(self) -> None:
        import json

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents, source="# secret")
            response = self._get(endpoints, include_source=True, user=object())
        payload = json.loads(response.body)
        assert payload["source"] == "# secret"
        assert response.headers["Cache-Control"] == endpoints.PRIVATE_CACHE

    def test_the_admin_body_has_its_own_etag_and_honours_it(self) -> None:
        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents, source="# secret")
            public = self._get(endpoints, include_source=False, user=None)
            admin = self._get(endpoints, include_source=True, user=object())
            same = self._get(
                endpoints,
                include_source=True,
                user=object(),
                headers={"if-none-match": admin.headers["ETag"]},
            )
        assert public.headers["ETag"] != admin.headers["ETag"]
        assert same.status_code == 304


class TestContentWriteEndpoint:
    """The content PATCH handler itself: what it stores, and what it returns."""

    @staticmethod
    def _patch(endpoints, source="# a", fmt="markdown", **kwargs):
        import asyncio

        return asyncio.run(
            endpoints.patch_content(
                update=ContentSource.model_validate({"format": fmt, "source": source}),
                ui_id="default",
                content_id="home",
                force=kwargs.pop("force", False),
                current_user=object(),
                _wire_safe_body=None,
                if_match=kwargs.pop("if_match", None),
            )
        )

    def test_a_create_stores_the_document_and_returns_it_uncacheable(self) -> None:
        import json

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            response = self._patch(endpoints, source="# hello")
            stored = config.mongo_collection_ui_contents.docs["default:home"]
        payload = json.loads(response.body)
        assert stored["source"] == "# hello"
        assert payload["rendered_html"].strip().startswith("<h1")
        assert response.headers["Cache-Control"] == endpoints.PRIVATE_CACHE
        assert response.headers["ETag"]

    def test_a_stale_if_match_on_the_write_is_412(self) -> None:
        from fastapi import HTTPException

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            self._patch(endpoints, source="# one")
            with pytest.raises(HTTPException) as raised:
                self._patch(endpoints, source="# two", if_match='"not-the-tag"')
        assert raised.value.status_code == 412

    def test_the_format_class_is_immutable_without_force(self) -> None:
        from fastapi import HTTPException

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            self._patch(endpoints, source="# one", fmt="markdown")
            with pytest.raises(HTTPException) as raised:
                self._patch(endpoints, source="<p>x</p>", fmt="sandbox-html")
            self._patch(endpoints, source="<p>x</p>", fmt="sandbox-html", force=True)
            stored = config.mongo_collection_ui_contents.docs["default:home"]
        assert raised.value.status_code == 409
        assert stored["format"] == "sandbox-html"


class TestContentDeleteEndpoint:
    """
    The delete handler's three failure answers, each produced by the state
    that really causes it rather than by a stubbed return value.
    """

    @staticmethod
    def _delete(endpoints, *, force=False, if_match=None):
        import asyncio

        return asyncio.run(
            endpoints.delete_content(
                ui_id="default",
                content_id="home",
                force=force,
                if_match=if_match,
                current_user=object(),
            )
        )

    def test_deleting_content_that_does_not_exist_is_404(self) -> None:
        from fastapi import HTTPException

        with _endpoints_with(_EndpointConfig()) as endpoints:
            with pytest.raises(HTTPException) as raised:
                self._delete(endpoints)
        assert raised.value.status_code == 404
        assert "No such content" in str(raised.value.detail)

    def test_a_stale_if_match_is_ignored_when_the_request_404s_anyway(self) -> None:
        from fastapi import HTTPException

        with _endpoints_with(_EndpointConfig()) as endpoints:
            with pytest.raises(HTTPException) as raised:
                self._delete(endpoints, if_match='"whatever"')
        assert raised.value.status_code == 404

    def test_an_unreferenced_delete_succeeds_with_204(self) -> None:
        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents)
            response = self._delete(endpoints)
        assert response.status_code == 204
        assert config.mongo_collection_ui_contents.docs == {}

    def test_content_that_vanished_during_the_reference_scan_is_404(self) -> None:
        from fastapi import HTTPException

        contents = _FakeCollection()
        settings = _ScanHookedCollection(
            on_find=lambda: contents.docs.pop("default:home", None)
        )
        config = _EndpointConfig(settings=settings, contents=contents)
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(contents)
            with pytest.raises(HTTPException) as raised:
                self._delete(endpoints)
        assert raised.value.status_code == 404

    def test_content_changed_during_the_reference_scan_is_409(self) -> None:
        from fastapi import HTTPException

        contents = _FakeCollection()

        def _concurrent_write():
            contents.docs["default:home"]["revision"] = 2
            contents.docs["default:home"]["cas_token"] = "another"

        settings = _ScanHookedCollection(on_find=_concurrent_write)
        config = _EndpointConfig(settings=settings, contents=contents)
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(contents)
            with pytest.raises(HTTPException) as raised:
                self._delete(endpoints)
        assert raised.value.status_code == 409
        assert "Re-read and retry" in str(raised.value.detail)
        assert contents.docs["default:home"]["revision"] == 2

    def test_a_referenced_content_document_is_protected_with_409(self) -> None:
        from fastapi import HTTPException

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents)
            _ui_record(
                config.mongo_collection_settings,
                content_refs=[{"ui_id": "default", "content_id": "home"}],
            )
            with pytest.raises(HTTPException) as raised:
                self._delete(endpoints)
            assert "default:home" in config.mongo_collection_ui_contents.docs
            assert self._delete(endpoints, force=True).status_code == 204
        assert raised.value.status_code == 409
        assert "ui/default" in str(raised.value.detail)

    def test_an_unreachable_database_refuses_the_delete_with_503(self) -> None:
        from fastapi import HTTPException

        config = _EndpointConfig(settings=_UnreachableCollection())
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents)
            with pytest.raises(HTTPException) as raised:
                self._delete(endpoints)
        assert raised.value.status_code == 503
        assert "referenced" in str(raised.value.detail)
        assert "default:home" in config.mongo_collection_ui_contents.docs


class TestReferenceScanHelpers:
    """`_collect_refs`, `_content_formats` and `_referring_uis` - the pieces the
    delete guard, the write validator and the audit all share."""

    def test_a_content_landing_block_is_a_reference(self) -> None:
        from freva_rest.settings_api import endpoints

        refs, cut = endpoints._collect_refs(
            {
                "landing_blocks": [
                    {"block": "hero", "heading": "hi"},
                    {
                        "block": "content",
                        "ref": {"ui_id": "default", "content_id": "home"},
                    },
                ]
            }
        )
        assert cut is False
        assert refs == [("default", "home", "rendered")]

    def test_content_formats_deduplicates_and_reports_what_it_found(self) -> None:
        import asyncio

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents, content_id="home")
            _seed_content_doc(
                config.mongo_collection_ui_contents,
                content_id="widget",
                format="sandbox-html",
                source="<p>x</p>",
            )
            formats = asyncio.run(
                endpoints._content_formats(
                    ["default:home", "default:home", "default:widget", "default:gone"]
                )
            )
        assert formats == {"default:home": "markdown", "default:widget": "sandbox-html"}

    def test_content_formats_chunks_the_lookup(self) -> None:
        import asyncio

        config = _EndpointConfig()
        seen = []

        class _Counting(_FakeCollection):
            def find(self, selector=None, projection=None, *a, **k):
                seen.append(len(selector["_id"]["$in"]))
                return super().find(selector, projection, *a, **k)

        config = _EndpointConfig(contents=_Counting())
        with _endpoints_with(config) as endpoints:
            keys = [f"default:p{index:04d}" for index in range(451)]
            asyncio.run(endpoints._content_formats(keys))
        assert seen == [200, 200, 51]
        assert max(seen) <= endpoints.CONTENT_LOOKUP_CHUNK

    def test_referring_uis_names_every_record_once(self) -> None:
        import asyncio

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _ui_record(
                config.mongo_collection_settings,
                "default",
                content_refs=[{"ui_id": "default", "content_id": "home"}],
                header={"content": {"ui_id": "default", "content_id": "home"}},
            )
            _ui_record(
                config.mongo_collection_settings,
                "other",
                content_refs=[{"ui_id": "default", "content_id": "elsewhere"}],
            )
            referrers = asyncio.run(endpoints._referring_uis("default", "home"))
        assert referrers == ["ui/default"]

    def test_referring_uis_fails_closed_on_a_database_error(self) -> None:
        import asyncio

        from fastapi import HTTPException

        config = _EndpointConfig(settings=_UnreachableCollection())
        with _endpoints_with(config) as endpoints:
            with pytest.raises(HTTPException) as raised:
                asyncio.run(endpoints._referring_uis("default", "home"))
        assert raised.value.status_code == 503
        assert "database is reachable" in str(raised.value.detail)


class TestWriteValidatorReferenceFailures:
    """The validator that runs inside the settings CAS loop."""

    @staticmethod
    def _validate(endpoints, manifest):
        import asyncio

        validator = endpoints._make_ref_validator("ui")
        assert validator is not None
        return asyncio.run(validator(UiConfig.model_validate(manifest)))

    def test_a_resource_without_references_gets_no_validator(self) -> None:
        from freva_rest.settings_api import endpoints
        assert endpoints._make_ref_validator("content") is None
        assert endpoints._make_ref_validator("ui") is not None

    def test_a_reference_to_missing_content_is_422(self) -> None:
        from fastapi import HTTPException

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            with pytest.raises(HTTPException) as raised:
                self._validate(
                    endpoints,
                    {"content_refs": [{"ui_id": "default", "content_id": "ghost"}]},
                )
        assert raised.value.status_code == 422
        assert "does not exist: default/ghost" in str(raised.value.detail)

    def test_each_missing_page_is_named_once(self) -> None:
        from fastapi import HTTPException

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            with pytest.raises(HTTPException) as raised:
                self._validate(
                    endpoints,
                    {
                        "content_refs": [
                            {"ui_id": "default", "content_id": "ghost"},
                            {"ui_id": "default", "content_id": "ghost"},
                        ]
                    },
                )
        assert str(raised.value.detail).count("default/ghost") == 1

    def test_a_valid_manifest_passes(self) -> None:
        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents)
            assert (
                self._validate(
                    endpoints,
                    {"content_refs": [{"ui_id": "default", "content_id": "home"}]},
                )
                is None
            )

    def test_an_unreachable_database_refuses_the_write_with_503(self) -> None:
        from fastapi import HTTPException

        config = _EndpointConfig(contents=_UnreachableCollection())
        with _endpoints_with(config) as endpoints:
            with pytest.raises(HTTPException) as raised:
                self._validate(
                    endpoints,
                    {"content_refs": [{"ui_id": "default", "content_id": "home"}]},
                )
        assert raised.value.status_code == 503
        assert "write is refused" in str(raised.value.detail)


class TestAuditEndpoint:
    """The compensating control, and the two answers it can give besides a
    clean page."""

    @staticmethod
    def _audit(endpoints, after=None):
        import asyncio

        return asyncio.run(endpoints.audit_content_references(after, object()))

    def test_a_clean_deployment_reports_a_complete_consistent_page(self) -> None:
        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents)
            _ui_record(
                config.mongo_collection_settings,
                content_refs=[{"ui_id": "default", "content_id": "home"}],
            )
            report = self._audit(endpoints)
        assert report == {"page_consistent": True, "problems": [], "complete": True}
        assert "consistent" not in report

    def test_a_reference_whose_format_no_longer_fits_is_reported(self) -> None:
        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(
                config.mongo_collection_ui_contents,
                content_id="widget",
                format="sandbox-html",
                source="<p>x</p>",
            )
            _ui_record(
                config.mongo_collection_settings,
                landing_blocks=[
                    {
                        "block": "content",
                        "ref": {"ui_id": "default", "content_id": "widget"},
                    }
                ],
            )
            report = self._audit(endpoints)
        assert report["page_consistent"] is False
        assert report["problems"] == [
            "ui/default: default/widget is sandbox-html and cannot be inlined; "
            "use a sandbox route"
        ]

    def test_a_dangling_reference_is_reported_against_its_record(self) -> None:
        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _ui_record(
                config.mongo_collection_settings,
                "site",
                content_refs=[{"ui_id": "default", "content_id": "gone"}],
            )
            report = self._audit(endpoints)
        assert report["problems"] == ["ui/site references missing content default/gone"]

    def test_an_unreachable_database_is_a_503(self) -> None:
        from fastapi import HTTPException

        config = _EndpointConfig(settings=_UnreachableCollection())
        with _endpoints_with(config) as endpoints:
            with pytest.raises(HTTPException) as raised:
                self._audit(endpoints)
        assert raised.value.status_code == 503
        assert "configuration database" in str(raised.value.detail)


class TestSandboxDocumentEndpoint:
    """The iframe source route: what it serves, and the headers that make it
    safe to serve at all."""

    @staticmethod
    def _document(endpoints, content_id="widget"):
        import asyncio

        return asyncio.run(
            endpoints.get_sandbox_document(ui_id="default", content_id=content_id)
        )

    def test_sandbox_content_is_served_as_a_locked_down_html_document(self) -> None:
        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(
                config.mongo_collection_ui_contents,
                content_id="widget",
                format="sandbox-html",
                source="<p>hi</p><script>go()</script>",
            )
            response = self._document(endpoints)
        assert response.media_type == "text/html"
        assert b"<script>go()</script>" in response.body
        csp = response.headers["Content-Security-Policy"]
        assert "sandbox allow-scripts" in csp
        assert "allow-same-origin" not in csp
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert response.headers["Cache-Control"] == endpoints.PRIVATE_CACHE

    def test_rendered_content_is_not_available_as_a_document(self) -> None:
        from fastapi import HTTPException

        config = _EndpointConfig()
        with _endpoints_with(config) as endpoints:
            _seed_content_doc(config.mongo_collection_ui_contents, content_id="home")
            with pytest.raises(HTTPException) as raised:
                self._document(endpoints, content_id="home")
        assert raised.value.status_code == 404

    def test_a_missing_document_is_404(self) -> None:
        from fastapi import HTTPException

        with _endpoints_with(_EndpointConfig()) as endpoints:
            with pytest.raises(HTTPException) as raised:
                self._document(endpoints)
        assert raised.value.status_code == 404


class TestValidatorGuardsHoldWithoutTheirGrammar:
    """Three checks whose comments say they exist for the day the grammar in
    front of them is loosened. Each is proved by loosening exactly that
    grammar, which is the only way to reach them - and the only way to notice
    if someone deletes them as dead code."""

    def test_an_empty_url_is_left_alone(self) -> None:
        from freva_rest.settings_api.field_types import _check_url
        assert _check_url("") == ""

    def test_a_non_string_timestamp_is_handed_on_untouched(self) -> None:
        from freva_rest.settings_api.field_types import _parse_rfc3339

        assert _parse_rfc3339(None) is None
        assert _parse_rfc3339(17) == 17

    def test_a_naive_timestamp_is_still_refused_if_the_grammar_admits_it(
        self, monkeypatch
    ) -> None:
        import re

        from freva_rest.settings_api import field_types

        monkeypatch.setattr(field_types, "_RFC3339_RE", re.compile(r".*"))
        with pytest.raises(ValueError, match="has no timezone"):
            field_types._parse_rfc3339("2026-05-01T09:00:00")
        # and an aware one still parses under the same loosened grammar, so the
        # test is not simply observing a blanket refusal
        assert field_types._parse_rfc3339("2026-05-01T09:00:00+00:00").endswith(
            "+00:00"
        )

    def test_a_non_http_scheme_is_still_refused_if_the_parser_admits_it(
        self, monkeypatch
    ) -> None:
        from freva_rest.settings_api import schema

        class _Lenient:
            def validate_python(self, value):
                class _Parsed:
                    scheme = "ftp"
                    host = "example.org"

                return _Parsed()

        monkeypatch.setattr(schema, "_HTTP_URL", _Lenient())
        with pytest.raises(ValueError, match="must use http or https"):
            schema._require_absolute_url("ftp://example.org/x")

    def test_a_hostless_url_is_still_refused_if_the_parser_admits_it(
        self, monkeypatch
    ) -> None:
        from freva_rest.settings_api import schema

        class _Lenient:
            def validate_python(self, value):
                class _Parsed:
                    scheme = "https"
                    host = None

                return _Parsed()

        monkeypatch.setattr(schema, "_HTTP_URL", _Lenient())
        with pytest.raises(ValueError, match="has no host"):
            schema._require_absolute_url("https://example.org/x")


class TestManifestCollectionBounds:
    """
    The size ceilings on a ui manifest, and the uniqueness rules beside them.
    """

    @staticmethod
    def _route(index):
        from freva_rest.settings_api.schema import ExternalRoute

        return ExternalRoute(
            kind="external", id=f"r{index:03d}", url=f"https://example.org/{index}"
        )

    @staticmethod
    def _manifest(**fields):
        """A UiConfig whose fields skip their own constraints."""
        return UiConfig.model_construct(
            **{
                "landing_blocks": [],
                "routes": [],
                "navigation": [],
                "announcements": [],
                **fields,
            }
        )

    def test_too_many_routes_is_refused_by_the_manifest_validator(self) -> None:
        from freva_rest.settings_api.schema import MAX_ROUTES

        routes = [self._route(i) for i in range(MAX_ROUTES + 1)]
        with pytest.raises(ValueError, match=f"at most {MAX_ROUTES} routes"):
            self._manifest(routes=routes)._validate_manifest()
        self._manifest(routes=routes[:-1])._validate_manifest()

    def test_too_many_navigation_items_is_refused_by_the_manifest_validator(
        self,
    ) -> None:
        from freva_rest.settings_api.schema import MAX_ROUTES, NavigationItem

        nav = [
            NavigationItem(route_id=f"r{i:03d}", label="x")
            for i in range(MAX_ROUTES + 1)
        ]
        with pytest.raises(ValueError, match="navigation items"):
            self._manifest(
                routes=[self._route(i) for i in range(MAX_ROUTES)], navigation=nav
            )._validate_manifest()

    def test_too_many_landing_blocks_is_refused_by_the_manifest_validator(self) -> None:
        from freva_rest.settings_api.schema import (
            MAX_LANDING_BLOCKS,
            HeroBlock,
        )

        blocks = [
            HeroBlock(block="hero", heading="x") for _ in range(MAX_LANDING_BLOCKS + 1)
        ]
        with pytest.raises(ValueError, match="landing blocks"):
            self._manifest(landing_blocks=blocks)._validate_manifest()

    def test_too_many_announcements_is_refused_by_the_manifest_validator(self) -> None:
        from freva_rest.settings_api.schema import (
            MAX_ANNOUNCEMENTS,
            Announcement,
        )

        items = [
            Announcement(id=f"a{i:03d}", message="x")
            for i in range(MAX_ANNOUNCEMENTS + 1)
        ]
        with pytest.raises(ValueError, match="announcements"):
            self._manifest(announcements=items)._validate_manifest()

    def test_the_field_constraint_reports_the_same_ceiling_to_a_client(self) -> None:
        from freva_rest.settings_api.schema import MAX_ROUTES

        with pytest.raises(ValidationError, match="at most 50 items"):
            UiConfig(routes=[self._route(i) for i in range(MAX_ROUTES + 1)])

    def test_duplicate_route_paths_are_refused(self) -> None:
        from freva_rest.settings_api.schema import SEARCH_FEATURE

        with pytest.raises(ValidationError, match="Route paths must be unique"):
            UiConfig(
                routes=[
                    {
                        "kind": "feature",
                        "id": "a",
                        "path": "/x",
                        "feature": SEARCH_FEATURE,
                    },
                    {
                        "kind": "feature",
                        "id": "b",
                        "path": "/x",
                        "feature": SEARCH_FEATURE,
                    },
                ]
            )

    def test_duplicate_announcement_ids_are_refused(self) -> None:
        with pytest.raises(ValidationError, match="Announcement ids must be unique"):
            UiConfig(
                announcements=[
                    {"id": "same", "message": "one"},
                    {"id": "same", "message": "two"},
                ]
            )

    def test_a_theme_token_with_a_trailing_newline_is_refused(self) -> None:
        with pytest.raises(ValidationError, match="should match pattern"):
            UiConfig(extra_colors={"brand\n": "#fff"})
        with pytest.raises(ValueError, match="valid theme-token name"):
            UiConfig._theme_token_names({"brand\n": "#fff"})
        assert UiConfig._theme_token_names({"brand": "#fff"}) == {"brand": "#fff"}


class TestRendererFallbacks:
    """The two renderer paths that only a broken input reaches."""

    def test_an_unknown_language_falls_back_to_plain_code(self) -> None:
        from freva_rest.settings_api.renderers import _highlight

        # narrow on purpose: an unknown lexer degrades this one block, and any
        # other failure is a real bug that must not be swallowed
        assert _highlight("x = 1", "no-such-language-at-all") is None
        assert _highlight("x = 1", "python") is not None

    def test_an_unknown_format_is_refused_before_any_work(self) -> None:
        from freva_rest.settings_api.renderers import render

        with pytest.raises(ValueError, match="is not a renderable format"):
            render("# a", "sandbox-html")

    def test_a_process_exit_from_the_writer_becomes_a_refusable_error(
        self, monkeypatch
    ) -> None:
        import docutils.core

        from freva_rest.settings_api import renderers

        calls = {"n": 0}

        def _exit_on_write(*args, **kwargs):
            calls["n"] += 1
            raise SystemExit("docutils gave up")

        monkeypatch.setattr(docutils.core, "publish_parts", _exit_on_write)
        with pytest.raises(ValueError, match="could not be rendered"):
            renderers.render("hello\n", "rst")
        assert calls["n"] == 1


class _SettingsOnlyConfig:
    """A config exposing only the settings collection, as SettingsStore uses."""

    def __init__(self, collection) -> None:
        self._collection = collection

    @property
    def mongo_collection_settings(self):
        return self._collection


def _settings_store(collection, record_id="default"):
    from freva_rest.settings_api.core import SettingsStore
    from freva_rest.settings_api.registry import REGISTRY

    entry = REGISTRY["ui"]
    return SettingsStore(
        _SettingsOnlyConfig(collection),
        "ui",
        record_id,
        entry.model,
        entry.update_model,
        entry.open_maps,
    )


def _content_store(collection, ui_id="default", content_id="home"):
    from freva_rest.settings_api.core import ContentStore

    return ContentStore(_FakeConfig(collection), ui_id, content_id)


def _run(coro):
    """
    Drive one coroutine on a loop that is then closed.
    """
    import asyncio

    return asyncio.run(coro)


class TestEnvironmentKnobs:
    """The import-time knobs fall back rather than refusing to boot.

    A typo in a deployment's environment must not take the api down; it must
    take the default and leave the operator a working service."""

    def test_a_count_knob_falls_back_on_anything_unparseable(self, monkeypatch) -> None:
        from freva_rest.settings_api.core import _positive_int

        monkeypatch.setenv("A_TEST_KNOB", "not-a-number")
        assert _positive_int("A_TEST_KNOB", 7) == 7
        monkeypatch.setenv("A_TEST_KNOB", "0")
        assert _positive_int("A_TEST_KNOB", 7) == 7
        monkeypatch.setenv("A_TEST_KNOB", "-3")
        assert _positive_int("A_TEST_KNOB", 7) == 7
        monkeypatch.setenv("A_TEST_KNOB", "11")
        assert _positive_int("A_TEST_KNOB", 7) == 11

    def test_bytes_are_charged_by_length_not_by_repr(self) -> None:
        from freva_rest.settings_api.core import _SIZEOF_OVERHEAD, _sizeof

        assert _sizeof(b"1234") == 4 + _SIZEOF_OVERHEAD
        assert _sizeof(bytearray(b"1234")) == 4 + _SIZEOF_OVERHEAD


class TestBodyCacheIsBounded:
    def test_a_body_larger_than_the_whole_budget_is_simply_not_cached(self) -> None:
        from freva_rest.settings_api import core

        core.reset_caches()
        huge = b"x" * (core._CACHE_BYTES + 1)
        core._body_put("k", huge, '"tag"', None)
        assert core._body_get("k") is None
        core._body_put("k", b"small", '"tag"', None)
        assert core._body_get("k") == (b"small", '"tag"')
        core.reset_caches()


class TestIfNoneMatch:
    """RFC 9110 says a GET's If-None-Match is a weak comparison over a list,
    and that `*` matches any current representation."""

    def test_a_star_matches_anything(self) -> None:
        from freva_rest.settings_api.core import if_none_match

        assert if_none_match("*", '"anything"') is True

    def test_a_weak_tag_matches_its_strong_twin(self) -> None:
        from freva_rest.settings_api.core import if_none_match
        assert if_none_match('W/"x"', '"x"') is True

    def test_any_member_of_the_list_matches(self) -> None:
        from freva_rest.settings_api.core import if_none_match

        assert if_none_match('"a", W/"b" , "c"', '"b"') is True

    def test_no_member_matching_is_not_a_match(self) -> None:
        from freva_rest.settings_api.core import if_none_match

        assert if_none_match('"a", "b"', '"c"') is False
        assert if_none_match(None, '"c"') is False
        assert if_none_match("", '"c"') is False


class TestWarnOnceLatch:
    def test_the_same_latch_logs_once_however_often_it_trips(self) -> None:
        from unittest import mock

        from freva_rest.settings_api import core

        core.reset_caches()
        with mock.patch.object(core.logger, "error") as logged:
            core._warn_once("a-latch", "something is wrong")
            core._warn_once("a-latch", "something is wrong")
            core._warn_once("another-latch", "something else is wrong")
        assert logged.call_count == 2
        core.reset_caches()


class TestSettingsStoreReadPaths:
    def test_exists_reports_whether_a_record_is_stored(self) -> None:
        from freva_rest.settings_api.core import reset_caches

        collection = _FakeCollection()
        reset_caches()
        assert _run(_settings_store(collection).exists()) is False
        _run(_settings_store(collection).patch(UiConfigUpdate(site_title="x")))
        assert _run(_settings_store(collection).exists()) is True
        reset_caches()

    def test_a_warm_body_cache_answers_without_touching_mongo(self) -> None:
        from freva_rest.settings_api.core import reset_caches

        collection = _FakeCollection()
        reset_caches()
        store = _settings_store(collection)
        first = _run(store.get())
        reads = {"n": 0}
        collection.on_read = lambda: reads.__setitem__("n", reads["n"] + 1)
        again = _run(store.get())
        assert again == first
        assert reads["n"] == 0
        reset_caches()

    def test_a_stored_record_too_large_to_serve_falls_back_to_defaults(self) -> None:
        from unittest import mock

        from freva_rest.settings_api import core

        collection = _FakeCollection()
        core.reset_caches()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "revision": 1,
            "cas_token": "tok",
            "public_extensions": {f"k{index:04d}": "x" * 1024 for index in range(80)},
        }
        with mock.patch.object(core.logger, "error") as logged:
            body, _ = _run(_settings_store(collection).get())
        import json

        assert json.loads(body)["public_extensions"] == {}
        assert any("too large" in str(call) for call in logged.call_args_list)
        core.reset_caches()


class TestSettingsStoreWriteFailures:
    """Every way a settings write can fail, and the status each maps to."""

    @staticmethod
    def _patch(collection, update=None, **kwargs):
        from freva_rest.settings_api.core import reset_caches

        reset_caches()
        store = _settings_store(collection)
        return _run(store.patch(update or UiConfigUpdate(site_title="ok"), **kwargs))

    def test_a_candidate_the_model_refuses_is_a_structured_422(self) -> None:
        from fastapi import HTTPException

        collection = _FakeCollection()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "revision": 1,
            "cas_token": "tok",
            "navigation": [{"route_id": "nope", "label": "x"}],
        }
        with pytest.raises(HTTPException) as raised:
            self._patch(collection)
        assert raised.value.status_code == 422
        import json

        assert isinstance(raised.value.detail, list)
        json.dumps(raised.value.detail)

    def test_a_resolved_document_over_the_response_ceiling_is_413(self) -> None:
        from fastapi import HTTPException

        from freva_rest.settings_api import core

        collection = _FakeCollection()
        with pytest.raises(HTTPException) as raised:
            self._patch(
                collection,
                UiConfigUpdate(
                    public_extensions={"big": "x" * (core.MAX_RESPONSE_BYTES + 1)}
                ),
            )
        assert raised.value.status_code == 413
        assert collection.docs == {}

    def test_a_lost_create_race_retries_instead_of_failing(self) -> None:
        from pymongo.errors import DuplicateKeyError

        from freva_rest.settings_api.core import reset_caches

        collection = _FakeCollection()
        reset_caches()
        attempts = {"n": 0}
        real_insert = collection.insert_one

        async def _first_insert_loses(doc):
            attempts["n"] += 1
            if attempts["n"] == 1:
                await real_insert(
                    {**doc, "site_title": "theirs", "cas_token": "theirs"}
                )
                raise DuplicateKeyError("duplicate _id")
            return await real_insert(doc)

        collection.insert_one = _first_insert_loses
        body, _ = _run(
            _settings_store(collection).patch(UiConfigUpdate(site_title="mine"))
        )
        import json

        assert attempts["n"] == 1
        assert json.loads(body)["site_title"] == "mine"
        reset_caches()

    def test_a_document_mongo_cannot_encode_is_a_422_not_a_500(self) -> None:
        from bson.errors import BSONError
        from fastapi import HTTPException

        collection = _FakeCollection()

        async def _refuse(doc):
            raise BSONError("cannot encode")

        collection.insert_one = _refuse
        with pytest.raises(HTTPException) as raised:
            self._patch(collection)
        assert raised.value.status_code == 422
        assert "cannot be stored by MongoDB" in str(raised.value.detail)

    def test_an_unreachable_database_on_a_write_is_a_503(self) -> None:
        from fastapi import HTTPException
        from pymongo.errors import PyMongoError

        collection = _FakeCollection()

        async def _unreachable(doc):
            raise PyMongoError("no connection")

        collection.insert_one = _unreachable
        with pytest.raises(HTTPException) as raised:
            self._patch(collection)
        assert raised.value.status_code == 503

    def test_an_unreachable_database_on_a_delete_is_a_503(self) -> None:
        from fastapi import HTTPException
        from pymongo.errors import PyMongoError

        from freva_rest.settings_api.core import reset_caches

        collection = _FakeCollection()

        async def _unreachable(predicate):
            raise PyMongoError("no connection")

        collection.delete_one = _unreachable
        reset_caches()
        with pytest.raises(HTTPException) as raised:
            _run(_settings_store(collection).delete())
        assert raised.value.status_code == 503
        reset_caches()


class TestSettingsSnapshotDisprovesStaleCaches:
    """
    A write-side read is authoritative.
    """

    def test_a_tombstone_is_dropped_when_the_record_turns_out_to_exist(self) -> None:
        from freva_rest.settings_api import core

        collection = _FakeCollection()
        core.reset_caches()
        store = _settings_store(collection)
        _run(store.get())
        store.note_absent()
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "revision": 9,
            "cas_token": "theirs",
            "site_title": "theirs",
        }
        assert _run(store.cas_snapshot()) is not None
        import json

        body, _ = _run(store.get())
        assert json.loads(body)["site_title"] == "theirs"
        core.reset_caches()

    def test_a_cached_document_is_dropped_when_the_record_turns_out_gone(self) -> None:
        from freva_rest.settings_api import core

        collection = _FakeCollection()
        core.reset_caches()
        store = _settings_store(collection)
        _run(store.patch(UiConfigUpdate(site_title="mine")))
        collection.docs.clear()
        assert _run(store.cas_snapshot()) is None
        import json

        body, _ = _run(store.get())
        assert json.loads(body)["site_title"] != "mine"
        core.reset_caches()


class TestContentStoreRepairDiagnostics:
    """
    A patch on top of a damaged stored document says what repairs it,
    instead of quietly writing the damage back.
    """

    @staticmethod
    def _patch_over(stored, patch):
        from freva_rest.settings_api.core import reset_caches

        collection = _FakeCollection()
        reset_caches()
        collection.docs["default:home"] = {
            "_id": "default:home",
            "ui_id": "default",
            "content_id": "home",
            "revision": 1,
            "cas_token": "tok",
            **stored,
        }
        store = _content_store(collection)
        result = _run(store.patch(ContentSource.model_validate(patch)))
        reset_caches()
        return result, collection

    def test_a_stored_document_with_no_format_is_a_422(self) -> None:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as raised:
            self._patch_over({"source": "# a"}, {"title": "New"})
        assert raised.value.status_code == 422
        assert "needs a format" in str(raised.value.detail)

    def test_an_inherited_format_this_build_cannot_serve_is_a_422(self) -> None:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as raised:
            self._patch_over({"format": "bogus", "source": "# a"}, {"title": "New"})
        assert raised.value.status_code == 422
        assert "does not support" in str(raised.value.detail)
        assert "explicit 'format'" in str(raised.value.detail)

    def test_an_inherited_title_that_is_not_text_is_a_422(self) -> None:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as raised:
            self._patch_over(
                {"format": "markdown", "source": "# a", "title": ["a", "list"]},
                {"source": "# b"},
            )
        assert raised.value.status_code == 422
        assert "stored title is list" in str(raised.value.detail)

    def test_an_explicit_null_title_clears_it(self) -> None:
        result, collection = self._patch_over(
            {"format": "markdown", "source": "# a", "title": "Old"},
            {"title": None},
        )
        assert collection.docs["default:home"]["title"] == ""

    def test_an_oversized_sandbox_source_is_refused(self) -> None:
        from fastapi import HTTPException

        from freva_rest.settings_api import core

        with pytest.raises(HTTPException) as raised:
            self._patch_over(
                {"format": "sandbox-html", "source": "<p>a</p>"},
                {
                    "format": "sandbox-html",
                    "source": "\u00e9" * (core.MAX_SANDBOX_SOURCE_BYTES // 2 + 1),
                },
            )
        assert raised.value.status_code == 422
        assert "too large" in str(raised.value.detail)


class TestContentStoreFailureMapping:
    @staticmethod
    def _patch(collection):
        from freva_rest.settings_api.core import reset_caches

        reset_caches()
        return _run(
            _content_store(collection).patch(
                ContentSource.model_validate({"format": "markdown", "source": "# a"})
            )
        )

    def test_a_document_mongo_cannot_encode_is_a_422_not_a_500(self) -> None:
        from bson.errors import BSONError
        from fastapi import HTTPException

        collection = _FakeCollection()

        async def _refuse(doc):
            raise BSONError("cannot encode")

        collection.insert_one = _refuse
        with pytest.raises(HTTPException) as raised:
            self._patch(collection)
        assert raised.value.status_code == 422
        assert "cannot be stored by MongoDB" in str(raised.value.detail)

    def test_an_unreachable_database_on_a_content_write_is_a_503(self) -> None:
        from fastapi import HTTPException
        from pymongo.errors import PyMongoError

        collection = _FakeCollection()

        async def _unreachable(doc):
            raise PyMongoError("no connection")

        collection.insert_one = _unreachable
        with pytest.raises(HTTPException) as raised:
            self._patch(collection)
        assert raised.value.status_code == 503

    def test_an_admin_read_of_missing_content_is_404(self) -> None:
        from fastapi import HTTPException

        from freva_rest.settings_api.core import reset_caches

        reset_caches()
        with pytest.raises(HTTPException) as raised:
            _run(_content_store(_FakeCollection()).get_admin())
        assert raised.value.status_code == 404
        reset_caches()

    def test_a_content_snapshot_drops_a_tombstone_it_disproves(self) -> None:
        from freva_rest.settings_api import core

        collection = _FakeCollection()
        core.reset_caches()
        store = _content_store(collection)
        with pytest.raises(Exception):
            _run(store.get_public())
        _seed_content_doc(collection)
        assert _run(store.cas_snapshot()) is not None
        body, _ = _run(store.get_public())
        assert b"<p>a</p>" in body
        core.reset_caches()


class TestRebuildClassification:
    """What the rebuild sweep does with documents it cannot or need not
    re-render."""

    def test_html_drift_ignores_what_it_cannot_judge(self) -> None:
        from freva_rest.settings_api import core
        assert (
            _run(core._html_drifts({"format": "sandbox-html", "rendered_html": "x"}))
            is False
        )
        assert _run(core._html_drifts({"format": "markdown"})) is False
        assert (
            _run(core._html_drifts({"format": "markdown", "rendered_html": ""}))
            is False
        )

    def test_html_too_big_to_serve_counts_as_drifted(self) -> None:
        from freva_rest.settings_api import core
        assert (
            _run(
                core._html_drifts(
                    {
                        "format": "markdown",
                        "rendered_html": "x" * (core.MAX_RENDERED_BYTES + 1),
                    }
                )
            )
            is True
        )

    def test_the_rebuild_skips_sandbox_html_without_calling_it_damaged(self) -> None:
        from freva_rest.settings_api.core import (
            rebuild_stale_content,
            reset_caches,
        )

        collection = _FakeCollection()
        reset_caches()
        collection.docs["default:widget"] = {
            "_id": "default:widget",
            "ui_id": "default",
            "content_id": "widget",
            "format": "sandbox-html",
            "source": "<p>x</p>",
            "revision": 1,
            "cas_token": "tok",
        }
        report = _run(rebuild_stale_content(_FakeConfig(collection)))

        assert report["rebuilt"] == 0
        assert report["failed"] == 0
        reset_caches()

    def test_unreachable_documents_are_counted_and_sampled(self) -> None:
        from unittest import mock

        from freva_rest.settings_api import core

        collection = _FakeCollection()
        core.reset_caches()
        for index in range(core.UNTYPED_ID_SAMPLE + 5):
            collection.docs[index] = {"_id": index, "format": "markdown"}
        with mock.patch.object(core.logger, "error") as logged:
            total = _run(core._count_untyped_ids(_FakeConfig(collection)))
        assert total == core.UNTYPED_ID_SAMPLE + 5
        sampled = [c for c in logged.call_args_list if "unreachable" in str(c)]
        assert len(sampled) == core.UNTYPED_ID_SAMPLE
        core.reset_caches()


class TestReadDefaultsAreConservative:
    """Two guards whose whole purpose is what happens when the caller, or the
    passage of time, leaves the reconciliation less to work with."""

    def test_a_read_that_forgets_its_epoch_still_refuses_a_stale_put(self) -> None:
        from freva_rest.settings_api import core

        collection = _FakeCollection()
        core.reset_caches()
        store = _settings_store(collection)
        collection.docs["ui:default"] = {
            "_id": "ui:default",
            "resource_name": "ui",
            "record_id": "default",
            "revision": 1,
            "cas_token": "tok",
            "site_title": "v1",
        }
        collection.on_read = lambda: core._cache_invalidate("settings:ui:default")
        result = _run(store._read_uncached())
        collection.on_read = None
        assert result["site_title"] == "v1"
        assert core._cache_get("settings:ui:default") is None
        assert _run(store._read_uncached())["site_title"] == "v1"
        assert core._cache_get("settings:ui:default") is not None
        core.reset_caches()

    def test_a_snapshot_reconciles_last_known_good_after_the_read_ttl_expires(
        self,
    ) -> None:
        from freva_rest.settings_api import core

        collection = _FakeCollection()
        core.reset_caches()
        store = _settings_store(collection)
        _run(store.patch(UiConfigUpdate(site_title="mine")))
        core._read_cache.pop("settings:ui:default", None)
        assert core._last_known_good("settings:ui:default") is not None
        collection.docs["ui:default"]["revision"] = 5
        collection.docs["ui:default"]["cas_token"] = "theirs"
        collection.docs["ui:default"]["site_title"] = "theirs"
        assert _run(store.cas_snapshot()) is not None
        assert core._last_known_good("settings:ui:default") is None
        import json

        body, _ = _run(store.get())
        assert json.loads(body)["site_title"] == "theirs"
        core.reset_caches()


def _elements(html: str) -> list:
    """
    Every element name the browser's parser would actually create.
    """
    from html.parser import HTMLParser

    class _Collect(HTMLParser):
        def __init__(self) -> None:
            super().__init__(convert_charrefs=True)
            self.names: list = []

        def handle_starttag(self, tag, attrs) -> None:
            self.names.append(tag)

        def handle_startendtag(self, tag, attrs) -> None:
            self.names.append(tag)

    parser = _Collect()
    parser.feed(html)
    parser.close()
    return parser.names


class TestStructuralWrappersReachTheFrontend:
    """
    `aside` and `section` carry the only machine-readable signal that says
    what a block *is*.
    """

    @staticmethod
    def _rst(source: str) -> str:
        return renderers.render(source, "rst")

    def test_a_note_is_one_element_with_its_kind_in_the_class(self) -> None:
        out = self._rst(".. note::\n\n   Careful here.\n")
        assert out == (
            '<aside class="admonition note">\n'
            '<p class="admonition-title">Note</p>\n'
            "<p>Careful here.</p>\n"
            "</aside>\n"
        )

    def test_a_note_and_a_warning_are_distinguishable_by_element(self) -> None:
        note = self._rst(".. note::\n\n   Same words.\n")
        warning = self._rst(".. warning::\n\n   Same words.\n")
        assert 'class="admonition note"' in note
        assert 'class="admonition warning"' in warning
        assert note.count("<aside") == warning.count("<aside") == 1

    def test_a_custom_admonition_keeps_its_derived_class(self) -> None:
        out = self._rst(".. admonition:: Custom\n\n   text\n")
        assert 'class="admonition admonition-custom"' in out

    def test_sidebar_and_topic_keep_their_wrappers(self) -> None:
        assert '<aside class="sidebar">' in self._rst(".. sidebar:: T\n\n   text\n")
        assert '<aside class="topic">' in self._rst(".. topic:: H\n\n   text\n")

    def test_the_footnote_list_keeps_its_wrapper(self) -> None:
        out = self._rst("Ref [1]_.\n\n.. [1] The note.\n")
        assert '<aside class="footnote-list brackets">' in out
        assert '<aside class="footnote brackets">' in out

    def test_footnote_targets_still_carry_no_id(self) -> None:
        out = self._rst("Ref [1]_.\n\n.. [1] The note.\n")
        assert 'href="#footnote-1"' in out
        assert "id=" not in out

    def test_an_rst_class_directive_reaches_the_section(self) -> None:
        out = self._rst(".. class:: highlight\n\nTitle\n=====\n\ntext\n")
        assert out == (
            '<section class="highlight">\n<h1>Title</h1>\n<p>text</p>\n</section>\n'
        )

    def test_an_html_author_may_use_both_wrappers(self) -> None:
        out = renderers.render(
            '<section class="intro"><aside class="callout"><p>a</p></aside></section>',
            "html-fragment",
        )
        assert out == (
            '<section class="intro"><aside class="callout"><p>a</p></aside></section>'
        )


    def test_event_handlers_on_the_new_elements_are_stripped(self) -> None:
        for tag in ("aside", "section"):
            for handler in ('onclick="x()"', 'onload="x()"', 'onmouseover="x()"'):
                out = sanitizer.sanitize_html(f"<{tag} {handler}><p>a</p></{tag}>")
                assert out == f"<{tag}><p>a</p></{tag}>", out

    def test_style_is_refused_on_the_new_elements(self) -> None:
        for tag in ("aside", "section"):
            out = sanitizer.sanitize_html(
                f'<{tag} style="position:fixed;top:0">x</{tag}>'
            )
            assert "style" not in out

    def test_id_is_refused_on_the_new_elements(self) -> None:
        for tag in ("aside", "section"):
            out = sanitizer.sanitize_html(f'<{tag} id="app-root" class="ok">x</{tag}>')
            assert "id=" not in out
            assert 'class="ok"' in out

    def test_a_class_value_that_looks_like_markup_stays_a_value(self) -> None:
        out = sanitizer.sanitize_html(
            "<aside class='a\"><script>evil()</script><b'>text</aside>"
        )
        assert "&quot;" in out
        assert _elements(out) == ["aside"]
        assert "script" not in _elements(out)

    def test_a_javascript_url_in_a_class_is_inert_and_unresolved(self) -> None:
        out = sanitizer.sanitize_html('<aside class="javascript:alert(1)">x</aside>')
        assert out == '<aside class="javascript:alert(1)">x</aside>'
        assert not _executable(out.replace("javascript:alert(1)", ""))

    def test_script_inside_an_aside_is_still_removed_with_its_body(self) -> None:
        out = sanitizer.sanitize_html(
            '<aside class="admonition"><script>evil()</script><p>a</p></aside>'
        )
        assert out == '<aside class="admonition"><p>a</p></aside>'

    def test_an_aside_cannot_smuggle_a_forbidden_child(self) -> None:
        out = sanitizer.sanitize_html(
            '<aside class="x"><iframe src="https://e.org"></iframe>'
            '<form><input name="a"></form></aside>'
        )
        assert not _executable(out)
        assert "<form" not in out and "<input" not in out

    # fixed point

    def test_the_new_elements_are_a_sanitizer_fixed_point(self) -> None:
        for source, fmt in (
            (".. note::\n\n   text\n", "rst"),
            (".. sidebar:: T\n\n   text\n", "rst"),
            ("Ref [1]_.\n\n.. [1] note\n", "rst"),
            (".. class:: c\n\nTitle\n=====\n\ntext\n", "rst"),
            (
                '<aside class="a"><section class="b"><p>x</p></section></aside>',
                "html-fragment",
            ),
        ):
            once = renderers.render(source, fmt)
            assert sanitizer.sanitize_html(once) == once, source
            assert sanitizer.stable_sanitize(once) == once, source

    def test_a_hostile_nesting_also_settles(self) -> None:
        hostile = (
            '<aside class="a"><aside class="b" onclick="x()">'
            "<section><p>deep</p></section></aside></aside>"
        )
        once = sanitizer.sanitize_html(hostile)
        assert sanitizer.sanitize_html(once) == once
        assert "onclick" not in once


class TestRstFieldListsAreNotDiscarded:
    """
    docutils puts a *leading* field list in `parts["docinfo"]`, not in
    `parts["body"]`.
    """

    @staticmethod
    def _rst(source: str) -> str:
        return renderers.render(source, "rst")

    def test_a_document_that_is_only_a_field_list_survives(self) -> None:
        out = self._rst(":Author: me\n:Version: 1\n")
        assert out == (
            '<dl class="docinfo simple">\n'
            '<dt class="author">Author<span class="colon">:</span></dt>\n'
            '<dd class="author"><p>me</p></dd>\n'
            '<dt class="version">Version<span class="colon">:</span></dt>\n'
            '<dd class="version">1</dd>\n'
            "</dl>\n"
        )

    def test_the_field_list_comes_before_the_body(self) -> None:
        out = self._rst(":Author: me\n\nSome text.\n")
        assert out.index("Author") < out.index("Some text.")
        assert "<p>Some text.</p>" in out

    def test_a_custom_field_name_is_kept(self) -> None:
        out = self._rst(":Contact: ops@example.org\n")
        assert "Contact" in out and "ops@example.org" in out

    def test_a_document_with_no_field_list_gains_nothing(self) -> None:
        assert self._rst("Just text.\n") == "<p>Just text.</p>\n"
        assert self._rst("Title\n=====\n\ntext\n") == (
            "<section>\n<h1>Title</h1>\n<p>text</p>\n</section>\n"
        )

    def test_a_field_list_below_the_first_block_is_body_content_as_before(self) -> None:
        out = self._rst("Text.\n\n:Author: me\n")
        assert out.count("Author") == 1

    def test_field_list_content_is_sanitized_like_everything_else(self) -> None:
        out = self._rst(":Author: `x <javascript:alert(1)>`_\n")
        assert not _executable(out)

    def test_a_field_list_is_a_sanitizer_fixed_point(self) -> None:
        once = self._rst(":Author: me\n:Version: 1\n\nBody.\n")
        assert sanitizer.sanitize_html(once) == once
        assert sanitizer.stable_sanitize(once) == once

    def test_the_whole_document_renders_as_one_exact_string(self) -> None:
        out = self._rst(
            ":Author: me\n"
            "\n"
            "Title\n"
            "=====\n"
            "\n"
            ".. note::\n"
            "\n"
            "   Read this.\n"
        )
        assert out == (
            '<dl class="docinfo simple">\n'
            '<dt class="author">Author<span class="colon">:</span></dt>\n'
            '<dd class="author"><p>me</p></dd>\n'
            "</dl>\n"
            "<section>\n"
            "<h1>Title</h1>\n"
            '<aside class="admonition note">\n'
            '<p class="admonition-title">Note</p>\n'
            "<p>Read this.</p>\n"
            "</aside>\n"
            "</section>\n"
        )


class TestDescriptionListsKeepTheirKind:
    """
    docutils encodes what a description list *is* in its classes and nowhere
    else.
    """

    @staticmethod
    def _rst(source: str) -> str:
        return renderers.render(source, "rst")

    def test_docinfo_is_marked_as_docinfo(self) -> None:
        out = self._rst(":Author: me\n:Version: 1\n")
        assert '<dl class="docinfo simple">' in out
        assert '<dd class="author"><p>me</p></dd>' in out
        assert '<dt class="version">' in out and '<dd class="version">' in out

    def test_a_body_field_list_is_marked_as_a_field_list(self) -> None:
        out = self._rst("Text.\n\n:Author: me\n")
        assert '<dl class="field-list simple">' in out
        assert "docinfo" not in out

    def test_an_ordinary_definition_list_is_neither(self) -> None:
        out = self._rst("term\n   definition\n")
        assert out == (
            '<dl class="simple">\n<dt>term</dt>\n<dd><p>definition</p>\n</dd>\n</dl>\n'
        )

    def test_the_three_kinds_are_mutually_distinguishable(self) -> None:
        docinfo = self._rst(":Author: me\n")
        field_list = self._rst("Text.\n\n:Author: me\n")
        definition = self._rst("Author\n   me\n")
        signatures = {
            html[html.index("<dl") : html.index(">", html.index("<dl")) + 1]
            for html in (docinfo, field_list, definition)
        }
        assert len(signatures) == 3, signatures

    def test_no_handler_style_or_id_rides_in_on_a_description_list(self) -> None:
        for tag in ("dl", "dt", "dd"):
            out = sanitizer.sanitize_html(
                f'<{tag} class="ok" id="app-root" style="position:fixed" '
                f'onclick="x()" title="t">v</{tag}>'
            )
            assert 'class="ok"' in out
            assert 'title="t"' in out
            for forbidden in ("id=", "style=", "onclick"):
                assert forbidden not in out, (tag, forbidden)

    def test_a_hostile_class_cannot_become_an_element(self) -> None:
        out = sanitizer.sanitize_html(
            "<dl class='a\"><script>evil()</script><b'><dt>t</dt></dl>"
        )
        assert _elements(out) == ["dl", "dt"]

    def test_description_lists_are_a_sanitizer_fixed_point(self) -> None:
        for source in (
            ":Author: me\n:Version: 1\n",
            "Text.\n\n:Author: me\n",
            "term\n   definition\n",
        ):
            once = self._rst(source)
            assert sanitizer.sanitize_html(once) == once, source
            assert sanitizer.stable_sanitize(once) == once, source


class TestTitleRoleSurvives:
    """`:title:` emits `<cite>` and nothing else - a semantic inline with no
    behaviour, no url and no attributes of its own."""

    def test_a_title_role_renders_as_cite(self) -> None:
        assert renderers.render("See :title:`A Book`.\n", "rst") == (
            "<p>See <cite>A Book</cite>.</p>\n"
        )

    def test_the_title_reference_spelling_is_the_same_role(self) -> None:
        out = renderers.render("A :title-reference:`Ref`.\n", "rst")
        assert "<cite>Ref</cite>" in out

    def test_cite_is_admitted_without_an_attribute_entry(self) -> None:
        out = sanitizer.sanitize_html(
            '<cite class="c" id="i" style="x" onclick="e()" title="t">A</cite>'
        )
        assert out == '<cite title="t">A</cite>'
        assert "cite" not in sanitizer.ALLOWED_ATTRS

    def test_nh3_permits_title_and_lang_on_every_element(self) -> None:
        out = sanitizer.sanitize_html(
            '<kbd class="c" id="i" style="s" title="t" lang="en" '
            'onclick="e()" data-x="1">k</kbd>'
        )
        assert out == '<kbd title="t" lang="en">k</kbd>'
        assert "kbd" not in sanitizer.ALLOWED_ATTRS

    def test_cite_is_a_sanitizer_fixed_point(self) -> None:
        once = renderers.render("See :title:`A Book`.\n", "rst")
        assert sanitizer.sanitize_html(once) == once
        assert sanitizer.stable_sanitize(once) == once


class TestGeneratedContentsIsRemoved:
    """
    `.. contents::` is intentionally not served.
    """

    SOURCE = (
        ".. contents::\n\nHead One\n========\n\ntext\n\nHead Two\n========\n\nmore\n"
    )
    WITHOUT = "Head One\n========\n\ntext\n\nHead Two\n========\n\nmore\n"

    @staticmethod
    def _rst(source: str) -> str:
        return renderers.render(source, "rst")

    def test_the_directive_leaves_no_trace_at_all(self) -> None:
        assert self._rst(self.SOURCE) == self._rst(self.WITHOUT)

    def test_no_nav_no_duplicate_list_and_no_dead_fragment_links(self) -> None:
        out = self._rst(self.SOURCE)
        assert "nav" not in _elements(out)
        assert "ul" not in _elements(out)  # the toc is the only list here
        assert "toc-backref" not in out
        assert "#toc-entry" not in out
        assert 'href="#' not in out
        assert "nav" not in sanitizer.ALLOWED_TAGS

    def test_the_headings_are_the_ordinary_ones(self) -> None:
        # a heading is its text, with no anchor wrapped around it, exactly as
        # without the directive
        out = self._rst(self.SOURCE)
        assert "<h1>Head One</h1>" in out
        assert "<h1>Head Two</h1>" in out
        assert _elements(out) == ["section", "h1", "p", "section", "h1", "p"]

    def test_the_local_variant_is_removed_too(self) -> None:
        out = self._rst("Top\n===\n\n.. contents::\n   :local:\n\nSub\n---\n\nt\n")
        assert out == self._rst("Top\n===\n\nSub\n---\n\nt\n")
        assert "nav" not in _elements(out) and "ul" not in _elements(out)

    def test_the_removal_is_reported_to_the_operator(self) -> None:
        from unittest import mock

        with mock.patch.object(renderers.logger, "warning") as warned:
            self._rst(self.SOURCE)
        assert warned.call_count == 1
        message = warned.call_args.args[0] % warned.call_args.args[1:]
        assert "dropped" in message
        assert "contents" in message
        assert "portal builds its own" in message

    def test_a_document_without_the_directive_reports_nothing(self) -> None:
        from unittest import mock

        with mock.patch.object(renderers.logger, "warning") as warned:
            self._rst(self.WITHOUT)
        assert warned.call_count == 0

    def test_an_authors_own_topic_is_not_removed(self) -> None:
        out = self._rst(".. topic:: Mine\n\n   text\n")
        assert '<aside class="topic">' in out
        assert "Mine" in out and "text" in out

    def test_the_doctree_helper_reports_what_it_removed(self) -> None:
        import io

        import docutils.core

        settings = renderers._rst_settings(renderers._rst_writer())
        settings.report_level = 5
        settings.warning_stream = io.StringIO()
        document = docutils.core.publish_doctree(source=self.SOURCE, settings=settings)
        assert renderers._strip_generated_contents(document) == 1
        # idempotent: a second pass finds nothing left to remove
        assert renderers._strip_generated_contents(document) == 0

    def test_removal_happens_before_the_html_exists(self) -> None:
        out = self._rst(self.SOURCE)
        assert "Head One" in out
        assert out.count("Head One") == 1  # heading only, no toc entry

    def test_the_result_is_a_sanitizer_fixed_point(self) -> None:
        once = self._rst(self.SOURCE)
        assert sanitizer.sanitize_html(once) == once
        assert sanitizer.stable_sanitize(once) == once


class TestNoIdsAndNoNavigationElement:
    """The two things the allow-list deliberately does not admit."""

    def test_nav_is_not_allow_listed(self) -> None:
        assert "nav" not in sanitizer.ALLOWED_TAGS
        assert sanitizer.sanitize_html('<nav class="c"><p>x</p></nav>') == "<p>x</p>"

    def test_no_tag_may_carry_an_id(self) -> None:
        assert not any("id" in attrs for attrs in sanitizer.ALLOWED_ATTRS.values())

    def test_an_empty_target_span_is_left_as_it_is(self) -> None:
        out = renderers.render(
            ".. _one:\n\n.. _two:\n\nText.\n",
            "rst",
        )
        assert out == "<p><span></span>Text.</p>\n"

    def test_a_single_target_carries_no_span_at_all(self) -> None:
        assert renderers.render(".. _one:\n\nText.\n", "rst") == "<p>Text.</p>\n"


class _CountingRefs(list):
    """A stored array that reports how far something read into it."""

    def __init__(self, size: int) -> None:
        super().__init__()
        self.size = size
        self.consumed = 0

    def __iter__(self):
        def _items():
            for index in range(self.size):
                self.consumed += 1
                yield {"ui_id": "a", "content_id": f"c{index:06d}"}

        return _items()

    def __len__(self) -> int:
        return self.size


class TestAuditReferenceBudget:
    """The audit is bounded by references as well as by documents."""

    @staticmethod
    def _record(index: int, refs) -> dict:
        return {
            "_id": f"ui:{index:03d}",
            "resource_name": "ui",
            "record_id": f"{index:03d}",
            "content_refs": refs,
        }

    @staticmethod
    def _refs(count: int, record: int) -> list:
        return [
            {"ui_id": "a", "content_id": f"gone-{record:03d}-{item:03d}"}
            for item in range(count)
        ]

    def _install(self, monkeypatch, settings, **limits):
        from freva_rest.settings_api import core, endpoints

        class _Config:
            mongo_collection_settings = settings
            mongo_collection_ui_contents = _FakeCollection()

        monkeypatch.setattr(endpoints, "server_config", _Config())
        for name, value in limits.items():
            monkeypatch.setattr(core, name, value)
            monkeypatch.setattr(endpoints, name, value)
        return endpoints

    @staticmethod
    def _run(coroutine):
        import asyncio

        return (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(coroutine)
        )

    def test_the_reference_budget_stops_a_pass_the_document_budget_would_not(
        self, monkeypatch
    ) -> None:
        settings = _FakeCollection()
        for index in range(5):
            settings.docs[f"ui:{index:03d}"] = self._record(
                index, self._refs(10, index)
            )
        endpoints = self._install(
            monkeypatch,
            settings,
            MAX_SCAN_DOCUMENTS=1_000,
            MAX_SCAN_REFERENCES=25,
        )

        problems, after, capped = self._run(endpoints.audit_references())
        assert after == "ui:002"
        assert len(problems) == 30
        assert capped is False

    def test_a_legal_record_is_never_read_only_in_part(self, monkeypatch) -> None:
        from freva_rest.settings_api import core

        settings = _FakeCollection()
        settings.docs["ui:000"] = self._record(0, self._refs(282, 0))
        endpoints = self._install(
            monkeypatch,
            settings,
            MAX_SCAN_DOCUMENTS=1_000,
            MAX_SCAN_REFERENCES=10,
        )

        problems, _, _ = self._run(endpoints.audit_references())
        assert core.MAX_RECORD_REFERENCES > 282
        assert len(problems) == 282

    def test_a_continuation_resumes_at_the_first_unprocessed_record(
        self, monkeypatch
    ) -> None:
        settings = _FakeCollection()
        for index in range(5):
            settings.docs[f"ui:{index:03d}"] = self._record(
                index, self._refs(10, index)
            )
        endpoints = self._install(
            monkeypatch,
            settings,
            MAX_SCAN_DOCUMENTS=1_000,
            MAX_SCAN_REFERENCES=25,
        )

        seen: list = []
        after = None
        for _ in range(10):
            problems, after, _ = self._run(endpoints.audit_references(after=after))
            seen.extend(problems)
            if after is None:
                break

        assert after is None
        assert len(seen) == 50
        assert len(set(seen)) == 50
        for index in range(5):
            assert sum(f"gone-{index:03d}-" in message for message in seen) == 10

    def test_one_record_always_makes_progress(self, monkeypatch) -> None:
        settings = _FakeCollection()
        settings.docs["ui:000"] = self._record(0, self._refs(10, 0))
        settings.docs["ui:001"] = self._record(1, self._refs(10, 1))
        endpoints = self._install(
            monkeypatch,
            settings,
            MAX_SCAN_DOCUMENTS=1_000,
            MAX_SCAN_REFERENCES=1,
        )

        # a budget smaller than one record still spends it on a whole record
        first, after, _ = self._run(endpoints.audit_references())
        assert after == "ui:000"
        assert len(first) == 10
        second, again, _ = self._run(endpoints.audit_references(after=after))
        assert len(second) == 10
        assert again is None

    def test_a_damaged_record_is_read_only_as_far_as_the_budget(
        self, monkeypatch
    ) -> None:
        oversized = _CountingRefs(1_000_000)
        settings = _FakeCollection()
        settings.docs["ui:000"] = self._record(0, oversized)
        settings.docs["ui:001"] = self._record(1, self._refs(1, 1))
        endpoints = self._install(
            monkeypatch,
            settings,
            MAX_SCAN_DOCUMENTS=1_000,
            MAX_SCAN_REFERENCES=1_000_000,
            MAX_RECORD_REFERENCES=50,
        )

        problems, after, _ = self._run(endpoints.audit_references())

        assert oversized.consumed == 51
        assert len(problems) == 52
        assert any("was read only as far as" in message for message in problems)
        assert after is None
        assert any("ui/001" in message for message in problems)

    def test_a_tail_hidden_behind_unreferenced_entries_is_reported(
        self, monkeypatch
    ) -> None:
        settings = _FakeCollection()
        routes = [{"kind": "landing", "id": f"l{index:04d}"} for index in range(1_000)]
        routes.append(
            {
                "kind": "content",
                "id": "late",
                "ui_id": "a",
                "content_id": "gone-late",
            }
        )
        settings.docs["ui:000"] = {
            "_id": "ui:000",
            "resource_name": "ui",
            "record_id": "000",
            "routes": routes,
        }
        endpoints = self._install(
            monkeypatch,
            settings,
            MAX_SCAN_DOCUMENTS=1_000,
            MAX_SCAN_REFERENCES=1_000,
            MAX_RECORD_REFERENCES=10,
        )

        problems, _, _ = self._run(endpoints.audit_references())
        assert problems
        assert any("was read only as far as" in message for message in problems)

    def test_a_record_read_to_the_last_reference_is_not_called_truncated(
        self, monkeypatch
    ) -> None:
        settings = _FakeCollection()
        settings.docs["ui:000"] = self._record(0, self._refs(10, 0))
        endpoints = self._install(
            monkeypatch,
            settings,
            MAX_SCAN_DOCUMENTS=1_000,
            MAX_SCAN_REFERENCES=1_000,
            MAX_RECORD_REFERENCES=10,
        )

        problems, _, _ = self._run(endpoints.audit_references())
        assert len(problems) == 10
        assert not any("was read only as far as" in message for message in problems)

    def test_the_collector_reports_an_unread_tail_to_its_other_callers_too(
        self,
    ) -> None:
        from freva_rest.settings_api.endpoints import _collect_refs

        document = {
            "content_refs": [
                {"ui_id": "a", "content_id": f"c{index}"} for index in range(5)
            ]
        }
        assert _collect_refs(document) == (
            [("a", f"c{index}", None) for index in range(5)],
            False,
        )
        found, cut = _collect_refs(document, limit=2)
        assert len(found) == 2 and cut is True

    def test_the_problem_list_is_bounded(self, monkeypatch) -> None:
        settings = _FakeCollection()
        for index in range(5):
            settings.docs[f"ui:{index:03d}"] = self._record(
                index, self._refs(20, index)
            )
        endpoints = self._install(
            monkeypatch,
            settings,
            MAX_SCAN_DOCUMENTS=1_000,
            MAX_SCAN_REFERENCES=1_000,
            MAX_AUDIT_PROBLEMS=7,
        )

        problems, _, capped = self._run(endpoints.audit_references())
        assert len(problems) == 7
        assert capped is True

        report = self._run(
            endpoints.audit_content_references(after=None, current_user=None)
        )
        assert report["problems_truncated"] is True
        assert len(report["problems"]) == 7

    def test_one_message_cannot_be_arbitrarily_long(self, monkeypatch) -> None:
        from freva_rest.settings_api.core import MAX_PROBLEM_LENGTH

        settings = _FakeCollection()
        settings.docs["ui:000"] = self._record(
            0, [{"ui_id": "a", "content_id": "x" * 1_000_000}]
        )
        endpoints = self._install(monkeypatch, settings)

        problems, _, _ = self._run(endpoints.audit_references())
        assert problems and all(
            len(message) <= MAX_PROBLEM_LENGTH for message in problems
        )

    def test_an_ordinary_audit_reports_exactly_what_it_did_before(
        self, monkeypatch
    ) -> None:
        settings = _FakeCollection()
        settings.docs["ui:000"] = self._record(0, self._refs(2, 0))
        endpoints = self._install(monkeypatch, settings)

        report = self._run(
            endpoints.audit_content_references(after=None, current_user=None)
        )
        assert set(report) == {"page_consistent", "problems", "complete"}
        assert report["page_consistent"] is False
        assert report["complete"] is True
        assert len(report["problems"]) == 2

    def test_a_clean_audit_is_byte_compatible(self, monkeypatch) -> None:
        settings = _FakeCollection()
        endpoints = self._install(monkeypatch, settings)

        report = self._run(
            endpoints.audit_content_references(after=None, current_user=None)
        )
        assert report == {"page_consistent": True, "problems": [], "complete": True}


class TestDiscriminatorsAreRequired:
    """A union tag carries no default, so the schema demands what the runtime
    already demands."""

    ROUTES = [
        {"kind": "landing", "id": "home", "path": "/"},
        {
            "kind": "content",
            "id": "docs",
            "path": "/docs",
            "ui_id": "default",
            "content_id": "page",
        },
        {
            "kind": "feature",
            "id": "browse",
            "path": "/browse",
            "feature": "databrowser",
        },
        {
            "kind": "sandbox",
            "id": "box",
            "path": "/box",
            "ui_id": "default",
            "content_id": "app",
        },
        {"kind": "external", "id": "away", "url": "https://example.org/"},
    ]
    BLOCKS = [
        {"block": "hero", "heading": "hi"},
        {"block": "search", "target_route_id": "browse"},
        {"block": "links", "links": []},
        {"block": "feature-link", "route_id": "browse", "label": "Browse"},
        {"block": "content", "ref": {"ui_id": "default", "content_id": "page"}},
        {"block": "cards", "cards": []},
    ]

    @staticmethod
    def _tagged(schema: dict) -> list:
        return [
            (name, tag)
            for name, definition in schema.get("$defs", {}).items()
            for tag in ("kind", "block")
            if tag in definition.get("properties", {})
        ]

    @pytest.mark.parametrize("model", [UiConfig, UiConfigUpdate])
    def test_every_discriminator_is_required(self, model) -> None:
        schema = model.model_json_schema()
        tagged = self._tagged(schema)
        assert tagged, schema.keys()
        for name, tag in tagged:
            definition = schema["$defs"][name]
            assert tag in definition.get("required", []), (model.__name__, name, tag)
            assert "default" not in definition["properties"][tag], (name, tag)

    def test_every_representative_route_validates(self) -> None:
        config = UiConfig(routes=self.ROUTES)
        assert [route.kind for route in config.routes] == [
            route["kind"] for route in self.ROUTES
        ]

    def test_every_representative_block_validates(self) -> None:
        config = UiConfig(
            routes=[
                {
                    "kind": "feature",
                    "id": "browse",
                    "path": "/browse",
                    "feature": "databrowser",
                }
            ],
            landing_blocks=self.BLOCKS,
        )
        assert [block.block for block in config.landing_blocks] == [
            block["block"] for block in self.BLOCKS
        ]

    def test_the_update_model_takes_the_same_payloads(self) -> None:
        update = UiConfigUpdate(routes=self.ROUTES, landing_blocks=self.BLOCKS)
        assert update.routes is not None and len(update.routes) == len(self.ROUTES)
        assert update.landing_blocks is not None

    @pytest.mark.parametrize("route", ROUTES)
    def test_a_route_without_its_tag_is_refused(self, route) -> None:
        untagged = {key: value for key, value in route.items() if key != "kind"}
        with pytest.raises(ValidationError):
            UiConfig(routes=[untagged])

    @pytest.mark.parametrize("block", BLOCKS)
    def test_a_block_without_its_tag_is_refused(self, block) -> None:
        untagged = {key: value for key, value in block.items() if key != "block"}
        with pytest.raises(ValidationError):
            UiConfig(landing_blocks=[untagged])


class TestAdminClaimShorthand:
    """A bare entry in `API_ADMIN_TOKEN_CLAIMS` is a value for the default
    claim path."""

    @staticmethod
    def _claims(monkeypatch, raw: str, default_key: str = "roles") -> dict:
        from freva_rest.config import env_to_dict

        monkeypatch.setenv("API_ADMIN_TOKEN_CLAIMS", raw)
        return env_to_dict("API_ADMIN_TOKEN_CLAIMS", default_key=default_key)

    def test_a_bare_value_lands_under_the_default_key(self, monkeypatch) -> None:
        assert self._claims(monkeypatch, "admin") == {"roles": ["admin"]}

    def test_two_bare_values_share_the_default_key(self, monkeypatch) -> None:
        assert self._claims(monkeypatch, "admin,ops") == {"roles": ["admin", "ops"]}

    def test_an_explicit_path_and_a_shorthand_coexist(self, monkeypatch) -> None:
        assert self._claims(monkeypatch, "groups:^admins$,admin") == {
            "groups": ["^admins$"],
            "roles": ["admin"],
        }

    def test_an_explicit_roles_entry_is_unchanged(self, monkeypatch) -> None:
        assert self._claims(monkeypatch, "roles:admin") == {"roles": ["admin"]}

    def test_empty_entries_are_ignored(self, monkeypatch) -> None:
        assert self._claims(monkeypatch, "") == {}
        assert self._claims(monkeypatch, ",,") == {}
        assert self._claims(monkeypatch, "admin,,ops") == {"roles": ["admin", "ops"]}

    def test_a_bare_value_needs_a_default_key(self, monkeypatch) -> None:
        assert self._claims(monkeypatch, "admin", default_key="") == {}

    def test_the_shorthand_authorises_the_way_the_changelog_says(
        self, monkeypatch
    ) -> None:
        from freva_rest.config import ServerConfig

        claims = self._claims(monkeypatch, "admin")
        config = ServerConfig()
        config.admin_token_claims = claims

        class _User:
            model_extra = {"roles": ["admin"]}

            @staticmethod
            def model_dump() -> dict:
                return {"roles": ["admin"]}

        class _Other(_User):
            model_extra = {"roles": ["admin-readonly"]}

            @staticmethod
            def model_dump() -> dict:
                return {"roles": ["admin-readonly"]}

        assert config.is_admin_user(_User()) is True
        assert config.is_admin_user(_Other()) is False


def _nested_past_this_stack() -> str:
    import json

    depth, raw = 20_000, ""
    while depth <= 5_120_000:
        raw = "[" * depth + "]" * depth
        try:
            json.loads(raw)
        except RecursionError:
            break
        depth *= 4
    return raw


TOO_DEEP_JSON = _nested_past_this_stack()


class TestParsingTheBodyIsItselfGuarded:
    """`json.loads` raises more than `JSONDecodeError`, and the guard maps
    every other way out of it to a 422 rather than letting it become a 500."""

    class _Request:
        def __init__(self, raw: bytes) -> None:
            self._raw = raw

        async def body(self) -> bytes:
            return self._raw

    @classmethod
    def _guard(cls, raw):
        import asyncio

        from freva_rest.settings_api.endpoints import reject_unserialisable_body

        body = raw.encode() if isinstance(raw, str) else raw
        return (
            asyncio.get_event_loop_policy()
            .new_event_loop()
            .run_until_complete(reject_unserialisable_body(cls._Request(body)))
        )

    @classmethod
    def _refusal(cls, raw):
        from freva_rest.settings_api.core import SettingsError

        with pytest.raises(SettingsError) as raised:
            cls._guard(raw)
        return raised.value

    def test_bytes_that_are_not_utf8_are_refused(self) -> None:
        error = self._refusal(b'{"site_title": "\xff\xfe"}')
        assert error.status_code == 422
        assert "not valid UTF-8" in error.detail

    def test_an_integer_past_pythons_conversion_limit_is_refused(self) -> None:
        error = self._refusal('{"public_extensions": {"a": ' + "1" * 4301 + "}}")
        assert error.status_code == 422
        assert "64-bit" in error.detail

    def test_json_nested_past_the_stack_is_refused(self) -> None:
        error = self._refusal(TOO_DEEP_JSON)
        assert error.status_code == 422
        assert "nested too deeply" in error.detail

    def test_a_non_finite_number_keeps_its_own_answer(self) -> None:
        error = self._refusal('{"public_extensions": {"a": 1e999}}')
        assert "finite" in error.detail
        assert "64-bit" not in error.detail

    def test_a_syntax_error_is_left_to_the_normal_validation_path(self) -> None:
        assert self._guard("{not json") is None
        assert self._guard("") is None

    @pytest.mark.parametrize(
        "raw",
        [
            b'{"site_title": "\xff\xfe"}',
            '{"public_extensions": {"a": ' + "1" * 4301 + "}}",
            TOO_DEEP_JSON,
            '{"public_extensions": {"a": 1e999}}',
            '{"site_title": "\\ud800"}',
        ],
    )
    def test_every_refusal_is_uncacheable_and_quotes_nothing_back(self, raw) -> None:
        error = self._refusal(raw)
        assert error.headers["Cache-Control"] == "private, no-store"
        if isinstance(raw, str):
            assert raw[:40] not in error.detail

    @pytest.mark.parametrize(
        "raw",
        [
            '{"public_extensions": {"a": 9223372036854775807}}',
            '{"public_extensions": {"a": -9223372036854775808}}',
            '{"public_extensions": {"a": 0}}',
            '{"public_extensions": {"a": -1}}',
            '{"public_extensions": {"a": 9999999999999999999}}',
        ],
    )
    def test_a_nineteen_digit_integer_is_the_models_business(self, raw: str) -> None:
        assert self._guard(raw) is None

    @pytest.mark.parametrize("sign", ["", "-"])
    def test_the_digit_count_ignores_the_sign(self, sign: str) -> None:
        from freva_rest.settings_api.core import SettingsError
        from freva_rest.settings_api.endpoints import MAX_INT_DIGITS, _bounded_int

        widest = sign + "9" * MAX_INT_DIGITS
        assert _bounded_int(widest) == int(widest)
        with pytest.raises(SettingsError):
            _bounded_int(sign + "9" * (MAX_INT_DIGITS + 1))

    def test_the_bound_is_this_apis_and_not_the_interpreters(self) -> None:
        import sys

        original = sys.get_int_max_str_digits()
        sys.set_int_max_str_digits(100_000)
        try:
            assert int("1" * 4301)
            error = self._refusal('{"public_extensions": {"a": 12345678901234567890}}')
        finally:
            sys.set_int_max_str_digits(original)
        assert error.status_code == 422
        assert "64-bit" in error.detail


class TestTheFrameworkParsesTheBodyFirst:
    """
    What reaches the guard at all, pinned against the framework.
    """

    JSON = {"Content-Type": "application/json"}

    @staticmethod
    def _app(*, admin: bool = True):
        from fastapi import Depends, FastAPI, HTTPException
        from fastapi.testclient import TestClient

        from freva_rest.settings_api.endpoints import reject_unserialisable_body
        from freva_rest.settings_api.registry import REGISTRY

        def _admin() -> None:
            if not admin:
                raise HTTPException(status_code=403, detail="not an admin")

        update_model = REGISTRY["ui"].update_model
        app = FastAPI()

        @app.patch(
            "/r",
            dependencies=[Depends(_admin), Depends(reject_unserialisable_body)],
        )
        async def route(
            payload: update_model,
        ) -> dict:
            return {"ok": True}

        return TestClient(app, raise_server_exceptions=False)

    def _patch(self, body, *, admin: bool = True):
        return self._app(admin=admin).patch("/r", content=body, headers=self.JSON)

    @pytest.mark.parametrize(
        "body",
        [
            b'{"site_title": "\xff\xfe"}',
            ('{"public_extensions": {"a": ' + "1" * 4301 + "}}").encode(),
            TOO_DEEP_JSON.encode(),
        ],
    )
    def test_the_framework_answers_before_the_route_does(self, body: bytes) -> None:
        res = self._patch(body)
        assert res.status_code == 400
        assert "64-bit" not in res.text and "UTF-8" not in res.text

    def test_that_answer_reveals_nothing_a_non_admin_could_not_see(self) -> None:
        res = self._patch(b'{"site_title": "\xff\xfe"}', admin=False)
        assert res.status_code == 400
        assert res.json() == self._patch(b'{"site_title": "\xff\xfe"}').json()

    def test_an_oversized_integer_the_framework_accepts_is_the_guards(self) -> None:
        res = self._patch('{"public_extensions": {"a": 12345678901234567890}}')
        assert res.status_code == 422
        assert "64-bit" in res.text
        assert res.headers["Cache-Control"] == "private, no-store"

    def test_that_refusal_still_comes_after_the_admin_check(self) -> None:
        res = self._patch(
            '{"public_extensions": {"a": 12345678901234567890}}', admin=False
        )
        assert res.status_code == 403
        assert "64-bit" not in res.text

    @pytest.mark.parametrize(
        "value", ["9223372036854775807", "-9223372036854775808", "0", "-1"]
    )
    def test_a_storable_integer_reaches_the_model(self, value: str) -> None:
        res = self._patch('{"public_extensions": {"a": %s}}' % value)
        assert res.status_code == 200, res.text

    def test_an_out_of_range_integer_is_the_models_422(self) -> None:
        res = self._patch('{"public_extensions": {"a": 9999999999999999999}}')
        assert res.status_code == 422
        assert "64-bit" not in res.text

    def test_a_syntax_error_still_takes_the_normal_json_path(self) -> None:
        res = self._patch("{not json")
        assert res.status_code == 422
        assert "json_invalid" in res.text
