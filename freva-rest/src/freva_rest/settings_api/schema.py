"""Pydantic models for the settings resources and the ui content documents.
The ui config carries references to content, never the bodies."""

import re
from datetime import datetime
from typing import Annotated, Dict, List, Literal, Optional, Union, get_args

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from .field_types import (
    ExtensionValue,
    Hex,
    LongText,
    MapKey,
    Rfc3339,
    Url,
    _check_url,
)

# identifiers

RECORD_ID_PATTERN = r"^[a-z][a-z0-9_-]{0,63}$"
"""Shape shared by a settings record id and a content id; no dot, because a dot
is a mongo path separator."""

SHARED_UI_ID = "_shared"
"""The one reserved namespace for content shared between uis."""

DEFAULT_RECORD_ID = "default"

UI_ID_PATTERN = rf"^(?:{SHARED_UI_ID}|[a-z][a-z0-9_-]{{0,63}})$"
"""A ui id: an ordinary record id, or the reserved ``_shared``; the underscore
prefix is reserved for that one name, not a general namespace."""

SCHEMA_VERSION = 1
"""The ui manifest contract version, bumped when a shape change needs a frontend
to adapt, so a frontend can refuse a manifest it does not understand."""

BuiltinFeature = Literal["databrowser", "stac"]
"""The built-in features the frontend ships, as a type, so the json schema
carries the enum and a generated form can offer a picker."""

BUILTIN_FEATURES = get_args(BuiltinFeature)
"""The same names as a runtime tuple, derived from the Literal so the two cannot
drift; a navigation entry or feature-link may only name one of these."""

SEARCH_FEATURE = "databrowser"
"""The feature a landing search block submits to; whether it is enabled is
deliberately not checked, so a route for a disabled feature is legal."""

ROUTE_PATH_PATTERN = r"^/(?:[a-z0-9_-][a-z0-9/_-]{0,126})?$"
"""An absolute, lower-case route path with no scheme or host; a second leading
slash is barred because ``//evil`` is a protocol-relative off-site url."""


_IPV4_RE = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")
"""Recognises an IPv4 shape; octet ranges are the URL parser's job, and it has
already said yes by the time this is consulted."""

_HOSTNAME_RE = re.compile(
    r"^(?=.{1,253}$)"  # total length
    r"[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"  # first label
    r"(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\.?$"  # further labels
)
"""A DNS hostname: non-empty labels, no leading or trailing hyphen in a label,
no empty label (so no ``a..b``), no bare ``.``."""


_RAW_AUTHORITY_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://[^/?#]", re.IGNORECASE)
"""The raw string must carry a non-empty authority after ``://``."""

_HTTP_URL = TypeAdapter(AnyHttpUrl)
"""Pydantic's browser-compatible URL parser, used purely as a validator: the
field keeps storing the original string."""


def _require_absolute_url(value: str) -> str:
    """An off-site link needs a scheme and an authority a browser can resolve;
    parser and label grammar both run, because neither covers the other."""
    # Checked on the raw string, before the parser sees it: pydantic reinterprets
    # the empty authority of `https:///path` as `https://path/`.
    if not _RAW_AUTHORITY_RE.match(value):
        raise ValueError(
            f"'{value}' has no host between '//' and the path. An external "
            "route needs a complete url such as 'https://example.org/page'."
        )
    try:
        parsed = _HTTP_URL.validate_python(value)
    except ValidationError:
        raise ValueError(
            f"'{value}' is not a usable absolute http(s) url. An external route "
            "links off this site; use a 'content', 'feature' or 'sandbox' route "
            "for a destination inside it."
        ) from None
    if parsed.scheme.lower() not in ("http", "https"):
        raise ValueError(f"'{value}' must use http or https.")
    host = parsed.host or ""
    if not host:
        raise ValueError(
            f"'{value}' has no host. An external route needs a complete url "
            "such as 'https://example.org/page'."
        )
    if ":" in host or host.startswith("["):
        return value  # an IPv6 literal, already validated by the parser
    if _IPV4_RE.match(host):
        return value  # an IPv4 literal, already validated by the parser
    if not _HOSTNAME_RE.match(host):
        raise ValueError(
            f"'{value}' does not have a valid host name ({host!r}). Use a "
            "complete url such as 'https://example.org/page'."
        )
    return value


ThemeToken = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9-]{0,63}$")]
"""A theme-token name as a type, so both the read and the update schema carry the
rule rather than only a field validator on ``UiConfig``."""

THEME_TOKEN_PATTERN = r"^[a-z][a-z0-9-]{0,63}$"
"""A theme-token name (a key in ``extra_colors``): css-custom-property-safe, so
it cannot inject css when the frontend maps it to a ``--var``."""

MAX_ROUTES = 50
MAX_LANDING_BLOCKS = 30
MAX_ANNOUNCEMENTS = 20
MAX_CONTENT_REFS = 200
"""Bounds on the collections a manifest may carry; 200 is
``CONTENT_LOOKUP_CHUNK``, so verifying ``content_refs`` is one round trip."""


# navigation - a typed union, never an arbitrary dict


class LandingRoute(BaseModel):
    """The landing page of this ui; at most one per ui, with its blocks coming
    from ``landing_blocks``."""

    model_config = ConfigDict(extra="forbid")
    kind: Literal["landing"]
    id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]
    path: Annotated[str, Field(pattern=ROUTE_PATH_PATTERN)] = "/"


class ContentRoute(BaseModel):
    """A route that renders a content document (markdown/rst/html-fragment)."""

    model_config = ConfigDict(extra="forbid")
    kind: Literal["content"]
    id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]
    path: Annotated[str, Field(pattern=ROUTE_PATH_PATTERN)]
    ui_id: Annotated[str, Field(pattern=UI_ID_PATTERN)]
    content_id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]


class FeatureRoute(BaseModel):
    """A route that mounts a built-in feature; the feature name is allow-listed,
    so runtime config selects an installed feature but cannot introduce one."""

    model_config = ConfigDict(extra="forbid")
    kind: Literal["feature"]
    id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]
    path: Annotated[str, Field(pattern=ROUTE_PATH_PATTERN)]
    # a Literal, so the allow-list reaches the schema as an enum rather than
    # living in a validator a form generator cannot see
    feature: BuiltinFeature


class SandboxRoute(BaseModel):
    """A route that embeds a sandbox-html document in an isolated iframe via the
    content's ``/document`` url."""

    model_config = ConfigDict(extra="forbid")
    kind: Literal["sandbox"]
    id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]
    path: Annotated[str, Field(pattern=ROUTE_PATH_PATTERN)]
    ui_id: Annotated[str, Field(pattern=UI_ID_PATTERN)]
    content_id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]


class ExternalRoute(BaseModel):
    """A link off to another site: it has a url, not a path, and that url must be
    absolute ``http``/``https``."""

    model_config = ConfigDict(extra="forbid")
    kind: Literal["external"]
    id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]
    url: Annotated[str, Field(max_length=2048)]

    @field_validator("url")
    @classmethod
    def _url_scheme(cls, value: str) -> str:
        return _require_absolute_url(_check_url(value))


Route = Annotated[
    Union[LandingRoute, ContentRoute, FeatureRoute, SandboxRoute, ExternalRoute],
    Field(discriminator="kind"),
]
"""One entry in the routing table, chosen by ``kind``; routes carry no labels,
because the tab bar is the navigation's concern."""


class NavigationItem(BaseModel):
    """One tab in the navigation bar: purely the display layer, referencing a
    route by id and contributing the label and icon."""

    model_config = ConfigDict(extra="forbid")
    route_id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]
    label: Annotated[str, Field(max_length=128)]
    icon: Optional[Annotated[str, Field(max_length=64)]] = None


# landing-page blocks - a typed, ordered union


class HeroBlock(BaseModel):
    """A landing hero: a heading, some text and an optional call to action."""

    model_config = ConfigDict(extra="forbid")
    block: Literal["hero"]
    heading: Annotated[str, Field(max_length=256)] = ""
    text: LongText = ""  # plain text; rich content goes in a content doc
    cta_label: Optional[Annotated[str, Field(max_length=64)]] = None
    cta_url: Optional[Annotated[str, Field(max_length=2048)]] = None

    @field_validator("cta_url")
    @classmethod
    def _cta_scheme(cls, value: Optional[str]) -> Optional[str]:
        return None if value is None else _check_url(value)


class SearchBlock(BaseModel):
    """A landing search box submitting to the route named by ``target_route_id``;
    ``flavour`` and ``fixed_facets`` override ``features.databrowser``."""

    model_config = ConfigDict(extra="forbid")
    block: Literal["search"]
    placeholder: Annotated[str, Field(max_length=128)] = "Search data"
    target_route_id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]
    flavour: Optional[Annotated[str, Field(max_length=64)]] = None
    fixed_facets: Optional[
        Dict[MapKey, List[Annotated[str, Field(max_length=256)]]]
    ] = None


class LinksBlock(BaseModel):
    """A landing section of link cards to arbitrary (scheme-checked) urls."""

    model_config = ConfigDict(extra="forbid")
    block: Literal["links"]
    heading: Annotated[str, Field(max_length=128)] = ""
    links: List["LinkCard"] = Field(default_factory=list)


class FeatureLinkBlock(BaseModel):
    """A landing section that links to a route by id (validated to exist)."""

    model_config = ConfigDict(extra="forbid")
    block: Literal["feature-link"]
    route_id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]
    label: Annotated[str, Field(max_length=128)]


class ContentBlock(BaseModel):
    """A landing section that embeds a content page by reference."""

    model_config = ConfigDict(extra="forbid")
    block: Literal["content"]
    ref: "ContentRef"


class CardsBlock(BaseModel):
    """A row of link cards."""

    model_config = ConfigDict(extra="forbid")
    block: Literal["cards"]
    cards: List["LinkCard"] = Field(default_factory=list)


class LinkCard(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: Annotated[str, Field(max_length=128)]
    text: Annotated[str, Field(max_length=512)] = ""
    url: Annotated[str, Field(max_length=2048)]

    @field_validator("url")
    @classmethod
    def _url_scheme(cls, value: str) -> str:
        return _check_url(value)


LandingBlock = Annotated[
    Union[
        HeroBlock,
        SearchBlock,
        ContentBlock,
        CardsBlock,
        LinksBlock,
        FeatureLinkBlock,
    ],
    Field(discriminator="block"),
]
"""One landing-page section, chosen by ``block`` - never a free-form dict."""


# header / footer - typed controls, not a bare reference


class ChromeLink(BaseModel):
    """A single link in the header or footer."""

    model_config = ConfigDict(extra="forbid")
    label: Annotated[str, Field(max_length=64)]
    url: Annotated[str, Field(max_length=2048)]

    @field_validator("url")
    @classmethod
    def _url_scheme(cls, value: str) -> str:
        return _check_url(value)


class HeaderConfig(BaseModel):
    """The header: whether it shows, its links, and an optional content banner."""

    model_config = ConfigDict(extra="forbid")
    show: bool = True
    links: List[ChromeLink] = Field(default_factory=list)
    content: Optional["ContentRef"] = None


class FooterGroup(BaseModel):
    """A titled column of links in the footer."""

    model_config = ConfigDict(extra="forbid")
    title: Annotated[str, Field(max_length=64)]
    links: List[ChromeLink] = Field(default_factory=list)


class FooterConfig(BaseModel):
    """The footer: whether it shows, its links and titled groups, its legal links
    and optional content."""

    model_config = ConfigDict(extra="forbid")
    show: bool = True
    links: List[ChromeLink] = Field(default_factory=list)
    groups: List[FooterGroup] = Field(default_factory=list)
    legal_links: List[ChromeLink] = Field(default_factory=list)
    content: Optional["ContentRef"] = None


# feature allow-list with typed per-feature options


class DatabrowserFeature(BaseModel):
    """Options for the built-in databrowser feature."""

    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    default_flavour: Annotated[str, Field(max_length=64)] = "freva"
    fixed_facets: Dict[MapKey, List[Annotated[str, Field(max_length=256)]]] = Field(
        default_factory=dict
    )


class StacFeature(BaseModel):
    """Options for the built-in STAC feature."""

    model_config = ConfigDict(extra="forbid")
    enabled: bool = False


class Features(BaseModel):
    """The feature allow-list: which built-in features this ui exposes, each with
    its own typed options; a feature the frontend does not ship cannot go here."""

    model_config = ConfigDict(extra="forbid")
    databrowser: DatabrowserFeature = Field(default_factory=DatabrowserFeature)
    stac: StacFeature = Field(default_factory=StacFeature)


# Patch-shaped mirrors of the feature models: every field optional and a nested
# map accepting null values, so a single key can be deleted.


class DatabrowserFeatureUpdate(BaseModel):
    """Patch shape for :class:`DatabrowserFeature`."""

    model_config = ConfigDict(extra="forbid")
    enabled: Optional[bool] = None
    default_flavour: Optional[Annotated[str, Field(max_length=64)]] = None
    fixed_facets: Optional[
        Dict[MapKey, Optional[List[Annotated[str, Field(max_length=256)]]]]
    ] = None


class StacFeatureUpdate(BaseModel):
    """Patch shape for :class:`StacFeature`."""

    model_config = ConfigDict(extra="forbid")
    enabled: Optional[bool] = None


class FeaturesUpdate(BaseModel):
    """Patch shape for :class:`Features`."""

    model_config = ConfigDict(extra="forbid")
    databrowser: Optional[DatabrowserFeatureUpdate] = None
    stac: Optional[StacFeatureUpdate] = None


class ContentRefUpdate(BaseModel):
    """Patch shape for :class:`ContentRef`; both halves optional, so a patch of
    one keeps the stored other and the merged pair is validated strictly."""

    model_config = ConfigDict(extra="forbid")
    ui_id: Optional[Annotated[str, Field(pattern=UI_ID_PATTERN)]] = None
    content_id: Optional[Annotated[str, Field(pattern=RECORD_ID_PATTERN)]] = None


class HeaderConfigUpdate(BaseModel):
    """Patch shape for :class:`HeaderConfig`: ``show: null`` restores the default
    and ``links``, being a list, is replaced whole."""

    model_config = ConfigDict(extra="forbid")
    show: Optional[bool] = None
    links: Optional[List["ChromeLink"]] = None
    content: Optional[ContentRefUpdate] = None


class FooterConfigUpdate(BaseModel):
    """Patch shape for :class:`FooterConfig`. Same rules as the header."""

    model_config = ConfigDict(extra="forbid")
    show: Optional[bool] = None
    links: Optional[List["ChromeLink"]] = None
    groups: Optional[List["FooterGroup"]] = None
    legal_links: Optional[List["ChromeLink"]] = None
    content: Optional[ContentRefUpdate] = None


# content references


class ContentRef(BaseModel):
    """A pointer from a ui config to a content document; the ``ui_id`` is always
    named, including ``_shared``, and there is no automatic fallback."""

    model_config = ConfigDict(extra="forbid")
    ui_id: Annotated[str, Field(pattern=UI_ID_PATTERN)]
    content_id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]


# announcements


class Announcement(BaseModel):
    """A site-wide banner; the client evaluates the active window, so the stored
    document only changes on a write and the strong etag stays honest."""

    model_config = ConfigDict(extra="forbid")
    id: Annotated[str, Field(pattern=RECORD_ID_PATTERN)]
    message: LongText  # plain text; not rendered by the api
    level: Literal["info", "warning", "critical"] = "info"
    dismissible: bool = True
    starts_at: Optional[Rfc3339] = None
    ends_at: Optional[Rfc3339] = None

    @model_validator(mode="after")
    def _window_is_forward(self) -> "Announcement":
        """A window that ends before it starts can never be open; offsets differ,
        so the comparison is on the instants rather than the strings."""
        if self.starts_at is None or self.ends_at is None:
            return self
        if datetime.fromisoformat(self.ends_at) < datetime.fromisoformat(
            self.starts_at
        ):
            raise ValueError(
                f"Announcement '{self.id}' ends at {self.ends_at}, before it "
                f"starts at {self.starts_at}, so it is never displayed. Swap "
                "them or drop one."
            )
        return self


# the ui settings resource


class UiConfig(BaseModel):
    """A complete ui configuration, one document per ``(ui, record_id)``; its
    defaults are what a GET of an empty record returns."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: int = Field(
        default=SCHEMA_VERSION,
        alias="schemaVersion",
        serialization_alias="schemaVersion",
        # Not patchable: UiConfigUpdate forbids it and the api owns the contract
        # version, so a form built from the read schema must not submit it.
        json_schema_extra={"readOnly": True},
    )

    # branding_enabled=False turns the branding chrome off without needing
    # placeholder title/logo values.
    branding_enabled: bool = True
    site_title: Annotated[str, Field(max_length=128)] = "Freva"
    site_subtitle: Annotated[str, Field(max_length=256)] = ""
    institution_name: Annotated[str, Field(max_length=256)] = "Freva"
    institution_url: Url = ""
    institution_logo: Url = ""
    favicon: Url = ""

    docs_url: Url = "https://freva-org.github.io"
    terms_url: Url = ""
    privacy_url: Url = ""

    main_color: Hex = "#286a9a"
    border_color: Hex = "#7f7f7f"
    hover_color: Hex = "#d0513a"
    extra_colors: Dict[ThemeToken, Hex] = Field(
        default_factory=dict, json_schema_extra={"x-widget": "colourmap"}
    )

    header: HeaderConfig = Field(default_factory=HeaderConfig)
    footer: FooterConfig = Field(default_factory=FooterConfig)
    homepage_heading: Annotated[str, Field(max_length=256)] = ""
    homepage_text: LongText = ""  # plain text; use a content ref for rich text
    landing_blocks: List[LandingBlock] = Field(
        default_factory=list,
        max_length=MAX_LANDING_BLOCKS,
        json_schema_extra={"x-widget": "blocks"},
    )

    # Databrowser defaults live in one place: features.databrowser. A search
    # block may override them per block; nothing else duplicates them.
    features: Features = Field(default_factory=Features)

    # the routing table and, separately, the tab bar that references it
    routes: List[Route] = Field(
        default_factory=list,
        max_length=MAX_ROUTES,
        json_schema_extra={"x-widget": "routes"},
    )
    navigation: List[NavigationItem] = Field(
        default_factory=list,
        max_length=MAX_ROUTES,
        json_schema_extra={"x-widget": "navigation"},
    )

    announcements: List[Announcement] = Field(
        default_factory=list,
        max_length=MAX_ANNOUNCEMENTS,
        json_schema_extra={"x-widget": "announcements"},
    )

    content_refs: List[ContentRef] = Field(
        default_factory=list,
        max_length=MAX_CONTENT_REFS,
    )

    public_extensions: Dict[MapKey, ExtensionValue] = Field(
        default_factory=dict,
        description="Arbitrary public values. World-readable; prefer a typed field.",
        json_schema_extra={"x-widget": "keyvalue"},
    )

    @field_validator(
        "institution_url",
        "institution_logo",
        "favicon",
        "docs_url",
        "terms_url",
        "privacy_url",
    )
    @classmethod
    def _url_scheme(cls, value: str) -> str:
        return _check_url(value)

    @field_validator("extra_colors")
    @classmethod
    def _theme_token_names(cls, value: Dict[str, str]) -> Dict[str, str]:
        import re

        for token in value:
            # fullmatch, not match: '$' also matches before a trailing newline,
            # so re.match would accept 'brand\n' as a css property name.
            if not re.fullmatch(THEME_TOKEN_PATTERN, token):
                raise ValueError(
                    f"'{token}' is not a valid theme-token name. A token becomes "
                    "a css custom property, so it must match "
                    f"{THEME_TOKEN_PATTERN} - no characters that could break out "
                    "of the property name."
                )
        return value

    @model_validator(mode="after")
    def _validate_manifest(self) -> "UiConfig":
        if len(self.routes) > MAX_ROUTES:
            raise ValueError(f"A ui may define at most {MAX_ROUTES} routes.")
        if len(self.navigation) > MAX_ROUTES:
            raise ValueError(f"A ui may define at most {MAX_ROUTES} navigation items.")
        if len(self.landing_blocks) > MAX_LANDING_BLOCKS:
            raise ValueError(
                f"A ui may define at most {MAX_LANDING_BLOCKS} landing blocks."
            )
        if len(self.announcements) > MAX_ANNOUNCEMENTS:
            raise ValueError(
                f"A ui may define at most {MAX_ANNOUNCEMENTS} announcements."
            )
        # the routing table: unique ids; unique paths among routes that have one
        # (an external route has a url, not a path); at most ONE landing route.
        route_ids = [route.id for route in self.routes]
        if len(route_ids) != len(set(route_ids)):
            raise ValueError("Route ids must be unique within a ui.")
        paths = [
            path
            for route in self.routes
            if (path := getattr(route, "path", None)) is not None
        ]
        if len(paths) != len(set(paths)):
            raise ValueError("Route paths must be unique within a ui.")
        landing = [r for r in self.routes if r.kind == "landing"]
        if len(landing) > 1:
            raise ValueError("A ui may define at most one landing route.")
        # navigation is the display layer: every tab references an existing
        # route, and no route gets two tabs.
        known = set(route_ids)
        nav_ids = [item.route_id for item in self.navigation]
        if len(nav_ids) != len(set(nav_ids)):
            raise ValueError("A route may appear at most once in the navigation.")
        unknown_nav = [rid for rid in nav_ids if rid not in known]
        if unknown_nav:
            raise ValueError(
                "Navigation references unknown route ids: "
                + ", ".join(sorted(set(unknown_nav)))
                + ". Every tab must name a route defined in 'routes'."
            )
        # landing blocks reference routes by id, never by raw path
        block_refs = []
        for block in self.landing_blocks:
            rid = getattr(block, "target_route_id", None) or getattr(
                block, "route_id", None
            )
            if rid is not None:
                block_refs.append(rid)
        unknown_blocks = [rid for rid in block_refs if rid not in known]
        if unknown_blocks:
            raise ValueError(
                "Landing blocks reference unknown route ids: "
                + ", ".join(sorted(set(unknown_blocks)))
                + "."
            )
        # A search box submits a query, so its target must be the databrowser
        # feature route: existing and correctly-typed are two different checks.
        by_id = {route.id: route for route in self.routes}
        for block in self.landing_blocks:
            if not isinstance(block, SearchBlock):
                continue
            target = by_id.get(block.target_route_id)
            if target is None:  # pragma: no cover - already reported above
                # Unreachable: a search block's target_route_id is collected
                # into block_refs, so an unknown one has raised already.
                continue
            if not isinstance(target, FeatureRoute) or (
                target.feature != SEARCH_FEATURE
            ):
                raise ValueError(
                    f"Search block targets route '{block.target_route_id}', "
                    f"which is a '{target.kind}' route. A search block submits "
                    "a query, so it must target a feature route with "
                    f"feature='{SEARCH_FEATURE}'."
                )
        ann_ids = [a.id for a in self.announcements]
        if len(ann_ids) != len(set(ann_ids)):
            raise ValueError("Announcement ids must be unique within a ui.")
        return self


class UiConfigUpdate(BaseModel):
    """The PATCH body for a ui record: every field optional, with
    ``exclude_unset`` telling an omitted field from one set to ``null``."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    branding_enabled: Optional[bool] = None
    site_title: Optional[str] = Field(default=None, max_length=128)
    site_subtitle: Optional[str] = Field(default=None, max_length=256)
    institution_name: Optional[str] = Field(default=None, max_length=256)
    # json_schema_extra on the field, not only on the annotated type: for an
    # Optional field pydantic buries the type's extras in the non-null anyOf.
    institution_url: Optional[Url] = Field(
        default=None, json_schema_extra={"x-widget": "url"}
    )
    institution_logo: Optional[Url] = Field(
        default=None, json_schema_extra={"x-widget": "url"}
    )
    favicon: Optional[Url] = Field(default=None, json_schema_extra={"x-widget": "url"})
    docs_url: Optional[Url] = Field(default=None, json_schema_extra={"x-widget": "url"})
    terms_url: Optional[Url] = Field(
        default=None, json_schema_extra={"x-widget": "url"}
    )
    privacy_url: Optional[Url] = Field(
        default=None, json_schema_extra={"x-widget": "url"}
    )
    # The same annotated types the read model uses, so the update schema an
    # editing form is generated from carries the same hints and constraints.
    main_color: Optional[Hex] = Field(
        default=None, json_schema_extra={"x-widget": "colour"}
    )
    border_color: Optional[Hex] = Field(
        default=None, json_schema_extra={"x-widget": "colour"}
    )
    hover_color: Optional[Hex] = Field(
        default=None, json_schema_extra={"x-widget": "colour"}
    )
    extra_colors: Optional[Dict[ThemeToken, Optional[Hex]]] = Field(
        default=None, json_schema_extra={"x-widget": "colourmap"}
    )
    header: Optional[HeaderConfigUpdate] = None
    footer: Optional[FooterConfigUpdate] = None
    homepage_heading: Optional[str] = Field(default=None, max_length=256)
    homepage_text: Optional[LongText] = Field(
        default=None, json_schema_extra={"x-widget": "textarea"}
    )
    landing_blocks: Optional[List[LandingBlock]] = Field(
        default=None,
        max_length=MAX_LANDING_BLOCKS,
        json_schema_extra={"x-widget": "blocks"},
    )
    features: Optional[FeaturesUpdate] = None
    routes: Optional[List[Route]] = Field(
        default=None, max_length=MAX_ROUTES, json_schema_extra={"x-widget": "routes"}
    )
    navigation: Optional[List[NavigationItem]] = Field(
        default=None,
        max_length=MAX_ROUTES,
        json_schema_extra={"x-widget": "navigation"},
    )
    announcements: Optional[List[Announcement]] = Field(
        default=None,
        max_length=MAX_ANNOUNCEMENTS,
        json_schema_extra={"x-widget": "announcements"},
    )
    content_refs: Optional[List[ContentRef]] = Field(
        default=None,
        max_length=MAX_CONTENT_REFS,
    )
    public_extensions: Optional[Dict[MapKey, Optional[ExtensionValue]]] = Field(
        default=None, json_schema_extra={"x-widget": "keyvalue"}
    )

    # The field list must match the read model's validator exactly; a field
    # missing here lets a url scheme the read model refuses in on the way in.
    @field_validator(
        "institution_url",
        "institution_logo",
        "favicon",
        "docs_url",
        "terms_url",
        "privacy_url",
    )
    @classmethod
    def _url_scheme(cls, value: Optional[str]) -> Optional[str]:
        return None if value is None else _check_url(value)


# content documents

ContentFormat = Literal["markdown", "rst", "html-fragment", "sandbox-html"]

VALID_CONTENT_FORMATS = frozenset(get_args(ContentFormat))
"""The same set as ``ContentFormat`` as a runtime membership test, letting the
read path degrade on a stored format this build cannot serve."""


class ContentSource(BaseModel):
    """The PATCH body for a content document: ``source`` and ``format`` drive
    rendering, while ``title`` is metadata that never triggers a re-render."""

    model_config = ConfigDict(extra="forbid")
    format: Optional[ContentFormat] = None
    source: Optional[str] = Field(default=None, max_length=256 * 1024)
    title: Optional[str] = Field(default=None, max_length=256)


class ContentPublic(BaseModel):
    """The public read shape, never the source; for ``sandbox-html``
    ``rendered_html`` is empty and the document comes from its ``/document`` url."""

    model_config = ConfigDict(extra="forbid")
    ui_id: str
    content_id: str
    format: ContentFormat
    title: str = ""
    rendered_html: str = ""
    renderer_version: str = ""
    """The renderer generation, not the full fingerprint, because this response is
    world-readable and the fingerprint names installed dependency versions."""
    is_stale: bool = False
    revision: int = 0
    updated_at: str = ""
    is_sandbox: bool = False


class ContentAdmin(ContentPublic):
    """The administrator read shape (``?include_source=true``): the public shape
    plus the raw source and its hash, required so the two shapes stay disjoint."""

    source: str
    renderer_fingerprint: str = ""
    """The full stored renderer identity, including dependency versions; admin
    only, and the reason ``renderer_version`` can stay coarse."""
    source_hash: str


# Blocks and header/footer name ContentRef and LinkCard before those are
# defined, so the forward references are resolved once everything is in scope.
ContentBlock.model_rebuild()
CardsBlock.model_rebuild()
LinksBlock.model_rebuild()
HeaderConfig.model_rebuild()
FooterConfig.model_rebuild()
HeaderConfigUpdate.model_rebuild()
FooterConfigUpdate.model_rebuild()
UiConfig.model_rebuild()
UiConfigUpdate.model_rebuild()
