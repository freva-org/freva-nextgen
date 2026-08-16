"""The one allow-list every rendered markdown, rst and author html passes through.
``nh3`` parses like a browser and is allow-list based, so obfuscation cannot pass.
"""

import re
from importlib import metadata
from typing import Dict, Optional, Set, Tuple

RENDERER_VERSION = "1"
"""The hand-maintained half of :data:`RENDERER_FINGERPRINT`: any change to the
html rendered from the same source takes the next generation."""

RENDERING_DEPENDENCIES: Tuple[str, ...] = (
    "docutils",
    "mistune",
    "nh3",
    "pygments",
)
"""Every installed package whose version can change rendered output.
``pygments`` is here so a routine bump marks documents stale on its own."""


def _dependency_versions() -> str:
    parts = []
    for package in sorted(RENDERING_DEPENDENCIES):
        try:
            # Reads the installed distribution's metadata without importing the
            # package, so the renderers' lazy imports stay lazy.
            version = metadata.version(package)
        except metadata.PackageNotFoundError:  # pragma: no cover - not installed
            version = "absent"
        parts.append(f"{package}={version}")
    return "+".join(parts)


RENDERER_FINGERPRINT = f"{RENDERER_VERSION}+{_dependency_versions()}"
"""The renderer identity stored on every document and compared by ``is_stale``.
Deriving it from installed versions catches upgrades inside the pinned ranges."""


def renderer_generation(fingerprint: str) -> str:
    """The generation prefix of a fingerprint; the rest is a private inventory.
    A malformed value passes through, since the read path must never raise."""
    return fingerprint.split("+", 1)[0]


ALLOWED_TAGS: Set[str] = {
    "p",
    "br",
    "hr",
    "span",
    "div",
    "b",
    "strong",
    "i",
    "em",
    "u",
    "s",
    "del",
    "sub",
    "sup",
    "small",
    "mark",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "ul",
    "ol",
    "li",
    "dl",
    "dt",
    "dd",
    "blockquote",
    "pre",
    "code",
    "kbd",
    "samp",
    "a",
    "img",
    "table",
    "thead",
    "tbody",
    "tfoot",
    "tr",
    "th",
    "td",
    "caption",
    "colgroup",
    "col",
    "figure",
    "figcaption",
    "section",
    "aside",
    "cite",
}
"""Every tag an author may end up with. ``script``, ``style``, ``iframe``,
``object``, ``embed``, ``form``, ``input`` and ``nav`` are absent on purpose."""

_COMMON = {"class", "title"}
ALLOWED_ATTRS: Dict[str, Set[str]] = {
    "a": _COMMON | {"href", "target"},  # rel is managed by nh3, see link_rel
    "img": _COMMON | {"src", "alt", "width", "height", "loading"},
    "td": _COMMON | {"colspan", "rowspan"},
    "th": _COMMON | {"colspan", "rowspan", "scope"},
    "col": {"span"},
    "colgroup": {"span"},
    "code": _COMMON,
    "pre": _COMMON,
    "span": _COMMON,
    # docutils' math writer carries equation positioning in classes; without
    # `class` the markup collapses into unpositioned inline text.
    "i": _COMMON,
    "sub": _COMMON,
    "sup": _COMMON,
    "div": _COMMON,
    "p": _COMMON,
    "table": _COMMON,
    "blockquote": _COMMON,
    "figure": _COMMON,
    "figcaption": _COMMON,
    # docutils encodes the *kind* of a description list in its classes and
    # nowhere else; stripped, docinfo, field list and definition list are alike.
    "dl": _COMMON,
    "dt": _COMMON,
    "dd": _COMMON,
    # Structural wrappers where `class` is the whole point: it is what marks an
    # `aside` a note rather than a warning, and carries rst `.. class::` intent.
    "aside": _COMMON,
    "section": _COMMON,
}
"""Attributes kept per tag: no ``on*`` handler, no ``style`` and no ``id``.
``nh3`` also allows inert ``title`` and ``lang`` on every element regardless."""

ALLOWED_URL_SCHEMES: Set[str] = {"http", "https", "mailto"}
"""Schemes allowed in ``href``/``src``. Blocks ``javascript:``, ``data:``,
``vbscript:``, ``file:`` - the classic script and exfiltration vectors."""

LINK_REL = "noopener noreferrer nofollow"
"""``rel`` forced onto every link. ``noopener`` closes the ``window.opener``
reverse-tabnabbing hole that ``target="_blank"`` opens."""

STRIP_CONTENT_TAGS: Set[str] = {"script", "style"}
"""Tags whose *content* is removed as well as the tag - a bare strip would leave
the script body as visible text."""

_URL_ATTRS = {"href", "src"}


_C0_OR_SPACE = re.compile(r"^[\x00-\x20]+|[\x00-\x20]+$")
"""The WHATWG "C0 control or space" range a browser strips from both ends of a
url before parsing it. See https://url.spec.whatwg.org/."""

_TAB_OR_NEWLINE = str.maketrans("", "", "\t\n\r")
"""ASCII tab and newline, which a browser removes from *anywhere* in a url."""


def _as_a_browser_reads_it(value: str) -> str:
    """The url a browser would actually parse: ``str.strip()`` leaves C0 controls
    that revive a protocol-relative url. C1 controls stay, as the parser does."""
    return _C0_OR_SPACE.sub("", value.translate(_TAB_OR_NEWLINE))


def _drop_protocol_relative(tag: str, attr: str, value: str) -> Optional[str]:
    """Remove a protocol-relative url from an href/src: ``url_schemes`` blocks
    only *schemed* urls, and a browser reads ``//evil.example`` as off-site."""
    if attr in _URL_ATTRS:
        probe = _as_a_browser_reads_it(value).replace("\\", "/")
        if probe.startswith("//"):
            return None
    return value


def sanitize_html(html: str) -> str:
    """Reduce arbitrary html to the allow-list. The one place markup is trusted.
    Fails closed: a missing ``nh3`` raises rather than serving unsanitised html."""
    if not html:
        return html
    try:
        import nh3
    except ImportError as error:  # pragma: no cover - deployment misconfig
        raise RuntimeError(
            "html rendering needs the 'nh3' package; it is not installed, so "
            "content is refused rather than served unsanitised. nh3 is the "
            "maintained successor to bleach (retired 2026-06-05)."
        ) from error
    return nh3.clean(
        html,
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRS,
        url_schemes=ALLOWED_URL_SCHEMES,
        link_rel=LINK_REL,
        clean_content_tags=STRIP_CONTENT_TAGS,
        attribute_filter=_drop_protocol_relative,
    )


MAX_SANITIZE_PASSES = 4
"""How many times :func:`stable_sanitize` will re-run before giving up."""


def stable_sanitize(html: str) -> str:
    """Sanitize until the output stops changing, and return that fixed point.
    The sanitizer is not idempotent: repaired nesting can itself need repair."""
    current = sanitize_html(html)
    for _ in range(MAX_SANITIZE_PASSES - 1):
        following = sanitize_html(current)
        if following == current:
            return current
        current = following
    raise ValueError(
        "This markup does not stabilise under sanitisation within "
        f"{MAX_SANITIZE_PASSES} passes - it is too malformed to store a "
        "predictable rendering for. Fix the tag nesting and retry."
    )
