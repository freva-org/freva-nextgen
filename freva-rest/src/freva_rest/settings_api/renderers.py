"""Convert content source to sanitized html, once, at write time.
``sandbox-html`` is not handled here: it stays executable behind an iframe.
"""

import copy
import hashlib
import importlib
import io
import re
from typing import Any, Callable, Dict, Optional, Tuple

from freva_rest.logger import logger

from .sanitizer import stable_sanitize

MAX_SOURCE_BYTES = 256 * 1024
"""Capped well below the request body limit: these are pages, not uploads."""

MAX_RENDERED_BYTES = 512 * 1024
"""Rendered html exceeds its source as tags expand, but ships to every visitor
and so stays bounded."""


def source_hash(source: str, fmt: str) -> str:
    """Identity of a rendered output: the source plus the format that shaped it.
    A metadata-only patch that leaves both unchanged reuses the stored html.
    """
    digest = hashlib.sha256()
    digest.update(fmt.encode("utf-8"))
    digest.update(b"\x00")
    digest.update(source.encode("utf-8"))
    return digest.hexdigest()


def rendered_hash(html: str) -> str:
    """Unkeyed drift-and-corruption checksum for rendered html, not an
    authenticator; it omits the renderer version, which ``is_stale`` answers.
    """
    digest = hashlib.sha256()
    digest.update(b"rendered-html\x00")
    digest.update(html.encode("utf-8", "surrogatepass"))
    return digest.hexdigest()


def _require(module: str, package: str) -> Any:
    """Import a renderer backend, or fail closed with a clear message.
    The caller turns the RuntimeError into a 422 rather than a 500.
    """
    try:
        return importlib.import_module(module)
    except ImportError as error:  # pragma: no cover - deployment misconfig
        raise RuntimeError(
            f"rendering this format needs the '{package}' package; it is not "
            "installed, so the content is refused rather than served unrendered."
        ) from error


MARKDOWN_PLUGINS: Tuple[str, ...] = ("table", "url", "strikethrough", "def_list")
"""Mistune 3 plugins whose markup the sanitizer's allow-list already carries;
plugins emitting ids, form controls, ``style`` or new url schemes stay off."""

HIGHLIGHT_LANGUAGES: Dict[str, str] = {
    # The value is the canonical name used for both lexer and class attribute,
    # so nothing an author writes reaches the html.
    "bash": "bash",
    "c": "c",
    "cdo": "bash",
    "cfg": "ini",
    "cmake": "cmake",
    "console": "console",
    "cpp": "cpp",
    "c++": "cpp",
    "css": "css",
    "diff": "diff",
    "docker": "docker",
    "dockerfile": "docker",
    "f90": "fortran",
    "fortran": "fortran",
    "html": "html",
    "ini": "ini",
    "javascript": "javascript",
    "js": "javascript",
    "json": "json",
    "julia": "julia",
    "make": "make",
    "makefile": "make",
    "matlab": "matlab",
    "ncl": "ncl",
    "python": "python",
    "py": "python",
    "python3": "python",
    "r": "r",
    "rst": "rst",
    "sh": "bash",
    "shell": "bash",
    "sql": "sql",
    "toml": "toml",
    "ts": "typescript",
    "typescript": "typescript",
    "xml": "xml",
    "yaml": "yaml",
    "yml": "yaml",
    "zsh": "bash",
}
"""Fence languages that get server-side highlighting, mapped to a canonical
name. Anything not here renders as escaped plain code."""

EXPENSIVE_LEXERS = frozenset({"console", "ini", "make", "r"})
"""Canonical lexers measured quadratic on adversarial input; every other lexer
in :data:`HIGHLIGHT_LANGUAGES` is linear. ``cfg`` maps to ``ini``."""

MAX_HIGHLIGHT_BLOCK_BYTES = 4 * 1024
"""Source bytes one ordinary fenced block may hand to pygments; bounding the
input is the only real bound, since cancelling an await frees no worker thread."""

EXPENSIVE_HIGHLIGHT_BLOCK_BYTES = 1 * 1024
"""The same, for a lexer in :data:`EXPENSIVE_LEXERS`, whose quadratic cost makes
the smaller cap necessary."""

MAX_HIGHLIGHT_DOCUMENT_BYTES = 16 * 1024
"""Total source bytes highlighted across one document, since a per-block cap
alone lets many legal blocks multiply the cost. Past it, code renders escaped."""


MIN_HIGHLIGHT_CHARGE = 256
"""The smallest charge a highlighted block may incur, so the document budget
bounds pygments invocations too: empty fences would otherwise cost nothing."""

MAX_HIGHLIGHT_BLOCKS = MAX_HIGHLIGHT_DOCUMENT_BYTES // MIN_HIGHLIGHT_CHARGE
"""How many blocks one document may highlight, derived from the byte budget so
the two cannot drift apart."""


class _HighlightBudget:
    """The highlighting budget for one render, so concurrent renders in worker
    threads cannot spend each other's source bytes or pygments calls.
    """

    __slots__ = ("remaining", "blocks")

    def __init__(self, total: int = MAX_HIGHLIGHT_DOCUMENT_BYTES) -> None:
        self.remaining = total
        self.blocks = 0

    def take(self, amount: int) -> bool:
        """Reserve a block, or report that the budget is spent; an empty block
        still costs :data:`MIN_HIGHLIGHT_CHARGE`.
        """
        if self.blocks >= MAX_HIGHLIGHT_BLOCKS:
            return False
        charge = max(amount, MIN_HIGHLIGHT_CHARGE)
        if charge > self.remaining:
            return False
        self.remaining -= charge
        self.blocks += 1
        return True


def _highlight(code: str, language: str) -> Optional[str]:
    """Pygments markup for ``code``, or ``None`` to fall back to plain text."""
    try:
        pygments = _require("pygments", "pygments")
        lexers = _require("pygments.lexers", "pygments")
        formatters = _require("pygments.formatters", "pygments")
        util = _require("pygments.util", "pygments")
    except RuntimeError:  # pragma: no cover - deployment misconfig
        # Highlighting is an enhancement; without pygments code stays plain.
        return None
    try:
        lexer = lexers.get_lexer_by_name(language)
    except util.ClassNotFound:
        # Narrow on purpose: a blanket except would hide a bug in the map above.
        return None
    # nowrap=True: token spans only, no wrapper and no embedded stylesheet,
    # since the portal ships the styles for these classes.
    return str(pygments.highlight(code, lexer, formatters.HtmlFormatter(nowrap=True)))


def _block_code(code: str, info: Optional[str], budget: "_HighlightBudget") -> str:
    """Render a fenced code block, highlighting only within the budgets.
    The author-controlled ``info`` never reaches the html, only a canonical name.
    """
    from html import escape

    stripped = (info or "").strip()
    token = stripped.split(None, 1)[0].lower() if stripped else ""
    language = HIGHLIGHT_LANGUAGES.get(token)
    if language is not None:
        size = len(code.encode("utf-8"))
        cap = (
            EXPENSIVE_HIGHLIGHT_BLOCK_BYTES
            if language in EXPENSIVE_LEXERS
            else MAX_HIGHLIGHT_BLOCK_BYTES
        )
        # Cap first, so an oversized block does not consume budget a later
        # legal block could have used.
        if size <= cap and budget.take(size):
            highlighted = _highlight(code, language)
            if highlighted is not None:
                return (
                    f'<pre><code class="language-{escape(language, quote=True)}">'
                    f"{highlighted}</code></pre>\n"
                )
    # Escaped here rather than trusted to the sanitizer, so the fallback is
    # safe on its own terms.
    body = escape(code, quote=False)
    if language is None:
        return f"<pre><code>{body}</code></pre>\n"
    return (
        f'<pre><code class="language-{escape(language, quote=True)}">'
        f"{body}</code></pre>\n"
    )


def _render_markdown(source: str) -> str:
    mistune = _require("mistune", "mistune")

    # One budget per render, so concurrent renders in worker threads are
    # independent.
    budget = _HighlightBudget()

    # `escape=True` makes the parser escape raw html in the markdown before the
    # sanitizer sees it; a markdown-only defence, nh3 is the boundary for rst.
    class _Renderer(mistune.HTMLRenderer):  # type: ignore[misc, name-defined]
        def block_code(
            self, code: str, info: Optional[str] = None
        ) -> str:  # noqa: D102
            return _block_code(code, info, budget)

    md = mistune.create_markdown(
        escape=True,
        renderer=_Renderer(escape=True),
        plugins=list(MARKDOWN_PLUGINS),
    )
    return str(md(source))


def _rst_settings(writer: Any) -> Any:
    """The hardened docutils settings shared by every rst render, shutting the
    directives that read local files or inject raw html.
    """
    get_default_settings = _require(
        "docutils.frontend", "docutils"
    ).get_default_settings
    Parser = _require("docutils.parsers.rst", "docutils").Parser

    settings = get_default_settings(Parser, writer)
    settings.file_insertion_enabled = False  # no .. include::  / .. raw:: file
    settings.raw_enabled = False  # no .. raw:: html injection
    settings.embed_stylesheet = False
    settings.input_encoding = "unicode"
    settings.line_length_limit = 50_000  # bounded input
    settings.halt_level = 5  # never raise; a bad directive is dropped, not fatal
    # Raise rather than call sys.exit, which docutils otherwise does on a
    # failure it does not recognise. _render_rst does not rely on this alone.
    settings.traceback = True
    # Keep the document title in the body: the promoted parts["title"] is not
    # returned here, so the page would lose its heading.
    settings.doctitle_xform = False
    settings.sectsubtitle_xform = False
    # The title stays a section heading, so start at h1 rather than docutils'
    # default h2.
    settings.initial_header_level = 1
    # Equations as plain html plus classes the portal styles; the markup is
    # author-influenced, so nh3 remains the boundary for rst math.
    settings.math_output = "HTML math.css"
    # Otherwise `.. code-block::` reaches pygments directly, bypassing
    # HIGHLIGHT_LANGUAGES and every byte budget the markdown path enforces.
    settings.syntax_highlight = "none"
    return settings


# Pristine copy of docutils' math tag table, whose class-level lists a hostile
# `.. math:: :class:` would otherwise mutate for every later render.
_PRISTINE_MATH_TAGS: Optional[Dict[str, Any]] = None

_SAFE_CLASS = re.compile(r"[A-Za-z0-9_+.-]{1,40}")
"""What may survive in a docutils-generated class attribute."""


def _null_parser() -> Any:
    """The do-nothing parser a doctree reader needs but never uses."""
    return _require("docutils.parsers.null", "docutils").Parser()


def _rst_writer() -> Any:
    """A writer whose translator owns its math tag table."""
    global _PRISTINE_MATH_TAGS
    html5 = _require("docutils.writers.html5_polyglot", "docutils")
    writer = html5.Writer()
    base = writer.translator_class
    if _PRISTINE_MATH_TAGS is None:
        # Safe lazily: nothing mutates the base class's table, so the first
        # capture is the pristine one.
        _PRISTINE_MATH_TAGS = copy.deepcopy(base.math_tags)

    def _starttag(
        self: Any,
        node: Any,
        tagname: str,
        suffix: str = "\n",
        empty: bool = False,
        **attributes: Any,
    ) -> Any:
        """Never let the caller's ``classes`` list be mutated in place, since
        every formula in one render is handed the same shared list.
        """
        classes = attributes.get("classes")
        if isinstance(classes, list):
            attributes["classes"] = list(classes)
        return base.starttag(self, node, tagname, suffix, empty, **attributes)

    writer.translator_class = type(
        "_IsolatedMathTranslator",
        (base,),
        {
            "math_tags": copy.deepcopy(_PRISTINE_MATH_TAGS),
            "starttag": _starttag,
        },
    )
    return writer


def _normalise_code_classes(document: Any) -> None:
    """Drop class tokens docutils built from author text, which is escaped but
    would still sit in an attribute a downstream html-grepper could misread.
    """
    for node in document.findall(lambda n: n.tagname == "literal_block"):
        node["classes"] = [
            token
            for token in node["classes"]
            if isinstance(token, str) and _SAFE_CLASS.fullmatch(token)
        ]


CONTENTS_DROPPED_NOTE = (
    '<string>: (INFO/1) The generated table of contents from ".. contents::" '
    "was removed; the portal builds its own from the headings."
)
"""The dropped-content log line for a removed generated toc, shaped like the
docutils system messages it shares a list and a warning with."""


def _strip_generated_contents(document: Any) -> int:
    """Remove the toc ``.. contents::`` generates, at the doctree because the
    sanitizer would unwrap the ``<nav>`` and orphan its list of dead links.
    """
    nodes = _require("docutils.nodes", "docutils")
    removed = 0
    for topic in list(document.findall(nodes.topic)):
        if "contents" in topic.get("classes", ()):
            topic.parent.remove(topic)
            removed += 1
    if removed:
        # Only when a toc was removed, so a document the directive never ran on
        # keeps its title attributes.
        for title in document.findall(nodes.title):
            title.attributes.pop("refid", None)
    return removed


RST_FAILURE_DETAIL = "This reStructuredText content could not be rendered."
"""What a client is told when docutils fails: stable and uninformative, because
the real exception names docutils internals and is logged for operators."""


def _rst_failed(stage: str, error: BaseException) -> ValueError:
    """Log what actually happened, return what the client is told."""
    logger.error(
        "rst content could not be %s (%s: %s)",
        stage,
        type(error).__name__,
        error,
    )
    return ValueError(RST_FAILURE_DETAIL)


def _render_rst(source: str) -> str:
    """Parse once, write once, and never let docutils end the process.
    ``SystemExit`` is caught by name; shutdown exceptions still propagate.
    """
    core = _require("docutils.core", "docutils")
    doctree_reader = _require("docutils.readers.doctree", "docutils")
    docutils_io = _require("docutils.io", "docutils")

    # report_level 2 records the dropped directives on the doctree; the write
    # below runs at 5, where the writer skips those nodes.
    parse_writer = _rst_writer()
    parse_settings = _rst_settings(parse_writer)
    parse_settings.report_level = 2
    parse_settings.warning_stream = io.StringIO()
    try:
        document = core.publish_doctree(source=source, settings=parse_settings)
    except SystemExit as error:  # pragma: no cover - traceback=True precedes it
        raise _rst_failed("parsed", error) from error
    except Exception as error:
        raise _rst_failed("parsed", error) from error

    dropped = [
        node.astext().splitlines()[0]
        for node in document.findall(lambda n: n.tagname == "system_message")
    ]
    _normalise_code_classes(document)
    if _strip_generated_contents(document):
        # The author wrote a directive and the page lacks what it asked for.
        dropped.append(CONTENTS_DROPPED_NOTE)

    # One write from the same doctree, so math conversion runs exactly once.
    write_writer = _rst_writer()
    write_settings = _rst_settings(write_writer)
    write_settings.report_level = 5  # never emit system-message html
    try:
        parts = core.publish_parts(
            source=document,
            source_class=docutils_io.DocTreeInput,
            # `parser` rather than `parser_name`, which docutils 2.0 removes;
            # a doctree reader never parses.
            reader=doctree_reader.Reader(parser=_null_parser()),
            writer=write_writer,
            settings=write_settings,
        )
    except SystemExit as error:
        # A malformed formula reaching math2html while writing makes docutils
        # exit the process.
        raise _rst_failed("rendered", error) from error
    except Exception as error:
        raise _rst_failed("rendered", error) from error

    if dropped:
        # The author gets a 200, so without this nobody finds out a directive
        # was thrown away.
        logger.warning(
            "rst content dropped %d directive(s) during rendering: %s",
            len(dropped),
            "; ".join(dropped[:10]),
        )
    # `docinfo` holds a leading field list and is the only content-bearing part
    # outside `body`, so returning the body alone would silently drop it.
    return str(parts.get("docinfo") or "") + str(parts["body"])


def _render_html(source: str) -> str:
    # Author-supplied html fragment: nothing to convert, straight to sanitizer.
    return source


_RENDERERS: Dict[str, Callable[[str], str]] = {
    "markdown": _render_markdown,
    "rst": _render_rst,
    "html-fragment": _render_html,
}

RENDERABLE_FORMATS = frozenset(_RENDERERS)
"""Formats that produce main-DOM html; ``sandbox-html`` is not here, being
served only as an isolated iframe document."""


def render(source: str, fmt: str) -> str:
    """Convert and sanitize, returning storable html. Raises on any failure.
    The caller turns a raised exception into a 422 and keeps the stored html.
    """
    if fmt not in _RENDERERS:
        raise ValueError(f"'{fmt}' is not a renderable format.")
    if len(source.encode("utf-8")) > MAX_SOURCE_BYTES:
        raise ValueError(f"The source is larger than {MAX_SOURCE_BYTES} bytes.")
    # The fixed point, not one pass: the size check must measure the bytes
    # actually served, and a second pass can be larger than the first.
    html = stable_sanitize(_RENDERERS[fmt](source))
    if len(html.encode("utf-8")) > MAX_RENDERED_BYTES:
        raise ValueError(
            f"The rendered html is larger than {MAX_RENDERED_BYTES} bytes."
        )
    return html
