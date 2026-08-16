Runtime settings and UI content
===============================

The settings API stores instance configuration in MongoDB and serves it over
REST, so a deployment can be re-branded and re-configured at runtime without a
redeploy. Reads are public; writes require an administrator.

Everything lives under one namespace::

    /api/freva-nextgen/settings

Settings records
----------------

A settings *resource* (``ui`` today) has one document per *record id*, so one
deployment can serve several complete configurations::

    GET    /api/freva-nextgen/settings/{resource}/_schema
    GET    /api/freva-nextgen/settings/{resource}/{record_id}
    PATCH  /api/freva-nextgen/settings/{resource}/{record_id}
    DELETE /api/freva-nextgen/settings/{resource}/{record_id}

    /settings/ui/default        /settings/ui/waterpark        /settings/ui/esgf

The default ui is ``/settings/ui/default``; there is no short alias, so nothing
has two names in the OpenAPI schema. That record is *synthesised*: a ``GET``
returns ``200`` built from the model defaults before any document exists.

Introspection is ``_schema``, with a leading underscore, because a record id may
not start with one (``RECORD_ID_PATTERN``). ``schema`` is otherwise a legal record
id, so ``/{resource}/schema`` would address a *record* under ``PATCH`` and
``DELETE`` while ``GET`` returned introspection. Content ids are unaffected.

Records are addressed by a deterministic ``_id`` (``"{resource}:{record_id}"``),
whose uniqueness MongoDB enforces natively, and are fully isolated from one
another. Adding a resource is one registry entry mapping a name to its model and
update model - no storage or endpoint change.

There are **no secondary indexes on either collection**: nothing queries
``resource_name`` or ``record_id`` as fields. The delete guard filters on an
``_id`` regex, the audit and rebuild sort and range over ``_id`` - which is what
makes them resumable - and the content lookup uses ``_id: {$in: [...]}``. A
by-resource listing endpoint would be the thing that justifies an index.

Deleting ``default`` means reset
................................

``default`` always has a representation, so ``DELETE /settings/ui/default``
discards the stored override and returns ``204`` **whether or not there was
one**; a subsequent ``GET`` returns the built-in defaults. The reset is therefore
idempotent and ``If-Match: *`` succeeds on it. ``DELETE`` of a *named* record
that does not exist is a ``404``.

Conditional DELETE
..................

Both ``DELETE`` routes accept an optional strong ``If-Match``, which is a
different guarantee from the internal compare-and-swap: the CAS only covers
changes made *during* the request, and does nothing about a client that read
revision 1, missed someone else's ``PATCH`` to revision 2, and then deleted.
The precondition and the CAS are evaluated against **one snapshot**
(``cas_snapshot()`` returns the document with its ``(revision, cas_token)``); a
second read would reopen the window the header exists to close.

Content ``DELETE`` accepts either tag a ``GET`` hands out - the public one and
the ``include_source=true`` admin one. Weak tags never match. When the target
does not exist the request is a ``404`` and the precondition is ignored, per
:rfc:`9110` #13.1.1.

UI content
----------

Page bodies (Markdown, RST, HTML) are **not** embedded in the UI configuration.
Each page is its own document, addressed under the owning UI::

    GET    /api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}
    PATCH  /api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}
    DELETE /api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}

The ``_id`` is ``"{ui_id}:{content_id}"``, so the same content id is independent
under different UIs (``ui/default/contents/home`` and
``ui/waterpark/contents/home`` are different pages). Content shared by several
UIs lives under the reserved ``_shared`` namespace and is referenced explicitly -
there is no automatic fallback between a named UI, ``default`` and ``_shared``. A
UI configuration references content with an explicit ``{ui_id, content_id}`` pair.

Rendering lifecycle
-------------------

Rendering happens on a content ``PATCH``, and only when ``source`` or ``format``
changes::

    validate source -> convert (markdown/rst) -> sanitize -> check size -> store

Whether a ``PATCH`` renders is decided by comparing the **effective** source and
format against what is stored, not by which keys the client sent - an editor that
saves the whole document round-trips every field.

A patch that leaves source and format unchanged is **metadata-only**: it carries
``rendered_html``, ``rendered_hash``, ``source_hash`` and ``renderer_version``
across verbatim, never calls the renderer, and cannot repair or migrate a
rendering. So renaming a page is not a renderer migration, it succeeds even when
the historical source would no longer render, and an inconsistent document stays
visibly inconsistent - nothing is recomputed, so drift cannot be laundered into
apparent consistency.

The HTML a metadata patch preserves may have been written straight into the
database, so the write path builds a **sanitised serving view** after the write
and uses it for the response body, the read cache and the last-known-good copy.
That also makes a write's ``ETag`` equal to the one the next ``GET`` produces.

A stored ``source`` that is *absent* is not an empty one: such a document is
refused with ``422`` naming the repair, and the rebuild counts it ``failed`` and
leaves it untouched. An explicitly empty ``source`` is a valid source.

Nothing is re-rendered on a ``GET``. Three stored fields answer three different
questions:

* ``source_hash`` covers the source and the format that shaped it;
* ``rendered_hash`` covers the stored HTML and **only** the HTML - it mixes in no
  renderer identity, keeping *integrity* ("is this HTML what the digest beside it
  says?") separate from *freshness* ("did the renderer running now produce it?");
* ``renderer_version`` answers freshness on its own.

``is_stale`` on a read and "needs rebuilding" in the migration pass are one
verdict, computed once on the stored document. A document is stale if its
renderer identity is behind, its ``rendered_html`` is not a string, its
``rendered_hash`` does not match that HTML, its ``source`` is missing or not a
string, its ``source_hash`` no longer matches that source, **or** the HTML
changes under sanitisation. The last case catches HTML written straight into
MongoDB with a matching digest: it passes every field check while the read, which
sanitises on the way out, serves something else.

.. warning::

   **Neither digest is an authenticator.** Both are unkeyed SHA-256 over public
   inputs, and ``rendered_hash`` is bound to neither the document's identity nor
   its source, so a valid ``(rendered_html, rendered_hash)`` pair copied from
   another record verifies. They detect drift and corruption. They prove nothing
   about who produced the HTML.

The guarantee comes from the sanitiser, which runs on the way **out** as well as
in: whatever is in ``rendered_html``, and whoever put it there, what ships to a
browser has been through the allow-list.

.. important::

   **The sanitiser is not idempotent.** It repairs malformed nesting by closing
   and reopening tags, and that repair can itself need repairing, so a second
   pass legitimately produces different - and larger - output::

       source = "<a><ul><div><table></ul></div><a><caption>" * 3799
       len(sanitize_html(source).encode())                  # 524_262
       len(sanitize_html(sanitize_html(source)).encode())   # 843_378

   Reproduced on nh3 0.2.15 and 0.3.6.

On **write**, ``render()`` therefore sanitises to a *fixed point*
(``stable_sanitize``, at most four passes) and enforces ``MAX_RENDERED_BYTES``
against that fixed point; markup that will not converge is refused with ``422``.
On **read**, re-sanitising is a no-op for anything this server wrote, so bytes and
``ETag`` do not move - only out-of-band markup changes. That pass is bounded and
paid once per document: HTML already over the ceiling is served empty *without*
being sanitised (a 4 MiB ``<a>x</a>`` blob expands to ~21.5 MiB in ~2.3 s), output
over it afterwards is dropped, the *sanitised* form is what gets cached, and the
work runs in ``asyncio.to_thread`` where ``nh3`` releases the GIL. A write-side
read keeps the raw stored HTML, since its CAS predicate compares against what is
actually stored.

A document whose ``rendered_hash`` is missing or wrong is still served
(sanitised) while flagged ``is_stale`` for the next rebuild. ``sandbox-html`` is
the deliberate exception: executable by design, never in ``rendered_html``, never
through the allow-list, served only through the iframe ``/document`` endpoint.

``json.loads`` accepts a lone surrogate such as ``"\ud800"`` that no UTF-8 encoder
will serialise, and a constrained ``str`` field rejects it while an
``ExtensionValue`` in an open map does not. Every write therefore walks its
candidate document - keys as well as values, into lists and nested maps - and
returns ``422`` naming the offending path. Inherited values cannot carry one,
since PyMongo's BSON encoder rejects them.

If conversion or sanitisation fails, the request returns ``422`` and the stored
document is untouched, so a bad edit never destroys the last working version.
Sources are capped at 256 KiB and rendered output at 512 KiB.

Read shapes
...........

One endpoint serves both, selected by ``include_source``::

    GET .../contents/{content_id}                     -> ContentPublic
    GET .../contents/{content_id}?include_source=true -> ContentAdmin

``ContentPublic`` is the rendered representation and its metadata, never the
author's source, public and cacheable. ``include_source=true`` additionally gives
an authenticated administrator ``source`` and ``source_hash``, and the response is
``private, no-store``; a non-administrator asking for it gets ``403`` rather than
a silently reduced body. Both shapes are a ``oneOf`` on the same operation and are
genuinely disjoint - ``source`` and ``source_hash`` are required in
``ContentAdmin``, and ``ContentPublic`` forbids extra properties.

FastAPI derives an operation's ``security`` from the dependency's *scheme*, not
from whether it may return ``None``, so this read declares both alternatives
explicitly - otherwise a generated client would never attempt the public read::

    security:
      - {}                        # anonymous is acceptable
      - <bearer-scheme>: []       # so is a bearer token

The scheme name is read out of the generated document rather than written here,
since it comes from the installed auth package. Only this operation is rewritten;
the writes on the same path keep the bearer requirement alone.

Sanitisation policy
-------------------

Markdown output, RST output and author HTML all pass through one ``nh3``
allow-list (the maintained successor to the retired ``bleach``), covering tags,
per-tag attributes, URL schemes and link ``rel`` values. ``<script>`` and
``<style>`` have their content removed entirely. The consequence: event handlers,
``javascript:``/``data:``/``file:``/``vbscript:`` URLs, ``<iframe>`` and raw HTML
in Markdown are neutralised, while formatting, tables and links survive, and
links opening in a new tab carry ``rel="noopener noreferrer nofollow"``.

RST is additionally locked down: raw directives, file insertion and includes are
disabled, and input and line length are bounded. A dropped directive produces no
output and no error, so the names of dropped directives go to the API log.

What the RST pipeline preserves, and why:

* **the document title** - ``doctitle_xform`` is off, so a lone top-level title
  stays in the body. Headings start at ``h1``, structure is carried by
  ``<section>``;
* **admonition structure** - docutils wraps every ``.. note::``, ``.. warning::``,
  ``.. admonition::``, ``.. sidebar::``, ``.. topic::`` and the footnote list in
  an ``<aside>`` whose ``class`` names the kind;
* **a leading field list** - docutils returns it in ``parts["docinfo"]`` rather
  than ``parts["body"]``, so it is emitted ahead of the body and sanitised with
  it. A document consisting only of ``:Author:`` and ``:Version:`` lines would
  otherwise render to the empty string;
* **description-list kinds** - ``dl``, ``dt`` and ``dd`` keep ``class``, because
  docutils encodes the kind there and nowhere else (``dl.docinfo`` for document
  metadata, ``dl.field-list`` for one in the body, bare ``dl`` for an ordinary
  definition list). Stripped, all three arrive identical;
* **``cite``** - the ``:title:`` role emits it and nothing else.

``<aside>`` and ``<section>`` are allow-listed with ``class`` and **without**
``id``. A class is inert until a stylesheet the deployment controls gives it
meaning, whereas an author-controlled ``id`` lands in a namespace shared with the
application and invites element clobbering. One consequence is deliberate:
footnote and citation *links* render but do not resolve, because their targets are
the stripped ids.

**A generated table of contents is not served.** ``.. contents::`` makes docutils
emit ``<nav class="contents">`` plus a ``toc-backref`` anchor in every heading,
and the portal builds its own from the headings with IDs it creates itself.
docutils' would be a second, duplicate list whose every entry points at a
fragment ID this allow-list strips. ``nav`` is not allow-listed and the generated
node is removed from the doctree before the HTML is written - removing it at the
sanitiser would not work, because ``nh3`` unwraps an unknown element rather than
deleting its children, leaving the ``<ul>`` of dead links. The removal is reported
through the same log line as a dropped directive.

``ALLOWED_ATTRS`` is not the complete allow-list: ``nh3`` permits ``title`` and
``lang`` on every element regardless of that map, so ``kbd``, ``cite`` and ``li``
still carry those two. Both are inert.

A URL in a settings field may not contain ASCII control characters, and a route
``path`` may not begin ``//``. Browsers strip tab, CR and LF before parsing a
URL's scheme, so ``java\nscript:alert(1)`` would otherwise reach an ``href`` as a
live ``javascript:`` link, and ``//host`` is an off-site load no scheme filter
sees. The attribute filter compares against the string a *browser* would parse:
``str.strip()`` is not that string, since a browser strips the whole WHATWG "C0
control or space" range (U+0000-U+0020) and removes tab, LF and CR from anywhere
in a URL. C1 controls are left alone, because the URL parser does not remove them
either.

Where the XSS boundary is
.........................

Markdown's parser sets ``escape=True``, which escapes raw HTML before it reaches
the sanitiser. That is a **markdown-only** second line of defence and says
nothing about RST.

RST html math is a converter, not a safe-output guarantee: ``\mbox{<img src=x
onerror=…>}`` produces a real ``<img>`` with the handler attached, and
``\href{javascript:…}`` a real link with that href. **nh3 is the boundary that
makes RST math safe** - not defence in depth, but the defence. The handler is
stripped and the element survives; ``javascript:``, ``data:``, ``vbscript:`` and
protocol-relative hrefs are dropped; a surviving link carries the forced ``rel``;
``style``, ``id``, MathML and scripts are allowed nowhere. Class tokens docutils
builds from author text are filtered to plain identifiers, so a hostile
``.. code-block:: py"><script>…`` language leaves nothing that looks like markup
in an attribute.

Highlighting budgets
....................

Pygments lexers are regexes and several are quadratic on adversarial input. A
timeout would not help - highlighting runs in a worker thread and cancelling the
await does not stop the thread - so the only real bound is on what the thread is
handed. Feeding each canonical lexer 1, 4 and 16 KiB of six adversarial shapes,
linear growth would be 16x; ``make``, ``r``, ``console`` and ``ini`` came back at
230-260x and are the ``EXPENSIVE_LEXERS``. So:

* an ordinary block may hand pygments **4 KiB**;
* an expensive-lexer block may hand it **1 KiB**;
* one document may hand it **16 KiB** in total.

Worst case is about 565 ms (16 expensive blocks at their cap); ordinary content is
about 83 ms. Anything over a cap or past the budget renders as escaped plain code
- never truncated, never dropped. The budget lives on the render, not the process.
Because an empty fence with a recognised language costs zero bytes, every
highlighted block is charged at least ``MIN_HIGHLIGHT_CHARGE`` (256 bytes), making
``MAX_HIGHLIGHT_BLOCKS`` (64) a consequence of the byte budget rather than a
second number to keep in step.

RST code blocks are not highlighted at all (``syntax_highlight = "none"``).
Docutils resolves the language after ``.. code-block::`` against pygments' entire
registry and lexes the whole block with no size bound - a second, unreviewed path
that ignores these budgets (``.. code-block:: make`` with 40 KiB of one long line
took roughly a minute in a single render). Restoring it means routing RST blocks
through the same language map and budgets, a custom translator rather than a
setting.

Render failures
...............

When docutils fails on an author's source, the client gets a ``422`` carrying a
fixed sentence - "This reStructuredText content could not be rendered." - and the
real exception goes to the log, because ``AttributeError: 'NoneType' object has no
attribute 'computesize'`` names internals and tells an author nothing they can act
on. Checks this feature makes itself (source too large, unrenderable format) keep
their specific, actionable messages.

Sandbox documents
-----------------

Executable HTML is supported only as ``sandbox-html``, served only through a
dedicated endpoint intended solely as an ``iframe`` source::

    GET /api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}/document

    Content-Security-Policy: sandbox allow-scripts; default-src 'none';
        script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:
    X-Content-Type-Options: nosniff

The ordinary content ``GET`` for such a page returns metadata and a flag only,
never the executable source for insertion into the main DOM. The frontend must
embed it with ``sandbox="allow-scripts"`` and must **not** add
``allow-same-origin``; the sandbox receives no access token or authenticated
application state, so even a hostile script cannot reach the parent origin, its
cookies or its tokens.

.. warning::

   **Any site may frame this document.** The CSP ``sandbox`` directive constrains
   what the document may *do* once framed - no same-origin access, no forms, no
   top-level navigation - not *who* may frame it. Only ``frame-ancestors`` or
   ``X-Frame-Options`` do that, and neither is set.

``X-Frame-Options: SAMEORIGIN`` is deliberately not added, because a supported
portal may legitimately be a different origin and would then be unable to embed
the document at all. The exposure is accepted - the document is public and
unauthenticated, so framing it obtains nothing that fetching the URL would not,
and it carries no user input to redress. A deployment that knows its portal
origins should set ``Content-Security-Policy: frame-ancestors https://portal…``
at the reverse proxy, the only place that knows which origins are legitimate.

Nor is the document inert. ``default-src 'none'`` blocks subresource loads, but a
CSP ``default-src`` is not a navigation prohibition: script inside the frame can
navigate *its own* iframe to an external URL. That is not an escape, and it is not
"no outbound activity" either. A deployment needing that property wants a
``sandbox`` directive without ``allow-top-navigation`` plus explicit frame
restrictions, stated as a requirement rather than assumed from this header.

**Deleting sandbox content is a revocation boundary.** ``/document`` is the one
read in this module that refuses to use a cache: it reads MongoDB on every
request, never populates the document or last-known-good caches, and returns
``503`` during an outage. Everywhere else a slightly stale read is the right trade
for a page body; it is the wrong trade for executable content, where a cached copy
would outlive the delete by the read TTL - or by the whole LKG hour if MongoDB
then became unreachable. The cost is one database read per iframe load and a
``503`` instead of a stale success. This endpoint fails closed. The response stays
``private, no-store``, and an out-of-band document over the 256 KiB write limit is
refused with ``422`` rather than returned in full on every uncached load.

**Class names are part of the rich-content contract.** ``class`` is allow-listed
for main-DOM HTML, so author-controlled class names can match the host
application's CSS and influence layout, even though ``style`` and every ``on*``
handler are stripped. That is the price of letting authors write content that
inherits the site's typography; a deployment that does not want it strips
``class`` from ``ALLOWED_ATTRS`` or narrows it to a namespaced prefix.

UI manifest contract
--------------------

The ui configuration is a versioned manifest; ``schemaVersion`` lets the frontend
refuse a shape it does not understand. Branding can be turned off with
``branding_enabled: false`` without placeholder values.

Routing and navigation are **separate lists**. ``routes`` is the routing table: a
typed union of ``landing`` (at most one per ui), ``content``, ``feature``
(allow-listed names), ``sandbox`` and ``external`` (url, not path), with unique
ids, unique paths and bounded counts. ``navigation`` is the display layer - an
ordered list of tabs, each naming a route by ``route_id`` with its own label and
icon. Every tab must name an existing route and no route gets two tabs; a route
without a tab is simply not in the menu. Landing blocks reference routes the same
way (``target_route_id`` for a ``search`` block, ``route_id`` for a
``feature-link``), never by raw path, so a typo cannot produce a dead link.

Both unions are discriminated and the tag is **required**: every route carries
``kind``, every landing block carries ``block``, and neither has a default, so the
JSON Schema states what the runtime enforces.

On write, the API checks route/content compatibility against the stored content
documents: a ``content`` route - and header, footer and landing-block content -
must reference rendered content (``markdown``, ``rst`` or ``html-fragment``),
while a ``sandbox`` route must reference a ``sandbox-html`` document. Two
cross-field rules go beyond "the referenced id exists":

* a ``search`` block must target a ``feature`` route whose feature is
  ``databrowser``. A search box submits a query, and a content page or external
  link leaves the frontend with nowhere to submit to;
* a ``feature`` route naming a **disabled** feature stays legal, deliberately -
  that is how a rollout is staged. **Clients must not expose a route whose feature
  is disabled**: navigation entries and landing blocks pointing at it should be
  hidden and the route should not resolve. That obligation is on the frontend.

Databrowser defaults live in exactly one place, ``features.databrowser``. A search
block may override ``flavour``/``fixed_facets``; unset means inherit, and
precedence is one rule - block override over feature default.

An announcement's ``starts_at``/``ends_at`` are RFC 3339 timestamps with an
explicit offset, advertised as ``date-time`` and normalised to one spelling, so
``...Z`` is stored as ``...+00:00``. A window whose end precedes its start is
refused, as is a naive timestamp - the window is evaluated by the client in its
own locale, and a naive value would force every client to guess a zone.

The footer supports flat ``links``, titled ``groups`` and a ``legal_links`` row.
How a deployment *selects* among several ui records (host name, path prefix, query
parameter) is not part of this contract yet; until that is decided, the frontend
reads ``ui/default``.

PATCH semantics
---------------

* An omitted field is kept.
* An explicit ``null`` removes the override and restores the default, at any
  depth.
* An empty string, list or map is preserved when valid.
* A **nested object** (``features``, ``header``, ``footer``) merges field by
  field, recursively: patching ``features.stac.enabled`` leaves
  ``features.databrowser`` as it was. An empty object ``{}`` is a no-op and does
  not *create* the object it names.
* In an **open map** (``extra_colors``, ``public_extensions``,
  ``features.databrowser.fixed_facets``) a key set to ``null`` deletes that key,
  ``{}`` clears the map while keeping it present, and ``false`` and ``0`` are
  values, not deletions.
* A **list** is always replaced whole. ``routes``, ``navigation``,
  ``landing_blocks``, ``announcements`` and ``content_refs`` have no key to merge
  an entry by, so send them complete - including the ``fixed_facets`` of a search
  block, which lives inside ``landing_blocks``.

Caching and concurrency
-----------------------

Records and content documents carry a strong ``ETag`` (the hash of the exact
response bytes) and honour ``If-None-Match`` with a ``304``. The public policy is
``public, max-age=30``; a write produces a new ``ETag``.

``max-age=30`` is the ordinary, reachable-origin bound, not an unconditional
ceiling - HTTP permits a cache to reuse a stale response when it cannot reach the
origin, and a deployment needing the stricter reading adds ``must-revalidate``.
There is no ``stale-while-revalidate``: total staleness visible to a client is the
origin TTL **plus** whatever the header allows, since the two windows add rather
than overlap.

Responses that are not representations are ``private, no-store``: a write's reply,
an admin body carrying source, and **every error this feature raises**. A
heuristically cached ``404`` would survive the creation of the content it denied,
and for ``/document`` a cached error would undermine revocation.

.. note::

   "Every error this feature raises" is narrower than "every error a client can
   receive": it covers the ``SettingsError`` subclass, which every ``raise`` in
   this module uses, and not errors FastAPI generates before or around the handler
   - request-validation ``422``\ s, routing ``404``/``405``\ s, authentication
   failures. Those carry no cache directive. Closing that gap needs an
   application-wide handler; a deployment that wants the blanket guarantee sets
   ``Cache-Control: private, no-store`` for 4xx/5xx at the reverse proxy.

A byte-bounded in-process cache (16 MiB per cache, not an entry count - one
document can carry half a megabyte of HTML) holds serialised response bytes, so a
warm read needs no database query. A **missing** content document is cached
briefly too, so a mistyped URL or a bot sweep does not reach MongoDB once per
request, and a confirmed absence is an explicit tombstone: "MongoDB says there is
no override" and "I have never asked" are different states, and only the first
licenses synthesising the default. Without it, a deployment that never customised
its ui would turn an available ``200`` into a ``503`` the moment the body cache
expired during a brief outage. A tombstone resolves to "no override" - the
synthesised default for ``default``, a ``404`` for a named record - so **a missing
named record does not become a default just because the database is down**.

Concurrent misses on the same key are coalesced, keyed by ``(document, epoch)``
so a reader starting after an invalidation does not join an older flight. If the
database is briefly unreachable a last-known-good copy is served to *reads* only:
a write never proceeds from cached data and fails with ``503``. ``/document`` opts
out of all of this.

The cache lives **in the worker process**. ``freva-rest-server --n-workers N``
runs N processes and a write invalidates only the caches of the worker that served
it, so the read TTL is deliberately short - two seconds by default
(``API_SETTINGS_CACHE_TTL``) - because it is also the window in which two workers
can answer the same ``GET`` differently after a write.

.. important::

   **Two seconds is the bound only while MongoDB is reachable.** During an outage
   a worker that never saw the write serves its last-known-good copy, which lives
   for ``API_SETTINGS_LKG_TTL`` - one hour by default. ``/document`` is the
   exception in both cases: no cache at all, failing closed.

Invalidation alone is not enough, because a read is not atomic: it can fetch the
old document, a write can commit and invalidate, and the read can then store what
it fetched. A single global mutation epoch, bumped by every invalidation, closes
this - a read snapshots it before querying, inside the read so no path can omit
it, and its cache write is dropped if the epoch has moved. The response still
returns what was fetched; it simply does not populate a cache it can no longer
prove is current.

Only *reads* populate the caches. A write-side read is a compare-and-swap
candidate whose snapshot is known-stale the moment the predicate fails, so a
successful write caches what it stored and a failed one caches nothing. The
rebuild follows the same rule, invalidating on a CAS skip while keeping
last-known-good.

Writes are a compare-and-swap on a ``revision`` counter *and* an opaque per-write
``cas_token``. The token makes the swap identify a document *generation*: a
revision counter alone is reset by delete-and-recreate, so a stale writer holding
revision 1 of a deleted generation would match revision 1 of the new one. The
comparison uses ``$expr`` with aggregation ``$eq``, ``$literal`` and ``$type``,
because query equality matches a missing field against ``null``, reads a stored
object as an operator expression and descends into arrays, and aggregation ``$eq``
alone still treats the numeric BSON types as mutually comparable.

An optional ``If-Match`` rejects a stale write with ``412``. Concurrent updates to
different fields compose; updates to the same field are last-write-wins. A
``DELETE`` is a compare-and-swap too: the content delete captures the document's
``(revision, cas_token)`` *before* scanning every ui record for references and
requires it for the removal, so a delete cannot destroy a generation that appeared
while it was still deciding - it returns ``409``.

``If-Match: *`` follows :rfc:`9110` and means *"if the resource has a current
representation"*, which is not "a document is stored". ``default`` has a
representation, so ``If-Match: *`` succeeds on its first ``PATCH``; a **named**
record ``404``\ s until written, so there it is a ``412`` rather than a create -
which is what a client asking "update, never create" means by it.

Renderer migrations
-------------------

* a **read** never renders;
* a **metadata-only** edit never renders;
* an edit that changes ``source`` or ``format`` renders immediately;
* the **rebuild** is the explicit migration and repair path for documents whose
  inputs have *not* changed - a renderer upgrade, a drifted digest, HTML written
  out of band::

      POST /api/freva-nextgen/settings/ui/contents/rebuild

A document is stale when its stored ``renderer_version`` no longer matches
``RENDERER_FINGERPRINT``, which is derived rather than hand-maintained::

    RENDERER_FINGERPRINT = "5+docutils=0.23+mistune=3.3.4+nh3=0.3.6+pygments=2.20.0"
                            ^ RENDERER_VERSION, bumped by hand for allow-list
                              and renderer changes

The dependencies are pinned as *ranges*, and an upgrade inside a range can change
the HTML produced for the same source, so the installed versions are read with
``importlib.metadata`` - which does not import the packages, keeping the renderers
lazy - and folded into the identity. The cost is deliberate: **upgrading the
rendering stack marks every rendered document stale and asks for one rebuild**,
which is exactly the set whose output may have moved. ``pygments`` counts, because
an upgrade changes the token spans emitted for the same code.

``RENDERER_VERSION`` is the hand-maintained half and moves whenever the HTML
rendered from the same source moves. An allow-list change is *code*, so it does
not move the derived half on its own; without the bump, HTML from an earlier build
would go on reporting itself current and nothing downstream would notice.

Deploying a new generation
..........................

* **every already-rendered document reports ``is_stale: true``** - the identity
  comparison doing its job, not damage;
* **reads keep working throughout**: a stale document still serves the sanitised
  HTML stored for it, and nothing is withheld;
* **run the paginated rebuild after deploying**, following ``next_after`` until a
  pass comes back without it;
* **repeat until a complete pass finds nothing stale, or quiesce writers.** The
  pagination is not a snapshot, so one complete pass means every document was
  looked at once, not that none is stale at the end. The work is idempotent.

The only frontend change a generation needs is styling: scoped equivalents of
docutils' ``math.css`` for the ``formula``, ``fraction``, ``limits``/``limit`` and
``bigoperator`` classes, and light/dark styles for the short pygments token
classes (``k``, ``n``, ``o``, ``mi`` and the rest). **No parser or highlighter
belongs in the browser** - the portal consumes ``rendered_html`` verbatim:

* a **markdown fence** with a recognised language arrives highlighted, as
  ``<pre><code class="language-python"><span class="n">…</span>…``;
* a fence that is unrecognised, too large or past the document budget arrives as
  escaped plain code, keeping its ``language-…`` class if the language was
  recognised. The portal must render plain code legibly - this is a normal
  outcome, not an error;
* an **RST code block** arrives as ``<pre class="code python literal-block">``
  with escaped plain text and no token spans, always.

Markdown tables can be wider than the content column, so give them horizontal
overflow. Column alignment markers parse but have no effect, because the ``style``
attribute they need is allowed on no tag.

Rebuild and audit passes
------------------------

Rebuild selection is ``needs_rebuild`` - the predicate the read reports - so a
document whose ``rendered_hash`` or ``source_hash`` no longer describes what is
stored beside it is rebuilt at the current identity too. The pass returns
``examined``, ``rebuilt``, ``failed`` and ``skipped``; a document that no longer
renders is counted ``failed`` and left untouched, so a rebuild can never destroy
content, and each rebuild is a compare-and-swap on the revision it read.

Both passes traverse a whole collection inside one HTTP request, so **both** are
bounded and **both** are resumable::

    POST /settings/ui/contents/rebuild            -> {..., "truncated": 1,
                                                     "next_after": "ui:home"}
    POST /settings/ui/contents/rebuild?after=ui:home
    GET  /settings/ui/contents/audit?after=...

A pass examines at most ``API_SETTINGS_MAX_SCAN`` documents, sorted by ``_id``,
and reports ``next_after`` when it stops early. Because a ui record carries up to
a few hundred content references, the audit also stops at
``API_SETTINGS_MAX_SCAN_REFS`` accumulated references - always at a record
boundary, so ``next_after`` names a record processed in full and a continuation
neither skips nor repeats one. ``API_SETTINGS_MAX_RECORD_REFS`` bounds how far
into one record's reference arrays it reads, and a record left partly unread says
so in its own problem message. ``API_SETTINGS_MAX_AUDIT_PROBLEMS`` bounds the
messages one response carries, each at most 500 characters; a capped response adds
``"problems_truncated": true``.

.. important::

   **A completed scan is not a snapshot.** Each page describes the records as they
   were while that page was being processed, so a record the cursor has already
   passed can change behind it and appear in nobody's answer. Following
   ``next_after`` to ``complete`` means every record was *observed once*, not that
   the collection was consistent at any instant, and AND-ing every
   ``page_consistent`` is a best-effort verdict whenever writes are running. For
   an authoritative result, **quiesce the writers** and scan from the beginning.
   This is a property of scanning a live collection without a transaction (see
   `Referential consistency is eventual, by decision`_).

The scan covers documents whose ``_id`` is a *string* - the whole domain this API
writes - because MongoDB range comparisons are type-bracketed and no string cursor
can reach a numeric or ObjectId one. Those are not ignored: the rebuild reports an
exact count under ``malformed_ids``, with a bounded sample in the log. It is a
count rather than another scan because nothing processes them - an ``_id`` is
immutable, so the repair is an operator copying each document to a correct id.

The audit's content lookup is deduplicated and issued in chunks of 200 ids, so one
command's size is a property of that constant rather than of the deployment. A
defect in a stored document is logged once rather than on every read, latched by
the document's *generation* (its CAS token) rather than its id, so a freshly
written malformed generation reports itself whoever wrote it.

A content document's *format class* - rendered versus ``sandbox-html`` - is
immutable once the document exists, and a change is refused with ``409``: a
rendered reference cannot inline a sandbox document, and a sandbox route has no
``/document`` URL for a rendered one. It is immutable **whether or not anything
currently references the document**, since a guard conditioned on existing
references would have to read the references and then write the content, leaving
room for a ui ``PATCH`` to commit one in between. Moving between ``markdown``,
``rst`` and ``html-fragment`` is free, and ``force=true`` is the
deliberate-migration escape.

.. _dangling-references:

Referential consistency is eventual, by decision
................................................

**This is an accepted product property, not an oversight.** Two ordinary requests
can leave a reference pointing at content that no longer exists:

1. a ui ``PATCH`` validates that content X exists;
2. a concurrent ``DELETE`` of X finds no committed reference and removes it;
3. the ui ``PATCH`` commits its reference to X.

Neither used ``force``, neither did anything wrong, and both succeed. The two
documents live in different collections, so closing the window needs a
multi-document transaction, which needs a replica set. This project's reference
deployment (``dev-env/docker-compose.yaml``) runs a standalone ``mongod``, so
requiring one would change the deployment contract for every operator to close a
window measured in milliseconds. That trade was considered and declined. A
process-local lock would not help - the workers are separate processes - and the
audit is a **detector, not a fix**. Outside that window a ``DELETE`` of referenced
content is refused with ``409``.

**The compensating control**::

    GET /api/freva-nextgen/settings/ui/contents/audit

It returns ``{"page_consistent": bool, "problems": [...], "complete": bool}``
naming every reference that no longer matches its content: missing content, a
rendered reference to a document that is now ``sandbox-html``, a sandbox route to
one that is not, or a stored format this build does not recognise.

.. warning::

   ``page_consistent`` describes **the page this call scanned**, not the
   deployment: a first page can answer ``true`` with an empty problem list while a
   later page holds a dangling reference. AND-ing the pages together is a
   deployment-wide verdict **only if the relevant writers were quiesced for the
   whole scan**. There is deliberately no ``consistent`` key - a CI job asserting
   one would go green on a truncated first page, from the very endpoint that
   exists to catch this race.

* **Who:** whoever administers the settings for a deployment - the role holding
  the admin claim. It is an authenticated admin route.
* **When:** from CI on every deployment, after any bulk content change, and after
  any ``force=true`` delete or format migration.
* **Repair:** each problem names a ui record and a content id. Either re-create
  the content document or remove the reference. There is no automatic repair -
  only an operator knows which was intended.

Until it is repaired the read path degrades rather than breaking: a content route
pointing at a sandbox document inlines an empty string, a missing one inlines
nothing, and the client sees ``is_sandbox``/``is_stale`` honestly. A dangling
reference never produces a ``500``.

Request bodies that no response could carry back
------------------------------------------------

Some bodies are syntactically valid JSON and break the *response* rather than the
request: pydantic puts the offending value into a validation error that starlette
then cannot serialise. A dependency scans the raw body before the body model runs
and refuses these with ``422``:

* a number JSON can spell and floats cannot survive - ``1e999`` is ``inf``;
* a string JSON can spell and UTF-8 cannot encode - ``"\ud800"``, an unpaired
  surrogate;
* an integer with more than 19 digits, which no signed 64-bit value has. The bound
  is lexical and this API's own, so the answer does not depend on
  ``sys.set_int_max_str_digits``; whether 19 digits name a *storable* value is a
  range question the model answers.

Every refusal carries a fixed message that **does not echo the offending value** -
quoting it back would reproduce the failure inside the error reporting it. Dict
keys are checked as well as values, and the walk is iterative, so a deeply nested
body cannot turn the guard into a ``RecursionError``. The guard maps the remaining
ways ``json.loads`` can fail - non-UTF-8 bytes, an integer past python's
conversion limit, nesting deep enough to exhaust the stack - to the same fixed
``422``\ s. Bodies that are not JSON at all are left to FastAPI, whose error
describes them better.

.. note::

   FastAPI parses a declared body **before** it solves dependencies, so a body its
   own parser cannot read never reaches the admin check or this guard: it is
   answered by the framework's generic parse error, identical for every route in
   the application. For everything the framework parses successfully, the order on
   a write route is ``401``, then ``403``, then anything about the body, because
   authentication and the administrator check are dependencies.

Errors
------

* ``401`` - a write without authentication.
* ``403`` - a write by a non-administrator. If no admin claim filter is configured
  (``API_ADMIN_TOKEN_CLAIMS`` unset) then no user is an admin and the message says
  so.
* ``404`` - unknown resource, or unknown content.
* ``409`` - deleting content a ui still references, moving referenced content
  across the rendered/sandbox boundary, or a write that lost its compare-and-swap
  too many times.
* ``412`` - a stale ``If-Match``, or ``If-Match: *`` on a record that does not
  exist.
* ``413`` / ``422`` - the resolved document or content is too large, or invalid.
* ``503`` - MongoDB is unreachable.

Administrator claims
--------------------

``API_ADMIN_TOKEN_CLAIMS`` maps a claim path to a list of regular expressions. The
environment variable takes comma-separated ``path:pattern`` pairs, and a bare
entry is shorthand for the ``roles`` path::

    API_ADMIN_TOKEN_CLAIMS=roles:admin
    API_ADMIN_TOKEN_CLAIMS=admin                    # the same thing
    API_ADMIN_TOKEN_CLAIMS=groups:^admins$,admin    # both paths

A configured path the token does not carry simply does not match: the flat
``roles`` claim is consulted **only** for the bare-list form or an explicit
``roles`` path, never as a fallback for some other nominated claim.

Each pattern must match a claim value **in full** (``re.fullmatch``), so ``admin``
does not also match ``non-admin``, ``grafana-admin`` or ``admin-readonly``.
Anchored patterns such as ``^admin$`` behave identically, the anchors being
redundant. Substring matching has to be asked for: ``.*admin.*``.

The settings write, delete, rebuild and audit routes sit behind this check, and so
does every other admin-only route in the service.

Values MongoDB cannot store
---------------------------

``public_extensions`` rejects ``inf`` and ``nan``, integers outside signed 64
bits, and map keys containing control characters, a ``.``, or a leading ``$``,
each with a ``422`` at the model boundary. The integer bound also prevents falling
through to ``float``, so ``2**63`` is an error rather than being stored as
``9.223372036854776e+18``.

JSON cannot spell ``inf`` and ``nan`` - ``json.dumps`` emits the bare tokens
``Infinity`` and ``NaN``, which every browser's ``JSON.parse`` refuses - so a
single ``1e999`` in a settings field would otherwise make the whole response
unreadable for every client. The serialiser carries ``allow_nan=False`` as a
second guard, and a value that reached MongoDB by another route degrades to the
model defaults rather than emitting invalid JSON.

Tuning knobs
------------

Seven environment variables tune this feature:

===================================== ========= =========================================
Variable                              Default   Effect
===================================== ========= =========================================
``API_SETTINGS_CACHE_TTL``            ``2``     seconds a cached read is served
``API_SETTINGS_LKG_TTL``              ``3600``  seconds a last-known-good copy survives
``API_SETTINGS_MAX_SCAN``             ``10000`` documents examined per audit/rebuild pass
``API_SETTINGS_MAX_SCAN_REFS``        ``20000`` references accumulated per audit pass
``API_SETTINGS_MAX_RECORD_REFS``      ``1000``  references read from one record
``API_SETTINGS_MAX_AUDIT_PROBLEMS``   ``1000``  problem messages per audit response
``API_SETTINGS_MONGO_TIMEOUT``        ``5``     seconds **one** mongo operation may take
===================================== ========= =========================================

.. warning::

   ``API_SETTINGS_MONGO_TIMEOUT`` is a **per-operation** deadline, not a request
   deadline: every operation gets a fresh budget, so one request can wait a
   multiple of it. A CAS write performs a read and a write per attempt and retries
   up to three times, so its worst case is over ``3 x 2 x
   API_SETTINGS_MONGO_TIMEOUT`` - **30 seconds at the defaults, not 5**. Nothing
   in the API bounds a whole request; if a deployment needs one, it belongs in the
   reverse proxy.

They are read at **import time** and are **startup-only**: changing one requires a
restart, and ``ServerConfig.reload()`` does not pick it up. Moving them into
``ServerConfig`` would not make them reloadable on its own - the TTLs become the
``ttl`` of a ``TTLCache`` instance, which cannot be changed after construction -
so making them reloadable means rebuilding the caches on reload. A malformed or
non-positive value falls back to the default rather than failing to boot.

Accepted limitations
--------------------

Each of these is a decision, not an omission.

**The sandbox /document route is public, uncached and unauthenticated by
design, and should be rate-limited at the reverse proxy.** Revocation requires it
to read MongoDB on every request, which makes it the most expensive public route
in the feature. Caching would break revocation and authentication would break the
iframe, so apply a request-rate and concurrency limit to
``/api/freva-nextgen/settings/ui/*/contents/*/document`` at the proxy, alongside
the ``frame-ancestors`` header.

**Cross-worker cache invalidation is bounded by a TTL, not solved.** Two workers
can answer the same ``GET`` differently for up to ``API_SETTINGS_CACHE_TTL`` while
MongoDB is reachable, and up to ``API_SETTINGS_LKG_TTL`` during an outage. Solving
it properly needs a shared invalidation channel.

**Referential consistency between the two collections is eventual.** See
`Referential consistency is eventual, by decision`_.

**A search block's fixed_facets cannot be patched key by key**, because
``landing_blocks`` is a list with no key to merge an entry by; send the whole
list. ``features.databrowser.fixed_facets`` - the inherited default, which is what
most deployments should edit - is a proper open map and does merge key by key.

**Deleting a ui record does not touch the content it owns.** ``DELETE
/settings/ui/waterpark`` leaves ``ui/waterpark/contents/*`` in place, because a
delete that silently removed page bodies would be unrecoverable. The cost is
orphaned documents, and **the audit will not find them**: it starts from ui
references and checks their targets, so content nothing references is exactly what
it cannot see. Finding orphans is a manual sweep - list ``ui_contents`` and
compare the ``ui_id`` prefixes against the ui records that exist.

**stable_sanitize gives up after four passes.** Four is an implementation
bound, not a proof; the known pathological input settles in three. Input that does
not converge is refused with ``422`` rather than stored. Raising the bound is
fine; removing it is not, because the CPU and output bounds depend on it
terminating.

**Outbound sanitisation makes markup safe, not authentic.** Someone with write
access to MongoDB can substitute the ``source`` of any document and the next
rebuild will render their text as the real content. No digest prevents this and
none is claimed to: what the sanitiser guarantees is that whatever reaches a
browser has been through the allow-list.

**The share-key TTL index is created without a fallback.** Startup calls
``create_index`` on ``zarr_shared_keys`` and does not catch a failure, so a worker
that cannot build it does not start. That predates this feature and is left alone
deliberately: the index is what expires shared keys, so starting without it would
silently change an existing retention contract.

