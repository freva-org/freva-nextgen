"""The settings and content routes; content documents are owned by a ui record.
The `default` record is synthesised from the model defaults when none is stored."""

import json
import re
from itertools import islice
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from fastapi import Depends, Header, Path, Query, Request, Response
from py_oidc_auth import IDToken as TokenPayload
from pymongo.errors import PyMongoError

from freva_rest.auth import auth
from freva_rest.logger import logger
from freva_rest.rest import app, server_config

from .core import (
    MAX_AUDIT_PROBLEMS,
    MAX_PROBLEM_LENGTH,
    MAX_RECORD_REFERENCES,
    MAX_SCAN_DOCUMENTS,
    MAX_SCAN_REFERENCES,
    ContentStore,
    SettingsError,
    SettingsStore,
    bounded_cursor,
    check_if_match,
    format_mismatch,
    if_none_match,
    rebuild_stale_content,
)
from .registry import REGISTRY
from .schema import (
    DEFAULT_RECORD_ID,
    RECORD_ID_PATTERN,
    UI_ID_PATTERN,
    ContentAdmin,
    ContentPublic,
    ContentSource,
    UiConfigUpdate,
)

# No stale-while-revalidate
PUBLIC_CACHE = "public, max-age=30"
# IMPORTANT: Anything carrying the source, or resulting from a write,
# must not be stored by a shared cache.
PRIVATE_CACHE = "private, no-store"

# The declared body for the generic settings PATCH
if TYPE_CHECKING:  # pragma: no cover
    SettingsUpdateBody = UiConfigUpdate
else:
    SettingsUpdateBody = Union[
        tuple(dict.fromkeys(entry.update_model for entry in REGISTRY.values()))
    ]

# Compiled from the schema's patterns rather than a second copy, so they cannot
# drift.
_UI_ID_RE = re.compile(UI_ID_PATTERN)
_RECORD_ID_RE = re.compile(RECORD_ID_PATTERN)


# helpers


def _require_admin(current_user: TokenPayload) -> None:
    if not server_config.is_admin_user(current_user):
        if not server_config.admin_token_claims:
            raise SettingsError(
                status_code=403,
                detail=(
                    "No admin claim filter is configured, so no user can be an "
                    "admin and the settings cannot be changed. Set "
                    "API_ADMIN_TOKEN_CLAIMS, for example "
                    "'API_ADMIN_TOKEN_CLAIMS=roles:admin'."
                ),
            )
        raise SettingsError(
            status_code=403,
            detail="Only administrators may perform this operation.",
        )


def require_admin(current_user: TokenPayload = auth.required()) -> TokenPayload:
    """Authenticate, then require an administrator, as a dependency.
    Dependencies solve before body validation, so the order is 401, 403, then body."""
    _require_admin(current_user)
    return current_user


def _settings_store(resource: str, record_id: str) -> SettingsStore:
    entry = REGISTRY.get(resource)
    if entry is None:
        raise SettingsError(
            status_code=404,
            detail=(
                f"No settings resource '{resource}'. "
                f"Valid resources: {', '.join(sorted(REGISTRY))}."
            ),
        )
    if not _RECORD_ID_RE.fullmatch(record_id):
        raise SettingsError(status_code=422, detail="Invalid record id.")
    return SettingsStore(
        server_config,
        resource,
        record_id,
        entry.model,
        entry.update_model,
        entry.open_maps,
    )


def _content_store(ui_id: str, content_id: str) -> ContentStore:
    # fullmatch for the same reason as _settings_store above.
    if not _UI_ID_RE.fullmatch(ui_id):
        raise SettingsError(status_code=422, detail="Invalid ui id.")
    if not _RECORD_ID_RE.fullmatch(content_id):
        raise SettingsError(status_code=422, detail="Invalid content id.")
    return ContentStore(server_config, ui_id, content_id)


def _conditional(body: bytes, etag: str, request: Request, cache: str) -> Response:
    """Return the body, or a 304 if the caller's ETag still matches; the 304
    carries the same cache directive as the 200."""
    headers = {"ETag": etag, "Cache-Control": cache}
    if if_none_match(request.headers.get("if-none-match"), etag):
        return Response(status_code=304, headers=headers)
    return Response(content=body, media_type="application/json", headers=headers)


def _written(body: bytes, etag: str) -> Response:
    """The response to a successful write: never cached by a shared cache."""
    return Response(
        content=body,
        media_type="application/json",
        headers={"ETag": etag, "Cache-Control": PRIVATE_CACHE},
    )


def _not_a_number(_token: str) -> float:
    raise SettingsError(
        status_code=422,
        detail=(
            "The request body contains a number that is not finite "
            "(inf or NaN). Json cannot represent one, so it is refused."
        ),
    )


def _bounded_int(token: str) -> int:
    digits = token[1:] if token.startswith("-") else token
    if len(digits) > MAX_INT_DIGITS:  # pragma: no cover
        raise SettingsError(status_code=422, detail=OVERSIZED_INT_DETAIL)
    return int(token)


def _finite(token: str) -> float:
    value = float(token)
    # -inf, +inf and nan are exactly the floats not equal to themselves or not
    # bounded; cheaper than importing math.isfinite and reads the same.
    if value != value or value in (float("inf"), float("-inf")):
        return _not_a_number(token)
    return value


MAX_INT_DIGITS = 19
"""Decimal digits in the widest integer mongo stores, ignoring the sign. A longer
token is refused before `int()` sees it and raises its own `ValueError`."""

OVERSIZED_INT_DETAIL = (
    "The request body contains an integer with more digits than a 64-bit value "
    "has. MongoDB stores at most a signed 64-bit integer, so it is refused."
)

UNDECODABLE_DETAIL = (
    "The request body is not valid UTF-8, so it cannot be read as json."
)

TOO_DEEP_DETAIL = "The request body is nested too deeply to parse, so it is refused."

UNENCODABLE_DETAIL = (
    "The request body contains text that cannot be encoded as UTF-8 - an "
    "unpaired surrogate such as \\ud800. Json accepts it, but no response "
    "could carry it back, so it is refused."
)
"""What a client is told; the offending string is not echoed, since quoting it
back would reproduce the failure inside the error that reports it."""


def _require_encodable(document: Any) -> None:
    """Refuse any string in the body that UTF-8 cannot represent, such as the
    lone surrogate `"\\ud800"`. Keys are checked as well as values."""
    stack = [document]
    while stack:
        node = stack.pop()
        if isinstance(node, str):
            _reject_if_unencodable(node)
        elif isinstance(node, dict):
            for key, value in node.items():
                if isinstance(key, str):
                    _reject_if_unencodable(key)
                stack.append(value)
        elif isinstance(node, list):
            stack.extend(node)


def _reject_if_unencodable(text: str) -> None:
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as error:
        raise SettingsError(status_code=422, detail=UNENCODABLE_DETAIL) from error


async def reject_unserialisable_body(request: Request) -> None:
    """Refuse a body no response could ever carry back, with a 422 not a 500.
    Declared after the auth dependencies, so a hostile body is still 401 or 403."""
    raw = await request.body()
    if not raw:
        return
    try:
        document = json.loads(
            raw,
            parse_float=_finite,
            parse_int=_bounded_int,
            parse_constant=_not_a_number,
        )
    except json.JSONDecodeError:
        # Not json at all
        return
    except UnicodeDecodeError as error:
        raise SettingsError(status_code=422, detail=UNDECODABLE_DETAIL) from error
    except RecursionError as error:
        raise SettingsError(status_code=422, detail=TOO_DEEP_DETAIL) from error
    except ValueError as error:
        # what is left of ValueError once json and unicode errors are taken
        raise SettingsError(status_code=422, detail=OVERSIZED_INT_DETAIL) from error
    _require_encodable(document)


# schema introspection


@app.get(
    "/api/freva-nextgen/settings/{resource}/_schema",
    tags=["Settings"],
    summary="The json schema of a settings resource",
)
async def get_settings_schema(
    request: Request,
    resource: str = Path(examples=["ui"]),
    variant: str = Query(
        default="read",
        pattern="^(read|update)$",
        description=(
            "`read` (default) is the resolved manifest a GET returns, including "
            "fields the api owns. `update` is the PATCH body's schema and is "
            "what an editing form should be generated from: it contains exactly "
            "the fields a client may send."
        ),
    ),
) -> Response:
    """Return the json schema of a resource's model, with the `x-widget` and
    `x-format` form hints. Public and cacheable; `variant=update` suits forms."""
    entry = REGISTRY.get(resource)
    if entry is None:
        raise SettingsError(
            status_code=404,
            detail=(
                f"No settings resource '{resource}'. "
                f"Valid resources: {', '.join(sorted(REGISTRY))}."
            ),
        )
    import hashlib
    import json

    model = entry.update_model if variant == "update" else entry.model
    schema = model.model_json_schema()
    body = json.dumps(
        schema, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    etag = f'"{hashlib.sha256(body).hexdigest()[:32]}"'
    return _conditional(body, etag, request, PUBLIC_CACHE)


# settings record routes


@app.get(
    "/api/freva-nextgen/settings/{resource}/{record_id}",
    tags=["Settings"],
    summary="Read a settings record",
    responses={
        200: {
            "description": (
                "The resolved record, shaped by the resource's registered "
                "model (see GET /settings/{resource}/_schema). For 'ui' this "
                "is the UiConfig manifest."
            )
        }
    },
)
async def get_settings_record(
    request: Request,
    resource: str = Path(examples=["ui"]),
    record_id: str = Path(examples=["default", "waterpark"]),
) -> Response:
    # A named record that was never written is a 404; only the default record
    # synthesises from the model defaults.
    allow_default = record_id == DEFAULT_RECORD_ID
    body, etag = await _settings_store(resource, record_id).get(
        allow_default=allow_default
    )
    return _conditional(body, etag, request, PUBLIC_CACHE)


@app.patch(
    "/api/freva-nextgen/settings/{resource}/{record_id}",
    tags=["Settings"],
    summary="Change a settings record",
)
async def patch_settings_record(
    payload: SettingsUpdateBody,
    resource: str = Path(examples=["ui"]),
    record_id: str = Path(examples=["default"]),
    current_user: TokenPayload = Depends(require_admin),
    _wire_safe_body: None = Depends(reject_unserialisable_body),
    if_match: Optional[str] = Header(default=None, alias="If-Match"),
) -> Response:
    """Apply a partial update to a settings record. The body is re-validated
    against the update model registered for the requested `resource`."""
    store = _settings_store(resource, record_id)
    entry = REGISTRY[resource]
    try:
        update = entry.update_model.model_validate(
            payload.model_dump(exclude_unset=True)
        )
    except ValueError as error:
        raise SettingsError(status_code=422, detail=str(error)) from error
    body, etag = await store.patch(
        update, if_match=if_match, validate=_make_ref_validator(resource)
    )
    return _written(body, etag)


@app.delete(
    "/api/freva-nextgen/settings/{resource}/{record_id}",
    tags=["Settings"],
    summary="Delete a settings record",
    status_code=204,
)
async def delete_settings_record(
    resource: str = Path(examples=["ui"]),
    record_id: str = Path(examples=["waterpark"]),
    if_match: Optional[str] = Header(
        default=None,
        alias="If-Match",
        description=(
            "Delete only if the record still matches this strong ETag, as "
            "returned by a GET. `*` matches any existing representation. "
            "Without the header the delete is unconditional from the client's "
            "point of view, though it is still a compare-and-swap internally."
        ),
    ),
    current_user: TokenPayload = Depends(require_admin),
) -> Response:
    """Delete a settings record; for `default` this resets to the built-in
    defaults and returns 204. A named record that does not exist is a 404."""
    store = _settings_store(resource, record_id)
    snapshot = await store.cas_snapshot()
    resets_to_default = store.synthesises_default
    if snapshot is None and not resets_to_default:
        # RFC 9110 13.1.1
        raise SettingsError(status_code=404, detail="No such settings record.")
    doc, expected = snapshot if snapshot is not None else ({}, None)
    if if_match is not None:  # do not build a representation nobody asked for
        check_if_match(if_match, store.etags_for(doc), "settings record")
    if expected is None:
        # Nothing stored, so the reset has already happened; recording the
        # confirmed absence leaves the same state a successful delete does.
        store.note_absent()
        return Response(status_code=204)
    outcome = await store.delete(expected=expected, seed_absent=resets_to_default)
    if outcome == "missing" and not resets_to_default:
        raise SettingsError(status_code=404, detail="No such settings record.")
    if outcome == "changed":
        raise SettingsError(
            status_code=409,
            detail=(
                "The record changed since this delete was prepared, so it was "
                "not removed - the delete would have destroyed a newer version "
                "than the one it was asked about. Re-read and retry."
            ),
        )
    # The absence is recorded inside store.delete (seed_absent), in the same step
    # that invalidates, so there is no window here to guard.
    return Response(status_code=204)


# content routes


_CONTENT_GET_RESPONSES: Dict[str, Any] = {
    "200": {
        "description": (
            "The content document. Which of the two shapes you get is decided by "
            "the `include_source` query parameter:\n\n"
            "* **default (`include_source` omitted or false)** - `ContentPublic`: "
            "the rendered representation and its metadata. Public, cacheable, and "
            "it never carries the author's source.\n"
            "* **`include_source=true`** - `ContentAdmin`: every `ContentPublic` "
            "field plus `source` and `source_hash`. This is the editor view; it "
            "requires an authenticated administrator and is returned "
            "`Cache-Control: private, no-store` so no shared cache keeps it.\n\n"
            "For `sandbox-html` content `rendered_html` is always empty and "
            "`is_sandbox` is true - fetch the document from its `/document` url "
            "instead of inlining it."
        ),
        "content": {
            "application/json": {
                "schema": {
                    "oneOf": [
                        ContentPublic.model_json_schema(),
                        ContentAdmin.model_json_schema(),
                    ],
                }
            }
        },
    },
    "304": {"description": "The caller's ETag still matches."},
    "403": {
        "description": ("`include_source=true` without an authenticated administrator.")
    },
    "404": {"description": "No such content."},
}
"""Documented by hand because one operation has two response shapes selected by a
query parameter, which `response_model` cannot express."""


@app.get(
    "/api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}",
    tags=["Settings"],
    summary="Read a content document (rendered, or with source for an admin)",
    openapi_extra={"responses": _CONTENT_GET_RESPONSES},
)
async def get_content(
    request: Request,
    ui_id: str = Path(examples=["default", "_shared"]),
    content_id: str = Path(examples=["home"]),
    include_source: bool = Query(
        default=False,
        description=(
            "Administrator only. When true the response is `ContentAdmin` - the "
            "usual rendered payload **plus** the raw `source` and its "
            "`source_hash` - and is marked `private, no-store`. Without it the "
            "response is the public `ContentPublic` shape and the source is "
            "never included. A non-administrator who asks for it gets 403, not "
            "a silently reduced body."
        ),
    ),
    current_user: Optional[TokenPayload] = auth.optional(),
) -> Response:
    store = _content_store(ui_id, content_id)
    if include_source:
        # the editor view: source is only for an authenticated administrator, and
        # the response must never be stored by a shared cache.
        if current_user is None:
            raise SettingsError(
                status_code=403,
                detail="include_source=true requires an administrator.",
            )
        _require_admin(current_user)
        body, etag = await store.get_admin()
        return _conditional(body, etag, request, PRIVATE_CACHE)
    body, etag = await store.get_public()
    return _conditional(body, etag, request, PUBLIC_CACHE)


OPTIONAL_AUTH_OPERATIONS = (
    ("/api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}", "get"),
)
"""Operations that are public, with authentication used only for a variant. FastAPI
would otherwise advertise their token as mandatory; see the fixup below."""


def _declare_optional_auth(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Rewrite those operations as anonymous-**or**-bearer, which OpenAPI spells
    `[{}, {<scheme>: []}]`. A no-op when no requirement was generated."""
    for path, method in OPTIONAL_AUTH_OPERATIONS:
        operation = schema.get("paths", {}).get(path, {}).get(method)
        if not operation:  # pragma: no cover
            continue
        generated = operation.get("security")
        if not generated or {} in generated:
            continue
        operation["security"] = [{}, *generated]
    return schema


if not getattr(app, "_settings_optional_auth_patched", False):
    _generated_openapi = app.openapi

    def _openapi_with_optional_auth() -> Dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema
        app.openapi_schema = _declare_optional_auth(_generated_openapi())
        return app.openapi_schema

    app.openapi = _openapi_with_optional_auth  # type: ignore
    app.openapi_schema = None
    app._settings_optional_auth_patched = True  # type: ignore


@app.patch(
    "/api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}",
    tags=["Settings"],
    summary="Create or change a content document",
)
async def patch_content(
    update: ContentSource,
    ui_id: str = Path(examples=["default"]),
    content_id: str = Path(examples=["home"]),
    force: bool = Query(
        default=False,
        description=(
            "Allow a change between a rendered format and sandbox-html on an "
            "existing content document. The format class is immutable without "
            "it, and such a change is refused with a 409."
        ),
    ),
    current_user: TokenPayload = Depends(require_admin),
    _wire_safe_body: None = Depends(reject_unserialisable_body),
    if_match: Optional[str] = Header(default=None, alias="If-Match"),
) -> Response:
    store = _content_store(ui_id, content_id)
    body, etag = await store.patch(update, if_match=if_match, force=force)
    return _written(body, etag)


@app.delete(
    "/api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}",
    tags=["Settings"],
    summary="Delete a content document",
    status_code=204,
)
async def delete_content(
    ui_id: str = Path(examples=["default"]),
    content_id: str = Path(examples=["home"]),
    force: bool = Query(
        default=False,
        description=(
            "Delete even if a ui configuration still references this content. "
            "Without it a referenced content is protected with a 409."
        ),
    ),
    if_match: Optional[str] = Header(
        default=None,
        alias="If-Match",
        description=(
            "Delete only if the document still matches this strong ETag. Either "
            "tag a GET can hand out is accepted - the public one and the "
            "`include_source=true` admin one - so an editor can use the tag it "
            "was actually given. `*` matches any existing document."
        ),
    ),
    current_user: TokenPayload = Depends(require_admin),
) -> Response:
    store = _content_store(ui_id, content_id)
    # Capture the document's identity before the reference scan, so a concurrent
    # write cannot make this delete remove a generation nobody checked.
    snapshot = await store.cas_snapshot()
    if snapshot is None:
        raise SettingsError(status_code=404, detail="No such content.")
    doc, expected = snapshot
    # RFC 9110 13.1.1
    if not force:
        referrers = await _referring_uis(ui_id, content_id)
        if referrers:
            raise SettingsError(
                status_code=409,
                detail=(
                    "This content is referenced by the ui configuration(s): "
                    f"{', '.join(referrers)}. Remove the reference first, or "
                    "pass force=true to delete anyway."
                ),
            )
    if if_match is not None:
        # Only when asked
        check_if_match(if_match, await store.read_etags(doc), "content document")
    outcome = await store.delete(expected=expected)
    if outcome == "missing":
        raise SettingsError(status_code=404, detail="No such content.")
    if outcome == "changed":
        raise SettingsError(
            status_code=409,
            detail=(
                "The content changed while this delete was being checked, so it "
                "was not removed - the delete would have destroyed a newer "
                "version whose references were never verified. Re-read and "
                "retry."
            ),
        )
    return Response(status_code=204)


_UI_RECORD_QUERY: Dict[str, Any] = {"_id": {"$regex": "^ui:", "$type": "string"}}
"""Which settings documents are ui records; selection is on `_id`, not the
redundant `resource_name` metadata an out-of-band document may have lost."""


CONTENT_LOOKUP_CHUNK = 200
"""How many content ids one `$in` lookup may carry, so the largest command is
a property of this constant rather than of the size of the deployment."""


def _record_label(doc: Dict[str, Any]) -> str:
    """A ui record's name for a message, taken from `_id` like the scans, not
    from the redundant `record_id` metadata."""
    doc_id = doc.get("_id")
    if isinstance(doc_id, str) and doc_id.startswith("ui:"):
        return f"ui/{doc_id[3:]}"
    record = doc.get("record_id")
    return f"ui/{record if isinstance(record, str) else '?'}"


async def _content_formats(keys: List[str]) -> Dict[str, Any]:
    """The stored format of each `"{ui_id}:{content_id}"`, deduplicated and
    fetched in bounded chunks."""
    unique = sorted(set(keys))
    formats: Dict[str, Any] = {}
    for start in range(0, len(unique), CONTENT_LOOKUP_CHUNK):
        end = start + CONTENT_LOOKUP_CHUNK
        batch = unique[start:end]
        cursor = server_config.mongo_collection_ui_contents.find(
            {"_id": {"$in": batch}}, {"format": 1}
        )
        async for found in bounded_cursor(cursor):
            formats[str(found["_id"])] = found.get("format")
    return formats


_NOTHING = object()


def _as_sequence(value: Any, limit: Optional[int] = None) -> Tuple[List[Any], bool]:
    """A stored field as a list, empty for anything else, plus whether `limit`
    cut it short; an out-of-band document can hold an int where a list belongs."""
    if not isinstance(value, (list, tuple)):
        return [], False
    if limit is None:
        return list(value), False
    remaining = iter(value)
    items = list(islice(remaining, limit))
    return items, next(remaining, _NOTHING) is not _NOTHING


def _collect_refs(
    data: Dict[str, Any], limit: Optional[int] = None
) -> Tuple[List[Tuple[str, str, Optional[str]]], bool]:
    """Every content reference a ui document makes, as `(ui_id, content_id,
    expected_kind)`; `limit` caps the read and the flag says if it was cut."""
    wanted: List[Tuple[str, str, Optional[str]]] = []
    cut = False

    def _add(ref: object, expect: Optional[str]) -> None:
        nonlocal cut
        if not (isinstance(ref, dict) and "ui_id" in ref and "content_id" in ref):
            return
        if limit is not None and len(wanted) >= limit:  # pragma: no cover
            cut = True
            return
        wanted.append((str(ref["ui_id"]), str(ref["content_id"]), expect))

    def _sequence(key: str) -> List[Any]:
        nonlocal cut
        items, truncated = _as_sequence(data.get(key), limit)
        cut = cut or truncated
        return items

    for key in ("header", "footer"):
        section = data.get(key)
        if isinstance(section, dict):
            _add(section.get("content"), "rendered")
    for ref in _sequence("content_refs"):
        _add(ref, None)
    for block in _sequence("landing_blocks"):
        if isinstance(block, dict) and block.get("block") == "content":
            _add(block.get("ref"), "rendered")
    for route in _sequence("routes"):
        if isinstance(route, dict):
            if route.get("kind") == "content":
                _add(route, "rendered")
            elif route.get("kind") == "sandbox":
                _add(route, "sandbox")
    return wanted, cut


def _make_ref_validator(
    resource: str,
) -> "Optional[Callable[[object], Awaitable[None]]]":
    """Build the async validator the settings CAS loop runs on the resolved
    candidate: referenced content must exist and suit how the reference uses it."""
    if resource != "ui":
        return None

    async def _validate(model: object) -> None:
        data = model.model_dump()  # type: ignore[attr-defined]
        wanted, _ = _collect_refs(data)

        missing: List[str] = []
        incompatible: List[str] = []
        # One query for every reference, not one per reference: this runs inside
        # the write's CAS loop, where per-reference round trips would multiply.
        formats: Dict[str, Any] = {}
        if wanted:
            # The whole read is wrapped, not just the find(): the I/O happens
            # lazily during iteration. An unverified reference set is not written.
            try:
                formats = await _content_formats([f"{u}:{c}" for u, c, _ in wanted])
            except PyMongoError as error:
                logger.error("Could not verify content references: %s", error)
                raise SettingsError(
                    status_code=503,
                    detail=(
                        "Could not verify the referenced content, so the write "
                        "is refused. Try again once the database is reachable."
                    ),
                ) from error
        # sorted(set(...)): identical references give identical verdicts. The key
        # is explicit because the Optional third element cannot sort against str.
        for ui_id, content_id, expect in sorted(
            set(wanted), key=lambda item: (item[0], item[1], item[2] or "")
        ):
            key = f"{ui_id}:{content_id}"
            if key not in formats:
                missing.append(f"{ui_id}/{content_id}")
                continue
            fmt = formats[key]
            # "not sandbox" is not "renderable": inequality would let a document
            # with format 'bogus', or none at all, satisfy a rendered reference.
            for problem in format_mismatch(ui_id, content_id, fmt, expect):
                incompatible.append(problem)
        problems = []
        if missing:
            problems.append(
                "references content that does not exist: " + ", ".join(missing)
            )
        if incompatible:
            problems.append("has incompatible references: " + "; ".join(incompatible))
        if problems:
            raise SettingsError(
                status_code=422,
                detail=(
                    "This configuration " + "; and ".join(problems) + ". "
                    "References are explicit - create the content, correct the "
                    "reference, or use the matching route type."
                ),
            )

    return _validate


async def _referring_uis(ui_id: str, content_id: str) -> List[str]:
    """Which ui records reference this content, protecting it from a careless
    delete; the query is an indexed scan of the small settings collection."""
    try:
        cursor = server_config.mongo_collection_settings.find(_UI_RECORD_QUERY)
        referrers = []
        async for doc in bounded_cursor(cursor):
            for ref_ui, ref_content, _ in _collect_refs(doc)[0]:
                if ref_ui == ui_id and ref_content == content_id:
                    referrers.append(_record_label(doc))
                    break
        return referrers
    except PyMongoError as error:
        # fail closed: if we cannot prove the content is unreferenced, refuse the
        # delete. An empty list here would let a mongo outage bypass protection.
        logger.error("Could not check content references: %s", error)
        raise SettingsError(
            status_code=503,
            detail=(
                "Could not verify whether this content is referenced, so the "
                "delete is refused. Try again once the database is reachable."
            ),
        ) from error


async def audit_references(
    after: Optional[str] = None,
) -> Tuple[List[str], Optional[str], bool]:
    """Every ui reference that no longer matches its content, plus a resume
    `_id` and a capped-problems flag; the two collections share no transaction."""
    problems: List[str] = []
    wanted: List[Tuple[str, str, Optional[str], str]] = []
    last_id: Optional[str] = None
    truncated = False
    problems_truncated = False

    def _note(message: str) -> None:
        nonlocal problems_truncated
        if len(problems) >= MAX_AUDIT_PROBLEMS:  # pragma: no cover
            problems_truncated = True
            return
        problems.append(message[:MAX_PROBLEM_LENGTH])

    # both constraints are on _id, so they merge into one operator document
    # rather than needing an $and
    selector: Dict[str, Any] = dict(_UI_RECORD_QUERY["_id"])
    if after is not None:  # "" is a legal _id; truthiness would restart the scan
        selector["$gt"] = after
    selector = {"_id": selector}
    # cursor iteration is where the I/O happens; the caller turns PyMongoError
    # into a 503.
    cursor = server_config.mongo_collection_settings.find(selector).sort("_id", 1)
    examined = 0
    references = 0
    async for doc in bounded_cursor(cursor):
        if examined >= MAX_SCAN_DOCUMENTS or (
            examined > 0 and references >= MAX_SCAN_REFERENCES
        ):
            truncated = True
            logger.warning(
                "Audit stopped after %d ui records and %d references; "
                "continue with after=%r.",
                examined,
                references,
                last_id,
            )
            break
        examined += 1
        record = _record_label(doc)
        found, cut = _collect_refs(doc, limit=MAX_RECORD_REFERENCES)
        if cut:
            _note(
                f"{record} was read only as far as {MAX_RECORD_REFERENCES} "
                "references; the rest of it was not checked"
            )
        references += len(found)
        for ref_ui, ref_content, expect in found:
            wanted.append((ref_ui, ref_content, expect, record))
        # $type: "string" in the selector, so this is always a usable cursor
        last_id = str(doc.get("_id"))
    next_after = last_id if truncated else None
    if not wanted or problems_truncated:
        return problems, next_after, problems_truncated
    formats = await _content_formats([f"{u}:{c}" for u, c, _, _ in wanted])
    for ref_ui, ref_content, expect, record in wanted:
        if len(problems) >= MAX_AUDIT_PROBLEMS:
            problems_truncated = True
            break
        key = f"{ref_ui}:{ref_content}"
        if key not in formats:
            _note(f"{record} references missing content {ref_ui}/{ref_content}")
            continue
        # same membership-based classification as the write validator, so the
        # audit cannot report a malformed document as consistent
        for problem in format_mismatch(ref_ui, ref_content, formats[key], expect):
            _note(f"{record}: {problem}")
    return problems, next_after, problems_truncated


@app.get(
    "/api/freva-nextgen/settings/ui/contents/audit",
    tags=["Settings"],
    summary="Find ui references that no longer match their content",
)
async def audit_content_references(
    after: Optional[str] = Query(
        default=None,
        description=(
            "Continue a scan that stopped early: pass the `next_after` value "
            "from the previous response. Omit it to start from the beginning."
        ),
    ),
    current_user: TokenPayload = Depends(require_admin),
) -> Dict[str, Any]:
    """The result describes one page, not the deployment: follow `next_after`
    until `complete` is true and AND the pages together for a full verdict."""
    try:
        problems, next_after, problems_truncated = await audit_references(after=after)
    except PyMongoError as error:
        logger.error("Could not audit content references: %s", error)
        raise SettingsError(
            status_code=503,
            detail="Could not reach the configuration database.",
        ) from error
    # `page_consistent`, never `consistent`: a bounded scan speaks only for the
    # page it read, and no `consistent` alias is emitted to be misread as global.
    report: Dict[str, Any] = {
        "page_consistent": not problems,
        "problems": problems,
        "complete": next_after is None,
    }
    if next_after is not None:
        report["next_after"] = next_after
    if problems_truncated:
        report["problems_truncated"] = True
    return report


# the renderer migration path


@app.post(
    "/api/freva-nextgen/settings/ui/contents/rebuild",
    tags=["Settings"],
    summary="Re-render content left stale by a renderer change",
    responses={
        200: {
            "description": (
                "A report: how many documents were examined, rebuilt, failed to "
                "render, and were skipped because a concurrent write moved them "
                "on."
            )
        }
    },
)
async def rebuild_content(
    after: Optional[str] = Query(
        default=None,
        description=(
            "Continue a pass that stopped early: pass the `next_after` value "
            f"from the previous response. A pass examines at most "
            f"{MAX_SCAN_DOCUMENTS} documents."
        ),
    ),
    current_user: TokenPayload = Depends(require_admin),
) -> Dict[str, Any]:
    """Re-render the content documents this pass observes as stale; a pass is
    bounded and resumable via `next_after`, and a failed render is left alone."""
    try:
        return await rebuild_stale_content(server_config, after=after)
    except PyMongoError as error:
        logger.error("Could not rebuild stale content: %s", error)
        raise SettingsError(
            status_code=503,
            detail="Could not reach the configuration database.",
        ) from error


# the sandbox document boundary

# The sandbox document runs its own inline script, so the CSP grants the sandbox
# with scripts and an inline script-src, not a bare `default-src 'none'`.
SANDBOX_CSP = (
    "sandbox allow-scripts; "
    "default-src 'none'; "
    "script-src 'unsafe-inline'; "
    "style-src 'unsafe-inline'; "
    "img-src data:"
)


@app.get(
    "/api/freva-nextgen/settings/ui/{ui_id}/contents/{content_id}/document",
    tags=["Settings"],
    summary="The sandboxed html document (iframe source only)",
    responses={
        200: {"content": {"text/html": {}}},
        404: {"description": "No such content, or it is not sandbox-html."},
    },
)
async def get_sandbox_document(
    ui_id: str = Path(examples=["default"]),
    content_id: str = Path(examples=["widget"]),
) -> Response:
    """**Any site may frame this public document**: the CSP `sandbox` limits
    what it may do, not who may embed it, and no `frame-ancestors` is set."""
    source = await _content_store(ui_id, content_id).get_document()
    return Response(
        content=source,
        media_type="text/html",
        headers={
            "Content-Security-Policy": SANDBOX_CSP,
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": PRIVATE_CACHE,
        },
    )
