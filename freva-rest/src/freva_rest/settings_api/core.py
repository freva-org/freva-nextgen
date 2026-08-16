"""Mongo-backed storage for settings records and content documents, with strong
etags, CAS writes on `revision`, and content rendered on write, never on read.
"""

import asyncio
import hashlib
import json
import math
import uuid
from datetime import datetime, timezone
from functools import partial
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    ContextManager,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    cast,
)

import pymongo
from bson.errors import BSONError
from fastapi import HTTPException
from pydantic import BaseModel, ValidationError
from pymongo.errors import DuplicateKeyError, PyMongoError

from freva_rest.config import ServerConfig
from freva_rest.logger import logger

from .renderers import (
    MAX_RENDERED_BYTES,
    RENDERABLE_FORMATS,
    render,
    rendered_hash,
    source_hash,
)
from .sanitizer import (
    RENDERER_FINGERPRINT,
    renderer_generation,
    sanitize_html,
)

MAX_RETRIES = 3
MAX_RESPONSE_BYTES = 64 * 1024
# The caches are module globals, hence per worker process: a PATCH invalidates
# only its own worker, so the read TTL must stay short. No cross-process purge.
import os  # noqa: E402

from cachetools import TTLCache  # noqa: E402

NO_STORE_HEADERS = {"Cache-Control": "private, no-store"}
"""Cache directive carried by every error this feature raises."""


class SettingsError(HTTPException):
    """An `HTTPException` that is never cached, since every error here is about
    a dynamic resource a shared cache would otherwise store heuristically."""

    def __init__(
        self,
        status_code: int,
        detail: Any = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(
            status_code=status_code,
            detail=detail,
            headers={**NO_STORE_HEADERS, **(headers or {})},
        )


def _positive_float(name: str, default: float) -> float:
    """Read a tuning knob from the environment, falling back on an unparsable or
    non-positive value rather than failing to boot over a typo."""
    try:
        value = float(os.environ.get(name, "") or default)
    except ValueError:
        return default
    # isfinite, not just > 0: float("inf") parses and is positive, and would make
    # the cache never expire.
    if not math.isfinite(value) or value <= 0:
        return default
    return value


def _positive_int(name: str, default: int) -> int:
    """As `_positive_float`, for a count."""
    try:
        value = int(os.environ.get(name, "") or default)
    except ValueError:
        return default
    return value if value > 0 else default


_READ_TTL = _positive_float("API_SETTINGS_CACHE_TTL", 2.0)
"""Seconds a cached read is served without touching mongo, and so how long two
workers can disagree after a write - while mongo is reachable."""

_LKG_TTL = _positive_float("API_SETTINGS_LKG_TTL", 3600.0)
"""Seconds a last-known-good copy survives a mongo outage. Only read when mongo
is unreachable, so a long value costs nothing in freshness."""

MONGO_OP_TIMEOUT = _positive_float("API_SETTINGS_MONGO_TIMEOUT", 5.0)
"""Seconds one mongo operation may take, so a request can spend a multiple. Set
per operation, not on the client, which databrowser and user-data writes share."""


def mongo_timeout(seconds: Optional[float] = None) -> ContextManager[None]:
    """Bound the settings mongo operations executed inside the block. The deadline
    lives in a context variable, so the block must stay open across the await."""
    return pymongo.timeout(MONGO_OP_TIMEOUT if seconds is None else seconds)


async def bounded_cursor(
    cursor: AsyncIterable[Dict[str, Any]],
) -> AsyncIterator[Dict[str, Any]]:
    """Iterate a cursor with every fetch bounded, but not the per-document work
    between, which a deadline around the whole loop would charge to mongo."""
    iterator = cursor.__aiter__()
    while True:
        with mongo_timeout():
            try:
                document = await iterator.__anext__()
            except StopAsyncIteration:
                return
        yield document


_CACHE_BYTES = 16 * 1024 * 1024
"""Byte budget per cache, not an entry count: one content document can carry
256 KiB of source and 512 KiB of rendered html."""


_SIZEOF_OVERHEAD = 48
"""Charged per container and per scalar for python object headers and slots.
Generous on purpose: under-charging breaks the byte bound."""


def _sizeof(value: Any) -> int:
    """Recursive byte size of a cached value. Strings are measured as UTF-8, not
    characters, since a non-ASCII page is up to 4x larger than `len()`."""
    if isinstance(value, str):
        return len(value.encode("utf-8", "ignore")) + _SIZEOF_OVERHEAD
    if isinstance(value, (bytes, bytearray)):
        return len(value) + _SIZEOF_OVERHEAD
    if isinstance(value, dict):
        return _SIZEOF_OVERHEAD + sum(
            _sizeof(key) + _sizeof(item) for key, item in value.items()
        )
    if isinstance(value, (list, tuple, set, frozenset)):
        return _SIZEOF_OVERHEAD + sum(_sizeof(item) for item in value)
    if isinstance(value, bool):
        return _SIZEOF_OVERHEAD  # bool before int: bool IS an int in python
    if isinstance(value, int):
        # Ints are arbitrary precision and public_extensions accepts them, so a
        # flat charge is bypassable; this approximates the decimal width.
        return _SIZEOF_OVERHEAD + (value.bit_length() // 3) + 1
    return _SIZEOF_OVERHEAD


def _sizeof_doc(doc: Dict[str, Any]) -> int:
    """Byte size of a cached document. Runs once per cache insert, not per read."""
    return _sizeof(doc)


def _sizeof_body(entry: Tuple[bytes, str]) -> int:
    body, etag = entry
    return len(body) + len(etag) + 64


_read_cache: "TTLCache[str, Dict[str, Any]]" = TTLCache(
    maxsize=_CACHE_BYTES, ttl=_READ_TTL, getsizeof=_sizeof_doc
)
_lkg_cache: "TTLCache[str, Dict[str, Any]]" = TTLCache(
    maxsize=_CACHE_BYTES, ttl=_LKG_TTL, getsizeof=_sizeof_doc
)
# Exact body bytes plus strong ETag, so a warm GET neither queries mongo nor
# re-serialises. Cleared by the same invalidation as the doc caches.
_body_cache: "TTLCache[str, Tuple[bytes, str]]" = TTLCache(
    maxsize=_CACHE_BYTES, ttl=_READ_TTL, getsizeof=_sizeof_body
)


_WARNED: "TTLCache[str, bool]" = TTLCache(maxsize=1024, ttl=3600.0)
"""Latch of already-reported malformed documents, so one bad record does not log
on every read. Bounded rather than a `set`: the keys come from document ids."""


_mutation_epoch = 0
"""Bumped by every cache invalidation; a reader snapshots it before querying and
drops its cache write if it moved. Global, not per key, so it cannot leak."""


def _current_epoch() -> int:
    return _mutation_epoch


def _bump_epoch() -> None:
    global _mutation_epoch
    _mutation_epoch += 1


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    try:
        return _copy_doc(_read_cache[key])
    except KeyError:
        return None


def _cache_put(key: str, doc: Dict[str, Any], seen: Optional[int] = None) -> None:
    """Cache a document, unless the key was mutated since `seen`, the generation
    observed before going to mongo. Omit `seen` for a just-written value."""
    if seen is not None and seen != _current_epoch():
        return
    try:
        _read_cache[key] = _copy_doc(doc)
        _lkg_cache[key] = _copy_doc(doc)
    except ValueError:  # larger than the whole cache budget: serve uncached
        pass


def _cache_invalidate(key: str, keep_lkg: bool = False) -> None:
    """Drop the cached copies for a key and mark it mutated. `keep_lkg` spares
    the last-known-good copy, so a failed write attempt does not destroy it."""
    _bump_epoch()
    _read_cache.pop(key, None)
    _body_cache.pop(key, None)
    if not keep_lkg:
        _lkg_cache.pop(key, None)


class _Flight:
    """One shared cold read, plus how many callers still wait for it. The count is
    what lets an abandoned flight be cancelled instead of stalling later joiners."""

    __slots__ = ("task", "callers")

    def __init__(self, task: "asyncio.Task[Optional[Dict[str, Any]]]") -> None:
        self.task = task
        self.callers = 0


_INFLIGHT: "Dict[Tuple[str, int], _Flight]" = {}
"""In-flight cold reads, so concurrent misses do the work once. The key includes
the epoch, so a result is never shared across an invalidation."""


def _forget_flight(
    key: Tuple[str, int], flight: _Flight, task: "asyncio.Task[Any]"
) -> None:
    """Drop a finished flight, but only if the map still holds this one: a later
    flight for the same key may already have replaced it."""
    if _INFLIGHT.get(key) is flight:
        del _INFLIGHT[key]
    # Retrieve the exception even if every caller went away, so a failed shared
    # read does not surface later as "Task exception was never retrieved".
    if not task.cancelled():
        task.exception()


async def _single_flight(
    key: Tuple[str, int], work: "Callable[[], Awaitable[Optional[Dict[str, Any]]]]"
) -> Optional[Dict[str, Any]]:
    """Run `work` once per key and give every caller the same result. The work
    owns its task, so cancelling one caller detaches only that caller."""
    flight = _INFLIGHT.get(key)
    if flight is None:
        task = asyncio.ensure_future(work())
        flight = _Flight(task)
        _INFLIGHT[key] = flight
        task.add_done_callback(partial(_forget_flight, key, flight))
    flight.callers += 1
    try:
        return await asyncio.shield(flight.task)
    finally:
        flight.callers -= 1
        if flight.callers == 0 and not flight.task.done():
            # Nobody is waiting any more, and an abandoned read has no bound on
            # its own. Same identity guard, so a replacement is never evicted.
            if _INFLIGHT.get(key) is flight:
                del _INFLIGHT[key]
            flight.task.cancel()


STRING_ID_ONLY: Dict[str, Any] = {"_id": {"$type": "string"}}
"""Restrict a resumable scan to string `_id`s, because mongo range queries are
type-bracketed; :func:`_count_untyped_ids` reports the rest separately."""

UNTYPED_ID_SAMPLE = 20
"""How many unreachable `_id`s to name in the log. The *count* is exact; this
only bounds how much of it is spelled out."""

MAX_SCAN_DOCUMENTS = _positive_int("API_SETTINGS_MAX_SCAN", 10_000)
"""How many documents one rebuild or audit pass examines, since both traverse a
collection inside one HTTP request; they sort by `_id` and resume via `after`."""

MAX_SCAN_REFERENCES = _positive_int("API_SETTINGS_MAX_SCAN_REFS", 20_000)
"""How many content references one audit pass accumulates. The pass stops at a
record boundary, so `next_after` names a record processed in full."""

MAX_RECORD_REFERENCES = _positive_int("API_SETTINGS_MAX_RECORD_REFS", 1_000)
"""How far into one record's reference arrays the audit reads. A schema-valid
record holds a few hundred, so only an out-of-band document reaches this."""

MAX_AUDIT_PROBLEMS = _positive_int("API_SETTINGS_MAX_AUDIT_PROBLEMS", 1_000)
"""How many problem messages one audit response carries."""

MAX_PROBLEM_LENGTH = 500
"""How long one of those messages may be. An id an out-of-band document invented
is quoted back in the message, and only the schema bounds the legal ones."""

STALE_VERDICT_KEY = "__is_stale__"
"""Private key on a read view carrying the staleness verdict computed from the raw
document, which a sanitized copy would answer differently and permanently."""

MAX_SANDBOX_SOURCE_BYTES = 256 * 1024
"""Ceiling on a sandbox document's source, enforced in and out. Matches
`renderers.MAX_SOURCE_BYTES`, but named separately: the sandbox never renders."""

API_OWNED_FIELDS = ("schema_version",)
"""Model fields the api owns outright, never inherited from storage, so the model
default always describes the shape this build produces."""


class _AbsentDocument(dict):  # type: ignore[type-arg]
    """The tombstone recording "mongo confirmed there is no document here". A type
    rather than a magic key, since pymongo decodes into plain `dict` only."""

    __slots__ = ()


ABSENT = _AbsentDocument()
"""The single tombstone value. Empty, so even if one escaped into a code path
that treated it as a document it would resolve to "no overrides"."""


def _absent_marker() -> Dict[str, Any]:
    return _AbsentDocument()


def _is_absent(doc: Optional[Dict[str, Any]]) -> bool:
    return isinstance(doc, _AbsentDocument)


def _copy_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Copy a cached document, preserving a tombstone's type: `dict(tombstone)`
    would read as an existing document with no fields."""
    return _AbsentDocument() if _is_absent(doc) else dict(doc)


def _last_known_good(key: str) -> Optional[Dict[str, Any]]:
    try:
        return _copy_doc(_lkg_cache[key])
    except KeyError:
        return None


def _body_get(key: str) -> Optional[Tuple[bytes, str]]:
    try:
        return _body_cache[key]
    except KeyError:
        return None


def _body_put(key: str, body: bytes, etag: str, seen: Optional[int] = None) -> None:
    """Cache serialised bytes, unless the key was mutated since `seen`. Same
    rule as `_cache_put`."""
    if seen is not None and seen != _current_epoch():
        return
    try:
        _body_cache[key] = (body, etag)
    except ValueError:  # larger than the whole cache budget: serve uncached
        pass


def reset_caches() -> None:
    """Clear every module cache. The live-test fixture calls this between tests,
    so a cached document cannot carry from one test into the next."""
    _read_cache.clear()
    _lkg_cache.clear()
    _body_cache.clear()
    # bump rather than zero: a read still in flight holding an old snapshot must
    # not be able to match the reset value and store into the fresh caches
    _bump_epoch()
    _WARNED.clear()


# shared helpers


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def serialise(payload: Dict[str, Any]) -> bytes:
    """Serialise to the exact bytes that are hashed and served. `allow_nan=False`
    keeps a non-finite float from emitting json no `JSON.parse` accepts."""
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def etag_of(body: bytes) -> str:
    return f'"{hashlib.sha256(body).hexdigest()[:32]}"'


def strong_if_match(header: str, etag: str, exists: bool = True) -> bool:
    """Evaluate an `If-Match` precondition against the current entity tag, where
    `exists` means "has a current representation", not "is stored in mongo"."""
    if not exists:
        # RFC 9110 13.1.1: If-Match fails when the resource has no current
        # representation, for *any* value - not just "*".
        return False
    header = header.strip()
    if header == "*":
        return True
    for candidate in header.split(","):
        candidate = candidate.strip()
        if candidate.startswith("W/"):  # weak never matches for If-Match
            continue
        if candidate == etag:
            return True
    return False


def if_none_match(header: Optional[str], etag: str) -> bool:
    if not header:
        return False
    header = header.strip()
    if header == "*":
        return True
    for candidate in header.split(","):
        candidate = candidate.strip()
        if candidate.startswith("W/"):
            candidate = candidate[2:]
        if candidate == etag:
            return True
    return False


def check_if_match(
    header: Optional[str], candidates: Tuple[str, ...], what: str
) -> None:
    """Enforce an `If-Match` precondition, or raise 412. `candidates` is every
    strong tag a client may legitimately hold for the representation."""
    if header is None:
        return
    # exists=True: a caller only reaches this once it has a representation, so
    # `*` matches; when there is nothing, the request has already 404'd.
    if not any(strong_if_match(header, tag, exists=True) for tag in candidates):
        raise SettingsError(
            status_code=412,
            detail=(
                f"The {what} changed since it was read, so it was not deleted. "
                "Re-read it and retry with the new ETag."
            ),
        )


def _warn_once(key: str, message: str, *args: Any) -> None:
    """Log a stored-document defect once per latch key. The message is logged
    verbatim, since each caller states its own remedy."""
    if key in _WARNED:
        return
    _WARNED[key] = True
    logger.error(message, *args)


def _doc_latch(identity: str, doc: Dict[str, Any], reason: str) -> str:
    """A warning latch key scoped to one generation of one document. `identity`
    is the requested cache key, since the damaged document's own ids may be gone."""
    generation = doc.get(CAS_TOKEN_FIELD, doc.get("revision", "?"))
    return f"{identity}:{generation}:{reason}"


def _ensure_encodable(value: Any, path: str = "") -> None:
    """Refuse anything that cannot be encoded as UTF-8, with a 422: a lone
    surrogate survives `json.loads` and the `ExtensionValue` union."""
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as error:
            raise SettingsError(
                status_code=422,
                detail=(
                    f"The value at {path or 'the document root'} contains "
                    "characters that cannot be encoded as UTF-8 (an unpaired "
                    "surrogate). Remove them and retry."
                ),
            ) from error
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _ensure_encodable(key, f"{path}.{key}" if path else str(key))
            _ensure_encodable(item, f"{path}.{key}" if path else str(key))
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _ensure_encodable(item, f"{path}[{index}]")


def _stored_html(doc: Dict[str, Any]) -> str:
    """The html field of a *read view*, already sanitized by
    :func:`sanitize_read_view` or by the write that rendered it."""
    html = doc.get("rendered_html")
    return html if isinstance(html, str) else ""


async def sanitize_read_view(identity: str, doc: Dict[str, Any]) -> Dict[str, Any]:
    """Return the document as it may be served: html forced through the
    allow-list, bounded in size, sanitized off the event-loop thread."""
    html = doc.get("rendered_html")
    stale = needs_rebuild(doc)
    if doc.get("format") == "sandbox-html" or not isinstance(html, str) or not html:
        # sandbox html is executable by design and leaves only through the
        # iframe /document endpoint, behind a CSP
        return {**doc, STALE_VERDICT_KEY: stale}
    # surrogatepass: this only measures, so it must not raise on a string mongo
    # could not have stored
    if len(html.encode("utf-8", "surrogatepass")) > MAX_RENDERED_BYTES:
        _warn_once(
            _doc_latch(identity, doc, "oversized-html"),
            "Stored html for %s/%s is larger than the %d byte ceiling, so it "
            "was not written through this api. Serving nothing for it and not "
            "sanitising it (that is the expensive part); re-render it with the "
            "rebuild endpoint.",
            doc.get("ui_id"),
            doc.get("content_id"),
            MAX_RENDERED_BYTES,
        )
        return {**doc, "rendered_html": "", STALE_VERDICT_KEY: True}
    try:
        cleaned = await asyncio.to_thread(sanitize_html, html)
    except RuntimeError:  # pragma: no cover - nh3 missing, deployment misconfig
        _warn_once(
            _doc_latch(identity, doc, "no-sanitizer"),
            "Cannot sanitize stored html for %s/%s (nh3 is not installed). "
            "Serving nothing rather than unsanitised markup; install nh3.",
            doc.get("ui_id"),
            doc.get("content_id"),
        )
        return {**doc, "rendered_html": "", STALE_VERDICT_KEY: True}
    if len(cleaned.encode("utf-8")) > MAX_RENDERED_BYTES:
        # sanitising can *grow* html - it closes and reopens tags to repair bad
        # nesting - so the ceiling has to be checked on the way out too
        _warn_once(
            _doc_latch(identity, doc, "expanded-html"),
            "Sanitising the stored html for %s/%s produced more than the %d "
            "byte ceiling. Serving nothing for it; re-render it with the "
            "rebuild endpoint.",
            doc.get("ui_id"),
            doc.get("content_id"),
            MAX_RENDERED_BYTES,
        )
        return {**doc, "rendered_html": "", STALE_VERDICT_KEY: True}
    if cleaned != html:
        _warn_once(
            _doc_latch(identity, doc, "untrusted-html"),
            "Stored html for %s/%s changed under sanitisation, so it was not "
            "written by this renderer. Serving the sanitised form; re-render it "
            "with the rebuild endpoint.",
            doc.get("ui_id"),
            doc.get("content_id"),
        )
        # Drift is staleness: a mongo writer computes html and digest together,
        # so nothing else here would call it stale and the rebuild would skip it.
        return {**doc, "rendered_html": cleaned, STALE_VERDICT_KEY: True}
    return {**doc, STALE_VERDICT_KEY: stale}


def needs_rebuild(doc: Dict[str, Any]) -> bool:
    """True when a stored document's rendering cannot be trusted; the single
    source of truth for `is_stale` on a read and for the rebuild's selection."""
    fmt = doc.get("format")
    if not isinstance(fmt, str) or fmt not in RENDERABLE_FORMATS:
        return False
    if _as_str(doc.get("renderer_version")) != RENDERER_FINGERPRINT:
        return True
    html = doc.get("rendered_html")
    if not isinstance(html, str):
        return True
    # source_hash covers source and format only, so hand-written rendered_html
    # passes every other check. This detects drift; it does not authenticate.
    if doc.get("rendered_hash") != rendered_hash(html):
        return True
    # MISSING, not "": defaulting to "" makes a source-less document look
    # current whenever its source_hash happens to be source_hash("", fmt).
    source = doc.get("source", MISSING)
    if not isinstance(source, str):
        return True
    return bool(doc.get("source_hash") != source_hash(source, fmt))


def _warn_forget(prefix: str) -> None:
    """Drop the latches for one document, so a later break is reported again."""
    # prefix + ":" - a bare startswith would let repairing content "home" clear
    # the latch for "homepage" as well
    scoped = prefix if prefix.endswith(":") else prefix + ":"
    for key in [k for k in list(_WARNED) if k.startswith(scoped)]:
        _WARNED.pop(key, None)


def _as_str(value: Any) -> str:
    """Coerce a stored field to `str`. Stored documents are not guaranteed to
    be well typed, and a read must degrade rather than raise."""
    return value if isinstance(value, str) else ("" if value is None else str(value))


BSON_INT64_MAX = 2**63 - 1
BSON_INT64_MIN = -(2**63)
"""Mongo stores integers as signed 64-bit; a revision outside this range is not
encodable, so the write meant to repair a damaged document would itself fail."""


def _as_int(value: Any) -> int:
    """Coerce a stored field to a *storable* `int`, 0 on anything unusable.
    Reset rather than coerce: a lossy revision can match the wrong generation."""
    if isinstance(value, bool):
        # bool is an int subclass; True as a revision is nonsense, not 1
        return 0
    if isinstance(value, float) and not value.is_integer():
        return 0
    if isinstance(value, str):
        # "12" is a plausible repair; "1.9" is not, and int() refuses it
        try:
            value = int(value)
        except ValueError:
            return 0
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    if not 0 <= number < BSON_INT64_MAX:
        return 0
    return number


def format_mismatch(
    ui_id: str, content_id: str, fmt: Any, expect: Optional[str]
) -> List[str]:
    """Why this content's stored format cannot serve this kind of reference;
    empty when the reference is fine."""
    if expect is None:
        return []
    # isinstance first: RENDERABLE_FORMATS is a frozenset, so an out-of-band
    # format=[] or {} would raise TypeError: unhashable type
    is_sandbox = isinstance(fmt, str) and fmt == "sandbox-html"
    is_rendered = isinstance(fmt, str) and fmt in RENDERABLE_FORMATS
    if expect == "rendered" and not is_rendered:
        if is_sandbox:
            return [
                f"{ui_id}/{content_id} is sandbox-html and cannot be inlined; "
                "use a sandbox route"
            ]
        return [
            f"{ui_id}/{content_id} has an unusable format {fmt!r} and cannot be "
            "rendered; repair the content document"
        ]
    if expect == "sandbox" and not is_sandbox:
        return [
            f"{ui_id}/{content_id} is {fmt!r}, not sandbox-html; a sandbox "
            "route needs sandbox-html content"
        ]
    return []


class _Missing:
    """Sentinel for "this field is not in the document at all". `None` cannot
    serve: mongo needs different filters for absent and `null` fields."""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<missing>"


MISSING = _Missing()


CAS_TOKEN_FIELD = "cas_token"
"""Field holding an opaque, per-write identity for a stored document. A revision
counter alone cannot express generation, since delete-and-recreate resets it."""


RESERVED_STORAGE_FIELDS: Tuple[str, ...] = (
    "_id",
    "resource_name",
    "record_id",
    "revision",
    CAS_TOKEN_FIELD,
)
"""Document keys the storage layer owns, which no resource model may declare: a
model field of one of these names makes every later write miss its predicate."""


def check_reserved_fields(name: str, *models: Type[BaseModel]) -> None:
    """Refuse a resource model that declares a storage-owned field. Called from
    the registry at import, so a misconfigured deployment fails to start."""
    for model in models:
        clashes = sorted(set(model.model_fields) & set(RESERVED_STORAGE_FIELDS))
        if clashes:
            raise RuntimeError(
                f"settings resource '{name}': {model.__name__} declares "
                f"{', '.join(clashes)}, which the storage layer owns. "
                "A model field of that name would be written over the "
                "document's identity or its compare-and-swap state, and every "
                "later write to the record would fail its own predicate. "
                f"Rename the field. Reserved: {', '.join(RESERVED_STORAGE_FIELDS)}."
            )


def _new_cas_token() -> str:
    return uuid.uuid4().hex


def _exact_field_clauses(field: str, value: Any) -> List[Dict[str, Any]]:
    """Clauses matching `field` to exactly `value` - same value, same type.
    Query equality matches missing fields, descends arrays and equates numbers."""
    if isinstance(value, _Missing):
        return [{field: {"$exists": False}}]
    path = f"${field}"
    literal = {"$literal": value}
    return [
        {field: {"$exists": True}},
        {
            "$expr": {
                "$and": [
                    {"$eq": [{"$type": path}, {"$type": literal}]},
                    {"$eq": [path, literal]},
                ]
            }
        },
    ]


def _cas_predicate(
    key: Dict[str, str], stored_revision: Any, stored_token: Any = MISSING
) -> Dict[str, Any]:
    """The compare-and-swap filter matching the document we actually read.
    `MISSING` means absent, which is not a stored `0` or `null`."""
    predicate: Dict[str, Any] = dict(key)
    # BOTH fields, always: an out-of-band edit can advance `revision` while
    # leaving `cas_token` alone, and a stale writer would match on the token.
    predicate["$and"] = _exact_field_clauses(
        CAS_TOKEN_FIELD, stored_token
    ) + _exact_field_clauses("revision", stored_revision)
    return predicate


# settings records


class SettingsStore:
    """One settings record, `(resource_name, record_id)`. `open_maps` names
    the fields that merge key-by-key on a patch, where `null` deletes a key."""

    def __init__(
        self,
        config: ServerConfig,
        resource_name: str,
        record_id: str,
        model: Type[BaseModel],
        update_model: Type[BaseModel],
        open_maps: Tuple[str, ...] = (),
    ) -> None:
        self._config = config
        self.resource_name = resource_name
        self.record_id = record_id
        self._model = model
        self._update_model = update_model
        self._open_maps = open_maps
        # Set by cas_snapshot: False when a write landed while its read was in
        # flight, so that snapshot is overtaken and may not write to any cache.
        self._snapshot_was_current = True

    @property
    def _doc_id(self) -> str:
        # The natural key IS the mongo _id, so uniqueness needs no secondary
        # index; ':' cannot appear in either component, so the join is exact.
        return f"{self.resource_name}:{self.record_id}"

    @property
    def _key(self) -> Dict[str, str]:
        return {"_id": self._doc_id}

    @property
    def _collection(self) -> Any:
        return self._config.mongo_collection_settings

    @property
    def _cache_key(self) -> str:
        return f"settings:{self.resource_name}:{self.record_id}"

    @property
    def synthesises_default(self) -> bool:
        """True when a GET of this record returns 200 even with nothing stored.
        This is what `If-Match: *` tests: a representation, not a document."""
        return self.record_id == DEFAULT_RECORD_ID

    async def _read(self, seen: Optional[int] = None) -> Dict[str, Any]:
        cached = _cache_get(self._cache_key)
        if cached is not None:
            return {} if _is_absent(cached) else cached
        # Resolve `seen` here: it is part of the flight's identity, and a `None`
        # would pool callers that observed different generations into one flight.
        if seen is None:
            seen = _current_epoch()
        # Coalesced: a cold cache on the record every page load reads would
        # otherwise mean one mongo query per concurrent request.
        result = await _single_flight(
            (self._cache_key, seen), lambda: self._read_uncached(seen=seen)
        )
        return result or {}

    async def _read_uncached(
        self, for_write: bool = False, seen: Optional[int] = None
    ) -> Dict[str, Any]:
        """Read straight from mongo so a CAS retry sees the real revision.
        `for_write` also disables last-known-good fallback and caching."""
        if seen is None:
            seen = _current_epoch()
        try:
            with mongo_timeout():
                doc = await self._collection.find_one(self._key)
            result = dict(doc) if doc else {}
            if not for_write:
                # An empty result is cached as an explicit tombstone: only
                # "mongo says no override" licenses defaults during an outage.
                _cache_put(self._cache_key, result or _absent_marker(), seen)
            return result
        except PyMongoError as error:
            good = None if for_write else _last_known_good(self._cache_key)
            if good is not None:
                logger.warning(
                    "Serving last-known-good %s settings; mongo read failed: %s",
                    self.resource_name,
                    error,
                )
                # A tombstone resolves to "no override", so a missing named
                # record still 404s rather than becoming the default.
                return {} if _is_absent(good) else good
            logger.error("Could not read settings: %s", error)
            raise SettingsError(
                status_code=503,
                detail="Could not reach the configuration database.",
            ) from error

    def _overrides(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        known = set(self._model.model_fields)
        # never let bookkeeping keys - or api-owned ones - reach the model
        return {
            k: v
            for k, v in doc.items()
            if k in known
            and k
            not in (
                "_id",
                "resource_name",
                "record_id",
                "revision",
                CAS_TOKEN_FIELD,
                *API_OWNED_FIELDS,
            )
        }

    def _resolve(self, overrides: Dict[str, Any]) -> BaseModel:
        return self._model(**overrides)

    def _safe_body(self, overrides: Dict[str, Any]) -> bytes:
        latch = f"settings:{self.resource_name}:{self.record_id}"
        try:
            body = serialise(self._resolve(overrides).model_dump(by_alias=True))
        except (ValidationError, ValueError) as error:
            # ValueError also covers serialise()'s allow_nan guard: a stored
            # non-finite float would otherwise emit json no browser can parse.
            _warn_once(
                latch + ":invalid",
                "Stored settings invalid: %s. Serving defaults instead; this "
                "needs an operator.",
                error,
            )
            return serialise(self._model().model_dump(by_alias=True))
        if len(body) > MAX_RESPONSE_BYTES:
            _warn_once(
                latch + ":big",
                "Stored settings too large. Serving defaults instead; this needs "
                "an operator.",
            )
            return serialise(self._model().model_dump(by_alias=True))
        return body

    async def get(self, allow_default: bool = True) -> Tuple[bytes, str]:
        """Get the resolved settings as response bytes and entity tag. False
        `allow_default` 404s an unwritten record instead of serving defaults."""
        cached = _body_get(self._cache_key)
        if cached is not None:
            return cached
        # snapshot before touching mongo, so both caches below can tell whether
        # a write overtook this read
        seen = _current_epoch()
        doc = await self._read(seen=seen)
        if not doc and not allow_default:
            raise SettingsError(
                status_code=404,
                detail=(
                    f"No '{self.resource_name}' record '{self.record_id}'. "
                    "Named records must be created before they can be read; "
                    "there is no fallback to the default."
                ),
            )
        body = self._safe_body(self._overrides(doc))
        etag = etag_of(body)
        _body_put(self._cache_key, body, etag, seen)
        return body, etag

    async def exists(self) -> bool:
        return bool(await self._read())

    def _merge(self, overrides: Dict[str, Any], update: BaseModel) -> Dict[str, Any]:
        """Fold a patch into the stored overrides: `null` removes an override
        at any depth, dicts merge recursively, and lists are replaced whole."""
        return self._merge_level(
            dict(overrides), update.model_dump(exclude_unset=True), ""
        )

    def _merge_level(
        self, current: Dict[str, Any], given: Dict[str, Any], prefix: str
    ) -> Dict[str, Any]:
        merged = dict(current)
        for field, value in given.items():
            path = f"{prefix}.{field}" if prefix else field
            if value is None:
                # null removes the override at this level, restoring the default
                merged.pop(field, None)
                continue
            if isinstance(value, dict):
                if path in self._open_maps:
                    if value == {}:
                        # an explicit empty map clears the map while keeping it
                        # present; null removes the override entirely
                        merged[field] = {}
                        continue
                    existing_map = merged.get(field)
                    # A stored open map may not be a dict, and dict() on a
                    # truthy non-dict raises; patch over it rather than 500.
                    entries = (
                        dict(existing_map) if isinstance(existing_map, dict) else {}
                    )
                    for key, entry in value.items():
                        if entry is None:
                            entries.pop(key, None)
                        else:
                            entries[key] = entry
                    merged[field] = entries
                    continue
                if not value:
                    # An empty object for a nested model patches nothing, so it
                    # must not create the container and trip its required fields.
                    continue
                # a nested model: recurse so siblings survive
                existing = merged.get(field)
                base = dict(existing) if isinstance(existing, dict) else {}
                merged[field] = self._merge_level(base, value, path)
                continue
            merged[field] = value
        return merged

    async def patch(
        self,
        update: BaseModel,
        if_match: Optional[str] = None,
        validate: Optional["Callable[[BaseModel], Awaitable[None]]"] = None,
    ) -> Tuple[bytes, str]:
        """Apply a patch under compare-and-swap. `validate` runs inside the CAS
        loop, so it checks the exact candidate that is about to be stored."""
        for _ in range(MAX_RETRIES):
            # A CAS retry must see the real current revision, so invalidate
            # first. LKG is kept: a failed write keeps readers' outage fallback.
            _cache_invalidate(self._cache_key, keep_lkg=True)
            doc = await self._read_uncached(for_write=True, seen=_current_epoch())
            # raw value for the CAS filter, normalised int for the value written
            # back, so a malformed revision stays matchable and is repaired
            stored_revision = doc.get("revision", MISSING)
            stored_token = doc.get(CAS_TOKEN_FIELD, MISSING)
            revision = _as_int(stored_revision)
            overrides = self._overrides(doc)

            if if_match is not None:
                if not strong_if_match(
                    if_match,
                    etag_of(self._safe_body(overrides)),
                    exists=bool(doc) or self.synthesises_default,
                ):
                    raise SettingsError(
                        status_code=412,
                        detail="The settings changed since read; re-read and retry.",
                    )

            candidate = self._merge(overrides, update)
            _ensure_encodable(candidate)
            try:
                model = self._resolve(candidate)
            except ValidationError as error:
                # include_context=False drops a custom validator's raw exception
                # objects, which are not json-serialisable and would 500.
                raise SettingsError(
                    status_code=422,
                    detail=error.errors(include_url=False, include_context=False),
                ) from error
            # Bound-check before the validator: the size check is local and
            # deterministic, while the validator goes to mongo on every retry.
            body = serialise(model.model_dump(by_alias=True))
            if len(body) > MAX_RESPONSE_BYTES:
                raise SettingsError(
                    status_code=413,
                    detail=f"Resolved settings exceed {MAX_RESPONSE_BYTES} bytes.",
                )
            if validate is not None:
                # runs on the resolved candidate about to be written, so the
                # thing validated is exactly the thing stored
                await validate(model)

            # API-owned identity last, so a model field that slipped past the
            # reserved-field check at import still cannot overwrite it
            stored = {
                **candidate,
                "_id": self._doc_id,
                "resource_name": self.resource_name,
                "record_id": self.record_id,
                "revision": revision + 1,
                CAS_TOKEN_FIELD: _new_cas_token(),
            }
            try:
                if not doc:
                    with mongo_timeout():
                        await self._collection.insert_one(stored)
                    _warn_forget(self._cache_key)
                    _cache_invalidate(self._cache_key)
                    _cache_put(self._cache_key, stored)
                    return body, etag_of(body)
                predicate = _cas_predicate(self._key, stored_revision, stored_token)
                with mongo_timeout():
                    result = await self._collection.replace_one(predicate, stored)
                if result.matched_count:
                    _warn_forget(self._cache_key)
                    _cache_invalidate(self._cache_key)
                    _cache_put(self._cache_key, stored)
                    return body, etag_of(body)
            except DuplicateKeyError:
                continue
            except (BSONError, OverflowError) as error:
                # Neither BSONError nor OverflowError is a PyMongoError, so
                # without this they escape the handler below and become a 500.
                raise SettingsError(
                    status_code=422,
                    detail=(
                        "The resolved document cannot be stored by MongoDB: "
                        f"{error}. Integers must fit in signed 64 bits and map "
                        "keys must not contain control characters."
                    ),
                ) from error
            except PyMongoError as error:
                logger.error("Could not update settings: %s", error)
                raise SettingsError(
                    status_code=503,
                    detail="Could not reach the configuration database.",
                ) from error
        raise SettingsError(
            status_code=409,
            detail="The settings are being updated concurrently, try again.",
        )

    async def cas_state(self) -> Optional[Dict[str, Any]]:
        """The identity of the stored document right now, or `None`. Captured
        before a delete's reference scan so the removal is a compare-and-swap."""
        snapshot = await self.cas_snapshot()
        return None if snapshot is None else snapshot[1]

    async def cas_snapshot(self) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
        """The stored document *and* its CAS identity, from a single read: from
        separate reads, `If-Match` could pass against another generation."""
        seen = _current_epoch()
        doc = await self._read_uncached(for_write=True, seen=seen)
        # Synchronous from here, so nothing lands between the generation check
        # and the cache writes it authorises. An overtaken snapshot still returns.
        self._snapshot_was_current = seen == _current_epoch()
        if self._snapshot_was_current:
            self._reconcile_cache(doc)
        if not doc:
            return None
        return doc, {
            "revision": doc.get("revision", MISSING),
            CAS_TOKEN_FIELD: doc.get(CAS_TOKEN_FIELD, MISSING),
        }

    def _reconcile_cache(self, authoritative: Optional[Dict[str, Any]]) -> None:
        """Drop local cache entries this snapshot has just disproved, including
        last-known-good. Same-worker knowledge only; nothing is replaced."""
        # Bump unconditionally, even when nothing is cached or the entries agree:
        # the epoch is what makes a read already in flight drop its result.
        _bump_epoch()
        cached = _cache_get(self._cache_key)
        good = _last_known_good(self._cache_key)

        # The serialised body goes unconditionally: it stores no generation
        # identity, so it can never be proven current against a snapshot.
        _body_cache.pop(self._cache_key, None)

        if cached is None and good is None:
            return  # nothing else held locally, nothing to disprove

        def _agrees(held: Optional[Dict[str, Any]]) -> bool:
            if held is None:
                return True  # absent entries cannot disagree
            if _is_absent(held):
                return not authoritative
            if not authoritative:
                return False
            return all(
                held.get(field, MISSING) == authoritative.get(field, MISSING)
                for field in ("revision", CAS_TOKEN_FIELD)
            )

        if not (_agrees(cached) and _agrees(good)):
            _cache_invalidate(self._cache_key)

    def note_absent(self) -> None:
        """Record that mongo has just confirmed this record has no document, so
        a following outage synthesises the default instead of answering 503."""
        if not self._snapshot_was_current:
            return
        self._invalidate(seed_absent=True)

    def etags_for(self, doc: Optional[Dict[str, Any]]) -> Tuple[str, ...]:
        """Every strong etag a client may legitimately be holding for `doc`.
        Plural because the content store's version returns public and admin."""
        return (etag_of(self._safe_body(self._overrides(doc or {}))),)

    def _invalidate(self, seed_absent: bool) -> None:
        """Drop the caches and, when asked, leave a tombstone behind.
        `seed_absent` is the caller's decision; it cannot be made here."""
        _cache_invalidate(self._cache_key)
        if seed_absent:
            _cache_put(self._cache_key, _absent_marker())

    async def delete(
        self, expected: Optional[Dict[str, Any]] = None, seed_absent: bool = False
    ) -> str:
        """Delete the document, optionally only if it still matches `expected`.
        Returns `"deleted"`, `"missing"` or `"changed"` for 204, 404, 409."""
        if expected is None:
            predicate: Dict[str, Any] = dict(self._key)
        else:
            predicate = _cas_predicate(
                self._key,
                expected.get("revision", MISSING),
                expected.get(CAS_TOKEN_FIELD, MISSING),
            )
        try:
            seen = _current_epoch()
            with mongo_timeout():
                result = await self._collection.delete_one(predicate)
            # Evaluated before _invalidate, which bumps the epoch itself.
            still_current = seen == _current_epoch()
            if result.deleted_count:
                _warn_forget(self._cache_key)
                self._invalidate(seed_absent and still_current)
                return "deleted"
            if expected is None:
                self._invalidate(seed_absent and still_current)
                return "missing"
            # Nothing matched, so invalidate before the follow-up query, not
            # after. LKG is kept until the document is confirmed gone.
            _cache_invalidate(self._cache_key, keep_lkg=True)
            # Re-captured: the invalidation above bumped the epoch, so the one
            # from the top cannot describe the interval this await opens.
            seen = _current_epoch()
            # either it is gone, or it moved on to a generation we never validated
            with mongo_timeout():
                still_there = await self._collection.find_one(self._key, {"_id": 1})
            still_current = seen == _current_epoch()
            if still_there:
                # Nothing was removed, so the last-known-good copy stays; a
                # failed delete must not punish readers.
                return "changed"
            self._invalidate(seed_absent and still_current)
            return "missing"
        except PyMongoError as error:
            logger.error("Could not delete %s: %s", self._cache_key, error)
            raise SettingsError(
                status_code=503,
                detail="Could not reach the configuration database.",
            ) from error


# content documents

from .schema import (  # noqa: E402
    DEFAULT_RECORD_ID,
    VALID_CONTENT_FORMATS,
    ContentAdmin,
    ContentFormat,
    ContentPublic,
    ContentSource,
)


class ContentStore:
    """One content document `(ui_id, content_id)`; a write renders when the
    source or format changes, and the public read never returns the source.
    """

    def __init__(self, config: ServerConfig, ui_id: str, content_id: str) -> None:
        self._config = config
        self.ui_id = ui_id
        self.content_id = content_id

    @property
    def _doc_id(self) -> str:
        # natural key as mongo _id; see SettingsStore._doc_id
        return f"{self.ui_id}:{self.content_id}"

    @property
    def _key(self) -> Dict[str, str]:
        return {"_id": self._doc_id}

    @property
    def _collection(self) -> Any:
        return self._config.mongo_collection_ui_contents

    @property
    def _cache_key(self) -> str:
        return f"content:{self.ui_id}:{self.content_id}"

    async def _read(self, seen: Optional[int] = None) -> Optional[Dict[str, Any]]:
        cached = _cache_get(self._cache_key)
        if cached is not None:
            return None if _is_absent(cached) else cached
        # Resolve `seen` here: it is part of the flight's identity, so a `None`
        # would make every caller that omitted it share one flight.
        if seen is None:
            seen = _current_epoch()
        # Coalesce the cold fill: read and sanitisation happen once for all
        # requests missing the cache that agree on the generation - see _INFLIGHT.
        return await _single_flight(
            (self._cache_key, seen), lambda: self._read_uncached(seen=seen)
        )

    async def _read_uncached(
        self, for_write: bool = False, seen: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Read straight from mongo so a CAS retry sees the real current
        revision; `for_write` also disables last-known-good."""
        if seen is None:
            seen = _current_epoch()
        try:
            with mongo_timeout():
                doc = await self._collection.find_one(self._key)
            result = dict(doc) if doc else None
            if not for_write:
                if result:
                    # The single point where a stored document becomes a
                    # servable one; a write-side read keeps the raw html.
                    result = await sanitize_read_view(self._cache_key, result)
                    # see SettingsStore._read_uncached
                    _cache_put(self._cache_key, result, seen)
                else:
                    # Cache the confirmed miss briefly, or every url typo and
                    # bot probe reaches mongo once per request for a whole TTL.
                    _cache_put(self._cache_key, _absent_marker(), seen)
            return result
        except PyMongoError as error:
            good = None if for_write else _last_known_good(self._cache_key)
            if good is not None:
                if _is_absent(good):
                    # known missing before the outage began: a 404 is the same
                    # answer mongo would give
                    return None
                logger.warning(
                    "Serving last-known-good content %s/%s; mongo read failed: " "%s",
                    self.ui_id,
                    self.content_id,
                    error,
                )
                return good
            logger.error("Could not read content: %s", error)
            raise SettingsError(
                status_code=503,
                detail="Could not reach the configuration database.",
            ) from error

    def _public(self, doc: Dict[str, Any]) -> ContentPublic:
        # Read defensively: a stored document may have no 'format' or one this
        # build cannot serve, and a read must never raise on a bad document.
        fmt = doc.get("format")
        # isinstance guard: an unhashable stored format would raise TypeError
        # out of the membership test.
        if not isinstance(fmt, str) or fmt not in VALID_CONTENT_FORMATS:
            _warn_once(
                _doc_latch(self._cache_key, doc, "format"),
                "Stored content %s/%s has an unusable format %r; serving it as "
                "empty and stale so the page degrades instead of 500ing.",
                self.ui_id,
                self.content_id,
                fmt,
            )
            return ContentPublic(
                # the route's identity, not the document's - see below
                ui_id=self.ui_id,
                content_id=self.content_id,
                format="html-fragment",
                title=_as_str(doc.get("title")),
                rendered_html="",
                renderer_version=renderer_generation(
                    _as_str(doc.get("renderer_version"))
                ),
                is_stale=True,
                # _as_int, not int(): this branch already has a malformed
                # document, so no other field may be assumed well typed.
                revision=_as_int(doc.get("revision")),
                updated_at=_as_str(doc.get("updated_at")),
                is_sandbox=False,
            )
        # narrowed by the VALID_CONTENT_FORMATS membership test above, which mypy
        # cannot see through a frozenset
        fmt = cast(ContentFormat, fmt)
        is_sandbox = fmt == "sandbox-html"
        # the generation only - see ContentPublic.renderer_version
        stored_version = renderer_generation(_as_str(doc.get("renderer_version")))
        return ContentPublic(
            # The identity the client asked for, always: a response built from
            # the document's stored copies could name a different document.
            ui_id=self.ui_id,
            content_id=self.content_id,
            format=fmt,
            title=_as_str(doc.get("title")),
            # Already sanitized by sanitize_read_view or by the render that
            # produced it; this function is sync, so it cannot do the work.
            rendered_html="" if is_sandbox else _stored_html(doc),
            renderer_version=stored_version,
            # The read never re-renders but reports staleness honestly: the
            # verdict comes from the raw document via sanitize_read_view.
            is_stale=(
                bool(doc[STALE_VERDICT_KEY])
                if STALE_VERDICT_KEY in doc
                # `.get(key, needs_rebuild(doc))` would evaluate the fallback
                # even when the key is present, re-hashing on every admin read
                else needs_rebuild(doc)
            ),
            revision=_as_int(doc.get("revision")),
            updated_at=_as_str(doc.get("updated_at")),
            is_sandbox=is_sandbox,
        )

    def _admin(self, doc: Dict[str, Any]) -> ContentAdmin:
        pub = self._public(doc).model_dump()
        return ContentAdmin(
            **pub,
            source=_as_str(doc.get("source")),
            source_hash=_as_str(doc.get("source_hash")),
            # the precise stack, for the operator who has to decide whether a
            # rebuild is needed - never in the public shape
            renderer_fingerprint=_as_str(doc.get("renderer_version")),
        )

    async def get_public(self) -> Tuple[bytes, str]:
        # public reads use the serialised body cache; admin reads never do, or
        # their bytes under the same key could serve the source publicly.
        cached = _body_get(self._cache_key)
        if cached is not None:
            return cached
        seen = _current_epoch()
        doc = await self._read(seen=seen)
        if doc is None:
            raise SettingsError(status_code=404, detail="No such content.")
        body = serialise(self._public(doc).model_dump())
        etag = etag_of(body)
        _body_put(self._cache_key, body, etag, seen)
        return body, etag

    async def get_admin(self) -> Tuple[bytes, str]:
        doc = await self._read()
        if doc is None:
            raise SettingsError(status_code=404, detail="No such content.")
        body = serialise(self._admin(doc).model_dump())
        return body, etag_of(body)

    async def get_document(self) -> str:
        """The raw sandbox-html source, for the iframe /document endpoint only.
        Deleting it revokes it, so `for_write=True` means no cache, no LKG."""
        doc = await self._read_uncached(for_write=True, seen=_current_epoch())
        if doc is None:
            raise SettingsError(status_code=404, detail="No such content.")
        # .get, not [...]: a document with no 'format' at all must 404 here like
        # any other non-sandbox content, not raise a KeyError into a 500.
        if doc.get("format") != "sandbox-html":
            raise SettingsError(
                status_code=404,
                detail="Only sandbox-html content has a document representation.",
            )
        source = doc.get("source")
        if not isinstance(source, str):
            # Coercing with _as_str would invent content: a None source becomes
            # an empty 200, and a list or dict becomes its python repr as html.
            _warn_once(
                _doc_latch(self._cache_key, doc, "malformed-sandbox-source"),
                "Stored sandbox source for %s/%s is %s, not text, so it was not "
                "written through this api. Refusing to serve it.",
                self.ui_id,
                self.content_id,
                type(source).__name__,
            )
            raise SettingsError(
                status_code=422,
                detail=(
                    f"The stored sandbox document's source is "
                    f"{type(source).__name__}, not text, so it is not served. "
                    "Repair it with a PATCH carrying an explicit 'source'."
                ),
            )
        # The write caps the source and this read has no cache, so an
        # oversized out-of-band document would be refetched on every iframe load.
        if len(source.encode("utf-8", "surrogatepass")) > MAX_SANDBOX_SOURCE_BYTES:
            _warn_once(
                _doc_latch(self._cache_key, doc, "oversized-sandbox"),
                "Stored sandbox source for %s/%s exceeds the %d byte ceiling, so "
                "it was not written through this api. Refusing to serve it.",
                self.ui_id,
                self.content_id,
                MAX_SANDBOX_SOURCE_BYTES,
            )
            raise SettingsError(
                status_code=422,
                detail=(
                    "The stored sandbox document is larger than the "
                    f"{MAX_SANDBOX_SOURCE_BYTES} byte limit and was not written "
                    "through this api, so it is not served. Replace it with a "
                    "PATCH."
                ),
            )
        return source

    async def patch(
        self,
        update: ContentSource,
        if_match: Optional[str] = None,
        force: bool = False,
    ) -> Tuple[bytes, str]:
        """Write a content document under compare-and-swap. `force` waives the
        format-class check, which is inside the loop because elsewhere it races.
        """
        for _ in range(MAX_RETRIES):
            # bypass the read cache so a CAS retry sees the real current
            # revision; a failed write must not strip readers of their fallback.
            _cache_invalidate(self._cache_key, keep_lkg=True)
            doc = await self._read_uncached(for_write=True, seen=_current_epoch()) or {}
            # keep the raw stored value for the CAS filter and a normalised int
            # to write back, so a malformed revision stays matchable
            stored_revision = doc.get("revision", MISSING)
            stored_token = doc.get(CAS_TOKEN_FIELD, MISSING)
            revision = _as_int(stored_revision)

            if if_match is not None:
                # An editor holds the admin body's ETag and a reader the public
                # one; either is valid, both taken from the sanitized view.
                view = await sanitize_read_view(self._cache_key, doc) if doc else doc
                public_etag = etag_of(
                    serialise(self._public(view).model_dump()) if view else b"{}"
                )
                admin_etag = etag_of(
                    serialise(self._admin(view).model_dump()) if view else b"{}"
                )
                if not (
                    strong_if_match(if_match, public_etag, exists=bool(doc))
                    or strong_if_match(if_match, admin_etag, exists=bool(doc))
                ):
                    raise SettingsError(
                        status_code=412,
                        detail="The content changed since read; re-read and retry.",
                    )

            given = update.model_dump(exclude_unset=True)
            new_format = given.get("format", doc.get("format"))
            # An explicit source is authoritative (a null clears it); an
            # inherited one is the stored value and is validated below.
            if "source" in given:
                new_source: Any = given["source"] or ""
            else:
                # MISSING, not "": defaulting an absent source to "" makes a
                # metadata-only patch look like a source change and erase the html.
                new_source = doc.get("source", MISSING)
            new_title = given.get("title", doc.get("title", ""))

            if new_format is None:
                raise SettingsError(
                    status_code=422, detail="A content document needs a format."
                )
            if not isinstance(new_format, str) or (
                new_format not in VALID_CONTENT_FORMATS
            ):
                # The patch body's format is a Literal, so an invalid value was
                # inherited from a stored document this build cannot serve.
                raise SettingsError(
                    status_code=422,
                    detail=(
                        f"The stored document has format '{new_format}', which "
                        "this version does not support. Send an explicit "
                        "'format' with the patch to repair it."
                    ),
                )

            if doc and not force and "format" in given:
                stored_format = doc.get("format")
                was_sandbox = (
                    isinstance(stored_format, str) and stored_format == "sandbox-html"
                )
                now_sandbox = new_format == "sandbox-html"
                if was_sandbox != now_sandbox:
                    raise SettingsError(
                        status_code=409,
                        detail=(
                            f"This content is stored as {stored_format!r} and "
                            f"cannot be changed to {new_format!r}: that crosses "
                            "the rendered/sandbox boundary, which every "
                            "reference to it depends on - a sandbox document "
                            "cannot be inlined, and a rendered document has no "
                            "/document url. Delete and recreate the content "
                            "under this id, or pass force=true and then check "
                            "GET /api/freva-nextgen/settings/ui/contents/audit."
                        ),
                    )

            if not isinstance(new_source, str):
                # An inherited source is not guaranteed to be a string, or to be
                # there at all, and must be checked before source_hash sees it.
                if not doc:
                    # Nothing is stored, so nothing is damaged: the author
                    # simply did not send a source.
                    raise SettingsError(
                        status_code=422,
                        detail=(
                            "A source is required when creating a content "
                            "document. Send 'source' together with 'format'."
                        ),
                    )
                missing = isinstance(new_source, _Missing)
                raise SettingsError(
                    status_code=422,
                    detail=(
                        (
                            "The stored document has no 'source' at all"
                            if missing
                            else f"The stored source is {type(new_source).__name__}"
                            ", not text"
                        )
                        + ", so this patch cannot be applied on top of it. Send "
                        "an explicit 'source' to repair it - the stored rendering "
                        "has been left untouched."
                    ),
                )

            if new_title is None:
                # an explicit null clears the title; that is a valid edit
                new_title = ""
            if not isinstance(new_title, str):
                # Same defect as the source check: an inherited title may be a
                # truthy list/dict, and stringifying it stores a python repr.
                raise SettingsError(
                    status_code=422,
                    detail=(
                        f"The stored title is {type(new_title).__name__}, not "
                        "text, so this patch cannot be applied on top of it. "
                        "Send an explicit 'title' with the patch to replace it."
                    ),
                )

            _ensure_encodable(new_source, "source")
            _ensure_encodable(new_title, "title")

            # Whether this patch changes the rendering inputs, by effective
            # values, so an editor's full-document save does not re-render.
            source_changed = new_source != doc.get("source") or new_format != doc.get(
                "format"
            )
            new_hash = source_hash(new_source, new_format)
            if new_format in RENDERABLE_FORMATS and source_changed:
                try:
                    # Off the loop: rendering and sanitising are CPU-bound and
                    # would block every other coroutine in the worker.
                    rendered = await asyncio.to_thread(render, new_source, new_format)
                except (ValueError, RuntimeError) as error:
                    # atomic failure: stored document untouched
                    raise SettingsError(status_code=422, detail=str(error)) from error
                rendering = {
                    "rendered_html": rendered,
                    "source_hash": new_hash,
                    "rendered_hash": rendered_hash(rendered),
                    "renderer_version": RENDERER_FINGERPRINT,
                }
            elif new_format in RENDERABLE_FORMATS:
                # A metadata-only patch carries the stored rendering across
                # verbatim, so a title edit is never a silent renderer migration.
                rendering = {
                    key: doc[key]
                    for key in (
                        "rendered_html",
                        "source_hash",
                        "rendered_hash",
                        "renderer_version",
                    )
                    if key in doc
                }
            elif source_changed:
                # sandbox-html: no main-DOM rendering, source kept verbatim
                if len(new_source.encode("utf-8")) > MAX_SANDBOX_SOURCE_BYTES:
                    raise SettingsError(
                        status_code=422, detail="The source is too large."
                    )
                rendering = {
                    "rendered_html": "",
                    "source_hash": new_hash,
                    "rendered_hash": rendered_hash(""),
                    "renderer_version": RENDERER_FINGERPRINT,
                }
            else:
                # A metadata-only sandbox patch preserves its bookkeeping too:
                # a rename must not relabel an older fingerprint as current.
                rendering = {
                    key: doc[key]
                    for key in (
                        "rendered_html",
                        "source_hash",
                        "rendered_hash",
                        "renderer_version",
                    )
                    if key in doc
                }

            stored = {
                "_id": self._doc_id,
                "ui_id": self.ui_id,
                "content_id": self.content_id,
                "format": new_format,
                "source": new_source,
                **rendering,
                "title": new_title,
                "revision": revision + 1,
                CAS_TOKEN_FIELD: _new_cas_token(),
                "updated_at": _now(),
            }
            # `stored` goes to mongo raw; the response and the caches are built
            # from `view`, so a preserved rendering is never served unsanitised.
            view = await sanitize_read_view(self._cache_key, stored)
            body = serialise(self._public(view).model_dump())

            try:
                if not doc:
                    with mongo_timeout():
                        await self._collection.insert_one(stored)
                    _warn_forget(self._cache_key)
                    _cache_invalidate(self._cache_key)
                    _cache_put(self._cache_key, view)
                    return body, etag_of(body)
                predicate = _cas_predicate(self._key, stored_revision, stored_token)
                with mongo_timeout():
                    result = await self._collection.replace_one(predicate, stored)
                if result.matched_count:
                    _warn_forget(self._cache_key)
                    _cache_invalidate(self._cache_key)
                    _cache_put(self._cache_key, view)
                    return body, etag_of(body)
            except DuplicateKeyError:
                continue
            except (BSONError, OverflowError) as error:
                # Backstop for shapes BsonInt and MapKey do not cover: neither
                # error is a PyMongoError, so both would escape as a 500.
                raise SettingsError(
                    status_code=422,
                    detail=(
                        "The resolved document cannot be stored by MongoDB: "
                        f"{error}. Integers must fit in signed 64 bits and map "
                        "keys must not contain control characters."
                    ),
                ) from error
            except PyMongoError as error:
                logger.error("Could not update content: %s", error)
                raise SettingsError(
                    status_code=503,
                    detail="Could not reach the configuration database.",
                ) from error
        raise SettingsError(
            status_code=409,
            detail="The content is being updated concurrently, try again.",
        )

    async def cas_state(self) -> Optional[Dict[str, Any]]:
        """The identity of the stored document right now, or `None`; captured
        before a delete's reference scan so the removal is a compare-and-swap.
        """
        snapshot = await self.cas_snapshot()
        return None if snapshot is None else snapshot[1]

    async def cas_snapshot(self) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
        """The stored document and its CAS identity from a single read: separate
        reads could let `If-Match` pass on one generation and delete another.
        """
        doc = await self._read_uncached(for_write=True, seen=_current_epoch())
        self._reconcile_cache(doc)
        if not doc:
            return None
        return doc, {
            "revision": doc.get("revision", MISSING),
            CAS_TOKEN_FIELD: doc.get(CAS_TOKEN_FIELD, MISSING),
        }

    def _reconcile_cache(self, authoritative: Optional[Dict[str, Any]]) -> None:
        """Drop local cache entries this authoritative snapshot has disproved,
        last-known-good included; the snapshot never becomes a cached value.
        """
        # Every local entry, not just the short-lived read cache. Advance the
        # epoch first and unconditionally, so an in-flight read cannot refill.
        _bump_epoch()
        cached = _cache_get(self._cache_key)
        good = _last_known_good(self._cache_key)

        # The serialised body goes unconditionally: it holds no generation
        # identity to compare, and `get_public` reads it before anything else.
        _body_cache.pop(self._cache_key, None)

        if cached is None and good is None:
            return  # nothing else held locally, nothing to disprove

        def _agrees(held: Optional[Dict[str, Any]]) -> bool:
            if held is None:
                return True  # absent entries cannot disagree
            if _is_absent(held):
                return not authoritative
            if not authoritative:
                return False
            return all(
                held.get(field, MISSING) == authoritative.get(field, MISSING)
                for field in ("revision", CAS_TOKEN_FIELD)
            )

        if not (_agrees(cached) and _agrees(good)):
            _cache_invalidate(self._cache_key)

    async def read_etags(self, doc: Dict[str, Any]) -> Tuple[str, ...]:
        """Both strong etags a client may hold for `doc`, admin and public -
        computed from the sanitized read view, which is what a reader is served.
        """
        view = await sanitize_read_view(self._cache_key, doc)
        return (
            etag_of(serialise(self._public(view).model_dump())),
            etag_of(serialise(self._admin(view).model_dump())),
        )

    async def delete(self, expected: Optional[Dict[str, Any]] = None) -> str:
        """Delete the document, only if it still matches `expected` when given.
        Returns `"deleted"`, `"missing"` or `"changed"` for 204, 404, 409.
        """
        if expected is None:
            predicate: Dict[str, Any] = dict(self._key)
        else:
            predicate = _cas_predicate(
                self._key,
                expected.get("revision", MISSING),
                expected.get(CAS_TOKEN_FIELD, MISSING),
            )
        try:
            with mongo_timeout():
                result = await self._collection.delete_one(predicate)
            if result.deleted_count:
                _warn_forget(self._cache_key)
                _cache_invalidate(self._cache_key)
                return "deleted"
            if expected is None:
                _cache_invalidate(self._cache_key)
                return "missing"
            # Invalidate before the follow-up query: if that query raises, the
            # 503 path returns and the stale copy would survive. LKG waits.
            _cache_invalidate(self._cache_key, keep_lkg=True)
            # either it is gone, or it moved on to a generation we never validated
            with mongo_timeout():
                still_there = await self._collection.find_one(self._key, {"_id": 1})
            if still_there:
                # Nothing was removed, so the last-known-good copy stays: a
                # failed delete must not punish readers.
                return "changed"
            _cache_invalidate(self._cache_key)
            return "missing"
        except PyMongoError as error:
            logger.error("Could not delete %s: %s", self._cache_key, error)
            raise SettingsError(
                status_code=503,
                detail="Could not reach the configuration database.",
            ) from error


async def _html_drifts(doc: Dict[str, Any]) -> bool:
    """True when the stored html is not what the sanitizer would return; asked
    alongside `needs_rebuild`, which only checks stored fields."""
    html = doc.get("rendered_html")
    if doc.get("format") == "sandbox-html" or not isinstance(html, str) or not html:
        return False
    if len(html.encode("utf-8", "surrogatepass")) > MAX_RENDERED_BYTES:
        return True  # too big to serve, so certainly not what we would produce
    try:
        return bool(await asyncio.to_thread(sanitize_html, html) != html)
    except RuntimeError:  # pragma: no cover - nh3 missing
        return True


async def rebuild_stale_content(
    config: ServerConfig, after: Optional[str] = None
) -> Dict[str, Any]:
    """Re-render the content documents *this pass observes* as stale: bounded,
    resumable and idempotent, and a failing document is left untouched."""
    examined = rebuilt = failed = skipped = 0
    truncated = False
    last_id: Optional[str] = None
    # `is not None`, not truthiness: "" is a legal mongo _id, and treating it as
    # "no cursor" restarts the scan from the beginning forever.
    selector: Dict[str, Any] = {"_id": dict(STRING_ID_ONLY["_id"])}
    if after is not None:
        selector["_id"]["$gt"] = after
    cursor = config.mongo_collection_ui_contents.find(selector).sort("_id", 1)
    async for doc in bounded_cursor(cursor):
        if examined >= MAX_SCAN_DOCUMENTS:
            truncated = True
            logger.warning(
                "Rebuild stopped after %d documents; continue with after=%r.",
                examined,
                last_id,
            )
            break
        examined += 1
        # the selector guarantees a string, so the cursor is always usable
        last_id = str(doc.get("_id"))
        try:
            fmt = doc.get("format")
            # classification is inside the per-record boundary: an out-of-band
            # format=[] would otherwise abort the whole rebuild on TypeError.
            if not isinstance(fmt, str) or fmt not in RENDERABLE_FORMATS:
                if isinstance(fmt, str) and fmt == "sandbox-html":
                    continue  # sandbox-html has no rendered form
                raise ValueError(f"unusable stored format {fmt!r}")
            # Identity is validated inside the boundary and before the staleness
            # test, so a broken identity is counted rather than skipped silently.
            doc_id = doc.get("_id")
            ui_id = doc.get("ui_id")
            content_id = doc.get("content_id")
            if not isinstance(ui_id, str) or not isinstance(content_id, str):
                raise ValueError(
                    "stored document has no usable (ui_id, content_id) identity"
                )
            # Never synthesise an _id: it is immutable in mongo, so one that is
            # unusable or disagrees with the natural key is a per-record failure.
            expected_id = f"{ui_id}:{content_id}"
            if not isinstance(doc_id, str) or not doc_id:
                raise ValueError(f"stored document has an unusable _id {doc_id!r}")
            if doc_id != expected_id:
                raise ValueError(
                    f"stored _id {doc_id!r} disagrees with its natural key "
                    f"{expected_id!r}; an _id cannot be rewritten in place"
                )

            if not needs_rebuild(doc) and not await _html_drifts(doc):
                continue
            # MISSING, not "": an absent source defaulted to "" would erase the
            # stored html and clear is_stale, inventing a source we never found.
            source = doc.get("source", MISSING)
            if not isinstance(source, str):
                # Do NOT coerce: rendering str(source) and stamping it current
                # is a silent content rewrite masquerading as a repair.
                raise ValueError(
                    "stored source is missing entirely"
                    if isinstance(source, _Missing)
                    else f"stored source is {type(source).__name__}, not a string"
                )
            html = await asyncio.to_thread(render, source, fmt)

            seen_revision = doc.get("revision", MISSING)
            seen_token = doc.get(CAS_TOKEN_FIELD, MISSING)
            # Compare-and-swap on the revision and token this rebuild read, so a
            # newer write is not clobbered and a deleted generation is not revived.
            predicate = _cas_predicate({"_id": doc_id}, seen_revision, seen_token)
            # Bounded: this is the one write that runs inside a loop, and the
            # timeout is a PyMongoError the handler below re-raises as 503.
            with mongo_timeout():
                result = await config.mongo_collection_ui_contents.update_one(
                    predicate,
                    {
                        "$set": {
                            "rendered_html": html,
                            # rewritten from the source actually rendered, so a
                            # drifted hash comes out self-consistent
                            "source_hash": source_hash(source, fmt),
                            "rendered_hash": rendered_hash(html),
                            "renderer_version": RENDERER_FINGERPRINT,
                            "updated_at": _now(),
                            # normalise on the way out: $inc would fail on a
                            # non-numeric revision, and this is the repair path
                            "revision": _as_int(seen_revision) + 1,
                            CAS_TOKEN_FIELD: _new_cas_token(),
                        },
                    },
                )
            if result.matched_count:
                # The repair is the event that makes a fresh warning meaningful
                # again, so a document that re-breaks is reported.
                _warn_forget(f"content:{ui_id}:{content_id}")
                _cache_invalidate(f"content:{ui_id}:{content_id}")
                rebuilt += 1
            else:
                # A CAS miss means the document moved on since the cursor read
                # it. LKG is kept: nothing was written, so readers keep it.
                _cache_invalidate(f"content:{ui_id}:{content_id}", keep_lkg=True)
                # Whatever won the race is a generation this process has never
                # examined, so the old latch keys are freed too.
                _warn_forget(f"content:{ui_id}:{content_id}")
                skipped += 1  # a concurrent write moved on; leave it for next run
        except PyMongoError:
            # NOT a per-record failure: a database outage must not answer 200
            # with a tally of failures. Re-raised so the endpoint maps it to 503.
            raise
        except Exception as error:  # noqa: BLE001
            # Deliberately broad and per-record: this is the repair path for
            # already-damaged documents, so one bad record fails alone.
            logger.warning("Could not rebuild content %s: %s", doc.get("_id"), error)
            failed += 1
            continue
    report: Dict[str, Any] = {
        "examined": examined,
        "rebuilt": rebuilt,
        "failed": failed,
        "skipped": skipped,
        # 1/0 rather than a bool so the counts stay uniform; an operator needs to
        # see that the pass stopped early rather than infer it
        "truncated": int(truncated),
    }
    if truncated and last_id is not None:
        # the cursor to hand back to continue this scan
        report["next_after"] = last_id
    else:
        # Only once the resumable pass has finished: a document whose _id is not
        # a string cannot be reached by a string cursor and stays invisible.
        report["malformed_ids"] = await _count_untyped_ids(config)
    return report


async def _count_untyped_ids(config: ServerConfig) -> int:
    """How many content documents a resumable scan can never reach; kept out of
    `failed`, where it would let `failed` exceed `examined`."""
    collection = config.mongo_collection_ui_contents
    selector = {"_id": {"$not": {"$type": "string"}}}
    with mongo_timeout():
        total = int(await collection.count_documents(selector))
    if not total:
        return 0
    logger.error(
        "%d content document(s) have a non-string _id, so they cannot have been "
        "written through this api and cannot be rebuilt. An _id is immutable: "
        "copy each to a correct id and remove the original.",
        total,
    )
    shown = 0
    cursor = collection.find(selector)
    async for doc in bounded_cursor(cursor):
        if shown >= UNTYPED_ID_SAMPLE:
            break
        shown += 1
        logger.error("  unreachable content document _id=%r", doc.get("_id"))
    return total
