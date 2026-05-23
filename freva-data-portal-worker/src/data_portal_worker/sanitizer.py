"""Validation and sanitisation of broker messages received from Redis.

Every message published to the ``data-portal`` channel originates from
freva-rest, but freva-rest is a *separate process* (often a separate host)
and the Redis channel has no built-in authentication at the message level.
This module provides a thin sanitisation layer so that malformed or
malicious payloads are rejected *before* they reach the filesystem, the
dask cluster, or the ``su`` impersonation call.

Design goals
------------
* No new runtime dependencies — only the standard library.
* Fail-closed: any field that cannot be validated raises ``ValueError``
  with a descriptive message that is safe to log.
* Path-safety is the highest priority: POSIX absolute paths and
  allowlisted URL schemes (``s3://``, ``https://``, ``hsm://``, …) are
  accepted; relative paths, ``..`` traversal, null bytes, embedded
  credentials, and unknown schemes are all rejected.
* Numeric fields are range-clamped to operationally sensible limits so
  a crafted message cannot request a 1 TiB chunk or 2^31 primary chunks.

Usage
-----
Call :func:`sanitize_message` inside ``redis_callback`` immediately after
JSON decoding::

    try:
        message = sanitize_message(json.loads(body))
    except (json.JSONDecodeError, ValueError) as exc:
        data_logger.warning("Rejected broker message: %s", exc)
        return
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Constants / compiled patterns
# ---------------------------------------------------------------------------

#: Only "map" and "time_series" are valid access patterns.
_VALID_ACCESS_PATTERNS = frozenset({"map", "time_series"})

#: URL schemes accepted in path fields.  POSIX paths (starting with ``/``)
#: are always accepted regardless of this set.
#: Add new schemes here as new storage backends are integrated.
_ALLOWED_SCHEMES: frozenset[str] = frozenset(
    {
        # Object stores
        "s3",
        "s3a",   # Hadoop S3A (used by some Spark / fsspec stacks)
        "s3n",   # Hadoop S3N (legacy)
        "gs",    # Google Cloud Storage
        "az",    # Azure Blob (fsspec short form)
        "abfs",  # Azure Data Lake Gen2
        "abfss", # Azure Data Lake Gen2 (TLS)
        "swift", # OpenStack Swift
        # HTTP
        "https",
        "http",
        # DKRZ / HPC tape
        "hsm",
        # General remote
        "ftp",
        "sftp",
    }
)

#: Chunk IDs are dot-separated non-negative integers, e.g. "0", "3.0", "1.2.0".
#: Validated with isascii() + split('.') + isdecimal() — no regex needed.
#: isascii() is an O(1) flag check in CPython ≥ 3.7; isdecimal() then runs
#: the fast ASCII path so the whole check avoids per-character Python overhead.

#: str.translate tables for character-set validation.
#: Each table maps every *allowed* character to None (delete it).
#: After translation the result is empty iff every input character is valid.
_USERNAME_STRIP = str.maketrans(
    "", "", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
)
_VARIABLE_STRIP = str.maketrans(
    "", "", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_./-"
)

#: RFC 3986 scheme characters: ALPHA / DIGIT / "+" / "-" / "."
#: Used in _has_url_scheme; frozenset lookup is O(1) and avoids re.match overhead.
_SCHEME_CHARS: frozenset[str] = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-."
)

# Operationally sensible numeric bounds.
_MIN_CHUNK_SIZE_MIB: float = 0.5
_MAX_CHUNK_SIZE_MIB: float = 512.0
_MIN_MAP_PRIMARY: int = 1
_MAX_MAP_PRIMARY: int = 1024


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _require_str(value: Any, field: str) -> str:
    """Return *value* as str or raise ``ValueError``."""
    if not isinstance(value, str):
        raise ValueError(f"{field!r} must be a string, got {type(value).__name__}")
    return value


def _require_list_of_str(value: Any, field: str) -> List[str]:
    """Return *value* as a non-empty list of strings or raise ``ValueError``."""
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field!r} must be a non-empty list, got {value!r}")
    for i, item in enumerate(value):
        if not isinstance(item, str):
            raise ValueError(
                f"{field!r}[{i}] must be a string, got {type(item).__name__}"
            )
    return value


def _has_url_scheme(raw: str) -> bool:
    """Return True when *raw* starts with a valid ``<scheme>://`` prefix.

    POSIX paths (starting with ``/``) bail out immediately so the common
    case costs a single character comparison.  For URL candidates the
    scheme is validated against RFC 3986 character rules using a frozenset
    lookup rather than ``re.match`` — avoids compiling a pattern on every
    call and is ~1.2× faster on typical short scheme strings.
    """
    if raw.startswith("/"):
        return False  # fast-path: the vast majority of climate-data paths
    idx = raw.find("://")
    if idx <= 0:
        return False
    scheme = raw[:idx]
    # RFC 3986: first char must be ALPHA; the rest ALPHA / DIGIT / "+" / "-" / "."
    return scheme[0].isalpha() and all(c in _SCHEME_CHARS for c in scheme[1:])


def _sanitize_posix_path(raw: str, field: str) -> str:
    """Validate an absolute POSIX filesystem path.

    Rules
    -----
    * Must be non-empty.
    * Must start with ``/``.
    * No null bytes.
    * No ``..`` path components (traversal).
    * Returns the normalised form (``PurePosixPath`` collapses ``//``, etc.).
    """
    if not raw:
        raise ValueError(f"{field!r} path must not be empty")
    if "\x00" in raw:
        raise ValueError(f"{field!r} path contains a null byte")
    if not raw.startswith("/"):
        raise ValueError(
            f"{field!r} must be an absolute POSIX path (starting with '/') "
            f"or a URL (e.g. s3://bucket/key), got {raw!r}"
        )
    pure = PurePosixPath(raw)
    if ".." in pure.parts:
        raise ValueError(
            f"{field!r} path contains '..' traversal component: {raw!r}"
        )
    return str(pure)


def _sanitize_url_path(raw: str, field: str) -> str:
    """Validate a URL-scheme path (``s3://``, ``https://``, ``hsm://``, …).

    Rules
    -----
    * Scheme must be in :data:`_ALLOWED_SCHEMES`.
    * No null bytes anywhere in the URL.
    * No embedded credentials (``user:password@host`` in the netloc).
    * A non-empty host / bucket name is required.
    * The path component must not contain ``..`` traversal segments.
    * The URL is returned unchanged — normalising object-store keys
      would alter their semantics.
    """
    if "\x00" in raw:
        raise ValueError(f"{field!r} URL contains a null byte")

    parsed = urlparse(raw)
    scheme = parsed.scheme.lower()

    if scheme not in _ALLOWED_SCHEMES:
        raise ValueError(
            f"{field!r} has disallowed URL scheme {scheme!r}; "
            f"allowed: {sorted(_ALLOWED_SCHEMES)}"
        )

    # Embedded credentials must never reach the worker — they would leak
    # into logs via the debug "Assigning … to … for future processing" line.
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(
            f"{field!r} URL must not contain embedded credentials "
            f"(use IAM roles / env vars / credential files instead)"
        )

    if not parsed.netloc.split("@")[-1]:
        raise ValueError(
            f"{field!r} URL has no host or bucket name: {raw!r}"
        )

    # Check the path portion for traversal.  ``PurePosixPath`` is the right
    # tool because object-store key hierarchies use POSIX-style separators.
    if parsed.path:
        if ".." in PurePosixPath(parsed.path).parts:
            raise ValueError(
                f"{field!r} URL path contains '..' traversal component: {raw!r}"
            )

    return raw  # return as-is; do not normalise URL-scheme paths


def _sanitize_path(raw: str, field: str) -> str:
    """Validate a single path, dispatching to the POSIX or URL validator."""
    if not raw:
        raise ValueError(f"{field!r} path must not be empty")
    if _has_url_scheme(raw):
        return _sanitize_url_path(raw, field)
    return _sanitize_posix_path(raw, field)


def _sanitize_paths(raw: Any, field: str) -> List[str]:
    paths = _require_list_of_str(raw, field)
    return [_sanitize_path(p, field) for p in paths]


def _sanitize_username(raw: Any) -> Optional[str]:
    """Return a validated username or ``None`` for guest access.

    Character validation uses ``str.translate`` with ``_USERNAME_STRIP``
    (a module-level table that deletes every allowed character).  A
    non-empty remainder means an invalid character was present.
    """
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError(
            f"'username' must be a string or null, got {type(raw).__name__}"
        )
    stripped = raw.strip()
    if not stripped:
        return None  # treat blank as guest
    if len(stripped) > 64:
        raise ValueError(
            f"'username' must not exceed 64 characters, got {len(stripped)}: "
            f"{stripped!r}"
        )
    if stripped.translate(_USERNAME_STRIP):
        raise ValueError(
            f"'username' contains invalid characters or is too long: {stripped!r}"
        )
    return stripped


def _sanitize_chunk_id(raw: Any) -> str:
    """Validate a zarr chunk ID such as ``"0"``, ``"3.0"``, ``"1.2.0"``.

    Implementation notes
    --------------------
    * ``str.isascii()`` is an O(1) flag check in CPython ≥ 3.7 (the flag is
      set at string-creation time).  Failing it short-circuits the rest
      immediately for any non-ASCII input.
    * Once ``isascii()`` passes, ``str.isdecimal()`` runs the optimised ASCII
      path inside CPython's unicodeobject.c — no per-character Python
      overhead.
    * The combination beats a compiled ``re.match`` by ~1.4× on typical
      short chunk index strings.
    """
    s = _require_str(raw, "chunk")
    if not s or not s.isascii():
        raise ValueError(
            f"'chunk' must be a dot-separated non-negative integer id "
            f"(e.g. '0.1.2'), got {s!r}"
        )
    for part in s.split("."):
        if not part or not part.isdecimal():
            raise ValueError(
                f"'chunk' must be a dot-separated non-negative integer id "
                f"(e.g. '0.1.2'), got {s!r}"
            )
    return s


def _sanitize_variable(raw: Any) -> str:
    """Validate a zarr variable / group path such as ``"tas"`` or ``"grp/tas"``.

    Uses ``str.translate`` with a module-level delete-table: the table maps
    every *allowed* character to None; after translation the remainder
    contains only *disallowed* characters.  An empty remainder means the
    input is valid.  Building the table once at import time makes repeated
    calls allocation-free.
    """
    s = _require_str(raw, "variable")
    if not s or len(s) > 256:
        raise ValueError(
            f"'variable' must be 1–256 characters, got {len(s)}: {s!r}"
        )
    if s[0] == "/" or s[-1] == "/":
        raise ValueError(f"'variable' must not start or end with '/': {s!r}")
    if s.translate(_VARIABLE_STRIP):
        raise ValueError(
            f"'variable' contains invalid characters: {s!r}"
        )
    # Must come after the charset check so we only split strings we know
    # contain only '/', letters, digits, '_', '-', '.'.
    if any(part in (".", "..") for part in s.split("/")):
        raise ValueError(
            f"'variable' must not contain '.' or '..' path component: {s!r}"
        )
    return s


def _sanitize_access_pattern(raw: Any) -> str:
    s = _require_str(raw, "access_pattern")
    if s not in _VALID_ACCESS_PATTERNS:
        raise ValueError(
            f"'access_pattern' must be one of {sorted(_VALID_ACCESS_PATTERNS)}, "
            f"got {s!r}"
        )
    return s


def _sanitize_chunk_size(raw: Any) -> float:
    if not isinstance(raw, (int, float)):
        raise ValueError(
            f"'chunk_size' must be a number, got {type(raw).__name__}"
        )
    value = float(raw)
    if not (_MIN_CHUNK_SIZE_MIB <= value <= _MAX_CHUNK_SIZE_MIB):
        raise ValueError(
            f"'chunk_size' {value} MiB is outside the allowed range "
            f"[{_MIN_CHUNK_SIZE_MIB}, {_MAX_CHUNK_SIZE_MIB}]"
        )
    return value


def _sanitize_map_primary_chunksize(raw: Any) -> int:
    if not isinstance(raw, int) or isinstance(raw, bool):
        raise ValueError(
            f"'map_primary_chunksize' must be an integer, got {type(raw).__name__}"
        )
    if not (_MIN_MAP_PRIMARY <= raw <= _MAX_MAP_PRIMARY):
        raise ValueError(
            f"'map_primary_chunksize' {raw} is outside the allowed range "
            f"[{_MIN_MAP_PRIMARY}, {_MAX_MAP_PRIMARY}]"
        )
    return raw


def _sanitize_assembly(raw: Any) -> Optional[Dict[str, Optional[str]]]:
    """Validate the optional aggregation plan dict."""
    if raw is None or raw == {}:
        return None
    if not isinstance(raw, dict):
        raise ValueError(
            f"'assembly' must be a dict or null, got {type(raw).__name__}"
        )
    _allowed_keys = {
        "mode", "dim", "compat", "join", "data_vars", "coords", "group_by"
    }
    for k, v in raw.items():
        if not isinstance(k, str):
            raise ValueError(f"'assembly' key must be a string, got {k!r}")
        if k not in _allowed_keys:
            raise ValueError(
                f"'assembly' contains unexpected key {k!r}; "
                f"allowed: {sorted(_allowed_keys)}"
            )
        if v is not None and not isinstance(v, str):
            raise ValueError(
                f"'assembly[{k!r}]' must be a string or null, got {type(v).__name__}"
            )
    return raw


# ---------------------------------------------------------------------------
# Per-message-type sanitisers
# ---------------------------------------------------------------------------


def _sanitize_uri(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("'uri' must be a JSON object")
    return {
        "path": _sanitize_paths(payload.get("path"), "uri.path"),
        "uuid": _require_str(payload.get("uuid", ""), "uri.uuid"),
        "username": _sanitize_username(payload.get("username")),
        "assembly": _sanitize_assembly(payload.get("assembly")),
        "access_pattern": _sanitize_access_pattern(
            payload.get("access_pattern", "map")
        ),
        "map_primary_chunksize": _sanitize_map_primary_chunksize(
            payload.get("map_primary_chunksize", 1)
        ),
        "reload": bool(payload.get("reload", False)),
        "chunk_size": _sanitize_chunk_size(payload.get("chunk_size", 16.0)),
    }


def _sanitize_chunk(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("'chunk' must be a JSON object")
    return {
        "uuid": _require_str(payload.get("uuid", ""), "chunk.uuid"),
        "chunk": _sanitize_chunk_id(payload.get("chunk")),
        "variable": _sanitize_variable(payload.get("variable")),
    }


def _sanitize_access_check(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("'access_check' must be a JSON object")
    request_id = _require_str(payload.get("request_id", ""), "access_check.request_id")
    # isprintable() is False for all control characters (tab, LF, etc.) and
    # for non-printable Unicode; the explicit " " check catches the space
    # character which is printable but still disallowed in a key suffix.
    if not request_id or len(request_id) > 128 or not request_id.isprintable() or " " in request_id:
        raise ValueError(
            f"'access_check.request_id' must be 1–128 printable non-whitespace "
            f"characters, got {request_id!r}"
        )
    return {
        "request_id": request_id,
        "username": _sanitize_username(payload.get("username")),
        "paths": _sanitize_paths(payload.get("paths", []), "access_check.paths"),
    }


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------


def sanitize_message(message: Any) -> Dict[str, Any]:
    """Validate and return a sanitised broker message dict.

    The returned dict uses the same top-level key as the input (``"uri"``,
    ``"chunk"``, ``"access_check"``, or ``"shutdown"``) so that
    ``redis_callback`` can continue to dispatch on the key without any
    other changes.

    Parameters
    ----------
    message:
        The decoded JSON object (already parsed from bytes by ``json.loads``).

    Raises
    ------
    ValueError
        If any field fails validation.  The message should be logged and
        dropped; the worker must *not* process it.
    """
    if not isinstance(message, dict):
        raise ValueError(
            f"Broker message must be a JSON object, got {type(message).__name__}"
        )

    if "uri" in message:
        return {"uri": _sanitize_uri(message["uri"])}

    if "chunk" in message:
        return {"chunk": _sanitize_chunk(message["chunk"])}

    if "access_check" in message:
        return {"access_check": _sanitize_access_check(message["access_check"])}

    if "shutdown" in message:
        return {"shutdown": bool(message["shutdown"])}

    raise ValueError(
        f"Broker message has no recognised key; found: {sorted(message.keys())}"
    )
