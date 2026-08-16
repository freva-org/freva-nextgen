"""
Reusable field types for the settings resources.
"""

import re
from datetime import datetime
from typing import Annotated, Any, List, Union

from pydantic import BeforeValidator, Field, StringConstraints

__all__ = [
    "Hex",
    "Rfc3339",
    "PlainText",
    "LongText",
    "Markdown",
    "Rst",
    "Number",
    "Url",
    "BsonInt",
    "MapKey",
    "FiniteFloat",
    "ExtensionValue",
    "SAFE_URL_SCHEMES",
]

# colour

Hex = Annotated[
    str,
    StringConstraints(pattern=r"^#[0-9a-fA-F]{6}$"),
    Field(json_schema_extra={"x-widget": "colour"}),
]
"""A `#rrggbb` colour. `fullmatch` semantics reject a trailing newline that
would otherwise reach a css context."""


# text

PlainText = Annotated[
    str,
    StringConstraints(max_length=4096, pattern=r"^[^\r\n]*$"),
    Field(json_schema_extra={"x-widget": "text"}),
]
"""A single line of text: the pattern rejects CR and LF. A model refines the
length with `Field(max_length=...)` where a tighter bound matters."""

LongText = Annotated[
    str,
    StringConstraints(max_length=32_000),
    Field(json_schema_extra={"x-widget": "textarea"}),
]
"""Several lines of unformatted text. Never parsed, so it can emit no markup."""


# rich text

Markdown = Annotated[
    str,
    StringConstraints(max_length=32_000),
    Field(json_schema_extra={"x-widget": "richtext", "x-format": "markdown"}),
]
"""Markdown source. Rendered by the content pipeline, then sanitised."""

Rst = Annotated[
    str,
    StringConstraints(max_length=32_000),
    Field(json_schema_extra={"x-widget": "richtext", "x-format": "rst"}),
]
"""reStructuredText source. Rendered by the content pipeline, then sanitised."""


# number

Number = Annotated[int, Field(json_schema_extra={"x-widget": "number"})]
"""A whole number. A model adds `Field(ge=..., le=...)` for its own bounds."""


# url

SAFE_URL_SCHEMES = ("http", "https")
"""The only schemes a url field may carry. A url ends up in an `href` or a
`src`; `javascript:` there is stored cross-site scripting, and
`data:`/`vbscript:`/`file:` are no better. A relative url has no scheme and
stays allowed."""


_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
"""ASCII C0 controls and DEL. The WHATWG url parser removes tab, CR and LF
anywhere in a url before it looks at the scheme, so `java\\nscript:alert(1)`
is parsed by a browser as `javascript:alert(1)` while a scheme regex on the raw
string sees `java\\nscript`. They are refused rather than normalised away, so
the admin learns their value was wrong instead of having it quietly changed."""


def _check_url(value: str) -> str:
    """
    Reject a url whose scheme could turn an `href` into script or point off
    to an attacker-controlled origin.
    """
    if not value:
        return value
    if _CONTROL_CHARS.search(value):
        raise ValueError(
            "Control characters are not allowed in a url. A browser removes "
            "tab, carriage return and newline before it parses the scheme, so "
            "a value like 'java\\nscript:alert(1)' would become an executable "
            "'javascript:' link. Remove them."
        )
    stripped = value.strip()
    # normalise backslashes the way a lenient browser would, for the check only
    probe = stripped.replace("\\", "/")
    if probe.startswith("//"):
        raise ValueError(
            "Protocol-relative urls (starting with '//') are not allowed; a "
            "browser reads them as an off-site https load. Use an explicit "
            "https:// url or a relative path."
        )
    scheme = re.match(r"^([a-zA-Z][a-zA-Z0-9+.-]*):", stripped)
    if scheme and scheme.group(1).lower() not in SAFE_URL_SCHEMES:
        allowed = ", ".join(s + ":" for s in SAFE_URL_SCHEMES)
        raise ValueError(
            f"'{scheme.group(1)}:' urls are not allowed, only {allowed} and "
            "relative urls. The value ends up in an href or src attribute."
        )
    return value


Url = Annotated[
    str,
    StringConstraints(max_length=2048),
    Field(json_schema_extra={"x-widget": "url"}),
]
"""A url for an `href` or `src`. The model applies `_check_url` as a
validator; this alias carries the length bound and the widget hint."""


# open-map value

BSON_INT64_MIN = -(2**63)
BSON_INT64_MAX = 2**63 - 1
"""Mongo stores integers as signed 64-bit. Python's are arbitrary precision, so
`2**63` is an ordinary value that both pydantic and `json.dumps` accept and
the BSON encoder then refuses with `OverflowError`."""

BsonInt = Annotated[int, Field(ge=BSON_INT64_MIN, le=BSON_INT64_MAX)]
"""An integer mongo can actually store. Rejected at the model boundary with a
422 naming the field, rather than at encode time with a 500."""


def _reject_unstorable_int(value: Any) -> Any:
    """
    Refuse an integer outside BSON's signed 64-bit range.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not BSON_INT64_MIN <= value <= BSON_INT64_MAX:
        raise ValueError(
            f"{value} is outside the range MongoDB can store "
            f"({BSON_INT64_MIN} to {BSON_INT64_MAX}); it would otherwise be "
            "silently converted to a float and lose precision."
        )
    return value


MapKey = Annotated[
    str,
    StringConstraints(
        min_length=1, max_length=128, pattern=r"^[^\x00-\x1f\x7f.$][^\x00-\x1f\x7f.]*$"
    ),
]
"""A key in a user-controlled map."""

FiniteFloat = Annotated[float, Field(allow_inf_nan=False)]
"""A float that is a real number. `inf` and `nan` are excluded because json
has no way to spell them: `json.dumps` emits the bare tokens `Infinity` and
`NaN`, which every browser's `JSON.parse` rejects, so accepting `1e999`
would let one admin edit make the settings response unparsable for everyone."""

ExtensionValue = Annotated[
    Union[bool, BsonInt, FiniteFloat, str, List[str]],
    BeforeValidator(_reject_unstorable_int),
]
"""A value in the `public_extensions` open map: a scalar or a list of strings.
World-readable, so the field name warns; prefer a typed field."""


_RFC3339_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"  # full-date
    r"[Tt]"  # RFC 3339 allows lower case; nothing else separates
    r"\d{2}:\d{2}:\d{2}"  # partial-time, seconds required
    r"(\.\d{1,9})?"  # optional fraction, bounded - see _six_digit_fraction
    r"([Zz]|[+-]\d{2}:\d{2})$"  # time-offset, minutes only
)
"""RFC 3339 #5.6 `date-time`, and nothing else."""

MAX_RFC3339_LENGTH = 40
"""Bound on the raw string, checked before parsing."""


_FRACTION_RE = re.compile(r"\.(\d{1,9})")
"""The fractional-seconds group of a value the grammar has already matched."""

MICROSECOND_DIGITS = 6


def _six_digit_fraction(value: str) -> str:
    """
    Rewrite the fraction to exactly six digits, or leave the value alone.
    """

    def _pad(match: "re.Match[str]") -> str:
        digits = match.group(1)
        return "." + digits[:MICROSECOND_DIGITS].ljust(MICROSECOND_DIGITS, "0")

    return _FRACTION_RE.sub(_pad, value, count=1)


def _parse_rfc3339(value: Any) -> Any:
    """
    Normalise an RFC 3339 timestamp, or refuse it.
    """
    if value is None or not isinstance(value, str):
        return value
    if len(value) > MAX_RFC3339_LENGTH:
        raise ValueError(
            f"A timestamp may not be longer than {MAX_RFC3339_LENGTH} characters."
        )
    if not _RFC3339_RE.match(value):
        raise ValueError(
            f"'{value}' is not an RFC 3339 timestamp. Use a form like "
            "'2026-05-01T09:00:00+00:00' or '2026-05-01T09:00:00Z' - a full "
            "date, a 'T', a time with seconds, and an explicit offset."
        )
    text = _six_digit_fraction(value)
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        # the grammar matched, so this is a real calendar error (month 13, the
        # 31st of February) rather than a spelling one
        raise ValueError(f"'{value}' is not a real date and time.") from None
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        raise ValueError(
            f"'{value}' has no timezone. An announcement window is evaluated by "
            "every client in its own locale, so the offset has to be explicit."
        )
    return parsed.isoformat()


Rfc3339 = Annotated[
    str,
    BeforeValidator(_parse_rfc3339),
    Field(
        max_length=64,
        json_schema_extra={"format": "date-time"},
    ),
]
"""An RFC 3339 timestamp with an explicit offset, normalised to one spelling."""
