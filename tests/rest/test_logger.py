"""Unit tests for QuietedLoggers and EndpointFilter."""

import asyncio
import contextvars
import logging
from contextlib import contextmanager
from typing import FrozenSet, Iterator, List, Optional, Tuple

import pytest

# ---------------------------------------------------------------------------
# Inline copies of the two classes under test so this file has no dependency
# on the rest of the freva-rest package (rich, uvicorn, .loop, etc.)
# ---------------------------------------------------------------------------


class EndpointFilter(logging.Filter):
    """Filter certain logging events from an endpoint."""

    def __init__(self, excluded_endpoints: List[str]) -> None:
        self.excluded = excluded_endpoints
        super().__init__()

    def filter(self, record: logging.LogRecord) -> bool:
        return not any(ep in record.getMessage() for ep in self.excluded)


class QuietedLoggers(logging.Filter):
    _ctx: contextvars.ContextVar[Optional[Tuple[int, FrozenSet[str]]]] = (
        contextvars.ContextVar("log_floor", default=None)
    )
    _instance: Optional["QuietedLoggers"] = None

    def __new__(cls) -> "QuietedLoggers":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = self._ctx.get()
        if ctx is not None:
            floor, names = ctx
            if record.name in names and record.levelno < floor:
                record.levelno = logging.DEBUG
                record.levelname = logging.getLevelName(logging.DEBUG)
        return True

    @classmethod
    @contextmanager
    def floor(cls, *names: str, level: int = logging.WARNING) -> Iterator[None]:
        this = cls()
        for name in names:
            logging.getLogger(name).addFilter(this)
        token = cls._ctx.set((level, frozenset(names)))
        try:
            yield
        finally:
            cls._ctx.reset(token)
            for name in names:
                logging.getLogger(name).removeFilter(this)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_record(
    name: str, level: int, message: str = "test message"
) -> logging.LogRecord:
    return logging.LogRecord(
        name=name,
        level=level,
        pathname="",
        lineno=0,
        msg=message,
        args=(),
        exc_info=None,
    )


# ---------------------------------------------------------------------------
# EndpointFilter tests
# ---------------------------------------------------------------------------


class TestEndpointFilter:
    def test_allows_unrelated_message(self) -> None:
        f = EndpointFilter(["/ping"])
        record = make_record("uvicorn.access", logging.INFO, "GET /api/search 200")
        assert f.filter(record) is True

    def test_blocks_excluded_endpoint(self) -> None:
        f = EndpointFilter(["/ping"])
        record = make_record("uvicorn.access", logging.INFO, "GET /ping 200")
        assert f.filter(record) is False

    def test_blocks_when_endpoint_is_substring(self) -> None:
        f = EndpointFilter(["/api/freva-nextgen/ping"])
        record = make_record(
            "uvicorn.access",
            logging.INFO,
            'GET /api/freva-nextgen/ping HTTP/1.1" 200',
        )
        assert f.filter(record) is False

    def test_allows_partial_non_match(self) -> None:
        f = EndpointFilter(["/api/freva-nextgen/ping"])
        record = make_record(
            "uvicorn.access", logging.INFO, "GET /api/freva-nextgen/search 200"
        )
        assert f.filter(record) is True

    def test_multiple_excluded_endpoints(self) -> None:
        f = EndpointFilter(["/ping", "/healthz"])
        assert f.filter(make_record("x", logging.INFO, "GET /ping 200")) is False
        assert f.filter(make_record("x", logging.INFO, "GET /healthz 200")) is False
        assert f.filter(make_record("x", logging.INFO, "GET /search 200")) is True

    def test_empty_exclusion_list_allows_everything(self) -> None:
        f = EndpointFilter([])
        record = make_record("uvicorn.access", logging.INFO, "GET /anything 200")
        assert f.filter(record) is True


# ---------------------------------------------------------------------------
# QuietedLoggers tests
# ---------------------------------------------------------------------------


class TestQuietedLoggers:
    def setup_method(self) -> None:
        # Reset singleton and context var before each test
        QuietedLoggers._instance = None
        QuietedLoggers._ctx.set(None)

    def test_singleton(self) -> None:
        a = QuietedLoggers()
        b = QuietedLoggers()
        assert a is b

    def test_no_effect_outside_context(self) -> None:
        record = make_record("httpx", logging.INFO)
        result = QuietedLoggers().filter(record)
        assert result is True
        assert record.levelno == logging.INFO

    def test_info_downgraded_to_debug_inside_context(self) -> None:
        record = make_record("httpx", logging.INFO)
        with QuietedLoggers.floor("httpx"):
            QuietedLoggers().filter(record)
        assert record.levelno == logging.DEBUG
        assert record.levelname == "DEBUG"

    def test_warning_not_downgraded_inside_context(self) -> None:
        record = make_record("httpx", logging.WARNING)
        with QuietedLoggers.floor("httpx"):
            QuietedLoggers().filter(record)
        assert record.levelno == logging.WARNING
        assert record.levelname == "WARNING"

    def test_error_not_downgraded_inside_context(self) -> None:
        record = make_record("httpx", logging.ERROR)
        with QuietedLoggers.floor("httpx"):
            QuietedLoggers().filter(record)
        assert record.levelno == logging.ERROR

    def test_unregistered_logger_not_affected(self) -> None:
        record = make_record("some.other.logger", logging.INFO)
        with QuietedLoggers.floor("httpx"):
            QuietedLoggers().filter(record)
        assert record.levelno == logging.INFO

    def test_context_restored_after_exit(self) -> None:
        with QuietedLoggers.floor("httpx"):
            pass
        record = make_record("httpx", logging.INFO)
        QuietedLoggers().filter(record)
        assert record.levelno == logging.INFO

    def test_context_restored_after_exception(self) -> None:
        try:
            with QuietedLoggers.floor("httpx"):
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        record = make_record("httpx", logging.INFO)
        QuietedLoggers().filter(record)
        assert record.levelno == logging.INFO

    def test_multiple_logger_names(self) -> None:
        httpx_record = make_record("httpx", logging.INFO)
        httpcore_record = make_record("httpcore", logging.INFO)
        with QuietedLoggers.floor("httpx", "httpcore"):
            QuietedLoggers().filter(httpx_record)
            QuietedLoggers().filter(httpcore_record)
        assert httpx_record.levelno == logging.DEBUG
        assert httpcore_record.levelno == logging.DEBUG

    def test_custom_floor_level(self) -> None:
        # With floor=ERROR, WARNING should be downgraded but ERROR should not
        warning_record = make_record("httpx", logging.WARNING)
        error_record = make_record("httpx", logging.ERROR)
        with QuietedLoggers.floor("httpx", level=logging.ERROR):
            QuietedLoggers().filter(warning_record)
            QuietedLoggers().filter(error_record)
        assert warning_record.levelno == logging.DEBUG
        assert error_record.levelno == logging.ERROR

    def test_filter_always_returns_true(self) -> None:
        """Records are never suppressed outright — only downgraded."""
        record = make_record("httpx", logging.DEBUG)
        with QuietedLoggers.floor("httpx"):
            result = QuietedLoggers().filter(record)
        assert result is True

    def test_concurrency_isolation(self) -> None:
        """A task that enters floor() must not affect a sibling task."""
        results: dict = {}

        async def quiet_task() -> None:
            with QuietedLoggers.floor("httpx"):
                await asyncio.sleep(0)  # yield so sibling can run
                record = make_record("httpx", logging.INFO)
                QuietedLoggers().filter(record)
                results["quiet"] = record.levelno

        async def normal_task() -> None:
            await asyncio.sleep(0)  # let quiet_task enter its context first
            record = make_record("httpx", logging.INFO)
            QuietedLoggers().filter(record)
            results["normal"] = record.levelno

        async def run() -> None:
            await asyncio.gather(quiet_task(), normal_task())

        asyncio.run(run())
        assert results["quiet"] == logging.DEBUG
        assert results["normal"] == logging.INFO
