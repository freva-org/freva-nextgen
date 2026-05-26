"""Define the asyncio loop."""

import asyncio
from functools import lru_cache
from types import ModuleType


@lru_cache(maxsize=None)
def get_async_model() -> ModuleType:
    """Get the asyncio module. Prefer uvloop over asyncio if installed."""
    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        return uvloop
    except ImportError:
        return asyncio  # pragma: no cover
