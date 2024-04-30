"""Various utilities for the restAPI."""

import os
from typing import Optional

import redis


def str_to_int(inp_str: Optional[str], default: int) -> int:
    """Convert an integer from a string. If it's not working return default."""
    try:
        return int(inp_str)
    except (ValueError, TypeError):
        return default


REDIS_HOST, _, REDIS_PORT = (
    (os.environ.get("REDIS_HOST") or "localhost").replace("redis://", "").partition(":")
)


RedisCache = redis.Redis(host=REDIS_HOST, port=str_to_int(REDIS_PORT, 6379), db=0)
