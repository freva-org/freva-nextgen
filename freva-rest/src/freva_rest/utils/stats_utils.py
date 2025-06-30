"""Utility functions for storing API statistics."""

import asyncio
from datetime import datetime
from functools import wraps
from typing import Any, Awaitable, Callable, Coroutine, Dict, Optional

from freva_rest.config import ServerConfig
from freva_rest.logger import logger


def ensure_future(
    async_func: Callable[..., Awaitable[Any]]
) -> Callable[..., Coroutine[Any, Any, asyncio.Task[Any]]]:
    """Decorator that runs any given asyncio function in the background."""

    @wraps(async_func)
    async def wrapper(*args: Any, **kwargs: Any) -> asyncio.Task[Any]:
        """Async wrapper function that creates the call."""
        try:
            loop = (
                asyncio.get_running_loop()
            )  # Safely get the current running event loop
        except RuntimeError:
            # No running event loop, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Schedule the coroutine for execution in the background
        return asyncio.ensure_future(async_func(*args, **kwargs))

    return wrapper


@ensure_future
async def store_api_statistics(
    config: ServerConfig,
    num_results: int,
    status: int,
    api_type: str,
    endpoint: str,
    query_params: Optional[Dict[str, Any]] = None,
    **extra_metadata: Any
) -> None:
    """Store API query statistics in MongoDB.

    Parameters
    ----------
    config: ServerConfig
        Server configuration instance
    num_results: int
        Number of results returned
    status: int
        HTTP status code
    api_type: str
        Type of API (e.g., 'databrowser', 'stacapi')
    endpoint: str
        Endpoint name
    query_params: Optional[Dict[str, Any]]
        Query parameters used
    **extra_metadata: Any
        Additional metadata to store
    """

    if num_results == 0 and api_type != "stacapi":
        return

    data = {
        "num_results": num_results,
        "server_status": status,
        "api_type": api_type,
        "endpoint": endpoint,
        "date": datetime.now(),
        **extra_metadata
    }

    try:
        await config.mongo_collection_search.insert_one({
            "metadata": data,
            "query": query_params or {}
        })
        logger.info(f"Stored {api_type} statistics: {endpoint}, {num_results} results")
    except Exception as error:
        logger.warning("Could not add stats to mongodb: %s", error)
