"""Periodic cache cleanup for xarray-prism downloaded files."""

from __future__ import annotations

import os
import time

from .utils import data_logger

# How often to run eviction (seconds). Default: every 6 hours.
_CLEANUP_INTERVAL = float(os.environ.get("API_CACHE_CLEANUP_INTERVAL", 6 * 3600))


def run_cache_cleanup(force: bool = False) -> None:
    """Evict stale/oversized xarray-prism cache files safely.
    """
    try:
        import xarray_prism

        xarray_prism.clear_cache()
        info = xarray_prism.cache_info()
        data_logger.info(
            "Cache cleanup done: %d file(s), %.1f MB remaining at %s",
            info["files"],
            info["size_bytes"] / 1024**2,
            info["path"],
        )
    except Exception as error:
        # housekeeping never break the data-loading daemon
        # on 6-hourly cleanup attempts, only warns on failure
        data_logger.warning("Cache cleanup failed (non-fatal): %s", error)


class CacheScheduler:
    """Tracks elapsed time and fires cleanup when interval is due."""

    def __init__(self) -> None:
        # run immediately on first tick,
        # then every _CLEANUP_INTERVAL
        # seconds thereafter
        self._last_run: float = 0.0

    def tick(self) -> None:
        """Call on every iteration of the daemon loop."""
        if time.monotonic() - self._last_run >= _CLEANUP_INTERVAL:
            self._last_run = time.monotonic()
            run_cache_cleanup()
