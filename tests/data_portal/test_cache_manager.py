"""tests for _cache_manager."""
 
from __future__ import annotations
 
from unittest.mock import MagicMock, patch
 
import pytest
import data_portal_worker._cache_manager as cm
 
 
def test_run_cache_cleanup_happy_path() -> None:
    """Successful cleanup logs info and does not raise."""
    mock_prism = MagicMock()
    mock_prism.cache_info.return_value = {
        "files": 3,
        "size_bytes": 1024 ** 2 * 8,
        "path": "/tmp/prism_cache",
    }
    with patch.dict("sys.modules", {"xarray_prism": mock_prism}):
        cm.run_cache_cleanup()
 
    mock_prism.clear_cache.assert_called_once()
    mock_prism.cache_info.assert_called_once()
 
 
def test_run_cache_cleanup_logs_warning_on_error() -> None:
    """Any exception inside cleanup is caught and logged as a warning."""
    mock_prism = MagicMock()
    mock_prism.clear_cache.side_effect = RuntimeError("disk full")

    with patch.dict("sys.modules", {"xarray_prism": mock_prism}):
        with patch.object(cm.data_logger, "warning") as mock_warn:
            cm.run_cache_cleanup()

    mock_warn.assert_called_once()
    assert "Cache cleanup failed" in mock_warn.call_args[0][0]
 

 
def test_scheduler_fires_immediately_on_first_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    """tick() should call run_cache_cleanup on the very first call."""
    calls: list[int] = []
    monkeypatch.setattr(cm, "run_cache_cleanup", lambda: calls.append(1))
    scheduler = cm.CacheScheduler()
    scheduler.tick()
    assert calls == [1]


def test_scheduler_does_not_fire_twice_within_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second tick() shortly after the first should not fire again."""
    calls: list[int] = []
    monkeypatch.setattr(cm, "run_cache_cleanup", lambda: calls.append(1))
    scheduler = cm.CacheScheduler()
    scheduler.tick()   # fires
    scheduler.tick()   # too soon — must not fire
    assert len(calls) == 1


def test_scheduler_fires_again_after_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """tick() fires again once the interval has elapsed."""
    calls: list[int] = []
    monkeypatch.setattr(cm, "run_cache_cleanup", lambda: calls.append(1))

    scheduler = cm.CacheScheduler()
    # fires immediately (_last_run was 0.0)
    scheduler.tick()          
    # rewind _last_run to simulate elapsed interval without sleep
    # and without touching the global _CLEANUP_INTERVAL
    scheduler._last_run = 0.0
    # fires again
    scheduler.tick()          
    assert len(calls) == 2


def test_cleanup_interval_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """_CLEANUP_INTERVAL should reflect API_CACHE_CLEANUP_INTERVAL."""
    monkeypatch.setenv("API_CACHE_CLEANUP_INTERVAL", "42")
    import importlib
    importlib.reload(cm)
    assert cm._CLEANUP_INTERVAL == 42.0
