"""
Fixtures shared by the rest-api tests for the STAC unit tests
"""

import importlib.util
import logging
import sys
import types
from pathlib import Path
from typing import Iterator

import pytest

# Directory that contains the ``freva_rest.stac_api`` package
_FREVA_REST_DIR = (
    Path(__file__).resolve().parents[2]
    / "freva-rest"
    / "src"
    / "freva_rest"
)
_STAC_API_DIR = _FREVA_REST_DIR / "stac_api"
_UTILS_DIR = _FREVA_REST_DIR / "utils"


def _load_real(name: str, path: Path) -> types.ModuleType:
    """Import a dependency-free helper module straight from disk."""
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def stac_module() -> Iterator[object]:
    """
    Import ``stac_api.core`` in isolation with stubbed dependencies.
    """
    overridden = [
        "freva_rest.config",
        "freva_rest.databrowser_api",
        "freva_rest.logger",
        "freva_rest.utils",
        "freva_rest.utils.stac_utils",
        "freva_rest.utils.stac_assets",
        "freva_rest.utils.stats_utils",
        "sc",
        "sc.schema",
        "sc.core",
    ]
    saved = {name: sys.modules.get(name) for name in overridden}

    cfg = types.ModuleType("freva_rest.config")
    cfg.ServerConfig = type("ServerConfig", (), {})
    sys.modules["freva_rest.config"] = cfg

    db = types.ModuleType("freva_rest.databrowser_api")
    db.Solr = type(
        "Solr",
        (),
        {
            "__init__": lambda self, *a, **k: None,
            "escape_chars": (
                "+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]",
                "^", "~", ":", "/",
            ),
        },
    )
    sys.modules["freva_rest.databrowser_api"] = db

    lg = types.ModuleType("freva_rest.logger")
    lg.logger = logging.getLogger("stac-test")
    sys.modules["freva_rest.logger"] = lg

    utils_pkg = types.ModuleType("freva_rest.utils")
    utils_pkg.__path__ = [str(_UTILS_DIR)]
    sys.modules["freva_rest.utils"] = utils_pkg
    _load_real("freva_rest.utils.stac_utils", _UTILS_DIR / "stac_utils.py")
    _load_real("freva_rest.utils.stac_assets", _UTILS_DIR / "stac_assets.py")

    stats = types.ModuleType("freva_rest.utils.stats_utils")

    async def _store(**kwargs):
        return None

    stats.store_api_statistics = _store
    sys.modules["freva_rest.utils.stats_utils"] = stats

    pkg = types.ModuleType("sc")
    pkg.__path__ = [str(_STAC_API_DIR)]
    sys.modules["sc"] = pkg
    for mod in ["schema", "core"]:
        spec = importlib.util.spec_from_file_location(
            f"sc.{mod}", str(_STAC_API_DIR / f"{mod}.py")
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[f"sc.{mod}"] = module
        spec.loader.exec_module(module)
    try:
        yield sys.modules["sc.core"]
    finally:
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


@pytest.fixture()
def make_api(stac_module):
    """Factory building a STACAPI instance against a fake config."""

    class _Cfg:
        proxy = "https://host"
        solr_fields = ["project", "product", "model", "variable"]

    def _factory(**kwargs):
        return stac_module.STACAPI(_Cfg(), **kwargs)

    return _factory
