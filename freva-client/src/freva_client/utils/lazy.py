"""Lazy load 'slow' deps."""

from importlib import import_module as _mod
from types import ModuleType
from typing import Generic, Optional, TypeVar

LazyType = TypeVar("LazyType", bound=ModuleType)


class LazyModule(Generic[LazyType]):
    def __init__(self, module_name: str):
        self._module_name = module_name
        self._module: Optional[LazyType] = None

    def _load(self) -> LazyType:
        if self._module is None:
            try:
                self._module = _mod(self._module_name)  # type: ignore[assignment]
            except ImportError as error:
                raise ImportError(
                    f"Optional dependency '{self._module_name}' is "
                    "required for this feature."
                ) from error
        return self._module

    def __getattr__(self, item: str):
        return getattr(self._load(), item)


intake = LazyModule("intake")
intake_esm = LazyModule("intake_esm")
pd = LazyModule("pandas")
xr = LazyModule("xarray")
