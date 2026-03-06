"""Test client utils."""

import pytest


class TestLazyImport:
    """Tests for the lazy import machinery."""

    def test_valid_module(self) -> None:
        """A valid module should allow attribute access."""
        from freva_client.utils.lazy import LazyModule

        pd = LazyModule("pandas")
        assert hasattr(pd, "DataFrame")

    def test_invalid_module_raises(self) -> None:
        """An invalid module should raise ImportError on attribute access."""
        from freva_client.utils.lazy import LazyModule

        wrong = LazyModule("foofo")
        with pytest.raises(ImportError):
            wrong.foo
