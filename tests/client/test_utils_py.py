"""Test client utils."""

import pytest


def test_lazy_import() -> None:
    """Test the lazy import machinery."""
    from freva_client.utils.lazy import LazyModule

    pd = LazyModule("pandas")
    wrong = LazyModule("foofo")
    assert hasattr(pd, "DataFrame")
    with pytest.raises(ImportError):
        wrong.foo
