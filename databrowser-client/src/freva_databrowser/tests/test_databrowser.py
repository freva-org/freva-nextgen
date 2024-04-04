"""Tests for the databrowser class."""

from pathlib import Path

import pytest
from freva_databrowser import databrowser
from freva_databrowser.logger import DatabrowserWarning


def test_search_files() -> None:
    """Test searching for files."""
    db = databrowser(host="localhost:8080")
    assert len(list(db)) > 0
    assert len(list(db)) == len(db)
    db = databrowser(host="localhost:8080", foo="bar", fail_on_error=True)
    with pytest.raises(ValueError):
        len(db)
    db = databrowser(host="localhost:8080", foo="bar", time="2000 to 2050")
    assert len(db) == 0
    db = databrowser(host="localhost:8080", model="bar")
    assert len(db) == len(list(db)) == 0
    db = databrowser(host="foo")
    with pytest.raises(ValueError):
        len(db)


def test_count_values() -> None:
    """Test counting the facets."""
    db = databrowser(host="localhost:8080")
    assert isinstance(len(db), int)
    counts1 = databrowser.count_values("*", host="localhost:8080")
    assert isinstance(counts1, dict)
    assert "dataset" not in counts1
    counts2 = databrowser.count_values(
        "*", host="localhost:8080", extendet_search=True
    )
    assert isinstance(counts2, dict)
    assert "dataset" in counts2
    assert isinstance(counts2["dataset"], dict)
    entry = list(counts2["dataset"].keys())[0]
    assert isinstance(counts2["dataset"][entry], int)


def test_metadata_search() -> None:
    """Test the metadata search."""
    db = databrowser(host="localhost:8080")
    assert isinstance(db.metadata, dict)
    metadata = databrowser.metadata_search(host="localhost:8080")
    assert isinstance(metadata, dict)
    assert len(db.metadata) > len(metadata)
    metadata = databrowser.metadata_search(
        host="localhost:8080", extendet_search=True
    )
    assert len(db.metadata) == len(metadata)


def test_bad_hostnames() -> None:
    """Test the behaviour of non existing host queries."""
    db = databrowser(host="foo")
    with pytest.raises(ValueError):
        len(db)
    with pytest.raises(ValueError):
        databrowser.metadata_search(host="foo")
    with pytest.raises(ValueError):
        databrowser.count_values(host="foo")


def test_bad_queries() -> None:
    """Test the behaviour of bad queries."""
    db = databrowser(host="localhost:8080", foo="bar")
    with pytest.warns(DatabrowserWarning):
        len(db)
    with pytest.warns(DatabrowserWarning):
        databrowser.count_values(host="localhost:8080", foo="bar")
    with pytest.warns(DatabrowserWarning):
        databrowser.metadata_search(host="localhost:8080", foo="bar")
    db = databrowser(host="localhost:8080", foo="bar", fail_on_error=True)
    with pytest.raises(ValueError):
        len(db)
    with pytest.raises(ValueError):
        databrowser.count_values(
            host="localhost:8080", foo="bar", fail_on_error=True
        )
    with pytest.raises(ValueError):
        databrowser.metadata_search(
            host="localhost:8080", foo="bar", fail_on_error=True
        )
    db = databrowser(host="localhost:8080", foo="bar", flavour="foo")  # type: ignore
    with pytest.raises(ValueError):
        len(db)


def test_repr() -> None:
    """Test the str rep."""
    db = databrowser(host="localhost:8080")
    assert "localhost" in db.__repr__()
    assert str(len(db)) in db._repr_html_()
