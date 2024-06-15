"""Tests for the databrowser class."""

import pytest
from freva_client import databrowser
from freva_client.utils.logger import DatabrowserWarning


def test_search_files(test_server: str) -> None:
    """Test searching for files."""
    db = databrowser(host=test_server)
    assert len(list(db)) > 0
    assert len(list(db)) == len(db)
    db = databrowser(host=test_server, foo="bar", fail_on_error=True)
    with pytest.raises(ValueError):
        len(db)
    db = databrowser(host=test_server, foo="bar", time="2000 to 2050")
    assert len(db) == 0
    db = databrowser(host=test_server, model="bar")
    assert len(db) == len(list(db)) == 0
    db = databrowser(host="foo")
    with pytest.raises(ValueError):
        len(db)
    assert (
        len(
            databrowser(
                "land",
                realm="ocean",
                product="reanalysis",
                host=test_server,
            )
        )
        == 0
        == 0
    )


def test_count_values(test_server: str) -> None:
    """Test counting the facets."""
    db = databrowser(host=test_server)
    assert isinstance(len(db), int)
    counts1 = databrowser.count_values("*", host=test_server)
    assert isinstance(counts1, dict)
    assert "dataset" not in counts1
    counts2 = databrowser.count_values(
        "ocean",
        realm="ocean",
        product="reanalysis",
        host=test_server,
        extended_search=True,
    )
    assert isinstance(counts2, dict)
    assert "dataset" in counts2
    assert isinstance(counts2["dataset"], dict)
    entry = list(counts2["dataset"].keys())[0]
    assert isinstance(counts2["dataset"][entry], int)


def test_metadata_search(test_server: str) -> None:
    """Test the metadata search."""
    db = databrowser(host=test_server)
    assert isinstance(db.metadata, dict)
    metadata = databrowser.metadata_search(host=test_server)
    assert isinstance(metadata, dict)
    assert len(db.metadata) > len(metadata)
    metadata = databrowser.metadata_search(
        host=test_server, extended_search=True
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


def test_bad_queries(test_server: str) -> None:
    """Test the behaviour of bad queries."""
    db = databrowser(host=test_server, foo="bar")
    with pytest.warns(DatabrowserWarning):
        len(db)
    with pytest.warns(DatabrowserWarning):
        databrowser.count_values(host=test_server, foo="bar")
    with pytest.warns(DatabrowserWarning):
        databrowser.metadata_search(host=test_server, foo="bar")
    db = databrowser(host=test_server, foo="bar", fail_on_error=True)
    with pytest.raises(ValueError):
        len(db)
    with pytest.raises(ValueError):
        databrowser.count_values(
            host=test_server, foo="bar", fail_on_error=True
        )
    with pytest.raises(ValueError):
        databrowser.metadata_search(
            host=test_server, foo="bar", fail_on_error=True
        )
    db = databrowser(host=test_server, foo="bar", flavour="foo")  # type: ignore
    with pytest.raises(ValueError):
        len(db)


def test_repr(test_server: str) -> None:
    """Test the str rep."""
    db = databrowser(host=test_server)
    assert test_server in repr(db)
    assert str(len(db)) in db._repr_html_()
    overview = db.overview(host=test_server)
    assert isinstance(overview, str)
    assert "flavour" in overview
    assert "cmip6" in overview
    assert "freva" in overview
