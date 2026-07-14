"""Tests for the fields parameter of the extended-search endpoint."""

from typing import Any, Dict, List, Optional

import requests

from freva_rest.databrowser_api.core import Solr


def _extended_search(
    server: str,
    *,
    flavour: str = "cmip6",
    uniq_key: str = "uri",
    fields: Optional[List[str]] = None,
    **params: Any,
) -> requests.Response:
    """GET /databrowser/extended-search/{flavour}/{uniq_key}."""
    query: Dict[str, Any] = {"max-results": 5, **params}
    if fields is not None:
        query["fields"] = fields
    return requests.get(
        f"{server}/databrowser/extended-search/{flavour}/{uniq_key}",
        params=query,
    )


class TestExtendedSearchFields:
    """The fields parameter is additive and strictly validated."""

    def test_default_is_unchanged(self, test_server: str) -> None:
        """Without fields the previous behaviour is preserved exactly."""
        res = _extended_search(test_server)
        assert res.status_code == 200
        for doc in res.json()["search_results"]:
            assert set(doc) == {"uri", "fs_type"}

    def test_time_and_bbox_are_returned(self, test_server: str) -> None:
        """The fields that this feature was built for come back."""
        res = _extended_search(test_server, fields=["time", "bbox"])
        assert res.status_code == 200
        docs = res.json()["search_results"]
        assert docs
        for doc in docs:
            assert "time" in doc

    def test_fields_are_additive(self, test_server: str) -> None:
        """
        Asking for time must not take fs_type away.
        """
        res = _extended_search(test_server, fields=["time"])
        assert res.status_code == 200
        for doc in res.json()["search_results"]:
            assert "fs_type" in doc
            assert "uri" in doc

    def test_uniq_key_always_present(self, test_server: str) -> None:
        """Both unique keys keep their key, whatever is requested."""
        for uniq_key in ("file", "uri"):
            res = _extended_search(
                test_server, uniq_key=uniq_key, fields=["time", "bbox"]
            )
            assert res.status_code == 200
            for doc in res.json()["search_results"]:
                assert uniq_key in doc

    def test_flavour_translation_round_trip(self, test_server: str) -> None:
        """A cmip6 client asks in cmip6 and is answered in cmip6."""
        res = _extended_search(test_server, fields=["variable_id"])
        assert res.status_code == 200
        docs = res.json()["search_results"]
        assert docs
        for doc in docs:
            assert "variable_id" in doc
            assert "variable" not in doc

    def test_untranslated_uses_native_names(self, test_server: str) -> None:
        """With translate=false the native solr names are used."""
        res = _extended_search(
            test_server, fields=["variable"], translate="false", multi_version="true"
        )
        assert res.status_code == 200
        for doc in res.json()["search_results"]:
            assert "variable" in doc

    def test_duplicates_are_collapsed(self, test_server: str) -> None:
        """Repeating a field, or naming a mandatory one, changes nothing."""
        res = _extended_search(test_server, fields=["time", "time", "fs_type"])
        assert res.status_code == 200
        for doc in res.json()["search_results"]:
            assert sorted(doc) == sorted({"uri", "fs_type", "time"})

    def test_unknown_field_rejected(self, test_server: str) -> None:
        """An unknown field name is a 422, not a silent pass-through."""
        res = _extended_search(test_server, fields=["not_a_field"])
        assert res.status_code == 422

    def test_solr_grammar_is_rejected(self, test_server: str) -> None:
        """Globs, transformers and functions never reach solr's fl."""
        for evil in ("*", "[docid]", "[child]", "field(price)", "uri:alias"):
            res = _extended_search(test_server, fields=[evil])
            assert res.status_code == 422, f"{evil} was not rejected"

    def test_internal_fields_rejected(self, test_server: str) -> None:
        """Stored but non public fields cannot be requested."""
        for internal in ("_version_", "file_no_version", "stac_collection_bbox"):
            res = _extended_search(test_server, fields=[internal])
            assert res.status_code == 422, f"{internal} was not rejected"

    def test_comma_packed_is_rejected(self, test_server: str) -> None:
        """Fields are repeated parameters, a comma packed value is not split."""
        res = _extended_search(test_server, fields=["time,bbox"])
        assert res.status_code == 422

    def test_too_many_fields_rejected(self, test_server: str) -> None:
        """The number of requested fields is bounded."""
        res = _extended_search(
            test_server, fields=["time"] * (Solr.max_return_fields + 1)
        )
        assert res.status_code == 422

    def test_fields_is_not_treated_as_a_facet(self, test_server: str) -> None:
        """
        fields must be popped by process_parameters.
        """
        res = _extended_search(test_server, fields=["time"])
        assert res.status_code == 200
        assert "fields" not in res.json()["facets"]


class TestFieldListUnit:
    """Unit level checks of the field list builder and the doc translation."""

    def test_translate_docs_keeps_the_uniq_key(self) -> None:
        """
        A custom flavour must not be able to overwrite the unique key.
        """

        class _Translator:
            translate = True
            forward_lookup = {"project": "uri"}

        solr = Solr.__new__(Solr)
        solr.uniq_key = "uri"
        solr.translator = _Translator()

        docs = [{"uri": "https://zarr/user-token", "project": "cmip6"}]
        translated = solr._translate_docs(docs)
        assert translated[0]["uri"] == "https://zarr/user-token"
        assert "cmip6" in translated[0].values()

    def test_translate_docs_noop_when_not_translating(self) -> None:
        """translate=false hands the documents back untouched."""

        class _Translator:
            translate = False
            forward_lookup: Dict[str, str] = {}

        solr = Solr.__new__(Solr)
        solr.uniq_key = "uri"
        solr.translator = _Translator()
        docs = [{"uri": "/a/b.nc", "fs_type": "posix"}]
        assert solr._translate_docs(docs) == docs


class TestReturnFieldPolicy:
    """Reserved result keys survive a bad flavour mapping."""

    def test_legacy_flavour_cannot_rename_fs_type(self) -> None:
        """
        A bad mapping already stored in mongo must not break a result.
        """

        class _Translator:
            translate = True
            forward_lookup = {"fs_type": "storage_type", "project": "uri"}

        solr = Solr.__new__(Solr)
        solr.uniq_key = "uri"
        solr.translator = _Translator()
        doc = {"uri": "https://zarr/token", "fs_type": "swift", "project": "cmip6"}
        out = solr._translate_docs([doc])[0]
        assert out["fs_type"] == "swift"
        assert out["uri"] == "https://zarr/token"
