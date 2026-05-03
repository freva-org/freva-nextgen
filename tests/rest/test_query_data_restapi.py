"""Unit tests for data queries via the rest-api."""

import json
import time
from typing import Dict

import requests
from pymongo import MongoClient
from pytest_mock import MockerFixture

from freva_rest.config import ServerConfig


class TestOverview:
    """Tests for the databrowser overview endpoint."""

    def test_attributes(self, test_server: str) -> None:
        """Test getting the attributes."""
        res = requests.get(f"{test_server}/databrowser/overview")
        assert isinstance(res.json()["flavours"], list)
        assert isinstance(res.json()["attributes"], dict)


class TestDataSearch:
    """Tests for the databrowser data-search endpoint."""

    def test_basic_search(self, test_server: str) -> None:
        """Test basic data search with valid and invalid parameters."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={"activity_id": "cmipx"},
        )
        assert res.status_code == 200
        assert len(res.text.split()) == 0

    def test_translate_equivalence(self, test_server: str) -> None:
        """Untranslated cmip6 and translated freva return the same results."""
        res_cmip6 = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={"translate": "false", "product": "cmip"},
        )
        res_freva = requests.get(
            f"{test_server}/databrowser/data-search/freva/uri",
            params={"product": "cmip"},
        )
        assert res_cmip6.text == res_freva.text

    def test_multi_version_returns_more(self, test_server: str) -> None:
        """Multi-version search returns more results than latest-only."""
        res_latest = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={"translate": "false", "product": "cmip"},
        )
        res_multi = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={
                "translate": "false",
                "product": "cmip",
                "multi-version": "true",
            },
        )
        assert len(res_latest.text.split()) < len(res_multi.text.split())

    def test_invalid_facet_rejected(self, test_server: str) -> None:
        """Unknown facet key returns 422."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={"foo": "bar"},
        )
        assert res.status_code == 422

    def test_invalid_flavour_rejected(self, test_server: str) -> None:
        """Non-existent flavour returns 422."""
        res = requests.get(f"{test_server}/databrowser/data-search/cmipx/file")
        assert res.status_code == 422


class TestMultiVersion:
    """Tests for multi-version search behaviour."""

    def test_metadata_version_facet_present(self, test_server: str) -> None:
        """Multi-version metadata includes the version facet."""
        res = requests.get(
            f"{test_server}/databrowser/metadata-search/freva/file",
            params={"multi-version": True},
        )
        assert res.status_code == 200
        assert "version" in res.json()["facets"]

    def test_metadata_version_facet_absent(self, test_server: str) -> None:
        """Latest-only metadata excludes the version facet."""
        res = requests.get(
            f"{test_server}/databrowser/metadata-search/freva/file",
            params={"multi-version": False},
        )
        assert res.status_code == 200
        assert "version" not in res.json()["facets"]

    def test_data_search_multi_returns_more(self, test_server: str) -> None:
        """Multi-version data search returns more files than latest-only."""
        res_multi = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"multi-version": True},
        )
        res_latest = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"multi-version": False},
        )
        assert res_multi.status_code == 200
        assert res_latest.status_code == 200
        assert len(res_latest.text.split()) < len(res_multi.text.split())

    def test_version_filter_with_multi(self, test_server: str) -> None:
        """Filtering by a specific version works with multi-version enabled."""
        res = requests.get(
            f"{test_server}/databrowser/metadata-search/freva/file",
            params={"multi-version": True},
        )
        version = res.json()["facets"]["version"][0]
        res_filtered = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"multi-version": True, "version": version},
        )
        assert res_filtered.status_code == 200

    def test_version_filter_without_multi_fails(self, test_server: str) -> None:
        """Filtering by version without multi-version is rejected."""
        res = requests.get(
            f"{test_server}/databrowser/metadata-search/freva/file",
            params={"multi-version": True},
        )
        version = res.json()["facets"]["version"][0]
        res_filtered = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"multi-version": False, "version": version},
        )
        assert res_filtered.status_code != 200


class TestFileAndUriSearch:
    """Tests for file and URI based search."""

    def test_file_glob_search(self, test_server: str) -> None:
        """Glob pattern search returns results."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/file",
            params={"file": "/arch/bb1203/*/CPC/*"},
        )
        assert res.status_code == 200
        assert len(res.text.split()) > 0

    def test_uri_metadata_search(self, test_server: str) -> None:
        """URI pattern search returns matching facets."""
        res = requests.get(
            f"{test_server}/databrowser/metadata-search/cmip6/file",
            params={"uri": "slk:///arch/bb1203/*/CPC/*"},
        )
        assert res.status_code == 200
        data = res.json()
        assert len(data) > 0
        assert "cmorph" in data["facets"]["experiment_id"]


class TestTimeSelection:
    """Tests for time-based filtering."""

    def test_valid_time_range(self, test_server: str) -> None:
        """A valid time range returns results."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"time": "1898 to 1901"},
        )
        assert len(res.text.split()) >= 1

    def test_invalid_time_select(self, test_server: str) -> None:
        """An invalid time_select value returns 500."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"time": "1898 to 1901", "time_select": "foo"},
        )
        assert res.status_code == 500

    def test_unparseable_time(self, test_server: str) -> None:
        """An unparseable time string returns 500."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"time": "fx"},
        )
        assert res.status_code == 500


class TestBboxSelection:
    """Tests for bounding-box filtering."""

    def test_valid_bbox(self, test_server: str) -> None:
        """A valid bbox returns the expected number of results."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"bbox": "-10,10,-10,10"},
        )
        assert len(res.text.split()) == 61

    def test_invalid_bbox_select(self, test_server: str) -> None:
        """An invalid bbox_select value returns 500."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"bbox": "-10,10,-10,10", "bbox_select": "foo"},
        )
        assert res.status_code == 500

    def test_unparseable_bbox(self, test_server: str) -> None:
        """An unparseable bbox string returns 500."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"bbox": "fx"},
        )
        assert res.status_code == 500

    def test_longitude_out_of_range(self, test_server: str) -> None:
        """Longitude outside [-180, 180] returns 500."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"bbox": "-181,181,-10,10"},
        )
        assert res.status_code == 500

    def test_latitude_out_of_range(self, test_server: str) -> None:
        """Latitude outside [-90, 90] returns 500."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/freva/file",
            params={"bbox": "-10,10,-91,91"},
        )
        assert res.status_code == 500


class TestPrimaryFacets:
    """Tests for primary facet definitions."""

    def test_primary_facets_present(self, test_server: str) -> None:
        """All flavours return primary_facets with equal length."""
        res_freva = requests.get(
            f"{test_server}/databrowser/metadata-search/freva/file"
        ).json()
        res_cmip6 = requests.get(
            f"{test_server}/databrowser/metadata-search/cmip6/file"
        ).json()
        res_cmip6_raw = requests.get(
            f"{test_server}/databrowser/metadata-search/cmip6/file",
            params={"translate": "f"},
        ).json()
        assert "primary_facets" in res_freva
        assert "primary_facets" in res_cmip6
        assert "primary_facets" in res_cmip6_raw
        assert (
            len(res_freva["primary_facets"])
            == len(res_cmip6["primary_facets"])
            == len(res_cmip6_raw["primary_facets"])
        )

    def test_primary_facets_translation(self, test_server: str) -> None:
        """Untranslated cmip6 matches freva, translated cmip6 differs."""
        res_freva = requests.get(
            f"{test_server}/databrowser/metadata-search/freva/file"
        ).json()
        res_cmip6 = requests.get(
            f"{test_server}/databrowser/metadata-search/cmip6/file"
        ).json()
        res_cmip6_raw = requests.get(
            f"{test_server}/databrowser/metadata-search/cmip6/file",
            params={"translate": "f"},
        ).json()
        assert res_freva["primary_facets"] == res_cmip6_raw["primary_facets"]
        assert res_freva["primary_facets"] != res_cmip6["primary_facets"]


class TestExtendedSearch:
    """Tests for the extended-search endpoint."""

    def test_basic_extended_search(self, test_server: str) -> None:
        """Extended search returns results with facets."""
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            params={"start": 0, "activity_id": "cmip", "max-results": 2},
        ).json()
        assert len(res["search_results"]) > 0
        assert "activity_id" in res["facets"]

    def test_offset_beyond_results(self, test_server: str) -> None:
        """A large start offset returns empty results."""
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            params={"start": 1000, "activity_id": "cmip", "max-results": 2},
        ).json()
        assert "rcm_name" not in res["primary_facets"]
        assert res["search_results"] == []

    def test_untranslated_cmip5_rejected(self, test_server: str) -> None:
        """Untranslated cmip5 query returns 422."""
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cmip5/uri",
            params={"activity_id": "cmip", "translate": "false", "max-results": 2},
        )
        assert res.status_code == 422

    def test_no_match_returns_200(self, test_server: str) -> None:
        """A query with no matches still returns 200."""
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            params={"activity_id": "cmipx", "translate": "true", "max-results": 2},
        )
        assert res.status_code == 200

    def test_cordex_facets(self, test_server: str) -> None:
        """Cordex search returns rcm_name in facets and primary_facets."""
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cordex/uri",
            params={"domain": "eur-11", "translate": "true"},
        ).json()
        assert "rcm_name" in res["facets"]
        assert "rcm_name" in res["primary_facets"]

    def test_single_facet_filter(self, test_server: str) -> None:
        """Requesting a single facet returns only that facet."""
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            params={"facets": "activity_id"},
        ).json()
        assert len(res["facets"].keys()) == 1

    def test_max_results_zero(self, test_server: str) -> None:
        """max-results=0 returns no search results."""
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            params={"facets": "activity_id", "max-results": 0},
        ).json()
        assert len(res["search_results"]) == 0

    def test_zarr_stream_with_auth(
        self, test_server: str, auth: Dict[str, str]
    ) -> None:
        """Zarr stream search with valid auth returns results."""
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            params={
                "facets": "activity_id",
                "max-results": 1,
                "zarr_stream": True,
            },
            headers={"Authorization": f"Bearer {auth['access_token']}"},
        ).json()
        assert len(res["search_results"]) == 1

    def test_zarr_stream_without_auth(self, test_server: str) -> None:
        """Zarr stream search without auth returns 401."""
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            params={
                "facets": "activity_id",
                "max-results": 1,
                "zarr_stream": True,
            },
        )
        assert res.status_code == 401

    def test_zarr_stream_broken_cache(
        self, test_server: str, auth: Dict[str, str], mocker: MockerFixture
    ) -> None:
        """Zarr stream with broken cache returns internal error."""
        mocker.patch("freva_rest.freva_data_portal.utils.Cache", "foo")
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            params={
                "facets": "activity_id",
                "max-results": 1,
                "zarr_stream": True,
            },
            headers={"Authorization": f"Bearer {auth['access_token']}"},
        ).json()
        assert (
            res["search_results"][0]["uri"]
            == "Internal error, service not able to publish"
        )

    def test_zarr_stream_service_disabled(
        self, test_server: str, mocker: MockerFixture
    ) -> None:
        """Zarr stream returns 200 when service is disabled (no zarr)."""
        mocker.patch("freva_rest.rest.server_config.services", "databrowser")
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cmip6/uri",
            params={
                "facets": "activity_id",
                "max-results": 1,
                "zarr_stream": True,
            },
        )
        assert res.status_code == 200


class TestMetadataSearch:
    """Tests for the metadata-search endpoint."""

    def test_basic_metadata_search(self, test_server: str) -> None:
        """Metadata search returns facets without search_results."""
        res = requests.get(
            f"{test_server}/databrowser/metadata-search/cmip6/uri",
            params={"activity_id": "cmip"},
        ).json()
        assert "search_results" not in res.keys()
        assert "activity_id" in res["facets"]

    def test_untranslated_cmip5_rejected(self, test_server: str) -> None:
        """Untranslated cmip5 metadata query returns 422."""
        res = requests.get(
            f"{test_server}/databrowser/extended-search/cmip5/uri",
            params={"activity_id": "cmip", "translate": "false"},
        )
        assert res.status_code == 422

    def test_no_match_returns_200(self, test_server: str) -> None:
        """A metadata query with no matches still returns 200."""
        res = requests.get(
            f"{test_server}/databrowser/metadata-search/cmip6/uri",
            params={"activity_id": "cmipx", "translate": "true"},
        )
        assert res.status_code == 200

    def test_cordex_rcm_name(self, test_server: str) -> None:
        """Cordex metadata includes rcm_name in facets and primary_facets."""
        res = requests.get(
            f"{test_server}/databrowser/metadata-search/cordex/uri",
            params={"domain": "eur-11", "translate": "true"},
        ).json()
        assert "rcm_name" in res["facets"]
        assert "rcm_name" in res["primary_facets"]

    def test_single_facet_filter(self, test_server: str) -> None:
        """Requesting a single facet returns only that facet."""
        res = requests.get(
            f"{test_server}/databrowser/metadata-search/cmip6/uri",
            params={"facets": "activity_id"},
        ).json()
        assert len(res["facets"].keys()) == 1


class TestNegationSearch:
    """Tests for negation operators in search queries."""

    def test_dash_and_not_prefix(self, test_server: str) -> None:
        """Dash and 'not' prefixes exclude matching facet values."""
        res = requests.get(
            f"{test_server}/databrowser/metadata-search/freva/file",
            params={
                "dataset": ["-cmip6-swift", "not cmip6-fs"],
                "project": "cmip6",
            },
        ).json()
        assert "cmip6-swift" not in res["facets"]["dataset"]
        assert "cmip6-fs" not in res["facets"]["dataset"]
        assert "cmip6-hsm" in res["facets"]["dataset"]

    def test_not_suffix(self, test_server: str) -> None:
        """The _not_ suffix excludes matching facet values."""
        res = requests.get(
            f"{test_server}/databrowser/metadata-search/freva/file",
            params={"project_not_": "cmip6"},
        ).json()
        assert "cmip6" not in res["facets"]["project"]


class TestIntakeCatalogue:
    """Tests for intake catalogue generation."""

    def test_basic_intake(self, test_server: str) -> None:
        """Intake catalogue response is valid JSON."""
        res = requests.get(
            f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
            params={"activity_id": "cmip", "multi-version": True},
        )
        assert res.json() == json.loads(res.text)

    def test_unfiltered_returns_more(self, test_server: str) -> None:
        """Unfiltered catalogue has more entries than filtered."""
        res_filtered = requests.get(
            f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
            params={"activity_id": "cmip", "multi-version": True},
        )
        res_all = requests.get(
            f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
        )
        assert len(res_all.json()["catalog_dict"]) > len(
            res_filtered.json()["catalog_dict"]
        )

    def test_latest_only_returns_fewer(self, test_server: str) -> None:
        """Latest-only catalogue has fewer entries than multi-version."""
        res_multi = requests.get(
            f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
            params={"activity_id": "cmip", "multi-version": True},
        )
        res_latest = requests.get(
            f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
            params={"activity_id": "cmip", "multi-version": False},
        )
        assert len(res_multi.json()["catalog_dict"]) > len(
            res_latest.json()["catalog_dict"]
        )

    def test_max_results_limit(self, test_server: str) -> None:
        """max-results=1 returns 413."""
        res = requests.get(
            f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
            params={"multi-version": False, "max-results": 1},
        )
        assert res.status_code == 413

    def test_no_match_returns_404(self, test_server: str) -> None:
        """A query with no matches returns 404."""
        res = requests.get(
            f"{test_server}/databrowser/intake-catalogue/cmip6/uri",
            params={"activity_id": "cmip2"},
        )
        assert res.status_code == 404


class TestStacCatalogue:
    """Tests for STAC catalogue generation."""

    def test_basic_stac(self, test_server: str) -> None:
        """STAC endpoint returns 200 for valid query."""
        res = requests.get(
            f"{test_server}/databrowser/stac-catalogue/cmip6/uri",
            params={
                "activity_id": "cmip",
                "multi-version": True,
                "max_results": 2,
            },
            allow_redirects=False,
        )
        assert res.status_code == 200

    def test_max_results_limit(self, test_server: str) -> None:
        """max-results=1 returns 413."""
        res = requests.get(
            f"{test_server}/databrowser/stac-catalogue/cmip6/uri",
            params={
                "activity_id": "cmip",
                "multi-version": False,
                "max-results": 1,
            },
        )
        assert res.status_code == 413

    def test_invalid_collection(self, test_server: str) -> None:
        """Invalid collection parameter returns 422."""
        res = requests.get(
            f"{test_server}/databrowser/stac-catalogue/cmip6/uri",
            params={"collection": "cmip2", "multi-version": False},
        )
        assert res.status_code == 422

    def test_no_match_returns_404(self, test_server: str) -> None:
        """A query with no matches returns 404."""
        res = requests.get(
            f"{test_server}/databrowser/stac-catalogue/cmip6/uri",
            params={"activity_id": "cmip3", "multi-version": False},
        )
        assert res.status_code == 404


class TestParameterValidation:
    """Tests for query parameter validation."""

    def test_invalid_parameters_rejected(self, test_server: str) -> None:
        """Various invalid parameter combinations return 422."""
        res1 = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={"activity_id": "cmip", "translate": "false"},
        )
        res2 = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={"product": "cmip", "translate": "true"},
        )
        res3 = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={"activity_": "cmip"},
        )
        assert res1.status_code == res2.status_code == res3.status_code == 422


class TestSolrConnection:
    """Tests for Solr connection error handling."""

    def test_no_solr(self, test_server: str, mocker: MockerFixture) -> None:
        """A bad Solr host returns 503."""
        mocker.patch("freva_rest.rest.server_config.solr_host", "foo.bar")
        res = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={"activity_id": "cmipx"},
        )
        assert res.status_code == 503


class TestZarrStreamService:
    """Tests for zarr stream service availability."""

    def test_not_implemented(
        self, test_server: str, auth: Dict[str, str], mocker: MockerFixture
    ) -> None:
        """Zarr request returns 503 when service is disabled."""
        mocker.patch("freva_rest.rest.server_config.services", "")
        res = requests.get(
            f"{test_server}/databrowser/load/freva",
            headers={"Authorization": f"Bearer {auth['access_token']}"},
        )
        assert res.status_code == 503


class TestMongoStatistics:
    """Tests for MongoDB search statistics insertion."""

    def test_failed_mongo_insert(
        self, test_server: str, mocker: MockerFixture
    ) -> None:
        """Search works even when MongoDB is unreachable."""
        from freva_rest.rest import server_config

        url = server_config.mongo_url
        with MongoClient(server_config.mongo_url) as mongo_client:
            collection = mongo_client[server_config.mongo_db]["search_queries"]
            collection.drop()
            assert collection.find_one() is None

        mocker.patch("freva_rest.rest.server_config.mongo_password", "foo")
        mocker.patch("freva_rest.rest.server_config._mongo_client", None)
        res = requests.get(
            f"{test_server}/databrowser/data-search/cmip6/uri",
            params={"activity_id": "cmip"},
        )
        assert res.status_code == 200

        mocker.patch("freva_rest.rest.server_config.mongo_password")
        with MongoClient(url) as mongo_client:
            collection = mongo_client[server_config.mongo_db]["search_queries"]
            assert collection.find_one() is None

    def test_successful_insert(
        self, test_server: str, cfg: ServerConfig
    ) -> None:
        """Search statistics are inserted into MongoDB."""
        res = requests.get(
            f"{test_server}/databrowser/data-search/cordex/uri",
            params={"variable": ["wind", "cape"]},
        )
        assert res.status_code == 200
        assert len(res.text.split()) > 0
        time.sleep(2)
        mongo_client = MongoClient(cfg.mongo_url)
        collection = mongo_client[cfg.mongo_db]["search_queries"]
        stats = list(collection.find())
        assert len(stats) > 0
        assert isinstance(stats[0], dict)
        assert "metadata" in stats[0]
        assert "query" in stats[0]
        assert isinstance(stats[0]["query"], dict)
        assert isinstance(stats[0]["metadata"], dict)
        assert (
            len([k for k in stats[0]["query"].values() if not isinstance(k, str)])
            == 0
        )
