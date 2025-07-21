"""Solr backend implementation for Apache Solr search."""

import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Literal, Tuple, Union, cast

import httpx
from fastapi import HTTPException
from pydantic import BaseModel

from freva_rest import __version__
from freva_rest.config import ServerConfig
from freva_rest.logger import logger

from .base import BaseBackend, IntakeCatalogue, SearchResult
from ..translation.translator import Translator
from ..utils.query_utils import adjust_time_string, adjust_bbox_string, join_facet_queries


class SolrBackend(BaseBackend):
    """Backend for Apache Solr search operations."""

    timeout: httpx.Timeout = httpx.Timeout(30)
    uniq_keys: Tuple[str, str] = ("file", "uri")

    def __init__(
        self,
        config: ServerConfig,
        translator: Translator,
        uniq_key: str,
        facets: Dict[str, List[str]],
        multi_version: bool = False,
        start: int = 0,
        **query_params,
    ):
        super().__init__(config, translator, uniq_key, facets, multi_version)
        self.start = start
        
        try:
            self.time = adjust_time_string(
                query_params.get("time", [""])[0],
                query_params.get("time_select", ["flexible"])[0],
                backend_type="solr"
            )
            self.bbox = adjust_bbox_string(
                query_params.get("bbox", [""])[0],
                query_params.get("bbox_select", ["flexible"])[0],
            )
        except ValueError as err:
            raise HTTPException(status_code=500, detail=str(err)) from err

        self.url, self.query = self._get_url_params()
        self.query.update({"start": start, "sort": "file desc"})

    @asynccontextmanager
    async def _session_get(self) -> AsyncIterator[Tuple[int, Dict[str, Any]]]:
        """Wrap the get request round a try and catch statement."""
        logger.info(
            "Query %s for uniq_key: %s with %s",
            self.url,
            self.uniq_key,
            self.query,
        )
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.get(self.url, params=self.query)
                status = response.status_code
                try:
                    await self._check_for_status(response)
                    search = response.json()
                except HTTPException:
                    search = {}
            except Exception as error:
                logger.exception("Connection to %s failed: %s", self.url, error)
                raise HTTPException(
                    status_code=503,
                    detail="Could not connect to Solr server",
                )
            yield status, search

    async def _check_for_status(self, response: httpx.Response) -> None:
        """Check if a query was successful."""
        if response.status_code not in (200, 201):
            raise HTTPException(
                status_code=response.status_code, detail=response.text
            )

    def _get_url_params(self) -> Tuple[str, Dict[str, Any]]:
        """Get the URL and query parameters for Solr search."""
        core = {
            True: self.config.solr_cores[0],
            False: self.config.solr_cores[-1],
        }[self.multi_version]
        url = f"{self.config.get_core_url(core)}/select/"
        
        query = []
        for key, value in self.facets.items():
            query_pos, query_neg = join_facet_queries(key, value, self.uniq_keys)
            key = key.lower().replace("_not_", "")
            if query_pos:
                query.append(f"{key}:({query_pos})")
            if query_neg:
                query.append(f"-{key}:({query_neg})")

        user_query = "user:*" if self.translator.flavour == "user" else "{!ex=userTag}-user:*"
        
        return url, {
            "q": "*:*",
            "fq": self.time + self.bbox + ["", user_query, " AND ".join(query) or "*:*"],
        }

    async def _create_intake_catalogue(self, *facets: str) -> Dict[str, Any]:
        """Create intake catalogue structure."""
        var_name = self.translator.forward_lookup["variable"]
        catalogue = {
            "esmcat_version": "0.1.0",
            "attributes": [{"column_name": v, "vocabulary": ""} for v in facets],
            "assets": {
                "column_name": self.uniq_key,
                "format_column_name": "format",
            },
            "id": "freva",
            "description": f"Catalogue from freva-databrowser v{__version__}",
            "title": "freva-databrowser catalogue",
            "last_updated": f"{datetime.now().isoformat()}",
            "aggregation_control": {
                "variable_column_name": var_name,
                "groupby_attrs": [],
                "aggregations": [
                    {"type": "union", "attribute_name": f, "options": {}}
                    for f in facets
                ],
            },
        }
        return catalogue

    def _set_catalogue_queries(self) -> None:
        """Set the query parameters for a catalogue search."""
        self.query["facet"] = "true"
        self.query["facet.mincount"] = "1"
        self.query["facet.limit"] = "-1"
        self.query["rows"] = self.batch_size
        self.query["facet.field"] = self.config.solr_fields
        self.query["fl"] = [self.uniq_key] + self.config.solr_fields
        self.query["wt"] = "json"

    async def init_intake_catalogue(self) -> Tuple[int, IntakeCatalogue]:
        """Create an intake catalogue from the solr search."""
        self._set_catalogue_queries()
        async with self._session_get() as res:
            search_status, search = res
        total_count = cast(int, search.get("response", {}).get("numFound", 0))
        facets = search.get("facet_counts", {}).get("facet_fields", {})
        facets = [
            self.translator.forward_lookup.get(v, v)
            for v in self.translator.facet_hierarchy
            if facets.get(v)
        ]
        catalogue = await self._create_intake_catalogue(*facets)
        return search_status, IntakeCatalogue(
            catalogue=catalogue, total_count=total_count
        )

    async def extended_search(
        self,
        facets: List[str],
        max_results: int,
        zarr_stream: bool = False,
    ) -> Tuple[int, SearchResult]:
        """Perform extended search on Solr."""
        search_facets = [f for f in facets if f not in ("*", "all")] or [
            f for f in self.config.solr_fields
        ]
        if self.multi_version:
            search_facets.append("version")

        self.query["facet"] = "true"
        self.query["rows"] = max_results
        self.query["facet.sort"] = "index"
        self.query["facet.mincount"] = "1"
        self.query["facet.limit"] = "-1"
        self.query["wt"] = "json"
        self.query["facet.field"] = self.translator.translate_facets(
            search_facets, backwards=True
        )
        self.query["fl"] = [self.uniq_key, "fs_type"]

        async with self._session_get() as res:
            search_status, search = res

        docs = search.get("response", {}).get("docs", [])

        if zarr_stream and docs:
            for doc in docs:
                zarr_path = await self._publish_to_zarr_stream(doc)
                doc[self.uniq_key] = zarr_path
                doc["fs_type"] = doc.get("fs_type", "posix")

        return search_status, SearchResult(
            total_count=search.get("response", {}).get("numFound", 0),
            facets=self.translator.translate_query(
                search.get("facet_counts", {}).get("facet_fields", {})
            ),
            search_results=docs,
            facet_mapping={
                k: self.translator.forward_lookup[k]
                for k in self.query["facet.field"]
                if k in self.translator.forward_lookup
            },
            primary_facets=self.translator.primary_keys,
        )

    async def init_stream(self) -> Tuple[int, int]:
        """Initialize search stream for Solr."""
        self.query["fl"] = ["file", "uri"]
        async with self._session_get() as res:
            search_status, search = res
        return search_status, search.get("response", {}).get("numFound", 0)

    async def _solr_page_response(self) -> AsyncIterator[Dict[str, Any]]:
        """Paginate through Solr results using cursor mark."""
        self.query["cursorMark"] = "*"
        self.query["rows"] = self.batch_size
        while True:
            async with self._session_get() as res:
                _, results = res
            for content in results.get("response", {}).get("docs", []):
                yield content
            next_cursor_mark = results.get("nextCursorMark", None)
            if next_cursor_mark == self.query["cursorMark"] or not results:
                break
            self.query["cursorMark"] = next_cursor_mark

    async def stream_response(self) -> AsyncIterator[str]:
        """Stream search results from Solr."""
        async for result in self._solr_page_response():
            yield f"{result[self.uniq_key]}\n"

    def _process_catalogue_result(self, out: Dict[str, Any]) -> Dict[str, Any]:
        """Process catalogue results."""
        return {
            k: (
                out[k][0]
                if isinstance(out.get(k), list) and len(out[k]) == 1
                else out.get(k)
            )
            for k in [self.uniq_key] + self.translator.facet_hierarchy
            if out.get(k)
        }

    async def _iter_intake(self) -> AsyncIterator[str]:
        """Iterator for catalogue entries."""
        encoder = json.JSONEncoder(indent=3)
        init = True
        yield ',\n   "catalog_dict": '

        async for result in self._solr_page_response():
            entry = self._process_catalogue_result(result)
            separator = "[" if init else ","
            yield f"{separator}\n   "
            init = False
            for line in list(encoder.iterencode(entry)):
                yield line

    async def intake_catalogue(
        self, catalogue: Dict[str, Any], header_only: bool = False
    ) -> AsyncIterator[str]:
        """Stream intake catalogue."""
        encoder = json.JSONEncoder(indent=3)
        for line in list(encoder.iterencode(catalogue))[:-1]:
            yield line
        if header_only is False:
            async for line in self._iter_intake():
                yield line
            yield "\n   ]\n}"

    async def _publish_to_zarr_stream(self, doc: Dict[str, Any]) -> str:
        """Publish URI to Redis for zarr streaming."""
        # This would need the Redis connection logic from the original code
        # For now, return a placeholder
        api_path = f"{self.config.proxy}/api/freva-nextgen/data-portal/zarr"
        uri = doc[self.uniq_key]
        uuid5 = str(uuid.uuid5(uuid.NAMESPACE_URL, uri))
        return f"{api_path}/{uuid5}.zarr"

    async def zarr_response(
        self,
        catalogue_type: Literal["intake", None],
        num_results: int,
    ) -> AsyncIterator[str]:
        """Create zarr endpoint response."""
        if catalogue_type == "intake":
            _, intake = await self.init_intake_catalogue()
            async for string in self.intake_catalogue(intake.catalogue, True):
                yield string
            yield ',\n   "catalog_dict": ['

        num = 1
        async for result in self._solr_page_response():
            prefix = suffix = ""
            zarr_path = await self._publish_to_zarr_stream(result)

            if catalogue_type == "intake":
                result[self.uniq_key] = zarr_path
                output = json.dumps(
                    self._process_catalogue_result(result), indent=3
                )
                prefix = "   "
                suffix = "," if num < num_results else ""
            else:
                output = zarr_path

            num += 1
            yield f"{prefix}{output}{suffix}\n"

        if catalogue_type == "intake":
            yield "\n   ]\n}"

    def configure_base_search(self) -> None:
        """Set up basic search configuration."""
        self.query["q"] = "*:*"
        self.query["wt"] = "json"
        self.query["facet"] = "true"
        self.query["facet.sort"] = "index"
        self.query["facet.mincount"] = "1"
        self.query["facet.limit"] = "-1"

    def set_query_params(self, **params) -> None:
        """Set multiple Solr query parameters at once."""
        param_mapping = {
            "facet_field": "facet.field",
            "facet_sort": "facet.sort", 
            "facet_mincount": "facet.mincount",
            "facet_limit": "facet.limit"
        }
        
        for key, value in params.items():
            if value is not None:
                actual_key = param_mapping.get(key, key)
                if actual_key == "fq" and isinstance(value, list):
                    self.query[actual_key] = value
                elif actual_key == "fl" and isinstance(value, list):
                    self.query[actual_key] = value
                elif isinstance(value, list):
                    self.query[actual_key] = value
                else:
                    self.query[actual_key] = str(value)

    def _set_catalogue_queries(self) -> None:
        """Set the query parameters for a catalogue search."""
        self.query["facet"] = "true"
        self.query["facet.mincount"] = "1"
        self.query["facet.limit"] = "-1"
        self.query["rows"] = self.batch_size
        self.query["facet.field"] = self.config.solr_fields
        self.query["fl"] = [self.uniq_key] + self.config.solr_fields
        self.query["wt"] = "json"