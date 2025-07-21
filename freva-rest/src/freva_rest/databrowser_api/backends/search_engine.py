"""Search Engine backend implementation for Elasticsearch/OpenSearch."""

import json
from contextlib import asynccontextmanager
from datetime import datetime
from functools import reduce
from typing import Any, AsyncIterator, Dict, List, Literal, Tuple, Union

import httpx
from fastapi import HTTPException

from freva_rest import __version__
from freva_rest.config import ServerConfig
from freva_rest.logger import logger

from .base import BaseBackend, IntakeCatalogue, SearchResult
from ..translation.translator import Translator
from ..utils.query_utils import adjust_time_string


class SearchEngineBackend(BaseBackend):
    """Backend for Search Engine (Elasticsearch/OpenSearch) operations."""

    timeout: httpx.Timeout = httpx.Timeout(30)

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
        
        # Search Engine specific configuration
        self.dns = config._read_config('secondary-backend', 'dns')
        
        # Get lookuptable configuration
        secondary_backend_config = config._config.get('secondary-backend', {})
        lookuptable_config = secondary_backend_config.get('lookuptable', {})
        
        self.lookuptable = {k: v for k, v in lookuptable_config.items()
                           if k != 'defaults' and isinstance(v, str)}
        self.lookuptable_defaults = lookuptable_config.get('defaults', {})
        
        self.allowed_fields = [
            'cmor_table', 'experiment', 'ensemble', 'fs_type', 'grid_label',
            'institute', 'model', 'project', 'product', 'realm', 'variable',
            'time_aggregation', 'time_frequency', 'dataset', 'driving_model',
            'format', 'grid_id', 'level_type', 'rcm_name', 'rcm_version', 'user'
        ]
        
        self.params = {'from': start, 'limit': self.batch_size}
        
        # Process time query if provided
        try:
            if query_params.get("time"):
                self.time_condition = adjust_time_string(
                    query_params.get("time", [""])[0],
                    query_params.get("time_select", ["flexible"])[0],
                    backend_type="SE",
                    lookuptable=self.lookuptable
                )
            else:
                self.time_condition = None
        except ValueError as err:
            raise HTTPException(status_code=500, detail=str(err)) from err

    @asynccontextmanager
    async def _session_get(self, url: str = None, query: Dict[str, Any] = None) -> AsyncIterator[Tuple[int, Dict[str, Any]]]:
        """Wrap the GET request with error handling."""
        if url is None:
            url = self.dns
        if query is None:
            query = {}
        
        logger.info("Query %s with %s", url, query)
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.get(url, params=query)
                status = response.status_code
                try:
                    await self._check_for_status(response)
                    search = response.json()
                except HTTPException:
                    search = {}
            except Exception as error:
                logger.exception("Connection to %s failed: %s", url, error)
                raise HTTPException(
                    status_code=503,
                    detail="Could not connect to Search Engine server",
                )
            yield status, search

    async def _check_for_status(self, response: httpx.Response) -> None:
        """Check if a query was successful."""
        if response.status_code not in (200, 201):
            raise HTTPException(
                status_code=response.status_code, detail=response.text
            )

    def _get_nested(self, dictionary: Dict[str, Any], path: str) -> Any:
        """Safely get nested dictionary value using dot notation."""
        try:
            return reduce(lambda d, key: d.get(key, {}), path.split("."), dictionary)
        except (AttributeError, TypeError):
            return None

    def _build_search_query(self, limit: int, from_param: int = 0, last_id: str = None) -> Dict[str, Any]:
        """Build Elasticsearch/OpenSearch query."""
        query_body = {
            "query": {
                "bool": {
                    "must": [{"match_all": {}}],
                    "must_not": []
                }
            },
            "_source": [self.lookuptable[field] for field in self.lookuptable],
            "sort": [{"_id": "asc"}],
            "size": limit
        }

        # Handle pagination
        if last_id:
            query_body.update({"search_after": [last_id], "from": 0})
        else:
            query_body.update({"from": from_param})

        # Add facet filters
        if hasattr(self, 'facets') and self.facets:
            for key, values in self.facets.items():
                if not values or key not in self.lookuptable:
                    continue
                    
                search_field = self.lookuptable[key]
                for value in values:
                    if isinstance(value, str) and value.lower().startswith("not "):
                        query_body["query"]["bool"]["must_not"].append({
                            "term": {f"{search_field}": value[4:]}
                        })
                    else:
                        query_body["query"]["bool"]["must"].append({
                            "term": {f"{search_field}": value}
                        })

        # Add aggregations for facets
        query_body["aggs"] = {
            f"facet_{field}": {
                "terms": {
                    "field": f"{self.lookuptable[field]}",
                    "size": 1000
                }
            }
            for field in self.allowed_fields
            if field in self.lookuptable
        }

        # Add time conditions if present
        if self.time_condition:
            query_body["query"]["bool"]["must"].append(self.time_condition)

        return query_body

    async def _query_executor(
        self, 
        offset: int, 
        last_id: str,
        limit: int, 
        desired_output: List[str]
    ) -> Dict[str, Any]:
        """Execute queries based on desired output."""
        results = {
            "total_count": 0,
            "search_results": [],
            "facets": {},
            "facet_mapping": {
                k: self.translator.forward_lookup[k]
                for k in self.lookuptable.keys()
                if k in self.translator.forward_lookup
            },
            "primary_facets": self.translator.primary_keys,
        }

        try:
            # Get total count if requested
            if "total_count" in desired_output:
                query_body = self._build_search_query(limit=0)
                count_query = {
                    'source': json.dumps({"query": query_body["query"]}),
                    'source_content_type': 'application/json'
                }
                
                async with self._session_get(f"{self.dns}/_count", count_query) as res:
                    _, response_count = res
                results["total_count"] = response_count["count"]

            # Get search results and facets if requested
            if {"search_results", "extended_search_results", "facets"} & set(desired_output):
                query_body = self._build_search_query(limit, offset, last_id)
                search_query = {
                    "source": json.dumps(query_body),
                    "source_content_type": "application/json"
                }

                async with self._session_get(f"{self.dns}/_search", search_query) as res:
                    _, response = res

                # Process search results
                if "extended_search_results" in desired_output:
                    results["search_results"] = [
                        {
                            field: self._get_nested(hit, f"_source.{self.lookuptable[field]}")
                            for field in self.lookuptable.keys()
                            if field != self.uniq_key
                        } | {
                            self.uniq_key: self._get_nested(hit, f"_source.{self.lookuptable[self.uniq_key]}"),
                            "fs_type": self._get_nested(hit, f"_source.{self.lookuptable.get('fs_type')}") or "posix",
                            "sort": hit.get("sort", [None])[0]
                        }
                        for hit in response["hits"]["hits"]
                    ]
                elif "search_results" in desired_output:
                    results["search_results"] = [
                        {
                            self.uniq_key: self._get_nested(hit, f"_source.{self.lookuptable[self.uniq_key]}"),
                            "fs_type": self._get_nested(hit, f"_source.{self.lookuptable.get('fs_type')}") or "posix",
                            "sort": hit.get("sort", [None])[0]
                        }
                        for hit in response["hits"]["hits"]
                    ]

                # Process facets
                if "facets" in desired_output and "aggregations" in response:
                    for field in self.allowed_fields:
                        if field in self.lookuptable:
                            translated_field = self.translator.translate_facets([field])[0]
                            if f"facet_{field}" in response["aggregations"]:
                                buckets = response["aggregations"][f"facet_{field}"]["buckets"]
                                results["facets"][translated_field] = [
                                    item for bucket in buckets if bucket["key"]
                                    for item in [str(bucket["key"]), int(bucket["doc_count"])]
                                ]

            return results

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"[SE]: Search failed: {str(e)}")

    async def init_intake_catalogue(self) -> Tuple[int, IntakeCatalogue]:
        """Create intake catalogue from Search Engine."""
        search_results = await self._query_executor(
            offset=0,
            last_id=None,
            limit=1,
            desired_output=["total_count", "facets"]
        )
        
        facets = [
            self.translator.forward_lookup.get(v, v)
            for v in self.translator.facet_hierarchy
            if v in search_results.get("facets", {})
        ]
        
        catalogue = await self._create_intake_catalogue(*facets)
        return 200, IntakeCatalogue(
            catalogue=catalogue,
            total_count=search_results["total_count"]
        )

    async def _create_intake_catalogue(self, *facets: str) -> Dict[str, Any]:
        """Create intake catalogue structure."""
        var_name = self.translator.forward_lookup["variable"]
        return {
            "esmcat_version": "0.1.0",
            "attributes": [{"column_name": v, "vocabulary": ""} for v in facets],
            "assets": {"column_name": self.uniq_key, "format_column_name": "format"},
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

    async def extended_search(
        self,
        facets: List[str],
        max_results: int,
        zarr_stream: bool = False,
    ) -> Tuple[int, SearchResult]:
        """Perform extended search on Search Engine."""
        search_results = await self._query_executor(
            offset=self.params.get('from', 0),
            last_id=None,
            limit=max_results,
            desired_output=["search_results", "total_count", "facets"]
        )
        
        # Remove sort field from results
        search_results["search_results"] = [
            {k: v for k, v in result.items() if k != "sort"}
            for result in search_results.get("search_results", [])
        ]
        
        return 200, SearchResult(
            total_count=search_results["total_count"],
            facets=search_results.get("facets", {}),
            search_results=search_results.get("search_results", []),
            facet_mapping=search_results.get("facet_mapping", {}),
            primary_facets=self.translator.primary_keys
        )

    async def init_stream(self) -> Tuple[int, int]:
        """Initialize search stream for Search Engine."""
        search_results = await self._query_executor(
            offset=0,
            last_id=None,
            limit=1,
            desired_output=["total_count"]
        )
        return 200, search_results["total_count"]

    async def _search_engine_page_response(self) -> AsyncIterator[Dict[str, Any]]:
        """Paginate through Search Engine results using search_after."""
        last_id = None
        
        while True:
            try:
                search_results = await self._query_executor(
                    offset=self.params.get('from', 0),
                    last_id=last_id,
                    limit=self.batch_size,
                    desired_output=["search_results"]
                )
                
                results = search_results["search_results"]
                if not results:
                    break
                    
                for idx, result in enumerate(results):
                    if idx == len(results) - 1:
                        last_id = result.get("sort")
                    yield {k: v for k, v in result.items() if k != "sort"}
                    
                if len(results) < self.batch_size:
                    break
                
            except Exception as e:
                logger.error("Error streaming results for Search Engine: %s", str(e))
                break

    async def stream_response(self) -> AsyncIterator[str]:
        """Stream search results from Search Engine."""
        async for result in self._search_engine_page_response():
            if self.uniq_key in result:
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
        
        async for result in self._search_engine_page_response():
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

    async def zarr_response(
        self,
        catalogue_type: Literal["intake", None],
        num_results: int,
    ) -> AsyncIterator[str]:
        """Create zarr endpoint response for Search Engine."""
        if catalogue_type == "intake":
            _, intake = await self.init_intake_catalogue()
            async for string in self.intake_catalogue(intake.catalogue, True):
                yield string
            yield ',\n   "catalog_dict": ['

        num = 1
        async for result in self._search_engine_page_response():
            prefix = suffix = ""
            # For now, return the file path as zarr path
            zarr_path = result.get(self.uniq_key, "")

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