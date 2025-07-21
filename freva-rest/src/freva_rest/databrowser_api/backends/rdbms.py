"""RDBMS backend implementation for database search."""

import json
from datetime import datetime
from functools import reduce
from typing import Any, AsyncIterator, Dict, List, Literal, Tuple, Union

from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError

from freva_rest import __version__
from freva_rest.config import ServerConfig
from freva_rest.logger import logger

from .base import BaseBackend, IntakeCatalogue, SearchResult
from ..translation.translator import Translator
from ..utils.query_utils import adjust_time_string


class RDBMSBackend(BaseBackend):
    """Backend for RDBMS (PostgreSQL, MySQL, etc.) search operations."""

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
        
        # RDBMS specific configuration
        self.table = config._read_config('secondary-backend', 'table')
        self.pagination_column = config._read_config('secondary-backend', 'pagination_column')
        self.limit_offset = config._read_config('secondary-backend', 'limit_offset')
        
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
        
        self.params = {'offset': start, 'limit': self.batch_size, 'table': self.table}
        
        # Process time query if provided
        try:
            if query_params.get("time"):
                self.time_condition, self.time_params = adjust_time_string(
                    query_params.get("time", [""])[0],
                    query_params.get("time_select", ["flexible"])[0],
                    backend_type="RDBMS",
                    lookuptable=self.lookuptable
                )
            else:
                self.time_condition = None
                self.time_params = {}
        except ValueError as err:
            raise HTTPException(status_code=500, detail=str(err)) from err

    def _build_where_clause(self) -> Tuple[str, Dict[str, Any]]:
        """Build WHERE clause and parameters for RDBMS query."""
        conditions = []
        params = dict(self.params)
        param_idx = 1

        for field, values in self.facets.items():
            if not values:
                continue
            field_expr = self.lookuptable.get(field)
            if not field_expr:
                continue
                
            field_conditions = []
            for value in values:
                param_name = f"p_{param_idx}"
                if isinstance(value, str) and value.lower().startswith("not "):
                    field_conditions.append(f"NOT ({field_expr} = :{param_name})")
                    params[param_name] = value[4:]
                else:
                    field_conditions.append(f"{field_expr} = :{param_name}")
                    params[param_name] = value
                param_idx += 1

            if field_conditions:
                conditions.append(f"({' OR '.join(field_conditions)})")

        # Add time conditions if present
        if self.time_condition:
            conditions.append(f"({self.time_condition})")
            params.update(self.time_params)

        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        return where_clause, params

    async def _execute_count_query(self, where_clause: str, params: Dict[str, Any]) -> int:
        """Execute count query."""
        query = f"SELECT COUNT(*) FROM {self.table} WHERE {where_clause}"
        async with self.config.session_query_rdbms(query, params) as res:
            return res.scalar() or 0

    async def _execute_search_query(
        self, 
        where_clause: str, 
        params: Dict[str, Any], 
        extended: bool = False,
        last_id: str = None
    ) -> List[Dict[str, Any]]:
        """Execute search query."""
        # Handle pagination with last_id
        if last_id:
            params.update({"last_id": str(last_id)})
            where_clause = f"({where_clause}) AND {self.pagination_column} < '{params['last_id']}'"

        # Build column selection
        base_columns = [
            f"COALESCE({self.lookuptable.get(self.uniq_key)}, '{self.lookuptable_defaults.get(self.uniq_key, 'NULL')}') as {self.uniq_key}",
            f"COALESCE({self.lookuptable.get('fs_type')}, '{self.lookuptable_defaults.get('fs_type')}') as fs_type",
            "id"
        ]

        if extended:
            additional_columns = [
                f"COALESCE({prop_name}, '{json.dumps(self.lookuptable_defaults.get(field, 'NULL'))}') as {field}"
                for field, prop_name in self.lookuptable.items()
                if field != self.uniq_key
            ]
            selected_columns = base_columns + additional_columns
            column_names = [self.uniq_key, "fs_type", "id"] + [
                field for field in self.lookuptable.keys() if field != self.uniq_key
            ]
        else:
            selected_columns = base_columns

        query = f"""
            SELECT {', '.join(selected_columns)}
            FROM {self.table}
            WHERE {where_clause}
            ORDER BY {self.pagination_column} DESC
            {self.limit_offset}
        """

        async with self.config.session_query_rdbms(query, params) as res:
            result = res.fetchall()

        if extended:
            return [dict(zip(column_names, row)) for row in result]
        return [
            {
                self.uniq_key: row[0],
                "fs_type": 'posix' if row[1] is None else row[1],
                self.pagination_column: row[2]
            }
            for row in result
        ]

    async def _execute_facets_query(self, where_clause: str, params: Dict[str, Any]) -> Dict[str, List[Union[str, int]]]:
        """Execute facet count queries."""
        facets = {}
        translated_fields = self.translator.translate_facets(self.lookuptable.keys())
        fields, prop_names = zip(*self.lookuptable.items())

        for field, translated_field, prop_name in zip(fields, translated_fields, prop_names):
            if field not in self.allowed_fields:
                continue
                
            query = f"""
                SELECT {prop_name} as value, COUNT(*) as count
                FROM {self.table}
                WHERE {where_clause}
                AND {prop_name} IS NOT NULL
                GROUP BY {prop_name}
            """
            
            try:
                async with self.config.session_query_rdbms(query, params) as res:
                    facets[translated_field] = [
                        item for row in res.fetchall() 
                        for item in [str(row[0]), int(row[1])]
                        if row[0]
                    ]
            except SQLAlchemyError as e:
                logger.error("[RDBMS]: Couldn't get facets for %s: %s", translated_field, str(e))
                continue
        
        return facets

    async def _query_executor(
        self, 
        offset: int, 
        last_id: str,
        limit: int, 
        desired_output: List[str]
    ) -> Dict[str, Any]:
        """Execute queries based on desired output."""
        where_clause, params = self._build_where_clause()
        params.update({"offset": offset, "limit": limit})
        
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
            if "total_count" in desired_output:
                results["total_count"] = int(await self._execute_count_query(where_clause, params))
            
            if any(x in desired_output for x in ["search_results", "extended_search_results"]):
                extended = "extended_search_results" in desired_output
                results["search_results"] = await self._execute_search_query(
                    where_clause, params, extended, last_id
                )
            
            if "facets" in desired_output:
                results["facets"] = await self._execute_facets_query(where_clause, params)
                
            return results

        except SQLAlchemyError as e:
            logger.error("[RDBMS]: Database error during search: %s", str(e))
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        except Exception as e:
            logger.error("[RDBMS]: Search failed: %s", str(e))
            raise HTTPException(status_code=500, detail=f"[RDBMS]: Search failed: {str(e)}")

    async def init_intake_catalogue(self) -> Tuple[int, IntakeCatalogue]:
        """Create intake catalogue from RDBMS search."""
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
        """Perform extended search on RDBMS."""
        search_results = await self._query_executor(
            offset=self.params.get('offset', 0),
            last_id=None,
            limit=max_results,
            desired_output=["search_results", "total_count", "facets"]
        )
        
        # Remove pagination column from results
        search_results["search_results"] = [
            {k: v for k, v in result.items() if k != self.pagination_column}
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
        """Initialize search stream for RDBMS."""
        search_results = await self._query_executor(
            offset=0,
            last_id=None,
            limit=1,
            desired_output=["total_count"]
        )
        return 200, search_results["total_count"]

    async def _rdbms_page_response(self) -> AsyncIterator[Dict[str, Any]]:
        """Paginate through RDBMS results."""
        last_id = None
        offset = 0
        
        while True:
            try:
                search_results = await self._query_executor(
                    offset=offset,
                    last_id=last_id,
                    limit=self.batch_size,
                    desired_output=["search_results"]
                )
                
                results = search_results["search_results"]
                if not results:
                    break
                    
                for result in results:
                    last_id = result.get(self.pagination_column)
                    yield {k: v for k, v in result.items() if k != self.pagination_column}
                    
                if len(results) < self.batch_size:
                    break
                    
                offset += self.batch_size
                
            except Exception as e:
                logger.error("Error streaming results for RDBMS: %s", str(e))
                break

    async def stream_response(self) -> AsyncIterator[str]:
        """Stream search results from RDBMS."""
        async for result in self._rdbms_page_response():
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
        
        async for result in self._rdbms_page_response():
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
        """Create zarr endpoint response for RDBMS."""
        if catalogue_type == "intake":
            _, intake = await self.init_intake_catalogue()
            async for string in self.intake_catalogue(intake.catalogue, True):
                yield string
            yield ',\n   "catalog_dict": ['

        num = 1
        async for result in self._rdbms_page_response():
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