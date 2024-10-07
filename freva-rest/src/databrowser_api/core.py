"""The core functionality to interact with the apache solr search system."""
# TODO: all prints should be replaced with a websocket response
import asyncio
import concurrent.futures
import json
import multiprocessing as mp
import os
import threading
import uuid
from concurrent.futures import Future
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property, wraps
from pathlib import Path
from threading import Lock
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Iterable,
    List,
    Literal,
    Sized,
    Tuple,
    Union,
    cast,
)

import aiohttp
import numpy as np
import xarray as xr
from databrowser_api import __version__
from dateutil.parser import ParserError, parse
from fastapi import HTTPException
from freva_rest.config import ServerConfig
from freva_rest.logger import logger
from freva_rest.utils import create_redis_connection
from pydantic import BaseModel
from pymongo import UpdateOne
from typing_extensions import TypedDict

FlavourType = Literal["freva", "cmip6", "cmip5", "cordex", "nextgems"]
IntakeType = TypedDict(
    "IntakeType",
    {
        "esmcat_version": str,
        "attributes": List[Dict[str, str]],
        "assets": Dict[str, str],
        "id": str,
        "description": str,
        "title": str,
        "last_updated": str,
        "aggregation_control": Dict[str, Any],
    },
)

UserDataType = TypedDict(
    "UserDataType",
    {
        "experiment": str,
        "institute": str,
        "model": str,
        "variable": str,
        "time_frequency": str,
        "ensemble": str,
        "project": str,
        "realm": str,
    },
)
# TODO: Needs more consideration


def run_async_in_thread(coro_func: Callable[..., Coroutine[Any, Any, Any]],
                        *args: Any, **kwargs: Any) -> Any:
    """Run an async function in a new event loop in a thread."""
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    try:
        result = new_loop.run_until_complete(coro_func(*args, **kwargs))
    finally:
        new_loop.close()
    return result  # pragma: no cover


# TODO: Need to be considered to refactor
def fixed_facets(
    allowed_facet_values: List[Dict[str, Union[str, List[str]]]]
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Coroutine[Any, Any, Any]]]:
    """Decorator that validates specific facet keys and their respective
    disallowed values."""

    def decorator(
        async_func: Callable[..., Awaitable[Any]]
    ) -> Callable[..., Coroutine[Any, Any, Any]]:
        @wraps(async_func)
        async def wrapper(self: Any, user: str, *args: Any, **fwrites: str) -> Any:
            """Async wrapper that performs facet validation."""
            for facet_rule in allowed_facet_values:
                for facet_key, allowed_values in facet_rule.items():
                    if facet_key in fwrites:
                        facet_value = fwrites[facet_key]
                        if isinstance(allowed_values, list):
                            if facet_value not in allowed_values:
                                logger.warning(
                                    f"Disallowed value '{facet_value}' "
                                    "for facet key '{facet_key}'"
                                )
                                fwrites[facet_key] = allowed_values[0]
            return await async_func(self, user, *args, **fwrites)

        return wrapper

    return decorator


def ensure_future(
    async_func: Callable[..., Awaitable[Any]]
) -> Callable[..., Coroutine[Any, Any, asyncio.Task[Any]]]:
    """Decorator that runs any given asyncio function in the background."""

    @wraps(async_func)
    async def wrapper(*args: Any, **kwargs: Any) -> asyncio.Task[Any]:
        """Async wrapper function that creates the call."""
        try:
            loop = (
                asyncio.get_running_loop()
            )  # Safely get the current running event loop
        except RuntimeError:
            # No running event loop, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Schedule the coroutine for execution in the background
        return asyncio.ensure_future(async_func(*args, **kwargs))

    return wrapper


class SearchResult(BaseModel):
    """Return Model of a uniq key search."""

    total_count: int
    facets: Dict[str, List[Union[str, int]]]
    search_results: List[Dict[str, Union[str, float, List[str]]]]
    facet_mapping: Dict[str, str]
    primary_facets: List[str]


class IntakeCatalogue(BaseModel):
    """Return Model of a uniq key search."""

    catalogue: IntakeType
    total_count: int


@dataclass
class Translator:
    """Class that defines the flavour translation.

    Parameters
    ----------
    flavour: str
        The target flavour, the facet names should be translated to.
    translate: bool, default: True
        Translate the search keys. Not translating (default: True) can be
        useful if the actual translation of the facets should be done on the
        client side.

    Attributes
    ----------
    """

    flavour: str
    translate: bool = True
    flavours: tuple[FlavourType, ...] = (
        "freva",
        "cmip6",
        "cmip5",
        "cordex",
        "nextgems",
    )

    @property
    def facet_hierarchy(self) -> list[str]:
        """Define the hierarchy of facets that define a dataset."""
        return [
            "project",
            "product",
            "institute",
            "model",
            "experiment",
            "time_frequency",
            "realm",
            "variable",
            "ensemble",
            "cmor_table",
            "fs_type",
            "grid_label",
            "grid_id",
        ]

    @property
    def _freva_facets(self) -> Dict[str, str]:
        """Define the freva search facets and their relevance"""
        return {
            "project": "primary",
            "product": "primary",
            "institute": "primary",
            "model": "primary",
            "experiment": "primary",
            "time_frequency": "primary",
            "realm": "primary",
            "variable": "primary",
            "ensemble": "primary",
            "time_aggregation": "primary",
            "fs_type": "secondary",
            "grid_label": "secondary",
            "cmor_table": "secondary",
            "driving_model": "secondary",
            "format": "secondary",
            "grid_id": "secondary",
            "level_type": "secondary",
            "rcm_name": "secondary",
            "rcm_version": "secondary",
            "dataset": "secondary",
            "time": "secondary",
        }

    @property
    def _cmip5_lookup(self) -> Dict[str, str]:
        """Define the search facets for the cmip5 standard."""
        return {
            "experiment": "experiment",
            "ensemble": "member_id",
            "fs_type": "fs_type",
            "grid_label": "grid_label",
            "institute": "institution_id",
            "model": "model_id",
            "project": "project",
            "product": "product",
            "realm": "realm",
            "variable": "variable",
            "time": "time",
            "time_aggregation": "time_aggregation",
            "time_frequency": "time_frequency",
            "cmor_table": "cmor_table",
            "dataset": "dataset",
            "driving_model": "driving_model",
            "format": "format",
            "grid_id": "grid_id",
            "level_type": "level_type",
            "rcm_name": "rcm_name",
            "rcm_version": "rcm_version",
        }

    @property
    def _cmip6_lookup(self) -> Dict[str, str]:
        """Define the search facets for the cmip6 standard."""
        return {
            "experiment": "experiment_id",
            "ensemble": "member_id",
            "fs_type": "fs_type",
            "grid_label": "grid_label",
            "institute": "institution_id",
            "model": "source_id",
            "project": "mip_era",
            "product": "activity_id",
            "realm": "realm",
            "variable": "variable_id",
            "time": "time",
            "time_aggregation": "time_aggregation",
            "time_frequency": "frequency",
            "cmor_table": "table_id",
            "dataset": "dataset",
            "driving_model": "driving_model",
            "format": "format",
            "grid_id": "grid_id",
            "level_type": "level_type",
            "rcm_name": "rcm_name",
            "rcm_version": "rcm_version",
        }

    @property
    def _cordex_lookup(self) -> Dict[str, str]:
        """Define the search facets for the cordex5 standard."""
        return {
            "experiment": "experiment",
            "ensemble": "ensemble",
            "fs_type": "fs_type",
            "grid_label": "grid_label",
            "institute": "institution",
            "model": "model",
            "project": "project",
            "product": "domain",
            "realm": "realm",
            "variable": "variable",
            "time": "time",
            "time_aggregation": "time_aggregation",
            "time_frequency": "time_frequency",
            "cmor_table": "cmor_table",
            "dataset": "dataset",
            "driving_model": "driving_model",
            "format": "format",
            "grid_id": "grid_id",
            "level_type": "level_type",
            "rcm_name": "rcm_name",
            "rcm_version": "rcm_version",
        }

    @property
    def _nextgems_lookup(self) -> Dict[str, str]:
        """Define the search facets for the cmip5 standard."""
        return {
            "experiment": "experiment",
            "ensemble": "member_id",
            "fs_type": "fs_type",
            "grid_label": "grid_label",
            "institute": "institution_id",
            "model": "source_id",
            "project": "project",
            "product": "experiment_id",
            "realm": "realm",
            "variable": "variable_id",
            "time": "time",
            "time_aggregation": "time_reduction",
            "time_frequency": "time_frequency",
            "cmor_table": "cmor_table",
            "dataset": "dataset",
            "driving_model": "driving_model",
            "format": "format",
            "grid_id": "grid_id",
            "level_type": "level_type",
            "rcm_name": "rcm_name",
            "rcm_version": "rcm_version",
        }

    @cached_property
    def forward_lookup(self) -> Dict[str, str]:
        """Define how things get translated from the freva standard"""

        return {
            "freva": {k: k for k in self._freva_facets},
            "cmip6": self._cmip6_lookup,
            "cmip5": self._cmip5_lookup,
            "cordex": self._cordex_lookup,
            "nextgems": self._nextgems_lookup,
        }[self.flavour]

    @cached_property
    def valid_facets(self) -> list[str]:
        """Get all valid facets for a flavour."""
        if self.translate:
            return list(self.forward_lookup.values())
        return list(self.forward_lookup.keys())

    @property
    def cordex_keys(self) -> Tuple[str, ...]:
        """Define the keys that make a cordex dataset."""
        return ("rcm_name", "driving_model", "rcm_version")

    @cached_property
    def primary_keys(self) -> list[str]:
        """Define which search facets are primary for which standard."""
        if self.translate:
            _keys = [
                self.forward_lookup[k]
                for (k, v) in self._freva_facets.items()
                if v == "primary"
            ]
        else:
            _keys = [k for (k, v) in self._freva_facets.items() if v == "primary"]
        if self.flavour in ("cordex",):
            for key in self.cordex_keys:
                _keys.append(key)
        return _keys

    @cached_property
    def backward_lookup(self) -> Dict[str, str]:
        """Translate the schema to the freva standard."""
        return {v: k for (k, v) in self.forward_lookup.items()}

    def translate_facets(
        self,
        facets: Iterable[str],
        backwards: bool = False,
    ) -> List[str]:
        """Translate the facets names to a given flavour."""
        if self.translate:
            if backwards:
                return [self.backward_lookup.get(f, f) for f in facets]
            return [self.forward_lookup.get(f, f) for f in facets]
        return list(facets)

    def translate_query(
        self,
        query: Dict[str, Any],
        backwards: bool = False,
    ) -> Dict[str, Any]:
        """Translate the queries names to a given flavour."""
        return dict(
            zip(
                self.translate_facets(query.keys(), backwards=backwards),
                query.values(),
            )
        )


class Solr:
    """Definitions for making search queries on apache solr and
    ingesting the user data into the apache solr.

    Parameters
    ----------
    config: ServerConfig
        An instance of the server configuration class.
    uniq_key: str, default: file
        The type of search result, which can be either "file" or "uri". This
        parameter determines whether the search will be based on file paths or
        Uniform Resource Identifiers (URIs).
    flavour: str, default: freva
        The Data Reference Syntax (DRS) standard specifying the type of climate
        datasets to query. The available DRS standards can be retrieved using the
        ``GET /overview`` method.
    start: int, default: 0
        Specify the starting point for receiving results.
    multi_version: bool, default: False
        Use versioned datasets in stead of latest versions.
    translate: bool, default: True
        Translate the output to the required DRS flavour.

    Attributes
    ----------
    """

    uniq_keys: Tuple[str, str] = ("file", "uri")
    """The names of all unique keys in the indexing system."""

    timeout: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=30)
    """30 seconds for timeout."""

    batch_size: int = 150
    """Maximum solr batch query size for one single query result."""
    suffixes = [".nc", ".nc4", ".grb", ".grib", ".tar", ".zarr"]
    escape_chars: Tuple[str, ...] = (
        "+",
        "-",
        "&&",
        "||",
        "!",
        "(",
        ")",
        "{",
        "}",
        "[",
        "]",
        "^",
        "~",
        ":",
        "/",
    )
    """Lucene (solr) special characters that need escaping."""

    def __init__(
        self,
        config: ServerConfig,
        *,
        uniq_key: Literal["file", "uri"] = "file",
        flavour: FlavourType = "freva",
        start: int = 0,
        multi_version: bool = True,
        translate: bool = True,
        _translator: Union[None, Translator] = None,
        **query: list[str],
    ) -> None:
        self._config = config
        self.uniq_key = uniq_key
        self.multi_version = multi_version
        self.translator = _translator or Translator(flavour, translate)
        try:
            self.time = self.adjust_time_string(
                query.pop("time", [""])[0],
                query.pop("time_select", ["flexible"])[0],
            )
        except ValueError as err:
            raise HTTPException(status_code=500, detail=str(err)) from err
        self.facets = self.translator.translate_query(query, backwards=True)
        self.url, self.query = self._get_url()
        self.query["start"] = start
        self.query["sort"] = "file desc"

        self.payload: Union[List[Dict[str, Union[str, List[str], Dict[str, str]]]],
                            Dict[str, Union[str, List[str], Dict[str, str]]]] = []
        self.fwrites: Dict[str, str] = {}
        self._lock = Lock()
        self.total_files = 0
        self.current_batch: List[Dict[str, str]] = []
        self.loop = asyncio.get_event_loop()
        self.suffixes = [".nc", ".nc4", ".grb", ".grib", ".zarr", "zar"]
        self.submitted_tasks_in_excutor: List[Future[str]] = []

    @asynccontextmanager
    async def _session_get(self) -> AsyncIterator[Tuple[int, Dict[str, Any]]]:
        """Wrap the get request round a try and catch statement."""
        logger.info(
            "Query %s for uniq_key: %s with %s",
            self.url,
            self.uniq_key,
            self.query,
        )
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            try:
                async with session.get(self.url, params=self.query) as res:
                    status = res.status
                    try:
                        await self.check_for_status(res)
                        search = await res.json()
                    except HTTPException:  # pragma: no cover
                        search = {}  # pragma: no cover
            except Exception as error:
                logger.error("Connection to %s failed: %s", self.url, error)
                raise HTTPException(
                    status_code=503,
                    detail="Could not connect to search instance",
                )
        yield status, search

    @asynccontextmanager
    async def _session_post(self) -> AsyncIterator[Tuple[int, Dict[str, Any]]]:
        """Wrap the post request round a try and catch statement."""
        logger.info(
            "Sending POST request to %s for uniq_key: %s with payload: %s",
            self._post_url,
            self.uniq_key,
            self.payload,
        )
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            try:
                async with session.post(self._post_url, json=self.payload) as res:
                    try:
                        await self.check_for_status(res)
                        logger.info(
                            "POST request successful with status: %d", res.status
                        )
                        response_data = await res.json()
                    except HTTPException:  # pragma: no cover
                        logger.error(
                            "POST request failed: %s", await res.text()
                        )
                        response_data = {}
            except Exception as error:
                logger.error("Connection to %s failed: %s", self.url, error)
                raise HTTPException(
                    status_code=503,
                    detail="Could not connect to Solr POST endpoint",
                )
        yield res.status, response_data

    @classmethod
    async def validate_parameters(
        cls,
        config: ServerConfig,
        *,
        uniq_key: Literal["file", "uri"] = "file",
        flavour: FlavourType = "freva",
        start: int = 0,
        multi_version: bool = False,
        translate: bool = True,
        **query: list[str],
    ) -> "Solr":
        """Create an instance of an Solr class with parameter validation.

        Parameters
        ----------
        config: ServerConfig
            An instance of the server configuration class.
        uniq_key: str, default: file
            The type of search result, which can be either "file" or "uri". This
            parameter determines whether the search will be based on file paths or
            Uniform Resource Identifiers (URIs).
        flavour: str, default: freva
            The Data Reference Syntax (DRS) standard specifying the type of climate
            datasets to query. The available DRS standards can be retrieved using the
            ``GET /overview`` method.
        start: int, default: 0
            Specify the starting point for receiving results.
        multi_version: bool, default: False
            Use versioned datasets in stead of latest versions.
        translate: bool, default: True
            Translate the output to the required DRS flavour.
        """
        translator = Translator(flavour, translate)
        for key in query:
            key = key.lower().replace("_not_", "")
            if (
                key not in translator.valid_facets
                and key not in ("time_select",) + cls.uniq_keys
            ):
                raise HTTPException(status_code=422, detail="Could not validate input.")
        return Solr(
            config,
            flavour=flavour,
            translate=translate,
            uniq_key=uniq_key,
            start=start,
            multi_version=multi_version,
            _translator=translator,
            **query,
        )

    @staticmethod
    def adjust_time_string(
        time: str,
        time_select: str = "flexible",
    ) -> List[str]:
        """Adjust the time select keys to a solr time query

        Parameters
        ----------

        time: str, default: ""
            Special search facet to refine/subset search results by time.
            This can be a string representation of a time range or a single
            time step. The time steps have to follow ISO-8601. Valid strings are
            ``%Y-%m-%dT%H:%M`` to ``%Y-%m-%dT%H:%M`` for time ranges and
            ``%Y-%m-%dT%H:%M``. **Note**: You don't have to give the full string
            format to subset time steps ``%Y``, ``%Y-%m`` etc are also valid.
        time_select: str, default: flexible
            Operator that specifies how the time period is selected. Choose from
            flexible (default), strict or file. ``strict`` returns only those files
            that have the *entire* time period covered. The time search ``2000 to
            2012`` will not select files containing data from 2010 to 2020 with
            the ``strict`` method. ``flexible`` will select those files as
            ``flexible`` returns those files that have either start or end period
            covered. ``file`` will only return files where the entire time
            period is contained within *one single* file.

        Raises
        ------
        ValueError: If parsing the dates failed.
        """
        if not time:
            return []
        time = "".join(time.split())
        select_methods: dict[str, str] = {
            "strict": "Within",
            "flexible": "Intersects",
            "file": "Contains",
        }
        try:
            solr_select = select_methods[time_select]
        except KeyError as exc:
            methods = ", ".join(select_methods.keys())
            raise ValueError(f"Choose `time_select` from {methods}") from exc
        start, _, end = time.lower().partition("to")
        try:
            start = parse(start or "1", default=datetime(1, 1, 1, 0, 0, 0)).isoformat()
            end = parse(
                end or "9999", default=datetime(9999, 12, 31, 23, 59, 59)
            ).isoformat()
        except ParserError as exc:
            raise ValueError(exc) from exc
        return [f"{{!field f=time op={solr_select}}}[{start} TO {end}]"]

    async def _create_intake_catalogue(self, *facets: str) -> IntakeType:
        var_name = self.translator.forward_lookup["variable"]
        catalogue: IntakeType = {
            "esmcat_version": "0.1.0",
            "attributes": [
                {
                    "column_name": v,
                    "vocabulary": "",
                }
                for v in facets
            ],
            "assets": {"column_name": "uri", "format_column_name": "format"},
            "id": "freva",
            "description": f"Catalogue from freva-databrowser v{__version__}",
            "title": "freva-databrowser catalogue",
            "last_updated": f"{datetime.now().isoformat()}",
            "aggregation_control": {
                "variable_column_name": var_name,
                "groupby_attrs": [],
                "aggregations": [
                    {
                        "type": "union",
                        "attribute_name": f,
                        "options": {},
                    }
                    for f in facets
                ],
            },
        }
        return catalogue

    def _set_catalogue_queries(self) -> None:
        """Set the query parameters for an catalogue search."""
        self.query["facet"] = "true"
        self.query["facet.mincount"] = "1"
        self.query["facet.limit"] = "-1"
        self.query["rows"] = self.batch_size
        self.query["facet.field"] = self._config.solr_fields
        self.query["fl"] = [self.uniq_key] + self._config.solr_fields
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

    async def _delete_from_mongo(self, search_keys: Dict[str, str]) -> None:
        """
        Delete bulk user metadata from MongoDB based on the search keys.

        Parameters
        ~~~~~~~~~~
        search_keys : Dict[str, str]
            A dictionariy containing search keys used to identify
            data for deletion.

        Returns
        ~~~~~~~
        None
        """
        try:
            query = {
                key: value if key.lower() == "file" else value.lower()
                for key, value in search_keys.items()
            }
            await self._config.mongo_collection_userdata.delete_many(query)
            logger.info(f"Deleted metadata from MongoDB with query: {query}")
        except Exception as error:
            logger.warning(f"Could not remove metadata from MongoDB: {error}")

    async def _insert_to_mongo(self, metadata_batch: List[Dict[str, Any]]) -> None:
        """
        Bulk upsert user metadata into MongoDB.

        Parameters
        ~~~~~~~~~~
        metadata_batch : List[Dict[str, Any]]
            A list of dictionaries containing metadata to insert into MongoDB.

        Returns
        ~~~~~~~
        None
        """
        try:
            bulk_operations = []
            for metadata in metadata_batch:
                filter_query = {"file": metadata["file"], "uri": metadata["uri"]}
                update_query = {"$set": metadata}
                bulk_operations.append(
                    UpdateOne(filter_query, update_query, upsert=True)
                )

            if bulk_operations:
                await self._config.mongo_collection_userdata.bulk_write(
                    bulk_operations, ordered=False
                )
                logger.info(
                    f"Inserted or updated {len(bulk_operations)} records into MongoDB."
                )

        except Exception as error:
            logger.warning(f"Could not add metadata to MongoDB: {error}")

    @ensure_future
    async def store_results(self, num_results: int, status: int) -> None:
        """Store the query into a database.

        Parameters
        ----------
        num_results: int
            The number of files that has been found.
        status: int
            The HTTP request status
        """
        if num_results == 0:
            return
        data = {
            "num_results": num_results,
            "flavour": self.translator.flavour,
            "uniq_key": self.uniq_key,
            "server_status": status,
            "date": datetime.now(),
        }
        facets = {k: "&".join(v) for (k, v) in self.facets.items()}
        try:
            await self._config.mongo_collection_search.insert_one(
                {"metadata": data, "query": facets}
            )
        except Exception as error:
            logger.warning("Could not add stats to mongodb: %s", error)

    def _process_catalogue_result(self, out: Dict[str, List[Sized]]) -> Dict[str, Any]:
        return {
            k: (
                out[k][0]
                if isinstance(out.get(k), list) and len(out[k]) == 1
                else out.get(k)
            )
            for k in [self.uniq_key] + self.translator.facet_hierarchy
            if out.get(k)
        }

    async def _iterintake(self) -> AsyncIterator[str]:
        encoder = json.JSONEncoder(indent=3)
        self.query["cursorMark"] = "*"
        init = True
        yield ',\n   "catalog_dict": '
        while True:
            async with self._session_get() as res:
                _, results = res

            for result in results.get("response", {}).get("docs", [{}]):
                entry = self._process_catalogue_result(result)
                if init is True:
                    sep = "["
                else:
                    sep = ","
                yield f"{sep}\n   "
                init = False
                for line in list(encoder.iterencode(entry)):
                    yield line
            next_cursor_mark = results.get("nextCursorMark", None)
            if next_cursor_mark == self.query["cursorMark"] or not results:
                break
            self.query["cursorMark"] = next_cursor_mark

    async def intake_catalogue(
        self, catalogue: IntakeType, header_only: bool = False
    ) -> AsyncIterator[str]:
        """Create an intake catalogue from the solr search."""
        encoder = json.JSONEncoder(indent=3)
        for line in list(encoder.iterencode(catalogue))[:-1]:
            yield line
        if header_only is False:
            async for line in self._iterintake():
                yield line
            yield "\n   ]\n}"

    async def extended_search(
        self,
        facets: List[str],
        max_results: int,
    ) -> Tuple[int, SearchResult]:
        """Initialise the apache solr metadata search.

        Returns
        -------
        int: status code of the apache solr query.
        """
        search_facets = [f for f in facets if f not in ("*", "all")]
        self.query["facet"] = "true"
        self.query["rows"] = max_results
        self.query["facet.sort"] = "index"
        self.query["facet.mincount"] = "1"
        self.query["facet.limit"] = "-1"
        self.query["wt"] = "json"
        self.query["facet.field"] = self.translator.translate_facets(
            search_facets or self._config.solr_fields, backwards=True
        )
        self.query["fl"] = [self.uniq_key, "fs_type"]
        logger.info(
            "Query %s for uniq_key: %s with %s",
            self.url,
            self.uniq_key,
            self.query,
        )

        async with self._session_get() as res:
            search_status, search = res
        return search_status, SearchResult(
            total_count=search.get("response", {}).get("numFound", 0),
            facets=self.translator.translate_query(
                search.get("facet_counts", {}).get("facet_fields", {})
            ),
            search_results=[
                {
                    **{self.uniq_key: k[self.uniq_key]},
                    **{"fs_type": k.get("fs_type", "posix")},
                }
                for k in search.get("response", {}).get("docs", [])
            ],
            facet_mapping={
                k: self.translator.forward_lookup[k]
                for k in self.query["facet.field"]
                if k in self.translator.forward_lookup
            },
            primary_facets=self.translator.primary_keys,
        )

    async def init_stream(self) -> Tuple[int, int]:
        """Initialise the apache solr search.

        Returns
        -------
        int: status code of the apache solr query.
        """
        self.query["fl"] = ["file", "uri"]
        logger.info(
            "Query %s for uniq_key: %s with %s",
            self.url,
            self.uniq_key,
            self.query,
        )
        async with self._session_get() as res:
            search_status, search = res
        return search_status, search.get("response", {}).get("numFound", 0)

    def _join_facet_queries(self, key: str, facets: List[str]) -> Tuple[str, str]:
        """Create lucene search contain and NOT contain search queries"""

        negative, positive = [], []
        for search_value in facets:
            if key not in self.uniq_keys:
                search_value = search_value.lower()
            if search_value.lower().startswith("not "):
                "len('not ') = 4"
                negative.append(search_value[4:])
            elif search_value[0] in ("!", "-"):
                negative.append(search_value[1:])
            elif "_not_" in key:
                negative.append(search_value)
            else:
                positive.append(search_value)
        search_value_pos = " OR ".join(positive)
        search_value_neg = " OR ".join(negative)
        for char in self.escape_chars:
            search_value_pos = search_value_pos.replace(char, "\\" + char)
            search_value_neg = search_value_neg.replace(char, "\\" + char)
        return search_value_pos, search_value_neg

    def _get_url(self) -> tuple[str, Dict[str, Any]]:
        """Get the url for the solr query."""
        core = {
            True: self._config.solr_cores[0],
            False: self._config.solr_cores[-1],
        }[self.multi_version]
        url = f"{self._config.get_core_url(core)}/select/"
        query = []
        for key, value in self.facets.items():
            query_pos, query_neg = self._join_facet_queries(key, value)
            key = key.lower().replace("_not_", "")
            if query_pos:
                query.append(f"{key}:({query_pos})")
            if query_neg:
                query.append(f"-{key}:({query_neg})")
        return url, {
            "fq": self.time + ["", " AND ".join(query) or "*:*"],
            "q": "*:*",
        }

    @property
    def _post_url(self) -> str:
        """Construct the URL and payload for a solr POST request."""
        core = {
            True: self._config.solr_cores[0],
            False: self._config.solr_cores[-1],
        }[self.multi_version]
        url = f"{self._config.get_core_url(core)}/update/json?commit=true"
        return url

    async def check_for_status(
        self, response: aiohttp.client_reqrep.ClientResponse
    ) -> None:
        """Check if a query was successful

        Parameters
        ----------
        response: aiohttp.client_reqrep.ClientResponse
            The response of the rest query.

        Raises
        ------
        fastapi.HTTPException: If anything went wrong an error is risen
        """
        if response.status not in (200, 201):
            raise HTTPException(
                status_code=response.status, detail=response.text
            )  # pragma: no cover

    async def _solr_page_response(self) -> AsyncIterator[Dict[str, Any]]:
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
        """Search for uniq keys matching given search facets.

        Returns
        -------
        AsyncIterator: Stream of search results.
        """
        async for result in self._solr_page_response():
            yield f"{result[self.uniq_key]}\n"

    async def zarr_response(
        self,
        catalogue_type: Literal["intake", None],
        num_results: int,
    ) -> AsyncIterator[str]:
        """Create a zarr endpoint from a given search.
        Parameters
        ----------
        search: SearchResult
            The search result object of the search query.

        Returns
        -------
        AsyncIterator: Stream of search results.
        """
        api_path = f"{os.environ.get('API_URL', '')}/api/freva-data-portal/zarr"
        if catalogue_type == "intake":
            _, intake = await self.init_intake_catalogue()
            async for string in self.intake_catalogue(intake.catalogue, True):
                yield string
            yield ',\n   "catalog_dict": ['
        num = 1
        async for result in self._solr_page_response():
            prefix = suffix = ""
            uri = result[self.uniq_key]
            uuid5 = str(uuid.uuid5(uuid.NAMESPACE_URL, uri))
            try:
                cache = await create_redis_connection()
                await cache.publish(
                    "data-portal",
                    json.dumps({"uri": {"path": uri, "uuid": uuid5}}).encode("utf-8"),
                )
            except Exception as error:
                logger.error("Cloud not connect to redis: %s", error)
                yield "Internal error, service not available\n"
                continue
            output = f"{api_path}/{uuid5}.zarr"
            if catalogue_type == "intake":
                result[self.uniq_key] = output
                if num < num_results:
                    suffix = ","
                else:
                    suffix = ""
                output = json.dumps(self._process_catalogue_result(result), indent=3)
                prefix = "   "
            num += 1
            yield f"{prefix}{output}{suffix}\n"

        if catalogue_type == "intake":
            yield "\n   ]\n}"

    async def _add_to_solr(
        self, metadata_batch: List[Dict[str, Union[str, List[str], Dict[str, str]]]]
    ) -> None:
        """
        Add a batch of metadata to the Apache Solr cataloguing system.

        Parameters
        ~~~~~~~~~~
        metadata_batch : List[Tuple[str, Dict[str, Union[str, List[str]]]]]
            A list of tuples, each containing a file identifier and its
            associated metadata.

        Returns
        ~~~~~~~
        None
        """
        for metadata in metadata_batch:
            self.payload = [metadata]
            async with self._session_post():
                pass

    async def _delete_from_solr(self, search_keys: Dict[str, str]) -> None:
        """
        Delete user data from Apache Solr based on search keys.

        Parameters
        ~~~~~~~~~~
        search_keys : Dict[str, str]
            A dictionary of search keys used to identify data to be deleted.
            Keys are field names and values are search values.

        Returns
        ~~~~~~~
        None
        """
        def escape_special_chars(value: str) -> str:
            for char in self.escape_chars:
                if char in value:
                    value = value.replace(char, f"\\{char}")
            return value
        query_parts = []
        for key, value in search_keys.items():
            key_lower = key.lower()
            if key_lower == "file":
                escaped_value = escape_special_chars(value)
                query_parts.append(f"{key_lower}:{escaped_value}")
            else:
                escaped_value = escape_special_chars(value.lower())
                query_parts.append(f"{key_lower}:{escaped_value}")
        query_str = " AND ".join(query_parts)
        self.payload = {"delete": {"query": query_str}}
        async with self._session_post():
            pass

    async def _ingest_user_data(self) -> None:
        """
        Ingest user data by processing all validated paths asynchronously.

        Returns
        ~~~~~~~
        None
        """
        tasks = [
            asyncio.ensure_future(self._iter_paths(path))
            for path in self.validated_userdata
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # TODO: needs to be refactored
    def get_executor_info(self) -> None:

        logger.debug("Executor Information:")
        logger.debug(f"Max workers: {self.executor._max_workers}")
        active_threads = [
            thread for thread in threading.enumerate()
            if thread.name and thread.name.startswith(
                cast(str, self.executor._thread_name_prefix))
        ]
        logger.info(f"Active executor threads: {len(active_threads)}")
        for thread in active_threads:
            logger.info(f"  Thread name: {thread.name}, Thread ID: {thread.ident}")
        for i, future in enumerate(self.submitted_tasks_in_excutor):
            if future.done():
                status = "Completed"
            elif future.running():
                status = "Running"
            else:
                status = "Pending"
            logger.debug(f"  Task {i}: Status - {status}")

    async def _iter_paths(self, path: os.PathLike[str]) -> None:
        """
        Iterate over files in a directory or process a single file and
        and submit the crawling and ingeting metadata to the executor.

        Parameters
        ~~~~~~~~~~
        path : os.PathLike
            The path to either a file or a directory containing user data.

        Returns
        ~~~~~~~
        None
        """
        paths_to_process = []
        if Path(path).is_file() and any(
            Path(path).suffix == suffix for suffix in self.suffixes
        ):
            paths_to_process.append(path)
        elif Path(path).is_dir():
            async for file_path in self._gather_files_from_dir(Path(path)):
                paths_to_process.append(file_path)
                if len(paths_to_process) >= self.batch_size:
                    future = self.executor.submit(
                        run_async_in_thread,
                        self._process_paths_in_executor,
                        paths_to_process,
                    )
                    self.submitted_tasks_in_excutor.append(future)
                    paths_to_process = []

        if paths_to_process:
            future = self.executor.submit(
                run_async_in_thread, self._process_paths_in_executor, paths_to_process
            )
            self.submitted_tasks_in_excutor.append(future)
        self.get_executor_info()

    async def _gather_files_from_dir(self, path: Path) -> AsyncIterator[Path]:
        """
        Gather files from a directory based on specific patterns and suffixes.

        Parameters
        ~~~~~~~~~~
        path : Path
            The directory path to scan for files.

        Yields
        ~~~~~~
        Path
            Files that match the given suffix patterns.
        """
        for item in Path(path).rglob("*.*"):
            if item.is_file() and any(
                item.suffix == suffix for suffix in self.suffixes
            ):
                yield item

    async def _process_paths_in_executor(self, paths: List[Path]) -> None:
        """
        Process a batch of file paths to crawl metadata and ingest them
        to Solr and MongoDB.

        Parameters
        ~~~~~~~~~~
        paths : List[Path]
            A list of file paths to be processed.

        Returns
        ~~~~~~
        None
        """
        metadata_collection: list[dict[str, str | list[str] | dict[str, str]]] = []
        for path in paths:
            metadata = await self._get_metadata(path)
            if isinstance(metadata, Exception) or metadata == {}:
                logger.warning("Error getting metadata: %s", metadata)
            else:
                metadata_collection.append(metadata)

        if metadata_collection:
            await self._add_to_solr(metadata_collection)
            try:
                asyncio.run_coroutine_threadsafe(
                    self._insert_to_mongo(metadata_collection), self.loop
                )
            except Exception as e:
                logger.warning(f"Error while adding data to MongoDB: {e}")
            self.total_files += len(metadata_collection)

    @ensure_future
    async def _purge_user_data(self, search_keys: Dict[str, str]) -> None:
        """
        Purge the user data from both the Apache Solr search system and MongoDB.

        Parameters
        ~~~~~~~~~~
        search_keys : Dict[str, str]
            A list of dictionaries containing search keys used to identify the
            data to be purged.

        Returns
        ~~~~~~~
        None
        """
        await self._delete_from_solr(search_keys)
        await self._delete_from_mongo(search_keys)

    async def _validating_userdata_paths(
        self, *paths: Union[str, os.PathLike[str]]
    ) -> List[os.PathLike[str]]:
        """
        Validate the user data input paths by checking if each path exists.

        Parameters
        ~~~~~~~~~~
        *paths : os.PathLike
            One or more paths to validate.

        Returns
        ~~~~~~~~~~
        List[os.PathLike]
            A list of valid paths that exist.
        """
        validated_paths: List[os.PathLike[str]] = []
        for path in paths:
            if not Path(path).exists():
                logger.warning(f"The path {str(path)} does not exist")
            else:
                validated_paths.append(cast(os.PathLike[str], path))

        if not validated_paths:
            raise FileNotFoundError("No valid paths found")

        return validated_paths

    def _timedelta_to_cmor_frequency(self, dt: float) -> str:
        """
        Convert a time delta to a CMOR-compliant frequency string.

        Parameters
        ~~~~~~~~~~
        dt : float
            Time delta in seconds.

        Returns
        ~~~~~~~
        str
            The CMOR frequency corresponding to the time delta.
        """
        for total_seconds, frequency in self.time_table.items():
            if dt >= total_seconds:
                return frequency
        return "fx"  # pragma: no cover

    @property
    def time_table(self) -> dict[int, str]:
        """
        Provide a mapping from time intervals (in seconds) to CMOR frequency strings.

        Returns
        ~~~~~~~
        Dict[int, str]
            A dictionary mapping time intervals (seconds) to CMOR frequencies.
        """
        return {
            315360000: "dec",  # Decade
            31104000: "yr",  # Year
            2538000: "mon",  # Month
            1296000: "sem",  # Seasonal (half-year)
            84600: "day",  # Day
            21600: "6h",  # Six-hourly
            10800: "3h",  # Three-hourly
            3600: "hr",  # Hourly
            1: "subhr",  # Sub-hourly
        }

    def get_time_frequency(self, time_delta: int, freq_attr: str = "") -> str:
        """
        Determine the CMOR-compliant time frequency based on the time delta
        and/or frequency attribute.

        Parameters
        ~~~~~~~~~~
        time_delta : int
            The time delta in seconds between consecutive time steps.
        freq_attr : str, optional
            The time frequency attribute that might already exist in
            the data, by default "".

        Returns
        ~~~~~~~
        str
            The CMOR-compliant time frequency.
        """
        if freq_attr in self.time_table.values():
            return freq_attr
        return self._timedelta_to_cmor_frequency(time_delta)

    async def _get_metadata(self, file_name: os.PathLike[str]
                            ) -> Dict[str, Union[str, List[str], Dict[str, str]]]:
        """
        Read metadata information from a given file using xarray.

        Parameters
        ~~~~~~~~~~
        file_name: os.PathLike
            The input file to read metadata from.

        Returns
        ~~~~~~~
        dict[str, str]
            A dictionary holding metadata information as key-value pairs.
            The metadata includes:
                - variable: The primary variable name from the file.
                - time_frequency: The time frequency of the data.
                - time: The time range or "fx" if no time data is present.
                - cmor_table: The CMOR table, defaults to the time frequency.
                - version: Version information, defaults to an empty string
                  if not found.
                - file: The file path.
                - uri: The file URI.

        """

        loop = asyncio.get_running_loop()

        def open_dataset_with_lock() -> xr.Dataset:
            with self._lock:
                with xr.open_mfdataset(
                    str(file_name), parallel=False, use_cftime=True, lock=False
                ) as dset:
                    return dset

        try:
            dset = await loop.run_in_executor(None, open_dataset_with_lock)
            time_freq = dset.attrs.get("frequency", "")
            data_vars = list(map(str, dset.data_vars))
            coords = list(map(str, dset.coords))

            try:
                times = dset["time"].values[:]
            except (KeyError, IndexError, TypeError):
                times = np.array([])

        except Exception as error:
            logger.warning("Failed to open data file %s: %s", str(file_name), error)
            return {}

        if len(times) > 0:
            time_str = f"[{times[0].isoformat()}Z TO {times[-1].isoformat()}Z]"
        else:
            time_str = "fx"

        if len(times) > 1:
            dt = abs((times[1] - times[0]).total_seconds())
        else:
            dt = 0

        variables = []
        for var in data_vars:
            if var in coords:
                continue
            if any(term in var.lower() for term in ["lon", "lat", "bnds", "x", "y"]):
                continue
            if var.lower() in ["rotated_pole", "rot_pole"]:
                continue
            variables.append(var)

        if len(variables) != 1:
            logger.error("Only one data variable allowed, found: %s", variables)

        _data = self.fwrites.copy()

        _data.setdefault("variable", variables[0])
        _data.setdefault("time_frequency", self.get_time_frequency(dt, time_freq))
        _data["time"] = time_str
        _data.setdefault("cmor_table", _data["time_frequency"])
        _data.setdefault("version", "")
        _data["file"] = str(file_name)
        _data["uri"] = str(file_name)

        return cast(Dict[str, Union[str, list[str], Dict[str, str]]], _data)

    @fixed_facets(
        [
            {"fs_type": ["posix"]},
        ]
    )
    async def add_userdata(
        self, user: str, *paths: Union[str, os.PathLike[str]], **fwrites: Dict[str, str]
    ) -> None:
        """
        Add user data to the Apache Solr search system and MongoDB.

        This method validates user data paths, prepares the necessary information,
        and ingests the data into the Apache Solr and MongoDB systems.

        Parameters:
        ~~~~~~~~~~
        - user (str): The identifier of the user whose data is being added.
        - *paths (os.PathLike): One or more file system paths that contain the
           user data.
        - **fwrites (Dict[str, str]): Key-value pairs representing additional
          metadata to be written,

        Returns:
        ~~~~~~~~
        - None
        """
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=min(mp.cpu_count(), 15)
        )

        try:
            self.validated_userdata = await self._validating_userdata_paths(*paths)
            self.fwrites |= {"user": user} | cast(Dict[str, str], fwrites)
            await self._ingest_user_data()

        finally:
            self.executor.shutdown(wait=True)
            logger.info(
                "Shutting down executor. Total ingested files: %s", self.total_files
            )

    async def delete_userdata(
        self, user: str, search_keys: Dict[str, Union[str, int]]
    ) -> None:
        """
        Delete user data from the Apache Solr search system and MongoDB.

        This method deletes the data associated with a user from both the Solr
        search system and MongoDB, using specific search keys to find and then
        purge the data.

        Parameters:
        ~~~~~~~~~~
        - user (str): The identifier of the user whose data is being deleted.
        - search_keys (Dict[str, Union[str, int]]): A dictionary of keys used to
          identify the data to be deleted.
        """
        search_keys["user"] = user
        await self._purge_user_data(search_keys)
        logger.info("Deleted user data from Solr: %s", search_keys)
