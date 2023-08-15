"""Main script that runs the rest API."""

import os
from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, Union
from urllib.parse import parse_qs

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Required

from ._version import __version__
from .config import ServerConfig, defaults
from .core import FlavourType, SolrSearch, Translator
from .logger import logger

app = FastAPI(
    debug=bool(int(os.environ.get("DEBUG", "0"))),
    title=defaults["NAME"],
    description="Key-Value pair data search api",
    version=__version__,
)

solr_config = ServerConfig(
    Path(os.environ.get("API_CONFIG", defaults["API_CONFIG"])),
    debug=bool(os.environ.get("DEBUG", int(defaults["DEBUG"]))),
)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Close the MongoDB connection on application shutdown."""
    try:
        solr_config.mongo_client.close()
    except Exception as error:  # pragma: no cover
        logger.warning("Could not shutdown mongodb connection: %s", error)


class SolrConfig:
    """Class holding all apache solr config parameters."""

    params: dict[str, Any] = {
        "batch_size": Query(
            alias="max-results",
            title="Max. results",
            description="Control the number of maximum result items returned.",
            ge=0,
            le=1500,
        ),
        "start": Query(
            alias="start",
            title="Start",
            description="Specify the starting point for receiving results.",
            ge=0,
        ),
        "multi_version": Query(
            alias="multi-version",
            title="Multi Version",
            description="Use versioned datasets instead of latest versions.",
        ),
        "translate": Query(
            title="Translate",
            alias="translate",
            description="Translate the output to the required DRS flavour.",
        ),
        "facets": Query(
            title="Facets",
            alias="facets",
            description=(
                "The facets that should be part of the output, "
                "by default all facets will be returned."
            ),
        ),
        "max_results": Query(
            title="Max. Results",
            alias="max-results",
            description=(
                "Raise an Error if more results are found than that"
                "number, -1 for all results."
            ),
        ),
    }

    @staticmethod
    def process_parameters(request: Request) -> dict[str, list[str]]:
        """Convert Starlette Request QueryParams to a dictionary."""

        query = parse_qs(str(request.query_params))
        for key in ("uniq_key", "flavour"):
            _ = query.pop("key", [""])
        for key, param in SolrConfig.params.items():
            _ = query.pop(key, [""])
            _ = query.pop(param.alias, [""])

        return query


class FacetResults(BaseModel):
    facets: Dict[str, List[Union[str, int]]]


class SearchFlavours(BaseModel):
    flavours: List[FlavourType]
    attributes: Dict[FlavourType, List[str]]


@app.get("/api/databrowser/overview")
async def overview() -> SearchFlavours:
    """Get all available search flavours and thier attributes."""
    attributes = {}
    for flavour in Translator.flavours:
        translator = Translator(flavour)
        if flavour in ("cordex",):
            attributes[flavour] = list(translator.foreward_lookup.values())
        else:
            attributes[flavour] = [
                f
                for f in translator.foreward_lookup.values()
                if f not in translator.cordex_keys
            ]
    return SearchFlavours(
        flavours=list(Translator.flavours), attributes=attributes
    )


@app.get("/api/databrowser/intake_catalogue/{flavour}/{uniq_key}")
async def intake_catalogue(
    flavour: FlavourType,
    uniq_key: Literal["file", "uri"],
    start: Annotated[int, SolrConfig.params["start"]] = 0,
    multi_version: Annotated[bool, SolrConfig.params["multi_version"]] = False,
    translate: Annotated[bool, SolrConfig.params["translate"]] = True,
    max_results: Annotated[int, SolrConfig.params["max_results"]] = -1,
    request: Request = Required,
) -> StreamingResponse:
    """Create an intake catalogue from a freva search."""
    solr_search = await SolrSearch.validate_parameters(
        solr_config,
        flavour=flavour,
        uniq_key=uniq_key,
        start=start,
        multi_version=multi_version,
        translate=translate,
        **SolrConfig.process_parameters(request),
    )
    status_code, result = await solr_search.init_intake_catalogue()
    await solr_search.store_results(result.total_count, status_code)
    if result.total_count == 0:
        raise HTTPException(status_code=400, detail="No results found.")
    elif result.total_count > max_results and max_results > 0:
        raise HTTPException(status_code=400, detail="Result stream too big.")
    return StreamingResponse(
        solr_search.intake_catalogue(result),
        status_code=status_code,
        media_type="application/x-ndjson",
    )


@app.get("/api/databrowser/metadata_search/{flavour}/{uniq_key}")
async def metadata_search(
    flavour: FlavourType,
    uniq_key: Literal["file", "uri"],
    start: Annotated[int, SolrConfig.params["start"]] = 0,
    multi_version: Annotated[bool, SolrConfig.params["multi_version"]] = False,
    translate: Annotated[bool, SolrConfig.params["translate"]] = True,
    facets: Annotated[
        Union[List[str], None], SolrConfig.params["facets"]
    ] = None,
    request: Request = Required,
) -> JSONResponse:
    """Get the search facets."""
    solr_search = await SolrSearch.validate_parameters(
        solr_config,
        flavour=flavour,
        uniq_key=uniq_key,
        start=start,
        multi_version=multi_version,
        translate=translate,
        **SolrConfig.process_parameters(request),
    )
    status_code, result = await solr_search.extended_search(
        facets or [], max_results=0
    )
    await solr_search.store_results(result.total_count, status_code)
    output = result.dict()
    del output["search_results"]
    return JSONResponse(content=output, status_code=status_code)


@app.get("/api/databrowser/extended_search/{flavour}/{uniq_key}")
async def extended_search(
    flavour: FlavourType,
    uniq_key: Literal["file", "uri"],
    start: Annotated[int, SolrConfig.params["start"]] = 0,
    multi_version: Annotated[bool, SolrConfig.params["multi_version"]] = False,
    translate: Annotated[bool, SolrConfig.params["translate"]] = True,
    max_results: Annotated[int, SolrConfig.params["batch_size"]] = 150,
    facets: Annotated[
        Union[List[str], None], SolrConfig.params["facets"]
    ] = None,
    request: Request = Required,
) -> JSONResponse:
    """Get the search facets."""
    solr_search = await SolrSearch.validate_parameters(
        solr_config,
        flavour=flavour,
        uniq_key=uniq_key,
        start=start,
        multi_version=multi_version,
        translate=translate,
        **SolrConfig.process_parameters(request),
    )
    status_code, result = await solr_search.extended_search(
        facets or [], max_results=max_results
    )
    await solr_search.store_results(result.total_count, status_code)
    return JSONResponse(content=result.dict(), status_code=status_code)


@app.get("/api/databrowser/data_search/{flavour}/{uniq_key}")
async def data_search(
    flavour: FlavourType,
    uniq_key: Literal["file", "uri"],
    start: Annotated[int, SolrConfig.params["start"]] = 0,
    multi_version: Annotated[bool, SolrConfig.params["multi_version"]] = False,
    translate: Annotated[bool, SolrConfig.params["translate"]] = True,
    request: Request = Required,
) -> StreamingResponse:
    """Search for datasets."""
    solr_search = await SolrSearch.validate_parameters(
        solr_config,
        flavour=flavour,
        uniq_key=uniq_key,
        start=start,
        multi_version=multi_version,
        translate=translate,
        **SolrConfig.process_parameters(request),
    )
    status_code, result = await solr_search.init_stream()
    await solr_search.store_results(result.total_count, status_code)
    return StreamingResponse(
        solr_search.stream_response(result),
        status_code=status_code,
        media_type="text/plain",
    )
