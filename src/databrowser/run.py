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

app = FastAPI(
    debug=bool(os.environ.get("DEBUG", int(defaults["DEBUG"]))),
    title=defaults["NAME"],
    description="Key-Value pair data search api",
    version=__version__,
)
solr_config = ServerConfig(
    Path(os.environ.get("API_CONFIG", defaults["API_CONFIG"])),
    debug=bool(os.environ.get("DEBUG", int(defaults["DEBUG"]))),
)


class SolrConfig:
    """Class holding all apache solr config parameters."""

    params: dict[str, Any] = {
        "batch-size": Query(
            alias="batch_size",
            title="Batch size",
            description="Control the number of maximum items returned.",
            ge=1,
            le=1500,
        ),
        "start": Query(
            alias="start",
            title="Start",
            description="Specify the starting point for receiving results.",
            ge=0,
        ),
        "multi-version": Query(
            alias="multi_version",
            title="Multi Version",
            description="Use versioned datasets in stead of latest versions.",
        ),
        "translate": Query(
            title="Translate",
            alias="translate",
            description="Translate the output to the required DRS flavour.",
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


@app.get("/overview")
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


@app.get("/intake_catalogue/{flavour}/{uniq_key}")
async def intake_catalogue(
    flavour: FlavourType,
    uniq_key: Literal["file", "uri"],
    batch_size: Annotated[int, SolrConfig.params["batch-size"]] = 150,
    start: Annotated[int, SolrConfig.params["start"]] = 0,
    multi_version: Annotated[bool, SolrConfig.params["multi-version"]] = True,
    translate: Annotated[bool, SolrConfig.params["translate"]] = True,
    request: Request = Required,
) -> StreamingResponse:
    """Create an intake catalogue from a freva search."""
    solr_search = SolrSearch(
        solr_config,
        flavour=flavour,
        uniq_key=uniq_key,
        batch_size=batch_size,
        start=start,
        multi_version=multi_version,
        translate=translate,
        **SolrConfig.process_parameters(request),
    )
    status_code, result = await solr_search.init_intake_catalogue()
    if result.total_count == 0:
        raise HTTPException(status_code=400, detail="No results found.")
    return StreamingResponse(
        solr_search.intake_catalogue(result),
        status_code=status_code,
        media_type="application/x-ndjson",
    )


@app.get("/metadata_search/{flavour}/{uniq_key}")
async def metadata_search(
    flavour: FlavourType,
    uniq_key: Literal["file", "uri"],
    batch_size: Annotated[int, SolrConfig.params["batch-size"]] = 150,
    start: Annotated[int, SolrConfig.params["start"]] = 0,
    multi_version: Annotated[bool, SolrConfig.params["multi-version"]] = True,
    translate: Annotated[bool, SolrConfig.params["translate"]] = True,
    request: Request = Required,
) -> JSONResponse:
    """Get the search facets."""
    solr_search = SolrSearch(
        solr_config,
        flavour=flavour,
        uniq_key=uniq_key,
        batch_size=batch_size,
        start=start,
        multi_version=multi_version,
        translate=translate,
        **SolrConfig.process_parameters(request),
    )
    status_code, result = await solr_search.metadata_search()
    return JSONResponse(content=result.dict(), status_code=status_code)


@app.get("/databrowser/{flavour}/{uniq_key}")
async def databrowser(
    flavour: FlavourType,
    uniq_key: Literal["file", "uri"],
    batch_size: Annotated[int, SolrConfig.params["batch-size"]] = 150,
    start: Annotated[int, SolrConfig.params["start"]] = 0,
    multi_version: Annotated[bool, SolrConfig.params["multi-version"]] = True,
    translate: Annotated[bool, SolrConfig.params["translate"]] = True,
    request: Request = Required,
) -> StreamingResponse:
    """Search for datasets."""
    solr_search = SolrSearch(
        solr_config,
        flavour=flavour,
        uniq_key=uniq_key,
        batch_size=batch_size,
        start=start,
        multi_version=multi_version,
        translate=translate,
        **SolrConfig.process_parameters(request),
    )
    status_code, result = await solr_search.init_stream()
    return StreamingResponse(
        solr_search.stream_response(result),
        status_code=status_code,
        media_type="text/plain",
    )
