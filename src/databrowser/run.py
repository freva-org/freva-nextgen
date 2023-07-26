"""Main script that runs the rest API."""

import os
from pathlib import Path
from typing import Dict, List, Literal, Union

from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    Query,
)
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel


from .core import FlavourType, SolrSearch, Translator
from .config import ServerConfig, defaults
from ._version import __version__

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


class FacetResults(BaseModel):
    facets: Dict[str, List[Union[str, int]]]


class SearchFlavours(BaseModel):
    flavours: List[FlavourType]
    attributes: Dict[FlavourType, List[str]]


@app.get("/search_attributes")
async def search_attributes() -> SearchFlavours:
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
    request: Request,
) -> StreamingResponse:
    """Create an intake catalogue from a freva search."""
    solr_search = SolrSearch(
        solr_config,
        flavour=flavour,
        uniq_key=uniq_key,
        query_params=str(request.query_params),
    )
    status_code, result = await solr_search.init_intake_catalogue()
    if result.total_count == 0:
        raise HTTPException(status_code=400, detail="No results found.")
    return StreamingResponse(
        solr_search.intake_catalogue(result),
        status_code=status_code,
        media_type="application/x-ndjson",
    )


@app.get("/facet_search/{flavour}/{uniq_key}")
async def search_facets(
    flavour: FlavourType,
    uniq_key: Literal["file", "uri"],
    request: Request,
) -> JSONResponse:
    """Get the search facets."""
    solr_search = SolrSearch(
        solr_config,
        flavour=flavour,
        uniq_key=uniq_key,
        query_params=str(request.query_params),
    )
    status_code, result = await solr_search.facet_search()
    return JSONResponse(content=result.dict(), status_code=status_code)


@app.get("/databrowser/{flavour}/{uniq_key}")
async def databrowser(
    flavour: FlavourType, uniq_key: Literal["file", "uri"], request: Request
) -> StreamingResponse:
    """Search for datasets."""
    solr_search = SolrSearch(
        solr_config,
        flavour=flavour,
        uniq_key=uniq_key,
        query_params=str(request.query_params),
    )
    status_code, result = await solr_search.init_stream()
    return StreamingResponse(
        solr_search.stream_response(result),
        status_code=status_code,
        media_type="text/plain",
    )
