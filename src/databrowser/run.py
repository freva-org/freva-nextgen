"""Main script that runs the rest API."""

import logging
import os
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel


from .core import FlavourType, SearchResult, SolrSearch
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


class Query(BaseModel):
    query: Optional[Dict[str, str]] = None


class FacetResults(BaseModel):
    facets: Dict[str, List[Union[str, int]]]


@app.get("/intake_catalogue/{flavour}/{uniq_key}")
async def intake_catalogue(
    flavour: FlavourType, uniq_key: Literal["file", "uri"], request: Request
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
    flavour: FlavourType, uniq_key: Literal["file", "uri"], request: Request
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
