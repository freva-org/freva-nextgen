"""Main script that runs the rest API."""

from typing import Annotated, Any, List, Literal, Union

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from freva_rest.logger import logger
from freva_rest.rest import app, server_config

from .core import FlavourType, SolrSearch, Translator
from .schema import FacetResults, Required, SearchFlavours, SolrSchema


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Close the MongoDB connection on application shutdown."""
    try:
        server_config.mongo_client.close()
    except Exception as error:  # pragma: no cover
        logger.warning("Could not shutdown mongodb connection: %s", error)


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
    start: Annotated[int, SolrSchema.params["start"]] = 0,
    multi_version: Annotated[bool, SolrSchema.params["multi_version"]] = False,
    translate: Annotated[bool, SolrSchema.params["translate"]] = True,
    max_results: Annotated[int, SolrSchema.params["max_results"]] = -1,
    request: Request = Required,
) -> StreamingResponse:
    """Create an intake catalogue from a freva search."""
    solr_search = await SolrSearch.validate_parameters(
        server_config,
        flavour=flavour,
        uniq_key=uniq_key,
        start=start,
        multi_version=multi_version,
        translate=translate,
        **SolrSchema.process_parameters(request),
    )
    status_code, result = await solr_search.init_intake_catalogue()
    await solr_search.store_results(result.total_count, status_code)
    if result.total_count == 0:
        raise HTTPException(status_code=404, detail="No results found.")
    if result.total_count > max_results and max_results > 0:
        raise HTTPException(status_code=413, detail="Result stream too big.")
    file_name = f"IntakeEsmCatalogue_{flavour}_{uniq_key}.json"
    return StreamingResponse(
        solr_search.intake_catalogue(result),
        status_code=status_code,
        media_type="application/x-ndjson",
        headers={"Content-Disposition": f'attachment; filename="{file_name}"'},
    )


@app.get("/api/databrowser/metadata_search/{flavour}/{uniq_key}")
async def metadata_search(
    flavour: FlavourType,
    uniq_key: Literal["file", "uri"],
    multi_version: Annotated[bool, SolrSchema.params["multi_version"]] = False,
    translate: Annotated[bool, SolrSchema.params["translate"]] = True,
    facets: Annotated[
        Union[List[str], None], SolrSchema.params["facets"]
    ] = None,
    request: Request = Required,
) -> JSONResponse:
    """Get the search facets."""
    solr_search = await SolrSearch.validate_parameters(
        server_config,
        flavour=flavour,
        uniq_key=uniq_key,
        multi_version=multi_version,
        translate=translate,
        start=0,
        **SolrSchema.process_parameters(request),
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
    start: Annotated[int, SolrSchema.params["start"]] = 0,
    multi_version: Annotated[bool, SolrSchema.params["multi_version"]] = False,
    translate: Annotated[bool, SolrSchema.params["translate"]] = True,
    max_results: Annotated[int, SolrSchema.params["batch_size"]] = 150,
    facets: Annotated[
        Union[List[str], None], SolrSchema.params["facets"]
    ] = None,
    request: Request = Required,
) -> JSONResponse:
    """Get the search facets."""
    solr_search = await SolrSearch.validate_parameters(
        server_config,
        flavour=flavour,
        uniq_key=uniq_key,
        start=start,
        multi_version=multi_version,
        translate=translate,
        **SolrSchema.process_parameters(request),
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
    start: Annotated[int, SolrSchema.params["start"]] = 0,
    multi_version: Annotated[bool, SolrSchema.params["multi_version"]] = False,
    translate: Annotated[bool, SolrSchema.params["translate"]] = True,
    request: Request = Required,
) -> StreamingResponse:
    """Search for datasets."""
    solr_search = await SolrSearch.validate_parameters(
        server_config,
        flavour=flavour,
        uniq_key=uniq_key,
        start=start,
        multi_version=multi_version,
        translate=translate,
        **SolrSchema.process_parameters(request),
    )
    status_code, result = await solr_search.init_stream()
    await solr_search.store_results(result.total_count, status_code)
    return StreamingResponse(
        solr_search.stream_response(result),
        status_code=status_code,
        media_type="text/plain",
    )
