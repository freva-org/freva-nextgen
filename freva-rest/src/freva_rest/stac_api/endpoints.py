"""Replicate the STAC-API Endpoints."""

from typing import Any, Dict, Optional, List

from fastapi import Query, Request
from fastapi.responses import (
    JSONResponse,
    StreamingResponse
)

from freva_rest.rest import app, server_config

from .schema import STACAPISchema, CONFORMANCE_URLS
from .core import STACAPI

# TODO: adding the description of each endpoint


@app.get(
    "/api/freva-nextgen/stacapi/",
    tags=["STAC API"],
    status_code=200,
    response_model=Dict[str, Any]
)
async def landing_page() -> JSONResponse:
    """STAC API landing page."""
    stac_instance = STACAPI(server_config)
    response = await stac_instance.get_landing_page()
    return JSONResponse(response)

#TODO: we need to figure out what we need to return here
@app.get(
    "/api/freva-nextgen/stacapi/conformance",
    tags=["STAC API"],
    status_code=200,
    response_model=List[str], #TODO: has to be change to the proper model
)
async def conformance() -> JSONResponse:
    """STAC API conformance declaration."""
    response = {
        "conformsTo": CONFORMANCE_URLS
    }
    return JSONResponse(response)


@app.get(
    "/api/freva-nextgen/stacapi/collections",
    tags=["STAC API"],
    status_code=200,
)
async def collections() -> StreamingResponse:
    """List all collections."""

    stacapi_instance = STACAPI(server_config)
    return StreamingResponse(
        stacapi_instance.get_collections(),
        media_type="application/json",
    )


@app.get(
    "/api/freva-nextgen/stacapi/collections/{collection_id}",
    tags=["STAC API"],
    status_code=200,
    response_model=Dict[str, Any],  # TODO: Define a proper model
)
async def collection(collection_id: str) -> JSONResponse:
    """Get a specific collection by ID."""
    # TODO: figure out, if we need to validate the query parameters here or not?
    stacapi_instance = STACAPI(server_config)
    # TODO: in this current design it returns the collection query of whatever we put as collection_id
    # It needs to be fixed
    collection = await stacapi_instance.get_collection(collection_id)
    return JSONResponse(collection.dict(exclude_none=True), media_type="application/json")


@app.get(
    "/api/freva-nextgen/stacapi/collections/{collection_id}/items",
    tags=["STAC API"],
    status_code=200,
)
async def collection_items(
    request: Request,
    collection_id: str,
    limit: Optional[int] = Query(10, ge=1, le=1000),
    token: Optional[str] = Query(None, title="Token", description="Pagination token"),
    datetime: Optional[str] = Query(None, title="Datetime", description="Datetime range (RFC 3339) format: start-date/end-date or exact-date"),
    bbox: Optional[str] = Query(None, title="Bounding Box", description="Bounding box in format: minx,miny,maxx,maxy"),

) -> StreamingResponse:
    """List items in a collection."""
    stac_instance = await STACAPI.validate_parameters(
        config=server_config,
        limit=limit,
        token=token,
        datetime=datetime,
        bbox=bbox,
        **STACAPISchema.process_parameters(request),
    )
    return StreamingResponse(
        stac_instance.get_collection_items(collection_id, limit, token, datetime, bbox),
        media_type="application/json",
    )

@app.get(
    "/api/freva-nextgen/stacapi/collections/{collection_id}/items/{item_id}",
    tags=["STAC API"],
    status_code=200,
)
async def collection_item(
    collection_id: str,
    item_id: str,
) -> JSONResponse:
    """Get a specific item by ID."""
    stac_instance = STACAPI(server_config)
    item = await stac_instance.get_collection_item(collection_id, item_id)
    return JSONResponse(
        item.to_dict(),
        media_type="application/json",
    )

