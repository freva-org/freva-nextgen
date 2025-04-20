"""Replicate the STAC-API Endpoints."""

from typing import Optional

from fastapi import Query, Request

###################################################
# TEMPORARY: IT WORKS ONLY FOR TESTING PURPOSES
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    JSONResponse,
    PlainTextResponse,
    StreamingResponse,
)

from freva_rest.rest import app, server_config

from .core import STACAPI
from .schema import CONFORMANCE_URLS, STACAPISchema

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
##################################################


@app.get(
    "/api/freva-nextgen/stacapi/",
    tags=["STAC API"],
    status_code=200,
    responses={
        503: {"description": "Search backend error"}
    },
    response_class=JSONResponse
)
async def landing_page() -> JSONResponse:
    """STAC API landing page declaration.

    This endpoint provides the landing page of the STAC API,
    which includes information about the API version, title,
    description, and links to collections and other resources.
    The landing page serves as an entry point for users to
    explore the available collections and items in the STAC API.
    """
    stac_instance = STACAPI(server_config)
    response = await stac_instance.get_landing_page()
    return JSONResponse(response)


# TODO: we need to figure out what we need to return here
@app.get(
    "/api/freva-nextgen/stacapi/conformance",
    tags=["STAC API"],
    status_code=200,
    responses={
        503: {"description": "Search backend error"}
    },
    response_class=JSONResponse
)
async def conformance() -> JSONResponse:
    """STAC API conformance declaration.

    This endpoint returns the conformance classes that the STAC API
    implementation conforms to. It provides information about the
    supported features and capabilities of the API.
    """
    response = {
        "conformsTo": CONFORMANCE_URLS
    }
    return JSONResponse(response)


@app.get(
    "/api/freva-nextgen/stacapi/collections",
    tags=["STAC API"],
    status_code=200,
    responses={
        503: {"description": "Search backend error"}
    },
    response_class=PlainTextResponse,
)
async def collections() -> StreamingResponse:
    """List all collections in the STAC API.

    This endpoint retrieves a list of all collections available in the STAC API.
    Each collection represents a group of related items and provides metadata
    about the collection, including its ID, title, description, and spatial
    and temporal extents.
    """

    stacapi_instance = STACAPI(server_config)
    return StreamingResponse(
        stacapi_instance.get_collections(),
        media_type="application/json",
    )


@app.get(
    "/api/freva-nextgen/stacapi/collections/{collection_id}",
    tags=["STAC API"],
    status_code=200,
    responses={
        404: {"description": "Collection not found"},
        503: {"description": "Search backend error"}
    },
    response_class=JSONResponse,
)
async def collection(collection_id: str) -> JSONResponse:
    """Get a specific collection.

    This endpoint retrieves a specific collection from the STAC API.
    The collection is identified by its ID.
    """
    stacapi_instance = STACAPI(server_config)
    collection = await stacapi_instance.get_collection(collection_id)
    return JSONResponse(collection.dict(exclude_none=True),
                        media_type="application/json")


@app.get(
    "/api/freva-nextgen/stacapi/collections/{collection_id}/items",
    tags=["STAC API"],
    status_code=200,
    responses={
        422: {"description": "Invalid query parameters"},
        503: {"description": "Search backend error"},
    },
    response_class=PlainTextResponse,
)
async def collection_items(
    request: Request,
    collection_id: str,
    limit: int = Query(10, ge=1, le=1000),
    token: Optional[str] = Query(
        None,
        title="Token",
        description="Pagination token in format direction:collection_id:item_id, \
                    where direction is 'next' or 'prev'.",
        regex=r"^(?:next|prev):[A-Za-z0-9_-]+:[A-Za-z0-9_-]+$",
    ),
    datetime: Optional[str] = Query(
        None,
        title="Datetime",
        description=(
            "Datetime range (RFC 3339) format: start-date/end-date or exact-date"
        ),
        regex=(
            r"^"
            r"\d{4}-\d{2}-\d{2}"
            r"(?:T\d{2}:\d{2}:\d{2}Z)?"
            r"(?:/"
            r"\d{4}-\d{2}-\d{2}"
            r"(?:T\d{2}:\d{2}:\d{2}Z)?"
            r")?$"
        ),
    ),
    bbox: Optional[str] = Query(
        None,
        title="Bounding Box",
        description="minx,miny,maxx,maxy",
        regex=(
            r"^-?\d+(\.\d+)?,"
            r"-?\d+(\.\d+)?,"
            r"-?\d+(\.\d+)?,"
            r"-?\d+(\.\d+)?$"
        ),
    ),

) -> StreamingResponse:
    """Get items from a specific collection.

    This endpoint retrieves items from a specific collection in the STAC API.
    The collection is identified by its ID, and the items can be filtered
    using various query parameters such as limit, token, datetime, and bbox.
    """

    stac_instance = await STACAPI.validate_parameters(
        config=server_config,
        limit=limit,
        token=token,
        datetime=datetime,
        bbox=bbox,
        uniuq_key="file",
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
    responses={
        404: {"description": "Item not found"},
        503: {"description": "Search backend error"}
    },
    response_class=JSONResponse,
)
async def collection_item(
    collection_id: str,
    item_id: str,
) -> JSONResponse:
    """Get a specific item from a collection.

    This endpoint retrieves a specific item from a collection in the STAC API.
    The collection is identified by its ID, and the item is identified by its ID.
    """
    stac_instance = STACAPI(server_config)
    item = await stac_instance.get_collection_item(collection_id, item_id)
    return JSONResponse(
        item.to_dict(),
        media_type="application/json",
    )
