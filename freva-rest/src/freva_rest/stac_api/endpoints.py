"""Replicate the STAC-API Endpoints"""

import functools
import inspect
import json
from typing import Any, Callable, List, Optional

from fastapi import Body, HTTPException, Query, Request
from fastapi.responses import (
    JSONResponse,
    PlainTextResponse,
    StreamingResponse,
)

from freva_rest.rest import app, server_config

from .core import STACAPI
from .schema import (
    CONFORMANCE_URLS,
    VISIBLE_COLLECTIONS_QUERY,
    CollectionsResponse,
    ConformanceResponse,
    ItemCollectionResponse,
    LandingPageResponse,
    PingResponse,
    QueryablesResponse,
    SearchPostRequest,
    STACAPISchema,
    STACCollection,
    STACItem,
    parse_visible,
)

_STAC_BASE = "/api/freva-nextgen/stacapi"


def stac_route(
    suffix: str, *, method: str = "get", **kwargs: Any
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Register an endpoint under both the legacy (unscoped) and the
    ``{axis}``-scoped STAC path with a single declaration.
    """
    register = getattr(app, method)

    def decorator(impl: Callable[..., Any]) -> Callable[..., Any]:
        # Scoped route
        register(
            f"{_STAC_BASE}/{{axis}}{suffix}",
            include_in_schema=False,
            **kwargs,
        )(impl)

        # Legacy route
        sig = inspect.signature(impl)
        legacy_params = [
            p for name, p in sig.parameters.items() if name != "axis"
        ]

        @functools.wraps(impl)
        async def legacy(*args: Any, **kw: Any) -> Any:
            kw.pop("axis", None)
            return await impl(*args, axis=None, **kw)

        # Present the axis-free signature to FastAPI for dependency parsing.
        legacy.__signature__ = sig.replace(  # type: ignore[attr-defined]
            parameters=legacy_params
        )
        register(f"{_STAC_BASE}{suffix}", **kwargs)(legacy)
        return impl

    return decorator


@stac_route(
    "/",
    tags=["STAC API"],
    status_code=200,
    response_model=LandingPageResponse,
    responses={503: {"description": "Search backend error"}},
    response_class=JSONResponse,
)
async def landing_page(
    axis: Optional[str] = None,
    visible_collections: Optional[List[str]] = VISIBLE_COLLECTIONS_QUERY,
) -> JSONResponse:
    """STAC API landing page declaration.

    This endpoint provides the landing page of the STAC API,
    which includes information about the API version, title,
    description, and links to collections and other resources.
    The landing page serves as an entry point for users to
    explore the available collections and items in the STAC API.
    """
    stac_instance = STACAPI(
        server_config,
        collection_axis=axis,
        visible_collections=parse_visible(visible_collections),
        axis_in_path=axis is not None,
    )
    await stac_instance.store_results(0, 200, "landing_page", {})
    response = await stac_instance.get_landing_page()
    return JSONResponse(response)


@stac_route(
    "/conformance",
    tags=["STAC API"],
    status_code=200,
    response_model=ConformanceResponse,
    responses={503: {"description": "Search backend error"}},
    response_class=JSONResponse,
)
async def conformance(
    axis: Optional[str] = None,
    visible_collections: Optional[List[str]] = VISIBLE_COLLECTIONS_QUERY,
) -> JSONResponse:
    """STAC API conformance declaration.

    This endpoint returns the conformance classes that the STAC API
    implementation conforms to. It provides information about the
    supported features and capabilities of the API.
    """
    response = {"conformsTo": CONFORMANCE_URLS}
    return JSONResponse(response)


@stac_route(
    "/collections",
    tags=["STAC API"],
    status_code=200,
    response_model=CollectionsResponse,
    responses={503: {"description": "Search backend error"}},
    response_class=PlainTextResponse,
)
async def collections(
    axis: Optional[str] = None,
    visible_collections: Optional[List[str]] = VISIBLE_COLLECTIONS_QUERY,
) -> StreamingResponse:
    """List all collections in the STAC API.

    This endpoint retrieves a list of all collections available in the STAC API.
    Each collection represents a group of related items and provides metadata
    about the collection, including its ID, title, description, and spatial
    and temporal extents.
    """
    stacapi_instance = STACAPI(
        server_config,
        collection_axis=axis,
        visible_collections=parse_visible(visible_collections),
        axis_in_path=axis is not None,
    )
    await stacapi_instance.store_results(0, 200, "collections", {})
    # validate against:
    # a bad glob (no-match / too broad)= 400
    await stacapi_instance.get_all_collection_facets()
    return StreamingResponse(
        stacapi_instance.get_collections(),
        media_type="application/json",
    )


@stac_route(
    "/collections/{collection_id}",
    tags=["STAC API"],
    status_code=200,
    response_model=STACCollection,
    responses={
        404: {"description": "Collection not found"},
        503: {"description": "Search backend error"},
    },
    response_class=JSONResponse,
)
async def collection(
    collection_id: str,
    axis: Optional[str] = None,
    visible_collections: Optional[List[str]] = VISIBLE_COLLECTIONS_QUERY,
) -> JSONResponse:
    """Get a specific collection.

    This endpoint retrieves a specific collection from the STAC API.
    The collection is identified by its ID.
    """
    stacapi_instance = STACAPI(
        server_config,
        collection_axis=axis,
        visible_collections=parse_visible(visible_collections),
        axis_in_path=axis is not None,
    )
    await stacapi_instance.store_results(
        0, 200, "collection", {"collection_id": collection_id}
    )
    collection = await stacapi_instance.get_collection(collection_id)
    return JSONResponse(
        collection.model_dump(exclude_none=True), media_type="application/json"
    )


@stac_route(
    "/collections/{collection_id}/items",
    tags=["STAC API"],
    status_code=200,
    response_model=ItemCollectionResponse,
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
        pattern=r"^(?:next|prev):[^:]+:[^:]+$",
    ),
    datetime: Optional[str] = Query(
        None,
        title="Datetime",
        description=(
            "Datetime range (RFC 3339) format: start-date/end-date or exact-date"
        ),
        pattern=(
            r"^"
            r"\d{4}-\d{2}-\d{2}"
            r"(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)?"
            r"(?:/"
            r"\d{4}-\d{2}-\d{2}"
            r"(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)?"
            r")?"
            r"$"
        ),
    ),
    bbox: Optional[str] = Query(
        None,
        title="Bounding Box",
        description="minx,miny,maxx,maxy",
        pattern=(
            r"^-?\d+(\.\d+)?,"
            r"-?\d+(\.\d+)?,"
            r"-?\d+(\.\d+)?,"
            r"-?\d+(\.\d+)?$"
        ),
    ),
    axis: Optional[str] = None,
    visible_collections: Optional[List[str]] = VISIBLE_COLLECTIONS_QUERY,
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
        collection_axis=axis,
        visible_collections=parse_visible(visible_collections),
        axis_in_path=axis is not None,
        **STACAPISchema.process_parameters(request),
    )
    query_params = {
        "collection_id": collection_id,
        "limit": limit,
        "token": token,
        "datetime": datetime,
        "bbox": bbox,
    }
    await stac_instance.store_results(0, 200, "collection_items", query_params)
    # Validate against:
    # missing/hidden collection = 404
    # bad pagination-token context = 400
    await stac_instance.prepare_collection_items(collection_id)
    stac_instance._validate_pagination_token(token, collection_id.lower())
    return StreamingResponse(
        stac_instance.get_collection_items(
            collection_id, limit, token, datetime, bbox
        ),
        media_type="application/json",
    )


@stac_route(
    "/collections/{collection_id}/items/{item_id}",
    tags=["STAC API"],
    status_code=200,
    response_model=STACItem,
    responses={
        404: {"description": "Item not found"},
        503: {"description": "Search backend error"},
    },
    response_class=JSONResponse,
)
async def collection_item(
    collection_id: str,
    item_id: str,
    axis: Optional[str] = None,
    visible_collections: Optional[List[str]] = VISIBLE_COLLECTIONS_QUERY,
) -> JSONResponse:
    """Get a specific item from a collection.

    This endpoint retrieves a specific item from a collection in the STAC API.
    The collection is identified by its ID, and the item is identified by its ID.
    """
    stac_instance = STACAPI(
        server_config,
        collection_axis=axis,
        visible_collections=parse_visible(visible_collections),
        axis_in_path=axis is not None,
    )
    await stac_instance.store_results(
        0,
        200,
        "collection_item",
        {"collection_id": collection_id, "item_id": item_id},
    )
    item = await stac_instance.get_collection_item(collection_id, item_id)
    return JSONResponse(
        item.to_dict(),
        media_type="application/json",
    )


@stac_route(
    "/search",
    tags=["STAC API"],
    status_code=200,
    response_model=ItemCollectionResponse,
    responses={
        422: {"description": "Invalid query parameters"},
        503: {"description": "Search backend error"},
    },
    response_class=StreamingResponse,
)
async def search_get(
    request: Request,
    collections: Optional[str] = Query(
        None,
        title="Collections",
        description="Comma-separated list of collection IDs to search",
    ),
    ids: Optional[str] = Query(
        None,
        title="IDs",
        description="Comma-separated list of item IDs to search",
    ),
    bbox: Optional[str] = Query(
        None,
        title="Bounding Box",
        description="minx,miny,maxx,maxy",
        pattern=(
            r"^-?\d+(\.\d+)?,"
            r"-?\d+(\.\d+)?,"
            r"-?\d+(\.\d+)?,"
            r"-?\d+(\.\d+)?$"
        ),
    ),
    datetime: Optional[str] = Query(
        None,
        title="Datetime",
        description=(
            "Datetime range (RFC 3339) format: start-date/end-date or exact-date"
        ),
        pattern=(
            r"^"
            r"\d{4}-\d{2}-\d{2}"
            r"(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)?"
            r"(?:/"
            r"\d{4}-\d{2}-\d{2}"
            r"(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)?"
            r")?"
            r"$"
        ),
    ),
    limit: int = Query(10, ge=1, le=1000, title="Limit"),
    token: Optional[str] = Query(
        None,
        title="Token",
        description="Pagination token in format direction:search:item_id",
        pattern=r"^(?:next|prev):search:[^:]+$",
    ),
    q: Optional[str] = Query(
        None,
        title="Free Text Search",
        description=(
            "Free text search query. Comma-separated terms (OR logic)."
            " Case-insensitive search across item properties."
        ),
        examples=["climate,temperature,precipitation"],
    ),
    query: Optional[str] = Query(
        None,
        title="Query",
        description="Additional query parameters as JSON string",
    ),
    sortby: Optional[str] = Query(
        None,
        title="Sort By",
        description="Sort criteria as JSON string",
    ),
    fields: Optional[str] = Query(
        None,
        title="Fields",
        description="Fields to include/exclude as JSON string",
    ),
    filter: Optional[str] = Query(
        None,
        title="Filter",
        description="CQL filter as JSON string",
    ),
    axis: Optional[str] = None,
    visible_collections: Optional[List[str]] = VISIBLE_COLLECTIONS_QUERY,
) -> StreamingResponse:
    """STAC API search endpoint (GET).

    This endpoint allows searching across all collections using query parameters.
    It supports spatial, temporal, and property-based filtering of STAC items.
    """
    stac_instance = await STACAPI.validate_parameters(
        config=server_config,
        limit=limit,
        token=token,
        datetime=datetime,
        bbox=bbox,
        uniuq_key="file",
        collection_axis=axis,
        visible_collections=parse_visible(visible_collections),
        axis_in_path=axis is not None,
        **STACAPISchema.process_parameters(request),
    )
    query_params = {
        "collections": collections,
        "ids": ids,
        "bbox": bbox,
        "datetime": datetime,
        "limit": limit,
        "q": q,
    }
    await stac_instance.store_results(0, 200, "search_get", query_params)
    # Validate before the stream starts against:
    # out-of-view collection, malformed CQL2, bad token context= 400
    await stac_instance.prepare_search(collections=collections, filter=filter)
    stac_instance._validate_pagination_token(token, "search")
    return StreamingResponse(
        stac_instance.get_search(
            collections=collections,
            ids=ids,
            bbox=bbox,
            datetime=datetime,
            limit=limit,
            token=token,
            q=q,
            query=query,
            sortby=sortby,
            fields=fields,
            filter=filter,
        ),
        media_type="application/geo+json",
    )


@stac_route(
    "/search",
    method="post",
    tags=["STAC API"],
    status_code=200,
    response_model=ItemCollectionResponse,
    responses={
        422: {"description": "Invalid request body"},
        503: {"description": "Search backend error"},
    },
    response_class=StreamingResponse,
)
async def search_post(
    request: Request,
    body: SearchPostRequest = Body(...),
    axis: Optional[str] = None,
    visible_collections: Optional[List[str]] = VISIBLE_COLLECTIONS_QUERY,
) -> StreamingResponse:
    """STAC API search endpoint (POST)"""

    query_visible = parse_visible(visible_collections)
    # IMPORTANT: Visibility may come from the query string (so STAC
    # Browser global request parameters work) or the JSON body. If
    # both are given and disagree, reject rather than silently
    # picking one.
    if (
        query_visible is not None
        and body.visible_collections is not None
        and set(query_visible) != set(body.visible_collections)
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Conflicting 'visible_collections' in query string and body."
            ),
        )
    visible = query_visible or body.visible_collections

    stac_instance = await STACAPI.validate_parameters(
        config=server_config,
        limit=body.limit or 10,
        token=body.token,
        datetime=body.datetime,
        bbox=",".join(map(str, body.bbox)) if body.bbox else None,
        uniuq_key="file",
        collection_axis=axis,
        visible_collections=visible,
        axis_in_path=axis is not None,
        **STACAPISchema.process_parameters(request),
    )

    query_params = {
        "collections": body.collections,
        "ids": body.ids,
        "bbox": body.bbox,
        "datetime": body.datetime,
        "limit": body.limit,
        "q": body.q,
    }
    await stac_instance.store_results(0, 200, "search_post", query_params)

    # Validate against:
    # malformed request= 400 rather than a truncated stream.
    collections_str = ",".join(body.collections) if body.collections else None
    filter_str = (
        json.dumps(body.filter) if isinstance(body.filter, dict) else body.filter
    )
    await stac_instance.prepare_search(
        collections=collections_str, filter=filter_str
    )
    stac_instance._validate_pagination_token(body.token, "search")
    return StreamingResponse(
        stac_instance.post_search(
            collections=body.collections,
            ids=body.ids,
            bbox=body.bbox,
            # intersects=body.intersects,
            datetime=body.datetime,
            limit=body.limit or 10,
            token=body.token,
            q=body.q,
            query=body.query,
            sortby=body.sortby,
            fields=body.fields,
            filter=body.filter,
        ),
        media_type="application/geo+json",
    )


@stac_route(
    "/queryables",
    tags=["STAC API"],
    status_code=200,
    response_model=QueryablesResponse,
    responses={503: {"description": "Search backend error"}},
    response_class=JSONResponse,
)
async def queryables(
    axis: Optional[str] = None,
    visible_collections: Optional[List[str]] = VISIBLE_COLLECTIONS_QUERY,
) -> JSONResponse:
    """Global queryables endpoint.

    This endpoint returns the queryables that can be used in filter expressions
    across all collections. It returns a JSON Schema document describing the
    available properties that can be used for filtering.
    """
    stac_instance = STACAPI(
        server_config,
        collection_axis=axis,
        visible_collections=parse_visible(visible_collections),
        axis_in_path=axis is not None,
    )
    response = await stac_instance.get_queryables()
    return JSONResponse(response, media_type="application/schema+json")


@stac_route(
    "/collections/{collection_id}/queryables",
    tags=["STAC API"],
    status_code=200,
    response_model=QueryablesResponse,
    responses={
        404: {"description": "Collection not found"},
        503: {"description": "Search backend error"},
    },
    response_class=JSONResponse,
)
async def collection_queryables(
    collection_id: str,
    axis: Optional[str] = None,
    visible_collections: Optional[List[str]] = VISIBLE_COLLECTIONS_QUERY,
) -> JSONResponse:
    """Collection-specific queryables endpoint.

    This endpoint returns the queryables that can be used in filter expressions
    for a specific collection. It returns a JSON Schema document describing the
    available properties that can be used for filtering within that collection.
    """
    stac_instance = STACAPI(
        server_config,
        collection_axis=axis,
        visible_collections=parse_visible(visible_collections),
        axis_in_path=axis is not None,
    )
    response = await stac_instance.get_collection_queryables(collection_id)
    return JSONResponse(response, media_type="application/schema+json")


@app.get(
    "/api/freva-nextgen/stacapi/_mgmt/ping",
    tags=["STAC API"],
    status_code=200,
    response_model=PingResponse,
    responses={200: {"description": "Successful Response"}},
    response_class=JSONResponse,
)
async def ping() -> JSONResponse:
    """
    Liveliness/readiness probe.
    """
    return JSONResponse({"message": "PONG"})
