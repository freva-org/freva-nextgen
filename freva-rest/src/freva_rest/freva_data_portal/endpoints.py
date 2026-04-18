"""Definition of endpoints for loading/streaming and manipulating data."""

import time
from typing import Annotated, Dict, List, Optional, Union, cast

import cloudpickle
from fastapi import HTTPException, Path, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import AnyHttpUrl, BaseModel, Field

from freva_rest.auth import RequiredUser, check_token
from freva_rest.logger import logger
from freva_rest.rest import app, server_config
from freva_rest.utils.base_utils import Cache, add_ttl_key_to_db_and_cache
from freva_rest.utils.presign_utils import (
    MAX_TTL_SECONDS,
    MIN_TTL_SECONDS,
    get_cache_token,
    normalise_path,
    payload_from_url,
    verify_token,
)

from .schema import PresignUrlRequest, PresignUrlResponse, ZarrConversion
from .utils import (
    STATUS_LOOKUP,
    process_zarr_data,
    publish_datasets,
    read_redis_data,
)


class LoadResponse(BaseModel):
    """Response schema returning the URL of the future Zarr dataset."""

    urls: List[str] = Field(
        ...,
        description=(
            "URLs where the converted Zarr dataset will be available "
            "after the asynchronous conversion has finished."
        ),
        title="Zarr URLs",
        examples=[
            [f"{server_config.proxy}/api/freva-nextgen/data-portal/zarr/abc123.zarr"]
        ],
    )


class ZarrStatus(BaseModel):
    """Schema for the zarr loading status."""

    status: Annotated[
        int,
        Field(
            title="Status",
            description=(
                "Integer representation of the status"
                "the following status codes are defined:\n"
                f"{str(STATUS_LOOKUP)}"
            ),
            examples=list(STATUS_LOOKUP.keys()),
        ),
    ]
    reason: Annotated[
        str,
        Field(
            title="Reason",
            description="Human readable status",
            examples=list(STATUS_LOOKUP.values()),
        ),
    ]


@app.post(
    "/api/freva-nextgen/data-portal/zarr/convert",
    summary="Request asynchronous Zarr conversion",
    description=(
        "Submit a file or object path to be converted into a Zarr store.  "
        "This endpoint only publishes a message to the data‑portal worker via "
        "a broker; it does **not** verify that the path exists or perform the "
        "conversion itself.  It returns a URL containing a token where the Zarr "
        "dataset will be available once processing is complete.  "
        "\n\n"
        "If the data‑loading service cannot access the file , "
        "it will record the failure and the returned Zarr dataset will be in "
        "a failed state with a reason.  You can query the status endpoint to "
        "check whether the conversion succeeded or failed."
    ),
    tags=["Load data"],
    status_code=200,
    responses={
        401: {"description": "Unauthorised / not a valid token."},
        503: {"description": "If the service is currently unavailable."},
        500: {"description": "Internal error while publishing to the broker."},
    },
    response_class=JSONResponse,
)
async def load_files(
    convert: ZarrConversion,
    current_user: RequiredUser,
) -> LoadResponse:
    """Publish a conversion request to the data‑portal worker.

    - **path**: absolute filesystem or object‑store path to the input file.
    - **returns**: a URL containing a token where the Zarr store will be served.
    - **note**: this function does **not** check that the input path exists or
      is readable by; that check occurs asynchronously in the worker.
    """
    paths = convert.path if isinstance(convert.path, list) else [convert.path]
    aggregation_plan: Dict[str, Optional[str]] = {
        "mode": convert.aggregate,
        "dim": convert.dim,
        "compat": convert.compat,
        "join": convert.join,
        "data_vars": convert.data_vars,
        "coords": convert.coords,
        "group_by": convert.group_by,
    }

    async def publish(path: Union[str, List[str]]) -> str:
        return await publish_datasets(
            path,
            aggregation_plan={k: v for k, v in aggregation_plan.items() if v},
            ttl_seconds=convert.ttl_seconds,
            public=convert.public,
            access_pattern=convert.access_pattern,
            map_primary_chunksize=convert.map_primary_chunksize,
            reload=convert.reload,
            chunk_size=convert.chunk_size,
            publish=True,
        )

    try:
        if convert.aggregate is None:
            urls = [await publish(p) for p in paths]
        else:
            urls = [await publish(paths)]
        return LoadResponse(urls=urls)
    except HTTPException as error:
        logger.exception(error)
        raise
    except Exception as error:
        logger.exception(error)
        raise HTTPException(detail="Internal error.", status_code=500) from error


def _is_public_zarr_url(url: str) -> bool:
    """
    Public URLs are those whose PATH contains /data-portal/share/<keys>
    and ends with .zarr (adjust if you want).
    """
    path_no_suffix = url.removesuffix(".zarr")

    _, split, keys = path_no_suffix.partition("/data-portal/share/")
    return bool(split and keys)


@app.get(
    "/api/freva-nextgen/data-portal/zarr-utils/status",
    tags=["Load data"],
    status_code=200,
    summary="Check the status of a loaded dataset.",
    responses={
        401: {"description": "Unauthorised / not a valid token."},
        404: {"description": "If the token is not known to the system."},
        503: {"description": "If the service is currently unavailable."},
    },
    description=(
        "Once an instruction to create a a dynamic zarr dataset has"
        " been submitted the `/status/` endpoint can be used to check"
        " progress of the data conversion."
    ),
    response_model=ZarrStatus,
)
async def get_status(
    request: Request,
    url: Annotated[
        str,
        Query(
            title="URL to zarr store",
            description="The fully qualified url to the zarr store.",
            examples=[f"{server_config.proxy}/api/data-portal/zarr/1234.zarr"],
        ),
    ],
    timeout: Annotated[
        int,
        Query(
            alias="timeout",
            title="Cache timeout for getting results.",
            description="Set a timeout to wait for results.",
            examples=[10],
            ge=0,
            le=1500,
        ),
    ] = 1,
) -> ZarrStatus:
    """Get the status of a loading process."""
    _, split, keys = url.removesuffix(".zarr").partition("/data-portal/share/")
    if not _is_public_zarr_url(url):
        auth_header = request.headers.get("authorization")
        await check_token(auth_header)

    try:
        if split and keys:
            slug, key = keys.split("/", 1)
            payload = await verify_token(key, slug)
            token = payload["_id"]
        else:
            token = get_cache_token(url)
        await Cache.check_connection()
        stat = cloudpickle.loads(await Cache.get(token) or b"\x80\x05}\x94.")
        return ZarrStatus(
            status=stat.get("status", 5), reason=stat.get("reason", "Unknown")
        )
    except HTTPException as error:
        return ZarrStatus(status=5, reason=error.detail)
    except Exception as error:
        logger.warning(error)
        raise HTTPException(503, "Not available")


@app.get(
    "/api/freva-nextgen/data-portal/zarr-utils/html",
    tags=["Load data"],
    response_model=None,
    summary="Get HTML representation of Zarr dataset",
    description=(
        "Returns a human-readable HTML representation of the Zarr dataset "
        "using Xarray's HTML formatter. This endpoint is intended for "
        "interactive exploration and visualization in web browsers."
    ),
    response_class=HTMLResponse,
)
async def zarr_html_view(
    current_user: RequiredUser,
    url: Annotated[
        str,
        Query(
            title="URL to zarr store",
            description="The fully qualified url to the zarr store.",
            examples=[f"{server_config.proxy}/api/data-portal/zarr/1234.zarr"],
        ),
    ],
    timeout: Annotated[
        int,
        Query(
            alias="timeout",
            title="Cache timeout for getting results.",
            description="Set a timeout to wait for results.",
            ge=0,
            le=1500,
        ),
    ] = 1,
) -> HTMLResponse:
    """Get HTML representation of the Zarr dataset.

    This endpoint provides a human-readable HTML view of the dataset structure
    and metadata, generated using Xarray's HTML representation method.
    """
    token = get_cache_token(url)
    return HTMLResponse(
        content=await read_redis_data(token, "repr_html", timeout=timeout)
    )


@app.get(
    "/api/freva-nextgen/data-portal/zarr/{token}.zarr/{zarr_key:path}",
    tags=["Load data"],
)
async def zarr_key_data(
    current_user: RequiredUser,
    token: Annotated[
        str,
        Path(
            title="token",
            description=(
                "The token that was generated, when task to stream data was created."
            ),
        ),
    ],
    zarr_key: Annotated[
        str,
        Path(
            title="zarr_key",
            description=(
                "A slash-separated key within the zarr store.  Clients like "
                "xarray and zarr will request keys such as '.zmetadata', "
                "'var/.zarray', 'group/var/0.0.0', etc.  This endpoint will "
                "dispatch to the appropriate handler based on the key suffix."
            ),
        ),
    ],
    timeout: Annotated[
        int,
        Query(
            alias="timeout",
            title="Cache timeout for getting results.",
            description="Set a timeout to wait for results.",
            ge=0,
            le=1500,
        ),
    ] = 1,
) -> Response:
    """
    Serve arbitrary Zarr metadata or chunk keys.

    Zarr clients access stores by issuing HTTP GET requests on a hierarchy of
    keys rather than downloading a single monolithic file.  This endpoint
    enables clients to access any key under the `{token}.zarr` namespace,
    whether it refers to root-level metadata (e.g. `.zmetadata`, `.zgroup`,
    `.zattrs`), variable-specific metadata (e.g. `tas/.zarray`), or data
    chunks (e.g. `tas/0.0.0`).  For root-level metadata keys we call
    ``load_zarr_metadata``, and for all other keys we delegate to
    ``load_chunk`` using the parent path as the variable and the final
    segment as the chunk identifier.
    """
    return await process_zarr_data(token, zarr_key, timeout=timeout)


@app.get(
    "/api/freva-nextgen/data-portal/share/{sig}/{token}.zarr/{zarr_key:path}",
    tags=["Load data"],
)
async def zarr_key_data_shared(
    sig: Annotated[
        str,
        Path(
            title="Signature",
            description=(
                "The signature which was created by the /share-zarr endpoint."
            ),
        ),
    ],
    token: Annotated[
        str,
        Path(
            title="token",
            description=(
                "The token that was generated, when task to stream data was created."
            ),
        ),
    ],
    zarr_key: Annotated[
        str,
        Path(
            title="zarr_key",
            description=(
                "A slash-separated key within the zarr store.  Clients like "
                "xarray and zarr will request keys such as '.zmetadata', "
                "'var/.zarray', 'group/var/0.0.0', etc.  This endpoint will "
                "dispatch to the appropriate handler based on the key suffix."
            ),
        ),
    ],
    timeout: Annotated[
        int,
        Query(
            alias="timeout",
            title="Cache timeout for getting results.",
            description="Set a timeout to wait for results.",
            ge=0,
            le=1500,
        ),
    ] = 1,
) -> Response:
    """
    Serve arbitrary Zarr metadata or chunk keys for shared datasets.

    This endpoint mirrors ``zarr_key_data`` but first verifies the provided
    signature and decodes the token before dispatching.  The remainder of
    the logic is identical to the non-shared catch-all route.
    """
    payload = await verify_token(token, sig)
    return await process_zarr_data(payload["_id"], zarr_key, timeout=timeout)


@app.post(
    "/api/freva-nextgen/data-portal/share-zarr",
    tags=["Load data"],
    status_code=status.HTTP_201_CREATED,
    summary="Create a pre-signed URL for a Zarr chunk",
    description=(
        "Create a short-lived, shareable pre-signed URL for a specific Zarr "
        "chunk. The caller must authenticate with a normal OAuth2 access "
        "token.\n\n The returned URL includes `expires` and `sig` query "
        "parameters. Anyone who knows the URL can perform a `GET` request on "
        "the target resource until the expiry time is reached, without "
        "needing an access token."
    ),
    responses={
        201: {
            "description": "Pre-signed URL created successfully.",
            "content": {
                "application/json": {
                    "example": {
                        "url": (
                            "https://api.example.org"
                            "/api/freva-nextgen/data-portal/share/"
                            "MTc2NjEzNzY5Ng/sunny-chestnut-snail.zarr"
                            "?expires=1731600000&sig=AbCdEf..."
                        ),
                        "expires_at": int(time.time()) + 600,
                        "method": "GET",
                    }
                }
            },
        },
        400: {"description": "Invalid path or parameters."},
        401: {"description": "Missing or invalid OAuth2 access token."},
        403: {
            "description": "Authenticated user is not allowed to pre-sign this "
            "resource."
        },
    },
)
async def create_presigned_url(
    request: Request,
    body: PresignUrlRequest,
    token: RequiredUser,
) -> PresignUrlResponse:
    """Create a new pre-signed URL.

    This endpoint is intended for authenticated users who want to share
    short-lived links to individual Zarr chunks. Authorisation rules for
    *who may pre-sign which chunk* can be implemented based on `token`.
    """
    payload = payload_from_url(normalise_path(str(body.path)))
    # TODO: we should check if the user is allowed to read the dataset.
    ttl = max(MIN_TTL_SECONDS, min(body.ttl_seconds, MAX_TTL_SECONDS))
    res = await add_ttl_key_to_db_and_cache(payload["path"], ttl, payload["assembly"])
    url = f"{server_config.proxy}/api/freva-nextgen/data-portal/share/{res['key']}.zarr"
    return PresignUrlResponse(
        url=cast(AnyHttpUrl, url),
        token=res["token"],
        sig=res["signature"],
        expires_at=res["expires_at"].timestamp(),
        method=body.method.upper(),
    )
