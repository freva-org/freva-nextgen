"""Definition of endpoints for loading/streaming and manipulating data."""

from typing import Annotated, Dict, List, Optional, Union

import cloudpickle
from fastapi import Path, Query, Request, Security
from fastapi.exceptions import HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi_third_party_auth import IDToken as TokenPayload
from pydantic import BaseModel, Field

from freva_rest.auth import auth
from freva_rest.auth.presign import get_cache_token, verify_token
from freva_rest.logger import logger
from freva_rest.rest import app, server_config
from freva_rest.utils.base_utils import Cache

from .schema import ZarrConversion
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
            [
                f"{server_config.proxy}/api/freva-nextgen/data-portal/zarr/abc123.zarr"
            ]
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
    current_user: TokenPayload = Security(
        auth.create_auth_dependency(), scopes=["oidc.claims"]
    ),
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
            publish=True,
        )

    try:
        if convert.aggregate is None:
            urls = [await publish(p) for p in paths]
        else:
            urls = [await publish(paths)]
        return LoadResponse(urls=urls)
    except HTTPException:
        raise
    except Exception as error:
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
        await auth.check_token_from_headers(auth_header, required=True)

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
    current_user: TokenPayload = Security(
        auth.create_auth_dependency(), scopes=["oidc.claims"]
    ),
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
    token: Annotated[
        str,
        Path(
            title="token",
            description=(
                (
                    "The token that was generated, when task to stream data "
                    "was created."
                )
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
    current_user: TokenPayload = Security(
        auth.create_auth_dependency(), scopes=["oidc.claims"]
    ),
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
                (
                    "The token that was generated, when task to stream data "
                    "was created."
                )
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
