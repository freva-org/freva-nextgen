"""Definition of endpoints for loading/streaming and manipulating data."""

from typing import Annotated, List

import cloudpickle
from fastapi import Path, Query, Security, status
from fastapi.exceptions import HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi_third_party_auth import IDToken as TokenPayload
from pydantic import BaseModel, Field

from freva_rest.auth import auth
from freva_rest.auth.presign import path_from_url, verify_token
from freva_rest.logger import logger
from freva_rest.rest import app, server_config
from freva_rest.utils.base_utils import (
    Cache,
    encode_path_token,
    publish_dataset,
)

from .utils import (
    STATUS_LOOKUP,
    load_chunk,
    load_zarr_metadata,
    read_redis_data,
    ZARRAY_JSON,
    ZGROUP_JSON,
    ZATTRS_JSON,
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


@app.get(
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
    path: Annotated[
        List[str],
        Query(
            title="Path to data.",
            description="Absolute or object‑store paths to the data files to convert.",
            examples=["/work/abc1234/myuser/my-data.nc"],
        ),
    ],
) -> LoadResponse:
    """Publish a conversion request to the data‑portal worker.

    - **path**: absolute filesystem or object‑store path to the input file.
    - **returns**: a URL containing a token where the Zarr store will be served.
    - **note**: this function does **not** check that the input path exists or
      is readable by; that check occurs asynchronously in the worker.
    """
    try:
        return LoadResponse(urls=[await publish_dataset(_p) for _p in path])
    except HTTPException as error:
        raise HTTPException(detail=error.detail, status_code=error.status_code)
    except Exception as error:
        logger.error("Error while publishing data for zarr-conversion: %s", error)
        raise HTTPException(detail="Internal error.", status_code=500) from error


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
    current_user: TokenPayload = Security(
        auth.create_auth_dependency(), scopes=["oidc.claims"]
    ),
) -> ZarrStatus:
    """Get the status of a loading process."""
    path = path_from_url(url)
    token = encode_path_token(path)
    await Cache.check_connection()
    status = (
        cloudpickle.loads(
            await Cache.get(token) or cloudpickle.dumps({"status": 5})
        )
    ).get("status", 5)
    return ZarrStatus(status=status, reason=STATUS_LOOKUP.get(status, "Unknown"))


@app.get(
    "/api/freva-nextgen/data-portal/zarr/{token}.zarr/.zmetadata",
    tags=["Load data"],
)
async def zemtadata(
    token: Annotated[
        str,
        Path(
            title="token",
            description=(
                (
                    "The token that was generated, when task to stream data was "
                    "created."
                )
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
) -> JSONResponse:
    """Consolidate zarr metadata

    This endpoint returns the metadata about the structure and organization of
    data within the particular zarr store in question.
    """
    return await load_zarr_metadata(token, timeout=timeout)


@app.get(
    "/api/freva-nextgen/data-portal/share/{sig}/{token}.zarr/.zmetadata",
    tags=["Load data"],
)
async def zemtadata_shared(
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
                    "The token that was generated, when task to stream data was "
                    "created."
                )
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
) -> JSONResponse:
    """Consolidate zarr metadata

    This endpoint returns the metadata about the structure and organization of
    data within the particular zarr store in question.
    """
    payload = await verify_token(token, sig)
    return await load_zarr_metadata(payload["_id"], timeout=timeout)


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
    path = path_from_url(url)
    token = encode_path_token(path)
    return HTMLResponse(
        content=await read_redis_data(token, "repr_html", timeout=timeout)
    )


@app.get(
    "/api/freva-nextgen/data-portal/zarr/{token}.zarr/.zgroup",
    tags=["Load data"],
    status_code=status.HTTP_200_OK,
)
async def zgroup(
    token: Annotated[
        str,
        Path(
            title="token",
            description=(
                (
                    "The token that was generated, when task to stream data was "
                    "created."
                )
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
) -> JSONResponse:
    """Zarr group data.

    This `.zarrgroup` metadata includes information about the arrays and
    subgroups contained within the group, as well as their attributes such as
    data types, shapes, and chunk sizes. The `.zarrgroup` endpoint helps in
    organizing and managing the structure of data within a Zarr group,
    allowing users to access and manipulate arrays and subgroups efficiently.
    """
    return await load_zarr_metadata(token, ".zgroup", timeout)


@app.get(
    "/api/freva-nextgen/data-portal/share/{sig}/{token}.zarr/.zgroup",
    tags=["Load data"],
    status_code=status.HTTP_200_OK,
)
async def zgroup_shared(
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
                    "The token that was generated, when task to stream data was "
                    "created."
                )
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
) -> JSONResponse:
    """Zarr group data.

    This `.zarrgroup` metadata includes information about the arrays and
    subgroups contained within the group, as well as their attributes such as
    data types, shapes, and chunk sizes. The `.zarrgroup` endpoint helps in
    organizing and managing the structure of data within a Zarr group,
    allowing users to access and manipulate arrays and subgroups efficiently.
    """
    payload = await verify_token(token, sig)
    return await load_zarr_metadata(payload["_id"], ".zgroup", timeout)


@app.get(
    "/api/freva-nextgen/data-portal/zarr/{token}.zarr/.zattrs",
    tags=["Load data"],
)
async def zattrs(
    token: Annotated[
        str,
        Path(
            title="token",
            description=(
                (
                    "The token that was generated, when task to stream data was "
                    "created."
                )
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
) -> JSONResponse:
    """Get zarr Attributes.

    Get metadata attributes associated with the dataset or arrays within the
    dataset. These attributes provide additional information about the dataset
    or arrays, such as descriptions, units, creation dates, or any other
    custom metadata relevant to the data.
    """
    return await load_zarr_metadata(token, ".zattrs", timeout)


@app.get(
    "/api/freva-nextgen/data-portal/share/{sig}/{token}.zarr/.zattrs",
    tags=["Load data"],
)
async def zattrs_shared(
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
                    "The token that was generated, when task to stream data was "
                    "created."
                )
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
) -> JSONResponse:
    """Get zarr Attributes.

    Get metadata attributes associated with the dataset or arrays within the
    dataset. These attributes provide additional information about the dataset
    or arrays, such as descriptions, units, creation dates, or any other
    custom metadata relevant to the data.
    """
    payload = await verify_token(token, sig)
    return await load_zarr_metadata(payload["_id"], ".zattrs", timeout)


@app.get(
    "/api/freva-nextgen/data-portal/zarr/{token}.zarr/{variable}/{chunk}",
    tags=["Load data"],
)
async def chunk_data(
    token: Annotated[
        str,
        Path(
            title="token",
            description=(
                (
                    "The token that was generated, when task to stream data was "
                    "created."
                )
            ),
        ),
    ],
    variable: Annotated[
        str,
        Path(
            title="variable",
            description=("The variable name that should be read."),
        ),
    ],
    chunk: Annotated[
        str,
        Path(title="chunk", description="The chnuk number that should be read."),
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
    """Get a zarr array chunk.

    This method reads the zarr data."""

    return await load_chunk(token, variable, chunk, timeout)


@app.get(
    "/api/freva-nextgen/data-portal/share/{sig}/{token}.zarr/{variable}/{chunk}",
    tags=["Load data"],
)
async def chunk_data_shared(
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
                    "The token that was generated, when task to stream data was "
                    "created."
                )
            ),
        ),
    ],
    variable: Annotated[
        str,
        Path(
            title="variable",
            description=("The variable name that should be read."),
        ),
    ],
    chunk: Annotated[
        str,
        Path(title="chunk", description="The chnuk number that should be read."),
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
    """Get a zarr array chunk.

    This method reads the zarr data."""
    payload = await verify_token(token, sig)
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
    # Root-level keys: `.zmetadata`, `.zgroup` and `.zattrs` have no
    # variable context and therefore need special handling.  We explicitly
    # check for these cases and call the appropriate metadata loader.
    if zarr_key == ".zmetadata":
        return await load_zarr_metadata(token, timeout=timeout)
    if zarr_key == ZGROUP_JSON:
        return await load_zarr_metadata(token, ZGROUP_JSON, timeout)
    if zarr_key == ZATTRS_JSON:
        return await load_zarr_metadata(token, ZATTRS_JSON, timeout)
    if zarr_key == ZARRAY_JSON or zarr_key == ZATTRS_JSON:
        # Requests like `/.../.zarray` or `/.../.zattrs` at the root level are
        # invalid because a variable path is required.  Return a descriptive
        # error rather than delegating to the chunk loader, which would
        # misinterpret these keys.
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A variable name must precede .zarray or .zattrs",
        )
    # The remaining keys must include at least one slash to separate the
    # variable path from the final chunk or metadata suffix.  Without a
    # slash, the request is malformed.
    if "/" not in zarr_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Invalid zarr key: expected a slash-separated variable/chunk "
                f"path. Received '{zarr_key}'."
            ),
        )
    parts = zarr_key.split("/")
    variable = "/".join(parts[:-1])
    chunk = parts[-1]
    # Delegate to load_chunk.  It will detect `.zarray` and `.zattrs`
    # requests at the variable level and return the metadata accordingly,
    # otherwise it will stream the requested data chunk.
    return await load_chunk(token, variable, chunk, timeout)

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
    _id = payload["_id"]
    # Handle root-level metadata keys
    if zarr_key == ".zmetadata":
        return await load_zarr_metadata(_id, timeout=timeout)
    if zarr_key == ZGROUP_JSON:
        return await load_zarr_metadata(_id, ZGROUP_JSON, timeout)
    if zarr_key == ZATTRS_JSON:
        return await load_zarr_metadata(_id, ZATTRS_JSON, timeout)
    if zarr_key == ZARRAY_JSON or zarr_key == ZATTRS_JSON:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A variable name must precede .zarray or .zattrs",
        )
    if "/" not in zarr_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Invalid zarr key: expected a slash-separated variable/chunk "
                f"path. Received '{zarr_key}'."
            ),
        )
    parts = zarr_key.split("/")
    variable = "/".join(parts[:-1])
    chunk = parts[-1]
    return await load_chunk(_id, variable, chunk, timeout)
 return await load_chunk(payload["_id"], variable, chunk, timeout)


