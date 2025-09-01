"""Definition of endpoints for loading/streaming and manipulating data."""

import asyncio
import json
import uuid
from typing import Annotated, Any, Dict, List, Optional

import cloudpickle
from fastapi import Path, Query, Security, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, Response
from fastapi_third_party_auth import IDToken as TokenPayload
from pydantic import BaseModel, Field

from freva_rest.auth import auth
from freva_rest.logger import logger
from freva_rest.rest import app, server_config
from freva_rest.utils.base_utils import create_redis_connection

ZARRAY_JSON = ".zarray"
ZGROUP_JSON = ".zgroup"
ZATTRS_JSON = ".zattrs"


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


async def read_redis_data(
    key: str,
    subkey: Optional[str] = None,
    timeout: int = 1,
) -> Any:
    """Read the cache data given by a key.

    Parameters
    ----------
    key: str
        The key under which the data is stored.
    subkey: str|None
        If the data under key is a pickled dict the the will be
        unpickled and and the value of that subkey will be returned
    timeout: int
        Wait for timeout seconds until a not found error is risen.
    """

    cache = await create_redis_connection()
    data: Optional[bytes] = await cache.get(key)
    npolls = 0
    while data is None:
        npolls += 1
        await asyncio.sleep(1)
        data = await cache.get(key)
        if npolls >= timeout:
            break
    if data is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"{key} uuid does not exist (anymore).",
        )
    if not subkey:
        return data
    lookup = {
        0: "finished, ok",
        1: "finished, failed",
        2: "waiting",
        3: "processing",
    }
    p_data: Dict[str, Any] = cloudpickle.loads(data)
    task_status = p_data.get("status", 1)
    if task_status != 0:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=lookup.get(task_status, "unknown"),
        )
    return p_data[subkey]


@app.get(
    "/api/freva-nextgen/data-portal/zarr/convert",
    summary="Request asynchronous Zarr conversion",
    description=(
        "Submit a file or object path to be converted into a Zarr store.  "
        "This endpoint only publishes a message to the data‑portal worker via "
        "a broker; it does **not** verify that the path exists or perform the "
        "conversion itself.  It returns a URL containing a UUID where the Zarr "
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
    current_user: TokenPayload = Security(
        auth.create_auth_dependency(), scopes=["oidc.claims"]
    ),
) -> LoadResponse:
    """Publish a conversion request to the data‑portal worker.

    - **path**: absolute filesystem or object‑store path to the input file.
    - **returns**: a URL containing a UUID where the Zarr store will be served.
    - **note**: this function does **not** check that the input path exists or
      is readable by; that check occurs asynchronously in the worker.
    """
    if "zarr-stream" not in (server_config.services or []):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not enabled.",
        )
    out_paths: List[str] = []
    try:
        cache = await create_redis_connection()
        api_path = f"{server_config.proxy}/api/freva-nextgen/data-portal/zarr"
        for _path in path if isinstance(path, list) else [path]:
            uuid5 = str(uuid.uuid5(uuid.NAMESPACE_URL, _path))
            out_paths.append(f"{api_path}/{uuid5}.zarr")
            await cache.publish(
                "data-portal",
                json.dumps({"uri": {"path": _path, "uuid": uuid5}}).encode(
                    "utf-8"
                ),
            )
    except Exception as pub_err:
        logger.error("Failed to publish to Redis: %s", pub_err)
        raise HTTPException(
            status_code=500,
            detail="Internal error, service not able to publish.",
        )
    return LoadResponse(urls=out_paths)


@app.get(
    "/api/freva-nextgen/data-portal/zarr/{uuid5}.zarr/status",
    tags=["Load data"],
    status_code=200,
    responses={
        401: {"description": "Unauthorised / not a valid token."},
        404: {"description": "If the uuid is not known to the system."},
        503: {"description": "If the service is currently unavailable."},
    },
    response_class=JSONResponse,
)
async def get_status(
    uuid5: Annotated[
        str,
        Path(
            title="uuid",
            description=(
                (
                    "The uuid that was generated, when task to stream data was "
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
    """Get the status of a loading process."""
    meta: Dict[str, Any] = await read_redis_data(uuid5, "status", timeout=timeout)
    return JSONResponse(content={"status": meta}, status_code=status.HTTP_200_OK)


@app.get(
    "/api/freva-nextgen/data-portal/zarr/{uuid5}.zarr/.zmetadata",
    tags=["Load data"],
)
async def zemtadata(
    uuid5: Annotated[
        str,
        Path(
            title="uuid",
            description=(
                (
                    "The uuid that was generated, when task to stream data was "
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

    meta: Dict[str, Any] = await read_redis_data(
        uuid5, "json_meta", timeout=timeout
    )
    return JSONResponse(
        content=meta,
        status_code=status.HTTP_200_OK,
    )


@app.get(
    "/api/freva-nextgen/data-portal/zarr/{uuid5}.zarr/.zgroup",
    tags=["Load data"],
    status_code=status.HTTP_200_OK,
)
async def zgroup(
    uuid5: Annotated[
        str,
        Path(
            title="uuid",
            description=(
                (
                    "The uuid that was generated, when task to stream data was "
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
    meta: Dict[str, Any] = await read_redis_data(
        uuid5, "json_meta", timeout=timeout
    )
    return JSONResponse(
        content=meta["metadata"][".zgroup"],
        status_code=status.HTTP_200_OK,
    )


@app.get(
    "/api/freva-nextgen/data-portal/zarr/{uuid5}.zarr/.zattrs",
    tags=["Load data"],
)
async def zattrs(
    uuid5: Annotated[
        str,
        Path(
            title="uuid",
            description=(
                (
                    "The uuid that was generated, when task to stream data was "
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
    meta: Dict[str, Any] = await read_redis_data(
        uuid5, "json_meta", timeout=timeout
    )
    return JSONResponse(
        content=meta["metadata"][".zattrs"], status_code=status.HTTP_200_OK
    )


@app.get(
    "/api/freva-nextgen/data-portal/zarr/{uuid5}.zarr/{variable}/{chunk}",
    tags=["Load data"],
)
async def chunk_data(
    uuid5: Annotated[
        str,
        Path(
            title="uuid",
            description=(
                (
                    "The uuid that was generated, when task to stream data was "
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

    if ZARRAY_JSON in chunk or ZATTRS_JSON in chunk:
        json_meta: Dict[str, Any] = await read_redis_data(
            uuid5, "json_meta", timeout=timeout
        )
        if ZATTRS_JSON in chunk:
            key = f"{variable}/{ZATTRS_JSON}"
        else:
            key = f"{variable}/{ZARRAY_JSON}"
        try:
            content = json_meta["metadata"][key]
        except KeyError as error:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(error))
        return JSONResponse(
            content=content,
            status_code=status.HTTP_200_OK,
        )
    if ZGROUP_JSON in chunk:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Sub groups are not supported.",
        )
    chunk_key = f"{uuid5}-{variable}-{chunk}"
    detail = {"chunk": {"uuid": uuid5, "variable": variable, "chunk": chunk}}
    cache = await create_redis_connection()
    await cache.publish("data-portal", json.dumps(detail).encode("utf-8"))
    data: bytes = await read_redis_data(chunk_key, timeout=timeout)
    return Response(data, media_type="application/octet-stream")
