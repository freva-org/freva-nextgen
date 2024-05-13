"""Definition of endpoints for loading/streaming and manipulating data."""

import asyncio
import os
from typing import Annotated, Optional

import cloudpickle
from fastapi import Path, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, Response
import redis.asyncio as redis

from freva_rest.rest import app
from freva_rest.utils import send_borker_message

REDIS_HOST, _, REDIS_PORT = (
    (os.environ.get("REDIS_HOST") or "localhost")
    .removeprefix("redis://")
    .partition(":")
)


async def read_redis_data(
    key: str, subkey: Optional[str] = None, timeout: int = 1
) -> bytes:
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
    try:
        client = redis.Redis(
            host=REDIS_HOST, port=int(REDIS_PORT or "6379"), db=0
        )
        data: Optional[bytes] = await client.get(key)
        npolls = 0
        while data is None:
            npolls += 1
            await asyncio.sleep(1)
            data = await client.get(key)
            if npolls >= timeout:
                break
    finally:
        await client.aclose()
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
            detail=lookup.get(task_status, "unkown"),
        )
    try:
        return p_data[subkey]
    except KeyError:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, detail="key Error"
        )


@app.get(
    "/api/freva-data-portal/zarr/{uuid5}/.zmetadata",
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
    ]
) -> JSONResponse:
    """Consolidate zarr metadata

    This endpoint returns the metadata about the structure and organization of
    data within the particular zarr store in question.
    """
    import json

    meta = cloudpickle.loads(await read_redis_data(uuid5, "json_meta"))
    print(json.dumps(meta).encode("utf-8"))
    print(type(meta))
    return JSONResponse(
        content=meta,
        status_code=status.HTTP_200_OK,
    )


@app.get(
    "/api/freva-data-portal/zarr/{uuid5}/.zgroup",
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
    ]
) -> JSONResponse:
    """Zarr group data.

    This `.zarrgroup` metadata includes information about the arrays and
    subgroups contained within the group, as well as their attributes such as
    data types, shapes, and chunk sizes. The `.zarrgroup` endpoint helps in
    organizing and managing the structure of data within a Zarr group,
    allowing users to access and manipulate arrays and subgroups efficiently.
    """
    meta = cloudpickle.loads(await read_redis_data(uuid5, "json_meta"))
    return JSONResponse(
        content=meta["metadata"][".zgroup"],
        status_code=status.HTTP_200_OK,
    )


@app.get(
    "/api/freva-data-portal/zarr/{uuid5}/.zattrs",
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
    ]
) -> JSONResponse:
    """Get zarr Attributes.

    Get metadata attributes associated with the dataset or arrays within the
    dataset. These attributes provide additional information about the dataset
    or arrays, such as descriptions, units, creation dates, or any other
    custom metadata relevant to the data.
    """
    meta = cloudpickle.loads(await read_redis_data(uuid5, "json_meta"))
    return JSONResponse(
        content=meta["metadata"][".zattrs"], status_code=status.HTTP_200_OK
    )


@app.get(
    "/api/freva-data-portal/zarr/{uuid5}/{variable}/{chunk}",
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
        Path(
            title="chunk", description="The chnuk number that should be read."
        ),
    ],
) -> Response:
    """Get a zarr array chunk.

    This method reads the zarr data."""

    if array_meta_key in chunk or attrs_key in chunk:
        meta = cloudpickle.loads(await read_redis_data(uuid5, "json_meta"))
        if attrs_key in chunk:
            key = f"{variable}/{attrs_key}"
        else:
            key = f"{variable}/{array_meta_key}"
        return JSONResponse(
            content=meta[key],
            status_code=status.HTTP_200_OK,
        )
    if group_meta_key in chunk:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Sub groups are not supported.",
        )
    chunk_key = f"{uuid5}-{variable}-{chunk}"
    detail = {"chunk": {"uuid": uuid5, "variable": variable, "chunk": chunk}}
    await send_borker_message(json.dumps(detail).encode("utf-8"))
    try:
        meta = await DataLoadFactory.get_zarr_metadata(
            uuid5, jsonfify=False, cache=RedisCache
        )
    except RuntimeError as error:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
        )
    except KeyError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(error))
    dset = await DataLoadFactory.load_dataset(uuid5, RedisCache)
    return Response(data, media_type="application/octet-stream")
