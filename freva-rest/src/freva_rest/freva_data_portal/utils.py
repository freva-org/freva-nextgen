"""Utilities for zarr loading."""

import asyncio
import json
from typing import Any, Dict, Optional

import cloudpickle
from fastapi import status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, Response

from freva_rest.utils.base_utils import (
    Cache,
    decode_path_token,
    publish_dataset,
)

ZARRAY_JSON = ".zarray"
ZGROUP_JSON = ".zgroup"
ZATTRS_JSON = ".zattrs"
STATUS_LOOKUP = {
    0: "finished, ok",
    1: "finished, failed",
    2: "waiting",
    3: "processing",
    5: "gone",
}


async def read_redis_data(
    token: str,
    subkey: Optional[str] = None,
    timeout: int = 1,
    token_suffix: str = "",
) -> Any:
    """Read the cache data given by a key.

    Parameters
    ----------
    token: str
        The token used to decode the path.
    subkey: str|None
        If the data under key is a pickled dict the the will be
        unpickled and and the value of that subkey will be returned
    timeout: int
        Wait for timeout seconds until a not found error is risen.
    """

    await Cache.check_connection()
    try:
        path = decode_path_token(token)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Invalid path.")
    key = token + token_suffix
    data: Optional[bytes] = await Cache.get(key)
    if data is None:
        await publish_dataset(path, publish=True)
        timeout += 1
    npolls = 0.0
    dt = 0.5
    while data is None:
        npolls += dt
        await asyncio.sleep(dt)
        data = await Cache.get(key)
        if npolls >= timeout:
            break
    if data is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"{key} uuid does not exist (anymore).",
        )
    if not subkey:
        return data
    p_data: Dict[str, Any] = cloudpickle.loads(data)
    task_status = p_data.get("status", 1)
    if task_status != 0:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=STATUS_LOOKUP.get(task_status, "unknown"),
        )
    return p_data[subkey]


async def load_chunk(
    _id: str, variable: str, chunk: str, timeout: int = 1
) -> Response:
    """Load a zarr chunk from the cache."""

    if ZARRAY_JSON in chunk or ZATTRS_JSON in chunk:
        json_meta: Dict[str, Any] = await read_redis_data(
            _id, "json_meta", timeout=timeout
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
    detail = {"chunk": {"uuid": _id, "variable": variable, "chunk": chunk}}
    await Cache.check_connection()
    await Cache.publish("data-portal", json.dumps(detail).encode("utf-8"))
    data: bytes = await read_redis_data(
        _id, token_suffix=f"-{variable}-{chunk}", timeout=timeout
    )
    return Response(data, media_type="application/octet-stream")


async def load_zarr_metadata(
    _id: str, attr: Optional[str] = None, timeout: int = 1
) -> JSONResponse:
    """Read the .zarrattr."""
    meta: Dict[str, Any] = await read_redis_data(
        _id, "json_meta", timeout=timeout
    )
    if attr:
        return JSONResponse(
            content=meta["metadata"][attr], status_code=status.HTTP_200_OK
        )
    return JSONResponse(content=meta, status_code=status.HTTP_200_OK)
