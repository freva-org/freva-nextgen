"""Utilities for zarr loading."""

import asyncio
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import cloudpickle
from fastapi import status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, Response

from freva_rest.rest import server_config
from freva_rest.utils.base_utils import (
    Cache,
    add_ttl_key_to_db_and_cache,
    decode_cache_token,
    encode_cache_token,
)

ZARRAY_JSON = ".zarray"
ZGROUP_JSON = ".zgroup"
ZATTRS_JSON = ".zattrs"
ZMETADATA_JSON = ".zmetadata"
ZARR_JSON = "zarr.json"
STATUS_LOOKUP = {
    0: "finished, ok",
    1: "finished, failed",
    2: "finished, not found",
    3: "waiting",
    4: "processing",
    5: "gone",
}


class LoadStatus(Enum):
    """Definitions of the load status."""

    finished_ok = 0
    finished_failed = 1
    finished_not_found = 2
    waiting = 3
    processing = 4
    unknown = 5

    @property
    def response(self) -> int:
        """Translate the internal status to a restful request status."""
        return {
            "finished_ok": 200,
            "finished_failed": 500,
            "finished_not_found": 404,
            "waiting": 503,
            "processing": 503,
            "unknown": 503,
        }.get(self.name, 503)


async def publish_datasets(
    paths: Union[str, List[str]],
    public: bool = False,
    ttl_seconds: float = 86400.0,
    publish: bool = False,
    aggregation_plan: Optional[Dict[str, Optional[str]]] = None,
) -> str:
    """Publish a path on disk for zarr conversion to the broker.

    Parameters
    ----------
    paths:
        The path or a sequence of paths that needs to be converted.
    public: bool, default: False
        Create a public zarr store.
    ttl_seconds: float, default: 84600.0
        TTL of the public zarr url, if any
    publish: bool, default: False
        Send the loading instruction to the broker.
    aggregation_plan: dict, optional
        A plan dict describing how to aggregate the datasets.
        If None, the worker will infer a plan.

    Returns
    -------
    str:
        The url to the converted zarr endpoint

    """
    await Cache.check_connection()
    paths = paths if isinstance(paths, list) else [paths]
    norm_paths = [p.replace("file:///", "/") for p in paths]
    token = encode_cache_token(norm_paths, assembly=aggregation_plan)
    api_path = f"{server_config.proxy}/api/freva-nextgen/data-portal"
    if publish:
        await Cache.publish(
            "data-portal",
            json.dumps(
                {
                    "uri": {
                        "path": norm_paths,
                        "uuid": token,
                        "assembly": aggregation_plan or {},
                    }
                }
            ).encode("utf-8"),
        )
    if public is True:
        res = await add_ttl_key_to_db_and_cache(
            norm_paths, ttl_seconds, aggregation_plan
        )
        return f"{api_path}/share/{res['key']}.zarr"
    return f"{api_path}/zarr/{token}.zarr"


async def read_redis_data(
    token: str,
    subkey: str = "data",
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
    jsonify: bool
        Jsonify data.
    """

    await Cache.check_connection()
    try:
        payload = decode_cache_token(token)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Invalid path.")
    key = token + token_suffix
    meta_data = cloudpickle.loads((await Cache.get(token)) or b"\x80\x05}\x94.")
    if not meta_data:
        await publish_datasets(
            payload["path"],
            aggregation_plan=payload["assembly"],
            ttl_seconds=payload["exp"],
            publish=True,
        )
        timeout += 1
    npolls = 0.0
    dt = 0.5
    data = cloudpickle.loads((await Cache.get(key)) or b"\x80\x05}\x94.")
    task_status = LoadStatus(data.get("status", 5))
    while task_status.value > 2:
        npolls += dt
        await asyncio.sleep(dt)
        data = cloudpickle.loads((await Cache.get(key)) or b"\x80\x05}\x94.")
        task_status = LoadStatus(data.get("status", LoadStatus.processing.value))
        if npolls >= timeout:
            break

    task_status = LoadStatus(data.get("status", LoadStatus.unknown.value))
    if task_status.value != 0:
        raise HTTPException(
            task_status.response,
            detail=data.get("reason", f"{key} uuid does not exist (anymore)."),
        )
    return data[subkey]


async def load_chunk(
    _id: str, variable: str, chunk: str, timeout: int = 1
) -> Response:
    """Load a zarr chunk from the cache."""

    if ZARRAY_JSON in chunk or ZATTRS_JSON in chunk:
        json_meta: Dict[str, Any] = await read_redis_data(
            _id, "data", timeout=timeout
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
        _id, "data", token_suffix=f"-{variable}-{chunk}", timeout=timeout
    )
    return Response(data, media_type="application/octet-stream")


async def load_zarr_metadata(
    _id: str, attr: Optional[str] = None, timeout: int = 1
) -> JSONResponse:
    """Read the .zarrattr."""
    meta: Dict[str, Any] = await read_redis_data(_id, "data", timeout=timeout)
    if attr:
        try:
            return JSONResponse(
                content=meta["metadata"][attr], status_code=status.HTTP_200_OK
            )
        except KeyError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Key not found {attr}",
            )

    return JSONResponse(content=meta, status_code=status.HTTP_200_OK)


async def process_zarr_data(
    token: str, zarr_key: str, timeout: int = 1
) -> Response:
    """Serve arbitrary Zarr metadata or chunk keys.

    Zarr clients access stores by issuing HTTP GET requests on a hierarchy of
    keys rather than downloading a single monolithic file.  This method
    enables the rest endpoints to access any key under a namespace,
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
    if zarr_key == ZARR_JSON:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Zarr v3 not supported.",
        )
    if zarr_key == ZMETADATA_JSON:
        return await load_zarr_metadata(token, timeout=timeout)
    if zarr_key in (ZMETADATA_JSON, ZGROUP_JSON, ZATTRS_JSON):
        return await load_zarr_metadata(token, zarr_key, timeout=timeout)
    zarr_key = zarr_key.lstrip("/")
    if zarr_key.endswith(
        ("/" + ZGROUP_JSON, "/" + ZATTRS_JSON, "/" + ZARRAY_JSON)
    ):
        # prefix may be "group0" or "group0/tas"
        return await load_zarr_metadata(
            token,
            zarr_key,
            timeout=timeout,
        )
    if zarr_key == ZARRAY_JSON or zarr_key == ZATTRS_JSON:
        # Requests like `/.../.zarray` or `/.../.zattrs` at the root level are
        # invalid because a variable path is required.  Return a descriptive
        # error rather than delegating to the chunk loader, which would
        # misinterpret these keys.
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A group/variable name must precede .zarray or .zattrs",
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
    array_path, leaf = zarr_key.rsplit("/", 1)
    if leaf in (ZARRAY_JSON, ZATTRS_JSON):
        return await load_zarr_metadata(
            token,
            leaf,
            timeout=timeout,
        )
    # Delegate to load_chunk.  It will detect `.zarray` and `.zattrs`
    # requests at the variable level and return the metadata accordingly,
    # otherwise it will stream the requested data chunk.
    return await load_chunk(token, array_path, leaf, timeout)
