"""Utilities for zarr loading."""

import asyncio
import binascii
import hashlib
import json
import uuid
from enum import Enum
from typing import Any, Awaitable, Dict, List, Literal, Optional, Union, cast

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
    6: "permission denied",
}

# Default retry interval in seconds, sent via Retry-After header
_RETRY_AFTER = 2


class LoadStatus(Enum):
    """Definitions of the load status.

    Each member maps an internal processing state to an appropriate
    HTTP response code:

    * ``finished_ok`` → 200: data ready.
    * ``finished_failed`` → 500: loading failed permanently.
    * ``finished_permission_denied`` →  403: permission denied
    * ``finished_not_found`` → 404: source file does not exist.
    * ``waiting`` → 503 + Retry-After: queued, not yet started.
    * ``processing`` → 503 + Retry-After: actively being loaded.
    * ``unknown`` → 404: never submitted or cache evicted.
    """

    finished_ok = 0
    finished_failed = 1
    finished_not_found = 2
    waiting = 3
    processing = 4
    unknown = 5
    finished_permission_denied = 6

    @property
    def response(self) -> int:
        """Translate the internal status to a restful request status."""
        return {
            "finished_ok": 200,
            "finished_failed": 500,
            "finished_not_found": 404,
            "waiting": 503,
            "processing": 503,
            "unknown": 404,
            "finished_permission_denied": 403,
        }.get(self.name, 500)

    @property
    def retryable(self) -> bool:
        """Whether the client should retry after this status."""
        return self.value in (self.waiting.value, self.processing.value)

    @property
    def detail(self) -> str:
        """Default human-readable message for this status."""
        return {
            "finished_ok": "Data ready.",
            "finished_failed": "Data loading failed.",
            "finished_not_found": "Source file not found.",
            "finished_permisson_denied": "Not allowed to access resource",
            "waiting": "Data is queued for loading, please retry.",
            "processing": "Data is being loaded, please retry.",
            "unknown": "Dataset not found.",
        }.get(self.name, "Unknown status.")


async def _query_broker_on_permissions(
    username: str, paths: List[str], timeout: float = 5.0
) -> bool:
    request_id = str(uuid.uuid4())
    await Cache.publish(
        "data-portal",
        json.dumps(
            {
                "access_check": {
                    "request_id": request_id,
                    "username": username or None,
                    "paths": paths,
                }
            }
        ).encode("utf-8"),
    )
    # Block-wait for the reply
    result = await cast(
        Awaitable[Optional[List[bytes]]],
        Cache.blpop(f"access-reply:{request_id}", timeout=timeout),
    )
    if result is None:
        raise HTTPException(503, "Data-loader service unavailable.")
    allowed: bool = json.loads(result[1]).get("allowed", False)
    return allowed


async def check_read_permission(username: str, paths: List[str]) -> None:
    """Check (via data-loader) if a given user has read access to a path."""
    paths_bytes = json.dumps(paths).encode("utf-8")
    hex_digest = hashlib.sha256(paths_bytes).hexdigest()
    cache_key = f"access:{username}:{hex_digest}"
    cached = await Cache.get(cache_key)
    if cached is None:
        allowed = await _query_broker_on_permissions(username, paths)
        await Cache.set(cache_key, b"1" if allowed else b"0", ex=300)
    else:
        allowed = cached == b"1"
    if not allowed:
        raise HTTPException(status_code=403, detail="User not allowed to read paths.")


async def _trigger_loading(
    paths: List[str],
    token: str,
    assembly: Optional[Dict[str, Optional[str]]] = None,
    access_pattern: str = "map",
    map_primary_chunksize: int = 1,
    reload: bool = False,
    chunk_size: float = 16.0,
    username: Optional[str] = None,
) -> None:
    """Send a loading instruction to the data-loader via Redis.

    This is an internal helper that bypasses permission checks.
    It should only be called from code paths where permissions have
    already been verified (e.g. lazy re-publish from ``read_redis_data``).
    """
    await Cache.publish(
        "data-portal",
        json.dumps(
            {
                "uri": {
                    "username": username,
                    "path": paths,
                    "uuid": token,
                    "assembly": assembly or {},
                    "access_pattern": access_pattern,
                    "map_primary_chunksize": map_primary_chunksize,
                    "reload": reload,
                    "chunk_size": chunk_size,
                }
            }
        ).encode("utf-8"),
    )


async def publish_datasets(
    paths: Union[str, List[str]],
    public: bool = False,
    ttl_seconds: float = 86400.0,
    publish: bool = False,
    aggregation_plan: Optional[Dict[str, Optional[str]]] = None,
    access_pattern: Literal["map", "time_series"] = "map",
    map_primary_chunksize: int = 1,
    reload: bool = False,
    chunk_size: float = 16.0,
    username: Optional[str] = None,
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
    access_pattern: str
        Apply chunk size optimisation for this access pattern.
    map_primary_chunksize: int
        Size of the primary chunks in map access pattern.
    reload: bool
        (Re)trigger the loading of a data set.
    chunk_size: float
        Set the target chunk size.
    username: str, optional
        Username for filesystem permission checks. If ``None``,
        permission checks are skipped (caller asserts they were
        already performed).

    Returns
    -------
    str:
        The url to the converted zarr endpoint.
    """
    await Cache.check_connection()
    paths = paths if isinstance(paths, list) else [paths]
    norm_paths = [p.replace("file:///", "/") for p in paths]
    if username is not None:
        await check_read_permission(username, norm_paths)
    token = encode_cache_token(norm_paths, assembly=aggregation_plan)
    api_path = f"{server_config.proxy}/api/freva-nextgen/data-portal"
    if publish or reload:
        await _trigger_loading(
            norm_paths,
            token,
            assembly=aggregation_plan,
            access_pattern=access_pattern,
            map_primary_chunksize=map_primary_chunksize,
            reload=reload,
            chunk_size=chunk_size,
            username=username,
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
    timeout: int = 3,
    token_suffix: str = "",
) -> Any:
    """Read cached data for a zarr token, triggering lazy loading if needed.

    Parameters
    ----------
    token: str
        The token used to decode the path.
    subkey: str
        If the data under key is a pickled dict then it will be
        unpickled and the value of that subkey will be returned.
    timeout: int
        Wait for timeout seconds until a not-ready error is raised.
    token_suffix: str
        Suffix appended to the token to form the cache key for
        chunk-level data.
    """
    await Cache.check_connection()
    try:
        payload = decode_cache_token(token)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Invalid path.")

    key = token + token_suffix
    raw = await Cache.get(token)

    just_triggered = bool(token_suffix)
    if raw is None:
        # No metadata in cache — lazy publish: send loading instruction
        # directly without re-checking permissions (already verified at
        # the endpoint level).
        await _trigger_loading(payload["path"], token, assembly=payload["assembly"])
        just_triggered = True

    else:
        meta_data = cloudpickle.loads(raw)
        load_status = meta_data.get("status", LoadStatus.unknown.value)
        if load_status == LoadStatus.finished_failed.value:
            # Previously failed — retry loading
            await _trigger_loading(
                payload["path"],
                token,
                assembly=payload["assembly"],
                reload=True,
            )
            just_triggered = True
    # If we just triggered loading, default to "waiting" — we know
    # work is in flight. Otherwise default to "unknown" — the data
    # should already be there.
    default_status = (
        LoadStatus.waiting.value if just_triggered else LoadStatus.unknown.value
    )

    # Poll for completion
    npolls = 0.0
    dt = 0.5
    data = cloudpickle.loads((await Cache.get(key)) or b"\x80\x05}\x94.")
    task_status = LoadStatus(data.get("status", default_status))
    while task_status.retryable:
        npolls += dt
        if npolls >= timeout:
            break
        await asyncio.sleep(dt)
        data = cloudpickle.loads((await Cache.get(key)) or b"\x80\x05}\x94.")
        task_status = LoadStatus(data.get("status", LoadStatus.processing.value))

    task_status = LoadStatus(data.get("status", LoadStatus.unknown.value))
    if task_status.value != LoadStatus.finished_ok.value:
        raise HTTPException(
            task_status.response,
            detail=data.get("reason") or task_status.detail,
            headers={"Retry-After": str(_RETRY_AFTER)}
            if task_status.retryable
            else None,
        )
    return data[subkey]


async def load_chunk(_id: str, variable: str, chunk: str, timeout: int = 1) -> Response:
    """Load a zarr chunk from the cache."""
    detail = {"chunk": {"uuid": _id, "variable": variable, "chunk": chunk}}
    await Cache.check_connection()
    await Cache.publish("data-portal", json.dumps(detail).encode("utf-8"))
    data: bytes = await read_redis_data(
        _id, "data", token_suffix=f"-{variable}-{chunk}", timeout=timeout
    )
    return Response(data, media_type="application/octet-stream")


async def load_zarr_metadata(
    _id: str, attr: Optional[str] = None, timeout: int = 10
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


async def process_zarr_data(token: str, zarr_key: str, timeout: int = 10) -> Response:
    """Serve arbitrary Zarr metadata or chunk keys.

    Zarr clients access stores by issuing HTTP GET requests on a hierarchy of
    keys rather than downloading a single monolithic file.  This method
    enables the rest endpoints to access any key under a namespace,
    whether it refers to root-level metadata (e.g. ``.zmetadata``, ``.zgroup``,
    ``.zattrs``), variable-specific metadata (e.g. ``tas/.zarray``), or data
    chunks (e.g. ``tas/0.0.0``).  For root-level metadata keys we call
    ``load_zarr_metadata``, and for all other keys we delegate to
    ``load_chunk`` using the parent path as the variable and the final
    segment as the chunk identifier.
    """
    if zarr_key == ZARR_JSON:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Zarr v3 not supported.",
        )
    if zarr_key == ZMETADATA_JSON:
        return await load_zarr_metadata(token, timeout=timeout)
    if zarr_key in (ZGROUP_JSON, ZATTRS_JSON):
        return await load_zarr_metadata(token, zarr_key, timeout=timeout)
    zarr_key = zarr_key.lstrip("/")
    if zarr_key.endswith(("/" + ZGROUP_JSON, "/" + ZATTRS_JSON, "/" + ZARRAY_JSON)):
        return await load_zarr_metadata(
            token,
            zarr_key,
            timeout=timeout,
        )
    if zarr_key in (ZARRAY_JSON, ZATTRS_JSON):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A group/variable name must precede .zarray or .zattrs",
        )
    if "/" not in zarr_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Invalid zarr key: expected a slash-separated variable/chunk "
                f"path. Received '{zarr_key}'."
            ),
        )
    array_path, _, leaf = zarr_key.rpartition("/")
    return await load_chunk(token, array_path, leaf, timeout)
