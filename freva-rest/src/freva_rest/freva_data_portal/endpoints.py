"""Definition of endpoints for loading/streaming and manipulating data."""

from typing import Annotated, List, Literal, Optional, Union

import cloudpickle
from fastapi import Path, Query, Security
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
)

from .utils import (
    STATUS_LOOKUP,
    AggregationPlan,
    ConcatOptions,
    MergeOptions,
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
        Union[str, List[str]],
        Query(
            title="Path to data.",
            description=(
                "Absolute or object‑store paths to the data files to "
                "convert. You can add multiple files if you whish to aggregate"
                " data."
            ),
            examples=["/work/abc1234/myuser/my-data.nc"],
        ),
    ],
    aggregate: Annotated[
        Optional[Literal["auto", "merge", "concat"]],
        Query(
            title="Aggregte Data",
            description=(
                "If data needs to be aggregated, "
                "instruct which aggregation method should be used  "
                "(auto, merge or concat). If set to auto the system will "
                "try to infer a plan."
            ),
            examples=["concat"],
        ),
    ] = None,
    join: Annotated[
        Optional[Literal["outer", "inner", "exact", "left", "right"]],
        Query(
            title="Join Mode",
            description=(
                "How to align coordinate indexes across inputs "
                "(outer, inner, exact, left, right)."
            ),
            examples=["inner"],
        ),
    ] = None,
    compat: Annotated[
        Optional[Literal["no_conflicts", "equals", "override"]],
        Query(
            title="Compat Mode",
            description=(
                "How to treat variables with same name. "
                " choose from: equals, no_conflicts, override."
            ),
            examples=["no_conflicts"],
        ),
    ] = None,
    data_vars: Annotated[
        Optional[Literal["minimal", "different", "all"]],
        Query(
            title="Variable concat.",
            alias="data-vars",
            description=(
                "Which data variables to concatenate (minimal, different, all)"
            ),
            examples=["minimal"],
        ),
    ] = None,
    coords: Annotated[
        Optional[Literal["minimal", "different", "all"]],
        Query(
            title="Coordinate concat.",
            description=(
                "Which data coordinates to concatenate (minimal, different, all)"
            ),
            examples=["minimal"],
        ),
    ] = None,
    dim: Annotated[
        Optional[str],
        Query(
            title="Dim",
            description=(
                "Dimension to concatenate along. If it does not exist,"
                "a new dimension is created."
            ),
            examples=["tas"],
        ),
    ] = None,
    group_by: Annotated[
        Optional[str],
        Query(
            title="Group by",
            alias="group-by",
            description=(
                "If set, forces grouping by a signature key. "
                "Otherwise grouping is attempted only when direct combine "
                "fails."
            ),
            examples=["ensemble"],
        ),
    ] = None,
) -> LoadResponse:
    """Publish a conversion request to the data‑portal worker.

    - **path**: absolute filesystem or object‑store path to the input file.
    - **returns**: a URL containing a token where the Zarr store will be served.
    - **note**: this function does **not** check that the input path exists or
      is readable by; that check occurs asynchronously in the worker.
    """
    aggregation_plan = AggregationPlan(
        mode=aggregate,
        concat=ConcatOptions(
            dim=dim,
            compat=compat,
            join=join,
            data_vars=data_vars,
            coords=coords,
        ),
        merge=MergeOptions(compat=compat, join=join),
        group_by=group_by,
    )

    try:
        return LoadResponse(
            urls=await publish_datasets(path, aggregation_plan=aggregation_plan)
        )
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
    stat = cloudpickle.loads(
        await Cache.get(token) or cloudpickle.dumps({"status": 5})
    )
    return ZarrStatus(
        status=stat.get("status", 5), reason=stat.get("reason", "Unkown")
    )


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
