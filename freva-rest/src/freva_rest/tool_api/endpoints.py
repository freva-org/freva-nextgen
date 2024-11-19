"""Define all public endpoints."""

from datetime import datetime
from typing import Annotated, Dict
from uuid import NAMESPACE_URL, UUID, uuid5

from fastapi import Body, Depends, Path, status
from fastapi.responses import (
    JSONResponse,
    PlainTextResponse,
    StreamingResponse,
)

from freva_rest.auth import TokenPayload, auth
from freva_rest.rest import app

from .internal_endpoints import tool_communication
from .schemas import (
    CancelJobStatus,
    ChangeVisibilityStatus,
    DisableVisibilityPaylaod,
    EnableVisibilityPayload,
    SubmitJobStatus,
    ToolAddPayload,
    ToolParameterPayload,
)
from .tool_abstract import ToolAbstract

__all__ = [
    "tool_communication",
    "tool_overview",
    "add_tool",
    "submit_tool",
    "cancel_job",
    "disable_tool",
]


@app.get(
    "/api/freva-nextgen/tool/overview",
    tags=["Analysis Tools"],
    response_model=Dict[str, ToolAbstract],
    status_code=status.HTTP_200_OK,
    responses={
        401: {"description": "Unauthorised / not a valid token."},
        503: {"description": "Service is currently unavailable."},
        200: {"description": "List of tools that are currently available"},
    },
)
async def tool_overview(
    current_user: TokenPayload = Depends(auth.required),
) -> JSONResponse:
    """Get all available tools and their attributes."""
    tool = ToolAbstract(
        name="foo",
        author="bar",
        version="0.0",
        summary="",
        parameters=[],
        added=datetime.now(),
        command="fakecommand",
        binary="fakebinary",
        output_type="data",
    ).model_dump()
    return JSONResponse(status_code=status.HTTP_200_OK, content={"foo": tool})


@app.post(
    "/api/freva-nextgen/tool/add/{tool}",
    tags=["Analysis Tools"],
    status_code=status.HTTP_200_OK,
    responses={
        400: {"description": "Invalid tool configuration."},
        401: {"description": "Unauthorised / not a valid token."},
        500: {"description": "Adding the tool was unsuccessful."},
        503: {"description": "Service is currently unavailable."},
    },
    response_class=PlainTextResponse,
)
async def add_tool(
    tool: Annotated[
        str,
        Path(
            title="Tool",
            description=(
                "The name of the data analysis tool that should be added."
            ),
            examples=["animator"],
        ),
    ],
    properties: Annotated[
        ToolAddPayload, Body(..., description=ToolAddPayload.__doc__)
    ],
    current_user: TokenPayload = Depends(auth.required),
) -> StreamingResponse:
    """Add a tool to the MongoDB or update it if it already exists."""
    return StreamingResponse(
        ("" for _ in range(10)),
        status_code=status.HTTP_200_OK,
        media_type="text/plain",
    )


@app.post(
    "/api/freva-nextgen/tool/disable/{tool}",
    tags=["Analysis Tools"],
    status_code=status.HTTP_200_OK,
    responses={
        401: {"description": "Unauthorised / not a valid token."},
        404: {"description": "The requested tool cannot be found."},
        500: {"description": "Removing tool was unsuccessful."},
        503: {"description": "Service is currently unavailable."},
    },
    response_model=ChangeVisibilityStatus,
)
async def disable_tool(
    tool: Annotated[
        str,
        Path(
            title="Tool",
            description=(
                "The name of the data analysis tool that should be disbled."
            ),
            examples=["animator"],
        ),
    ],
    properties: Annotated[DisableVisibilityPaylaod, Body(...)],
    current_user: TokenPayload = Depends(auth.required),
) -> ChangeVisibilityStatus:
    """Remove a tool and its metadata from the database."""
    return ChangeVisibilityStatus(
        message="Foo bar! Tool was disabled.", status_code=0
    )


@app.post(
    "/api/freva-nextgen/tool/enable/{tool}",
    tags=["Analysis Tools"],
    status_code=status.HTTP_200_OK,
    responses={
        401: {"description": "Unauthorised / not a valid token."},
        404: {"description": "The requested tool cannot be found."},
        500: {"description": "Removing tool was unsuccessful."},
        503: {"description": "Service is currently unavailable."},
    },
    response_model=ChangeVisibilityStatus,
)
async def enable_tool(
    tool: Annotated[
        str,
        Path(
            title="Tool",
            description=(
                "The name of the data analysis tool that should be disbled."
            ),
            examples=["animator"],
        ),
    ],
    properties: Annotated[EnableVisibilityPayload, Body(...)],
    current_user: TokenPayload = Depends(auth.required),
) -> ChangeVisibilityStatus:
    """Enable a tool for a list of users."""
    return ChangeVisibilityStatus(
        message="Foo bar! Tool was enabled.", status_code=0
    )


@app.post(
    "/api/freva-nextgen/tool/submit/{tool}",
    tags=["Analysis Tools"],
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        400: {"description": "Invalid input parameters were given."},
        401: {"description": "Unauthorised / not a valid token."},
        404: {"description": "If the tool is not known to the system."},
        500: {"description": "If the job submission failed."},
        503: {"description": "If the service is currently unavailable."},
        202: {"description": "The job has been successfully submitted."},
    },
    response_model=SubmitJobStatus,
)
async def submit_tool(
    tool: Annotated[
        str,
        Path(
            title="Tool",
            description=(
                "The name of the data analysis tool that should be " "applied."
            ),
            examples=["animator"],
        ),
    ],
    payload: Annotated[ToolParameterPayload, Body(...)],
    current_user: TokenPayload = Depends(auth.required),
) -> SubmitJobStatus:
    """Submit an existing tool in via the workload manager.

    This endpoint should be submitting a tool that is knwon to the system.

    Business Logic:
        - Check if tool exists -> if not: 404.
        - Parse given input parameters -> if failed: 400.
        - Connect to remote machine. -> if failed: 503
        - Create input script on remote machine.
        - Submit script. -> if failed: 500.
        - Exit 201
    """
    return SubmitJobStatus(
        uuid=uuid5(NAMESPACE_URL, "foo"),
        status_code=0,
        time_submitted=datetime.now().isoformat(),
        hostname="foo.bar",
        batch_mode=True,
    )


@app.get(
    "/api/freva-nextgen/tool/log/{uuid}",
    tags=["Analysis Tools"],
    status_code=status.HTTP_200_OK,
    responses={
        401: {"description": "Unauthorised / not a valid token."},
        404: {"description": "The job id is not known to the system."},
        500: {"description": "If reading the logs failed."},
    },
    response_class=PlainTextResponse,
)
async def job_log(
    uuid: Annotated[
        UUID,
        Path(
            title="Tool id.",
            description="Unique identifyer for the tool.",
            examples=["095be615-a8ad-4c33-8e9c-c7612fbf6c9f"],
        ),
    ],
    current_user: TokenPayload = Depends(auth.required),
) -> StreamingResponse:
    """Read the logs from a tool.

    This endpoint streams the logs (stdout & stderr) of a tool.

    Business Logic:
        - Check if job exists -> if not: 404
        - Check if output exists and can be read -> if not: 500
    """
    return StreamingResponse(
        ("" for _ in range(10)),
        status_code=status.HTTP_200_OK,
        media_type="text/plain",
    )


@app.post(
    "/api/freva-nextgen/tool/cancel/{uuid}",
    tags=["Analysis Tools"],
    status_code=status.HTTP_200_OK,
    responses={
        401: {"description": "Unauthorised / not a valid token."},
        404: {"description": "The job id is not known to the system."},
        500: {"description": "If the job cancelation failed."},
    },
    response_model=CancelJobStatus,
)
async def cancel_job(
    uuid: Annotated[
        UUID,
        Path(
            title="Tool id.",
            description="Unique identifyer for the tool.",
            examples=["095be615-a8ad-4c33-8e9c-c7612fbf6c9f"],
        ),
    ],
    current_user: TokenPayload = Depends(auth.required),
) -> CancelJobStatus:
    """Cancel a running job of a given uuid.

    This endpoint cancels a *running* of a given uuid that belongs to a user.

    Business Logic:
        - Check if job exists -> if not: 404
        - Check if job has already bin cacneled -> if so: 200 status: 1
        - Check if job belongs to user: if not: 401
        - Cancel job: if failed: 500
    """
    return CancelJobStatus(message="foo", status_code=0)
