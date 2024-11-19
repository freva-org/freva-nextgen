"""API schema definitions."""

from datetime import datetime
from typing import Annotated, Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field

ParameterItems = Union[str, float, int, datetime, bool, None]


class DisableVisibilityPaylaod(BaseModel):
    """Define the payload for disabling the visibility of a tool."""

    versions: Annotated[
        Optional[List[str]],
        Field(
            title="Versions",
            description=(
                "List of tool versions that are effected by the "
                "change of visibility, by default all versions are "
                "affected."
            ),
            examples=[["v0.0.1", "v0.0.2"]],
        ),
    ] = None


class EnableVisibilityPayload(DisableVisibilityPaylaod):
    """This class changes the visibility of the a tool."""

    users: Annotated[
        Optional[List[str]],
        Field(
            title="Users",
            description=(
                "List of username that are affected by the change "
                "of visibility. By default this tool will be available to all "
                "users."
            ),
            examples=[["a1234", "b1234"]],
        ),
    ] = None


class ToolAddPayload(BaseModel):
    """Define how a tool is added to the system.

    There are three fundamental options:

        - Directly passing a toml string that holds he tool configuration.
        - Passing a git url where the tool configuration is stored.
        - Using a pre-defined tool from the central tool repository.

    """

    url: Annotated[
        Optional[str],
        Field(
            title="Git URL",
            description=(
                "Use the url of a *public* git repository to add the tool."
            ),
        ),
    ] = None
    name: Annotated[
        Optional[str],
        Field(
            title="Predefined tool",
            description=(
                "You can add tools from the pre-defined tool "
                "repository by simply providing the name of the "
                "tool"
            ),
        ),
    ] = None


class ToolParameterPayload(BaseModel):
    """This class represents the payload for the tool import parameters."""

    parameters: Annotated[
        Optional[Dict[str, Union[ParameterItems, List[ParameterItems]]]],
        Field(
            title="Parameters",
            description="The parameters you pass to the tool.",
            examples=[
                {
                    "variable": "tas",
                    "product": "EUR-11",
                    "project": "cordex",
                    "time-frequency": "day",
                    "experiment": "historical",
                    "time-period": ["1970", "2000"],
                }
            ],
        ),
    ] = None
    version: Annotated[
        Optional[str],
        Field(
            title="Version",
            description=(
                "The version of the tool, if not given the latest "
                "version will be applied."
            ),
            examples=["v0.0.1"],
        ),
    ] = None


class AddToolStatus(BaseModel):
    """Response model for successful addition of a tool."""

    uuid: Annotated[
        UUID,
        Field(
            title="Tool id.",
            description="Unique identifyer for the tool.",
        ),
    ]
    status_code: Annotated[
        int,
        Field(
            title="Status code",
            description="The status code of the tool submission.",
        ),
    ]


class ChangeVisibilityStatus(BaseModel):
    """Response model for successful disabling of a tool from the database."""

    message: Annotated[
        str,
        Field(
            title="Human readable status.",
        ),
    ]
    status_code: Annotated[
        int,
        Field(
            title="Status code",
            description=("Status code" "0: success, 1: could not change"),
        ),
    ]


class CancelJobStatus(BaseModel):
    """Response model for canceling jobs."""

    message: Annotated[
        str,
        Field(
            description="Human readable status of the cancelation.",
            examples=[
                "Job with id 095be615-a8ad-4c33-8e9c-c7612fbf6c9f was canceled",
                (
                    "Job with id 095be615-a8ad-4c33-8e9c-c7612fbf6c9f has already "
                    "been cancelled"
                ),
            ],
        ),
    ]
    status_code: Annotated[
        int,
        Field(
            description=(
                "Machine readable status of the cancelation"
                "0: success, 1: could not cancel"
            ),
            examples=[0, 1],
        ),
    ]


class SubmitJobStatus(BaseModel):
    """Response model for job successful job submission."""

    uuid: Annotated[
        UUID,
        Field(
            title="Tool id.",
            description="Unique identifyer for the tool.",
        ),
    ]
    status_code: Annotated[
        int,
        Field(
            title="Status code",
            description="The status code of the job submission.",
        ),
    ]
    hostname: Annotated[
        str,
        Field(
            title="Hostname",
            description="The remote host the job is running on.",
        ),
    ]
    time_submitted: Annotated[
        str,
        Field(
            title="Submit time",
            alias="time-submitted",
            description="The time the job was submitted",
        ),
    ]
    batch_mode: Annotated[
        bool,
        Field(
            title="Batch mode",
            alias="batch-mode",
            description=(
                "Boolean indicating wether the job is a batch mode job."
            ),
        ),
    ]
