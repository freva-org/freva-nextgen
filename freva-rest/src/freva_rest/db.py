"""Database definitions for the freva-rest api."""

import enum
from datetime import datetime
from typing import Annotated, Optional, Union

from pydantic import BaseModel, Field, field_serializer, field_validator

InputType = Union[str, int, float, None]


@enum.unique
class ToolState(enum.IntEnum):
    """Define the states a tool could be in."""

    UNKOWN = -1  # The tool seems to be lost.
    SUBMITTED = 0
    IN_PROGRESS = 1
    FINISHED = 2
    CANCEL = 3  # The tool should will be cancled
    CANCELED = 4
    FAILED = 5

    @classmethod
    def get_status_from_value(cls, value: int) -> "ToolState":
        """Get the status name from status value."""
        for state in cls:
            if value == state.value:
                return state
        raise ValueError(f"Unknown ToolState value: {value}")


class ToolParameter(BaseModel):
    """Define a tool parameter."""

    name: Annotated[str, Field(required=True, description="Parameter name")]
    var_type: Annotated[
        str, Field(required=True, description="The type of parameter.")
    ]
    help: Annotated[
        Optional[str], Field(required=False, description="Parameter help")
    ] = None
    required: Annotated[
        bool, Field(required=False, description="Is the parameter mandatory")
    ] = False
    default: Annotated[
        Optional[Union[InputType, list[InputType]]],
        Field(required=False, description="The default value"),
    ] = None
    value: Annotated[
        Optional[Union[InputType, list[InputType]]],
        Field(required=False, description="The set value"),
    ] = None


class ToolConfig(BaseModel):
    """Define the database model for the tool configuration."""

    name: Annotated[str, Field(required=True, description="Name of the tool")]
    author: Annotated[str, Field(required=True, description="Tool Author(s)")]
    version: Annotated[str, Field(required=True, description="Tool version")]
    summary: Annotated[
        str,
        Field(
            required=True,
            description="Short description of what the tool is supposed to do.",
        ),
    ]
    added: Annotated[
        datetime, Field(required=True, description="When the tool was added.")
    ]
    parameters: Annotated[
        list[ToolParameter],
        Field(required=True, description="The input parameters of the tool."),
    ]
    command: Annotated[
        str, Field(required=True, description="The command that is executed.")
    ]
    description: Annotated[
        Optional[str],
        Field(
            required=False,
            description="A more detailed description of the tool",
        ),
    ] = None
    title: Annotated[
        Optional[str],
        Field(required=False, description="An optional title of the tool."),
    ] = None


class RunningJobDB(BaseModel):
    """Define the database model for tools that are currently running."""

    uuid: Annotated[
        str,
        Field(
            title="UUID",
            required=True,
            description="Unique id to identify the tool jobs.",
        ),
    ]
    tool_name: Annotated[
        str, Field(required=True, description="Name of the tool")
    ]
    tool_version: Annotated[
        str, Field(required=True, description="The version of the tool")
    ]
    username: Annotated[str, Field(required=True, title="User name")]
    log_path: Annotated[
        str,
        Field(
            required=True,
            description="Path that containes to logs of the tool",
        ),
    ]
    status: Annotated[
        ToolState,
        Field(
            required=True,
            description="State representation of the running tool.",
        ),
    ]
    input_fields: Annotated[
        dict[str, Union[InputType, list[InputType]]],
        Field(required=True, description="The parsed tool input"),
    ]
    last_seen: Annotated[
        Optional[datetime],
        Field(required=False, description="Tool was last seen at"),
    ] = None

    @field_serializer("status")
    def serialize_state(self, value: ToolState) -> int:
        """Serialise the ToolState object."""
        return value.value


class FinishedJobDB(RunningJobDB):
    """Define the database model for tools that are fininished."""

    data_output: Annotated[
        list[str],
        Field(
            required=True,
            description="The tool output that is data",
        ),
    ]
    vis_output: Annotated[
        list[str],
        Field(required=True, description="The tool output that is visuals."),
    ]
