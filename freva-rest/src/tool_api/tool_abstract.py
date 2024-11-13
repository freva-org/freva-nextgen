from pydantic import BaseModel, Field, model_validator

from typing import Optional, Literal, Union
from typing_extensions import Annotated
from datetime import datetime


from input_parameters import ParameterList


class ToolAbstract(BaseModel):
    """Define the database model for the tool configuration."""

    # basic tool metadata 
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
    description: Annotated[
        str,
        Field(
            required=False,
            description="A more detailed description of the tool",
        ),
    ] = None
    title: Annotated[
        str,
        Field(required=False, description="An optional title of the tool."),
    ] = None
    added: Annotated[
        datetime, Field(required=True, description="When the tool was added.")
    ]
    # input parameters 
    parameters: Annotated[
        ParameterList,
        Field(required=True, description="The input parameters of the tool."),
    ]
    # tool command and binary used to execute tool
    command: Annotated[
        str, Field(required=True, description="The command that is executed.")
    ]
    binary: Annotated[
        str, Field(required=True, description="Binary used to execute tool command.")
    ]
    # output parameters
    output_type: Annotated[
        Literal["plots", "data", "both"], 
        Field(required=True, description="Type of output. Can be either 'plots', 'data' or 'both'")
    ]

    @model_validator(mode="before")
    def _validate_config(self, config: Union[str, dict]) -> dict:
        """
        Run a number of tests on the given config, for example that it contains certain keys 
        for the different categories of the config (such as tool_metadata, input_parameters, runtime_parameters, output_parameters)
        raises an error if checks fail. 
        Idea is that it gets a TOML file string as input, parses it as a dictionary, checks this dictionary
        and returns one that is 'flattened', i.e. where table ids from the TOML file no longer appear.
        """
        raise NotImplementedError
    
    # Methods to interface with the Rest API
    def _post_tool(self) -> str:
        """Post config to Freva Rest API
        If accepted, creates a new db entry for the tool.
        The db entry should probably be tied to the user that posts the config, 
        so the posted tool load in the user's workspace.
        If tool already exists in db, reject.
        """
        raise NotImplementedError
    
    def _put_tool(self) -> str:
        """Update config of a given tool.
        If plugin already exists centrally, overwrite it with the current config.
        This allows users to use their own versions of a given plugin within Freva.
        """
        raise NotImplementedError
    
    def _del_tool(self, tool_id:str) -> str:
        """Delete tool entry in the db, given by its id in the database.
        This should be rejected if a user tries to delete a tool that isn't theirs.
        """
        raise NotImplementedError
    








