"""Definitions of Tool parameters and types."""

from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.functional_validators import field_validator

BaseParamType = Union[
    str,
    int,
    float,
    bool,
    datetime,
    Path,
    List[Union[str, int, float, bool, datetime, Path]],
]


class BaseParam(BaseModel):
    """
    Base class for all parameter types. Inherits from pydantic.BaseModel.

    All other available parameters inherit from this class.
    A parameter object created by this class holds the following user-provided
    information. Values taken by the parameter can be any of type
    BaseParamType, which include str, int, float, bool, datetime, Sequence.
    This base class does not allow for any user-defined attributes, other than
    those described below.

    By being based on the pydantic.BaseModel, parameter values are
    automatically validated by the framework. Conversions are performed
    according to the conversion table found under
    https://docs.pydantic.dev/latest/concepts/conversion_table .

    Parameters
    ----------
    name (str):
        Name of the parameter. Must be at least on character.
    title (str):
        Title under which the parameter will appear. If none is given,
        defaults to name.
    help (str, optional):
        Description of the parameter's function. Defaults to None.
    mandatory(bool, optional):
        Boolean value determining if setting this parameter is required.
        Defaults to False.
    default(BaseParamType, optional):
        A default value for the parameter, in case non is set by the user.
        Defaults to None.
    type(Literal["BaseParam"]):
        A description of the parameter's type. Defaults to "BaseParam".

    Attributes
    ----------
    name (str):
        Name of the parameter.
    help (str):
        Description of the parameter's function.
    mandatory(bool):
        Boolean value determining if setting this parameter is required.
    default(BaseParamType):
        A default value for the parameter, in case non is set by the user.
    type(Literal["BaseParam"]):
        A description of the parameter's type.
    """

    model_config = ConfigDict(
        extra="forbid"
    )  # forbid extra fields entered by a user

    name: Annotated[
        str, Field(min_length=1, description="Name of the parameter.")
    ]
    title: Annotated[
        Optional[str], Field(description="Title of parameter.")
    ] = None
    help: str = Field(description="Description of parameter's function.")
    mandatory: Annotated[
        bool,
        Field(
            description=(
                "Boolean value determining if parameter value has to be set."
            ),
        ),
    ] = False
    default: Annotated[
        Optional[Union[BaseParamType, List[BaseParamType]]],
        Field(mandatory=False, description="Default value of parameter."),
    ] = None

    @field_validator("title", mode="after")
    def set_default_title(cls, title: Optional[str]) -> str:
        """Assign the title."""
        if title is None:
            title = cls.name
        return title


class Bool(BaseParam):
    """
    A simple boolean parameter.

    Parameters
    ----------
    default (bool, optional):
        The default value of the parameter. Defaults to None.
    type (Literal["Bool"]):
        A description of the parameter's type. Must be "Bool".

    Attributes
    ----------
    default (bool, optional):
        The default value of the parameter.
    type (Literal["Bool"]):
        A description of the parameter's type.
    """

    type: Literal["bool"]
    default: Optional[bool] = None


class Integer(BaseParam):
    """
    A simple integer parameter.
    Parameter values can be any that

    Parameters
    ----------
    default (int, optional):
        The default value of the parameter. Defaults to None.
    type (Literal["Integer"]):
        A description of the parameter's type. Must be "Integer".

    Attributes
    ----------
    default (int):
        The default value of the parameter.
    type (Literal["Integer"]):
        A description of the parameter's type.
    """

    type: Literal["integer"]
    default: Optional[int] = None


class Float(BaseParam):
    """
    A simple parameter for floating-point numbers.

    Parameters
    ----------
    default (float, optional):
        The default value of the parameter. Defaults to None.
    type (Literal["Float"]):
        A description of the parameter's type. Must be "Float".

    Attributes
    ----------
    default (float):
        The default value of the parameter.
    type (Literal["Float"]):
        A description of the parameter's type.
    """

    type: Literal["float"]
    default: Optional[float] = None


class String(BaseParam):
    """
    A simple string parameter.

    Parameters
    ----------
    default (str, optional):
        The default value of the parameter. Defaults to None.
    type (Literal["String"]):
        A description of the parameter's type. Must be "String".

    Attributes
    ----------
    default (str):
        The default value of the parameter.
    type (Literal["String"]):
        A description of the parameter's type.
    """

    type: Literal["string"]
    default: Optional[str] = None


class Date(BaseParam):
    """
    A simple datetime parameter.

    Parameters
    ----------
    default (datetime, optional)
        The default value of the parameter. Defaults to None.
    type (Literal["Date"])
        A description of the parameter's type. Must be "Date".

    Attributes
    ----------
    default (datetime):
        The default value of the parameter.
    type (Literal["Date"]):
        A description of the parameter's type.
    """

    type: Literal["datetime"]
    default: Optional[datetime] = None


class DataField(BaseParam):
    """
    A parameter for selecting valid values using the databrowser.

    Parameters
    ----------
    facet (str):
        The databrowser facet used for this parameter.
    group (int, optional):
        The group this search facet belongs to. This can be used to group
        different search facets together, for example for comparing
        multi model ensembles. Default is 1.
    multiple(bool, optional):
        Flag indicating whether multiple values can be selected for the
        selected facet. Default is False.
    predefined_facets(Dict[str, Union[str, Sequence[str]]], optional):
        A dict containing default values for other search facets.
        Defaults to None.
    default (str, optional):
        The default value of the parameter. Defaults to None.
    type (Literal["DataField"]):
        A description of the parameter's type. Must be "DataField".

    Attributes
    ----------
    facet (str):
        The databrowser facet used for this parameter.
    group (int, optional):
        The group this search facet belongs to. This can be used to group
        different search facets together, for example for comparing
        multi model ensembles.
    multiple(bool, optional):
        Flag indicating whether multiple values can be selected for the
        selected facet.
    predefined_facets(Dict[str, Union[str, List[str]]], optional):
        A dict containing default values for other search facets.
    default (str, optional):
        The default value of the parameter.
    type (Literal["DataField"]):
        A description of the parameter's type.

    Example
    -------

    .. code-block:: python

        from input_parameters import DataField

        datafield = DataField(
                        name="variable_name",
                        default="tas",
                        facet="variable",
                        max_items=1,
                        group=2,
                        predefined_facet={"time_frequency":["1hr"]},
                        help="Select the variable name"
        )
    """

    type: Literal["databrowser"]
    facet: str = Field()
    group: int = 1
    multiple: bool = False
    predefined_facets: Optional[Dict[str, Union[str, List[str]]]] = None
    default: Optional[str] = None


class Range(BaseParam):
    """
    A simple Range parameter for iterable, indexable sequences such as lists and tuples.

    Parameters
    ----------
    default (List[BaseParamType], optional):
        The default value of the parameter. Defaults to None.
    type (Literal["Range"]):
        A description of the parameter's type. Must be "Range".

    Attributes
    ----------
    default (List[BaseParamType]):
        The default value of the parameter.
    type (Literal["Range"]):
        A description of the parameter's type.
    """

    type: Literal["range"]
    default: Optional[List[BaseParamType]] = None


class File(BaseParam):
    """
    A simple parameter to define a file stored on the system.

    Parameters
    ----------
    default (Path, optional):
        The default value of the parameter. Defaults to None.
    type (Literal["File"]):
        A description of the parameter's type. Must be "File".

    Attributes
    ----------
    default (Path):
        The default value of the parameter.
    type (Literal["File"]):
        A description of the parameter's type.
    """

    type: Literal["path"]
    default: Optional[Path] = None

    @field_validator("default")
    @classmethod
    def assert_is_file(cls, path: Union[str, Path]) -> Path:
        return Path(path)


ParameterType = Annotated[
    Union[Bool, String, Float, Integer, Date, DataField, Range, File, Range],
    Field(discriminator="type"),
]


class ToolAbstract(BaseModel):
    """Define the database model for the tool configuration."""

    # basic tool metadata
    name: Annotated[str, Field(description="Name of the tool")]
    author: Annotated[str, Field(description="Tool Author(s)")]
    version: Annotated[str, Field(description="Tool version")]
    summary: Annotated[
        str,
        Field(
            description="Short description of what the tool is supposed to do.",
        ),
    ]
    description: Annotated[
        Optional[str],
        Field(
            description="A more detailed description of the tool",
        ),
    ] = None
    title: Annotated[
        Optional[str],
        Field(description="An optional title of the tool."),
    ] = None
    added: Annotated[datetime, Field(description="When the tool was added.")]
    # input parameters
    parameters: Annotated[
        Optional[List[ParameterType]],
        Field(description="The input parameters of the tool."),
    ] = None
    # tool command and binary used to execute tool
    command: Annotated[str, Field(description="The command that is executed.")]
    binary: Annotated[
        str,
        Field(description="Binary used to execute tool command."),
    ]
    # output parameters
    output_type: Annotated[
        Literal["plots", "data", "both"],
        Field(
            description="Type of output. Can be either 'plots', 'data' or 'both'",
        ),
    ]

    @model_validator(mode="before")
    def _validate_config(cls, config: str) -> dict[str, Any]:
        """
        Run a number of tests on the given config, for example that it contains
        certain keys for the different categories of the config (such as
        tool_metadata, input_parameters, runtime_parameters, output_parameters)
        raises an error if checks fail. The idea is that it gets a TOML file
        string as input, parses it as a dictionary, checks this dictionary
        and returns one that is 'flattened', i.e. where table ids from the
        TOML file no longer appear.
        """
        raise NotImplementedError
