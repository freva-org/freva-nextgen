from pydantic import ConfigDict, BaseModel, Field
from pydantic.functional_validators import field_validator

from pathlib import Path
from typing import Any,  Dict, Optional, Union, List, Literal
from typing_extensions import Annotated
from datetime import datetime

import json

BaseParamType = Union[str, int, float, bool, datetime, list, tuple]

class BaseParam(BaseModel):
    """
    Base class for all parameter types. Inherits from pydantic.BaseModel.

    All other available parameters inherit from this class. 
    A parameter object created by this class holds the following user-provided information.
    Values taken by the parameter can be any of type BaseParamType, which include str, int, float, bool, datetime, Sequence.
    This base class does not allow for any user-defined attributes, other than those described below.

    By being based on the pydantic.BaseModel, parameter values are automatically validated by the framework. Conversions are performed
    according to the conversion table found under https://docs.pydantic.dev/latest/concepts/conversion_table .

    Args:
        name (str): Name of the parameter. Must be at least on character.
        help (str, optional): Description of the parameter's function. Defaults to None.
        mandatory(bool, optional): Boolean value determining if setting this parameter is required. Defaults to False.
        default(BaseParamType, optional): A default value for the parameter, in case non is set by the user. Defaults to None.
        type(Literal["BaseParam"]): A description of the parameter's type. Defaults to "BaseParam".
    
    Attributes:
        name (str): Name of the parameter.
        help (str): Description of the parameter's function.
        mandatory(bool): Boolean value determining if setting this parameter is required.
        default(BaseParamType): A default value for the parameter, in case non is set by the user.
        type(Literal["BaseParam"]): A description of the parameter's type. 
    """

    model_config = ConfigDict(extra="forbid") # forbid extra fields entered by a user

    name: str = Field(min_length=1)
    help: str = None
    mandatory: bool = False
    default: Optional[BaseParamType] = None
    type: Literal["BaseParam"] = "BaseParam"

class Bool(BaseParam):
    """
    A simple boolean parameter. 

    Args:
        default (bool, optional): The default value of the parameter. Defaults to None.
        type (Literal["Bool"]): A description of the parameter's type. Must be "Bool".
    
    Attributes:
        default (bool, optional): The default value of the parameter. 
        type (Literal["Bool"]): A description of the parameter's type. 
    """
    default: Optional[bool] = None
    type: Literal["Bool"] = "Bool"

class Integer(BaseParam):
    """
    A simple integer parameter.
    Parameter values can be any that

    Args:
        default (int, optional): The default value of the parameter. Defaults to None.
        type (Literal["Integer"]): A description of the parameter's type. Must be "Integer".

    Attributes:
        default (int): The default value of the parameter. 
        type (Literal["Integer"]): A description of the parameter's type. 
    """
    default: Optional[int] = None
    type: Literal["Integer"] = "Integer"

class Float(BaseParam):
    """
    A simple parameter for floating-point numbers.

    Args:
        default (float, optional): The default value of the parameter. Defaults to None.
        type (Literal["Float"]): A description of the parameter's type. Must be "Float".

    Attributes:
        default (float): The default value of the parameter. 
        type (Literal["Float"]): A description of the parameter's type. 
    """
    default: Optional[float] = None
    type: Literal["Float"] = "Float"

class String(BaseParam):
    """
    A simple string parameter.

    Args:
        default (str, optional): The default value of the parameter. Defaults to None.
        type (Literal["String"]): A description of the parameter's type. Must be "String".

    Attributes:
        default (str): The default value of the parameter. 
        type (Literal["String"]): A description of the parameter's type. 
    """
    default: Optional[str] = None
    type: Literal["String"] = "String"

class Date(BaseParam):
    """
    A simple datetime parameter.

    Args:
        default (datetime, optional): The default value of the parameter. Defaults to None.
        type (Literal["Date"]): A description of the parameter's type. Must be "Date".

    Attributes:
        default (datetime): The default value of the parameter. 
        type (Literal["Date"]): A description of the parameter's type. 
    """
    default: Optional[datetime] = None
    type: Literal["Date"] = "Date"

class DataField(BaseParam):
    """
    A parameter for selecting valid values using the databrowser.

    Args:
        facet (str): The databrowser facet used for this parameter. 
        group (int, optional): The group this search facet belongs to. This can be used to group
                               different search facets together, for example for comparing
                               multi model ensembles. Default is 1.
        multiple(bool, optional): Flag indicating whether multiple values can be selected for the selected facet. Default is False.
        predefined_facets(Dict[str, Union[str, Sequence[str]]], optional): A dict containing default values for other search facets. Defaults to None.
        default (str, optional): The default value of the parameter. Defaults to None.
        type (Literal["DataField"]): A description of the parameter's type. Must be "DataField".

    Attributes:
        facet (str): The databrowser facet used for this parameter. 
        group (int, optional): The group this search facet belongs to. This can be used to group
                               different search facets together, for example for comparing
                               multi model ensembles.
        multiple(bool, optional): Flag indicating whether multiple values can be selected for the selected facet.
        predefined_facets(Dict[str, Union[str, List[str]]], optional): A dict containing default values for other search facets. 
        default (str, optional): The default value of the parameter. 
        type (Literal["DataField"]): A description of the parameter's type. 

    Example:

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
    facet: str = Field()
    group: int = 1
    multiple: bool = False
    predefined_facets: Optional[Dict[str, Union[str, List[str]]]] = None
    default: str = None
    type: Literal["DataField"] = "DataField"

class Range(BaseParam):
    """
    A simple Range parameter for iterable, indexable sequences such as lists and tuples.

    Args:
        default (List[BaseParamType], optional): The default value of the parameter. Defaults to None.
        type (Literal["Range"]): A description of the parameter's type. Must be "Range".

    Attributes:
        default (List[BaseParamType]): The default value of the parameter. 
        type (Literal["Range"]): A description of the parameter's type. 
    """
    default: Optional[List[BaseParamType]] = None
    type: Literal["Range"] = "Range"

class File(BaseParam):
    """
    A simple parameter to define a file stored on the system.

    Args:
        default (Path, optional): The default value of the parameter. Defaults to None.
        type (Literal["File"]): A description of the parameter's type. Must be "File".

    Attributes:
        default (Path): The default value of the parameter. 
        type (Literal["File"]): A description of the parameter's type. 
    """
    default: Optional[Path] = None
    type: Literal["File"] = "File"

    
    @field_validator("default")
    @classmethod
    def assert_is_file(cls, path: Path):
        if path and not path.is_file():
            raise ValueError("Default value must be a file!")
        return path

ParameterType = Annotated[
    Union[Bool, String, Float, Integer, Date, DataField, Range, File, Range],
      Field(discriminator="type")
]
class ParameterList(BaseModel):
    """
    A list holding all parameters for a given Freva plugin.

    The parameters are all validated using the pydantic framework and the list can be parsed to a JSON string
    using ParameterList.model_dump_json().

    Args:
        input_parameters (List[ParameterType]): A list containing valid plugin input parameters. 
                                                Valid parameters are Bool, String, Integer, Date, DataField, Range, File, Range.

    Attributes:
        input_parameters(List[ParameterType]): A list containing valid parameters. 
    """
    input_parameters : List[ParameterType]
        
            

    

            






        





