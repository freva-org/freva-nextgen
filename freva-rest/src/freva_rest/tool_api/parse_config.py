import logging
import tomllib
from pathlib import Path
from typing import Any, Union

from pydantic import ValidationError

from .tool_abstract import ParameterList


def load_toml_to_dict(input_file:Union[str, Path]) -> dict:
    """
    Function to parse a given TOML file to a python dict object.

    Args:
        input_file (Union[str, Path]): Path to the TOML file to be parsed.

    Returns:
        dict: Dictionary containing keys and values of the TOML file.

    Raises:
        AssertionError: Raised when input_file does not exist or does not have a valid TOML file extension.
        IOError: Raised when file cannot be opened in read-only mode. 
        tomllib.TOMLDecodError: Raised when TOML file cannot be parsed by tomllib.
    """
    if isinstance(input_file, str): input_file = Path(input_file)
    valid_toml_file_extensions = [".toml"]
    if not input_file.exists():
        raise FileNotFoundError(f"Input file {input_file} cannot be found. Please make sure it exists and that you have read permissions for it.")
    if not input_file.is_file() or input_file.suffix.lower() not in valid_toml_file_extensions:
        raise ValueError(f"Path {input_file} does not point to a recognised TOML file.")
    try:
        with input_file.open(mode='rb') as fp:
            try:
                parsed_toml_dict = tomllib.load(fp)
            except tomllib.TOMLDecodeError as e:
                logging.error(f"Encountered error when trying to parse TOML file {input_file}: {e}")
                raise
    except IOError as e:
        logging.error(f"Encountered error when trying to open the file {input_file}: {e}")
        raise 

    return parsed_toml_dict

def parse_plugin_config_to_parameter_model(config_dict:dict[str, dict]) -> ParameterList:
    """
    Parses and validates a given plugin configuration dictionary to an input_parameter.ParameterList object.
    Uses the pydantic framework to validate that the parsed dictionary contains valid keys and values.

    Args:
        config_dict (dict[str, dict]): Dictionary defining the plugin configuration. 

    Returns:
        ParameterList: A list of all parameters defined in the configuration.
    
    Raises:
        pydantic.ValidationError: Raised when given dictionary cannot be successfully validated.
    """
    parameter_dict = dict(input_parameters = [])
    assert "input_parameters" in config_dict.keys(), "config does not include required key 'input_parameters'"
    for key, values in config_dict["input_parameters"].items():
        new_entry = dict(name = key, **values)
        parameter_dict["input_parameters"].append(new_entry)
    try:
        validated_model: ParameterList = ParameterList.model_validate(parameter_dict)
    except ValidationError as e:
        logging.error("Encountered an error when validating the configuration: {e}")
        raise

    return validated_model









