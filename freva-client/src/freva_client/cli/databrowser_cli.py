"""Command line interface for the freva databrowser.

Search quickly and intuitively for many different climate datasets.
"""

import json
from enum import Enum
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import typer
import typer.models
import xarray as xr

# import pprinr from rich
from rich import print as pprint

from freva_client import databrowser
from freva_client.auth import Auth
from freva_client.utils import exception_handler, logger
from freva_client.utils.auth_utils import requires_authentication

from .cli_utils import parse_cli_args, print_df, version_callback


class UniqKeys(str, Enum):
    """Literal implementation for the cli."""

    file = "file"
    uri = "uri"


class FlavourAction(str, Enum):
    add = "add"
    update = "update"
    delete = "delete"
    list = "list"


class BuiltinFlavours(str, Enum):
    """Literal implementation for the cli."""
    freva = "freva"
    cmip6 = "cmip6"
    cmip5 = "cmip5"
    cordex = "cordex"
    user = "user"


class SelectMethod(str, Enum):
    """Literal implementation for the cli."""

    strict = "strict"
    flexible = "flexible"
    file = "file"

    @staticmethod
    def get_help(context: str) -> str:
        """Generate the help string for time or bbox selection methods.

        Parameters
        ----------
        context: str, default: "time"
            Either "time" or "bbox" to generate appropriate help text.
        """
        examples = {
            "time": ("2000 to 2012", "2010 to 2020"),
            "bbox": ("-10 10 -10 10", "0 5 0 5"),
        }
        descriptions = {
            "time": {
                "unit": "time period",
                "start_end": "start or end period",
                "subset": "time period",
            },
            "bbox": {
                "unit": "spatial extent",
                "start_end": "any part of the extent",
                "subset": "spatial extent",
            },
        }

        context_info = descriptions.get(context, descriptions["time"])
        example = examples.get(context, examples["time"])

        return (
            f"Operator that specifies how the {context_info['unit']} is selected. "
            "Choose from flexible (default), strict or file. "
            "``strict`` returns only those files that have the *entire* "
            f"{context_info['unit']} covered. The {context} search ``{example[0]}`` "
            f"will not select files containing data from {example[1]} with "
            "the ``strict`` method. ``flexible`` will select those files as "
            f"``flexible`` returns those files that have {context_info['start_end']} "
            f"covered. ``file`` will only return files where the entire "
            f"{context_info['subset']} is contained within *one single* file."
        )


databrowser_app = typer.Typer(
    help="Data search related commands", callback=logger.set_cli
)


@databrowser_app.command(
    name="data-overview",
    help="Get an overview over what is available in the databrowser.",
)
@exception_handler
def overview(
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser, if not set (default) "
            "the hostname is read from a config file"
        ),
    ),
) -> None:
    """Get a general overview of the databrowser's search capabilities."""
    print(databrowser.overview(host=host))


@databrowser_app.command(
    name="metadata-search", help="Search databrowser for metadata (facets)."
)
@exception_handler
def metadata_search(
    search_keys: Optional[List[str]] = typer.Argument(
        default=None,
        help="Refine your data search with this `key=value` pair search "
        "parameters. The parameters could be, depending on the DRS standard, "
        "flavour product, project model etc.",
    ),
    facets: Optional[List[str]] = typer.Option(
        None,
        "--facet",
        help=(
            "If you are not sure about the correct search key's you can use"
            " the ``--facet`` flag to search of any matching entries. For "
            "example --facet 'era5' would allow you to search for any entries"
            " containing era5, regardless of project, product etc."
        ),
    ),
    flavour: Optional[str] = typer.Option(
        None,
        "--flavour",
        help=(
            "The Data Reference Syntax (DRS) standard specifying the type "
            "of climate datasets to query."
        ),
    ),
    time_select: SelectMethod = typer.Option(
        "flexible",
        "-ts",
        "--time-select",
        help=SelectMethod.get_help("time"),
    ),
    time: Optional[str] = typer.Option(
        None,
        "-t",
        "--time",
        help=(
            "Special search facet to refine/subset search results by time. "
            "This can be a string representation of a time range or a single "
            "time step. The time steps have to follow ISO-8601. Valid strings "
            "are ``%Y-%m-%dT%H:%M`` to ``%Y-%m-%dT%H:%M`` for time ranges and "
            "``%Y-%m-%dT%H:%M``. **Note**: You don't have to give the full "
            "string format to subset time steps ``%Y``, ``%Y-%m`` etc are also"
            " valid."
        ),
    ),
    bbox: Optional[Tuple[float, float, float, float]] = typer.Option(
        None,
        "-b",
        "--bbox",
        help=(
            "Special search facet to refine/subset search results by spatial "
            "extent. This can be a string representation of a bounding box. "
            "The bounding box has to follow the format ``min_lon max_lon "
            "min_lat,max_lat``. Valid strings are ``-10 10 -10 10`` to "
            "``0 5 0 5``."
        ),
    ),
    bbox_select: SelectMethod = typer.Option(
        "flexible",
        "-bs",
        "--bbox-select",
        help=SelectMethod.get_help("bbox"),
    ),
    extended_search: bool = typer.Option(
        False,
        "-e",
        "--extended-search",
        help="Retrieve information on additional search keys.",
    ),
    multiversion: bool = typer.Option(
        False,
        "--multi-version",
        help="Select all versions and not just the latest version (default).",
    ),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser, if not set (default) "
            "the hostname is read from a config file"
        ),
    ),
    parse_json: bool = typer.Option(
        False, "-j", "--json", help="Parse output in json format."
    ),
    token_file: Optional[Path] = typer.Option(
        None,
        "--token-file",
        "-tf",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
    version: Optional[bool] = typer.Option(
        False,
        "-V",
        "--version",
        help="Show version an exit",
        callback=version_callback,
    ),
) -> None:
    """Search metadata (facets) based on the specified Data Reference Syntax
    (DRS) standard (flavour) and the type of search result (uniq_key), which
    can be either file or uri. Facets represent the metadata categories
    associated with the climate datasets, such as experiment, model,
    institute, and more. This method provides a comprehensive view of the
    available facets and their corresponding counts based on the provided
    search criteria.
    """
    logger.set_verbosity(verbose)
    logger.debug("Search the databrowser")
    if requires_authentication(flavour=flavour, databrowser_url=host):
        Auth(token_file).authenticate(host=host, _cli=True)
    result = databrowser.metadata_search(
        *(facets or []),
        time=time or "",
        time_select=time_select.value,
        bbox=bbox or None,
        bbox_select=bbox_select.value,
        flavour=flavour,
        host=host,
        extended_search=extended_search,
        multiversion=multiversion,
        fail_on_error=False,
        **(parse_cli_args(search_keys or [])),
    )
    if parse_json:
        print(result.to_json())
        return
    print_df(result)


@databrowser_app.command(
    name="data-search", help="Search the databrowser for datasets."
)
@exception_handler
def data_search(
    search_keys: Optional[List[str]] = typer.Argument(
        default=None,
        help="Refine your data search with this `key=value` pair search "
        "parameters. The parameters could be, depending on the DRS standard, "
        "flavour product, project model etc.",
    ),
    facets: Optional[List[str]] = typer.Option(
        None,
        "--facet",
        help=(
            "If you are not sure about the correct search key's you can use"
            " the ``--facet`` flag to search of any matching entries. For "
            "example --facet 'era5' would allow you to search for any entries"
            " containing era5, regardless of project, product etc."
        ),
    ),
    uniq_key: UniqKeys = typer.Option(
        "file",
        "--uniq-key",
        "-u",
        help=(
            "The type of search result, which can be either “file” "
            "or “uri”. This parameter determines whether the search will be "
            "based on file paths or Uniform Resource Identifiers"
        ),
    ),
    flavour: Optional[str] = typer.Option(
        None,
        "--flavour",
        help=(
            "The Data Reference Syntax (DRS) standard specifying the type "
            "of climate datasets to query."
        ),
    ),
    time_select: SelectMethod = typer.Option(
        "flexible",
        "-ts",
        "--time-select",
        help=SelectMethod.get_help("time"),
    ),
    zarr: bool = typer.Option(False, "--zarr", help="Create zarr stream files."),
    token_file: Optional[Path] = typer.Option(
        None,
        "--token-file",
        "-tf",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    time: Optional[str] = typer.Option(
        None,
        "-t",
        "--time",
        help=(
            "Special search facet to refine/subset search results by time. "
            "This can be a string representation of a time range or a single "
            "time step. The time steps have to follow ISO-8601. Valid strings "
            "are ``%Y-%m-%dT%H:%M`` to ``%Y-%m-%dT%H:%M`` for time ranges and "
            "``%Y-%m-%dT%H:%M``. **Note**: You don't have to give the full "
            "string format to subset time steps ``%Y``, ``%Y-%m`` etc are also"
            " valid."
        ),
    ),
    bbox: Optional[Tuple[float, float, float, float]] = typer.Option(
        None,
        "-b",
        "--bbox",
        help=(
            "Special search facet to refine/subset search results by spatial "
            "extent. This can be a string representation of a bounding box. "
            "The bounding box has to follow the format ``min_lon max_lon "
            "min_lat max_lat``. Valid strings are ``-10 10 -10 10`` to "
            "``0 5 0 5``."
        ),
    ),
    bbox_select: SelectMethod = typer.Option(
        "flexible",
        "-bs",
        "--bbox-select",
        help=SelectMethod.get_help("bbox"),
    ),
    parse_json: bool = typer.Option(
        False, "-j", "--json", help="Parse output in json format."
    ),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser, if not set (default) "
            "the hostname is read from a config file"
        ),
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
    multiversion: bool = typer.Option(
        False,
        "--multi-version",
        help="Select all versions and not just the latest version (default).",
    ),
    version: Optional[bool] = typer.Option(
        False,
        "-V",
        "--version",
        help="Show version an exit",
        callback=version_callback,
    ),
) -> None:
    """Search for climate datasets based on the specified Data Reference Syntax
    (DRS) standard (flavour) and the type of search result (uniq_key), which
    can be either “file” or “uri”. The databrowser method provides a flexible
    and efficient way to query datasets matching specific search criteria and
    retrieve a list of data files or locations that meet the query parameters.
    """
    logger.set_verbosity(verbose)
    logger.debug("Search the databrowser")

    result = databrowser(
        *(facets or []),
        time=time or "",
        time_select=time_select.value,
        bbox=bbox or None,
        bbox_select=bbox_select.value,
        flavour=flavour,
        uniq_key=uniq_key.value,
        host=host,
        fail_on_error=False,
        multiversion=multiversion,
        stream_zarr=zarr,
        **(parse_cli_args(search_keys or [])),
    )
    if requires_authentication(flavour=flavour, zarr=zarr, databrowser_url=host):
        Auth(token_file).authenticate(host=host, _cli=True)
    if parse_json:
        print(json.dumps(sorted(result)))
    else:
        for res in result:
            print(res)


@databrowser_app.command(
    name="intake-catalogue", help="Create an intake catalogue from the search."
)
@exception_handler
def intake_catalogue(
    search_keys: Optional[List[str]] = typer.Argument(
        default=None,
        help="Refine your data search with this `key=value` pair search "
        "parameters. The parameters could be, depending on the DRS standard, "
        "flavour product, project model etc.",
    ),
    facets: Optional[List[str]] = typer.Option(
        None,
        "--facet",
        help=(
            "If you are not sure about the correct search key's you can use"
            " the ``--facet`` flag to search of any matching entries. For "
            "example --facet 'era5' would allow you to search for any entries"
            " containing era5, regardless of project, product etc."
        ),
    ),
    uniq_key: UniqKeys = typer.Option(
        "file",
        "--uniq-key",
        "-u",
        help=(
            "The type of search result, which can be either “file” "
            "or “uri”. This parameter determines whether the search will be "
            "based on file paths or Uniform Resource Identifiers"
        ),
    ),
    flavour: Optional[str] = typer.Option(
        None,
        "--flavour",
        help=(
            "The Data Reference Syntax (DRS) standard specifying the type "
            "of climate datasets to query."
        ),
    ),
    time_select: SelectMethod = typer.Option(
        "flexible",
        "-ts",
        "--time-select",
        help=SelectMethod.get_help("time"),
    ),
    time: Optional[str] = typer.Option(
        None,
        "-t",
        "--time",
        help=(
            "Special search facet to refine/subset search results by time. "
            "This can be a string representation of a time range or a single "
            "time step. The time steps have to follow ISO-8601. Valid strings "
            "are ``%Y-%m-%dT%H:%M`` to ``%Y-%m-%dT%H:%M`` for time ranges and "
            "``%Y-%m-%dT%H:%M``. **Note**: You don't have to give the full "
            "string format to subset time steps ``%Y``, ``%Y-%m`` etc are also"
            " valid."
        ),
    ),
    bbox: Optional[Tuple[float, float, float, float]] = typer.Option(
        None,
        "-b",
        "--bbox",
        help=(
            "Special search facet to refine/subset search results by spatial "
            "extent. This can be a string representation of a bounding box. "
            "The bounding box has to follow the format ``min_lon max_lon "
            "min_lat max_lat``. Valid strings are ``-10 10 -10 10`` to "
            "``0 5 0 5``."
        ),
    ),
    bbox_select: SelectMethod = typer.Option(
        "flexible",
        "-bs",
        "--bbox-select",
        help=SelectMethod.get_help("bbox"),
    ),
    zarr: bool = typer.Option(
        False, "--zarr", help="Create zarr stream files, as catalogue targets."
    ),
    token_file: Optional[Path] = typer.Option(
        None,
        "--token-file",
        "-tf",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    filename: Optional[Path] = typer.Option(
        None,
        "-f",
        "--filename",
        help=(
            "Path to the file where the catalogue, should be written to. "
            "if None given (default) the catalogue is parsed to stdout."
        ),
    ),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser, if not set (default) "
            "the hostname is read from a config file"
        ),
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
    multiversion: bool = typer.Option(
        False,
        "--multi-version",
        help="Select all versions and not just the latest version (default).",
    ),
    version: Optional[bool] = typer.Option(
        False,
        "-V",
        "--version",
        help="Show version an exit",
        callback=version_callback,
    ),
) -> None:
    """Create an intake catalogue for climate datasets based on the specified "
    "Data Reference Syntax (DRS) standard (flavour) and the type of search "
    result (uniq_key), which can be either "file" or "uri"."""
    logger.set_verbosity(verbose)
    logger.debug("Search the databrowser")
    result = databrowser(
        *(facets or []),
        time=time or "",
        time_select=time_select.value,
        bbox=bbox or None,
        bbox_select=bbox_select.value,
        flavour=flavour,
        uniq_key=uniq_key.value,
        host=host,
        fail_on_error=False,
        multiversion=multiversion,
        stream_zarr=zarr,
        **(parse_cli_args(search_keys or [])),
    )
    if requires_authentication(flavour=flavour, zarr=zarr, databrowser_url=host):
        Auth(token_file).authenticate(host=host, _cli=True)
    with NamedTemporaryFile(suffix=".json") as temp_f:
        result._create_intake_catalogue_file(str(filename or temp_f.name))
        if not filename:
            print(Path(temp_f.name).read_text())


@databrowser_app.command(
    name="stac-catalogue", help="Create a static STAC catalogue from the search."
)
@exception_handler
def stac_catalogue(
    search_keys: Optional[List[str]] = typer.Argument(
        default=None,
        help="Refine your data search with this `key=value` pair search "
        "parameters. The parameters could be, depending on the DRS standard, "
        "flavour product, project model etc.",
    ),
    facets: Optional[List[str]] = typer.Option(
        None,
        "--facet",
        help=(
            "If you are not sure about the correct search key's you can use"
            " the ``--facet`` flag to search of any matching entries. For "
            "example --facet 'era5' would allow you to search for any entries"
            " containing era5, regardless of project, product etc."
        ),
    ),
    uniq_key: UniqKeys = typer.Option(
        "file",
        "--uniq-key",
        "-u",
        help=(
            "The type of search result, which can be either “file” "
            "or “uri”. This parameter determines whether the search will be "
            "based on file paths or Uniform Resource Identifiers"
        ),
    ),
    flavour: Optional[str] = typer.Option(
        None,
        "--flavour",
        help=(
            "The Data Reference Syntax (DRS) standard specifying the type "
            "of climate datasets to query."
        ),
    ),
    time_select: SelectMethod = typer.Option(
        "flexible",
        "-ts",
        "--time-select",
        help=SelectMethod.get_help("time"),
    ),
    time: Optional[str] = typer.Option(
        None,
        "-t",
        "--time",
        help=(
            "Special search facet to refine/subset search results by time. "
            "This can be a string representation of a time range or a single "
            "time step. The time steps have to follow ISO-8601. Valid strings "
            "are ``%Y-%m-%dT%H:%M`` to ``%Y-%m-%dT%H:%M`` for time ranges and "
            "``%Y-%m-%dT%H:%M``. **Note**: You don't have to give the full "
            "string format to subset time steps ``%Y``, ``%Y-%m`` etc are also"
            " valid."
        ),
    ),
    bbox: Optional[Tuple[float, float, float, float]] = typer.Option(
        None,
        "-b",
        "--bbox",
        help=(
            "Special search facet to refine/subset search results by spatial "
            "extent. This can be a string representation of a bounding box. "
            "The bounding box has to follow the format ``min_lon max_lon "
            "min_lat max_lat``. Valid strings are ``-10 10 -10 10`` to "
            "``0 5 0 5``."
        ),
    ),
    bbox_select: SelectMethod = typer.Option(
        "flexible",
        "-bs",
        "--bbox-select",
        help=SelectMethod.get_help("bbox"),
    ),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser, if not set (default) "
            "the hostname is read from a config file"
        ),
    ),
    token_file: Optional[Path] = typer.Option(
        None,
        "--token-file",
        "-tf",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
    multiversion: bool = typer.Option(
        False,
        "--multi-version",
        help="Select all versions and not just the latest version (default).",
    ),
    version: Optional[bool] = typer.Option(
        False,
        "-V",
        "--version",
        help="Show version an exit",
        callback=version_callback,
    ),
    filename: Optional[Path] = typer.Option(
        None,
        "-o",
        "--filename",
        help=(
            "Path to the file where the static STAC catalogue, "
            "should be written to. If you don't specify or the path "
            "does not exist, the file will be created in the current "
            "working directory. "
        ),
    ),
) -> None:
    """Create a STAC catalogue for climate datasets based on the specified
    Data Reference Syntax (DRS) standard (flavour) and the type of search
    result (uniq_key), which can be either "file" or "uri"."""
    logger.set_verbosity(verbose)
    result = databrowser(
        *(facets or []),
        time=time or "",
        time_select=time_select.value,
        bbox=bbox or None,
        bbox_select=bbox_select.value,
        flavour=flavour,
        uniq_key=uniq_key.value,
        host=host,
        fail_on_error=False,
        multiversion=multiversion,
        stream_zarr=False,
        **(parse_cli_args(search_keys or [])),
    )
    if requires_authentication(flavour=flavour, databrowser_url=host):
        Auth(token_file).authenticate(host=host, _cli=True)
    print(result.stac_catalogue(filename=filename))


@databrowser_app.command(
    name="data-count", help="Count the databrowser search results"
)
@exception_handler
def count_values(
    search_keys: Optional[List[str]] = typer.Argument(
        default=None,
        help="Refine your data search with this `key=value` pair search "
        "parameters. The parameters could be, depending on the DRS standard, "
        "flavour product, project model etc.",
    ),
    facets: Optional[List[str]] = typer.Option(
        None,
        "--facet",
        help=(
            "If you are not sure about the correct search key's you can use"
            " the ``--facet`` flag to search of any matching entries. For "
            "example --facet 'era5' would allow you to search for any entries"
            " containing era5, regardless of project, product etc."
        ),
    ),
    detail: bool = typer.Option(
        False,
        "--detail",
        "-d",
        help=("Separate the count by search facets."),
    ),
    flavour: Optional[str] = typer.Option(
        None,
        "--flavour",
        help=(
            "The Data Reference Syntax (DRS) standard specifying the type "
            "of climate datasets to query."
        ),
    ),
    time_select: SelectMethod = typer.Option(
        "flexible",
        "-ts",
        "--time-select",
        help=SelectMethod.get_help("time"),
    ),
    time: Optional[str] = typer.Option(
        None,
        "-t",
        "--time",
        help=(
            "Special search facet to refine/subset search results by time. "
            "This can be a string representation of a time range or a single "
            "time step. The time steps have to follow ISO-8601. Valid strings "
            "are ``%Y-%m-%dT%H:%M`` to ``%Y-%m-%dT%H:%M`` for time ranges and "
            "``%Y-%m-%dT%H:%M``. **Note**: You don't have to give the full "
            "string format to subset time steps ``%Y``, ``%Y-%m`` etc are also"
            " valid."
        ),
    ),
    bbox: Optional[Tuple[float, float, float, float]] = typer.Option(
        None,
        "-b",
        "--bbox",
        help=(
            "Special search facet to refine/subset search results by spatial "
            "extent. This can be a string representation of a bounding box. "
            "The bounding box has to follow the format ``min_lon max_lon "
            "min_lat max_lat``. Valid strings are ``-10 10 -10 10`` to "
            "``0 5 0 5``."
        ),
    ),
    bbox_select: SelectMethod = typer.Option(
        "flexible",
        "-bs",
        "--bbox-select",
        help=SelectMethod.get_help("bbox"),
    ),
    extended_search: bool = typer.Option(
        False,
        "-e",
        "--extended-search",
        help="Retrieve information on additional search keys.",
    ),
    multiversion: bool = typer.Option(
        False,
        "--multi-version",
        help="Select all versions and not just the latest version (default).",
    ),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser, if not set (default) "
            "the hostname is read from a config file"
        ),
    ),
    parse_json: bool = typer.Option(
        False, "-j", "--json", help="Parse output in json format."
    ),
    token_file: Optional[Path] = typer.Option(
        None,
        "--token-file",
        "-tf",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
    version: Optional[bool] = typer.Option(
        False,
        "-V",
        "--version",
        help="Show version an exit",
        callback=version_callback,
    ),
) -> None:
    """Search metadata (facets) based on the specified Data Reference Syntax
    (DRS) standard (flavour) and the type of search result (uniq_key), which
    can be either file or uri. Facets represent the metadata categories
    associated with the climate datasets, such as experiment, model,
    institute, and more. This method provides a comprehensive view of the
    available facets and their corresponding counts based on the provided
    search criteria.
    """
    logger.set_verbosity(verbose)
    logger.debug("Search the databrowser")
    result: Union[int, Dict[str, Dict[str, int]]] = 0
    search_kws = parse_cli_args(search_keys or [])
    time = cast(str, time or search_kws.pop("time", ""))
    bbox = cast(
        Optional[Tuple[float, float, float, float]],
        bbox or search_kws.pop("bbox", None),
    )
    facets = facets or []
    if requires_authentication(flavour=flavour, databrowser_url=host):
        Auth(token_file).authenticate(host=host, _cli=True)
    if detail:
        result = databrowser.count_values(
            *facets,
            time=time or "",
            time_select=time_select.value,
            bbox=bbox or None,
            bbox_select=bbox_select.value,
            flavour=flavour,
            host=host,
            extended_search=extended_search,
            multiversion=multiversion,
            fail_on_error=False,
            **search_kws,
        )
    else:
        result = len(
            databrowser(
                *facets,
                time=time or "",
                time_select=time_select.value,
                bbox=bbox or None,
                bbox_select=bbox_select.value,
                flavour=flavour,
                host=host,
                multiversion=multiversion,
                fail_on_error=False,
                uniq_key="file",
                stream_zarr=False,
                **search_kws,
            )
        )
    if parse_json:
        print(json.dumps(result))
        return
    if isinstance(result, dict):
        for key, values in result.items():
            counts = []
            for facet, count in values.items():
                counts.append(f"{facet}[{count}]")
            print(f"{key}: {', '.join(counts)}")
    else:
        print(result)


user_data_app = typer.Typer(help="Add or delete user data.")
databrowser_app.add_typer(user_data_app, name="user-data")


@user_data_app.command(name="add", help="Add user data into the databrowser.")
@exception_handler
def user_data_add(
    paths: List[str] = typer.Option(
        ...,
        "--path",
        "-p",
        help="Paths to the user's data to be added.",
    ),
    facets: Optional[List[str]] = typer.Option(
        None,
        "--facet",
        help="Key-value metadata pairs to categorize the user"
        "input data in the format key=value.",
    ),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser. If not set (default), "
            "the hostname is read from a config file."
        ),
    ),
    token_file: Optional[Path] = typer.Option(
        None,
        "--token-file",
        "-tf",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
) -> None:
    """Add user data into the databrowser."""
    logger.set_verbosity(verbose)
    logger.debug("Checking if the user has the right to add data")
    Auth(token_file).authenticate(host=host, _cli=True)

    facet_dict = {}
    if facets:
        for facet in facets:
            if "=" not in facet:
                logger.error(
                    f"Invalid facet format: {facet}. Expected format: key=value."
                )
                raise typer.Exit(code=1)
            key, value = facet.split("=", 1)
            facet_dict[key] = value

    logger.debug(f"Adding user data with paths {paths} and facets {facet_dict}")
    databrowser.userdata(
        action="add",
        userdata_items=cast(List[Union[str, xr.Dataset]], paths),
        metadata=facet_dict,
        host=host,
    )


@user_data_app.command(
    name="delete", help="Delete user data from the databrowser."
)
@exception_handler
def user_data_delete(
    search_keys: List[str] = typer.Option(
        None,
        "--search-key",
        "-s",
        help="Key-value metadata pairs to search and identify user data "
        "for deletion in the format key=value.",
    ),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser. If not set (default), "
            "the hostname is read from a config file."
        ),
    ),
    token_file: Optional[Path] = typer.Option(
        None,
        "--token-file",
        "-tf",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
) -> None:
    """Delete user data from the databrowser."""
    logger.set_verbosity(verbose)
    logger.debug("Checking if the user has the right to delete data")
    Auth(token_file).authenticate(host=host, _cli=True)

    search_key_dict = {}
    if search_keys:
        for search_key in search_keys:
            if "=" not in search_key:
                logger.error(
                    f"Invalid search key format: {search_key}. "
                    "Expected format: key=value."
                )
                raise typer.Exit(code=1)
            key, value = search_key.split("=", 1)
            search_key_dict[key] = value
    databrowser.userdata(action="delete", metadata=search_key_dict, host=host)


flavour_app = typer.Typer(help="Manage custom flavours")
databrowser_app.add_typer(flavour_app, name="flavour")


@flavour_app.command("add", help="Add a new custom flavour.")
@exception_handler
def flavour_add(
    name: str = typer.Argument(..., help="Name of the flavour to add"),
    mapping: List[str] = typer.Option(
        [],
        "--map",
        "-m",
        help="Key-value mappings in the format key=value",
    ),
    global_: bool = typer.Option(
        False,
        "--global",
        help="Make the flavour available to all users (requires admin privileges)",
    ),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser. If not set (default), "
            "the hostname is read from a config file."
        ),
    ),
    token_file: Optional[Path] = typer.Option(
        None,
        "--token-file",
        "-tf",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
) -> None:
    """Add a new custom flavour to the databrowser.
    This command allows user to create custom DRS (Data Reference Syntax)
    flavours that define how search facets are mapped to different data
    standards."""
    logger.set_verbosity(verbose)
    logger.debug(f"Adding flavour '{name}' with mapping {mapping} and global={global_}")
    Auth(token_file).authenticate(host=host, _cli=True)
    mapping_dict = {}
    for map_item in mapping:
        if "=" not in map_item:
            logger.error(
                f"Invalid mapping format: {map_item}. Expected format: key=value."
            )
            raise typer.Exit(code=1)
        key, value = map_item.split("=", 1)
        mapping_dict[key] = value

    if not mapping_dict:
        logger.error("At least one mapping must be provided using --map")
        raise typer.Exit(code=1)

    databrowser.flavour(
        action="add",
        name=name,
        mapping=mapping_dict,
        is_global=global_,
        host=host,
    )


@flavour_app.command("update", help="Update an existing custom flavour.")
@exception_handler
def flavour_update(
    name: str = typer.Argument(..., help="Name of the flavour to update"),
    mapping: List[str] = typer.Option(
        [],
        "--map",
        "-m",
        help="Key-value mappings to update in the format key=value",
    ),
    new_name: Optional[str] = typer.Option(
        None,
        "--new-name",
        help="New name for the flavour (optional)",
    ),
    global_: bool = typer.Option(
        False,
        "--global",
        help="Update global flavour (requires admin privileges)",
    ),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser. If not set (default), "
            "the hostname is read from a config file."
        ),
    ),
    token_file: Optional[Path] = typer.Option(
        None,
        "--token-file",
        "-tf",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
) -> None:
    """Update an existing custom flavour in the databrowser.
    This command allows partial updates to flavour mappings. Only the keys
    provided will be updated; other keys remain unchanged."""
    logger.set_verbosity(verbose)
    logger.debug(f"Updating flavour '{name}' with mapping {mapping}")
    Auth(token_file).authenticate(host=host, _cli=True)

    mapping_dict = {}
    for map_item in mapping:
        if "=" not in map_item:
            logger.error(
                f"Invalid mapping format: {map_item}. Expected format: key=value."
            )
            raise typer.Exit(code=1)
        key, value = map_item.split("=", 1)
        mapping_dict[key] = value

    databrowser.flavour(
        action="update",
        name=name,
        mapping=mapping_dict if mapping_dict else None,
        new_name=new_name,
        is_global=global_,
        host=host,
    )


@flavour_app.command("delete", help="Delete an existing custom flavour.")
@exception_handler
def flavour_delete(
    name: str = typer.Argument(..., help="Name of the flavour to delete"),
    global_: bool = typer.Option(
        False,
        "--global",
        help="Delete global flavour (requires admin privileges)",
    ),
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser. If not set (default), "
            "the hostname is read from a config file."
        ),
    ),
    token_file: Optional[Path] = typer.Option(
        None,
        "--token-file",
        "-tf",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
) -> None:
    """Delete a custom flavour from the databrowser.
    This command removes a custom flavour that was previously created.
    You can only delete flavours that you own, unless you are an admin
    user who can delete global flavours."""
    logger.set_verbosity(verbose)
    logger.debug(f"Deleting flavour '{name}'")
    Auth(token_file).authenticate(host=host, _cli=True)

    databrowser.flavour(
        action="delete",
        name=name,
        is_global=global_,
        host=host,
    )


@flavour_app.command("list", help="List all available custom flavours.")
@exception_handler
def flavour_list(
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser. If not set (default), "
            "the hostname is read from a config file."
        ),
    ),
    parse_json: bool = typer.Option(
        False, "-j", "--json", help="Parse output in json format."
    ),
    token_file: Optional[Path] = typer.Option(
        None,
        "--token-file",
        "-tf",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
) -> None:
    """List all custom flavours available to the current user.
    This command shows both personal flavours created by the user and
    global flavours available to all users."""
    logger.set_verbosity(verbose)
    logger.debug("Listing custom flavours")
    results = cast(Dict[str, Any], databrowser.flavour(action="list", host=host))
    flavours_json = json.dumps(results["flavours"], indent=2)
    if parse_json:
        print(flavours_json)
    else:
        pprint(f"[yellow]{results.get('Note', '')}[/yellow]")
        print("🎨 Available Data Reference Syntax (DRS) Flavours")
        print("=" * 55)
        for flavour in results["flavours"]:
            owner_icon = "🌐" if flavour['owner'] == "global" else "👤"
            print(f"  {owner_icon} {flavour['flavour_name']} (by {flavour['owner']})")
            print(f"    Mapping: {flavour['mapping']}")
            print()
