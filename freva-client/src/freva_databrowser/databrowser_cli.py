"""Command line interface for the freva databrowser.

Search quickly and intuitively for many different climate datasets.
"""

import json
from enum import Enum
from typing import Dict, List, Literal, Optional, Union, cast

import typer
from freva_databrowser import __version__
from freva_databrowser.query import databrowser
from freva_databrowser.utils import (
    APP_NAME,
    exception_handler,
    logger,
    parse_cli_args,
)
from rich import print as pprint


def version_callback(version: bool) -> None:
    """Print the version and exit."""
    if version:
        pprint(f"{APP_NAME}: {__version__}")
        raise typer.Exit()


app = typer.Typer(
    name=APP_NAME,
    help=__doc__,
    add_completion=False,
    callback=logger.set_cli,
)


class UniqKeys(str, Enum):
    """Literal implementation for the cli."""

    file: str = "file"
    uri: str = "uri"


class Flavours(str, Enum):
    """Literal implementation for the cli."""

    freva: str = "freva"
    cmip6: str = "cmip6"
    cmip5: str = "cmip5"
    cordex: str = "cordex"
    nextgems: str = "nextgems"


class TimeSelect(str, Enum):
    """Literal implementation for the cli."""

    strict: str = "strict"
    flexible: str = "flexible"
    file: str = "file"

    @staticmethod
    def get_help() -> str:
        """Generate the help string."""
        return (
            "Operator that specifies how the time period is selected. "
            "Choose from flexible (default), strict or file. "
            "``strict`` returns only those files that have the *entire* "
            "time period covered. The time search ``2000 to 2012`` will "
            "not select files containing data from 2010 to 2020 with "
            "the ``strict`` method. ``flexible`` will select those files "
            "as  ``flexible`` returns those files that have either start "
            "or end period covered. ``file`` will only return files where "
            "the entire time period is contained within *one single* file."
        )


@app.command(name="overview", help="Get an overview over what is available.")
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


@app.command(name="metadata-search", help="Search for metadata (facets).")
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
    flavour: Flavours = typer.Option(
        "freva",
        "--flavour",
        "-f",
        help=(
            "The Data Reference Syntax (DRS) standard specifying the type "
            "of climate datasets to query."
        ),
    ),
    time_select: TimeSelect = typer.Option(
        "flexible",
        "-ts",
        "--time-select",
        help=TimeSelect.get_help(),
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
    verbose: int = typer.Option(
        0, "-v", help="Increase verbosity", count=True
    ),
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
    result = databrowser.metadata_search(
        *(facets or []),
        time=time or "",
        time_select=cast(
            Literal["file", "flexible", "strict"], time_select.value
        ),
        flavour=cast(
            Literal["freva", "cmip6", "cmip5", "cordex", "nextgems"],
            flavour.value,
        ),
        host=host,
        extended_search=extended_search,
        multiversion=multiversion,
        fail_on_error=False,
        **(parse_cli_args(search_keys or [])),
    )
    if parse_json:
        print(json.dumps(result))
        return
    for key, values in result.items():
        print(f"{key}: {', '.join(values)}")


@app.command(name="data-search", help="Search for datasets.")
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
    flavour: Flavours = typer.Option(
        "freva",
        "--flavour",
        "-f",
        help=(
            "The Data Reference Syntax (DRS) standard specifying the type "
            "of climate datasets to query."
        ),
    ),
    time_select: TimeSelect = typer.Option(
        "flexible",
        "-ts",
        "--time-select",
        help=TimeSelect.get_help(),
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
    verbose: int = typer.Option(
        0, "-v", help="Increase verbosity", count=True
    ),
    multiversion: bool = typer.Option(
        False,
        "--mulit-version",
        help="Select all versions and not just the latest version (default).",
    ),
    version: Optional[bool] = typer.Option(
        False,
        "-V",
        "--version",
        help="Show verion an exit",
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
        time_select=cast(Literal["file", "flexible", "strict"], time_select),
        flavour=cast(
            Literal["freva", "cmip6", "cmip5", "cordex", "nextgems"],
            flavour.value,
        ),
        uniq_key=cast(Literal["uri", "file"], uniq_key.value),
        host=host,
        fail_on_error=False,
        multiversion=multiversion,
        **(parse_cli_args(search_keys or [])),
    )
    if parse_json:
        print(json.dumps(sorted(result)))
    else:
        for res in result:
            print(res)


@app.command(name="count", help="Count the search results")
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
    flavour: Flavours = typer.Option(
        "freva",
        "--flavour",
        "-f",
        help=(
            "The Data Reference Syntax (DRS) standard specifying the type "
            "of climate datasets to query."
        ),
    ),
    time_select: TimeSelect = typer.Option(
        "flexible",
        "-ts",
        "--time-select",
        help=TimeSelect.get_help(),
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
    verbose: int = typer.Option(
        0, "-v", help="Increase verbosity", count=True
    ),
    version: Optional[bool] = typer.Option(
        False,
        "-V",
        "--version",
        help="Show verion an exit",
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
    search_keys = parse_cli_args(search_keys or [])
    time = time or search_keys.pop("time", "")
    facets = facets or []
    if detail:
        result = databrowser.count_values(
            *facets,
            time=time or "",
            time_select=cast(
                Literal["file", "flexible", "strict"], time_select
            ),
            flavour=cast(
                Literal["freva", "cmip6", "cmip5", "cordex", "nextgems"],
                flavour.value,
            ),
            host=host,
            extended_search=extended_search,
            multiversion=multiversion,
            fail_on_error=False,
            **search_keys,
        )
    else:
        result = len(
            databrowser(
                *facets,
                time=time or "",
                time_select=cast(
                    Literal["file", "flexible", "strict"], time_select
                ),
                flavour=cast(
                    Literal["freva", "cmip6", "cmip5", "cordex", "nextgems"],
                    flavour.value,
                ),
                host=host,
                multiversion=multiversion,
                fail_on_error=False,
                uniq_key="file",
                **search_keys,
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
