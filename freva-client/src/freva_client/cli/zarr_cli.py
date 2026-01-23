"""CLI for zarr utilities."""

import json
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Literal, Optional, TypedDict, cast

import typer

import freva_client.zarr_utils as z_utils
from freva_client.auth import Auth
from freva_client.utils import exception_handler, logger

from .cli_utils import version_callback


class _AggOpts(TypedDict, total=False):

    join: Literal["outer", "inner", "exact", "left", "right"]
    compat: Literal["no_conflicts", "equals", "override"]
    data_vars: Literal["minimal", "different", "all"]
    coords: Literal["minimal", "different", "all"]
    dim: Optional[str]
    group_by: Optional[str]


class Aggregate(str, Enum):
    """Literal implementation for the cli."""

    auto = "auto"
    merge = "merge"
    concat = "concat"


class AggregationJoin(str, Enum):
    """Literal implementation for the cli."""

    outer = "outer"
    inner = "inner"
    exact = "exact"
    left = "left"
    right = "right"


class AggregationCompat(str, Enum):
    """Literal implementation for the cli."""

    no_conflicts = "no_conflicts"
    equals = "equals"
    override = "override"


class AggregationCombine(str, Enum):
    """Literal implementation for the cli."""

    minimal = "minimal"
    different = "different"
    all = "all"


@dataclass
class AggregationOption:
    """Helper to make mypy happy about the aggregation options."""

    join: Optional[AggregationJoin] = None
    compat: Optional[AggregationCompat] = None
    data_vars: Optional[AggregationCombine] = None
    coords: Optional[AggregationCombine] = None
    dim: Optional[str] = None
    group_by: Optional[str] = None

    def to_dict(self) -> _AggOpts:
        """Drop all None options."""
        _dict = {}
        for k, v in asdict(self).items():
            _dict[k] = getattr(v, "value", v)
        return cast(_AggOpts, _dict)


zarr_app = typer.Typer(help="Zarr utility cli", callback=logger.set_cli)


@zarr_app.command(
    "convert",
    help="Convert different data sets (files) to a http ready zarr store.",
)
@exception_handler
def zarr_convert(
    paths: List[str] = typer.Argument(
        help="Paths to data files that should be aggregated."
    ),
    public: bool = typer.Option(
        False, "--public", help="Make any zarr url public"
    ),
    ttl_seconds: float = typer.Option(
        86400.0,
        "--ttl-seconds",
        help="Set the expiry time of any public zarr urls",
    ),
    aggregate: Optional[Aggregate] = typer.Option(
        None, "--aggregate", help="How aggregation should be realised (if any)"
    ),
    join: Optional[AggregationJoin] = typer.Option(
        None,
        "--join",
        help="How different indexes should be combined for aggregation.",
    ),
    compat: Optional[AggregationCompat] = typer.Option(
        None,
        "--compat",
        help="How to compare non-concatenated variables for aggregation.",
    ),
    data_vars: Optional[AggregationCombine] = typer.Option(
        None,
        "--data-vars",
        help="How to combine data variables for aggregation.",
    ),
    coords: Optional[AggregationCombine] = typer.Option(
        None,
        "--coords",
        help="How to combine coords for aggregation.",
    ),
    dim: Optional[str] = typer.Option(
        None,
        "--dim",
        help="Name of the dimension to concatenate along for aggregation.",
    ),
    group_by: Optional[str] = typer.Option(
        None,
        "--group-by",
        help="If set, forces grouping by a signature key for aggregation.",
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
    version: Optional[bool] = typer.Option(
        False,
        "-V",
        "--version",
        help="Show version an exit",
        callback=version_callback,
    ),
) -> None:
    """Aggregate different data sets (files) into a common zarr store."""
    logger.set_verbosity(verbose)
    logger.debug("Aggregating data files: %s", " ".join(paths))
    aggregation_options = AggregationOption(
        join=AggregationJoin[join] if join else None,
        compat=AggregationCompat[compat] if compat else None,
        data_vars=AggregationCombine[data_vars] if data_vars else None,
        coords=AggregationCombine[coords] if coords else None,
        dim=dim,
        group_by=group_by,
    )

    Auth(token_file).authenticate(host=host, _cli=True)
    zarr_options = {
        "public": public,
        "ttl_seconds": ttl_seconds,
    }

    results = z_utils.convert(
        *paths,
        aggregate=Aggregate[aggregate].value if aggregate else None,
        host=host,
        zarr_options=zarr_options,
        **aggregation_options.to_dict(),
    )
    results.sort()
    if parse_json:
        print(json.dumps(results))
    else:
        for r in results:
            print(r)


@zarr_app.command(
    "status",
    help="Get the status of a pre signed zarr store",
)
@exception_handler
def zarr_status(
    url: str = typer.Argument(help="Url of the zarr store to check."),
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
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser, if not set (default) "
            "the hostname is read from a config file"
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
    """Aggregate different data sets (files) into a common zarr store."""
    logger.set_verbosity(verbose)
    logger.debug("Checking status of: %s", url)
    headers: Optional[Dict[str, str]] = None
    if token_file:
        headers = (
            Auth(token_file).authenticate(host=host, _cli=True).get("headers")
        )
    results = z_utils.status(
        url,
        host=host,
        headers=headers,
    )
    print(json.dumps(results))
