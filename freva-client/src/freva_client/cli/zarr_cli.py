"""CLI for zarr utilities."""

import json
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Literal, Optional, TypedDict, cast

import typer

import freva_client.zarr_utils as z_utils
from freva_client.auth import authenticate
from freva_client.utils import exception_handler, logger
from freva_client.utils.types import ZarrOptionsDict

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


class AccessPattern(str, Enum):
    """Literal implementation for the cli."""

    map = "map"
    time_series = "time_series"


class TimeFrequency(str, Enum):
    """Target frequency for temporal reduction."""

    hourly = "hourly"
    three_hourly = "3hourly"
    six_hourly = "6hourly"
    daily = "daily"
    monthly = "monthly"
    seasonal = "seasonal"
    yearly = "yearly"
    decadal = "decadal"


class TimeMethod(str, Enum):
    """How each time group is reduced."""

    mean = "mean"
    sum = "sum"
    min = "min"
    max = "max"
    std = "std"
    var = "var"
    median = "median"
    # `count` shadows `str.count` on this str-mixin enum.  Harmless here --
    # nothing looks members up by name, and typer renders values, not names --
    # but mypy flags the signature clash.
    count = "count"  # type: ignore[assignment]


class ReduceDtype(str, Enum):
    """Output dtype of reduced variables."""

    float32 = "float32"
    float64 = "float64"
    keep = "keep"


#: Help strings for the reduction flags, shared by ``freva-client zarr
#: convert`` and ``freva-client databrowser data-search`` so the two stay in
#: sync.
TIME_FREQ_HELP = (
    "Reduce the time dimension to this frequency server side, e.g. to "
    "stream monthly means instead of daily data."
)
TIME_METHOD_HELP = (
    "How to reduce each time group. Requires --time-freq. Default: mean."
)
CLIMATOLOGY_HELP = (
    "Interpret --time-freq as a climatology (one value per period of the "
    "year, over the whole record) instead of a resampling (one value per "
    "period in the record)."
)
MIN_COVERAGE_HELP = (
    "Mask an output step unless at least this fraction (0-1) of its source "
    "time steps were valid. Requires --time-freq."
)
REDUCE_DTYPE_HELP = (
    "Output dtype of reduced variables. Requires --time-freq. "
    "Default: float32."
)


def reduction_options(
    time_freq: Optional[TimeFrequency],
    time_method: Optional[TimeMethod],
    climatology: bool,
    min_coverage: float,
    dtype: Optional[ReduceDtype],
) -> ZarrOptionsDict:
    """Build the reduction part of the zarr options from cli flags.

    Options that only make sense together with a target frequency are
    dropped when none was given, so that a request without ``--time-freq``
    is byte-identical to one from before reduction existed. That matters:
    the server derives its cache key from the request, so a spurious
    reduction key would needlessly miss the cache.
    """
    if time_freq is None:
        return {}
    options: ZarrOptionsDict = {"time_freq": time_freq.value}
    if time_method is not None:
        options["time_method"] = time_method.value
    if climatology:
        options["climatology"] = True
    if min_coverage:
        options["min_coverage"] = min_coverage
    if dtype is not None:
        options["dtype"] = dtype.value
    return options


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
    access_pattern: AccessPattern = typer.Option(
        "map",
        "--access-pattern",
        help="Optimise the chunk sizes for those access pattern.",
    ),
    map_primary_chunksize: int = typer.Option(
        1,
        "--map-primary-chunksize",
        help="Chunk sizes of the primary dimension.",
    ),
    chunk_size: float = typer.Option(
        16.0,
        "--chunk-size",
        help="Set the target chunk size in megabytes.",
    ),
    reload: bool = typer.Option(
        False,
        "--reload-zarr",
        help="Trigger a zarr data-store reload.",
    ),
    time_freq: Optional[TimeFrequency] = typer.Option(
        None, "--time-freq", help=TIME_FREQ_HELP
    ),
    time_method: Optional[TimeMethod] = typer.Option(
        None, "--time-method", help=TIME_METHOD_HELP
    ),
    climatology: bool = typer.Option(
        False, "--climatology", help=CLIMATOLOGY_HELP
    ),
    min_coverage: float = typer.Option(
        0.0, "--min-coverage", min=0.0, max=1.0, help=MIN_COVERAGE_HELP
    ),
    dtype: Optional[ReduceDtype] = typer.Option(
        None, "--dtype", help=REDUCE_DTYPE_HELP
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

    authenticate(host=host, token_file=token_file)
    zarr_options: ZarrOptionsDict = {
        "public": public,
        "ttl_seconds": ttl_seconds,
        "reload": reload,
        "access_pattern": access_pattern,
        "map_primary_chunksize": map_primary_chunksize,
        "chunk_size": chunk_size,
    }
    zarr_options = {k: v for k, v in zarr_options.items() if v is not None}
    zarr_options.update(
        reduction_options(
            time_freq, time_method, climatology, min_coverage, dtype
        )
    )

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
        headers = authenticate(host=host, token_file=token_file).get("headers")
    results = z_utils.status(
        url,
        host=host,
        headers=headers,
    )
    print(json.dumps(results))
