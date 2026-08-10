"""Dimension reduction of xarray datasets.

This module is the reduction counterpart to :mod:`aggregator`.  Where the
aggregator combines several inputs into one dataset, the reducer collapses
dimensions of that dataset *before* it is chunked and served as Zarr, so
that clients transfer far fewer bytes.

Design
------
The module is split into a *pure planning* stage and a *pure application*
stage, mirroring the rest of the worker:

* :func:`plan_reduction` resolves user-supplied options against a concrete
  dataset and returns a fully determined :class:`ReductionPlan`.  It performs
  every validation that can be done without touching data.
* :func:`apply_reduction` executes a plan.  It never inspects user options
  and never raises for user error -- everything user-facing is caught during
  planning.

Neither function does I/O or touches Redis.  :func:`reduce_datasets` is the
only convenience wrapper and simply maps over the group mapping produced by
:class:`~data_portal_worker.aggregator.DatasetAggregator`.

CF decoding
-----------
The loading backend deliberately opens datasets with ``decode_cf=False`` so
that raw bytes can be passed through to the Zarr client untouched.  That is
not viable for reductions: ``resample``/``groupby`` require a real datetime
index, and averaging packed integers with an in-band ``_FillValue`` produces
nonsense.  Reduction therefore decodes the dataset first.  This is a
deliberate, documented deviation -- a reduced store is always CF-decoded and
floating point, while a pass-through store is not.

Spatial reduction
-----------------
Spatial reduction is *specified* here (see :class:`SpaceReduction` and the
``space``/``space_dims``/``weighting`` options) so that the wire format, the
cache-token identity and the sanitiser contract are already final, but it is
not implemented yet: planning raises :class:`ReductionError` if it is
requested.  Implementing it means filling in :func:`_resolve_weights` and
the corresponding branch of :func:`apply_reduction`; nothing else in the
pipeline needs to change.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    cast,
)

import numpy as np
import xarray as xr

from .utils import data_logger

__all__ = [
    "FREQUENCIES",
    "METHODS",
    "WEIGHTINGS",
    "ReductionError",
    "ReductionOptions",
    "ReductionPlan",
    "SpaceReduction",
    "TimeReduction",
    "apply_reduction",
    "plan_reduction",
    "reduce_datasets",
]


# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

#: Closed frequency vocabulary mapped to pandas offset aliases.  A closed
#: vocabulary (rather than free-form offset strings) keeps the sanitiser
#: trivial, keeps cache tokens canonical, and stays inside the DRS frequency
#: vocabulary users already know from the databrowser.
FREQUENCY_OFFSETS: Dict[str, str] = {
    "hourly": "h",
    "3hourly": "3h",
    "6hourly": "6h",
    "daily": "D",
    "monthly": "MS",
    "seasonal": "QS-DEC",
    "yearly": "YS",
    "decadal": "10YS",
}

#: Frequencies that also make sense as a climatology, mapped to the xarray
#: ``groupby`` key.  ``yearly``/``decadal`` climatologies are meaningless and
#: are rejected during planning.
CLIMATOLOGY_GROUPS: Dict[str, str] = {
    "hourly": "time.hour",
    "daily": "time.dayofyear",
    "monthly": "time.month",
    "seasonal": "time.season",
}

FREQUENCIES: Tuple[str, ...] = tuple(FREQUENCY_OFFSETS)

#: Nominal duration of each frequency in seconds, used only to detect a
#: request that would *up*-sample.  Mean lengths, not minimums: calendar
#: months run 28-31 days, so exact comparison is meaningless here.
_FREQUENCY_SECONDS: Dict[str, float] = {
    "hourly": 3600.0,
    "3hourly": 3.0 * 3600.0,
    "6hourly": 6.0 * 3600.0,
    "daily": 86400.0,
    "monthly": 30.44 * 86400.0,
    "seasonal": 91.31 * 86400.0,
    "yearly": 365.25 * 86400.0,
    "decadal": 3652.5 * 86400.0,
}

#: How much longer than the input the output axis may be before the request
#: stops counting as a reduction.  Slack absorbs calendar irregularity --
#: resampling monthly data to ``monthly`` can land one step either side.
_INFLATION_TOLERANCE = 1.01

#: CF time units mapped to seconds, for reading an undecoded time axis.
_UNIT_SECONDS: Dict[str, float] = {
    "second": 1.0,
    "seconds": 1.0,
    "sec": 1.0,
    "s": 1.0,
    "minute": 60.0,
    "minutes": 60.0,
    "min": 60.0,
    "hour": 3600.0,
    "hours": 3600.0,
    "hr": 3600.0,
    "h": 3600.0,
    "day": 86400.0,
    "days": 86400.0,
    "d": 86400.0,
}

#: Reduction methods.  These are the names of the aggregation methods on
#: xarray's ``Resample``/``GroupBy``/``Dataset`` objects.
METHODS: Tuple[str, ...] = (
    "mean",
    "sum",
    "min",
    "max",
    "std",
    "var",
    "median",
    "count",
)

#: Area-weighting strategies for spatial reduction (reserved).
WEIGHTINGS: Tuple[str, ...] = ("auto", "cell_area", "cos_lat", "none")

#: Output dtype policy.
DTYPES: Tuple[str, ...] = ("float32", "float64", "keep")

#: Candidate names for the time dimension, in order of preference.
_TIME_DIM_CANDIDATES: Tuple[str, ...] = ("time", "valid_time", "t")

#: Candidate names for cell-area variables, used by spatial reduction.
_CELL_AREA_CANDIDATES: Tuple[str, ...] = (
    "areacella",
    "areacello",
    "cell_area",
    "area",
)

#: Dimension names that indicate an equal-area (HEALPix-like) grid, where an
#: unweighted spatial mean is already correct.
_EQUAL_AREA_DIMS: Tuple[str, ...] = ("cell", "cells", "values", "ncells")


class ReductionError(RuntimeError):
    """Raised when a reduction cannot be planned or applied.

    Mirrors :class:`~data_portal_worker.aggregator.AggregationError`: the
    ``reason`` is safe to surface to users and ends up in the job status
    that the REST API serves.

    Parameters
    ----------
    reason:
        Human-readable explanation, safe to show to users.
    details:
        Optional structured metadata to help debugging.
    """

    def __init__(
        self, reason: str, details: Optional[Mapping[str, Any]] = None
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.details = dict(details or {})

    def __str__(self) -> str:
        details = "\n".join(f"{k}: {v}" for (k, v) in self.details.items())
        return f"{self.reason}:\n\n{details}" if details else self.reason


class ReductionOptions(TypedDict, total=False):
    """User-supplied reduction options, as they arrive over the broker.

    Every key is optional; an empty mapping means "no reduction".  Values are
    JSON-native so that the whole mapping can be embedded verbatim in the
    cache token.
    """

    time_freq: Optional[str]
    time_method: Optional[str]
    climatology: Optional[bool]
    min_coverage: Optional[float]
    dtype: Optional[str]
    # Reserved for spatial reduction -- accepted by the sanitiser so the wire
    # format is stable, rejected during planning until implemented.
    space: Optional[str]
    space_dims: Optional[List[str]]
    weighting: Optional[str]


@dataclass(frozen=True)
class TimeReduction:
    """A fully resolved temporal reduction.

    Parameters
    ----------
    dim:
        Name of the time dimension in the dataset.
    freq:
        Frequency name from :data:`FREQUENCIES`.
    method:
        Reduction method from :data:`METHODS`.
    offset:
        Pandas offset alias used for ``resample``.  ``None`` for a
        climatology.
    group_key:
        xarray ``groupby`` key used for a climatology, e.g. ``"time.month"``.
        ``None`` for a resample.
    min_coverage:
        Minimum fraction of valid (non-missing) source steps a group must
        contain for its result to be kept; groups below the threshold are
        masked to NaN.  ``0.0`` disables the check.
    """

    dim: str
    freq: str
    method: str
    offset: Optional[str] = None
    group_key: Optional[str] = None
    min_coverage: float = 0.0

    @property
    def is_climatology(self) -> bool:
        """Whether this is a ``groupby`` climatology rather than a resample."""
        return self.group_key is not None

    @property
    def cell_method(self) -> str:
        """CF ``cell_methods`` fragment describing this reduction."""
        if self.is_climatology:
            return f"{self.dim}: {self.method} over {self.freq}"
        return f"{self.dim}: {self.method} ({self.freq})"


@dataclass(frozen=True)
class SpaceReduction:
    """A fully resolved spatial reduction.

    Reserved.  Planning currently rejects any request that would produce
    one of these; the dataclass exists so that :class:`ReductionPlan`, the
    wire format and the cache token do not have to change when spatial
    reduction is implemented.

    Parameters
    ----------
    method:
        Reduction method from :data:`METHODS`.
    dims:
        Dimensions to collapse.
    weighting:
        Resolved weighting strategy (never ``"auto"`` -- planning resolves
        it against the grid).
    weight_var:
        Name of the cell-area variable when ``weighting == "cell_area"``.
    """

    method: str
    dims: Tuple[str, ...]
    weighting: str
    weight_var: Optional[str] = None

    @property
    def cell_method(self) -> str:
        """CF ``cell_methods`` fragment describing this reduction."""
        return f"area: {self.method}"


@dataclass(frozen=True)
class ReductionPlan:
    """A fully resolved reduction, ready to be applied.

    Parameters
    ----------
    time:
        Temporal reduction, if any.
    space:
        Spatial reduction, if any (reserved).
    dtype:
        Output dtype for floating-point data variables, or ``None`` to keep
        whatever decoding produced.
    """

    time: Optional[TimeReduction] = None
    space: Optional[SpaceReduction] = None
    dtype: Optional[str] = None

    @property
    def is_noop(self) -> bool:
        """Whether applying this plan would leave the dataset unchanged."""
        return self.time is None and self.space is None

    @property
    def removes_spatial_dims(self) -> bool:
        """Whether the result loses its spatial dimensions.

        The chunk planner uses this: a dataset whose spatial dimensions are
        gone is a plain time series, and chunking it for ``map`` access
        produces one tiny chunk per time step.
        """
        return self.space is not None

    def describe(self) -> str:
        """Short human-readable summary, used for logging."""
        parts: List[str] = []
        if self.time is not None:
            kind = "climatology" if self.time.is_climatology else "resample"
            parts.append(f"{kind} {self.time.freq}/{self.time.method}")
        if self.space is not None:
            parts.append(
                f"space {self.space.method} over "
                f"{','.join(self.space.dims)} ({self.space.weighting})"
            )
        if self.dtype is not None:
            parts.append(f"cast {self.dtype}")
        return "; ".join(parts) or "no-op"


# ---------------------------------------------------------------------------
# Planning helpers (pure)
# ---------------------------------------------------------------------------


def _require_choice(
    value: Any, field: str, choices: Sequence[str]
) -> str:
    """Return ``value`` if it is one of ``choices``, else raise."""
    if not isinstance(value, str) or value not in choices:
        raise ReductionError(
            f"Unknown {field} {value!r}.",
            {"allowed": ", ".join(choices)},
        )
    return value


def _describe_seconds(seconds: float) -> str:
    """Render a duration in the largest unit that keeps it >= 1."""
    for scale, unit in ((86400.0, "day"), (3600.0, "hour"), (60.0, "minute")):
        if seconds >= scale:
            value = seconds / scale
            return f"{value:.6g} {unit}{'s' if value != 1 else ''}"
    return f"{seconds:.6g} seconds"


def _is_time_like(var: xr.DataArray) -> bool:
    """Whether ``var`` is (or can be decoded to) a time coordinate."""
    if np.issubdtype(var.dtype, np.datetime64):
        return True
    if var.dtype == object and var.size:
        # cftime objects for non-standard calendars.
        return hasattr(np.ravel(var.values)[0], "calendar")
    units = str(var.attrs.get("units", ""))
    return " since " in units


def find_time_dim(ds: xr.Dataset) -> Optional[str]:
    """Find the name of the time dimension of ``ds``.

    Prefers the conventional names in :data:`_TIME_DIM_CANDIDATES`, then
    falls back to any dimension coordinate that looks time-like -- either
    already decoded, or carrying a CF ``units`` attribute of the form
    ``"<unit> since <reference>"``.

    Returns ``None`` when the dataset has no time dimension.
    """
    for name in _TIME_DIM_CANDIDATES:
        if name in ds.dims and name in ds.variables:
            return name
    for name in map(str, ds.dims):
        if name in ds.variables and _is_time_like(ds[name]):
            return name
    return None


def find_spatial_dims(ds: xr.Dataset, time_dim: Optional[str]) -> Tuple[str, ...]:
    """Guess the spatial dimensions of ``ds``.

    Everything that is not the time dimension and not obviously a
    non-spatial auxiliary axis (``bnds``, ``nv``, ``level``, ...).  Used
    only by spatial reduction; kept here so the heuristic lives next to the
    grid-detection helpers.
    """
    skip = {"bnds", "bounds", "nv", "nvertex", "vertices", "string_length"}
    vertical = {"lev", "level", "plev", "height", "depth", "z", "pressure"}
    return tuple(
        str(d)
        for d in ds.dims
        if str(d) != time_dim and str(d) not in skip and str(d) not in vertical
    )


def _resolve_weights(
    ds: xr.Dataset, dims: Sequence[str], weighting: str
) -> Tuple[str, Optional[str]]:
    """Resolve the ``auto`` weighting strategy against a concrete grid.

    Returns the resolved strategy and, for ``cell_area``, the name of the
    area variable.

    The resolution order is deliberately fail-loud: an unweighted mean over
    a regular lat/lon grid is wrong by several kelvin for a global field and
    looks entirely plausible in a plot, so a grid we cannot weight is an
    error rather than a guess.

    Reserved -- called only once spatial reduction is implemented.
    """
    if weighting != "auto":
        if weighting == "cell_area":
            for cand in _CELL_AREA_CANDIDATES:
                if cand in ds.variables:
                    return "cell_area", cand
            raise ReductionError(
                "No cell-area variable found for weighting='cell_area'.",
                {"looked_for": ", ".join(_CELL_AREA_CANDIDATES)},
            )
        return weighting, None

    for cand in _CELL_AREA_CANDIDATES:
        if cand in ds.variables:
            return "cell_area", cand

    for lat_name in ("lat", "latitude"):
        if lat_name in ds.coords and ds[lat_name].ndim == 1:
            return "cos_lat", None

    if any(d in _EQUAL_AREA_DIMS for d in dims):
        # HEALPix and friends: cells are equal-area, unweighted is correct.
        return "none", None

    raise ReductionError(
        "Cannot determine area weights for this grid.",
        {
            "dims": ", ".join(dims),
            "hint": (
                "Provide a cell-area variable, or pass weighting='none' to "
                "accept an unweighted (area-biased) result."
            ),
        },
    )


def _axis_seconds(ds: xr.Dataset, dim: str) -> Optional[np.ndarray]:
    """The time axis expressed in seconds, or ``None`` if unknowable.

    Works on the undecoded axis: CF ``units`` give the unit, so no decoding
    is needed in order to reason about spacing.  Returns ``None`` for a
    single-step axis, unparsable units or values that will not become
    numbers, in which case the caller must not guess.
    """
    if dim not in ds.variables or ds.sizes.get(dim, 0) < 2:
        return None
    var = ds[dim]
    if np.issubdtype(var.dtype, np.datetime64):
        return var.values.astype("datetime64[s]").astype("float64")
    units = str(var.attrs.get("units", "")).lower()
    unit = units.split(" since ", 1)[0].strip() if " since " in units else ""
    scale = _UNIT_SECONDS.get(unit)
    if scale is None:
        return None
    try:
        return np.asarray(var.values, dtype="float64") * scale
    except (TypeError, ValueError):
        return None


def _median_step_seconds(ds: xr.Dataset, dim: str) -> Optional[float]:
    """Median spacing of the time axis in seconds, for error messages."""
    axis = _axis_seconds(ds, dim)
    if axis is None:
        return None
    steps = np.diff(axis)
    return float(np.median(steps)) if steps.size else None


def _check_not_inflating(ds: xr.Dataset, dim: str, freq: str) -> None:
    """Reject a resampling that would produce more steps than it consumes.

    ``resample`` emits a group for every period between the first and last
    timestamp, whether or not any data falls in it.  That inflates the axis
    in two distinct ways, and both are memory bombs in the worker because
    the empty groups are materialised eagerly:

    * the target is simply finer than the source -- monthly data asked for
      at ``6hourly`` becomes a 121x longer, 99.2% NaN array, 13 GiB from a
      112 MiB source;
    * the axis is sparse -- aggregating files from disjoint periods (say a
      piControl slice alongside a historical one) leaves a 22-step axis
      spanning 1500 years, which ``yearly`` expands to ~1550 steps.

    Comparing nominal frequencies only catches the first: the sparse axis
    has a perfectly ordinary median step of one month.  So the check is on
    the invariant that actually matters -- a reduction must not grow the
    axis -- which covers both.
    """
    axis = _axis_seconds(ds, dim)
    target = _FREQUENCY_SECONDS.get(freq)
    if axis is None or target is None:
        return
    span = float(np.nanmax(axis) - np.nanmin(axis))
    if span <= 0:
        return
    n_in = int(axis.size)
    n_out = int(span // target) + 1
    if n_out <= n_in * _INFLATION_TOLERANCE + 1:
        return
    step = _median_step_seconds(ds, dim)
    raise ReductionError(
        f"Cannot reduce to {freq!r}: that would produce {n_out} time steps "
        f"from {n_in}, which is not a reduction.",
        {
            "source_steps": str(n_in),
            "source_span": _describe_seconds(span),
            "source_step": _describe_seconds(step) if step else "unknown",
            "requested": _describe_seconds(target),
            "hint": (
                "Either the requested frequency is finer than the data, or "
                "the time axis is sparse -- resampling fills every period "
                "between the first and last time step."
            ),
        },
    )


def plan_reduction(
    ds: xr.Dataset, options: Optional[Mapping[str, Any]] = None
) -> ReductionPlan:
    """Resolve user options against a dataset into an executable plan.

    This is the only place user input is validated.  Every failure mode that
    depends on the options or on the dataset's structure -- unknown
    frequency, missing time axis, unweightable grid -- surfaces here, before
    any data is touched.

    Parameters
    ----------
    ds:
        The dataset the plan will be applied to.  Only its structure is
        inspected; no data is read.
    options:
        Raw option mapping as received over the broker.  ``None`` or an
        empty mapping yields a no-op plan.

    Returns
    -------
    ReductionPlan
        A fully resolved plan.  ``plan.is_noop`` is ``True`` when nothing
        was requested.

    Raises
    ------
    ReductionError
        If the options are invalid or cannot be satisfied by ``ds``.
    """
    opts = cast(ReductionOptions, {k: v for k, v in (options or {}).items() if v})
    if not opts:
        return ReductionPlan()

    if opts.get("space") or opts.get("space_dims") or opts.get("weighting"):
        raise ReductionError(
            "Spatial reduction is not implemented yet.",
            {"hint": "Only temporal reduction is currently supported."},
        )

    dtype: Optional[str] = None
    if opts.get("time_freq"):
        # Default to float32 here as well as in the REST schema.  A broker
        # message that omits the key must not silently fall back to the
        # float64 that CF-decoding packed data produces.
        choice = _require_choice(opts.get("dtype") or "float32", "dtype", DTYPES)
        dtype = None if choice == "keep" else choice
    elif opts.get("dtype"):
        _require_choice(opts["dtype"], "dtype", DTYPES)

    time: Optional[TimeReduction] = None
    freq = opts.get("time_freq")
    if freq:
        freq = _require_choice(freq, "time_freq", FREQUENCIES)
        method = _require_choice(
            opts.get("time_method") or "mean", "time_method", METHODS
        )
        climatology = bool(opts.get("climatology"))
        min_coverage = float(opts.get("min_coverage") or 0.0)
        if not 0.0 <= min_coverage <= 1.0:
            raise ReductionError(
                f"min_coverage must be between 0 and 1, got {min_coverage}."
            )
        dim = find_time_dim(ds)
        if dim is None:
            raise ReductionError(
                "Cannot reduce in time: the dataset has no time dimension.",
                {"available_dims": ", ".join(sorted(map(str, ds.dims)))},
            )
        if climatology and freq not in CLIMATOLOGY_GROUPS:
            raise ReductionError(
                f"A {freq!r} climatology is not meaningful.",
                {"allowed": ", ".join(CLIMATOLOGY_GROUPS)},
            )
        if not climatology:
            # A climatology always collapses, so only resampling can inflate.
            _check_not_inflating(ds, dim, freq)
        time = TimeReduction(
            dim=dim,
            freq=freq,
            method=method,
            offset=None if climatology else FREQUENCY_OFFSETS[freq],
            group_key=(
                f"{dim}.{CLIMATOLOGY_GROUPS[freq].split('.', 1)[1]}"
                if climatology
                else None
            ),
            min_coverage=min_coverage,
        )
    elif opts.get("time_method") or opts.get("climatology"):
        raise ReductionError(
            "'time_method'/'climatology' require 'time_freq' to be set."
        )

    return ReductionPlan(time=time, space=None, dtype=dtype)


# ---------------------------------------------------------------------------
# Application helpers (pure)
# ---------------------------------------------------------------------------


def _bounds_variables(ds: xr.Dataset) -> List[str]:
    """Collect bounds/vertex variables that must not be reduced.

    Because the loader opens with ``decode_coords=False``, bounds arrays are
    plain data variables.  Averaging them produces meaningless numbers that
    xarray would happily serve, so they are dropped before reduction.
    """
    names: set[str] = set()
    for name, var in ds.variables.items():
        bounds = var.attrs.get("bounds")
        if isinstance(bounds, str) and bounds in ds.variables:
            names.add(bounds)
        if str(name).endswith(("_bnds", "_bounds", "_vertices")):
            names.add(str(name))
    return sorted(names)


def _decode(ds: xr.Dataset) -> xr.Dataset:
    """CF-decode a dataset for reduction.

    Applies ``mask_and_scale`` and time decoding.  ``decode_coords`` stays
    off to match the loader and to keep auxiliary coordinates as plain data
    variables.  Decoding is idempotent: an already-decoded dataset carries
    its ``units`` in ``encoding`` rather than ``attrs`` and passes through
    untouched.
    """
    return xr.decode_cf(ds, decode_times=True, decode_coords=False)


def _coverage_fraction(
    da: xr.DataArray, time: TimeReduction
) -> xr.DataArray:
    """Fraction of valid source steps contributing to each output group."""
    valid = da.notnull()
    ones = xr.ones_like(da[time.dim], dtype="int64")
    if time.is_climatology:
        n_valid = valid.groupby(cast(str, time.group_key)).sum(time.dim)
        n_total = ones.groupby(cast(str, time.group_key)).sum(time.dim)
    else:
        n_valid = valid.resample({time.dim: time.offset}).sum(time.dim)
        n_total = ones.resample({time.dim: time.offset}).sum(time.dim)
    return n_valid / n_total.where(n_total > 0)


def _reducible(ds: xr.Dataset, dim: str) -> Tuple[List[str], List[str]]:
    """Split data variables into those to reduce and those to carry over.

    Only numeric (or boolean) variables that actually carry the time
    dimension can be reduced.  Everything else has to be carried across
    untouched, because xarray would otherwise broadcast it onto the new time
    axis: a time-invariant ``orog`` field comes back with one identical copy
    per output step, and a scalar CF grid-mapping container such as
    ``rotated_pole`` -- a zero-dimensional ``|S1`` variable -- raises

        TypeError: the resolved dtypes are not compatible with add.reduce

    since numpy tries to sum bytes.  ``numeric_only`` does not save us here:
    a scalar has no reduce dimensions, so xarray reduces it regardless.

    Returns the names to reduce and the names to pass through.  Non-numeric
    variables that *do* span the time dimension are in neither list -- they
    cannot be reduced and their length would no longer match, so they are
    dropped.
    """
    reduce: List[str] = []
    keep: List[str] = []
    for name, var in ds.data_vars.items():
        numeric = np.issubdtype(var.dtype, np.number) or var.dtype == np.bool_
        if dim not in var.dims:
            keep.append(str(name))
        elif numeric:
            reduce.append(str(name))
        else:
            data_logger.debug(
                "Dropping non-numeric time-dependent variable %s (%s)",
                name,
                var.dtype,
            )
    return reduce, keep


def _reduce_time(ds: xr.Dataset, time: TimeReduction) -> xr.Dataset:
    """Apply a temporal reduction to every variable carrying the time dim."""
    to_reduce, to_keep = _reducible(ds, time.dim)
    if not to_reduce:
        raise ReductionError(
            "No numeric variable spans the time dimension.",
            {"time_dim": time.dim, "variables": ", ".join(map(str, ds.data_vars))},
        )
    source = ds[to_reduce]

    if time.is_climatology:
        grouped: Any = source.groupby(cast(str, time.group_key))
    else:
        grouped = source.resample({time.dim: time.offset})
    reduced = cast(xr.Dataset, getattr(grouped, time.method)(keep_attrs=True))

    if time.min_coverage > 0.0:
        for name in to_reduce:
            da = source[name]
            if name not in reduced or not np.issubdtype(da.dtype, np.floating):
                continue
            fraction = _coverage_fraction(da, time)
            reduced[name] = reduced[name].where(fraction >= time.min_coverage)
            reduced[name].attrs = dict(da.attrs)

    # Grid mappings, fixed fields and other time-invariant variables ride
    # along unchanged rather than being broadcast onto the new time axis.
    for name in to_keep:
        reduced[name] = ds[name]
    return reduced


def _tag_cell_methods(ds: xr.Dataset, fragments: Iterable[str]) -> xr.Dataset:
    """Append CF ``cell_methods`` fragments to every data variable."""
    suffix = " ".join(fragments)
    if not suffix:
        return ds
    for name in ds.data_vars:
        existing = str(ds[name].attrs.get("cell_methods", "")).strip()
        ds[name].attrs["cell_methods"] = f"{existing} {suffix}".strip()
    return ds


def _cast(ds: xr.Dataset, dtype: str) -> xr.Dataset:
    """Downcast floating-point data variables to ``dtype``.

    CF decoding of packed integers promotes to ``float64`` whenever
    ``scale_factor`` is stored as a double, which is the common case.  That
    quadruples the wire size relative to the source for no gain in
    precision.
    """
    for name, da in ds.data_vars.items():
        if np.issubdtype(da.dtype, np.floating) and da.dtype != np.dtype(dtype):
            ds[name] = da.astype(dtype, copy=False)
            ds[name].attrs = dict(da.attrs)
    return ds


def apply_reduction(ds: xr.Dataset, plan: ReductionPlan) -> xr.Dataset:
    """Execute a reduction plan against a dataset.

    The dataset is CF-decoded first (see the module docstring).  Bounds
    variables are dropped, the requested reductions are applied lazily, CF
    ``cell_methods`` are updated, and floating-point variables are cast to
    the requested dtype.

    The result stays dask-backed: nothing is computed here.

    Parameters
    ----------
    ds:
        Dataset to reduce, typically straight out of the aggregator.
    plan:
        Plan produced by :func:`plan_reduction` for this dataset.

    Returns
    -------
    xarray.Dataset
        The reduced dataset, or ``ds`` unchanged for a no-op plan.
    """
    if plan.is_noop:
        return ds

    out = _decode(ds)
    drop = _bounds_variables(out)
    if drop:
        data_logger.debug("Dropping bounds variables before reduction: %s", drop)
        out = out.drop_vars(drop, errors="ignore")

    fragments: List[str] = []
    if plan.time is not None:
        out = _reduce_time(out, plan.time)
        fragments.append(plan.time.cell_method)
    if plan.space is not None:  # pragma: no cover - reserved
        raise ReductionError("Spatial reduction is not implemented yet.")

    out = _tag_cell_methods(out, fragments)
    if plan.dtype is not None:
        out = _cast(out, plan.dtype)
    return out


def reduce_datasets(
    datasets: Mapping[str, xr.Dataset],
    options: Optional[Mapping[str, Any]] = None,
) -> Dict[str, xr.Dataset]:
    """Plan and apply a reduction to each group of an aggregated result.

    Each group is planned independently, because groups may sit on different
    grids or carry different time axes.

    Parameters
    ----------
    datasets:
        Group mapping as returned by
        :meth:`~data_portal_worker.aggregator.DatasetAggregator.aggregate`.
    options:
        Raw reduction options from the broker message.

    Returns
    -------
    dict[str, xarray.Dataset]
        The same group keys, with reduced datasets.

    Raises
    ------
    ReductionError
        If the reduction cannot be planned or applied for any group.
    """
    if not options:
        return dict(datasets)
    out: Dict[str, xr.Dataset] = {}
    for name, ds in datasets.items():
        plan = plan_reduction(ds, options)
        if plan.is_noop:
            out[name] = ds
            continue
        data_logger.info("Reduction plan for %s: %s", name, plan.describe())
        try:
            out[name] = apply_reduction(ds, plan)
        except ReductionError:
            raise
        except Exception as error:
            raise ReductionError(
                "Reduction failed.",
                {"group": name, "exception": repr(error), "detail": str(error)},
            ) from error
    return out


def plan_removes_spatial_dims(
    datasets: Mapping[str, xr.Dataset],
    options: Optional[Mapping[str, Any]] = None,
) -> bool:
    """Whether a reduction collapses the spatial dimensions of any group.

    Used by the caller to pick a chunking strategy: a dataset that has lost
    its spatial dimensions is a plain time series and must not be chunked
    for ``map`` access, or it degenerates into one tiny chunk per time step.

    Always ``False`` while spatial reduction is unimplemented; the hook is
    wired up now so the chunking behaviour does not have to be revisited.
    """
    if not options:
        return False
    return any(
        plan_reduction(ds, options).removes_spatial_dims
        for ds in datasets.values()
    )
