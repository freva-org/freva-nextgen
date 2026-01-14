"""Module for aggregation of xarray datasets."""

import json
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
)

import xarray as xr

from .zarr_utils import create_zmetadata, jsonify_zmetadata


class RedisLike(Protocol):
    """
    Minimal Redis protocol for caching aggregation errors.

    This matches the subset of methods your worker already uses.
    """

    def setex(self, key: str, ttl: int, value: bytes) -> Any: ...
    def get(self, key: str) -> Any: ...


class AggregationError(RuntimeError):
    """
    Raised when aggregation fails for reasons that should be user-visible.

    Attributes
    ----------
    reason:
        Human-readable explanation, safe to surface to users.
    details:
        Optional structured metadata to help debugging.
    """

    def __init__(self, reason: str, details: Optional[Mapping[str, Any]] = None):
        super().__init__(reason)
        self.reason = reason
        self.details = dict(details or {})


@dataclass(frozen=True)
class ConcatOptions:
    """
    Options for concatenation.

    Parameters
    ----------
    dim:
        Dimension to concatenate along. If it does not exist, a new dimension
        is created.
    join:
        How to align coordinate indexes across inputs ("outer", "inner",
        "exact", "left", "right").
    compat:
        How to treat non-concatenated variables with the same name.
        Common values: "equals", "no_conflicts", "override".
    data_vars:
        Which data variables to concatenate ("minimal", "different", "all").
    coords:
        Which coordinate variables to concatenate ("minimal", "different", "all").
    """

    dim: str
    join: Literal["outer", "inner", "exact", "left", "right"] = "outer"
    compat: Literal["no_conflicts", "equals", "override"] = "no_conflicts"
    data_vars: Literal["minimal", "different", "all"] = "all"
    coords: Literal["minimal", "different", "all"] = "minimal"


@dataclass(frozen=True)
class MergeOptions:
    """
    Options for merging.

    Parameters
    ----------
    join:
        How to align coordinate indexes across inputs ("outer", "inner",
        "exact", "left", "right").
    compat:
        How to treat variables with same name.
        Common values: "equals", "no_conflicts", "override".
    """

    join: Literal["outer", "inner", "exact", "left", "right"] = "outer"
    compat: Literal["no_conflicts", "equals", "override"] = "no_conflicts"


@dataclass(frozen=True)
class AggregationPlan:
    """
    A user-provided or inferred plan.

    mode:
        "merge" or "concat".
    concat:
        Options for concat if mode == "concat".
    merge:
        Options for merge if mode == "merge".
    group_by:
        If set, forces grouping by a signature key. Otherwise grouping is
        attempted only when direct combine fails.
    """

    mode: Literal["merge", "concat"] = "concat"
    concat: Optional[ConcatOptions] = None
    merge: Optional[MergeOptions] = None
    group_by: Optional[str] = None


@dataclass(frozen=True)
class WriteZarrOptions:
    """
    Options for writing datasets to Zarr.

    Parameters
    ----------
    mode:
        Mode passed to `Dataset.to_zarr`. Typical values:
        - "w" (overwrite)
        - "a" (append/update)
    compute:
        Whether to compute immediately (True) or return dask graph (False).
    consolidated:
        Always False here. Consolidation is handled by our `.zmetadata` writer.
    """

    mode: str = "w"
    compute: bool = True
    consolidated: bool = False


RegridFn = Callable[[xr.Dataset], xr.Dataset]


def _guess_concat_dim(dsets: Iterable[xr.Dataset]) -> Optional[str]:
    """
    Guess a concatenation dimension.

    Prefers "time" if present across all datasets. Otherwise selects the first
    common dimension name across all datasets.
    """
    dsets_list = list(dsets)
    if not dsets_list:
        return None

    dims_sets = [set(ds.dims.keys()) for ds in dsets_list]
    common = set.intersection(*dims_sets) if dims_sets else set()

    if "time" in common:
        return "time"
    if common:
        return sorted(common)[0]
    return None


def _grid_signature(ds: xr.Dataset) -> str:
    """
    Build a stable-ish signature for grouping datasets.

    This is intentionally simple: it avoids any expensive computations and
    catches obvious grid mismatches (dimension names and sizes, plus key coords).

    You can extend this later (e.g., hash of lat/lon arrays) if needed.
    """
    dim_sig = ",".join(f"{k}={ds.dims[k]}" for k in sorted(ds.dims.keys()))
    coord_keys = [
        k for k in ("lat", "lon", "rlat", "rlon", "x", "y") if k in ds.coords
    ]
    coord_sig_parts: list[str] = []
    for k in coord_keys:
        c = ds.coords[k]
        coord_sig_parts.append(f"{k}:{c.dims}:{c.shape}")
    coord_sig = ",".join(coord_sig_parts)
    return f"dims[{dim_sig}]|coords[{coord_sig}]"


def _vars_signature(ds: xr.Dataset) -> str:
    """
    Signature for grouping by variable set.
    """
    return ",".join(sorted(ds.data_vars.keys()))


def _write_json(store: MutableMapping[str, bytes], key: str, obj: Any) -> None:
    """
    Write JSON object as UTF-8 bytes to a Zarr store mapping.
    """
    data = json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
    store[key] = data


def _choose_group_key(ds: xr.Dataset, group_by: str) -> str:
    if group_by == "grid":
        return _grid_signature(ds)
    if group_by == "vars":
        return _vars_signature(ds)
    raise ValueError(f"Unknown group_by='{group_by}'")


def write_grouped_zarr(
    datasets: Mapping[str, xr.Dataset],
    store: MutableMapping[str, bytes],
    *,
    options: Optional[WriteZarrOptions] = None,
    zmetadata_key: str = ".zmetadata",
) -> Dict[str, Any]:
    """
    Write grouped datasets into a single Zarr store and write consolidated
    `.zmetadata` covering the whole hierarchy.

    Group mapping
    -------------
    - datasets["root"]  -> root group "/"
    - datasets["group0"] -> "/group0"
    - datasets["group1"] -> "/group1"
    - ...

    Parameters
    ----------
    datasets:
        Mapping of group name to dataset. "root" is special and is written to
        the store root. Other keys are written as Zarr groups under that name.
    store:
        A Zarr-compatible mapping (e.g. fsspec mapper or any MutableMapping
        that accepts bytes values).
    options:
        Write options for `to_zarr`.
    zmetadata_key:
        Key name for consolidated metadata, usually ".zmetadata".

    Returns
    -------
    dict
        The combined `.zmetadata` JSON dict that was written to the store.

    Notes
    -----
    - This function writes each dataset with `consolidated=False`.
    - It then builds a combined `.zmetadata` by:
      1) generating per-dataset metadata with your existing `create_zmetadata`
      2) prefixing subgroup keys (e.g. "tas/.zarray" -> "group0/tas/.zarray")
      3) writing one root `.zmetadata` that references all arrays in all groups

    This is compatible with clients that open the store root and then access
    group paths (e.g. "group0").
    """
    options = options or WriteZarrOptions()

    # 1) Write datasets into the store (root and groups).
    for name, ds in datasets.items():
        group = None if name == "root" else name
        ds.to_zarr(
            store,
            group=group,
            mode=options.mode if name == "root" else "a",
            consolidated=options.consolidated,
            compute=options.compute,
        )

    # 2) Build combined consolidated metadata.
    combined_meta: Dict[str, Any] = {}
    combined_meta.setdefault("zarr_consolidated_format", 1)
    combined_meta.setdefault("metadata", {})

    for name, ds in datasets.items():
        # Your existing logic.
        meta = create_zmetadata(ds)
        json_meta = jsonify_zmetadata(ds, meta)

        # `jsonify_zmetadata` typically returns the dict you serve, i.e.
        # {"zarr_consolidated_format": 1, "metadata": {...}} or similar.
        metadata_block = json_meta.get("metadata", json_meta)

        if not isinstance(metadata_block, dict):
            raise TypeError("jsonify_zmetadata returned unexpected structure")

        prefix = "" if name == "root" else f"{name}/"
        for key, value in metadata_block.items():
            combined_meta["metadata"][f"{prefix}{key}"] = value

        # Ensure the group marker exists for subgroups.
        # Some implementations only include array entries; add `.zgroup` if absent.
        if name != "root":
            group_key = f"{name}/.zgroup"
            if group_key not in combined_meta["metadata"]:
                combined_meta["metadata"][group_key] = {"zarr_format": 2}

    # 3) Write consolidated metadata at the store root.
    _write_json(store, zmetadata_key, combined_meta)

    return combined_meta


class DatasetAggregator:
    """
    Aggregate multiple xarray Datasets into either one Dataset or groups.

    The core API is `aggregate()`, which returns a mapping:
    - key "root" for a single merged/concatenated dataset, or
    - multiple group keys when grouping is required.

    Error handling:
    - If aggregation fails, AggregationError is raised.
    - If a Redis cache is provided, the error reason is stored under
      `<error_cache_prefix><job_id>`.

    Notes
    -----
    This class does NOT write to Zarr. It only returns datasets (or groups of
    datasets) to be written by your existing Zarr logic (possibly as Zarr groups).
    """

    def __init__(
        self,
        *,
        regrid: Optional[RegridFn] = None,
    ) -> None:
        self._regrid = regrid

    def aggregate(
        self,
        datasets: List[xr.Dataset],
        *,
        job_id: str,
        plan: Optional[AggregationPlan] = None,
    ) -> Dict[str, xr.Dataset]:
        """
        Aggregate datasets according to a plan or inferred strategy.

        Parameters
        ----------
        datasets:
            Input datasets.
        job_id:
            Unique identifier used for caching errors in Redis.
        plan:
            Optional plan. If None, a plan will be inferred.

        Returns
        -------
        dict[str, xarray.Dataset]
            If a single dataset can be produced, returns {"root": dataset}.
            Otherwise returns multiple groups, e.g. {"group0": ds0, "group1": ds1}.

        Raises
        ------
        AggregationError
            If aggregation fails and cannot be resolved by grouping.
        """
        if not datasets:
            return {"root": xr.Dataset()}

        try:
            prepped = [self._apply_regrid(ds) for ds in datasets]
            inferred = plan or self._infer_plan(prepped)
            try:
                combined = self._combine(prepped, inferred)
                return {"root": combined}
            except Exception as exc:
                # Attempt grouping if direct combine fails.
                groups = self._group(prepped, inferred)
                if len(groups) == 1:
                    # One group but still failed -> raise original error as reason.
                    raise AggregationError(
                        "Aggregation failed for a single group.",
                        {"exception": repr(exc)},
                    ) from exc

                out: Dict[str, xr.Dataset] = {}
                for idx, (gkey, gsets) in enumerate(groups.items()):
                    gplan = plan or self._infer_plan(gsets)
                    try:
                        out[f"group{idx}"] = self._combine(gsets, gplan)
                    except Exception as gexc:
                        raise AggregationError(
                            "Aggregation failed for at least one group.",
                            {"group_key": gkey, "exception": repr(gexc)},
                        ) from gexc
                return out

        except Exception as error:
            agg_exc = AggregationError(
                "Unexpected aggregation failure.",
                {"exception": repr(error)},
            )
            raise agg_exc from error

    def _apply_regrid(self, ds: xr.Dataset) -> xr.Dataset:
        if self._regrid is None:
            return ds
        return self._regrid(ds)

    def _infer_plan(self, dsets: list[xr.Dataset]) -> AggregationPlan:
        """
        Infer whether to merge or concat.

        Heuristic:
        - If variable names are mostly disjoint -> merge.
        - Else if all share a concat dim (prefer "time") -> concat.
        - Else -> merge.
        """
        var_sets = [set(ds.data_vars.keys()) for ds in dsets]
        union = set.union(*var_sets)
        inter = set.intersection(*var_sets) if var_sets else set()

        # Disjoint-ish variables: merge
        if len(inter) == 0 and len(union) > 0:
            return AggregationPlan(mode="merge", merge=MergeOptions())

        concat_dim = _guess_concat_dim(dsets)
        if concat_dim is not None:
            return AggregationPlan(
                mode="concat",
                concat=ConcatOptions(dim=concat_dim),
            )
        return AggregationPlan(mode="merge", merge=MergeOptions())

    def _combine(
        self, dsets: list[xr.Dataset], plan: AggregationPlan
    ) -> xr.Dataset:
        if plan.mode == "merge":
            opts = plan.merge or MergeOptions()
            return xr.merge(dsets, join=opts.join, compat=opts.compat)
        if plan.mode == "concat":
            opts = plan.concat
            if opts is None:
                dim = _guess_concat_dim(dsets)
                if dim is None:
                    raise AggregationError(
                        "Cannot infer concat dimension.",
                        {
                            "available_dims": [
                                sorted(ds.dims.keys()) for ds in dsets
                            ]
                        },
                    )
                opts = ConcatOptions(dim=dim)
            return xr.concat(
                dsets,
                dim=opts.dim,
                join=opts.join,
                compat=opts.compat,
                data_vars=opts.data_vars,
                coords=opts.coords,
            )
        raise AggregationError("Unknown plan mode.", {"mode": plan.mode})

    def _group(
        self,
        dsets: list[xr.Dataset],
        plan: AggregationPlan,
    ) -> Dict[str, list[xr.Dataset]]:
        """
        Group datasets to enable Zarr groups when direct combine fails.

        Default behaviour: group by grid signature.
        """
        group_by = plan.group_by or "grid"
        grouped: Dict[str, list[xr.Dataset]] = {}
        for ds in dsets:
            key = _choose_group_key(ds, group_by)
            grouped.setdefault(key, []).append(ds)
        return grouped
