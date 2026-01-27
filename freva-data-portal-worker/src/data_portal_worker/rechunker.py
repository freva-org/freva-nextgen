from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Tuple, cast

import numpy as np
import xarray as xr
from dask.utils import parse_bytes

AccessPattern = Literal["map", "time_series"]


@dataclass(frozen=True)
class ChunkPlan:
    chunks: Dict[str, int]
    target_bytes: int
    access_pattern: AccessPattern
    primary_axis: Optional[str]
    groups: Tuple["VarGroup", ...]  # diagnostic
    estimated_bytes_per_chunk_by_group: Dict[str, int]  # group_key -> bytes


@dataclass(frozen=True)
class VarGroup:
    """
    A group of variables that share the same dim signature (e.g. ('time','lat','lon')).
    We keep the largest itemsize among them to be conservative.
    """

    key: str  # e.g. "time,lat,lon"
    dims: Tuple[str, ...]
    max_itemsize: int
    var_names: Tuple[str, ...]


def _dtype_itemsize(dtype: Any) -> int:
    dt = np.dtype(dtype)
    if dt == object:
        return 64
    return cast(int, dt.itemsize)


def _group_vars_by_dims(ds: xr.Dataset) -> Tuple[VarGroup, ...]:
    groups: Dict[Tuple[str, ...], List[str]] = {}
    itemsize_max: Dict[Tuple[str, ...], int] = {}

    for name, da in ds.data_vars.items():
        dims = cast(Tuple[str, ...], tuple(da.dims))
        groups.setdefault(dims, []).append(str(name))
        itemsize_max[dims] = max(
            itemsize_max.get(dims, 0), _dtype_itemsize(da.dtype)
        )

    out: List[VarGroup] = []
    for dims, names in groups.items():
        key = ",".join(dims) if dims else "<scalar>"
        out.append(
            VarGroup(
                key=key,
                dims=dims,
                max_itemsize=itemsize_max[dims],
                var_names=tuple(sorted(names)),
            )
        )

    # Sort groups: higher risk first (bigger itemsize + more dims)
    out.sort(key=lambda g: (g.max_itemsize, len(g.dims)), reverse=True)
    return tuple(out)


@dataclass
class ChunkOptimizer:
    """
    Heuristic chunk planner with a simple user-facing knob: access_pattern.

    - access_pattern="map": optimize for slicing a single primary step (e.g. one time)
      across large secondary axes.
    - access_pattern="time_series": optimize for long runs along the primary axis at
      fixed/small secondary axes.

    It supports variables with different shapes/dim sets by grouping them by
    dim signature and planning conservatively across those groups.
    """

    target: str | int = "16MiB"
    access_pattern: AccessPattern = "map"

    # Default dimension name candidates users commonly have.
    primary_axis_candidates: Tuple[str, ...] = ("time", "step")

    # When access_pattern="map", we usually pin primary to 1
    map_primary_chunksize: int = 1

    # When access_pattern="time_series", we grow primary.
    max_primary_chunksize: Optional[int] = None

    # Spatial-ish dim name candidates for priority ordering in "map"
    spatial_candidates: Tuple[str, ...] = (
        "y",
        "x",
        "lat",
        "lon",
        "latitude",
        "longitude",
        "rlon",
        "rlat",
        "long",
        "X",
        "Y",
    )

    # Constraints
    min_chunks: Dict[str, int] = field(default_factory=dict)
    max_chunks: Dict[str, int] = field(default_factory=dict)

    # Growth policy
    growth_factor: int = 2
    overshoot_ratio: float = 1.25

    def _target_bytes(self) -> int:
        return (
            parse_bytes(self.target)
            if isinstance(self.target, str)
            else int(self.target)
        )

    def _find_primary_axis(self, ds: xr.Dataset) -> Optional[str]:
        for cand in self.primary_axis_candidates:
            if cand in ds.dims:
                return cand
        return None

    def _initial_chunks(
        self, ds: xr.Dataset, primary_axis: Optional[str]
    ) -> Dict[str, int]:
        chunks = {str(d): 1 for d in ds.dims}

        # Apply access pattern defaults
        if primary_axis is not None:
            if self.access_pattern == "map":
                chunks[primary_axis] = int(self.map_primary_chunksize)
            else:  # time_series
                # start small, then grow via planning; min cap can be applied below
                chunks[primary_axis] = max(
                    chunks[primary_axis],
                    int(self.min_chunks.get(primary_axis, 1)),
                )
                if self.max_primary_chunksize is not None:
                    self.max_chunks = {
                        **self.max_chunks,
                        primary_axis: int(self.max_primary_chunksize),
                    }

        # Apply floors/caps and sanitize by dim lengths
        for d, v in self.min_chunks.items():
            if d in chunks:
                chunks[d] = max(int(chunks[d]), int(v))
        for d, v in self.max_chunks.items():
            if d in chunks:
                chunks[d] = min(int(chunks[d]), int(v))
        for d in map(str, chunks):
            chunks[d] = max(1, min(int(chunks[d]), int(ds.sizes[d])))

        return chunks

    def _axis_priority(
        self, ds: xr.Dataset, primary_axis: Optional[str]
    ) -> Tuple[str, ...]:
        dims = list(ds.dims)

        if self.access_pattern == "map":
            # Grow spatial first, then everything else (except primary).
            spatial = [
                str(d)
                for d in self.spatial_candidates
                if d in ds.dims and d != primary_axis
            ]
            rest = [
                str(d) for d in dims if d != primary_axis and d not in spatial
            ]
            return tuple(spatial + rest)

        # time_series: grow primary first, then other dims
        prio: List[str] = []
        if primary_axis is not None:
            prio.append(primary_axis)
        prio.extend([str(d) for d in dims if d != primary_axis])
        return tuple(prio)

    def _est_bytes_for_group(
        self, group: VarGroup, chunks: Dict[str, int]
    ) -> int:
        n = 1
        for d in group.dims:
            if d in chunks:
                n *= int(chunks[d])
        return n * int(group.max_itemsize)

    def plan(self, ds: xr.Dataset) -> ChunkPlan:
        target_bytes = self._target_bytes()
        limit = int(target_bytes * float(self.overshoot_ratio))

        primary_axis = self._find_primary_axis(ds)
        chunks = self._initial_chunks(ds, primary_axis)
        prio = self._axis_priority(ds, primary_axis)
        groups = _group_vars_by_dims(ds)

        def worst_bytes() -> int:
            # Conservative: ensure we consider the "worst" group under current chunks
            return (
                max(self._est_bytes_for_group(g, chunks) for g in groups)
                if groups
                else 0
            )

        def can_grow(dim: str) -> bool:
            cur = int(chunks[dim])
            if cur >= int(ds.sizes[dim]):
                return False
            if dim in self.max_chunks and cur >= int(self.max_chunks[dim]):
                return False
            return True

        def propose(dim: str) -> int:
            cur = int(chunks[dim])
            dim_len = int(ds.sizes[dim])
            nxt = min(cur * int(self.growth_factor), dim_len)
            if dim in self.max_chunks:
                nxt = min(nxt, int(self.max_chunks[dim]))
            return max(cur, nxt)

        # Growth loop: iterate priority dims; try growing while it helps reach target
        for dim in prio:
            # In "map" pattern, primary is pinned by init; don't grow it.
            if self.access_pattern == "map" and dim == primary_axis:
                continue

            # If dim isn't present in any group, skip
            if not any(dim in g.dims for g in groups):
                continue

            while can_grow(dim):
                before = worst_bytes()
                nxt = propose(dim)
                if nxt == chunks[dim]:
                    break

                chunks[dim] = nxt
                after = worst_bytes()

                # If we jump way beyond limit while still below target before,
                # rollback and stop this dim
                if after > limit and before < target_bytes:
                    # rollback one step (approx)
                    rollback = max(int(nxt // self.growth_factor), 1)
                    if dim in self.min_chunks:
                        rollback = max(rollback, int(self.min_chunks[dim]))
                    chunks[dim] = min(rollback, int(ds.sizes[dim]))
                    break

                if after >= target_bytes:
                    break

            if worst_bytes() >= target_bytes:
                break

        for d in list(chunks):
            if d in self.min_chunks:
                chunks[d] = max(int(chunks[d]), int(self.min_chunks[d]))
            if d in self.max_chunks:
                chunks[d] = min(int(chunks[d]), int(self.max_chunks[d]))
            chunks[d] = max(1, min(int(chunks[d]), int(ds.sizes[d])))

        est_by_group = {
            g.key: self._est_bytes_for_group(g, chunks) for g in groups
        }

        return ChunkPlan(
            chunks=dict(chunks),
            target_bytes=target_bytes,
            access_pattern=self.access_pattern,
            primary_axis=primary_axis,
            groups=groups,
            estimated_bytes_per_chunk_by_group=est_by_group,
        )

    def apply(self, ds: xr.Dataset) -> xr.Dataset:
        plan = self.plan(ds)
        return ds.chunk(plan.chunks).unify_chunks()
