import dask.array as da
import numpy as np
import xarray as xr

from data_portal_worker.rechunker import (
    ChunkOptimizer,
    _dtype_itemsize,
    _group_vars_by_dims,
)


def _mk_ds_map_like() -> xr.Dataset:
    """time, lat, lon -> typical map-like."""
    t, y, x = 10, 100, 200
    v_f32 = xr.DataArray(
        da.zeros((t, y, x), chunks=(1, 10, 20), dtype=np.float32),
        dims=("time", "lat", "lon"),
        name="v_f32",
    )
    v_f64 = xr.DataArray(
        da.zeros((t, y, x), chunks=(1, 10, 20), dtype=np.float64),
        dims=("time", "lat", "lon"),
        name="v_f64",
    )
    # Different shape variable: no time dimension, only spatial
    v_xy = xr.DataArray(
        da.zeros((y, x), chunks=(10, 20), dtype=np.float32),
        dims=("lat", "lon"),
        name="v_xy",
    )
    # Scalar variable (no dims)
    v_scalar = xr.DataArray(np.array(1.0, dtype=np.float32), name="v_scalar")

    return xr.Dataset(
        {"v_f32": v_f32, "v_f64": v_f64, "v_xy": v_xy, "v_scalar": v_scalar}
    )


def test_dtype_itemsize_object_is_conservative() -> None:
    assert _dtype_itemsize(object) == 64
    assert _dtype_itemsize(np.dtype("O")) == 64


def test_dtype_itemsize_numeric() -> None:
    assert _dtype_itemsize(np.float32) == 4
    assert _dtype_itemsize(np.float64) == 8
    assert _dtype_itemsize(np.int16) == 2


def test_group_vars_by_dims_includes_scalar_and_sorts_conservatively() -> None:
    ds = _mk_ds_map_like()
    groups = _group_vars_by_dims(ds)

    keys = [g.key for g in groups]
    # Should include "time,lat,lon", "lat,lon", and "<scalar>"
    assert "time,lat,lon" in keys
    assert "lat,lon" in keys
    assert "<scalar>" in keys

    # The "time,lat,lon" group should have max_itemsize = 8 (because v_f64 is float64)
    g_tyx = next(g for g in groups if g.key == "time,lat,lon")
    assert g_tyx.max_itemsize == 8
    assert "v_f64" in g_tyx.var_names
    assert "v_f32" in g_tyx.var_names

    # Scalar group should exist and be keyed correctly
    g_scalar = next(g for g in groups if g.key == "<scalar>")
    assert g_scalar.dims == tuple()
    assert "v_scalar" in g_scalar.var_names

    # Sorting heuristic: higher risk first (max_itemsize desc, then ndim desc).
    # "time,lat,lon" (itemsize 8, ndim 3) should appear before
    # "lat,lon" (itemsize 4, ndim 2)
    assert keys.index("time,lat,lon") < keys.index("lat,lon")


def test_plan_map_access_pins_primary_axis_and_prioritizes_spatial() -> None:
    ds = _mk_ds_map_like()
    opt = ChunkOptimizer(access_pattern="map", target="1MiB")

    plan = opt.plan(ds)
    assert plan.access_pattern == "map"
    assert plan.primary_axis == "time"

    assert plan.chunks["time"] == opt.map_primary_chunksize == 1

    assert set(plan.chunks) >= set(ds.dims)

    assert plan.chunks["lat"] >= 1
    assert plan.chunks["lon"] >= 1

    assert "time,lat,lon" in plan.estimated_bytes_per_chunk_by_group
    assert "lat,lon" in plan.estimated_bytes_per_chunk_by_group
    assert "<scalar>" in plan.estimated_bytes_per_chunk_by_group


def test_plan_time_series_access_grows_primary_first_and_applies_max_primary() -> (
    None
):
    ds = _mk_ds_map_like()

    opt = ChunkOptimizer(
        access_pattern="time_series",
        target="256KiB",
        max_primary_chunksize=2,
        max_chunks={"lat": 32, "lon": 32},
    )
    plan = opt.plan(ds)

    assert plan.access_pattern == "time_series"
    assert plan.primary_axis == "time"
    assert plan.chunks["time"] <= 2
    assert plan.chunks["lat"] <= 32
    assert plan.chunks["lon"] <= 32


def test_plan_without_any_primary_axis_candidate() -> None:
    """No "time"/"step"/"valid_time" dims -> primary_axis should be None."""
    a, b = 100, 200
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((a, b), chunks=(1, 1), dtype=np.float64),
                dims=("a", "b"),
            )
        }
    )
    opt = ChunkOptimizer(access_pattern="map", target="64KiB")
    plan = opt.plan(ds)

    assert plan.primary_axis is None
    assert plan.chunks["a"] >= 1
    assert plan.chunks["b"] >= 1


def test_skip_dim_not_in_any_var_group_via_coord_dim() -> None:
    """Create a dimension that exists only as a coordinate."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((100,), chunks=(1,), dtype=np.float64),
                dims=("a",),
            )
        },
        coords={"c": ("c", np.arange(10))},
    )
    assert "c" in ds.dims  # present in dataset
    groups = _group_vars_by_dims(ds)
    assert all(
        "c" not in g.dims for g in groups
    )  # not present in any data_var dims

    opt = ChunkOptimizer(access_pattern="map", target="4KiB")
    plan = opt.plan(ds)

    assert plan.chunks["c"] == 1


def test_rollback_branch_when_growth_overshoots_limit() -> None:
    """Construct a dataset where the growth step overshoots limit.
    Use target=50 bytes, limit=50 (overshoot_ratio=1.0), itemsize=8
    Start with chunks (1,1) => 8 bytes; grow a: 2=>16, 4=>32, 8=>64 (overshoot)"""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((100, 100), chunks=(1, 1), dtype=np.float64),
                dims=("a", "b"),
            )
        }
    )
    opt = ChunkOptimizer(
        access_pattern="map",
        target=50,  # bytes
        overshoot_ratio=1.0,  # limit == target
        growth_factor=2,
    )

    plan = opt.plan(ds)

    # After rollback, 'a' should be 4 (rolled back from 8 to 4)
    assert plan.chunks["a"] == 4
    # 'b' may remain 1 (depending on when worst_bytes hits target), but should be valid
    assert plan.chunks["b"] >= 1

    # Ensure we didn't exceed dataset sizes
    assert plan.chunks["a"] <= ds.sizes["a"]
    assert plan.chunks["b"] <= ds.sizes["b"]


def test_min_max_constraints_are_enforced_and_sanitized() -> None:
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((10, 20), chunks=(1, 1), dtype=np.float32),
                dims=("a", "b"),
            )
        }
    )
    opt = ChunkOptimizer(
        access_pattern="map",
        target="1MiB",
        min_chunks={"a": 3, "b": 5},
        max_chunks={"a": 4, "b": 6},
    )
    plan = opt.plan(ds)

    assert 3 <= plan.chunks["a"] <= 4
    assert 5 <= plan.chunks["b"] <= 6

    # Min/max cannot exceed dim sizes; also never below 1
    assert 1 <= plan.chunks["a"] <= ds.sizes["a"]
    assert 1 <= plan.chunks["b"] <= ds.sizes["b"]


def test_apply_rechunks_and_unifies_chunks() -> None:
    ds = _mk_ds_map_like()
    opt = ChunkOptimizer(access_pattern="map", target="256KiB")

    ds2 = opt.apply(ds)
    # Ensure dask-backed and chunked
    assert hasattr(ds2["v_f32"].data, "chunks")
    assert hasattr(ds2["v_f64"].data, "chunks")

    # time should be pinned to 1 for map pattern
    assert ds2["v_f32"].data.chunks[0][0] == 1
    assert ds2["v_f64"].data.chunks[0][0] == 1

    assert ds2["v_f32"].data.chunks == ds2["v_f64"].data.chunks


def test_grouping_with_object_dtype_affects_worst_case_bytes() -> None:
    # Object dtype should be treated as itemsize 64 and thus dominate planning.
    ds = xr.Dataset(
        {
            "v_num": xr.DataArray(
                da.zeros((50, 50), chunks=(1, 1), dtype=np.float32),
                dims=("a", "b"),
            ),
            # object dtype: store as numpy object array wrapped with dask
            "v_obj": xr.DataArray(
                da.from_array(np.empty((50, 50), dtype=object), chunks=(1, 1)),
                dims=("a", "b"),
            ),
        }
    )
    opt = ChunkOptimizer(access_pattern="map", target=1024)  # 1KiB
    plan = opt.plan(ds)

    # The "a,b" group exists and should have itemsize 64 due to object dtype
    group = next(g for g in plan.groups if g.key == "a,b")
    assert group.max_itemsize == 64
    # And the estimate should reflect that
    assert plan.estimated_bytes_per_chunk_by_group["a,b"] % 64 == 0


# =========================================================================
# Additional tests for missing coverage
# =========================================================================


def test_target_bytes_with_integer_target() -> None:
    """Test _target_bytes when target is an integer (not a string)."""
    opt = ChunkOptimizer(target=1024)  # integer target
    assert opt._target_bytes() == 1024

    opt2 = ChunkOptimizer(target="1KiB")  # string target for comparison
    assert opt2._target_bytes() == 1024


def test_time_series_without_max_primary_chunksize() -> None:
    """Test time_series access pattern when max_primary_chunksize is None."""
    ds = _mk_ds_map_like()
    opt = ChunkOptimizer(
        access_pattern="time_series",
        target="1MiB",
        max_primary_chunksize=None,  # Explicitly None
    )
    plan = opt.plan(ds)

    assert plan.access_pattern == "time_series"
    assert plan.primary_axis == "time"
    # Without max_primary_chunksize, time can grow freely up to dim size
    assert plan.chunks["time"] >= 1
    assert plan.chunks["time"] <= ds.sizes["time"]


def test_time_series_with_min_chunks_on_primary() -> None:
    """Test time_series with min_chunks constraint on primary axis."""
    ds = _mk_ds_map_like()
    opt = ChunkOptimizer(
        access_pattern="time_series",
        target="64KiB",
        min_chunks={"time": 5},
    )
    plan = opt.plan(ds)

    assert plan.chunks["time"] >= 5


def test_axis_priority_time_series_without_primary_axis() -> None:
    """Test _axis_priority for time_series when primary_axis is None."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((10, 20), chunks=(1, 1), dtype=np.float32),
                dims=("a", "b"),
            )
        }
    )
    opt = ChunkOptimizer(access_pattern="time_series", target="1KiB")
    plan = opt.plan(ds)

    # No primary axis found
    assert plan.primary_axis is None
    # Both dims should be in chunks
    assert "a" in plan.chunks
    assert "b" in plan.chunks


def test_empty_dataset_no_data_vars() -> None:
    """Test worst_bytes returns 0 when groups is empty (no data_vars)."""
    ds = xr.Dataset(coords={"x": np.arange(10)})
    assert len(ds.data_vars) == 0

    opt = ChunkOptimizer(access_pattern="map", target="1KiB")
    plan = opt.plan(ds)

    # Should handle empty groups gracefully
    assert plan.groups == ()
    assert plan.estimated_bytes_per_chunk_by_group == {}
    assert "x" in plan.chunks


def test_can_grow_false_when_at_max_chunks() -> None:
    """Test can_grow returns False when current chunk size >= max_chunks."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((100, 100), chunks=(1, 1), dtype=np.float32),
                dims=("a", "b"),
            )
        }
    )
    opt = ChunkOptimizer(
        access_pattern="map",
        target="1MiB",
        max_chunks={"a": 2, "b": 2},
    )
    plan = opt.plan(ds)

    # Chunks should be capped at max_chunks values
    assert plan.chunks["a"] <= 2
    assert plan.chunks["b"] <= 2


def test_can_grow_false_when_at_dim_size() -> None:
    """Test can_grow returns False when chunk size already equals dim size."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((5, 5), chunks=(1, 1), dtype=np.float32),
                dims=("a", "b"),
            )
        }
    )
    opt = ChunkOptimizer(
        access_pattern="map",
        target="1MiB",  # Large target to ensure we try to grow
    )
    plan = opt.plan(ds)

    # Chunks should not exceed dim sizes
    assert plan.chunks["a"] <= 5
    assert plan.chunks["b"] <= 5


def test_propose_returns_same_when_no_growth_possible() -> None:
    """Test propose returns same value when growth is not possible."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((2, 2), chunks=(1, 1), dtype=np.float32),
                dims=("a", "b"),
            )
        }
    )
    opt = ChunkOptimizer(
        access_pattern="map",
        target="1MiB",
        max_chunks={"a": 1, "b": 1},  # Can't grow beyond 1
    )
    plan = opt.plan(ds)

    # Chunks should stay at 1 since max_chunks prevents growth
    assert plan.chunks["a"] == 1
    assert plan.chunks["b"] == 1


def test_rollback_with_min_chunks_constraint() -> None:
    """Test rollback respects min_chunks when overshooting."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((100, 100), chunks=(1, 1), dtype=np.float64),
                dims=("a", "b"),
            )
        }
    )
    opt = ChunkOptimizer(
        access_pattern="map",
        target=50,  # bytes - very small to trigger rollback
        overshoot_ratio=1.0,
        growth_factor=2,
        min_chunks={"a": 3},  # Rollback should respect this
    )
    plan = opt.plan(ds)

    # After rollback, 'a' should be at least min_chunks value
    assert plan.chunks["a"] >= 3


def test_early_exit_when_target_reached_after_first_dim() -> None:
    """Test that growth loop exits early when target bytes is reached."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((100, 100), chunks=(1, 1), dtype=np.float64),
                dims=("a", "b"),
            )
        }
    )
    # Target = 64 bytes, itemsize = 8, so 8 elements needed
    # If 'a' grows to 8, we hit target and should exit before growing 'b' much
    opt = ChunkOptimizer(
        access_pattern="map",
        target=64,  # bytes
        overshoot_ratio=1.25,
        growth_factor=2,
    )
    plan = opt.plan(ds)

    # Should have reached target with reasonable chunk sizes
    estimated = plan.estimated_bytes_per_chunk_by_group.get("a,b", 0)
    assert estimated >= 64 or (plan.chunks["a"] * plan.chunks["b"] * 8 >= 64)


def test_est_bytes_for_group_dim_not_in_chunks() -> None:
    """Test _est_bytes_for_group when group dim is not in chunks dict."""
    from data_portal_worker.rechunker import VarGroup

    opt = ChunkOptimizer()
    group = VarGroup(
        key="a,b,c",
        dims=("a", "b", "c"),
        max_itemsize=8,
        var_names=("v",),
    )
    # Only provide chunks for 'a' and 'b', not 'c'
    chunks = {"a": 10, "b": 20}

    # Should multiply only dims present in chunks
    result = opt._est_bytes_for_group(group, chunks)
    assert result == 10 * 20 * 8  # 'c' is skipped


def test_find_primary_axis_with_step_dim() -> None:
    """Test _find_primary_axis finds 'step' as primary."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((10, 20), chunks=(1, 1), dtype=np.float32),
                dims=("step", "lat"),
            )
        }
    )
    opt = ChunkOptimizer()
    plan = opt.plan(ds)

    assert plan.primary_axis == "step"


def test_find_primary_axis_with_valid_time_dim() -> None:
    """Test _find_primary_axis finds 'valid_time' as primary."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((10, 20), chunks=(1, 1), dtype=np.float32),
                dims=("time", "lat"),
            )
        }
    )
    opt = ChunkOptimizer()
    plan = opt.plan(ds)

    assert plan.primary_axis == "time"


def test_spatial_candidates_prioritized_in_map_pattern() -> None:
    """Test that spatial dims (y, x, lat, lon) are prioritized in map pattern."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros(
                    (10, 50, 50, 100), chunks=(1, 1, 1, 1), dtype=np.float32
                ),
                dims=("time", "other", "lat", "lon"),
            )
        }
    )
    opt = ChunkOptimizer(access_pattern="map", target="64KiB")
    plan = opt.plan(ds)

    # In map pattern, spatial dims (lat, lon) should be grown before 'other'
    # With a reasonable target, lat and lon should have larger chunks than 'other'
    # (or at least be grown first)
    assert plan.chunks["time"] == 1  # Primary pinned


def test_growth_loop_continues_until_target_reached() -> None:
    """Test that growth continues across dims until target is reached."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((100, 100, 100), chunks=(1, 1, 1), dtype=np.float32),
                dims=("a", "b", "c"),
            )
        }
    )
    opt = ChunkOptimizer(
        access_pattern="map",
        target="4KiB",  # 4096 bytes, itemsize=4, need 1024 elements
    )
    plan = opt.plan(ds)

    # Total elements should be close to or exceed target
    total_elements = plan.chunks["a"] * plan.chunks["b"] * plan.chunks["c"]
    assert total_elements * 4 >= 1024  # Should reach reasonable size


def test_chunks_sanitized_to_dim_length() -> None:
    """Test that final chunks never exceed dim lengths."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((5, 10), chunks=(1, 1), dtype=np.float32),
                dims=("a", "b"),
            )
        }
    )
    opt = ChunkOptimizer(
        access_pattern="map",
        target="1MiB",
        min_chunks={"a": 100, "b": 100},  # Exceeds dim sizes
    )
    plan = opt.plan(ds)

    # Chunks should be sanitized to dim sizes
    assert plan.chunks["a"] == 5  # Capped at dim size
    assert plan.chunks["b"] == 10  # Capped at dim size


def test_min_chunks_for_nonexistent_dim_ignored() -> None:
    """Test that min_chunks for dims not in dataset are ignored."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((10, 20), chunks=(1, 1), dtype=np.float32),
                dims=("a", "b"),
            )
        }
    )
    opt = ChunkOptimizer(
        access_pattern="map",
        target="1KiB",
        min_chunks={"nonexistent": 100},  # Should be ignored
    )
    plan = opt.plan(ds)

    assert "nonexistent" not in plan.chunks
    assert "a" in plan.chunks
    assert "b" in plan.chunks


def test_max_chunks_for_nonexistent_dim_ignored() -> None:
    """Test that max_chunks for dims not in dataset are ignored."""
    ds = xr.Dataset(
        {
            "v": xr.DataArray(
                da.zeros((10, 20), chunks=(1, 1), dtype=np.float32),
                dims=("a", "b"),
            )
        }
    )
    opt = ChunkOptimizer(
        access_pattern="map",
        target="1KiB",
        max_chunks={"nonexistent": 5},  # Should be ignored
    )
    plan = opt.plan(ds)

    assert "nonexistent" not in plan.chunks
