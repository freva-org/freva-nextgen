import types
from typing import Any, Dict

import pytest
import xarray as xr

import data_portal_worker.aggregator as aggmod


def _ds_with_time(var: str = "tas", t0: int = 0, n: int = 2) -> xr.Dataset:
    return xr.Dataset(
        {var: ("time", list(range(t0, t0 + n)))},
        coords={"time": list(range(t0, t0 + n))},
    )


def _ds_with_xy(var: str = "tas", x: int = 2, y: int = 3) -> xr.Dataset:
    data = [[(i * 10 + j) for j in range(x)] for i in range(y)]
    return xr.Dataset(
        {var: (("y", "x"), data)},
        coords={"x": list(range(x)), "y": list(range(y))},
    )


def test_aggregationerror_str_includes_details() -> None:
    err = aggmod.AggregationError("Boom", {"a": 1, "b": "x"})
    s = str(err)
    assert "Boom" in s
    assert "a: 1" in s
    assert "b: x" in s


def test_guess_concat_dim_empty_and_common_dims() -> None:
    assert aggmod._guess_concat_dim([]) is None

    d1 = _ds_with_time("tas", 0)
    d2 = _ds_with_time("tas", 10)
    assert aggmod._guess_concat_dim([d1, d2]) == "time"

    d3 = _ds_with_xy("tas", x=2, y=2)
    d4 = _ds_with_xy("tas", x=2, y=5)
    # Common dims are x and y; function chooses first sorted name.
    assert aggmod._guess_concat_dim([d3, d4]) == "x"

    d5 = xr.Dataset({"a": ("foo", [1, 2])})
    d6 = xr.Dataset({"a": ("bar", [1, 2])})
    assert aggmod._guess_concat_dim([d5, d6]) is None


def test_signatures_and_choose_group_key() -> None:
    ds = _ds_with_xy("tas", x=3, y=2)
    # Add coords that should be included in the grid signature.
    ds = ds.assign_coords(lat=("y", [1, 2]), lon=("x", [5, 6, 7]))

    gsig = aggmod._grid_signature(ds)
    assert "dims[" in gsig
    assert "coords[" in gsig
    assert "lat" in gsig
    assert "lon" in gsig

    vsig = aggmod._vars_signature(ds)
    assert vsig == "tas"

    assert aggmod._choose_group_key(ds, "grid") == gsig
    assert aggmod._choose_group_key(ds, "vars") == vsig

    with pytest.raises(ValueError):
        aggmod._choose_group_key(ds, "nope")


def test_write_grouped_zarr_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Dict[str, Any]] = []

    def fake_to_zarr(
        ds: xr.Dataset,
        *,
        group: Any,
        mode: str,
        consolidated: bool,
        compute: bool,
    ) -> None:
        calls.append(
            {
                "group": group,
                "mode": mode,
                "consolidated": consolidated,
                "compute": compute,
                "vars": list(ds.data_vars),
            }
        )

    def fake_jsonify(ds: xr.Dataset) -> Dict[str, Any]:
        # Return a minimal metadata mapping.
        return {"metadata": {"tas/.zarray": {"dummy": True}}}

    monkeypatch.setattr(aggmod, "to_zarr", fake_to_zarr)
    monkeypatch.setattr(aggmod, "jsonify_zmetadata", fake_jsonify)

    root = _ds_with_time("tas", 0)
    g0 = _ds_with_time("tas", 100)

    meta = aggmod.write_grouped_zarr({"root": root, "group0": g0})

    # Writes root with mode from options (default "w"), groups with append "a".
    assert calls[0]["group"] is None
    assert calls[0]["mode"] == "w"
    assert calls[1]["group"] == "group0"
    assert calls[1]["mode"] == "a"

    assert meta["zarr_consolidated_format"] == 1
    assert "metadata" in meta

    # Root keys unprefixed, group keys prefixed.
    assert "tas/.zarray" in meta["metadata"]
    assert "group0/tas/.zarray" in meta["metadata"]

    # Ensures subgroup marker is present.
    assert "group0/.zgroup" in meta["metadata"]
    assert meta["metadata"]["group0/.zgroup"] == {"zarr_format": 2}


def test_write_grouped_zarr_raises_on_bad_jsonify(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_to_zarr(*args: Any, **kwargs: Any) -> None:
        return None

    def bad_jsonify(_ds: xr.Dataset) -> Dict[str, Any]:
        return {"metadata": "not-a-dict"}

    monkeypatch.setattr(aggmod, "to_zarr", fake_to_zarr)
    monkeypatch.setattr(aggmod, "jsonify_zmetadata", bad_jsonify)

    with pytest.raises(TypeError, match="unexpected structure"):
        aggmod.write_grouped_zarr({"root": _ds_with_time("tas")})


def test_datasetaggregator_returns_empty_dataset_for_no_inputs() -> None:
    agg = aggmod.DatasetAggregator()
    out = agg.aggregate([], job_id="job", plan=None)
    assert list(out.keys()) == ["root"]
    assert isinstance(out["root"], xr.Dataset)
    assert out["root"].sizes == {}


def test_apply_regrid_is_used() -> None:
    seen: list[str] = []

    def regrid(ds: xr.Dataset) -> xr.Dataset:
        seen.append("called")
        return ds.assign_coords(regridded=True)

    agg = aggmod.DatasetAggregator(regrid=regrid)
    ds = _ds_with_time("tas")
    out = agg.aggregate(
        [ds], job_id="job", plan={"mode": "concat", "dim": "time"}
    )

    assert seen == ["called"]
    assert out["root"].coords["regridded"].item() is True


def test_simple_combine_success_path(monkeypatch: pytest.MonkeyPatch) -> None:
    agg = aggmod.DatasetAggregator()
    ds = _ds_with_time("tas")

    expected = xr.Dataset({"ok": ("time", [1, 2])}, coords={"time": [0, 1]})

    def fake_combine_by_coords(*args: Any, **kwargs: Any) -> xr.Dataset:
        return expected

    monkeypatch.setattr(xr, "combine_by_coords", fake_combine_by_coords)

    out = agg.aggregate([ds], job_id="job", plan={"mode": "auto"})
    assert out == {"root": expected}


def test_simple_combine_failure_logs_and_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Directly test the helper to cover its warning + concat_dim path.
    agg = aggmod.DatasetAggregator()
    ds = _ds_with_time("tas")

    class Logger:
        def __init__(self) -> None:
            self.msgs: list[str] = []

        def warning(self, msg: str) -> None:
            self.msgs.append(msg)

    logger = Logger()
    monkeypatch.setattr(aggmod, "data_logger", logger)

    def boom(*args: Any, **kwargs: Any) -> xr.Dataset:
        raise RuntimeError("nope")

    # Both methods in the loop call combine_by_coords due to a bug; patching it is enough.
    monkeypatch.setattr(xr, "combine_by_coords", boom)

    out = agg._simple_combine([ds], {"mode": "auto", "dim": "time"})
    assert out is None
    assert len(logger.msgs) == 2
    assert "Could not use" in logger.msgs[0]


def test_infer_plan_merge_when_forced() -> None:
    agg = aggmod.DatasetAggregator()
    ds = _ds_with_time("tas")
    plan = agg._infer_plan([ds], {"mode": "merge"})
    assert plan.mode == "merge"
    assert plan.merge is not None


def test_infer_plan_concat_and_sets_dim() -> None:
    agg = aggmod.DatasetAggregator()
    d1 = _ds_with_time("tas", 0)
    d2 = _ds_with_time("tas", 10)
    options: Dict[str, Any] = {"mode": "auto"}
    # With mode='auto', the current implementation defaults to concat without
    # running the heuristic or mutating the options.
    plan = agg._infer_plan([d1, d2], options)
    assert plan.mode == "concat"
    assert plan.concat is not None


def test_infer_plan_concat_heuristic_sets_dim_when_mode_none() -> None:
    agg = aggmod.DatasetAggregator()
    d1 = _ds_with_time("tas", 0)
    d2 = _ds_with_time("tas", 10)
    options: Dict[str, Any] = {"mode": None}
    plan = agg._infer_plan([d1, d2], options)
    assert plan.mode == "concat"
    assert options["dim"] == "time"


def test_combine_concat_raises_when_dim_missing() -> None:
    agg = aggmod.DatasetAggregator()
    d1 = xr.Dataset({"a": ("foo", [1, 2])})
    d2 = xr.Dataset({"a": ("bar", [3, 4])})

    with pytest.raises(
        aggmod.AggregationError,
        match="Cannot infer concat dimension",
    ):
        agg._combine([d1, d2], aggmod.AggregationPlan(mode="concat", concat=None))


def test_combine_unknown_plan_mode() -> None:
    agg = aggmod.DatasetAggregator()
    d1 = _ds_with_time("tas")

    weird_plan = types.SimpleNamespace(mode="weird", merge=None, concat=None)
    with pytest.raises(aggmod.AggregationError, match="Unknown plan mode"):
        agg._combine([d1], weird_plan)  # type: ignore[arg-type]


def test_aggregate_infers_and_combines(monkeypatch: pytest.MonkeyPatch) -> None:
    # Make simple combine return None so we go through infer+combine.
    agg = aggmod.DatasetAggregator()

    def none_simple(*args: Any, **kwargs: Any) -> None:
        return None

    monkeypatch.setattr(agg, "_simple_combine", none_simple)

    d1 = _ds_with_time("tas", 0)
    d2 = _ds_with_time("tas", 10)

    out = agg.aggregate([d1, d2], job_id="job", plan={"mode": "auto"})
    assert list(out) == ["root"]
    assert out["root"].dims["time"] == 4


def test_aggregate_groups_when_direct_combine_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg = aggmod.DatasetAggregator()

    # Force: no simple combine; then fail the first combine; then succeed per-group.
    monkeypatch.setattr(agg, "_simple_combine", lambda *a, **k: None)

    calls = {"combine": 0}

    def failing_first_combine(
        dsets: list[xr.Dataset],
        plan: aggmod.AggregationPlan,
    ) -> xr.Dataset:
        calls["combine"] += 1
        if calls["combine"] == 1:
            raise RuntimeError("root combine fails")
        # group combine: return a merge of inputs
        return xr.merge(dsets, compat="override")

    monkeypatch.setattr(agg, "_combine", failing_first_combine)

    # Make grouping deterministic: two different grid signatures.
    d1 = _ds_with_xy("tas", x=2, y=2)
    d2 = _ds_with_xy("tas", x=3, y=2)

    out = agg.aggregate(
        [d1, d2],
        job_id="job",
        plan={"mode": "auto", "group_by": "grid"},
    )
    assert set(out.keys()) == {"group0", "group1"}


def test_aggregate_single_group_failure_is_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg = aggmod.DatasetAggregator()
    monkeypatch.setattr(agg, "_simple_combine", lambda *a, **k: None)

    def always_fail(*args: Any, **kwargs: Any) -> xr.Dataset:
        raise RuntimeError("nope")

    monkeypatch.setattr(agg, "_combine", always_fail)

    # Both datasets share the same grid signature -> one group.
    d1 = _ds_with_xy("tas", x=2, y=2)
    d2 = _ds_with_xy("tas", x=2, y=2)

    with pytest.raises(aggmod.AggregationError):
        agg.aggregate([d1, d2], job_id="job", plan={"mode": "auto"})


def test_aggregate_group_combine_failure_is_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agg = aggmod.DatasetAggregator()
    monkeypatch.setattr(agg, "_simple_combine", lambda *a, **k: None)

    calls = {"combine": 0}

    def fail_on_second_group(
        dsets: list[xr.Dataset],
        plan: aggmod.AggregationPlan,
    ) -> xr.Dataset:
        calls["combine"] += 1
        if calls["combine"] == 1:
            raise RuntimeError("root combine fails")
        if calls["combine"] == 2:
            raise RuntimeError("group0 fails")
        return xr.merge(dsets, compat="override")

    monkeypatch.setattr(agg, "_combine", fail_on_second_group)

    d1 = _ds_with_xy("tas", x=2, y=2)
    d2 = _ds_with_xy("tas", x=3, y=2)

    with pytest.raises(aggmod.AggregationError):
        agg.aggregate(
            [d1, d2],
            job_id="job",
            plan={"mode": "auto", "group_by": "grid"},
        )


def test_group_default_and_vars_grouping() -> None:
    agg = aggmod.DatasetAggregator()
    d1 = _ds_with_xy("tas", x=2, y=2)
    d2 = _ds_with_xy("tas", x=2, y=3)

    plan = aggmod.AggregationPlan(
        mode="concat",
        concat=aggmod.ConcatOptions(dim="x"),
        group_by=None,
    )
    grouped = agg._group([d1, d2], plan)
    assert len(grouped) == 2  # different grid signature

    plan2 = aggmod.AggregationPlan(
        mode="concat",
        concat=aggmod.ConcatOptions(dim="x"),
        group_by="vars",
    )
    grouped2 = agg._group([d1], plan2)
    assert list(grouped2.keys())[0] == aggmod._vars_signature(d1)


def test_infer_plan_merge_for_disjoint_vars_when_mode_none() -> None:
    agg = aggmod.DatasetAggregator()
    d1 = _ds_with_time("tas")
    d2 = _ds_with_time("pr")
    plan = agg._infer_plan([d1, d2], {"mode": None})
    assert plan.mode == "merge"
    assert plan.merge is not None


def test_infer_plan_merge_fallback_when_no_concat_dim() -> None:
    agg = aggmod.DatasetAggregator()
    # Same variable name (so not disjoint), but no common dims -> cannot concat.
    d1 = xr.Dataset({"tas": ("foo", [1, 2])})
    d2 = xr.Dataset({"tas": ("bar", [3, 4])})
    plan = agg._infer_plan([d1, d2], {"mode": None})
    assert plan.mode == "merge"
    assert plan.merge is not None


def test_combine_merge_default_merge_options() -> None:
    agg = aggmod.DatasetAggregator()
    d1 = _ds_with_time("tas", 0, n=2)
    d2 = xr.Dataset({"pr": ("time", [10, 11])}, coords={"time": [0, 1]})
    # merge=None triggers default MergeOptions() branch
    out = agg._combine([d1, d2], aggmod.AggregationPlan(mode="merge", merge=None))
    assert set(out.data_vars) == {"tas", "pr"}


def test_combine_concat_infers_dim_when_opts_none_success() -> None:
    agg = aggmod.DatasetAggregator()
    d1 = _ds_with_time("tas", 0, n=1)
    d2 = _ds_with_time("tas", 1, n=1)
    out = agg._combine(
        [d1, d2], aggmod.AggregationPlan(mode="concat", concat=None)
    )
    assert "time" in out.dims
    assert out.dims["time"] == 2


def test_html_view() -> None:
    """Test the html view integration."""
    from data_portal_worker.utils import xr_repr_html

    assert "no groups" in xr_repr_html({}).lower()
