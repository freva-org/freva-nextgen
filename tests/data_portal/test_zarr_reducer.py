"""Tests for the Zarr dimension-reduction helpers.

This suite exercises ``data_portal_worker.reducer``: option validation and
plan resolution, the reductions themselves, and the CF-decoding behaviour
that reduction depends on.  Tests are self-contained -- no Redis, no REST
API, no client layer.

A recurring theme is that reduction *must* CF-decode first.  The loading
backend deliberately opens datasets with ``decode_cf=False`` so that raw
bytes pass straight through to the Zarr client, but ``resample``/``groupby``
need a real datetime index, and averaging packed integers that still carry
an in-band ``_FillValue`` produces silent nonsense.  Several tests below pin
that behaviour down.
"""

from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd
import pytest
import xarray as xr

import data_portal_worker.reducer as redmod


def _packed_daily(
    n: int = 120,
    start: str = "2000-01-01",
    fill_first: int = 0,
) -> xr.Dataset:
    """Build a CF-encoded daily dataset the way the loader would see it.

    The variable is packed ``int16`` with ``scale_factor``/``add_offset`` and
    an in-band ``_FillValue``, and ``time`` is an integer offset carrying CF
    ``units`` -- i.e. exactly what ``open_dataset(decode_cf=False)`` yields.

    ``fill_first`` marks that many leading steps as missing at ``(0, 0)``.
    """
    values = np.full((n, 2, 2), 100, dtype="int16")
    if fill_first:
        values[:fill_first, 0, 0] = -9999
    return xr.Dataset(
        {
            "tas": (
                ("time", "lat", "lon"),
                values,
                {
                    "scale_factor": 0.1,
                    "add_offset": 0.0,
                    "_FillValue": np.int16(-9999),
                    "units": "K",
                },
            )
        },
        coords={
            "time": (
                "time",
                np.arange(n, dtype="int64"),
                {
                    "units": f"days since {start} 00:00:00",
                    "calendar": "proleptic_gregorian",
                    "standard_name": "time",
                },
            ),
            "lat": [0.0, 1.0],
            "lon": [0.0, 1.0],
        },
    )


def _packed_hourly(n: int = 240, start: str = "2000-01-01") -> xr.Dataset:
    """An hourly source, so that no target frequency counts as upsampling."""
    return xr.Dataset(
        {"tas": (("time",), np.full(n, 100, dtype="int16"), {"units": "K"})},
        coords={
            "time": (
                "time",
                np.arange(n, dtype="int64"),
                {"units": f"hours since {start} 00:00:00", "calendar": "standard"},
            )
        },
    )


def _undecoded(ds: xr.Dataset) -> xr.Dataset:
    """Assert the fixture really is undecoded, then return it."""
    assert ds["tas"].dtype == np.dtype("int16")
    assert not np.issubdtype(ds["time"].dtype, np.datetime64)
    return ds


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


def test_reductionerror_str_includes_details() -> None:
    err = redmod.ReductionError("Boom", {"a": 1, "b": "x"})
    text = str(err)
    assert "Boom" in text
    assert "a: 1" in text
    assert "b: x" in text


def test_reductionerror_str_without_details() -> None:
    assert str(redmod.ReductionError("Boom")) == "Boom"


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


class TestPlanReduction:
    """``plan_reduction`` is where every user-facing failure must surface."""

    def test_empty_options_is_a_noop(self) -> None:
        ds = _packed_daily()
        for options in (None, {}, {"time_freq": None}):
            plan = redmod.plan_reduction(ds, options)
            assert plan.is_noop
            assert plan.describe() == "no-op"

    def test_minimal_plan_defaults_to_mean_resample(self) -> None:
        plan = redmod.plan_reduction(_packed_daily(), {"time_freq": "monthly"})
        assert plan.time is not None
        assert plan.time.method == "mean"
        assert plan.time.dim == "time"
        assert plan.time.offset == "MS"
        assert plan.time.group_key is None
        assert not plan.time.is_climatology
        assert plan.dtype == "float32"
        assert not plan.removes_spatial_dims

    def test_climatology_uses_groupby_not_resample(self) -> None:
        plan = redmod.plan_reduction(
            _packed_daily(), {"time_freq": "monthly", "climatology": True}
        )
        assert plan.time is not None
        assert plan.time.is_climatology
        assert plan.time.group_key == "time.month"
        assert plan.time.offset is None
        assert "climatology" in plan.describe()

    @pytest.mark.parametrize(
        "freq,offset",
        [
            ("hourly", "h"),
            ("3hourly", "3h"),
            ("6hourly", "6h"),
            ("daily", "D"),
            ("monthly", "MS"),
            ("seasonal", "QS-DEC"),
            ("yearly", "YS"),
            ("decadal", "10YS"),
        ],
    )
    def test_every_frequency_resolves_to_an_offset(
        self, freq: str, offset: str
    ) -> None:
        # Hourly source, so no target counts as upsampling.
        plan = redmod.plan_reduction(_packed_hourly(), {"time_freq": freq})
        assert plan.time is not None
        assert plan.time.offset == offset

    def test_dtype_keep_means_no_cast(self) -> None:
        plan = redmod.plan_reduction(
            _packed_daily(), {"time_freq": "monthly", "dtype": "keep"}
        )
        assert plan.dtype is None

    def test_unknown_frequency_is_rejected(self) -> None:
        with pytest.raises(redmod.ReductionError, match="Unknown time_freq"):
            redmod.plan_reduction(_packed_daily(), {"time_freq": "fortnightly"})

    def test_unknown_method_is_rejected(self) -> None:
        with pytest.raises(redmod.ReductionError, match="Unknown time_method"):
            redmod.plan_reduction(
                _packed_daily(), {"time_freq": "monthly", "time_method": "avg"}
            )

    def test_unknown_dtype_is_rejected(self) -> None:
        with pytest.raises(redmod.ReductionError, match="Unknown dtype"):
            redmod.plan_reduction(
                _packed_daily(), {"time_freq": "monthly", "dtype": "int8"}
            )

    def test_dtype_alone_is_validated_but_does_nothing(self) -> None:
        """``dtype`` is meaningless without a frequency, but still checked.

        Accepting a bogus value silently here would let it reach the worker
        as part of a plan that later grows a frequency.
        """
        plan = redmod.plan_reduction(_packed_daily(), {"dtype": "float64"})
        assert plan.is_noop
        assert plan.dtype is None

    def test_unknown_dtype_alone_is_still_rejected(self) -> None:
        with pytest.raises(redmod.ReductionError, match="Unknown dtype"):
            redmod.plan_reduction(_packed_daily(), {"dtype": "int8"})

    @pytest.mark.parametrize("freq", ["yearly", "decadal"])
    def test_meaningless_climatology_is_rejected(self, freq: str) -> None:
        with pytest.raises(redmod.ReductionError, match="not meaningful"):
            redmod.plan_reduction(
                _packed_daily(), {"time_freq": freq, "climatology": True}
            )

    def test_options_without_a_frequency_are_rejected(self) -> None:
        """Silently ignoring these would hand back unreduced data."""
        for options in ({"time_method": "mean"}, {"climatology": True}):
            with pytest.raises(redmod.ReductionError, match="require 'time_freq'"):
                redmod.plan_reduction(_packed_daily(), options)

    def test_out_of_range_min_coverage_is_rejected(self) -> None:
        with pytest.raises(redmod.ReductionError, match="between 0 and 1"):
            redmod.plan_reduction(
                _packed_daily(), {"time_freq": "monthly", "min_coverage": 2.0}
            )

    @pytest.mark.parametrize("freq", ["hourly", "3hourly", "6hourly"])
    def test_upsampling_is_rejected(self, freq: str) -> None:
        """Resampling upwards is not a reduction, it is a memory bomb.

        xarray emits a group for every period between the first and last
        time step, so monthly CORDEX data asked for at ``6hourly`` becomes
        a 121x longer, 99.2% NaN array -- 13 GiB from a 112 MiB source,
        materialised eagerly inside ``resample``.
        """
        with pytest.raises(redmod.ReductionError, match="not a reduction"):
            redmod.plan_reduction(_packed_daily(), {"time_freq": freq})

    def test_sparse_time_axis_is_rejected(self) -> None:
        """A short axis spanning centuries inflates just as badly.

        Aggregating files from disjoint periods -- a piControl slice
        alongside a historical one -- gives few steps across a huge span.
        Comparing nominal frequencies cannot catch this: the median step is
        a perfectly ordinary month.
        """
        ds = xr.Dataset(
            {"tas": (("time",), np.arange(22.0))},
            coords={
                "time": (
                    "time",
                    # two steps ~1500 years before the other twenty
                    np.array([0, 31] + list(range(547500, 548109, 31)), dtype="int64"),
                    {"units": "days since 0455-01-01"},
                )
            },
        )
        # The median step alone looks entirely benign.
        assert redmod._median_step_seconds(ds, "time") == 31 * 86400.0
        with pytest.raises(redmod.ReductionError) as excinfo:
            redmod.plan_reduction(ds, {"time_freq": "yearly"})
        assert "not a reduction" in excinfo.value.reason
        assert excinfo.value.details["source_steps"] == "22"
        assert "sparse" in excinfo.value.details["hint"]

    def test_unsorted_axis_is_measured_by_its_span(self) -> None:
        """Order must not matter: the span is what drives the group count."""
        values = np.arange(0, 620, 31, dtype="int64")
        rng = np.random.default_rng(0)
        rng.shuffle(values)
        ds = xr.Dataset(
            {"tas": (("time",), np.arange(float(values.size)))},
            coords={
                "time": ("time", values, {"units": "days since 2000-01-01"})
            },
        )
        assert redmod.plan_reduction(ds, {"time_freq": "yearly"}).time
        with pytest.raises(redmod.ReductionError, match="not a reduction"):
            redmod.plan_reduction(ds, {"time_freq": "daily"})

    def test_a_constant_time_axis_does_not_block_the_request(self) -> None:
        """Zero span means nothing to divide by."""
        ds = xr.Dataset(
            {"tas": (("time",), np.arange(4.0))},
            coords={
                "time": ("time", np.zeros(4, dtype="int64"),
                         {"units": "days since 2000-01-01"})
            },
        )
        assert redmod.plan_reduction(ds, {"time_freq": "yearly"}).time

    def test_same_frequency_is_allowed(self) -> None:
        """Month lengths vary 28-31 days; the guard must tolerate that."""
        month = _packed_daily(n=1500).isel(time=slice(None, None, 31))
        assert redmod.plan_reduction(month, {"time_freq": "monthly"}).time

    def test_coarsening_is_always_allowed(self) -> None:
        ds = _packed_daily(n=1500)
        for freq in ("daily", "monthly", "seasonal", "yearly", "decadal"):
            assert redmod.plan_reduction(ds, {"time_freq": freq}).time

    def test_climatology_is_exempt_from_the_upsampling_guard(self) -> None:
        """A climatology always collapses, whatever the target."""
        assert redmod.plan_reduction(
            _packed_daily(), {"time_freq": "hourly", "climatology": True}
        ).time

    def test_unparseable_time_units_do_not_block_the_request(self) -> None:
        """If the source spacing is unknowable, do not guess."""
        ds = xr.Dataset(
            {"tas": (("time",), np.arange(4.0))},
            coords={"time": ("time", np.arange(4), {"units": "parsecs since 2000"})},
        )
        assert redmod.plan_reduction(ds, {"time_freq": "hourly"}).time

    def test_unreadable_time_values_do_not_block_the_request(self) -> None:
        """Parseable units but values that will not become numbers."""
        ds = xr.Dataset(
            {"tas": (("time",), np.arange(3.0))},
            coords={
                "time": (
                    "time",
                    np.array(["a", "b", "c"], dtype=object),
                    {"units": "days since 2000-01-01"},
                )
            },
        )
        assert redmod._median_step_seconds(ds, "time") is None
        assert redmod.plan_reduction(ds, {"time_freq": "hourly"}).time

    def test_missing_time_variable_has_no_step(self) -> None:
        """A dimension without a coordinate cannot be measured."""
        ds = xr.Dataset({"tas": (("time",), np.arange(3.0))})
        assert redmod._median_step_seconds(ds, "time") is None
        assert redmod._axis_seconds(ds, "time") is None

    def test_decoded_axis_is_measured_directly(self) -> None:
        ds = xr.Dataset(
            {"tas": (("time",), np.arange(3.0))},
            coords={"time": pd.date_range("2000-01-01", periods=3, freq="D")},
        )
        assert redmod._median_step_seconds(ds, "time") == 86400.0

    def test_single_step_axis_does_not_block_the_request(self) -> None:
        ds = _packed_daily(n=1)
        assert redmod.plan_reduction(ds, {"time_freq": "hourly"}).time

    def test_dataset_without_a_time_dimension_is_rejected(self) -> None:
        ds = xr.Dataset({"z": (("lat", "lon"), np.zeros((2, 2)))})
        with pytest.raises(redmod.ReductionError, match="no time dimension"):
            redmod.plan_reduction(ds, {"time_freq": "monthly"})

    @pytest.mark.parametrize(
        "options",
        [
            {"space": "mean"},
            {"space_dims": ["lat", "lon"]},
            {"weighting": "cos_lat"},
        ],
    )
    def test_spatial_reduction_is_rejected_until_implemented(
        self, options: Dict[str, Any]
    ) -> None:
        with pytest.raises(redmod.ReductionError, match="not implemented yet"):
            redmod.plan_reduction(_packed_daily(), options)


class TestPlanDescription:
    """``describe`` and the CF fragments feed logs and ``cell_methods``."""

    def test_time_fragment(self) -> None:
        plan = redmod.plan_reduction(_packed_daily(), {"time_freq": "monthly"})
        assert plan.time is not None
        assert plan.time.cell_method == "time: mean (monthly)"

    def test_climatology_fragment(self) -> None:
        plan = redmod.plan_reduction(
            _packed_daily(), {"time_freq": "monthly", "climatology": True}
        )
        assert plan.time is not None
        assert plan.time.cell_method == "time: mean over monthly"

    def test_space_fragment(self) -> None:
        """Reserved, but the fragment has to be right when it ships."""
        space = redmod.SpaceReduction(
            method="mean", dims=("lat", "lon"), weighting="cos_lat"
        )
        assert space.cell_method == "area: mean"

    def test_describe_covers_a_spatial_plan(self) -> None:
        plan = redmod.ReductionPlan(
            time=redmod.TimeReduction(
                dim="time", freq="monthly", method="mean", offset="MS"
            ),
            space=redmod.SpaceReduction(
                method="sum", dims=("lat", "lon"), weighting="cell_area"
            ),
            dtype="float32",
        )
        described = plan.describe()
        assert "resample monthly/mean" in described
        assert "space sum over lat,lon (cell_area)" in described
        assert "cast float32" in described
        assert plan.removes_spatial_dims


class TestTimeDimensionDetection:
    """``find_time_dim`` has to cope with undecoded and decoded inputs."""

    def test_prefers_the_conventional_name(self) -> None:
        assert redmod.find_time_dim(_packed_daily()) == "time"

    def test_finds_a_conventional_alias_directly(self) -> None:
        ds = xr.Dataset(
            {"x": (("valid_time",), np.arange(3.0))},
            coords={"valid_time": pd.date_range("2000", periods=3, freq="D")},
        )
        assert redmod.find_time_dim(ds) == "valid_time"

    def test_finds_a_decoded_datetime_axis_under_an_unconventional_name(
        self,
    ) -> None:
        """Falls through the name list and recognises the dtype instead."""
        ds = xr.Dataset(
            {"x": (("forecast_reference",), np.arange(3.0))},
            coords={
                "forecast_reference": pd.date_range("2000", periods=3, freq="D")
            },
        )
        assert redmod.find_time_dim(ds) == "forecast_reference"

    def test_finds_a_cftime_axis_under_an_unconventional_name(self) -> None:
        """Non-standard calendars give an object array of cftime scalars."""
        ds = xr.Dataset(
            {"x": (("forecast_reference",), np.arange(3.0))},
            coords={
                "forecast_reference": xr.date_range(
                    "2000-01-01", periods=3, freq="D",
                    calendar="360_day", use_cftime=True,
                )
            },
        )
        assert redmod.find_time_dim(ds) == "forecast_reference"

    def test_object_axis_that_is_not_time_is_ignored(self) -> None:
        ds = xr.Dataset(
            {"x": (("station",), np.arange(3.0))},
            coords={"station": np.array(["a", "b", "c"], dtype=object)},
        )
        assert redmod.find_time_dim(ds) is None

    def test_finds_an_undecoded_axis_by_its_cf_units(self) -> None:
        ds = xr.Dataset(
            {"x": (("t0",), np.arange(3.0))},
            coords={"t0": ("t0", np.arange(3), {"units": "days since 2000-01-01"})},
        )
        assert redmod.find_time_dim(ds) == "t0"

    def test_returns_none_without_a_time_axis(self) -> None:
        assert redmod.find_time_dim(xr.Dataset({"z": (("a",), [1, 2])})) is None


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------


class TestApplyReduction:
    """The reduction itself, including the CF-decoding it depends on."""

    def test_noop_plan_returns_the_input_unchanged(self) -> None:
        ds = _packed_daily()
        assert redmod.apply_reduction(ds, redmod.ReductionPlan()) is ds

    def test_resample_collapses_time_and_keeps_space(self) -> None:
        ds = _undecoded(_packed_daily(n=120, start="2000-01-01"))
        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert out.sizes == {"time": 4, "lat": 2, "lon": 2}
        assert [str(t)[:10] for t in out["time"].values] == [
            "2000-01-01",
            "2000-02-01",
            "2000-03-01",
            "2000-04-01",
        ]

    def test_climatology_groups_across_the_whole_record(self) -> None:
        """Two years of daily data give 12 months, not 24."""
        ds = _packed_daily(n=730, start="2000-01-01")
        out = redmod.apply_reduction(
            ds,
            redmod.plan_reduction(
                ds, {"time_freq": "monthly", "climatology": True}
            ),
        )
        assert out.sizes["month"] == 12
        assert "time" not in out.dims

    def test_decoding_is_required_and_happens_automatically(self) -> None:
        """The raw dataset cannot be resampled; the reducer decodes it."""
        ds = _undecoded(_packed_daily())
        with pytest.raises(TypeError):
            # Guards the premise: without decoding this is not possible.
            ds.resample(time="MS").mean()

        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert np.issubdtype(out["time"].dtype, np.datetime64)
        assert np.issubdtype(out["tas"].dtype, np.floating)

    def test_fill_values_are_excluded_from_the_mean(self) -> None:
        """Packed fill values must not be averaged in as -9999."""
        ds = _packed_daily(n=60, fill_first=28)
        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        january = float(out["tas"].isel(time=0, lat=0, lon=0))
        # 100 * scale_factor == 10.0; the fill values contribute nothing.
        assert january == pytest.approx(10.0)

    def test_min_coverage_masks_undersampled_groups(self) -> None:
        ds = _packed_daily(n=60, start="2000-01-01", fill_first=28)
        plan = redmod.plan_reduction(
            ds, {"time_freq": "monthly", "min_coverage": 0.5}
        )
        out = redmod.apply_reduction(ds, plan)
        # January has 3/31 valid steps at (0, 0) -> masked.
        assert np.isnan(float(out["tas"].isel(time=0, lat=0, lon=0)))
        # February is untouched, and so is the rest of January.
        assert float(out["tas"].isel(time=1, lat=0, lon=0)) == pytest.approx(10.0)
        assert float(out["tas"].isel(time=0, lat=1, lon=1)) == pytest.approx(10.0)

    def test_min_coverage_masks_undersampled_climatology_groups(self) -> None:
        """The groupby branch needs the same guard as the resample one."""
        # Two years of daily data; January at (0, 0) is missing in year one.
        ds = _packed_daily(n=730, start="2000-01-01", fill_first=31)
        plan = redmod.plan_reduction(
            ds,
            {
                "time_freq": "monthly",
                "climatology": True,
                "min_coverage": 0.75,
            },
        )
        out = redmod.apply_reduction(ds, plan)
        # January pools 62 steps across both years, 31 of them missing.
        assert np.isnan(float(out["tas"].isel(month=0, lat=0, lon=0)))
        # February is complete in both years.
        assert float(out["tas"].isel(month=1, lat=0, lon=0)) == pytest.approx(10.0)

    def test_min_coverage_skips_non_floating_variables(self) -> None:
        """An integer counter has no missing values to count."""
        ds = _packed_daily(n=60)
        ds["nobs"] = (("time",), np.arange(60, dtype="int32"))
        out = redmod.apply_reduction(
            ds,
            redmod.plan_reduction(
                ds, {"time_freq": "monthly", "min_coverage": 0.9}
            ),
        )
        assert "nobs" in out.data_vars
        assert not np.isnan(out["nobs"].values).any()

    def test_min_coverage_zero_keeps_every_group(self) -> None:
        ds = _packed_daily(n=60, fill_first=28)
        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert not np.isnan(float(out["tas"].isel(time=0, lat=0, lon=0)))

    def test_dtype_is_downcast_by_default(self) -> None:
        """Decoding promotes to float64; float32 halves the wire size."""
        ds = _packed_daily()
        decoded = xr.decode_cf(ds, decode_times=True, decode_coords=False)
        assert decoded["tas"].dtype == np.dtype("float64")

        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert out["tas"].dtype == np.dtype("float32")

    def test_dtype_keep_preserves_the_decoded_precision(self) -> None:
        ds = _packed_daily()
        out = redmod.apply_reduction(
            ds,
            redmod.plan_reduction(ds, {"time_freq": "monthly", "dtype": "keep"}),
        )
        assert out["tas"].dtype == np.dtype("float64")

    def test_attributes_survive_the_reduction(self) -> None:
        ds = _packed_daily()
        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert out["tas"].attrs.get("units") == "K"
        # Packing attributes must not leak into a decoded, unpacked variable.
        assert "scale_factor" not in out["tas"].attrs
        assert "_FillValue" not in out["tas"].attrs

    def test_cell_methods_are_recorded(self) -> None:
        ds = _packed_daily()
        resampled = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert resampled["tas"].attrs["cell_methods"] == "time: mean (monthly)"

        clim = redmod.apply_reduction(
            ds,
            redmod.plan_reduction(
                ds, {"time_freq": "monthly", "climatology": True}
            ),
        )
        assert clim["tas"].attrs["cell_methods"] == "time: mean over monthly"

    def test_tagging_with_no_fragments_leaves_the_dataset_alone(self) -> None:
        """Defensive: a plan with nothing to record must not add an attr."""
        ds = _packed_daily(n=10)
        out = redmod._tag_cell_methods(ds, [])
        assert "cell_methods" not in out["tas"].attrs

    def test_existing_cell_methods_are_appended_to(self) -> None:
        ds = _packed_daily()
        ds["tas"].attrs["cell_methods"] = "area: mean"
        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert out["tas"].attrs["cell_methods"] == "area: mean time: mean (monthly)"

    def test_bounds_variables_are_dropped(self) -> None:
        """`decode_coords=False` leaves bounds as data vars; averaging them
        would produce meaningless numbers that xarray happily serves."""
        ds = _packed_daily(n=60)
        ds["time_bnds"] = (("time", "bnds"), np.zeros((60, 2)))
        ds["time"].attrs["bounds"] = "time_bnds"
        ds["lat_vertices"] = (("lat", "bnds"), np.zeros((2, 2)))

        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert "time_bnds" not in out.variables
        assert "lat_vertices" not in out.variables
        assert "tas" in out.data_vars

    def test_grid_mapping_container_survives(self) -> None:
        """A scalar CF grid-mapping variable used to crash the reduction.

        ``rotated_pole`` is a zero-dimensional ``|S1`` container.  It has no
        reduce dimensions, so ``numeric_only`` does not protect it and numpy
        raises "the resolved dtypes are not compatible with add.reduce"
        trying to sum bytes.
        """
        ds = _packed_daily(n=60)
        ds["rotated_pole"] = (
            (),
            np.array("", dtype="S1"),
            {"grid_mapping_name": "rotated_latitude_longitude"},
        )
        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert out["rotated_pole"].dims == ()
        assert out["rotated_pole"].dtype == np.dtype("S1")
        assert out["rotated_pole"].attrs["grid_mapping_name"] == (
            "rotated_latitude_longitude"
        )

    def test_time_invariant_fields_are_not_broadcast_onto_time(self) -> None:
        """An fx field such as ``orog`` must not gain a time axis.

        xarray's resample broadcasts variables that lack the reduce
        dimension, producing one identical copy per output step -- inflating
        the store, which is the opposite of the point.
        """
        ds = _packed_daily(n=60)
        ds["orog"] = (("lat", "lon"), np.ones((2, 2), dtype="float32"))
        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert out["orog"].dims == ("lat", "lon")
        assert out["orog"].shape == (2, 2)

    def test_non_numeric_time_dependent_variables_are_dropped(self) -> None:
        """They cannot be reduced and their length would no longer match."""
        ds = _packed_daily(n=60)
        ds["label"] = (("time",), np.array(["a"] * 60, dtype="S1"))
        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert "label" not in out.variables
        assert "tas" in out.data_vars

    def test_nothing_reducible_is_an_error_not_an_empty_store(self) -> None:
        ds = xr.Dataset(
            {"label": (("time",), np.array(["a"] * 60, dtype="S1"))},
            coords={
                "time": (
                    "time",
                    np.arange(60, dtype="int64"),
                    {"units": "days since 2000-01-01"},
                )
            },
        )
        with pytest.raises(redmod.ReductionError, match="No numeric variable"):
            redmod.apply_reduction(
                ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
            )

    def test_reduction_stays_lazy(self) -> None:
        """Chunks are materialised on demand, never at conversion time."""
        import dask.array as dsa

        def explode(block_info: Any = None) -> Any:
            raise AssertionError("apply_reduction must not compute chunks")

        arr = dsa.map_blocks(
            explode,
            chunks=((30,) * 4,),
            dtype="float32",
            meta=np.array((), dtype="float32"),
        )
        ds = xr.Dataset(
            {"tas": (("time",), arr)},
            coords={"time": pd.date_range("2000-01-01", periods=120, freq="D")},
        )
        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert out["tas"].chunks is not None

    @pytest.mark.parametrize("calendar", ["360_day", "noleap", "proleptic_gregorian"])
    def test_non_standard_calendars_are_supported(self, calendar: str) -> None:
        time = xr.date_range(
            "2000-01-01", periods=180, freq="D", calendar=calendar, use_cftime=True
        )
        ds = xr.Dataset({"x": (("time",), np.arange(180.0))}, coords={"time": time})
        out = redmod.apply_reduction(
            ds, redmod.plan_reduction(ds, {"time_freq": "monthly"})
        )
        assert out.sizes["time"] == 6

    def test_applying_a_spatial_plan_raises(self) -> None:
        """Defence in depth: planning rejects it, application does too."""
        plan = redmod.ReductionPlan(
            space=redmod.SpaceReduction(
                method="mean", dims=("lat", "lon"), weighting="none"
            )
        )
        with pytest.raises(redmod.ReductionError, match="not implemented yet"):
            redmod.apply_reduction(_packed_daily(), plan)


# ---------------------------------------------------------------------------
# Group mapping helpers
# ---------------------------------------------------------------------------


class TestReduceDatasets:
    """``reduce_datasets`` maps over the aggregator's group mapping."""

    def test_without_options_datasets_pass_through_untouched(self) -> None:
        ds = _packed_daily()
        out = redmod.reduce_datasets({"root": ds}, None)
        assert out["root"] is ds

    def test_every_group_is_reduced(self) -> None:
        groups = {"root": _packed_daily(n=60), "group0": _packed_daily(n=60)}
        out = redmod.reduce_datasets(groups, {"time_freq": "monthly"})
        assert sorted(out) == ["group0", "root"]
        for ds in out.values():
            assert ds.sizes["time"] == 2

    def test_a_noop_plan_passes_groups_through_by_identity(self) -> None:
        """Options can be present yet resolve to nothing to do.

        ``dtype`` without ``time_freq`` is the case that reaches the worker,
        since it is meaningless on its own.
        """
        groups = {"root": _packed_daily(), "group0": _packed_daily()}
        out = redmod.reduce_datasets(groups, {"dtype": "float64"})
        assert out["root"] is groups["root"]
        assert out["group0"] is groups["group0"]

    def test_a_reduction_error_from_apply_is_not_rewrapped(self) -> None:
        """Plan errors already carry a usable reason; keep it verbatim.

        Wrapping would bury "No numeric variable spans the time dimension"
        under a generic "Reduction failed" that tells the user nothing.
        """
        ds = xr.Dataset(
            {"label": (("time",), np.array(["a"] * 60, dtype="S1"))},
            coords={
                "time": (
                    "time",
                    np.arange(60, dtype="int64"),
                    {"units": "days since 2000-01-01"},
                )
            },
        )
        with pytest.raises(redmod.ReductionError) as excinfo:
            redmod.reduce_datasets({"root": ds}, {"time_freq": "monthly"})
        assert "No numeric variable" in excinfo.value.reason
        assert "group" not in excinfo.value.details

    def test_a_failing_group_names_itself_in_the_error(self) -> None:
        groups = {
            "root": _packed_daily(n=60),
            "group0": xr.Dataset({"z": (("lat",), np.zeros(2))}),
        }
        with pytest.raises(redmod.ReductionError, match="no time dimension"):
            redmod.reduce_datasets(groups, {"time_freq": "monthly"})

    def test_unexpected_failures_are_wrapped_with_the_group_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(_ds: xr.Dataset, _plan: Any) -> xr.Dataset:
            raise RuntimeError("kaboom")

        monkeypatch.setattr(redmod, "apply_reduction", boom)
        with pytest.raises(redmod.ReductionError) as excinfo:
            redmod.reduce_datasets(
                {"group0": _packed_daily()}, {"time_freq": "monthly"}
            )
        assert excinfo.value.details["group"] == "group0"
        assert "kaboom" in excinfo.value.details["detail"]


class TestPlanRemovesSpatialDims:
    """The hook the worker uses to pick a chunking strategy."""

    def test_false_without_options(self) -> None:
        assert not redmod.plan_removes_spatial_dims({"root": _packed_daily()}, None)

    def test_false_for_a_purely_temporal_reduction(self) -> None:
        """A time series keeps its spatial dims, so `map` chunking is fine."""
        assert not redmod.plan_removes_spatial_dims(
            {"root": _packed_daily()}, {"time_freq": "monthly"}
        )

    def test_true_once_a_spatial_plan_can_be_built(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pins the contract spatial reduction will rely on.

        Without this, a collapsed dataset would be chunked for ``map``
        access, giving one chunk of a few bytes per time step -- one HTTP
        round-trip each.
        """
        spatial = redmod.ReductionPlan(
            space=redmod.SpaceReduction(
                method="mean", dims=("lat", "lon"), weighting="none"
            )
        )
        monkeypatch.setattr(
            redmod, "plan_reduction", lambda _ds, _options: spatial
        )
        assert redmod.plan_removes_spatial_dims(
            {"root": _packed_daily()}, {"space": "mean"}
        )


class TestWeightResolution:
    """``_resolve_weights`` is reserved, but its contract is worth pinning.

    An unweighted mean over a regular lat/lon grid is wrong by several kelvin
    for a global field and looks entirely plausible in a plot, so a grid we
    cannot weight has to be an error rather than a guess.
    """

    def test_explicit_cell_area_variable_wins(self) -> None:
        ds = _packed_daily()
        ds["areacella"] = (("lat", "lon"), np.ones((2, 2)))
        assert redmod._resolve_weights(ds, ["lat", "lon"], "auto") == (
            "cell_area",
            "areacella",
        )

    def test_regular_lat_lon_falls_back_to_cos_lat(self) -> None:
        assert redmod._resolve_weights(_packed_daily(), ["lat", "lon"], "auto") == (
            "cos_lat",
            None,
        )

    def test_equal_area_grids_need_no_weights(self) -> None:
        ds = xr.Dataset(
            {"tas": (("time", "cell"), np.zeros((2, 4)))},
            coords={"time": [0, 1]},
        )
        assert redmod._resolve_weights(ds, ["cell"], "auto") == ("none", None)

    def test_unweightable_grid_is_an_error_not_a_guess(self) -> None:
        ds = xr.Dataset(
            {"tas": (("y", "x"), np.zeros((2, 2)))},
            coords={"lat": (("y", "x"), np.zeros((2, 2)))},
        )
        with pytest.raises(redmod.ReductionError, match="Cannot determine area"):
            redmod._resolve_weights(ds, ["y", "x"], "auto")

    def test_explicit_cell_area_resolves_to_the_variable(self) -> None:
        ds = _packed_daily()
        ds["areacello"] = (("lat", "lon"), np.ones((2, 2)))
        assert redmod._resolve_weights(ds, ["lat", "lon"], "cell_area") == (
            "cell_area",
            "areacello",
        )

    def test_requesting_cell_area_without_one_is_an_error(self) -> None:
        with pytest.raises(redmod.ReductionError, match="No cell-area variable"):
            redmod._resolve_weights(_packed_daily(), ["lat", "lon"], "cell_area")

    def test_explicit_none_is_honoured(self) -> None:
        assert redmod._resolve_weights(_packed_daily(), ["lat", "lon"], "none") == (
            "none",
            None,
        )


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (86400.0, "1 day"),
        (2 * 86400.0, "2 days"),
        (3600.0, "1 hour"),
        (6 * 3600.0, "6 hours"),
        (60.0, "1 minute"),
        (30.0, "30 seconds"),
        (0.5, "0.5 seconds"),
    ],
)
def test_describe_seconds_picks_a_readable_unit(
    seconds: float, expected: str
) -> None:
    """This string ends up in the error a user sees, so it must read well."""
    assert redmod._describe_seconds(seconds) == expected


def test_find_spatial_dims_skips_time_and_auxiliary_axes() -> None:
    ds = xr.Dataset(
        {"tas": (("time", "lev", "lat", "lon", "bnds"), np.zeros((2, 2, 2, 2, 2)))},
        coords={"time": [0, 1]},
    )
    dims: List[str] = list(redmod.find_spatial_dims(ds, "time"))
    assert sorted(dims) == ["lat", "lon"]
