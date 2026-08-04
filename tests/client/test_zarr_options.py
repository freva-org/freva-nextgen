"""Tests for client-side Zarr option handling.

These are pure unit tests: no server, no authentication.  They pin the
payload the client builds, because the server derives its cache key from
that payload -- a spurious or renamed key silently changes which store a
request maps to, or which options reach the worker at all.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict

import pytest

from freva_client.cli.zarr_cli import (
    ReduceDtype,
    TimeFrequency,
    TimeMethod,
    reduction_options,
)
from freva_client.utils.types import ZarrOptions


class TestZarrOptionDefaults:
    """Defaults must match the REST schema, since they are sent verbatim."""

    def test_reduction_is_off_by_default(self) -> None:
        options = ZarrOptions()
        assert options.time_freq is None
        assert options.time_method is None
        assert options.climatology is False
        assert options.min_coverage == 0.0

    def test_dtype_defaults_to_float32(self) -> None:
        """CF-decoding packed data otherwise promotes to float64."""
        assert ZarrOptions().dtype == "float32"

    def test_from_dict_picks_up_reduction_keys(self) -> None:
        options = ZarrOptions.from_dict(
            {
                "time_freq": "monthly",
                "time_method": "max",
                "climatology": True,
                "min_coverage": 0.8,
                "dtype": "keep",
            }
        )
        assert options.time_freq == "monthly"
        assert options.time_method == "max"
        assert options.climatology is True
        assert options.min_coverage == 0.8
        assert options.dtype == "keep"

    def test_unknown_keys_are_still_ignored(self) -> None:
        assert ZarrOptions.from_dict({"bogus": 1, "time_freq": "daily"}).time_freq == (
            "daily"
        )


class TestConvertPayload:
    """The payload keys have to line up with the REST model exactly."""

    @staticmethod
    def _payload(**zarr_options: Any) -> Dict[str, Any]:
        """Rebuild the body ``zarr_utils.convert`` posts."""
        data: Dict[str, Any] = {
            "aggregate": None,
            "join": "outer",
            "compat": "override",
            "data_vars": "minimal",
            "coords": "minimal",
            "dim": None,
            "group_by": None,
            "path": ["/a.nc"],
        }
        data.update(asdict(ZarrOptions.from_dict(zarr_options)))
        return data

    def test_data_vars_key_is_not_hyphenated(self) -> None:
        """``data-vars`` was silently dropped by the server's model."""
        payload = self._payload()
        assert "data_vars" in payload
        assert "data-vars" not in payload

    def test_reduction_keys_are_present(self) -> None:
        payload = self._payload(time_freq="monthly")
        for key in (
            "time_freq",
            "time_method",
            "climatology",
            "min_coverage",
            "dtype",
        ):
            assert key in payload
        assert payload["time_freq"] == "monthly"


class TestReductionOptionsFromCli:
    """``reduction_options`` turns cli flags into the wire payload."""

    def test_nothing_is_sent_without_a_frequency(self) -> None:
        """A non-reducing request must look exactly like it always did.

        The server derives its cache key from the request, so emitting a
        stray ``dtype`` here would miss the cache for no reason.
        """
        assert (
            reduction_options(
                None, TimeMethod.mean, True, 0.9, ReduceDtype.float64
            )
            == {}
        )

    def test_only_the_frequency_is_sent_when_nothing_else_is_given(self) -> None:
        assert reduction_options(TimeFrequency.monthly, None, False, 0.0, None) == {
            "time_freq": "monthly"
        }

    def test_every_flag_is_forwarded(self) -> None:
        assert reduction_options(
            TimeFrequency.three_hourly,
            TimeMethod.max,
            True,
            0.5,
            ReduceDtype.keep,
        ) == {
            "time_freq": "3hourly",
            "time_method": "max",
            "climatology": True,
            "min_coverage": 0.5,
            "dtype": "keep",
        }

    def test_enum_values_not_names_are_sent(self) -> None:
        """``3hourly`` cannot be a Python identifier, so name != value."""
        assert TimeFrequency.three_hourly.value == "3hourly"
        assert reduction_options(
            TimeFrequency.three_hourly, None, False, 0.0, None
        ) == {"time_freq": "3hourly"}

    @pytest.mark.parametrize(
        "freq",
        [
            "hourly",
            "3hourly",
            "6hourly",
            "daily",
            "monthly",
            "seasonal",
            "yearly",
            "decadal",
        ],
    )
    def test_cli_vocabulary_matches_the_worker(self, freq: str) -> None:
        """The cli must not offer a frequency the worker would reject."""
        assert TimeFrequency(freq).value == freq

    @pytest.mark.parametrize(
        "method",
        ["mean", "sum", "min", "max", "std", "var", "median", "count"],
    )
    def test_method_vocabulary_matches_the_worker(self, method: str) -> None:
        assert TimeMethod(method).value == method
