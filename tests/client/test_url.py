"""Test for the configuration utility.

Tests for URL resolution from various config file formats
(TOML freva config, evaluation system config, etc).
"""

import sys
from pathlib import Path

import mock
import pytest

from freva_client import databrowser


class TestInvalidConfigs:
    """Tests for invalid configuration files."""

    def test_invalid_eval_config(self, invalid_eval_conf_file: Path) -> None:
        """Loading an invalid evaluation system config should raise."""
        assert invalid_eval_conf_file.is_file()
        with pytest.raises(ValueError):
            databrowser()
        db = databrowser(host="www.example.com:8080")
        assert (
            db.url == "http://www.example.com:8080/api/freva-nextgen/databrowser"
        )

    def test_invalid_freva_config(self, invalid_freva_conf_file: Path) -> None:
        """Loading an invalid freva config should raise."""
        assert invalid_freva_conf_file.is_file()
        with pytest.raises(ValueError):
            databrowser()
        db = databrowser(host="https://www.example.com")
        assert (
            db.url == "https://www.example.com/api/freva-nextgen/databrowser"
        )

    def test_empty_host_toml_config(
        self, valid_freva_config_commented_host: Path
    ) -> None:
        """TOML config with a commented-out host should raise."""
        assert valid_freva_config_commented_host.is_file()
        with pytest.raises(ValueError):
            databrowser()


class TestValidConfigs:
    """Tests for valid configuration files."""

    def test_valid_eval_config(self, valid_eval_conf_file: Path) -> None:
        """A valid evaluation system config should produce the correct URL."""
        assert valid_eval_conf_file.is_file()
        db = databrowser()
        assert (
            db.url
            == "https://www.eval.conf:8081/api/freva-nextgen/databrowser"
        )
        valid_eval_conf_file.write_text(
            "[evaluation_system]\ndatabrowser.host = http://www.eval.conf/api\n"
        )
        db = databrowser()
        assert (
            db.url == "http://www.eval.conf/api/freva-nextgen/databrowser"
        )

    def test_valid_freva_config(self, valid_freva_config: Path) -> None:
        """A valid freva config should produce the correct URL."""
        assert valid_freva_config.is_dir()
        # Mock osx user
        with mock.patch.object(sys, "platform", "darwin"):
            with mock.patch("sysconfig.get_config_var", lambda x: x):
                with mock.patch(
                    "sysconfig.get_path",
                    lambda x, y="foo": str(valid_freva_config),
                ):
                    db = databrowser()
                    assert db.url == (
                        "https://www.freva.com:80/api/freva-nextgen/databrowser"
                    )
        config_file = valid_freva_config / "share" / "freva" / "freva.toml"
        assert config_file.is_file()
        config_file.write_text(config_file.read_text().replace(":80", ""))
        # Mock any user
        with mock.patch.object(sys, "platform", "linux"):
            with mock.patch(
                "sysconfig.get_path",
                lambda x, y="foo": str(valid_freva_config),
            ):
                db = databrowser()
                assert db.url == (
                    "https://www.freva.com/api/freva-nextgen/databrowser"
                )
            # test if custom flavour is read correctly
            assert db._flavour == "cmip6"
