"""Definitions for the test environment."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Iterator

import pytest
import mock
from typer.testing import CliRunner

from freva_databrowser.utils import logger


def _prep_env(**config: str) -> Dict[str, str]:
    env = os.environ.copy()
    config = config or {}
    for key in ("FREVA_CONFIG", "EVALUATION_SYSTEM_CONFIG_FILE"):
        _ = env.pop(key, "")
    for key, value in config.items():
        env[key] = value
    return env


@pytest.fixture(scope="function")
def cli_runner() -> Iterator[CliRunner]:
    """Set up a cli mock app."""
    yield CliRunner(mix_stderr=False)
    logger.reset_cli()


@pytest.fixture(scope="function")
def valid_freva_config() -> Iterator[Path]:
    """Mock a valid freva config path."""
    with mock.patch.dict(os.environ, _prep_env(), clear=True):
        with TemporaryDirectory() as temp_dir:
            freva_config = Path(temp_dir) / "share" / "freva" / "freva.toml"
            freva_config.parent.mkdir(exist_ok=True, parents=True)
            freva_config.write_text(
                "[freva]\ndatabrowser_host = 'https://www.freva.com/api'"
            )
            yield Path(temp_dir)


@pytest.fixture(scope="function")
def invalid_freva_conf_file() -> Iterator[Path]:
    """Mock a broken freva config."""
    with TemporaryDirectory() as temp_dir:
        freva_config = Path(temp_dir) / "share" / "freva" / "freva.toml"
        freva_config.parent.mkdir(parents=True)
        with mock.patch.dict(
            os.environ,
            _prep_env(FREVA_CONFIG=str(freva_config)),
            clear=True,
        ):
            freva_config.write_text(
                "[freva]\ndatabrowser_host = https://freva_conf/api"
            )
            yield freva_config


@pytest.fixture(scope="function")
def valid_eval_conf_file() -> Iterator[Path]:
    """Mock a valid evaluation config file."""
    with TemporaryDirectory() as temp_dir:
        eval_file = Path(temp_dir) / "eval.conf"
        eval_file.write_text(
            "[evaluation_system]\n"
            "solr.host = https://www.eval.conf:8081/api\n"
            "databrowser.port = 8080"
        )
        with mock.patch.dict(
            os.environ,
            _prep_env(EVALUATION_SYSTEM_CONFIG_FILE=str(eval_file)),
            clear=True,
        ):
            with mock.patch(
                "sysconfig.get_path", lambda x, y="foo": str(temp_dir)
            ):
                yield eval_file


@pytest.fixture(scope="function")
def invalid_eval_conf_file() -> Iterator[Path]:
    """Mock a valid evaluation config file."""
    with TemporaryDirectory() as temp_dir:
        eval_file = Path(temp_dir) / "eval.conf"
        eval_file.write_text(
            "[foo]\n"
            "solr.host = http://localhost\n"
            "databrowser.port = 8080"
        )
        with mock.patch.dict(
            os.environ,
            _prep_env(EVALUATION_SYSTEM_CONFIG_FILE=str(eval_file)),
            clear=True,
        ):
            with mock.patch(
                "sysconfig.get_path", lambda x, y="foo": str(temp_dir)
            ):
                yield eval_file
