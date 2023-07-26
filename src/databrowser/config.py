"""Module for accessing basic server configuration.

The minimal configuration is accessed via environment variables. Entries can
be overridden with a specific toml file holding configurations.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
import os
from typing import Tuple, TypedDict
import tomli

import requests

from .logger import logger, logger_file_handle, THIS_NAME

CONFIG_TYPE = TypedDict(
    "CONFIG_TYPE",
    {"API_CONFIG": Path, "LOGGER": logging.Logger, "NAME": str, "DEBUG": bool},
)
defaults: CONFIG_TYPE = {
    "API_CONFIG": Path(__file__).parent / "api_config.toml",
    "LOGGER": logger,
    "NAME": THIS_NAME,
    "DEBUG": False,
}


@dataclass
class ServerConfig:
    """Read the basic configuration for the server.

    The configuration can either be set via environment variables or a server
    config file.

    Parameters
    ----------
    config_file: pathlib.Path
        Path to the basic configuration file.
    debug: bool, default: False
        Set the logging level to DEBUG
    """

    config_file: Path = Path(os.environ.get("API_CONFIG") or defaults["API_CONFIG"])
    debug: bool = False

    def reload(self) -> None:
        """Reload the configuration."""
        self.config_file = Path(os.environ.get("API_CONFIG") or defaults["API_CONFIG"])
        self.debug = defaults["DEBUG"]
        self.__post_init__()

    @property
    def solr_fields(self) -> list[str]:
        """Get all relevant solr facet fields."""
        return self._solr_fields

    @property
    def log_level(self) -> int:
        """Get the name of the current logger level."""
        return defaults["LOGGER"].level

    @staticmethod
    def set_debug() -> None:
        """Set the logger levels to debug."""
        logger.setLevel(logging.DEBUG)
        logger_file_handle.setLevel(logging.DEBUG)

    @property
    def solr_cores(self) -> Tuple[str, str]:
        """Get the names of the solr core."""
        core = os.environ.get("API_CORE", "") or self._config["solr"]["core"]
        return core, "latest"

    @property
    def solr_host(self) -> str:
        """Get the hostname of the running apache solr server."""
        return (
            os.environ.get("API_SOLR_SERVER", "").partition(":")[0]
            or self._config["solr"]["hostname"]
        )

    @property
    def solr_port(self) -> str:
        """Get the port of the running apache solr server."""
        return os.environ.get("API_SOLR_SERVER", "").partition(":")[-1] or str(
            self._config["solr"]["port"]
        )

    def get_core_url(self, core: str) -> str:
        """Get the url for a specific solr core."""
        server = f"{self.solr_host}:{self.solr_port}"
        return f"http://{server}/solr/{core}"

    def _get_solr_fields(self) -> list[str]:
        url = f"{self.get_core_url(self.solr_cores[-1])}/schema/fields"
        fields = []
        for entry in requests.get(url, timeout=5).json().get("fields", []):
            if entry["type"] in ("extra_facet", "text_general") and entry[
                "name"
            ] not in ("file_name", "file", "file_no_version"):
                fields.append(entry["name"])
        return fields

    def __post_init__(self) -> None:
        try:
            self._config = tomli.loads(self.config_file.read_text("utf-8"))
        except Exception as error:
            logger.warning("Failed to load %s", error)
            self._config = tomli.loads(defaults["API_CONFIG"].read_text("utf-8"))
        if self.debug:
            self.set_debug()
        self._solr_fields = self._get_solr_fields()
