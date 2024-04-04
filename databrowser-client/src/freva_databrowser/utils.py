"""Various utilities for getting the databrowser working."""

import logging
import os
import sys
import sysconfig
from configparser import ConfigParser, ExtendedInterpolation
from functools import cached_property, wraps
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, cast

import appdirs
import requests
from rich import print as pprint
import tomli

from .logger import Logger

APP_NAME: str = "freva-databrowser"

logger: Logger = cast(Logger, logging.getLogger(APP_NAME))


def parse_cli_args(cli_args: List[str]) -> Dict[str, List[str]]:
    """Convert the cli arguments to a dictionary."""
    logger.debug("parsing command line arguments.")
    kwargs = {}
    for entry in cli_args:
        key, _, value = entry.partition("=")
        if value and key not in kwargs:
            kwargs[key] = [value]
        elif value:
            kwargs[key].append(value)
    logger.debug(kwargs)
    return kwargs


def exception_handler(func: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap an exception handler around a function."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        """Wrapper function that handles the exeption."""
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            pprint("[red][b]User interrupt: Exit[/red][/b]")
            if logger.is_cli is True:
                raise SystemExit(150) from None
        except BaseException as error:
            if logger.is_cli is True:
                logger.error(error)
                raise SystemExit(1) from None
            raise error from None
        return None

    return wrapper


class Config:
    """Client config class.

    This class is used for basic configuration of the databrowser
    client.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        uniq_key: Literal["file", "uri"] = "file",
        flavour: str = "freva",
    ) -> None:

        self.databrowser_url = self.get_databrowser_url(host)
        self.uniq_key = uniq_key
        self._flavour = flavour

    def _read_ini(self, path: Path) -> str:
        """Read an ini file."""
        ini_parser = ConfigParser(interpolation=ExtendedInterpolation())
        ini_parser.read_string(path.read_text())
        config = ini_parser["evaluation_system"]
        scheme, host = self._split_url(
            config.get("databrowser.host") or config.get("solr.host")
        )
        host, _, port = (host or "").partition(":")
        port = port or config.get("databrowser.port", "")
        if port:
            host = f"{host}:{port}"
        return f"{scheme}://{host}"

    def _read_toml(self, path: Path) -> str:
        """Read a new style toml config file."""
        try:
            config = tomli.loads(path.read_text()).get("freva", {})
        except tomli.TOMLDecodeError as error:
            raise ValueError(f"Could not parse config file content: {error}") from None
        scheme, host = self._split_url(cast(str, config.get("databrowser_host", "")))
        host, _, port = host.partition(":")
        if port:
            host = f"{host}:{port}"
        return f"{scheme}://{host}"

    def _read_config(self, path: Path, file_type: Literal["toml", "ini"]) -> str:
        """Read the configuration."""
        data_types = {"toml": self._read_toml, "ini": self._read_ini}
        try:
            return data_types[file_type](path)
        except KeyError:
            pass
        return ""

    @cached_property
    def overview(self) -> Dict[str, Any]:
        try:
            res = requests.get(f"{self.databrowser_url}/overview", timeout=3)
        except requests.exceptions.ConnectionError:
            raise ValueError(f"Could not connect to {self.databrowser_url}") from None
        return cast(Dict[str, Any], res.json())

    def _get_databrowser_host_from_config(self) -> str:
        """Get the config file order."""

        eval_conf = self.get_dirs(user=False) / "evaluation_system.conf"
        freva_config = Path(
            os.environ.get("FREVA_CONFIG")
            or Path(self.get_dirs(user=False)) / "freva.toml"
        )
        paths: Dict[Path, Literal["toml", "ini"]] = {
            Path(appdirs.user_config_dir("freva")) / "freva.toml": "toml",
            Path(self.get_dirs(user=True)) / "freva.toml": "toml",
            freva_config: "toml",
            Path(os.environ.get("EVALUATION_SYSTEM_CONFIG_FILE") or eval_conf): "ini",
        }
        for config_path, config_type in paths.items():
            if config_path.is_file():
                host = self._read_config(config_path, config_type)
                if host:
                    return host
        raise ValueError(
            "No databrowser host configured, please use a"
            " configuration defining a databrowser host or"
            " set a host name using the `host` key"
        )

    @cached_property
    def flavour(self) -> str:
        """Get the flavour."""
        flavours = self.overview.get("flavours", [])
        if self._flavour not in flavours:
            raise ValueError(
                f"Search {self._flavour} not available, select from"
                f" {','.join(flavours)}"
            )
        return self._flavour

    @property
    def search_url(self) -> str:
        """Define the data search endpoint."""
        return f"{self.databrowser_url}/data_search/" f"{self.flavour}/{self.uniq_key}"

    @property
    def metadata_url(self) -> str:
        """Define the endpoint for the metadata search."""
        return (
            f"{self.databrowser_url}/metadata_search/" f"{self.flavour}/{self.uniq_key}"
        )

    @staticmethod
    def _split_url(url: str) -> Tuple[str, str]:
        scheme, _, hostname = url.partition("://")
        if not hostname:
            hostname = scheme
            scheme = ""
        scheme = scheme or "http"
        return scheme, hostname

    def get_databrowser_url(self, url: Optional[str]) -> str:
        """Construct the databrowser url from a given hostname."""
        url = url or self._get_databrowser_host_from_config()
        scheme, hostname = self._split_url(url)
        hostname, _, port = hostname.partition(":")
        if port:
            hostname = f"{hostname}:{port}"
        hostname = hostname.partition("/")[0]
        return f"{scheme}://{hostname}/api/databrowser"

    @staticmethod
    def get_dirs(user: bool = True) -> Path:
        """Get the 'scripts' and 'purelib' directories we'll install into.

        This is now a thin wrapper around sysconfig.get_paths(). It's not inlined,
        because some tests mock it out to install to a different location.
        """
        if user:
            if (sys.platform == "darwin") and sysconfig.get_config_var(
                "PYTHONFRAMEWORK"
            ):
                scheme = "osx_framework_user"
            else:
                scheme = f"{os.name}_user"
            return Path(sysconfig.get_path("data", scheme)) / "share" / "freva"
        # The default scheme is 'posix_prefix' or 'nt', and should work for e.g.
        # installing into a virtualenv
        return Path(sysconfig.get_path("data")) / "share" / "freva"
