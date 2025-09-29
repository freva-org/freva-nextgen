"""Various utilities for getting the databrowser working."""

import concurrent.futures
import os
import sys
import sysconfig
from configparser import ConfigParser, ExtendedInterpolation
from functools import cached_property
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import appdirs
import numpy as np
import requests
import tomli
import xarray as xr

from . import logger


class Config:
    """Client config class.

    This class is used for basic configuration of the databrowser
    client.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        uniq_key: Literal["file", "uri"] = "file",
        flavour: Optional[str] = None,
    ) -> None:
        config_host = host or cast(str, self._get_databrowser_param_from_config("host"))

        self.databrowser_url = f"{self.get_api_url(config_host)}/databrowser"
        self.auth_url = f"{self.get_api_url(config_host)}/auth/v2"
        self.get_api_main_url = self.get_api_url(config_host)
        self.uniq_key = uniq_key
        self._flavour = (
            flavour or self._get_databrowser_param_from_config(
                "flavour",
                optional=True
            )
        )

    @cached_property
    def validate_server(self) -> bool:
        """Ping the databrowser to check if it is reachable."""
        try:
            res = requests.get(f"{self.get_api_main_url}/ping", timeout=15)
            return res.status_code == 200
        except Exception as e:
            raise ValueError(
                f"Could not connect to {self.databrowser_url}: {e}"
            ) from None

    @property
    def flavour(self) -> str:
        """Get the flavour, using server default if not configured."""
        if self._flavour:
            return self._flavour
        try:
            flavours = self.overview.get("flavours", [])
            self._flavour = flavours[0] if flavours else "freva"
        except (ValueError, IndexError, KeyError):
            self._flavour = "freva"
        return self._flavour

    def _read_ini(self, path: Path) -> Dict[str, str]:
        """Read an ini file.

        Parameters
        ----------
        path : Path
            Path to the ini configuration file.
        Returns
        -------
        Dict[str, str]
            Dictionary with the configuration.
        """
        ini_parser = ConfigParser(interpolation=ExtendedInterpolation())
        ini_parser.read_string(path.read_text())
        config = ini_parser["evaluation_system"]
        scheme, host = self._split_url(
            config.get("databrowser.host", config.get("solr.host", ""))
        )
        host, _, port = (host or "").partition(":")
        port = port or config.get("databrowser.port", "")
        if port:
            host = f"{host}:{port}"
        host = f"{scheme}://{host}"
        flavour = config.get("databrowser.default_flavour", "")
        return {
            "host": host,
            "flavour": flavour,
        }

    def _read_toml(self, path: Path) -> Dict[str, str]:
        """Read a new style toml config file.

        Parameters
        ----------
        path : Path
            Path to the toml configuration file.
        Returns
        -------
        Dict[str, str]
            Dictionary with the configuration.
        """
        try:
            config = tomli.loads(path.read_text()).get("freva", {})
            raw_host = cast(str, config.get("host", ""))
            flavour = config.get("default_flavour", "")
            if not raw_host:
                return {"host": "", "flavour": flavour}
            scheme, host = self._split_url(raw_host)
        except (tomli.TOMLDecodeError, KeyError):
            return {}
        host, _, port = host.partition(":")
        if port:
            host = f"{host}:{port}"
        host = f"{scheme}://{host}"
        return {
            "host": host,
            "flavour": flavour,
        }

    def _read_config(
            self, path: Path, file_type: Literal["toml", "ini"]
    ) -> Dict[str, str]:
        """Read the configuration.

        Parameters
        ----------
        path : Path
            Path to the configuration file.
        file_type : Literal["toml", "ini"]
            Type of the configuration file.
        Returns
        -------
        Dict[str, str]
            Dictionary with the configuration.
        """
        data_types = {"toml": self._read_toml, "ini": self._read_ini}
        try:
            return data_types[file_type](path)
        except KeyError:
            pass
        return {}

    @cached_property
    def _get_headers(self) -> Optional[Dict[str, str]]:
        """Get the headers for requests."""
        from freva_client.auth import Auth

        from .auth_utils import load_token
        auth = Auth()
        token = auth._auth_token or load_token(auth.token_file)
        if token and "access_token" in token:
            return {"Authorization": f"Bearer {token['access_token']}"}
        return None

    @cached_property
    def overview(self) -> Dict[str, Any]:
        """Get an overview of all databrowser flavours
        and search keys including custom flavours."""
        headers = self._get_headers
        try:
            res = requests.get(
                f"{self.databrowser_url}/overview",
                headers=headers,
                timeout=15
            )
            data = cast(Dict[str, Any], res.json())
            if not headers:
                data["Note"] = (
                    "Displaying only global flavours. "
                    "Authenticate to see custom user flavours as well."
                )
            return data
        except requests.exceptions.ConnectionError:
            raise ValueError(
                f"Could not connect to {self.databrowser_url}"
            ) from None

    def _get_databrowser_param_from_config(
            self, key: str, optional: bool = False
    ) -> Optional[str]:
        """Get a single config parameter following proper precedence."""
        eval_conf = self.get_dirs(user=False) / "evaluation_system.conf"
        freva_config = Path(
            os.environ.get("FREVA_CONFIG")
            or Path(self.get_dirs(user=False)) / "freva.toml"
        )
        paths: Dict[Path, Literal["toml", "ini"]] = {
            Path(appdirs.user_config_dir("freva")) / "freva.toml": "toml",
            Path(self.get_dirs(user=True)) / "freva.toml": "toml",
            freva_config: "toml",
            Path(
                os.environ.get("EVALUATION_SYSTEM_CONFIG_FILE") or eval_conf
            ): "ini",
        }

        for config_path, config_type in paths.items():
            if config_path.is_file():
                config_data = self._read_config(config_path, config_type)
                value = config_data.get(key, "")
                # we cannot igonore the empty string here, because
                # it needs to check the next config file
                if value:
                    return value if value else None
        if optional:
            return None

        raise ValueError(
            f"No databrowser {key} configured, please use a"
            f" configuration defining a databrowser {key} or"
            f" set a host name using the `{key}` key"
        )

    @property
    def search_url(self) -> str:
        """Define the data search endpoint."""
        return (
            f"{self.databrowser_url}/data-search/{self.flavour}/{self.uniq_key}"
        )

    @property
    def zarr_loader_url(self) -> str:
        """Define the url for getting zarr files."""
        return f"{self.databrowser_url}/load/{self.flavour}"

    @property
    def intake_url(self) -> str:
        """Define the url for creating intake catalogues."""
        return f"{self.databrowser_url}/intake-catalogue/{self.flavour}/{self.uniq_key}"

    @property
    def stac_url(self) -> str:
        """Define the url for creating stac catalogue."""
        return f"{self.databrowser_url}/stac-catalogue/{self.flavour}/{self.uniq_key}"

    @property
    def metadata_url(self) -> str:
        """Define the endpoint for the metadata search."""
        return (
            f"{self.databrowser_url}/metadata-search/"
            f"{self.flavour}/{self.uniq_key}"
        )

    @staticmethod
    def _split_url(url: str) -> Tuple[str, str]:
        scheme, _, hostname = url.partition("://")
        if not hostname:
            hostname = scheme
            scheme = ""
        scheme = scheme or "http"
        return scheme, hostname

    def get_api_url(self, url: str) -> str:
        """Construct the databrowser url from a given hostname."""
        scheme, hostname = self._split_url(url)
        hostname, _, port = hostname.partition(":")
        if port:
            hostname = f"{hostname}:{port}"
        hostname = hostname.partition("/")[0]
        return f"{scheme}://{hostname}/api/freva-nextgen"

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

    @property
    def userdata_url(self) -> str:
        """Define the url for adding and deleting user-data."""
        return f"{self.databrowser_url}/userdata"


class UserDataHandler:
    """Class for processing user data.
    This class is used for processing user data and extracting metadata
    from the data files.
    """

    def __init__(self, userdata_items: List[Union[str, xr.Dataset]]) -> None:
        self._suffixes = [".nc", ".nc4", ".grb", ".grib", ".zarr", "zar"]
        self.user_metadata: List[
            Dict[str, Union[str, List[str], Dict[str, str], None]]
        ] = []
        self._metadata_collection: List[Dict[str, Union[str, List[str]]]] = []
        try:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=min(int(os.cpu_count() or 4), 15)
            )
            self._process_user_data(userdata_items)
        finally:
            self._executor.shutdown(wait=True)

    def _gather_files(self, path: Path, pattern: str = "*") -> Iterator[Path]:
        """Gather all valid files from directory and wildcard pattern."""
        try:
            for item in path.rglob(pattern):
                if item.is_file() and item.suffix in self._suffixes:
                    yield item
        except (OSError, PermissionError) as e:  # pragma: no cover
            logger.warning(f"Error accessing path {path}: {e}")

    def _validate_user_data(
        self,
        user_data: Sequence[Union[str, xr.Dataset]],
    ) -> Dict[str, Union[List[Path], List[xr.Dataset]]]:
        validated_paths: List[Path] = []
        validated_xarray_datasets: List[xr.Dataset] = []
        for data in user_data:
            if isinstance(data, (str, Path)):
                path = Path(data).expanduser().absolute()
                if path.is_dir():
                    validated_paths.extend(self._gather_files(path))
                elif path.is_file() and path.suffix in self._suffixes:
                    validated_paths.append(path)
                else:
                    validated_paths.extend(
                        self._gather_files(path.parent, pattern=path.name)
                    )
            elif isinstance(data, xr.Dataset):
                validated_xarray_datasets.append(data)

        if not validated_paths and not validated_xarray_datasets:
            raise FileNotFoundError(
                "No valid file paths or xarray datasets found."
            )
        return {
            "validated_user_paths": validated_paths,
            "validated_user_xrdatasets": validated_xarray_datasets,
        }

    def _process_user_data(
        self,
        userdata_items: List[Union[str, xr.Dataset]],
    ) -> None:
        """Process xarray datasets and file paths using thread pool."""
        futures = []
        validated_userdata: Dict[str, Union[List[Path], List[xr.Dataset]]] = (
            self._validate_user_data(userdata_items)
        )
        if validated_userdata["validated_user_xrdatasets"]:
            futures.append(
                self._executor.submit(
                    self._process_userdata_in_executor,
                    validated_userdata["validated_user_xrdatasets"],
                )
            )

        if validated_userdata["validated_user_paths"]:
            futures.append(
                self._executor.submit(
                    self._process_userdata_in_executor,
                    validated_userdata["validated_user_paths"],
                )
            )
        for future in futures:
            try:
                future.result()
            except Exception as e:  # pragma: no cover
                logger.error(f"Error processing batch: {e}")

    def _process_userdata_in_executor(
        self, validated_userdata: Union[List[Path], List[xr.Dataset]]
    ) -> None:
        for data in validated_userdata:
            metadata = self._get_metadata(data)
            if metadata == {}:
                logger.warning("Error getting metadata: %s", metadata)
            else:
                self.user_metadata.append(metadata)

    def _timedelta_to_cmor_frequency(self, dt: float) -> str:
        for total_seconds, frequency in self._time_table.items():
            if dt >= total_seconds:
                return frequency
        return "fx"  # pragma: no cover

    @property
    def _time_table(self) -> dict[int, str]:
        return {
            315360000: "dec",  # Decade
            31104000: "yr",  # Year
            2538000: "mon",  # Month
            1296000: "sem",  # Seasonal (half-year)
            84600: "day",  # Day
            21600: "6h",  # Six-hourly
            10800: "3h",  # Three-hourly
            3600: "hr",  # Hourly
            1: "subhr",  # Sub-hourly
        }

    def _get_time_frequency(self, time_delta: int, freq_attr: str = "") -> str:
        if freq_attr in self._time_table.values():
            return freq_attr
        return self._timedelta_to_cmor_frequency(time_delta)

    def _get_metadata(
        self, path: Union[os.PathLike[str], xr.Dataset]
    ) -> Dict[str, Optional[Union[str, List[str], Dict[str, str]]]]:
        """Get metadata from a path or xarray dataset."""
        time_coder = xr.coders.CFDatetimeCoder(use_cftime=True)
        try:
            dset = (
                path
                if isinstance(path, xr.Dataset)
                else xr.open_mfdataset(
                    str(path), parallel=False, decode_times=time_coder, lock=False
                )
            )
            time_freq = dset.attrs.get("frequency", "")
            data_vars = list(map(str, dset.data_vars))
            coords = list(map(str, dset.coords))
            try:
                times = dset["time"].values[:]
            except (KeyError, IndexError, TypeError):
                times = np.array([])

        except Exception as error:
            logger.error("Failed to open data file %s: %s", str(path), error)
            return {}
        if len(times) > 0:
            try:
                try:
                    time_str = (
                        f"[{times[0].isoformat()}Z TO {times[-1].isoformat()}Z]"
                    )
                except (AttributeError, IndexError):
                    time_str = "fx"
                dt = (
                    abs((times[1] - times[0]).total_seconds())
                    if len(times) > 1
                    else 0
                )
            except Exception as non_cftime:
                logger.info(
                    "The time var is not based on the cftime: %s", non_cftime
                )
                time_str = (
                    f"[{np.datetime_as_string(times[0], unit='s')}Z TO "
                    f"{np.datetime_as_string(times[-1], unit='s')}Z]"
                )
                dt = (
                    abs(
                        (times[1] - times[0]).astype("timedelta64[s]").astype(int)
                    )
                    if len(times) > 1
                    else 0
                )
        else:
            time_str = "fx"
            dt = 0

        variables = [
            var
            for var in data_vars
            if var not in coords
            and not any(
                term in var.lower() for term in ["lon", "lat", "bnds", "x", "y"]
            )
            and var.lower() not in ["rotated_pole", "rot_pole"]
        ]

        _data: Dict[str, Optional[Union[str, List[str], Dict[str, str]]]] = {}
        if variables:
            _data.setdefault("variable", variables[0])
        elif data_vars:  # pragma: no cover
            _data.setdefault("variable", data_vars[0])
            logger.info(f"No filtered variables found in {path}, using {data_vars[0]}")
        else:  # pragma: no cover
            _data.setdefault("variable", None)
            logger.warning(f"No data variables found in {path}")
        _data.setdefault(
            "time_frequency", self._get_time_frequency(dt, time_freq)
        )
        _data["time"] = time_str
        _data.setdefault("cmor_table", _data["time_frequency"])
        _data.setdefault("version", "")
        if isinstance(path, Path):
            _data["file"] = str(path)
        if isinstance(path, xr.Dataset):
            _data["file"] = dset.encoding.get("source", None)
        return _data
