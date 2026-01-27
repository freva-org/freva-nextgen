"""Pytest configuration settings.

This conftest configuration file for all tests in this mulit repository
It defines a number of fixtures and helper functions used throughout the test
suite. Notably, it resets loggers to a sensible level before starting the REST
server, spawns a loader worker process, prepares temporary configuration files,
provides helper factories for authentication tokens, and constructs sample
payloads for user data tests.

The fixtures are organised with different scopes (function, module, session)
depending on how long their resources should persist. See each fixture’s
docstring for details.
"""

import asyncio
import datetime
import json
import logging
import os
import socket
import threading
import time
from base64 import b64encode
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Dict, Iterator

import jwt
import mock
import pymongo
import pytest
import requests
import uvicorn
from typer.testing import CliRunner

from data_portal_worker.cli import _main as run_data_loader
from data_portal_worker.load_data import RedisCacheFactory as Cache
from freva_client.auth import Auth
from freva_client.utils import logger
from freva_client.utils.auth_utils import TOKEN_ENV_VAR, Token
from freva_rest.api import app
from freva_rest.config import ServerConfig
from freva_rest.databrowser_api.mock import read_data
from freva_rest.logger import reset_loggers


def load_data() -> None:
    """Create a valid server config and populate the Solr cores with mock data."""
    conf = ServerConfig(debug=True)
    for core in conf.solr_cores:
        asyncio.run(read_data(core, conf.solr_url))


def run_test_server(port: int) -> None:
    """Start a test server using uvicorn.

    The server is started with the logging level set to WARNING in order to
    suppress verbose output from the underlying application. The Solr batch
    size and proxy are mocked to use a local endpoint.
    """
    logging_config = uvicorn.config.LOGGING_CONFIG
    logger.setLevel(logging.ERROR)
    reset_loggers(logging.ERROR)
    logging_config["loggers"]["uvicorn"]["level"] = "ERROR"
    logging_config["loggers"]["uvicorn.error"]["level"] = "ERROR"
    logging_config["loggers"]["uvicorn.access"]["level"] = "ERROR"

    with mock.patch("freva_rest.databrowser_api.endpoints.Solr.batch_size", 3):
        with mock.patch(
            "freva_rest.databrowser_api.endpoints.server_config.proxy",
            f"http://localhost:{port}",
        ):
            load_data()
            uvicorn.run(
                app,
                host="0.0.0.0",
                port=port,
                reload=False,
                workers=None,
                log_level=logging.ERROR,
                log_config=logging_config,
            )


def mock_token_data(
    valid_for: int = 3600,
    refresh_for: int = 7200,
) -> Token:
    """Generate a mock access and refresh token valid for testing purposes."""
    now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    token_data = {
        "result": "test_access_token",
        "exp": now + valid_for,
        "iat": now + valid_for,
        "auth_time": now,
        "aud": ["freva", "account"],
        "realm_access": {"groups": ["/foo"]},
    }
    access_token = jwt.encode(token_data, "PyJWK")
    return Token(
        access_token=access_token,
        token_type="Bearer",
        expires=now + valid_for,
        refresh_token="test_refresh_token",
        refresh_expires=now + refresh_for,
        scope="profile email address",
        # The token headers are intentionally not set here; tests that
        # require headers explicitly add them to requests.
    )


def find_free_port() -> int:
    """Get a free port where we can start the test server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def get_data_loader_config() -> bytes:
    """Create the config for the data-loader process.

    The configuration is base64 encoded JSON containing Redis connection
    parameters such as user, password, host and SSL certificate paths.
    """
    return b64encode(
        json.dumps(
            {
                "user": os.getenv("API_REDIS_USER", ""),
                "passwd": os.getenv("API_REDIS_PASSWORD", ""),
                "host": os.getenv("API_REDIS_HOST", ""),
                "ssl_cert": Path(
                    os.getenv("API_REDIS_SSL_CERTFILE", "")
                ).read_text(),
                "ssl_key": Path(
                    os.getenv("API_REDIS_SSL_KEYFILE", "")
                ).read_text(),
            }
        ).encode("utf-8")
    )


def run_loader_process(port: int) -> None:
    """Start the data loader process on the given port."""
    with NamedTemporaryFile(suffix=".json") as temp_f:
        Path(temp_f.name).write_bytes(get_data_loader_config())
        run_data_loader(Path(temp_f.name), port=port, dev=True)


def shutdown_data_loader() -> None:
    """Publish a shutdown message to the cache and flush it."""
    cache = Cache()
    for _ in range(5):
        cache.publish(
            "data-portal", json.dumps({"shutdown": True}).encode("utf-8")
        )
        time.sleep(1)
    cache.flushdb()


def _prep_env(**config: str) -> Dict[str, str]:
    """Prepare a clean environment dictionary for use in tests."""
    env = os.environ.copy()
    config = config or {}
    for key in ("FREVA_CONFIG", "EVALUATION_SYSTEM_CONFIG_FILE"):
        _ = env.pop(key, "")
    for key, value in config.items():
        env[key] = value
    return env


def setup_server() -> Iterator[str]:
    """Start the test server and the data loader process.

    Two threads are started: one for the REST API server and one for the
    data loader worker. After both are running, the fixture yields the
    server base URL. Upon teardown, both threads are allowed to exit.
    """
    port = find_free_port()
    cache = Cache()
    cache.flushdb()
    thread1 = threading.Thread(target=run_test_server, args=(port,))
    thread1.daemon = True
    thread1.start()
    time.sleep(1)
    thread2 = threading.Thread(
        target=run_loader_process, args=(find_free_port(),)
    )
    thread2.daemon = True
    thread2.start()
    time.sleep(5)
    yield f"http://localhost:{port}/api/freva-nextgen"


@pytest.fixture(scope="function", autouse=True)
def user_cache_dir() -> Iterator[str]:
    """Mock the default user token file for each test.

    A temporary file is created and the TOKEN_ENV_VAR environment variable is
    patched to point to it. The test can then write tokens into this file.
    """
    with NamedTemporaryFile(suffix=".json") as temp_f:
        with mock.patch.dict(
            os.environ, {TOKEN_ENV_VAR: temp_f.name}, clear=False
        ):
            yield temp_f.name


@pytest.fixture(scope="function")
def temp_dir() -> Iterator[Path]:
    """Create a temporary working directory for tests that require files."""
    cwd = Path.cwd()
    with TemporaryDirectory() as temp_dir:
        os.chdir(temp_dir)
        yield Path(temp_dir)
    os.chdir(cwd)


@pytest.fixture(scope="session")
def loader_config() -> Iterator[bytes]:
    """Provide the base64 encoded Redis loader configuration to tests."""
    yield get_data_loader_config()


@pytest.fixture(scope="function")
def auth_instance() -> Iterator[Auth]:
    """Provide a fresh Auth instance for each test."""
    auth = Auth()
    auth._auth_token = mock_token_data()
    try:
        yield auth
    finally:
        auth._auth_token = None


@pytest.fixture(scope="function")
def cli_runner() -> Iterator[CliRunner]:
    """Set up a CLI test runner and reset the CLI log handlers after use."""
    yield CliRunner()
    logger.reset_cli()


@pytest.fixture(scope="function")
def valid_freva_config() -> Iterator[Path]:
    """Mock a valid freva configuration directory."""
    with mock.patch.dict(os.environ, _prep_env(), clear=True):
        with TemporaryDirectory() as temp_dir:
            freva_config = Path(temp_dir) / "share" / "freva" / "freva.toml"
            freva_config.parent.mkdir(exist_ok=True, parents=True)
            freva_config.write_text(
                "[freva]\nhost = 'https://www.freva.com:80/api'\ndefault_flavour = 'cmip6'\n"
            )
            yield Path(temp_dir)


@pytest.fixture(scope="function")
def invalid_freva_conf_file() -> Iterator[Path]:
    """Mock a broken freva configuration file."""
    with TemporaryDirectory() as temp_dir:
        freva_config = Path(temp_dir) / "share" / "freva" / "freva.toml"
        freva_config.parent.mkdir(parents=True)
        with mock.patch.dict(
            os.environ,
            _prep_env(FREVA_CONFIG=str(freva_config)),
            clear=True,
        ):
            freva_config.write_text("[freva]\nhost = https://freva_conf/api")
            yield freva_config


@pytest.fixture(scope="function")
def valid_freva_config_commented_host() -> Iterator[Path]:
    """Mock a valid freva config where the host key is commented out."""
    with mock.patch.dict(os.environ, _prep_env(), clear=True):
        with TemporaryDirectory() as temp_dir:
            freva_config = Path(temp_dir) / "share" / "freva" / "freva.toml"
            freva_config.parent.mkdir(exist_ok=True, parents=True)
            freva_config.write_text(
                "[freva]\n # host = 'https://www.freva.com:80/api'\ndefault_flavour = 'cmip6'\n"
            )
            yield freva_config


@pytest.fixture(scope="function")
def free_port() -> Iterator[int]:
    """Provide a free port for network bound tests."""
    yield find_free_port()


@pytest.fixture(scope="function")
def valid_eval_conf_file() -> Iterator[Path]:
    """Mock a valid evaluation system configuration file."""
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
    """Mock an invalid evaluation system configuration file."""
    with TemporaryDirectory() as temp_dir:
        eval_file = Path(temp_dir) / "eval.conf"
        eval_file.write_text(
            "[foo]\n" "solr.host = http://localhost\n" "databrowser.port = 8080"
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


@pytest.fixture(scope="module")
def cfg() -> Iterator[ServerConfig]:
    """Create and initialise a valid server configuration."""
    conf = ServerConfig(debug=True)
    load_data()
    yield conf


@pytest.fixture(scope="session", autouse=True)
def test_server() -> Iterator[str]:
    """Start a test server and ensure proper cleanup afterwards."""
    env = os.environ.copy()
    for key in (
        "REDIS_HOST",
        "REDIS_USER",
        "REDIS_SSL_KEYFILE",
        "REDIS_SSL_CERTFILE",
    ):
        env[key] = os.getenv(f"API_{key}", "")
    env["REDIS_PASS"] = os.getenv("API_REDIS_PASSWORD", "")
    with mock.patch.dict(os.environ, env, clear=True):
        # with mock.patch(
        #    "data_portal_worker.backends.posix_and_cloud.dask.config.set",
        #    return_value=None,
        # ):
        yield from setup_server()
        shutdown_data_loader()


@pytest.fixture(scope="function")
def flavour_server(test_server: str) -> Iterator[str]:
    """Set up a temporary MongoDB collection for flavours and clean up afterwards."""

    server_config = ServerConfig()
    mongo_client = pymongo.MongoClient(server_config.mongo_url)
    col = mongo_client[server_config.mongo_db]["custom_flavours"]
    original_docs = list(col.find({}))
    try:
        col.delete_many({})
        yield test_server
    finally:
        col.delete_many({})
        if original_docs:
            col.insert_many(original_docs)


@pytest.fixture(scope="function")
def auth(test_server: str):
    """Factory to create auth tokens for different user types."""

    def _create_auth(user_type: str = "user") -> Token:
        from getpass import getuser

        server_config = ServerConfig()
        user_configs = {
            "user": {"username": "janedoe", "password": "janedoe123"},
            "admin": {"username": getuser(), "password": "secret"},
        }
        config = user_configs.get(user_type, user_configs["user"])
        data = {
            "client_id": server_config.oidc_client_id or "",
            "client_secret": server_config.oidc_client_secret or "",
            "grant_type": "password",
            **config,
        }
        res = requests.post(
            server_config.oidc_overview["token_endpoint"],
            data={k: v for (k, v) in data.items() if v.strip()},
        )
        return _build_token(res.json())

    user_token = _create_auth("user")
    admin_token = _create_auth("admin")

    return {**user_token, "admin": admin_token}


def _build_token(token_data: dict) -> Token:
    """Helper to build a Token object from a JSON response."""
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    expires_at = (
        token_data.get("exp")
        or token_data.get("expires")
        or token_data.get("expires_at")
        or now + token_data.get("expires_in", 180)
    )
    refresh_expires_at = (
        token_data.get("refresh_exp")
        or token_data.get("refresh_expires")
        or token_data.get("refresh_expires_at")
        or now + token_data.get("refresh_expires_in", 180)
    )

    return Token(
        access_token=token_data["access_token"],
        token_type=token_data["token_type"],
        expires=int(expires_at),
        refresh_token=token_data["refresh_token"],
        refresh_expires=int(refresh_expires_at),
        scope=token_data["scope"],
        headers={
            "Authorization": f"{token_data['token_type']} {token_data['access_token']}"
        },
    )


@pytest.fixture(scope="function")
def token_file(auth: Token) -> Path:
    """Dump the auth token into a temporary JSON file and return its path."""
    with NamedTemporaryFile(suffix=".json") as temp_f:
        out_f = Path(temp_f.name)
        out_f.write_text(json.dumps(auth))
        yield out_f


@pytest.fixture(scope="function")
def user_data_payload_sample() -> Dict[str, list[str]]:
    """Create a sample user data payload for successful insertion tests."""
    return {
        "user_metadata": [
            {
                "variable": "ua",
                "time_frequency": "mon",
                "time": "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                "cmor_table": "mon",
                "version": "",
                "file": (
                    "freva-rest/src/freva_rest/databrowser_api/mock/data/model"
                    "/global/cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip"
                    "/r2i1p1f1/Amon/ua/gn/v20190815/"
                    "ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc"
                ),
            },
            {
                "variable": "ua",
                "time_frequency": "mon",
                "time": "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                "cmor_table": "mon",
                "version": "",
                "file": (
                    "freva-rest/src/freva_rest/databrowser_api/mock/data/model/"
                    "global/cmip6/CMIP6/CMIP/CSIRO-ARCCSS/ACCESS-CM2/"
                    "amip/r1i1p1f1/Amon/ua/gn/v20201108/"
                    "ua_Amon_ACCESS-CM2_amip_r1i1p1f1_gn_197901-201412.nc"
                ),
            },
            {
                "variable": "ua",
                "time_frequency": "mon",
                "time": "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                "cmor_table": "mon",
                "version": "",
                "file": (
                    "freva-rest/src/freva_rest/databrowser_api/mock/data/model/"
                    "global/cmip6/CMIP6/CMIP/CSIRO-ARCCSS/ACCESS-CM2/"
                    "amip/r1i1p1f1/Amon/ua/gn/v20191108/"
                    "ua_Amon_ACCESS-CM2_amip_r1i1p1f1_gn_197001-201512.nc"
                ),
            },
            {
                "variable": "pr",
                "time_frequency": "3h",
                "time": "[2007-01-02T01:30:00Z TO 2007-01-02T04:30:00Z]",
                "cmor_table": "3h",
                "version": "",
                "file": (
                    "freva-rest/src/freva_rest/databrowser_api/mock/data/model/"
                    "regional/cordex/output/EUR-11/GERICS/NCC-NorESM1-M/"
                    "rcp85/r1i1p1/GERICS-REMO2015/v1/3hr/pr/v20181212/"
                    "pr_EUR-11_NCC-NorESM1-M_rcp85_r1i1p1_GERICS-REMO2015"
                    "_v2_3hr_200701020130-200701020430.nc"
                ),
            },
            {
                "variable": "tas",
                "time_frequency": "day",
                "time": "[1949-12-01T12:00:00Z TO 1949-12-10T12:00:00Z]",
                "cmor_table": "day",
                "version": "",
                "file": (
                    "freva-rest/src/freva_rest/databrowser_api/mock/data/model/"
                    "regional/cordex/output/EUR-11/CLMcom/"
                    "MPI-M-MPI-ESM-LR/historical/r0i0p0/"
                    "CLMcom-CCLM4-8-17/v1/fx/orog/v20140515/"
                    "orog_EUR-11_MPI-M-MPI-ESM-LR_historical_r1i1p1_"
                    "CLMcom-CCLM4-8-17_v1_fx.nc"
                ),
            },
            {
                "variable": "tas",
                "time_frequency": "day",
                "time": "[1949-12-01T12:00:00Z TO 1949-12-10T12:00:00Z]",
                "cmor_table": "day",
                "version": "",
                "file": (
                    "freva-rest/src/freva_rest/databrowser_api/mock/data/model/"
                    "regional/cordex/output/EUR-11/CLMcom/"
                    "MPI-M-MPI-ESM-LR/historical/r1i1p1/"
                    "CLMcom-CCLM4-8-17/v1/daypt/tas/v20140515/"
                    "tas_EUR-11_MPI-M-MPI-ESM-LR_historical_r1i1p1_"
                    "CLMcom-CCLM4-8-17_v1_daypt_194912011200-194912101200.nc"
                ),
            },
        ],
        "facets": {"project": "cmip5", "experiment": "myFavExp"},
    }


@pytest.fixture(scope="function")
def user_data_payload_sample_partially_success() -> Dict[str, list[str]]:
    """Create a sample user data payload where one file entry is missing."""
    return {
        "user_metadata": [
            {
                "variable": "ua",
                "time_frequency": "mon",
                "time": "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                "cmor_table": "mon",
                "version": "",
                "file": (
                    "freva-rest/src/freva_rest/databrowser_api/mock/data/model/"
                    "global/cmip6/CMIP6/CMIP/MPI-M/MPI-ESM1-2-LR/amip/"
                    "r2i1p1f1/Amon/ua/gn/v20190815/"
                    "ua_mon_MPI-ESM1-2-LR_amip_r2i1p1f1_gn_197901-199812.nc"
                ),
            },
            {
                "variable": "ua",
                "time_frequency": "mon",
                "time": "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                "cmor_table": "mon",
                "version": "",
                "file": "",
            },
            {
                "variable": "tas",
                "time_frequency": "day",
                "time": "fx",
                "file": (
                    "freva-rest/src/freva_rest/databrowser_api/mock/data/model/"
                    "regional/cordex/output/EUR-11/CLMcom/"
                    "MPI-M-MPI-ESM-LR/historical/r0i0p0/"
                    "CLMcom-CCLM4-8-17/v1/fx/orog/v20140515/"
                    "orog_EUR-11_MPI-M-MPI-ESM-LR_historical_r1i1p1_"
                    "CLMcom-CCLM4-8-17_v1_fx.nc"
                ),
            },
        ],
        "facets": {"project": "cmip5", "experiment": "myFavExp"},
    }
