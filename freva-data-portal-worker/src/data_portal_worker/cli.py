"""The freva data loading portal."""

import argparse
import json
import logging
import os
from base64 import b64decode
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Optional, Union

import appdirs
from watchfiles import run_process

from ._version import __version__
from .load_data import CLIENT, ProcessQueue, RedisKw
from .utils import data_logger


def read_file_content(input_file: Optional[Union[str, Path]] = None) -> str:
    """Read the content of a file, if can be written."""
    if input_file is None:
        return ""
    try:
        return Path(input_file).read_text()
    except Exception as error:
        data_logger.warning(
            "Could not read content of file: %s: %s", input_file, error
        )
    return ""


def get_redis_config(
    config_file: Optional[Path] = None,
    redis_password: Optional[str] = None,
    redis_user: Optional[str] = None,
    redis_ssl_certfile: Optional[str] = None,
    redis_ssl_keyfile: Optional[str] = None,
) -> RedisKw:
    """Read redis connection information."""
    file_config = read_file_content(config_file)
    cache_config: dict[str, str] = {}
    if file_config:
        try:
            cache_config = json.loads(b64decode(file_config.encode()))
        except Exception as error:
            data_logger.warning(
                "Could not decode file: %s: %s", config_file, error
            )
    return RedisKw(
        user=cache_config.get("user", redis_user or ""),
        passwd=cache_config.get("passwd", redis_password or ""),
        ssl_key=cache_config.get("ssl_key", read_file_content(redis_ssl_keyfile)),
        ssl_cert=cache_config.get(
            "ssl_cert", read_file_content(redis_ssl_certfile)
        ),
    )


def _main(
    config_file: Optional[Path] = None,
    port: int = 40000,
    exp: int = 3600,
    redis_host: str = "redis://localhost:6379",
    redis_password: Optional[str] = None,
    redis_user: Optional[str] = None,
    redis_ssl_certfile: Optional[str] = None,
    redis_ssl_keyfile: Optional[str] = None,
    dev: bool = False,
) -> None:
    """Run the loader process."""
    data_logger.debug("Loading cluster config from %s", config_file)
    env = os.environ.copy()
    cache_config = get_redis_config(
        config_file=config_file,
        redis_password=redis_password,
        redis_user=redis_user,
        redis_ssl_certfile=redis_ssl_certfile,
        redis_ssl_keyfile=redis_ssl_keyfile,
    )

    try:
        os.environ["DASK_PORT"] = str(port)
        os.environ["API_CACHE_EXP"] = str(exp)
        os.environ["API_REDIS_HOST"] = redis_host
        os.environ["API_REDIS_USER"] = cache_config["user"]
        os.environ["API_REDIS_PASSWORD"] = cache_config["passwd"]
        with TemporaryDirectory() as temp:
            if cache_config["ssl_cert"] and cache_config["ssl_key"]:
                cert_file = Path(temp) / "client-cert.pem"
                key_file = Path(temp) / "client-key.pem"
                cert_file.write_text(cache_config["ssl_cert"])
                key_file.write_text(cache_config["ssl_key"])
                key_file.chmod(0o600)
                cert_file.chmod(0o600)
                os.environ["API_REDIS_SSL_CERTFILE"] = str(cert_file)
                os.environ["API_REDIS_SSL_KEYFILE"] = str(key_file)

            data_logger.debug("Starting data-loader process")
            queue = ProcessQueue(dev_mode=dev)
            queue.run_for_ever("data-portal")
    except KeyboardInterrupt:
        pass
    finally:
        if CLIENT is not None:
            CLIENT.shutdown()  # pragma: no cover
        for handler in logging.root.handlers:
            handler.flush()
            handler.close()
            logging.root.removeHandler(handler)
        os.environ = env  # type: ignore


def run_data_loader(argv: Optional[List[str]] = None) -> None:
    """Daemon that waits for messages to load the data."""
    config_file = os.getenv(
        "API_CONFIG",
        os.path.join(
            appdirs.user_cache_dir("freva"), "data-portal-cluster-config.json"
        ),
    )

    redis_host, _, redis_port = (
        (os.environ.get("API_REDIS_HOST") or "localhost")
        .replace("redis://", "")
        .partition(":")
    )
    redis_port = redis_port or "6379"
    parser = argparse.ArgumentParser(
        prog="Data Loader",
        description=("Starts the data loading service."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-c",
        "--config-file",
        help="Path to the config file.",
        type=Path,
        default=config_file,
    )
    parser.add_argument(
        "-e",
        "--exp",
        type=int,
        help="Set the expiry time of the redis cache.",
        default=os.environ.get("API_CACHE_EXP") or "3600",
    )
    parser.add_argument(
        "-r",
        "--redis-host",
        type=str,
        help="Host:Port of the redis cache.",
        default=f"redis://{redis_host}:{redis_port}",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        help="Dask scheduler port for loading data.",
        default=os.getenv("API_PORT", "40000"),
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Development mode",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Display debug messages.",
        default=False,
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version="%(prog)s {version}".format(version=__version__),
    )
    parser.add_argument(
        "--redis-username",
        type=str,
        help="User name for redis connection.",
        default=os.getenv("API_REDIS_USER"),
    )
    parser.add_argument(
        "--redis-password",
        type=str,
        help="Password for redis connection.",
        default=os.getenv("API_REDIS_PASSWORD"),
    )
    parser.add_argument(
        "--redis-ssl-keyfile",
        type=Path,
        help="Path to the private redis key file.",
        default=os.getenv("API_REDIS_SSL_KEYFILE"),
    )
    parser.add_argument(
        "--redis-ssl-certfile",
        type=Path,
        help="Path to the public redis cert file.",
        default=os.getenv("API_REDIS_SSL_CERTFILE"),
    )
    args = parser.parse_args(argv)
    if args.verbose is True:
        data_logger.setLevel(logging.DEBUG)
    kwargs = {
        "port": args.port,
        "exp": args.exp,
        "redis_host": args.redis_host,
        "dev": args.dev,
        "redis_password": args.redis_password,
        "redis_user": args.redis_username,
        "redis_ssl_certfile": args.redis_ssl_certfile,
        "redis_ssl_keyfile": args.redis_ssl_keyfile,
    }
    if args.dev:
        run_process(
            Path(__file__).parent,
            target=_main,
            args=(args.config_file.expanduser(),),
            kwargs=kwargs,
        )
    else:
        _main(args.config_file.expanduser(), **kwargs)
