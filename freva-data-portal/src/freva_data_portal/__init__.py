"""The freva data loading portal."""

import argparse
from base64 import b64decode
import json
import logging
import os
from pathlib import Path

import appdirs

from .utils import data_logger
from .load_data import CLIENT, DataLoaderConfig, ProcessQueue

__version__ = "2405.0.0"


def data_loader() -> None:
    """Daemon that waits for messages to load the data."""
    config_file = (
        Path(appdirs.user_cache_dir()) / "data-portal-cluster-config.json"
    )

    redis_host, _, redis_port = (
        (os.environ.get("REDIS_HOST") or "localhost")
        .replace("redis://", "")
        .partition(":")
    )
    redis_port = redis_port or "6379"
    parser = argparse.ArgumentParser(
        prog="Data Loder",
        description=("Starts the data loading service."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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
        "-a",
        "--api-url",
        type=str,
        help="Host:Port for the databrowser api",
        default=os.environ.get("API_URL"),
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        help="API port, only used if --api-host not set.",
        default=8080,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Display debug messages.",
        default=False,
    )
    args = parser.parse_args()
    if args.verbose is True:
        data_logger.setLevel(logging.DEBUG)
    data_logger.debug("Loading cluster config from %s", config_file)
    config: DataLoaderConfig = json.loads(b64decode(config_file.read_bytes()))
    data_logger.debug("Deleting cluster config file %s", config_file)
    env = os.environ.copy()
    try:
        os.environ["API_CACHE_EXP"] = str(args.exp)
        os.environ["REDIS_HOST"] = args.redis_host
        data_logger.debug("Starting data-loader process")
        broker = ProcessQueue(
            args.api_url or f"http://localhost:{args.port}",
            config["ssh_config"],
        )
        broker.run_for_ever(
            "data-portal",
            config["broker_config"],
        )
    except KeyboardInterrupt:
        pass
    finally:
        if CLIENT is not None:
            CLIENT.shutdown()
        os.environ = env
