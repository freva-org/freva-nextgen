"""The freva data loading portal."""

import argparse
import json
import logging
import os
import signal
import time
from base64 import b64decode
from multiprocessing import Process
from pathlib import Path
from tempfile import TemporaryDirectory
from types import FrameType
from typing import Any, List, Optional, Union

import appdirs

from ._version import __version__
from .load_data import ProcessQueue, RedisKw
from .utils import DEFAULT_LOG_LEVEL, data_logger, logger_handlers


def _sigterm_handler(signum: int, frame: Optional[FrameType]) -> None:
    raise KeyboardInterrupt("SIGTERM received")


def read_file_content(input_file: Optional[Union[str, Path]] = None) -> str:
    """Read the content of a file, if can be written."""
    if input_file is None:
        return ""
    try:
        return Path(input_file).read_text()
    except Exception as error:
        data_logger.warning("Could not read content of file: %s: %s", input_file, error)
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
            data_logger.warning("Could not decode file: %s: %s", config_file, error)
    return RedisKw(
        user=cache_config.get("user", redis_user or ""),
        passwd=cache_config.get("passwd", redis_password or ""),
        ssl_key=cache_config.get("ssl_key", read_file_content(redis_ssl_keyfile)),
        ssl_cert=cache_config.get("ssl_cert", read_file_content(redis_ssl_certfile)),
    )


def _main(
    config_file: Optional[Path] = None,
    exp: int = 3600,
    redis_host: str = "redis://localhost:6379",
    redis_password: Optional[str] = None,
    redis_user: Optional[str] = None,
    redis_ssl_certfile: Optional[str] = None,
    redis_ssl_keyfile: Optional[str] = None,
    log_level: str = "WARNING",
) -> None:
    """Run the loader process."""
    try:
        signal.signal(signal.SIGTERM, _sigterm_handler)
    except Exception as error:
        data_logger.warning(error)
    data_logger.debug("Loading cluster config from %s", config_file)
    cache_config = get_redis_config(
        config_file=config_file,
        redis_password=redis_password,
        redis_user=redis_user,
        redis_ssl_certfile=redis_ssl_certfile,
        redis_ssl_keyfile=redis_ssl_keyfile,
    )
    os.environ["API_LOGLEVEL"] = log_level
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
        with ProcessQueue() as q:
            q.run_for_ever("data-portal")


def _set_loglevel_from_verbosity(level: int = 0) -> str:
    _levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    _level = _levels[max(min(level, 2), 0)] if level else DEFAULT_LOG_LEVEL
    data_logger.setLevel(_level)
    for hdl in logger_handlers:
        hdl.setLevel(_level)  # type: ignore [attr-defined]
    return logging.getLevelName(data_logger.level)


def _load_wrapper(config_file: Path, **kwargs: Any) -> None:
    from watchfiles import run_process

    try:
        run_process(
            Path(__file__).parent, target=_main, args=(config_file,), kwargs=kwargs
        )
    finally:
        for handler in logging.root.handlers:
            handler.flush()
            handler.close()
            logging.root.removeHandler(handler)


def daemon(config_file: Path, num_proc: int, **kwargs: Any) -> None:
    """Run the main function as a daemon with retry strategy."""
    data_logger.info("Starting data-loading daemon")
    processes: List[Optional[Process]] = [
        Process(
            target=_main,
            args=(config_file.expanduser(),),
            kwargs=kwargs,
            daemon=True,
            name=f"data-portal-worker-{i}",
        )
        for i in range(num_proc)
    ]
    for p in processes:
        if p is not None:
            p.start()
    try:
        # Restart any worker that dies unexpectedly
        while not all(p is None for p in processes):
            for i, p in enumerate(processes):
                if p is None or p.is_alive():
                    continue
                if p.exitcode == 0:
                    data_logger.info("Worker %s exited cleanly", p.name)
                    processes[i] = None
                else:
                    data_logger.warning(
                        "Worker %s died (exit %s), restarting", p.name, p.exitcode
                    )
                    new = Process(
                        target=_main,
                        args=(config_file.expanduser(),),
                        kwargs=kwargs,
                        daemon=True,
                        name=p.name,
                    )
                    new.start()
                    processes[i] = new
            time.sleep(5)
    except KeyboardInterrupt:
        for p in processes:
            if p is not None:
                p.terminate()
                p.join(timeout=10)
        for p in processes:
            if p is not None and p.is_alive():
                data_logger.warning("Worker %s did not terminate, killing", p.name)
                p.kill()
                p.join(timeout=3)
    finally:
        for handler in logging.root.handlers:
            handler.flush()
            handler.close()
            logging.root.removeHandler(handler)


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
        "--dev",
        action="store_true",
        help="Development mode",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity",
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
    parser.add_argument(
        "--num-proc",
        "-p",
        type=int,
        default=max(1, min(8, int(os.getenv("API_NUM_PROCS", str(os.cpu_count()))))),
    )
    args = parser.parse_args(argv)
    log_level = _set_loglevel_from_verbosity(args.verbose)
    kwargs = {
        "exp": args.exp,
        "redis_host": args.redis_host,
        "dev": args.dev,
        "redis_password": args.redis_password,
        "redis_user": args.redis_username,
        "redis_ssl_certfile": args.redis_ssl_certfile,
        "redis_ssl_keyfile": args.redis_ssl_keyfile,
        "log_level": log_level,
    }
    nprocs = args.num_proc if args.dev is False else 1
    if args.dev:
        _load_wrapper(args.config_file.expanduser(), **kwargs)
    else:
        daemon(args.config_file.expanduser(), nprocs, **kwargs)
