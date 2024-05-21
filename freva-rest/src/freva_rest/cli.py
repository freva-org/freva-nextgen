"""Command line interface (cli) for running the rest server.

Configuring
-----------

There are two fundamental different options to configure the service.

1. via the `config` ``.toml`` file.
2. via environment variables.

Note, that the order here is important. First, any configuration from the
config file is loaded, only if the configuration wasn't found in the config
file environment variables are evaluated. The following environment
variables can be set:

- ``DEBUG``: Start server in debug mode (1), (default: 0 -> no debug).
- ``API_PORT``: the port the rest service should be running on (default 8080).
- ``API_WORKER``: the number of multi-process work serving the API (default: 8).
- ``SOLR_HOST``: host name of the solr server, host name and port should be
                 separated by a ``:``, for example ``localhost:8983``
- ``SOLR_CORE`` : name of the solr core that contains datasets with multiple
                  versions
- ``MONGO_HOST``: host name of the mongodb server, where query statistics are
                 stored. Host name and port should separated by a ``:``, for
                 example ``localhost:27017``
- ``MONGO_USER``: user name for the mongodb.
- ``MONGO_PASSWORD``: password to log on to the mongodb.
- ``MONGO_DB``: database name of the mongodb instance.
- ``API_URL``: url of the machine that runs of the rest api
- ``API_CACHE_EXP``: expiry time in seconds of the cached data
- ``API_BROKER_HOST``: Host and port of the message broker url.
                       Host name and port should separated by a ``:``, for
                       example ``localhost:6379``
- ``REDIS_HOST``: Host and port of the redis cache
                  Host name and port should separated by a ``:``, for
                  example ``localhost:5672``
- ``DATA_PORTAL_HOST``: Host name(s) of the data portal worker, use multiple
                        hosts by ',' separation. For example
                        `hostname1,hostname2,hostenam3` will create a cluster
                        of three hosts with `hostname1` being the scheduler and
                        `hostname2` and `hostname3` being the executors.
- ``DATA_PORTAL_USER``: Set the username(s) for the data portal host(s), use
                        multiple user names by ',' separation. For example if
                        you set three host names you can assign each host name
                        to a distinct user name by setting the 
                        `--data-portal-user` flag to `user1,user2,user3`
                        if you have set multiple host names but only assigned
                        one user then all host names will be assigned to that
                        single user name.
- ``DATA_PORTAL_PYTHON_BIN``: Path of the remote machine python executable
                              running the data loader process. To assign
                              multiple paths you can use ',' separators for 
                              example: `/path/1,/path/2,/path/3`

- ``DATA_PORTAL_PASSWD``: Password(s) for the user/host name pairs to log on
                          to the data loader remote machine(s). To assign
                          multiple passwords you can use ',' separators for 
                          example: `passwd1,passwd2,passwd3`
- ``DATA_PORTAL_TMP_DIR``: Path to the directory where temporary data can be 
                           stored. To assign multiple paths you can use ','
                           separators for example `/path1,/path2,/path3`

📝  You can override the path to the default config file using the
    ``API_CONFIG`` environment variable. The default location of this config 
    file is ``/opt/databrowser/api_config.toml``.
"""

import asyncio
import base64
from getpass import getuser
import json
import multiprocessing
import os
from pathlib import Path
from socket import gethostname
import sys
from tempfile import NamedTemporaryFile
from typing import Any, Optional, Tuple

import appdirs
import dask
from paramiko import AutoAddPolicy, SSHClient
from paramiko.ssh_exception import (
    SSHException,
)
from rich.prompt import Prompt
from rich import print as pprint
import typer
import uvicorn

import freva_data_portal
from .logger import logger
from .config import ServerConfig, defaults

cli = typer.Typer(name="freva-rest-server", help=__doc__, epilog=__doc__)


def get_passwd(ask_for_password: bool) -> None:
    """Get the password for the ssh clients."""
    msg = (
        "[bold]Type the SSH pasword(s) now\nmultiple passwords "
        "can be [red]','[/red]separated[/bold]"
    )
    if ask_for_password is False:
        return os.environ.get("DATA_PORTAL_PASSWD")
    return os.environ.get("DATA_PORTAL_PASSWD") or Prompt(msg, password=True)


class DataPortalWorker:
    """Class that starts the data loading portal."""

    proc_stop = multiprocessing.Event()

    def __init__(self, **kwargs: str) -> None:
        worker_urls = (kwargs.get("DATA_PORTAL_HOST") or "localhost").split(
            ","
        )
        usernames = [
            u.strip()
            for u in (kwargs.get("DATA_PORTAL_USER") or getuser()).split(",")
        ]
        usernames.insert(0, usernames[0])
        passwords = [
            p.strip()
            for p in (kwargs.get("DATA_PORTAL_PASSWD") or "").split(",")
        ]
        passwords.insert(0, passwords[0])
        self.redis_host = kwargs.get("REDIS_HOST") or "localhost"
        self.api_url = kwargs.get("API_URL") or "localhost"
        self.api_cache_exp = kwargs.get("API_CACHE_EXP") or "3600"
        broker_host = kwargs.get("API_BROKER_HOST") or "localhost"
        self.ssh_connection_args = {
            "hostname": worker_urls[0].strip(),
            "username": usernames[0].strip(),
            "password": passwords[0].strip() or None,
        }
        self._worker_proc = None
        ssh_workers = [h.strip() for h in worker_urls if h.strip()]
        ssh_workers.insert(0, ssh_workers[0])
        ssh_cluster_config = []
        for num, worker in enumerate(ssh_workers):
            ssh_cluster_config.append(
                {
                    "username": usernames[min(num, len(usernames) - 1)].strip()
                    or None,
                    "password": passwords[min(num, len(passwords) - 1)].strip()
                    or None,
                }
            )
        self.ssh_cluster_config_file = (
            Path(appdirs.user_cache_dir("freva"))
            / "data-portal-cluster-config.json"
        )
        self.ssh_cluster_config_file.parent.mkdir(exist_ok=True, parents=True)
        for prefix in ("http", "https", "tcp"):
            broker_host = broker_host.removeprefix(f"{prefix}://")
        broker_host, _, broker_port = broker_host.partition(":")
        broker_port = broker_port or "5672"
        self.cluster_config = {
            "ssh_config": {
                "hosts": ssh_workers,
                "connect_options": ssh_cluster_config,
            },
            "broker_config": {
                "user": os.environ.get("BROKER_USER", "rabbit"),
                "passwd": os.environ.get("BROKER_PASS", "secret"),
                "host": broker_host,
                "port": int(broker_port),
            },
        }
        self.ssh_cluster_config_file.write_bytes(
            base64.b64encode(json.dumps(self.cluster_config).encode("utf-8"))
        )
        self.ssh_cluster_config_file.chmod(0o600)

    def start(self, python_binary: str = sys.executable) -> None:
        """Start the worker process."""
        cli_file = self._remote_setup(python_binary)
        flags = [
            f"{f}={v}"
            for f, v in (
                ("-e", self.api_cache_exp),
                ("-r", self.redis_host),
                ("-a", self.api_url),
            )
            if v
        ]
        deps = "-d xpublish -d asyncssh -d netcdf4"
        cmd = " ".join([f"{python_binary} -m fades {deps} {cli_file}"] + flags)
        if self._worker_proc is None:
            self._worker_proc = multiprocessing.Process(
                target=self._start,
                args=(cmd, self.proc_stop),
                kwargs=self.ssh_connection_args,
            )
            self._worker_proc.daemon = True
            self._worker_proc.start()

    @staticmethod
    def _copy_file(
        source: str, target: str, mode: int = 0o600, **kwargs: Optional[str]
    ) -> None:
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        ssh_client.load_system_host_keys()
        try:
            ssh_client.connect(**kwargs)
            sftp = ssh_client.open_sftp()
            try:
                sftp.remove(target)
            except FileNotFoundError:
                pass
            sftp.put(source, target)
            sftp.chmod(target, mode)
            sftp.close()
        except SSHException as error:
            logger.error("SSH failure: %s", error)
        finally:
            ssh_client.close()

    @staticmethod
    def _exec_ssh_command(
        command: str, get_ouptput: bool = False, **kwargs: Optional[str]
    ) -> str:
        logger.info("Executing command %s", command)
        output = ""
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        ssh_client.load_system_host_keys()
        try:
            ssh_client.connect(**kwargs)
            ssh_session = ssh_client.get_transport().open_session()
            ssh_session.exec_command(command)
            while not ssh_session.exit_status_ready():
                if ssh_session.recv_ready():
                    stdout = ssh_session.recv(1024).decode("utf-8")
                    if get_ouptput:
                        output += stdout
                    else:
                        pprint(stdout, end="")
                if ssh_session.recv_stderr_ready():
                    pprint(
                        ssh_session.recv_stderr(1024).decode("utf-8"), end=""
                    )

            exit_status = ssh_session.recv_exit_status()
            if exit_status != 0:
                msg = f"{command} failed with status {exit_status}"
                if get_ouptput:
                    raise RuntimeError(msg)
                logger.error(msg)
        except SSHException as error:
            if get_ouptput:
                raise RuntimeError(error) from None
            logger.error("SSH failure: %s", error)
        finally:
            ssh_session.close()
            ssh_client.close()
        return output

    def _remote_setup(self, python_binary: str) -> str:
        """Copy the cli script and setup all dependencies."""
        cli_file = Path(freva_data_portal.__file__).parent / "load_data.py"
        _ = self._exec_ssh_command(
            f"{python_binary} -m pip install --user fades appdirs",
            **self.ssh_connection_args,
        )
        stdout = []
        while not stdout:
            stdout = self._exec_ssh_command(
                f"{python_binary} -c 'import appdirs; print(appdirs.user_cache_dir())'",
                get_ouptput=True,
                **self.ssh_connection_args,
            ).splitlines()
        cache_dir = Path(stdout[-1].strip())
        target_file = str(cache_dir / cli_file.name)
        ssh_config = str(cache_dir / self.ssh_cluster_config_file.name)
        self._copy_file(str(cli_file), target_file, **self.ssh_connection_args)
        self._copy_file(
            str(self.ssh_cluster_config_file),
            ssh_config,
            **self.ssh_connection_args,
        )
        self.ssh_cluster_config_file.unlink()
        return target_file

    @classmethod
    def _start(
        cls,
        command: str,
        event: multiprocessing.Event,
        **connection_kwargs: Optional[str],
    ) -> None:
        num_times = 0
        while not event.is_set():
            logger.info(
                "%sstarting loader process on %s",
                num_times * "re",
                connection_kwargs.get("hostname", "localhost"),
            )
            cls._exec_ssh_command(command, **connection_kwargs)
            num_times = 1

    def __enter__(self) -> "DataPortalWorker":
        return self

    def __exit__(self, *args: Any) -> None:
        if self._worker_proc is not None:
            self.proc_stop.set()
            self._worker_proc.terminate()


@cli.command(name="freva-rest-api")
def start(
    config_file: Optional[Path] = typer.Option(
        os.environ.get("API_CONFIG", defaults["API_CONFIG"]),
        "-c",
        "--config-file",
        help="Path to the server configuration file",
    ),
    port: int = typer.Option(
        os.environ.get("API_PORT", 8080),
        "-p",
        "--port",
        help="The port the api is running on",
    ),
    data_portal_host: str = typer.Option(
        os.environ.get("DATA_PORTAL_HOST", "localhost"),
        "--data-portal-host",
        help=(
            "Host name(s) of the data portal worker, use multiple hosts"
            " by ',' separation. For example `hostname1,hostname2,hostenam3` "
            "will create a cluster of three hosts with `hostname1` being the "
            "scheduler and `hostname2` and `hostname3` being the executors."
        ),
    ),
    data_portal_user: Optional[str] = typer.Option(
        os.environ.get("DATA_PORTAL_USER"),
        "--data-portal-user",
        help=(
            "Set the username(s) for the data portal host(s), use multiple "
            "user names by ',' separation. For example if you set three "
            "host names you can assign each host name to a distinct user "
            "name by setting the data-portal-user flag to `user1,user2,user3`"
            " if you have set multiple host names but only assigned one user "
            " then all host names will be assigned to that single user name."
        ),
    ),
    ask_passwd: bool = typer.Option(
        False,
        "--password",
        help="Ask the password for the ssh login to the data-portal host",
    ),
    scratch_dir: str = typer.Option(
        os.environ.get("DATA_PORTAL_TMP_DIR", "/tmp"),
        "--scratch-dir",
        help=(
            "Path to the directory where temporary data can be stored. "
            " To assign multiple paths you can use ',' separators for example"
            " `/path1,/path2,/path3`"
        ),
    ),
    python_binary: str = typer.Option(
        os.environ.get("DATA_PORTAL_PYTHON_BIN", sys.executable),
        "--python-bin",
        help=(
            "Path of the remote machine python executable running the data "
            "loader process. To assign multiple paths you can use ',' "
            "separators for example: `/path/1,/path/2,/path/3`"
        ),
    ),
    dev: bool = typer.Option(False, help="Add test data to the dev solr."),
    debug: bool = typer.Option(
        bool(int(os.environ.get("DEBUG", 0))), help="Turn on debug mode."
    ),
) -> None:
    """Start the freva rest API."""
    defaults["API_CONFIG"] = (config_file or defaults["API_CONFIG"]).absolute()
    defaults["DEBUG"] = debug
    defaults["API_BROKER_HOST"] = (
        os.environ.get("API_BROKER_HOST") or "localhost:5672"
    )
    defaults["API_CACHE_EXP"] = os.environ.get("API_CACHE_EXP") or "3600"
    defaults["REDIS_HOST"] = (
        os.environ.get("REDIS_HOST") or "redis://localhost:6379"
    )
    defaults["API_URL"] = (
        os.environ.get("API_URL") or f"http://localhost:{port}"
    )
    defaults["PYTHON_BINARY"] = python_binary
    defaults["DATA_PORTAL_HOST"] = data_portal_host
    defaults["DATA_PORTAL_USER"] = data_portal_user or getuser()
    defaults["DATA_PORTAL_PASSWD"] = get_passwd(ask_passwd)
    defaults["DATA_PORTAL_TMP_DIR"] = scratch_dir
    cfg = ServerConfig(defaults["API_CONFIG"], debug=debug)
    if dev:
        from databrowser_api.tests.mock import read_data

        for core in cfg.solr_cores:
            asyncio.run(read_data(core, cfg.solr_host, cfg.solr_port))
    workers = {False: int(os.environ.get("API_WORKER", 8)), True: None}
    with DataPortalWorker(**defaults) as dpw:
        dpw.start(python_binary)
        with NamedTemporaryFile(suffix=".conf", prefix="env") as temp_f:
            Path(temp_f.name).write_text(
                (
                    f"DEBUG={int(debug)}\n"
                    f"API_CONFIG={defaults['API_CONFIG']}\n"
                    f"API_PORT={port}\n"
                    f"API_CACHE_EXP={defaults['API_CACHE_EXP']}\n"
                    f"REDIS_HOST={defaults['REDIS_HOST']}\n"
                    f"API_URL={defaults['API_URL']}\n"
                    f"API_BROKER_HOST={dpw.cluster_config['broker_config']['host']}\n"
                    f"API_BROKER_PORT={dpw.cluster_config['broker_config']['port']}\n"
                    f"API_BROKER_USER={dpw.cluster_config['broker_config']['user']}\n"
                    f"API_BROKER_PASS={dpw.cluster_config['broker_config']['passwd']}\n"
                ),
                encoding="utf-8",
            )
            uvicorn.run(
                "freva_rest.api:app",
                host="0.0.0.0",
                port=port,
                reload=dev,
                log_level=cfg.log_level,
                workers=workers[dev],
                env_file=temp_f.name,
            )


if __name__ == "__main__":
    cli()
