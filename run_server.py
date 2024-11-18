"""Script that runs the API server."""

import argparse
import json
import os
import sys
import tempfile
import time
import urllib.request
from base64 import b64encode
from pathlib import Path
from subprocess import Popen, run

REDIS_CONFIG = {
    "user": "redis",
    "passwd": "secret",
    "host": "redis://localhost:6379",
    "ssl_cert": "",
    "ssl_key": "",
    "scheduler_host": "localhost:4000",
    "cache_exp": 86000,
}
TEMP_DIR = Path(
    os.getenv("TEMP_DIR", os.path.join(tempfile.gettempdir(), "freva-nextgen"))
)
TEMP_DIR.mkdir(exist_ok=True, parents=True)
OIDC_URL = os.getenv(
    "OIDC_URL",
    "http://localhost:8080/realms/freva/.well-known/openid-configuration",
)


def prep_server(inp_dir: Path) -> None:
    """Prepare the first server startup."""
    cert_dir = inp_dir / "certs"
    cert_dir.mkdir(exist_ok=True, parents=True)
    key_file = cert_dir / "client-key.pem"
    cert_file = cert_dir / "client-cert.pem"
    if not key_file.is_file() or not cert_file.is_file():
        run(
            [sys.executable, str(inp_dir / "dev-utils.py"), "gen-certs"],
            check=True,
        )
    REDIS_CONFIG["ssl_key"] = key_file.read_text()
    REDIS_CONFIG["ssl_cert"] = cert_file.read_text()
    config_file = TEMP_DIR / "data-portal-cluster-config.json"
    config_file.write_bytes(b64encode(json.dumps(REDIS_CONFIG).encode("utf-8")))


def kill_proc(proc: str) -> None:
    """Kill a process."""
    run(
        [
            sys.executable,
            os.path.join("dev-env", "config", "dev-utils.py"),
            "kill",
            f'{TEMP_DIR / f"{proc}.pid"}',
        ],
        check=True,
    )


def start_server(inp_dir: Path, foreground: bool = False, *args: str) -> None:
    """Set up the server"""
    for proc in ("rest-server", "data-portal"):
        kill_proc(proc)
    prep_server(inp_dir)
    config_file = TEMP_DIR / "data-portal-cluster-config.json"
    args += ("--redis-ssl-certdir", str(inp_dir.absolute() / "certs"))
    python_exe = sys.executable
    portal_pid = TEMP_DIR / "data-portal.pid"
    rest_pid = TEMP_DIR / "rest-server.pid"
    try:
        portal_proc = Popen(
            [
                python_exe,
                "-m",
                "data_portal_worker",
                "-v",
                "--dev",
                "-c",
                f"{config_file}",
            ]
        )
        rest_proc = Popen([python_exe, "-m", "freva_rest.cli"] + list(args))
        portal_pid.write_text(str(portal_proc.pid))
        rest_pid.write_text(str(rest_proc.pid))
        if foreground:
            portal_proc.communicate()
            rest_proc.communicate()
    except KeyboardInterrupt:
        portal_proc.kill()
        portal_pid.unlink()
        rest_proc.kill()
        rest_pid.unlink()


def cli() -> None:
    """Setup a cli."""
    parser = argparse.ArgumentParser(
        description=("Start the dev restAPI."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--script-dir",
        type=Path,
        help="The the input directory all scripts are located.",
        default=Path(__file__).parent / "dev-env" / "config",
    )
    parser.add_argument(
        "--kill",
        "-k",
        help="Kill any running processes.",
        action="store_true",
    )
    parser.add_argument(
        "--foreground",
        "-f",
        help="Start service in the foreground",
        action="store_true",
    )
    args, server_args = parser.parse_known_args()
    if args.kill:
        for proc in ("rest-server", "data-portal"):
            kill_proc(proc)
        return
    run(
        [
            sys.executable,
            os.path.join("dev-env", "config", "dev-utils.py"),
            "oidc",
            OIDC_URL,
        ],
        check=True,
    )
    start_server(args.script_dir, args.foreground, *server_args)


if __name__ == "__main__":
    cli()
