"""Script that runs the API server."""

import argparse
import json
import os
import sys
import time
from base64 import b64encode
from pathlib import Path
from subprocess import Popen, run
import sys
import tempfile


REDIS_CONFIG = {
    "user": "redis",
    "passwd": "secret",
    "host": "redis://localhost:6379",
    "ssl_cert": "",
    "ssl_key": "",
}

TEMP_DIR = Path(tempfile.gettempdir()) / "freva-nextgen"
TEMP_DIR.mkdir(exist_ok=True, parents=True)


def kill_proc(proc: str) -> None:
    """Kill a potentially running process."""
    pid_file = TEMP_DIR / f"{proc}.pid"
    if pid_file.is_file():
        try:
            pid = int(pid_file.read_text())
            os.kill(pid, 15)
            time.sleep(2)
            if os.waitpid(pid, os.WNOHANG) == (0, 0):  # check running
                os.kill(pid, 9)
        except (ProcessLookupError, ValueError):
            pass


def prep_server() -> None:
    """Prepare the first server startup."""
    run(
        [sys.executable, "-m", "pip", "install", "tox", "cryptography"],
        check=True,
    )
    run([sys.executable, "-m", str(Path("dev-env") / "keys.py")], check=True)


def start_server(foreground: bool = False, *args: str) -> None:
    """Set up the server"""
    for proc in ("rest-server", "data-portal"):
        kill_proc(proc)
    key_file = Path("dev-env") / "certs" / "client-key.pem"
    cert_file = Path("dev-env") / "certs" / "client-cert.pem"
    cert_file.parent.mkdir(exist_ok=True, parents=True)
    if not key_file.is_file() or not cert_file.is_file():
        prep_server()
    REDIS_CONFIG["ssl_key"] = (Path("dev-env") / "certs" / "client-key.pem").read_text()
    REDIS_CONFIG["ssl_cert"] = (
        Path("dev-env") / "certs" / "client-cert.pem"
    ).read_text()
    (TEMP_DIR / "data-portal-cluster-config.json").write_bytes(
        b64encode(json.dumps(REDIS_CONFIG).encode("utf-8"))
    )
    args += ("--cert-dir", str(Path("dev-env").absolute() / "certs"))
    python_exe = sys.executable
    portal_pid = TEMP_DIR / "data-portal.pid"
    rest_pid = TEMP_DIR / "rest-server.pid"
    try:
        portal_proc = Popen([python_exe, "-m", "data_portal_worker", "-v", "--dev"])
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
        "--gen-certs",
        action="store_true",
        help="Generate a pair of self-signed certificats.",
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
    if args.gen_certs:
        with Popen([sys.executable, str(Path("dev-env").absolute() / "keys.py")]):
            return
    if args.kill:
        for proc in ("rest-server", "data-portal"):
            kill_proc(proc)
        return
    start_server(args.foreground, *server_args)


if __name__ == "__main__":
    cli()
