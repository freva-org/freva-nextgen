"""Script that runs the API server."""

import os
from pathlib import Path
import time
from subprocess import Popen
import sys

import appdirs


def kill_proc() -> None:
    """Kill a potentially running process."""
    pid_file = Path(appdirs.user_cache_dir("freva")) / "rest-server.pid"
    if pid_file.is_file():
        try:
            pid = int(pid_file.read_text())
            os.kill(pid, 15)
            time.sleep(2)
            if os.waitpid(pid, os.WNOHANG) == (0, 0):  # check running
                os.kill(pid, 9)
        except (ProcessLookupError, ValueError):
            pass


def run_in_background() -> None:
    """Set up the server in the background."""
    kill_proc()
    pid_file = Path(appdirs.user_cache_dir("freva")) / "rest-server.pid"
    pid_file.parent.mkdir(exist_ok=True, parents=True)
    proc = Popen(sys.argv[1:])
    pid_file.write_text(str(proc.pid))


if __name__ == "__main__":
    if "kill" in [s.strip("-").lower() for s in sys.argv[1:]]:
        kill_proc()
    else:
        run_in_background()
