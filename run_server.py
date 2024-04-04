"""Script that runs the API server."""

import os
import sys
from pathlib import Path
from subprocess import Popen

import appdirs


def kill_proc() -> None:
    """Kill a potentially running process."""
    pid_file = Path(appdirs.user_cache_dir("freva")) / "rest-server.pid"
    if pid_file.is_file():
        pid = int(pid_file.read_text())
        try:
            os.kill(pid, 9)
        except ProcessLookupError:
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
