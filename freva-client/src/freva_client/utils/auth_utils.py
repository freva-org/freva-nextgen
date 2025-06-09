"""Helper functions for authentication."""

import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Literal, Optional, TypedDict, Union, cast

from appdirs import user_cache_dir

TOKEN_EXPIRY_BUFFER = 60  # seconds
TOKEN_ENV_VAR = "FREVA_TOKEN_FILE"


class Token(TypedDict):
    """Token information."""

    access_token: str
    token_type: str
    expires: int
    refresh_token: str
    refresh_expires: int
    scope: str


def get_default_token_file() -> Path:
    """Get the location of the default token file."""
    path_str = os.getenv(TOKEN_ENV_VAR, "").strip()

    path = Path(
        path_str or os.path.join(user_cache_dir("freva"), "auth-token.json")
    )
    path.parent.mkdir(exist_ok=True, parents=True)
    return path


def is_job_env() -> bool:
    """Detect whether we are running in a batch or job-managed environment.

    Returns
    -------
    bool
        True if common batch or workload manager environment variables are present.
    """

    job_env_vars = [
        # Slurm, PBS, Moab
        "SLURM_JOB_ID",
        "SLURM_NODELIST",
        "PBS_JOBID",
        "PBS_ENVIRONMENT",
        "PBS_NODEFILE",
        # SGE
        "JOB_ID",
        "SGE_TASK_ID",
        "PE_HOSTFILE",
        # LSF
        "LSB_JOBID",
        "LSB_HOSTS",
        # OAR
        "OAR_JOB_ID",
        "OAR_NODEFILE",
        # MPI
        "OMPI_COMM_WORLD_SIZE",
        "PMI_RANK",
        "MPI_LOCALRANKID",
        # Kubernetes
        "KUBERNETES_SERVICE_HOST",
        "KUBERNETES_PORT",
        # FREVA BATCH MODE
        "FREVA_BATCH_JOB",
        # JHUB SESSION
        "JUPYTERHUB_USER",
    ]
    return any(var in os.environ for var in job_env_vars)


def is_jupyter_notebook() -> bool:
    """Check if running in a Jupyter notebook.

    Returns
    -------
    bool
        True if inside a Jupyter notebook or Jupyter kernel.
    """
    try:
        from IPython import get_ipython

        return get_ipython() is not None  # pragma: no cover
    except Exception:
        return False


def is_interactive_shell() -> bool:
    """Check whether we are running in an interactive terminal.

    Returns
    -------
    bool
        True if stdin and stdout are TTYs.
    """
    return sys.stdin.isatty() and sys.stdout.isatty()


def is_interactive_auth_possible() -> bool:
    """Decide if an interactive browser-based auth flow is possible.

    Returns
    -------
    bool
        True if not in a batch/job/JupyterHub context and either in a TTY or
        local Jupyter.
    """
    return (is_interactive_shell() or is_jupyter_notebook()) and not (
        is_job_env()
    )


def resolve_token_path(custom_path: Optional[Union[str, Path]] = None) -> Path:
    """Resolve the path to the token file.

    Parameters
    ----------
    custom_path : str or None
        Optional path override.

    Returns
    -------
    Path
        The resolved path to the token file.
    """
    if custom_path:
        return Path(custom_path).expanduser().absolute()
    path = get_default_token_file()
    return path.expanduser().absolute()


def load_token(path: Optional[Union[str, Path]]) -> Optional[Token]:
    """Load a token dictionary from the given file path.

    Parameters
    ----------
    path : Path or None
        Path to the token file.

    Returns
    -------
    dict or None
        Parsed token dict or None if load fails.
    """
    path = resolve_token_path(path)
    try:
        token: Token = json.loads(path.read_text())
        return token
    except Exception:
        return None


def is_token_valid(
    token: Optional[Token], token_type: Literal["access_token", "refresh_token"]
) -> bool:
    """Check if a refresh token is available.

    Parameters
    ----------
    token : dict
        Token dictionary.
    typken_type: str
        What type of token to check for.

    Returns
    -------
    bool
        True if a refresh token is present.
    """
    exp = cast(
        Literal["refresh_expires", "expires"],
        {
            "refresh_token": "refresh_expires",
            "access_token": "expires",
        }[token_type],
    )
    return cast(
        bool,
        (
            token
            and token_type in token
            and exp in token
            and (time.time() + TOKEN_EXPIRY_BUFFER < token[exp])
        ),
    )


def choose_token_strategy(
    token: Optional[Token] = None, token_file: Optional[Path] = None
) -> Literal["use_token", "refresh_token", "browser_auth", "fail"]:
    """Decide what action to take based on token state and environment.

    Parameters
    ----------
    token : dict|None, default: None
        Token dictionary or None if no token file found.
    token_file: Path|None, default: None
        Path to the file holding token information.

    Returns
    -------
    str
        One of:
        - "use_token"       : Access token is valid and usable.
        - "refresh_token"   : Refresh token should be used to get new access token.
        - "browser_auth"    : Interactive login via browser is allowed.
        - "fail"            : No way to log in in current environment.
    """
    if is_token_valid(token, "access_token"):
        return "use_token"
    if is_token_valid(token, "refresh_token"):
        return "refresh_token"
    if is_interactive_auth_possible():
        return "browser_auth"
    return "fail"


def wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
    """Wait until a TCP port starts accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            try:
                if sock.connect_ex((host, port)) == 0:
                    return
            except OSError:
                pass
        time.sleep(0.05)
    raise TimeoutError(
        f"Port {port} on {host} did not open within {timeout} seconds."
    )
