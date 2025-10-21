"""Helper functions for authentication."""

import json
import os
import random
import socket
import sys
import time
import urllib.parse
import webbrowser
from contextlib import contextmanager
from dataclasses import dataclass, field
from getpass import getuser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Event, Thread
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    TypedDict,
    Union,
    cast,
)

import requests
import rich.console
import rich.spinner
from appdirs import user_cache_dir
from rich.live import Live

from freva_client.utils import logger

TOKEN_EXPIRY_BUFFER = 60  # seconds
TOKEN_ENV_VAR = "FREVA_TOKEN_FILE"


REDIRECT_URI = "http://localhost:{port}/callback"
BROWSER_MESSAGE = """Will attempt to open the auth url in your browser.

If this doesn't work, try opening the following url:

{interactive}{uri}{interactive_end}

You might have to enter the this code manually {interactive}{user_code}{interactive_end}
"""

NON_INTERACTIVE_MESSAGE = """

Visit the following url to authorise your session:

{interactive}{uri}{interactive_end}

You might have to enter the this code manually {interactive}{user_code}{interactive_end}

"""


@contextmanager
def _clock(timeout: Optional[int] = None) -> Iterator[None]:
    Console = rich.console.Console(
        force_terminal=is_interactive_shell(), stderr=True
    )
    txt = f"- Timeout: {timeout:>3,.0f}s " if timeout else ""
    interactive = int(Console.is_terminal)
    if int(os.getenv("INTERACIVE_SESSION", str(interactive))):
        spinner = rich.spinner.Spinner(
            "moon", text=f"[b]Waiting for code {txt}... [/]"
        )
        live = Live(
            spinner, console=Console, refresh_per_second=2.5, transient=True
        )
        try:
            live.start()
            yield
        finally:
            live.stop()
    else:
        yield


class AuthError(Exception):
    """Athentication error."""

    def __init__(
        self,
        message: str,
        detail: Optional[Dict[str, str]] = None,
        status_code: int = 500,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or {}
        self.status_code = status_code

    def __str__(self) -> str:
        return f"{self.message}: {self.detail}" if self.detail else self.message


class Token(TypedDict, total=False):
    """Token information."""

    access_token: str
    token_type: str
    expires: int
    refresh_token: str
    refresh_expires: int
    scope: str


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        logger.debug(format, *args)

    def do_GET(self) -> None:
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)
        if "code" in params:
            setattr(self.server, "auth_code", params["code"][0])
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Login successful! You can close this tab.")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Authorization code not found.")


@dataclass
class CodeAuthClient:
    """Minimal OIDC Code Authorization Grand client.

    Parameters
    ----------
    device_endpoint : str
        Device endpoint URL, e.g. ``{issuer}/protocol/openid-connect/auth/device``.
    token_endpoint : str
        Token endpoint URL, e.g. ``{issuer}/protocol/openid-connect/token``.
    scope : str, optional
        Space-separated scopes; include ``offline_access`` if you need offline RTs.
    timeout : int | None, optional
        Overall timeout (seconds) for user approval. ``None`` waits indefinitely.
    session : requests.Session | None, optional
        Reusable HTTP session. A new one is created if omitted.

    """

    login_endpoint: str
    token_endpoint: str
    port_endpoint: str
    timeout: Optional[int] = 600
    session: requests.Session = field(default_factory=requests.Session)

    @staticmethod
    def _start_local_server(port: int, event: Event) -> HTTPServer:
        """Start local HTTP server to wait for a single callback."""
        server = HTTPServer(("localhost", port), OAuthCallbackHandler)

        def handle() -> None:
            logger.info("Waiting for browser callback on port %s ...", port)
            while not event.is_set():
                server.handle_request()
                if getattr(server, "auth_code", None):
                    event.set()

        thread = Thread(target=handle, daemon=True)
        thread.start()
        return server

    def _find_free_port(self) -> int:
        """Get a free port where we can start the test server."""
        ports: List[int] = (
            requests.get(self.port_endpoint).json().get("valid_ports", [])
        )
        for port in ports:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("localhost", port))
                    return port
                except (OSError, PermissionError):
                    pass
        raise OSError("No free ports available for login flow")

    @staticmethod
    def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
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

    def login(
        self,
    ) -> Token:
        """Start the login flow."""
        port = self._find_free_port()
        redirect_uri = REDIRECT_URI.format(port=port)
        params = {
            "redirect_uri": redirect_uri,
            "offline_access": "true",
            "prompt": "consent",
        }
        query = urllib.parse.urlencode(params)
        login_url = f"{self.login_endpoint}?{query}"
        logger.info("Opening browser for login:\n%s", login_url)
        logger.info(
            "If you are using this on a remote host, you might need to "
            "increase the login timeout and forward port %d:\n"
            "    ssh -L %d:localhost:%d %s@%s",
            port,
            port,
            port,
            getuser(),
            socket.gethostname(),
        )
        event = Event()
        server = self._start_local_server(port, event)
        code: Optional[str] = None
        reason = "Login failed."
        try:
            self._wait_for_port("localhost", port)
            webbrowser.open(login_url)
            success = event.wait(timeout=self.timeout or None)
            if not success:
                raise TimeoutError(
                    f"Login did not complete within {self.timeout} seconds. "
                    "Possibly headless environment."
                )
            code = getattr(server, "auth_code", None)
        except Exception as error:
            logger.warning(
                "Could not open browser automatically. %s"
                "Please open the URL manually.",
                error,
            )
            reason = str(error)

        finally:
            logger.debug("Cleaning up login state")
            if hasattr(server, "server_close"):
                try:
                    server.server_close()
                except Exception as error:
                    logger.debug("Failed to close server cleanly: %s", error)
        if not code:
            raise AuthError(reason)
        data = {
            "code": code,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }

        response = requests.post(self.token_endpoint, data=data)
        response.raise_for_status()
        return cast(Token, response.json())


@dataclass
class DeviceAuthClient:
    """Minimal OIDC Device Authorization Grant client.

    Parameters
    ----------
    device_endpoint : str
        Device endpoint URL, e.g. ``{issuer}/protocol/openid-connect/auth/device``.
    token_endpoint : str
        Token endpoint URL, e.g. ``{issuer}/protocol/openid-connect/token``.
    scope : str, optional
        Space-separated scopes; include ``offline_access`` if you need offline RTs.
    timeout : int | None, optional
        Overall timeout (seconds) for user approval. ``None`` waits indefinitely.
    session : requests.Session | None, optional
        Reusable HTTP session. A new one is created if omitted.

    Notes
    -----
    - For a CLI UX, show both ``verification_uri_complete`` (if present) and
      ``verification_uri`` + ``user_code`` as fallback.
    - Polling respects ``interval`` and ``slow_down`` per RFC 8628.
    """

    device_endpoint: str
    token_endpoint: str
    timeout: Optional[int] = 600
    session: requests.Session = field(default_factory=requests.Session)

    # ---------- Public API ----------

    def login(
        self,
        *,
        auto_open: bool = True,
    ) -> Token:
        """
        Run the full device flow and return an OIDC token payload.

        Parameters
        ----------
        auto_open : bool, optional
            Attempt to open ``verification_uri_complete`` in a browser.

        Returns
        -------
        Token
            The token payload. Includes a refresh token if granted and allowed.

        Raises
        ------
        AuthError
            If the device flow fails or times out.
        """
        init = self._authorize()

        uri = init.get("verification_uri_complete") or init["verification_uri"]
        user_code = init["user_code"]

        Console = rich.console.Console(
            force_terminal=is_interactive_shell(), stderr=True
        )
        pprint = Console.print if Console.is_terminal else print
        interactive, interactive_end = (
            ("[b]", "[/b]") if Console.is_terminal else ("", "")
        )

        if auto_open and init.get("verification_uri_complete"):
            try:
                pprint(
                    BROWSER_MESSAGE.format(
                        user_code=user_code,
                        uri=uri,
                        interactive=interactive,
                        interactive_end=interactive_end,
                    )
                )
                webbrowser.open(init["verification_uri_complete"])
            except Exception as error:
                logger.warning("Could not auto-open browser: %s", error)
        else:
            pprint(
                NON_INTERACTIVE_MESSAGE.format(
                    user_code=user_code,
                    uri=uri,
                    interactive=interactive,
                    interactive_end=interactive_end,
                )
            )
        return self._poll_for_token(
            device_code=init["device_code"],
            base_interval=int(init.get("interval", 5)),
        )

    # ---------- Internals ----------

    def _authorize(self) -> Dict[str, Any]:
        """Start device authorization; return the raw init payload."""
        payload = self._post_form(self.device_endpoint)
        # Validate essentials
        for k in ("device_code", "user_code", "verification_uri", "expires_in"):
            if k not in payload:
                raise AuthError(f"Device authorization missing '{k}'")
        return payload

    def _poll_for_token(self, *, device_code: str, base_interval: int) -> Token:
        """
        Poll token endpoint until approved/denied/expired; return token JSON.

        Raises
        ------
        AuthError
            On timeout, denial, or other OAuth errors.
        """
        start = time.monotonic()
        interval = max(1, base_interval)
        with _clock(self.timeout):
            while True:
                sleep = interval + random.uniform(-0.2, 0.4)
                if (
                    self.timeout is not None
                    and time.monotonic() - start > self.timeout
                ):
                    raise AuthError(
                        "Login did not complete within the allotted time; "
                        "approve the request in your browser and try again."
                    )

                data = {"device-code": device_code}
                try:
                    return cast(Token, self._post_form(self.token_endpoint, data))
                except AuthError as error:
                    err = (
                        error.detail.get("error")
                        if isinstance(error.detail, dict)
                        else None
                    )
                    if err is None or "authorization_pending" in err:
                        time.sleep(sleep)
                    elif "slow_down" in err:
                        interval += 5
                        time.sleep(sleep)
                    elif "expired_token" in err or "access_denied" in err:
                        raise AuthError(f"Device flow failed: {err}")
                    else:
                        # Unknown OAuth error
                        raise  # pragma: no cover

    def _post_form(
        self, url: str, data: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """POST x-www-form-urlencoded and return JSON or raise AuthError."""
        resp = self.session.post(
            url,
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Connection": "close",
            },
            timeout=30,
        )
        if resp.status_code >= 400:
            try:
                payload = resp.json()
            except Exception:
                payload = {
                    "error": "http_error",
                    "error_description": resp.text[:300],
                }
            raise AuthError(
                f"{url} -> {resp.status_code}",
                detail=payload,
                status_code=resp.status_code,
            )
        try:
            return cast(Dict[str, Any], resp.json())
        except Exception as error:
            raise AuthError(f"Invalid JSON from {url}: {error}")


def get_default_token_file(token_file: Optional[Union[str, Path]] = None) -> Path:
    """Get the location of the default token file."""
    path_str = token_file or os.getenv(TOKEN_ENV_VAR, "").strip()

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
        from IPython import get_ipython  # type: ignore[attr-defined]

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


def requires_authentication(
    flavour: Optional[str],
    zarr: bool = False,
    databrowser_url: Optional[str] = None,
) -> bool:
    """Check if authentication is required.

    Parameters
    ----------
    flavour : str or None
        The data flavour to check.
    zarr : bool, default: False
        Whether the request is for zarr data.
    databrowser_url : str or None
        The URL of the databrowser to query for available flavours.
        If None, the function will skip querying and assume authentication
        is required for non-default flavours.
    """
    if zarr:
        return True
    if flavour in {"freva", "cmip6", "cmip5", "cordex", "user", None}:
        return False
    try:
        response = requests.get(f"{databrowser_url}/flavours", timeout=30)
        response.raise_for_status()
        result = {"flavours": response.json().get("flavours", [])}
        if "flavours" in result:
            global_flavour_names = {f["flavour_name"] for f in result["flavours"]}
            return flavour not in global_flavour_names
    except Exception:
        pass

    return True
