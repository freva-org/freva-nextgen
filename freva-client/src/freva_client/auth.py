"""Module that handles the authentication at the rest service."""

import datetime
import socket
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, Optional, TypedDict, Union, cast

import requests

from .utils import logger
from .utils.databrowser_utils import Config

REDIRECT_URI = "http://localhost:{port}/callback"


Token = TypedDict(
    "Token",
    {
        "access_token": str,
        "token_type": str,
        "expires": int,
        "refresh_token": str,
        "refresh_expires": int,
        "scope": str,
    },
)


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        logger.info(format, *args)

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


def start_local_server(port: int) -> Optional[str]:
    server = HTTPServer(("localhost", port), OAuthCallbackHandler)
    logger.info("Waiting for callback ...")

    server.handle_request()
    return getattr(server, "auth_code", None)


class Auth:
    """Helper class for authentication."""

    _instance: Optional["Auth"] = None
    _auth_token: Optional[Token] = None

    def __new__(cls) -> "Auth":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        pass

    def get_token(self, token_url: str, data: Dict[str, str]) -> Token:
        try:
            response = requests.post(token_url, data=data)
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            raise ValueError(f"Fetching token failed: {error}")
        auth = response.json()
        return self.set_token(
            access_token=auth["access_token"],
            token_type=auth["token_type"],
            expires=auth["expires"],
            refresh_token=auth["refresh_token"],
            refresh_expires=auth["refresh_expires"],
            scope=auth["scope"],
        )

    def _login(self, auth_url: str, port: Optional[int] = None) -> Token:
        login_endpoint = f"{auth_url}/login"
        token_endpoint = f"{auth_url}/token"
        port = port or self.find_free_port()
        redirect_uri = REDIRECT_URI.format(port=port)
        login_url = (
            login_endpoint + f"?redirect_uri={urllib.parse.quote(redirect_uri)}"
        )
        logger.info("Opening browser for login:\n%s", login_url)
        logger.info(
            "If you are using this on a remote host you might need to "
            "forward port %i to your localhost via ssh:\n"
            "   ssh -L %i:localhost:%i user@remotehost",
            port,
            port,
            port,
        )

        try:
            webbrowser.open(login_url)
        except Exception:
            logger.warning(
                "Could not open browser automatically. Please open the URL manually."
            )
        code = start_local_server(port)
        if not code:
            raise ValueError("No code received. Login probably failed.") from None
        return self.get_token(
            token_endpoint,
            data={
                "code": code,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )

    @staticmethod
    def find_free_port() -> int:
        """Get a free port where we can start the test server."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            return cast(int, s.getsockname()[1])

    @property
    def token_expiration_time(self) -> datetime.datetime:
        """Get the expiration time of an access token."""
        if self._auth_token is None:
            exp = 0.0
        else:
            exp = self._auth_token["expires"]
        return datetime.datetime.fromtimestamp(exp, datetime.timezone.utc)

    def set_token(
        self,
        access_token: str,
        refresh_token: Optional[str] = None,
        expires_in: int = 10,
        refresh_expires_in: int = 10,
        expires: Optional[Union[float, int]] = None,
        refresh_expires: Optional[Union[float, int]] = None,
        token_type: str = "Bearer",
        scope: str = "profile email address",
    ) -> Token:
        """Override the existing auth token."""
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()

        self._auth_token = Token(
            access_token=access_token or "",
            refresh_token=refresh_token or "",
            token_type=token_type,
            expires=int(expires or now + expires_in),
            refresh_expires=int(refresh_expires or now + refresh_expires_in),
            scope=scope,
        )
        return self._auth_token

    def _refresh(
        self, url: str, refresh_token: str, port: Optional[int] = None
    ) -> Token:
        """Refresh the access_token with a refresh token."""
        try:
            return self.get_token(
                f"{url}/token", data={"refresh-token": refresh_token or ""}
            )
        except (ValueError, KeyError) as error:
            logger.warning("Failed to refresh token: %s", error)
            return self._login(url, port=port)

    def check_authentication(self, auth_url: Optional[str] = None) -> Token:
        """Check the status of the authentication.

        Raises
        ------
        ValueError: If user isn't or is no longer authenticated.
        """
        if not self._auth_token:
            raise ValueError("You must authenticate first.")
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        if now > self._auth_token["refresh_expires"]:
            raise ValueError("Refresh token has expired.")
        if now > self._auth_token["expires"] and auth_url:
            self._refresh(auth_url, self._auth_token["refresh_token"])
        return self._auth_token

    def authenticate(
        self,
        host: Optional[str] = None,
        refresh_token: Optional[str] = None,
        force: bool = False,
        *,
        helper_port: Optional[int] = None,
    ) -> Token:
        """Authenticate the user to the host."""
        cfg = Config(host)
        if refresh_token:
            try:
                return self._refresh(cfg.auth_url, refresh_token)
            except ValueError:
                logger.warning(("Could not use refresh token, lgging in "))
        if self._auth_token is None or force:
            return self._login(cfg.auth_url, port=helper_port)
        if self.token_expiration_time < datetime.datetime.now(
            datetime.timezone.utc
        ):
            self._refresh(
                cfg.auth_url, self._auth_token["refresh_token"], port=helper_port
            )
        return self._auth_token


def authenticate(
    *,
    refresh_token: Optional[str] = None,
    host: Optional[str] = None,
    force: bool = False,
    helper_port: Optional[int] = None,
) -> Token:
    """Authenticate to the host.

    This method generates a new access token that should be used for restricted methods.

    Parameters
    ----------
    refresh_token: str, optional
        Instead of setting a password, you can set a refresh token to refresh
        the access token. This is recommended for non-interactive environments.
    host: str, optional
        The hostname of the REST server.
    force: bool, default: False
        Force token recreation, even if current token is still valid.
    helper_port: int, default: None
        The authentication process will spawn a web server that will open
        the login url. You can specify the port where this web server should
        be running on. If None chosen (default) a random port will be set.

    Returns
    -------
    Token: The authentication token.

    Examples
    --------
    Interactive authentication:

    .. code-block:: python

        from freva_client import authenticate
        token = authenticate()
        print(token)

    Batch mode authentication with a refresh token:

    .. code-block:: python

        from freva_client import authenticate
        token = authenticate(refresh_token="MYTOKEN")
    """
    auth = Auth()
    return auth.authenticate(
        host=host,
        refresh_token=refresh_token,
        force=force,
        helper_port=helper_port,
    )
