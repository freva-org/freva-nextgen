"""Module that handles the authentication at the rest service."""

import datetime
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

import requests

from .utils import logger
from .utils.auth_utils import (
    AuthError,
    CodeAuthClient,
    DeviceAuthClient,
    Token,
    choose_token_strategy,
    get_default_token_file,
    is_interactive_auth_possible,
    load_token,
)
from .utils.databrowser_utils import Config

AUTH_FAILED_MSG = """Login failed or no valid token found in this environment.
If you appear to be in a non-interactive or remote session create a new token
via:

   {command}

Then pass the token file using:"

"    {token_path} or set the $TOKEN_ENV_VAR env. variable."""


class Auth:
    """Helper class for authentication."""

    _instance: Optional["Auth"] = None
    _auth_token: Optional[Token] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "Auth":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, token_file: Optional[Union[str, Path]] = None) -> None:
        self.token_file = str(token_file or "").strip() or None

    def get_token(
        self,
        token_url: str,
        data: Dict[str, str],
    ) -> Token:
        try:
            response = requests.post(token_url, data=data)
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            raise AuthError(f"Fetching token failed: {error}")
        auth = response.json()
        return self.set_token(
            access_token=auth["access_token"],
            token_type=auth["token_type"],
            expires=auth["expires"],
            refresh_token=auth["refresh_token"],
            refresh_expires=auth["refresh_expires"],
            scope=auth["scope"],
        )

    def _login(
        self,
        auth_url: str,
        _timeout: Optional[int] = 30,
    ) -> Token:
        device_endpoint = f"{auth_url}/device"
        token_endpoint = f"{auth_url}/token"
        device_client = DeviceAuthClient(
            device_endpoint=device_endpoint,
            token_endpoint=token_endpoint,
            timeout=_timeout,
        )
        code_client = CodeAuthClient(
            login_endpoint=f"{auth_url}/login",
            token_endpoint=f"{auth_url}/token",
            port_endpoint=f"{auth_url}/auth-ports",
        )

        is_interactive_auth = int(
            os.getenv("BROWSER_SESSION", str(int(is_interactive_auth_possible())))
        )
        try:
            response = device_client.login(auto_open=bool(is_interactive_auth))
        except AuthError as error:
            if error.status_code == 503:
                response = code_client.login()
            else:
                raise
        return self.set_token(
            access_token=response["access_token"],
            token_type=response["token_type"],
            expires=response["expires"],
            refresh_token=response["refresh_token"],
            refresh_expires=response["refresh_expires"],
            scope=response["scope"],
        )

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
        default_token_file = get_default_token_file()
        for _file in map(
            Path, set((self.token_file or default_token_file, default_token_file))
        ):
            _file.parent.mkdir(exist_ok=True, parents=True)
            _file.write_text(json.dumps(self._auth_token))
            _file.chmod(0o600)

        return self._auth_token

    def _refresh(
        self, url: str, refresh_token: str, timeout: Optional[int] = 30
    ) -> Token:
        """Refresh the access_token with a refresh token."""
        try:
            return self.get_token(
                f"{url}/token",
                data={"refresh-token": refresh_token or ""},
            )
        except (AuthError, KeyError) as error:
            logger.warning("Failed to refresh token: %s", error)
            return self._login(url, _timeout=timeout)

    def authenticate(
        self,
        host: Optional[str] = None,
        config: Optional[Config] = None,
        *,
        force: bool = False,
        timeout: Optional[int] = 30,
        _cli: bool = False,
    ) -> Token:
        """Authenticate the user to the host."""
        cfg = config or Config(host)
        token = self._auth_token or load_token(self.token_file)
        reason: Optional[str] = None
        if force:
            strategy = "browser_auth"
        else:
            strategy = choose_token_strategy(token)
        if strategy == "use_token" and token:
            self._auth_token = token
            return self._auth_token
        try:
            if strategy == "refresh_token" and token:
                return self._refresh(
                    cfg.auth_url, token["refresh_token"], timeout=timeout
                )
            if strategy == "browser_auth":
                return self._login(cfg.auth_url, _timeout=timeout)
        except AuthError as error:
            reason = str(error)

        command, token_path = {
            True: ("freva-client auth", "--token-file /path/to/token.json"),
            False: (
                "freva_client.auth",
                "`token_file='/path/to/token.json'`",
            ),
        }[_cli]

        reason = reason or AUTH_FAILED_MSG.format(
            command=command, token_path=token_path
        )
        if _cli:
            logger.critical(reason)
            raise SystemExit(1)
        else:
            raise AuthError(reason)


def authenticate(
    *,
    token_file: Optional[Union[Path, str]] = None,
    host: Optional[str] = None,
    force: bool = False,
    timeout: Optional[int] = 30,
) -> Token:
    """Authenticate to the host.

    This method generates a new access token that should be used for restricted methods.

    Parameters
    ----------
    token_file: str, optional
        Instead of setting a password, you can set a refresh token to refresh
        the access token. This is recommended for non-interactive environments.
    host: str, optional
        The hostname of the REST server.
    force: bool, default: False
        Force token recreation, even if current token is still valid.
    timeout: int, default: 30
        Set the timeout, None for indefinate.

    Returns
    -------
    Token: The authentication token.

    Examples
    --------
    Interactive authentication:

    .. code-block:: python

        from freva_client import authenticate
        token = authenticate(timeout=120)
        print(token)

    Batch mode authentication with a refresh token:

    .. code-block:: python

        from freva_client import authenticate
        token = authenticate(token_file="~/.freva-login-token.json")
    """
    auth = Auth(token_file=token_file or None)
    return auth.authenticate(
        host=host,
        force=force,
        timeout=timeout,
    )
