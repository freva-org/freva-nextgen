"""Module that handles the authentication at the rest service."""

from datetime import datetime
from getpass import getpass, getuser
from typing import Optional, TypedDict

import requests

from .utils import logger
from .utils.databrowser_utils import Config

Token = TypedDict(
    "Token",
    {
        "access_token": str,
        "token_type": str,
        "expires": float,
        "refresh_token": str,
        "refresh_expires": float,
    },
)
JsonPayload = TypedDict(
    "JsonPayload",
    {
        "access_token": str,
        "token_type": str,
        "expires_in": int,
        "refresh_token": str,
        "refresh_expires_in": int,
    },
)


class Auth:
    """Helper class for authentication."""

    _instance: Optional["Auth"] = None
    _auth_token: Optional[Token] = None

    def __new__(cls) -> "Auth":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def token_expiration_time(self, url: str, token: str) -> float:
        """Get the expiration time of an access token."""
        try:
            res = requests.get(
                f"{url}/status",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5,
            )
            res.raise_for_status()
            exp: float = res.json()["exp"]
            return exp
        except requests.HTTPError as error:
            logger.error("Error checking token expiration: %s", error)
            return 0.0

    def _token_is_expired(self, url: str, token: str) -> bool:
        """Check if the token has already expired."""
        exp = datetime.fromtimestamp(self.token_expiration_time(url, token))
        return datetime.now() > exp

    def _set_token(self, json_load: JsonPayload) -> Token:
        now = datetime.now().timestamp()
        self._auth_token = Token(
            access_token=json_load["access_token"],
            refresh_token=json_load["refresh_token"],
            token_type=json_load["token_type"],
            expires=now + json_load["expires_in"],
            refresh_expires=now + json_load["refresh_expires_in"],
        )
        return self._auth_token

    def _refresh(
        self, url: str, refresh_token: str, username: Optional[str] = None
    ) -> Token:
        """Refresh the access_token with a refresh token."""
        try:
            res = requests.post(
                f"{url}/refresh",
                data={"refresh-token": refresh_token},
                timeout=5,
            )
            res.raise_for_status()
        except requests.RequestException as error:
            logger.warning("Failed to refresh token: %s", error)
            if username:
                return self._login_with_password(url, username)
            raise ValueError("Could not use refresh token") from None
        auth = res.json()
        return self._set_token(
            JsonPayload(
                access_token=auth["access_token"],
                token_type=auth["token_type"],
                expires_in=auth["expires_in"],
                refresh_token=auth["refresh_token"],
                refresh_expires_in=auth["refresh_expires_in"],
            )
        )

    def check_authentication(self, auth_url: Optional[str] = None) -> None:
        """Check the status of the authentication.

        Raises
        ------
        ValueError: If user isn't or is no longer authenticated.
        """
        if not self._auth_token:
            raise ValueError("You must authenticate first.")
        now = datetime.now().timestamp()
        if now > self._auth_token["refresh_expires"]:
            raise ValueError("Refresh token has expired.")
        if now > self._auth_token["expires"] and auth_url:
            self._refresh(auth_url, self._auth_token["refresh_token"])

    def _login_with_password(self, auth_url: str, username: str) -> Token:
        """Create a new token."""
        pw_msg = "Give password for server authentication: "
        try:
            res = requests.post(
                f"{auth_url}/token",
                data={
                    "username": username,
                    "password": getpass(pw_msg),
                },
                timeout=5,
            )
            res.raise_for_status()
        except requests.HTTPError as error:
            logger.error("Failed to authenticate: %s", error)
            raise ValueError("Token creation failed") from error
        auth = res.json()
        return self._set_token(
            JsonPayload(
                access_token=auth["access_token"],
                token_type=auth["token_type"],
                expires_in=auth["expires_in"],
                refresh_token=auth["refresh_token"],
                refresh_expires_in=auth["refresh_expires_in"],
            )
        )

    def authenticate(
        self,
        host: Optional[str] = None,
        refresh_token: Optional[str] = None,
        username: Optional[str] = None,
        force: bool = False,
    ) -> Token:
        """Authenticate the user to the host."""
        cfg = Config(host)
        if refresh_token:
            try:
                return self._refresh(cfg.auth_url, refresh_token)
            except ValueError:
                logger.warning(
                    (
                        "Could not use refresh token, falling back "
                        "to username/password"
                    )
                )
        username = username or getuser()
        if self._auth_token is None or force:
            return self._login_with_password(cfg.auth_url, username)
        if self._token_is_expired(
            cfg.auth_url, self._auth_token["access_token"]
        ):
            self._refresh(
                cfg.auth_url, self._auth_token["refresh_token"], username
            )
        return self._auth_token


def authenticate(
    *,
    refresh_token: Optional[str] = None,
    username: Optional[str] = None,
    host: Optional[str] = None,
    force: bool = False,
) -> Token:
    """Authenticate to the host.

    This method generates a new access token that should be used for restricted methods.

    Parameters
    ----------
    refresh_token: str, optional
        Instead of setting a password, you can set a refresh token to refresh
        the access token. This is recommended for non-interactive environments.
    username: str, optional
        The username used for authentication. By default, the current
        system username is used.
    host: str, optional
        The hostname of the REST server.
    force: bool, default: False
        Force token recreation, even if current token is still valid.

    Returns
    -------
    Token: The authentication token.

    Examples
    --------
    Interactive authentication:

    .. code-block:: python

        from freva_client import authenticate
        token = authenticate(username="janedoe")
        print(token)

    Batch mode authentication with a refresh token:

    .. code-block:: python

        from freva_client import authenticate
        token = authenticate(refresh_token="MYTOKEN")
    """
    auth = Auth()
    return auth.authenticate(
        host=host, username=username, refresh_token=refresh_token, force=force
    )
