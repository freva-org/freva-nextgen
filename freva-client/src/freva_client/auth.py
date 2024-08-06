"""Module that handles the authentication at the rest service."""

import datetime
from getpass import getpass, getuser
from typing import Optional, TypedDict, Union

from authlib.integrations.requests_client import OAuth2Session

from .utils import logger
from .utils.databrowser_utils import Config

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


class Auth:
    """Helper class for authentication."""

    _instance: Optional["Auth"] = None
    _auth_token: Optional[Token] = None

    def __new__(cls) -> "Auth":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        self._auth_cls = OAuth2Session()

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
        self, url: str, refresh_token: str, username: Optional[str] = None
    ) -> Token:
        """Refresh the access_token with a refresh token."""
        auth = self._auth_cls.refresh_token(f"{url}/token", refresh_token or " ")
        try:
            return self.set_token(
                access_token=auth["access_token"],
                token_type=auth["token_type"],
                expires=auth["expires"],
                refresh_token=auth["refresh_token"],
                refresh_expires=auth["refresh_expires"],
                scope=auth["scope"],
            )
        except KeyError:
            logger.warning("Failed to refresh token: %s", auth.get("detail", ""))
            if username:
                return self._login_with_password(url, username)
            raise ValueError("Could not use refresh token") from None

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

    def _login_with_password(self, auth_url: str, username: str) -> Token:
        """Create a new token."""
        pw_msg = "Give password for server authentication: "
        auth = self._auth_cls.fetch_token(
            f"{auth_url}/token", username=username, password=getpass(pw_msg)
        )
        try:
            return self.set_token(
                access_token=auth["access_token"],
                token_type=auth["token_type"],
                expires=auth["expires"],
                refresh_token=auth["refresh_token"],
                refresh_expires=auth["refresh_expires"],
                scope=auth["scope"],
            )
        except KeyError:
            logger.error("Failed to authenticate: %s", auth.get("detail", ""))
            raise ValueError("Token creation failed") from None

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
        if self.token_expiration_time < datetime.datetime.now(datetime.timezone.utc):
            self._refresh(cfg.auth_url, self._auth_token["refresh_token"], username)
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
