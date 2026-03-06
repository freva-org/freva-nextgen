from pathlib import Path
from typing import Optional, Union

from py_oidc_auth_client import Token

from .utils import AuthConfig


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
        Set the timeout, None for indefinite.

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
    from py_oidc_auth_client import authenticate as _authenticate

    from .utils.databrowser_utils import Config

    cfg = Config(host)
    if token_file:
        auth = AuthConfig.from_token_file(cfg.api_url, token_file)
    else:
        auth = AuthConfig(cfg.api_url)
    return _authenticate(
        auth.config.host,
        force=force,
        timeout=timeout,
        store=auth.token_db,
        app_name=auth.app_name,
    )
