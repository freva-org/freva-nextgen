import asyncio
from pathlib import Path
from typing import Literal, Optional, Sequence, Union

from py_oidc_auth_client import Token

from .utils import AuthConfig

LoginFlowName = Literal["auto", "device", "code"]
#: The login flows ``authenticate`` can be told to use.
LOGIN_FLOWS = ("auto", "device", "code")


def authenticate(
    *,
    token_file: Optional[Union[Path, str]] = None,
    host: Optional[str] = None,
    force: bool = False,
    timeout: Optional[int] = 30,
    scopes: Optional[Union[str, Sequence[str]]] = None,
    flow: LoginFlowName = "auto",
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
    scopes: str or list of str, optional
        Scopes to request instead of the server's default ones, e.g.
        ``"openid email"``.  Such a token is cached next to the default
        login and never stands in for it.
    flow: str, default: auto
        ``auto`` logs in with the device flow, or with the code flow where
        the server has no device flow.  ``device`` and ``code`` force one.

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

    Only some scopes, with the browser based code flow:

    .. code-block:: python

        from freva_client import authenticate
        token = authenticate(scopes="openid email", flow="code")

    Batch mode authentication with a refresh token:

    .. code-block:: python

        from freva_client import authenticate
        token = authenticate(token_file="~/.freva-login-token.json")
    """
    from py_oidc_auth_client import CodeFlow, DeviceFlow
    from py_oidc_auth_client import authenticate as _authenticate

    from .utils.databrowser_utils import Config

    if flow not in LOGIN_FLOWS:
        raise ValueError(
            f"Unknown login flow {flow!r}, use one of {', '.join(LOGIN_FLOWS)}"
        )

    cfg = Config(host)
    if token_file:
        auth = AuthConfig.from_token_file(cfg.api_url, token_file)
    else:
        auth = AuthConfig(cfg.api_url)
    if flow == "auto":
        return _authenticate(
            auth.config.host,
            force=force,
            timeout=timeout,
            store=auth.token_db,
            app_name=auth.app_name,
            # The server's own callback ports (/auth/v2/auth-ports), which
            # its identity provider accepts as redirect URIs.
            redirect_ports=auth.config.redirect_ports,
            scopes=scopes,
        )
    login = (DeviceFlow if flow == "device" else CodeFlow)(
        auth.config.host,
        store=auth.token_db,
        timeout=timeout,
        redirect_ports=auth.config.redirect_ports,
        scopes=scopes,
    )
    return asyncio.run(login.authenticate(force=force))
