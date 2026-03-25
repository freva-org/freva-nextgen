"""Utilities for the general freva-client lib."""

import json
import logging
import sys
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Union, cast
from urllib.parse import urlsplit, urlunsplit

import requests
from py_oidc_auth_client import Config, Token, TokenStore
from py_oidc_auth_client.utils import choose_token_strategy
from rich import print as pprint

from .logger import Logger

logger: Logger = cast(Logger, logging.getLogger("freva-client"))


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


class AuthConfig:
    """Create a configuration for authentication."""

    app_name = "freva"

    def __init__(self, host: str, _redirect_ports: List[int] = []):

        self.token_db = TokenStore(self.app_name)
        host = self.get_rest_host(host)
        kwargs: Dict[str, Union[str, List[int]]] = {"app_name": self.app_name}
        if not _redirect_ports:
            try:
                _redirect_ports += (
                    requests.get(f"{host}/auth/v2/auth-ports")
                    .json()
                    .get("valid_ports", [])
                ) or []
                kwargs["redirect_ports"] = _redirect_ports
            except Exception:
                pass
        self.config = Config(host, **kwargs)

    @property
    def token_strategy(self) -> Optional[Token]:
        """Determine how an access token can be generated."""
        return choose_token_strategy(self.token_db.get(self.config.host) or None)

    @classmethod
    def from_token_file(
        cls, host: str, token_file: Union[str, Path]
    ) -> "AuthConfig":
        """Create an instance of AuthConfig by adding a token to the token store."""
        store = TokenStore(app_name=cls.app_name)
        host = cls.get_rest_host(host)
        try:
            store.put(host, json.loads(Path(token_file).read_text()))
        except Exception:
            pass
        return cls(host)

    @staticmethod
    def get_rest_host(host: str) -> str:
        """Define the rest-api server."""
        scheme, _, suffix = host.rpartition("://")
        scheme = scheme or "https"
        url_split = urlsplit(f"{scheme}://{suffix}")
        return urlunsplit(
            [url_split.scheme, url_split.netloc, "/api/freva-nextgen", "", ""]
        ).rstrip("/")


def exception_handler(func: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap an exception handler around the cli functions."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        """Wrapper function that handles the exception."""
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            pprint("[red][b]User interrupt: Exit[/red][/b]", file=sys.stderr)
            raise SystemExit(150) from None
        except BaseException as error:
            if logger.getEffectiveLevel() <= logging.DEBUG:
                logger.exception(error)
            else:
                logger.error(error)
            raise SystemExit(1) from None

    return wrapper


def do_request(
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"],
    url: str,
    data: Optional[Dict[str, Any]] = None,
    fail_on_error: bool = False,
    **kwargs: Any,
) -> Optional[requests.models.Response]:
    """Create a request to the rest-api."""
    method_upper = method.upper()
    timeout = kwargs.pop("timeout", 30)
    params = kwargs.pop("params", {})
    stream = kwargs.pop("stream", False)
    kwargs.setdefault("headers", {})
    logger.debug(
        "%s request to %s with data: %s and parameters: %s",
        method_upper,
        url,
        data,
        params,
    )

    try:
        req = requests.Request(
            method=method_upper,
            url=url,
            params=params,
            json=None if method_upper in "GET" else data,
            **kwargs,
        )
        with requests.Session() as session:
            prepared = session.prepare_request(req)
            res = session.send(prepared, timeout=timeout, stream=stream)
            res.raise_for_status()
            return res

    except KeyboardInterrupt:
        pprint("[red][b]User interrupt: Exit[/red][/b]", file=sys.stderr)
    except (
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
        requests.exceptions.InvalidURL,
    ) as error:
        server_msg = ""
        if hasattr(error, "response") and error.response is not None:
            try:
                error_data = error.response.json()
                error_var = {
                    error_data.get(
                        "detail",
                        error_data.get("message", error_data.get("error", "")),
                    )
                }
                server_msg = f" - {error_var}"
            except Exception:
                pass
        msg = f"{method_upper} request failed with: {error}{server_msg}"
        if fail_on_error:
            raise ValueError(msg) from None
        logger.warning(msg)
    return None
