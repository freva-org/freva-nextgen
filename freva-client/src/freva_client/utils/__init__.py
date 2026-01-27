"""Utilities for the general freva-client lib."""

import logging
import sys
from functools import wraps
from typing import Any, Callable, Dict, Literal, Optional, cast

import requests
from rich import print as pprint

from .logger import Logger

logger: Logger = cast(Logger, logging.getLogger("freva-client"))


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
