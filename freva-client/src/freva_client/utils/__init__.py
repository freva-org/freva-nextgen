"""Utilities for the general freva-client lib."""

import logging
import sys
from functools import wraps
from typing import Any, Callable, cast

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
