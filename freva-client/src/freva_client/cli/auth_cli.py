"""Command line interface for authentication."""

import json
from enum import Enum
from typing import List, Optional

import typer

from freva_client import authenticate
from freva_client.utils import exception_handler, logger

from .cli_utils import version_callback


class LoginFlow(str, Enum):
    """How ``freva-client auth`` logs in."""

    auto = "auto"
    device = "device"
    code = "code"


auth_app = typer.Typer(
    name="auth",
    help="Create OAuth2 access and refresh token.",
    pretty_exceptions_short=False,
)


@exception_handler
def authenticate_cli(
    host: Optional[str] = typer.Option(
        None,
        "--host",
        help=(
            "Set the hostname of the databrowser, if not set (default) "
            "the hostname is read from a config file"
        ),
    ),
    token_file: str = typer.Option(
        "",
        "--token-file",
        envvar="OIDC_TOKEN_FILE",
        help=(
            "Instead of authenticating via code based authentication flow "
            "you can set the path to the json file that contains a "
            "`refresh token` containing a refresh_token key."
        ),
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Force token recreation, even if current token is still valid.",
    ),
    scope: Optional[List[str]] = typer.Option(
        None,
        "--scope",
        "-s",
        help=(
            "Scope to request instead of the server's default ones. Repeat "
            "the option, or give several separated by spaces."
        ),
    ),
    flow: LoginFlow = typer.Option(
        LoginFlow.auto,
        "--flow",
        help=(
            "auto: the device flow, or the browser based code flow where "
            "the server has no device flow. device or code force one."
        ),
        case_sensitive=False,
    ),
    timeout: int = typer.Option(
        30,
        "--timeout",
        help="Set the timeout for login in secdonds, 0 for indefinite",
    ),
    verbose: int = typer.Option(0, "-v", help="Increase verbosity", count=True),
    version: Optional[bool] = typer.Option(
        False,
        "-V",
        "--version",
        help="Show version an exit",
        callback=version_callback,
    ),
) -> None:
    """Create OAuth2 access and refresh token."""
    logger.set_verbosity(verbose)
    token = authenticate(
        token_file=token_file.strip() or None,
        host=host,
        force=force,
        timeout=timeout or None,
        scopes=" ".join(scope) if scope else None,
        flow=flow.value,
    )
    print(json.dumps(token, indent=3))
