"""# Command line interface (cli) for running the rest server.

## Configuring


There are three fundamental different options to configure the service.

1. via command line arguments
1. via environment variables.
1. via the `config` ``.toml`` file.

Note, that the order here is important. First, given command line arguments
are evaluated, if present any of the below given environment variables
are evaluated, finally the api config file is evaluated. The following
environment variables are evaluated:

- ``DEBUG``: Start server in debug mode (1), (default: 0 -> no debug).
- ``API_PORT``: the port the rest service should be running on (default 8080).
- ``API_WORKER``: the number of multi-process work serving the API (default: 8).
- ``API_SOLR_HOST``: host name of the solr server, host name and port should be
                 separated by a ``:``, for example ``localhost:8983``
- ``API_SOLR_CORE`` : name of the solr core that contains datasets with multiple
                  versions
- ``API_MONGO_HOST``: host name of the mongodb server, where query statistics are
                 stored. Host name and port should separated by a ``:``, for
                 example ``localhost:27017``
- ``API_MONGO_USER``: user name for the mongodb.
- ``API_MONGO_PASSWORD``: password to log on to the mongodb.
- ``API_MONGO_DB``: database name of the mongodb instance.
- ``API_PROXY``: url of a proxy that servers the API - if any.
- ``API_CACHE_EXP``: expiry time in seconds of the cached data
- ``API_REDIS_HOST``: Host and port of the redis cache
                  Host name and port should separated by a ``:``, for
                  example ``localhost:5672``
- ``API_REDIS_PASSWORD``: Password for the redis connection.
- ``API_REDIS_USER``: Username for the redis connection.
- ``API_REDIS_SSL_CERTFILE``: Path to the TSL certificate file used to encrypt
                          the redis connection.
- ``API_REDIS_SSL_KEYFILE``: Path to the TSL key file used to encrypt the redis
                         connection.
- ``API_OIDC_URL``: Discovery of the open connect id service.
- ``API_OIDC_CLIENT_ID``: Name of the client (app) that is used to create
                          the access tokens, defaults to freva
- ``API_OIDC_CLIENT_SECRET``: You can set a client secret, if you have
- ``API_OIDC_TOKEN_CLAIMS``:  Valid token claims, to check against
- ``API_SERVICES``:  The services the api should serve.

📝  You can override the path to the default config file using the
    ``API_CONFIG`` environment variable. The default location of this config
    file is ``/opt/databrowser/api_config.toml``.
"""

import argparse
import asyncio
import logging
import os
import sys
from enum import Enum
from pathlib import Path
from socket import gethostname
from tempfile import NamedTemporaryFile
from typing import (
    Annotated,
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

import uvicorn
from pydantic.fields import FieldInfo
from rich import print as pprint
from rich.markdown import Markdown
from rich_argparse import ArgumentDefaultsRichHelpFormatter

from freva_rest import __version__

from .config import ServerConfig
from .logger import logger

NoneType = type(None)


def _dict_to_defaults(
    input_dict: Optional[Dict[str, Union[List[str], str]]],
) -> List[Tuple[str, str]]:
    """Convert to dict to argparse defaults."""
    output: List[Tuple[str, str]] = []
    input_dict = input_dict or {}
    for key, value in input_dict.items():
        if isinstance(value, str):
            value = [value]
        for v in value:
            output.append((key, v))
    return output


def _is_type_annotation(annotation: Any, target_type: Type[Any]) -> bool:
    """
    Recursively check if a type annotation represents or contains the target_type
    (e.g., dict, list, etc.), even if wrapped in Optional, Annotated, etc.
    """
    origin = get_origin(annotation)

    if origin is Annotated:
        return _is_type_annotation(get_args(annotation)[0], target_type)

    if origin is Union:
        return any(
            _is_type_annotation(arg, target_type) for arg in get_args(annotation)
        )

    return origin is target_type or annotation is target_type


class VersionAction(argparse._VersionAction):
    """Custom Action for displaying the programm versions."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: Optional[str] = None,
    ) -> None:
        version = self.version or "%(prog)s"
        pprint(version % {"prog": parser.prog or sys.argv[1]})
        parser.exit()


def create_arg_parser(fields: Dict[str, FieldInfo]) -> argparse.ArgumentParser:
    """Create the cli parser."""
    parser = argparse.ArgumentParser(
        prog="freva-rest-server",
        description=Markdown(__doc__),  # type: ignore
        formatter_class=ArgumentDefaultsRichHelpFormatter,
    )
    parser.add_argument(
        "-V",
        "--version",
        help="Display version and exit.",
        version=f"[b][red]%(prog)s[/red]: {__version__}[/b]",
        action=VersionAction,
    )
    parser.add_argument(
        "--port",
        "-p",
        help="Set the port the server should be running on.",
        default=7777,
        type=int,
    )
    for key, field in fields.items():
        args = [f'--{key.replace("_", "-")}']
        if field.alias:
            args.append(f"-{field.alias}")
        if field.annotation in (
            bool,
            Union[str, int, bool],
            Union[int, bool],
        ) or isinstance(field.default, bool):
            parser.add_argument(
                *args,
                help=field.description,
                action="store_true",
                default=False,
            )
        elif field.annotation in (
            Dict[str, str],
            Dict[str, List[str]],
            Union[Dict[str, str], NoneType],
            Union[Dict[str, List[str]], NoneType],
        ):
            default = _dict_to_defaults(field.default)

            parser.add_argument(
                *args,
                help=field.description,
                default=default,
                nargs=2,
                action="append",
            )

        elif field.annotation in (
            List[str],
            Union[List[str], NoneType],
        ):
            parser.add_argument(
                *args,
                help=field.description,
                default=field.default or None,
                nargs="+",
            )
        elif key:
            parser.add_argument(
                *args,
                help=field.description,
                default=field.default or None,
                type=type(field.default),
            )
    return parser


class Services(str, Enum):
    """Literal implementation for the cli."""

    zarr_stream = "zarr-stream"
    stream = "zarr-stream"
    data_portal = "zarr-stream"
    databrowser = "databrowser"
    search = "databrowser"
    stac = "stacapi"
    stacpi = "stacapi"
    stacbrowser = "stacapi"


def get_cert_file(
    cert_dir: Optional[str],
    cert_file: Optional[str],
    key_file: Optional[str],
) -> Tuple[str, str]:
    """Get the certificate (key and cert) if they are configured."""
    default_cert = default_key = ""
    if cert_dir:
        default_cert = str(Path(cert_dir) / "client-cert.pem")
        default_key = str(Path(cert_dir) / "client-key.pem")
    return cert_file or default_cert, key_file or default_key


def cli(argv: Optional[List[str]] = None) -> None:
    """Start the freva rest API."""
    cfg = ServerConfig()
    parser = create_arg_parser(cfg.model_fields)
    parser.add_argument(
        "--dev", action="store_true", help="Enable development mode"
    )
    parser.add_argument(
        "--n-workers",
        "-w",
        help="Number of parallel processes.",
        default=os.getenv("API_WORKER", "8"),
        type=int,
    )
    parser.add_argument(
        "--redis-ssl-certdir",
        help=(
            "The directory where the certficates are stored."
            " This can be used to instead of setting the cert, and key files."
        ),
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--reload", help="Enable hot reloading.", action="store_true"
    )

    args = parser.parse_args(argv)
    if args.debug:
        logger.setLevel(logging.DEBUG)
    ssl_cert, ssl_key = get_cert_file(
        args.redis_ssl_certdir, args.redis_ssl_certfile, args.redis_ssl_keyfile
    )
    defaults: Dict[str, str] = {
        "API_CONFIG": str(Path(args.config).expanduser().absolute()),
        "DEBUG": str(int(args.debug)),
        "API_REDIS_SSL_KEYFILE": str(ssl_key or ""),
        "API_REDIS_SSL_CERTFILE": str(ssl_cert or ""),
        "API_PROXY": args.proxy or f"http://{gethostname()}:{args.port}",
    }
    for key, value in args._get_kwargs():
        name = key.upper().removeprefix("API_")
        if key in ServerConfig.model_fields and key not in ["debug"]:
            annotation = ServerConfig.model_fields[key].annotation
            if _is_type_annotation(annotation, list):
                iter_value = value or []
                entries = [v.strip() for v in iter_value if v.strip()]
                defaults.setdefault(f"API_{name}", ",".join(entries))
            elif _is_type_annotation(annotation, dict):
                iter_value = value or []
                entries = list(set([f"{k}:{v}" for (k, v) in iter_value]))
                defaults.setdefault(f"API_{name}", ",".join(entries))
            else:
                defaults.setdefault(f"API_{name}", str(value or ""))
    defaults = {k: v for k, v in defaults.items() if v}
    if args.dev:
        from freva_rest.databrowser_api.mock import read_data

        for core in cfg.solr_cores:
            asyncio.run(read_data(core, cfg.solr_url))
    workers = {False: args.n_workers, True: None}
    with NamedTemporaryFile(suffix=".conf", prefix="env") as temp_f:
        env = "\n".join(
            set(
                [
                    f"{k}={v.strip()}"
                    for (k, v) in set(defaults.items())
                    if v.strip()
                ]
            )
        )
        Path(temp_f.name).write_text(env, encoding="utf-8")
        uvicorn.run(
            "freva_rest.api:app",
            host="0.0.0.0",
            port=args.port,
            reload=args.dev or args.reload,
            log_level=cfg.log_level,
            workers=workers[args.dev or args.reload],
            env_file=temp_f.name,
        )


if __name__ == "__main__":
    cli()
