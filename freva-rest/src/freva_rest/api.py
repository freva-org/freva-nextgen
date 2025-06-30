"""Definition of the whole rest api."""
from freva_rest.auth import *  # noqa: F401
from freva_rest.config import ServerConfig
from freva_rest.databrowser_api.endpoints import *  # noqa: F401
from freva_rest.freva_data_portal.endpoints import *  # noqa: F401
from freva_rest.rest import app

from .logger import logger

server_config = ServerConfig()
if "stacapi" in server_config.services:
    from freva_rest.stac_api.endpoints import *  # noqa: F401
    logger.info("STAC API endpoints loaded")

__all__ = ["app"]
