"""Definition of the whole rest api."""

from databrowser_api.endpoints import *  # noqa: F401
from freva_rest.rest import app

__all__ = ["app"]
