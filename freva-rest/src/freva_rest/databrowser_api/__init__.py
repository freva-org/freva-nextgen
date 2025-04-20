"""Rest API for the freva databrowser."""

from freva_rest import __version__

from .core import Solr
from .stac import STAC, Asset, Item, Link

__all__ = [
    "__version__",
    "Solr",
    "STAC",
    "Item",
    "Link",
    "Asset",
]


if __name__ == "__main__":
    print(__version__)
