"""Rest API for the freva databrowser."""

from freva_rest import __version__

from .core import Solr

__all__ = [
    "__version__",
    "Solr",
]

if __name__ == "__main__":
    print(__version__)
