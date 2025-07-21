"""Backend implementations for different data storage and search systems."""

from .base import BaseBackend, SearchResult, IntakeCatalogue
from .solr import SolrBackend
from .rdbms import RDBMSBackend
from .search_engine import SearchEngineBackend

__all__ = [
    "BaseBackend",
    "SearchResult", 
    "IntakeCatalogue",
    "SolrBackend",
    "RDBMSBackend", 
    "SearchEngineBackend",
]