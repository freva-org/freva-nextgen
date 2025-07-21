"""Base backend interface for data search."""

from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, List, Tuple, Union

from pydantic import BaseModel
from contextlib import asynccontextmanager
from ..translation.translator import Translator
from freva_rest.config import ServerConfig


class SearchResult(BaseModel):
    """Return Model of a uniq key search."""

    total_count: int
    facets: Dict[str, List[Union[str, int]]]
    search_results: List[Dict[str, Union[str, float, List[str]]]]
    facet_mapping: Dict[str, str]
    primary_facets: List[str]


class IntakeCatalogue(BaseModel):
    """Return Model of an intake catalogue search."""

    catalogue: Dict[str, Any]
    total_count: int


class BaseBackend(ABC):
    """Abstract base class for all search backends."""

    def __init__(
        self,
        config: ServerConfig,
        translator: Translator,
        uniq_key: str,
        facets: Dict[str, List[str]],
        multi_version: bool = False,
    ):
        self.config = config
        self.translator = translator
        self.uniq_key = uniq_key
        self.facets = facets
        self.multi_version = multi_version
        self.batch_size = 150

    @abstractmethod
    async def init_intake_catalogue(self) -> Tuple[int, IntakeCatalogue]:
        """Create an intake catalogue from the backend search."""
        pass

    @abstractmethod
    async def extended_search(
        self,
        facets: List[str],
        max_results: int,
        zarr_stream: bool = False,
    ) -> Tuple[int, SearchResult]:
        """Perform extended search with facets."""
        pass

    @abstractmethod
    async def init_stream(self) -> Tuple[int, int]:
        """Initialize search stream."""
        pass

    @abstractmethod
    async def stream_response(self) -> AsyncIterator[str]:
        """Stream search results."""
        pass

    @abstractmethod
    async def intake_catalogue(
        self, catalogue: Dict[str, Any], header_only: bool = False
    ) -> AsyncIterator[str]:
        """Stream intake catalogue."""
        pass

    @abstractmethod
    async def zarr_response(
        self,
        catalogue_type: str,
        num_results: int,
    ) -> AsyncIterator[str]:
        """Create zarr endpoint response."""
        pass

    def configure_base_search(self) -> None:
        """Set up basic search configuration."""
        pass

    def set_query_params(self, **params) -> None:
        """Set multiple query parameters."""
        pass

    @asynccontextmanager
    async def _session_get(self):
        """Session get method - default implementation."""
        raise NotImplementedError("Backend does not support _session_get")
        yield

    def _set_catalogue_queries(self) -> None:
        """Set the query parameters for a catalogue search."""
        pass        