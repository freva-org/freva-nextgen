"""core module that orchestrates different backend implementations."""

from typing import List, Literal

from fastapi import HTTPException

from freva_rest.config import ServerConfig
from freva_rest.utils.stats_utils import store_api_statistics

from .backends import BaseBackend, SolrBackend, RDBMSBackend, SearchEngineBackend
from .translation import Translator, FlavourType


class DataBrowserCore:
    """Core orchestrator for databrowser operations across different backends."""
    
    uniq_keys: tuple[str, str] = ("file", "uri")
    
    def __init__(
        self,
        config: ServerConfig,
        *,
        uniq_key: Literal["file", "uri"] = "file",
        flavour: FlavourType = "freva",
        start: int = 0,
        multi_version: bool = True,
        translate: bool = True,
        **query: list[str],
    ):
        self.config = config
        self.uniq_key = uniq_key
        self.multi_version = multi_version
        self.translator = Translator(flavour, translate)
        self.facets = self.translator.translate_query(query, backwards=True)
        
        # Determine which backend to use
        backend_type = config.secondary_backend_type or "solr"
        self.backend = self._create_backend(backend_type, start, **query)
    
    def _create_backend(self, backend_type: str, start: int, **query_params) -> BaseBackend:
        """Factory method to create the appropriate backend."""
        backend_map = {
            "solr": SolrBackend,
            "RDBMS": RDBMSBackend, 
            "SE": SearchEngineBackend,
        }
        
        backend_class = backend_map.get(backend_type)
        if not backend_class:
            # Default to Solr if unknown backend type
            backend_class = SolrBackend
            
        return backend_class(
            config=self.config,
            translator=self.translator,
            uniq_key=self.uniq_key,
            facets=self.facets,
            multi_version=self.multi_version,
            start=start,
            **query_params
        )
    
    @classmethod
    async def validate_parameters(
        cls,
        config: ServerConfig,
        *,
        uniq_key: Literal["file", "uri"] = "file",
        flavour: FlavourType = "freva",
        start: int = 0,
        multi_version: bool = False,
        translate: bool = True,
        **query: list[str],
    ) -> "DataBrowserCore":
        """Create an instance with parameter validation."""
        translator = Translator(flavour, translate)
        valid_facets = translator.valid_facets
        if multi_version:
            valid_facets = translator.valid_facets + ["version"]
        
        for key in query:
            key = key.lower().replace("_not_", "")
            if (
                key not in valid_facets
                and key not in ("time_select", "bbox_select", "zarr_stream") + cls.uniq_keys
            ):
                raise HTTPException(status_code=422, detail="Could not validate input.")
        
        return cls(
            config,
            flavour=flavour,
            translate=translate,
            uniq_key=uniq_key,
            start=start,
            multi_version=multi_version,
            **query,
        )
    
    async def store_results(
        self, num_results: int, status: int, endpoint: str = "databrowser"
    ) -> None:
        """Store query statistics."""
        facets = {k: "&".join(v) for (k, v) in self.facets.items()}
        
        await store_api_statistics(
            config=self.config,
            num_results=num_results,
            status=status,
            api_type="databrowser",
            endpoint=endpoint,
            query_params=facets,
            flavour=self.translator.flavour,
            uniq_key=self.uniq_key
        )
    
    # Delegate all operations to the backend
    async def init_intake_catalogue(self):
        """Create an intake catalogue."""
        return await self.backend.init_intake_catalogue()
    
    async def extended_search(self, facets: List[str], max_results: int, zarr_stream: bool = False):
        """Perform extended search with facets."""
        return await self.backend.extended_search(facets, max_results, zarr_stream)
    
    async def init_stream(self):
        """Initialize search stream."""
        return await self.backend.init_stream()
    
    async def stream_response(self):
        """Stream search results."""
        async for result in self.backend.stream_response():
            yield result
    
    async def intake_catalogue(self, catalogue, header_only: bool = False):
        """Stream intake catalogue."""
        async for result in self.backend.intake_catalogue(catalogue, header_only):
            yield result
    
    async def zarr_response(self, catalogue_type, num_results: int):
        """Create zarr endpoint response."""
        async for result in self.backend.zarr_response(catalogue_type, num_results):
            yield result
    
    def configure_base_search(self):
        """Set up basic search configuration."""
        if hasattr(self.backend, 'configure_base_search'):
            return self.backend.configure_base_search()
    
    def set_query_params(self, **params):
        """Set multiple query parameters."""
        if hasattr(self.backend, 'set_query_params'):
            return self.backend.set_query_params(**params)
    
    async def add_user_metadata(self, user_name: str, user_metadata: list, **fwrites):
        """Add user metadata."""
        if hasattr(self.backend, 'add_user_metadata'):
            return await self.backend.add_user_metadata(user_name, user_metadata, **fwrites)
    
    async def delete_user_metadata(self, user_name: str, search_keys: dict):
        """Delete user metadata."""
        if hasattr(self.backend, 'delete_user_metadata'):
            return await self.backend.delete_user_metadata(user_name, search_keys)
    
    def _session_get(self):
        return self.backend._session_get()

    def _set_catalogue_queries(self):
        """Set catalogue query parameters."""
        if hasattr(self.backend, '_set_catalogue_queries'):
            return self.backend._set_catalogue_queries()

    @property
    def query(self):
        """Get backend query parameters."""
        if hasattr(self.backend, 'query'):
            return self.backend.query
        return {}

    @property
    def batch_size(self):
        """Get backend batch size."""
        if hasattr(self.backend, 'batch_size'):
            return self.backend.batch_size
        return 150
    
# For the sake of backward compatibility
Solr = DataBrowserCore