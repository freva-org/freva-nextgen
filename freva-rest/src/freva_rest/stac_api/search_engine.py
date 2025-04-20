"""SearchEngine implementation for interacting with Solr backend for STAC-API.

This module provides a SearchEngine class that abstracts interactions with the Solr backend,
providing methods specific to the needs of a STAC-API implementation.
"""
import json
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
import aiohttp
from fastapi import HTTPException

from freva_rest.logger import logger
from freva_rest.rest import server_config
# TODO: we need to think if this is a good idea to use databrowser_api 
# as a dependency for the STAC API or we need to redifine the classes
# here again
from freva_rest.databrowser_api import Solr, Translator

def get_print():
    """
    Get the print function for debugging.
    
    Returns
    -------
    function
        The print function to use for debugging.
    """
    return str(Solr)


# class SearchEngine:
#     """
#     SearchEngine class to interact with Solr for STAC-API implementation.
    
#     This class abstracts the direct Solr interactions and provides methods
#     specific to the STAC-API needs. It handles querying the Solr backend,
#     parsing responses, and transforming data into formats suitable for
#     the STAC-API.
#     """
    
#     def __init__(self):
#         """Initialize the search engine with server configuration."""
#         self.config = server_config
#         self.timeout = aiohttp.ClientTimeout(total=30)
#         # We'll use the latest core by default for searching
#         self.core_url = self.config.get_core_url(self.config.solr_cores[-1])
#         self.select_url = f"{self.core_url}/select"
    
#     async def _query_solr(self, params: Dict[str, Any]) -> Dict[str, Any]:
#         """
#         Execute a query against Solr.
        
#         Parameters
#         ----------
#         params : Dict[str, Any]
#             Query parameters to send to Solr
            
#         Returns
#         -------
#         Dict[str, Any]
#             The Solr response
#         """
#         logger.info(f"Querying Solr with params: {params}")
        
#         async with aiohttp.ClientSession(timeout=self.timeout) as session:
#             try:
#                 async with session.get(self.select_url, params=params) as res:
#                     if res.status != 200:
#                         logger.error(f"Solr query failed with status {res.status}: {await res.text()}")
#                         raise HTTPException(status_code=503, detail="Search backend error")
                    
#                     return await res.json()
#             except Exception as e:
#                 logger.error(f"Failed to connect to Solr: {str(e)}")
#                 raise HTTPException(status_code=503, detail="Could not connect to search instance")
    
#     async def get_facets(self, facet_fields: List[str], query: str = "*:*") -> Dict[str, List[str]]:
#         """
#         Get facet values for the specified fields.
        
#         Parameters
#         ----------
#         facet_fields : List[str]
#             List of field names to get facets for
#         query : str, optional
#             Solr query to filter results, defaults to "*:*" (all records)
            
#         Returns
#         -------
#         Dict[str, List[str]]
#             Dictionary mapping field names to their facet values
#         """
#         params = {
#             "q": query,
#             "rows": 0,
#             "facet": "true",
#             "facet.mincount": 1,
#             "facet.limit": -1,
#             "facet.field": facet_fields,
#             "wt": "json"
#         }
        
#         response = await self._query_solr(params)
#         facet_counts = response.get("facet_counts", {}).get("facet_fields", {})
        
#         # Process facet format: Solr returns [value1, count1, value2, count2, ...]
#         # We convert it to [value1, value2, ...]
#         result = {}
#         for field, values in facet_counts.items():
#             processed_values = []
#             for i in range(0, len(values), 2):
#                 if i+1 < len(values):  # Ensure we have both value and count
#                     processed_values.append(values[i])
#             result[field] = processed_values
        
#         return result
        
#     async def get_facet_values(self, field: str, query: str = "*:*") -> List[str]:
#         """
#         Get all values for a specific facet field.
        
#         Parameters
#         ----------
#         field : str
#             Field name to get facet values for
#         query : str, optional
#             Solr query to filter results, defaults to "*:*" (all records)
            
#         Returns
#         -------
#         List[str]
#             List of facet values for the specified field
#         """
#         facets = await self.get_facets([field], query)
#         return facets.get(field, [])
    
#     async def count_items(self, query: str = "*:*") -> int:
#         """
#         Count the number of items matching a query.
        
#         Parameters
#         ----------
#         query : str, optional
#             Solr query to filter results, defaults to "*:*" (all records)
            
#         Returns
#         -------
#         int
#             Number of items matching the query
#         """
#         params = {
#             "q": query,
#             "rows": 0,
#             "wt": "json"
#         }
        
#         response = await self._query_solr(params)
#         return response.get("response", {}).get("numFound", 0)
    
#     async def get_time_extent(self, query: str = "*:*") -> Dict[str, str]:
#         """
#         Get the temporal extent (min and max dates) for items matching a query.
        
#         Parameters
#         ----------
#         query : str, optional
#             Solr query to filter results, defaults to "*:*" (all records)
            
#         Returns
#         -------
#         Dict[str, str]
#             Dictionary with 'start' and 'end' temporal bounds
#         """
#         params = {
#             "q": query,
#             "rows": 0,
#             "stats": "true",
#             "stats.field": "time",
#             "wt": "json"
#         }
        
#         response = await self._query_solr(params)
#         stats = response.get("stats", {}).get("stats_fields", {}).get("time", {})
        
#         # Default to wide range if no stats available
#         if not stats:
#             return {
#                 "start": "1900-01-01T00:00:00Z",
#                 "end": "2100-12-31T23:59:59Z"
#             }
        
#         # Extract min and max from stats
#         # Solr may return dates in different formats
#         min_date = stats.get("min", "1900-01-01T00:00:00Z")
#         max_date = stats.get("max", "2100-12-31T23:59:59Z")
        
#         # Handle cases where min/max are in array or bracket format
#         if isinstance(min_date, str) and "[" in min_date:
#             min_date = min_date.split(" TO ")[0].strip("[]")
#         if isinstance(max_date, str) and "[" in max_date:
#             max_date = max_date.split(" TO ")[1].strip("[]")
            
#         return {
#             "start": min_date,
#             "end": max_date
#         }
    
#     async def get_spatial_extent(self, query: str = "*:*") -> List[float]:
#         """
#         Get the spatial extent (bounding box) for items matching a query.
        
#         Parameters
#         ----------
#         query : str, optional
#             Solr query to filter results, defaults to "*:*" (all records)
            
#         Returns
#         -------
#         List[float]
#             Bounding box as [west, south, east, north]
#         """
#         # Since the bbox field is already stored in Solr, we extract min/max values
#         # We'll use stats.field if bbox is a numeric field, or facet for discrete values
        
#         # For the freva schema, bbox is of type bbox which is a custom field type
#         # We'll need custom handling to extract the extents
        
#         # For now, return a default global extent
#         # In a real implementation, you would query Solr to get the actual spatial extent
#         return [-180.0, -90.0, 180.0, 90.0]
    
#     async def search(self, query: str, limit: int = 10, offset: int = 0,
#                     fields: Optional[List[str]] = None) -> Dict[str, Any]:
#         """
#         Search for items matching a query.
        
#         Parameters
#         ----------
#         query : str
#             Solr query string
#         limit : int, optional
#             Maximum number of results to return, defaults to 10
#         offset : int, optional
#             Offset for pagination, defaults to 0
#         fields : List[str], optional
#             Fields to return in results, defaults to None (return all)
            
#         Returns
#         -------
#         Dict[str, Any]
#             Dictionary with 'docs' (list of documents) and 'numFound' (total count)
#         """
#         params = {
#             "q": query,
#             "rows": limit,
#             "start": offset,
#             "wt": "json",
#         }
        
#         if fields:
#             params["fl"] = fields
        
#         response = await self._query_solr(params)
        
#         return {
#             "docs": response.get("response", {}).get("docs", []),
#             "numFound": response.get("response", {}).get("numFound", 0)
#         }