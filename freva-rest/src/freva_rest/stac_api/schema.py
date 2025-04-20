from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

# Constants for STAC API conformance
STAC_API_VERSION = "1.0.0"
STAC_VERSION = "1.0.0"

CONFORMANCE_URLS = [
    "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text",
    "http://www.opengis.net/spec/cql2/1.0/conf/basic-cql2",
    "http://www.opengis.net/spec/cql2/1.0/conf/cql2-json",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
    "https://api.stacspec.org/v1.0.0/item-search#fields",
    "https://api.stacspec.org/v1.0.0/ogcapi-features",
    "https://api.stacspec.org/v1.0.0-rc.2/item-search#filter",
    "https://api.stacspec.org/v1.0.0/collections",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
    "https://api.stacspec.org/v1.0.0/item-search#sort",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
    "https://api.stacspec.org/v1.0.0/core",
    "https://api.stacspec.org/v1.0.0/item-search#query",
    "https://api.stacspec.org/v1.0.0/item-search",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30"
]


class CONFORMANCE(BaseModel):
    """STAC Conformance response model."""
    conformsTo: List[str] = Field(
        default=CONFORMANCE_URLS,
        description="List of conformance URLs"
    )
    

class STACLinks(BaseModel):
    """STAC Links for navigation."""
    href: str = Field(..., description="Link URL")
    rel: str = Field(..., description="Link relation")
    type: Optional[str] = Field(None, description="Media type")
    title: Optional[str] = Field(None, description="Link title")


class STACConformance(BaseModel):
    """STAC Conformance response model."""
    conformsTo: List[str] = Field(..., description="List of conformance URLs")


class STACCollection(BaseModel):
    """STAC Collection model."""
    id: str = Field(..., description="Collection identifier")
    type: str = Field("Collection", description="Collection type")
    title: Optional[str] = Field(None, description="Collection title")
    description: str = Field(..., description="Collection description")
    license: str = Field("proprietary", description="Collection license")
    extent: Dict[str, Any] = Field(..., description="Collection spatial and temporal extent")
    links: List[STACLinks] = Field(..., description="Collection links")
    keywords: Optional[List[str]] = Field(None, description="Collection keywords")
    providers: Optional[List[Dict[str, Any]]] = Field(None, description="Collection providers")


class STACCollections(BaseModel):
    """STAC Collections response model."""
    collections: List[STACCollection] = Field(..., description="List of collections")
    links: List[STACLinks] = Field(..., description="Navigation links")


class STACItem(BaseModel):
    """STAC Item model."""
    id: str = Field(..., description="Item identifier")
    type: str = Field("Feature", description="GeoJSON type")
    geometry: Dict[str, Any] = Field(..., description="GeoJSON geometry")
    properties: Dict[str, Any] = Field(..., description="Item properties including datetime")
    links: List[STACLinks] = Field(..., description="Item links")
    assets: Dict[str, Any] = Field(..., description="Item assets")
    collection: str = Field(..., description="Collection this item belongs to")


class STACItemCollection(BaseModel):
    """STAC Item Collection response model."""
    type: str = Field("FeatureCollection", description="GeoJSON type")
    features: List[STACItem] = Field(..., description="List of STAC items")
    links: List[STACLinks] = Field(..., description="Navigation links")



"""Schema definitions for the FastAPI endpoints."""

from typing import Any, Dict, List, Union
from urllib.parse import parse_qs

from fastapi import Query, Request
from pydantic import BaseModel


Required: Any = Ellipsis


class LoadFiles(BaseModel):
    """Schema for the load file endpoint response."""

    urls: List[str]


class STACAPISchema:
    """Class holding all apache solr config parameters."""

    params: Dict[str, Any] = {
        "limit": Query(
            alias="max-results",
            title="Max. results",
            description="Control the number of maximum result items returned.",
            ge=0,
            le=1500,
        ),
        "token": Query(
            alias="token",
            title="Token",
            description="Token for pagination.",
            min_length=1,
            max_length=100,
        ),
        "datetime": Query(
            alias="datetime",
            title="Datetime",
            description="Datetime range for filtering items.",
            min_length=1,
            max_length=100,
        ),
        "bbox": Query(
            alias="bbox",
            title="Bounding Box",
            description="Bounding box for filtering items.",
            min_length=1,
            max_length=100,
        ),
    }

    @classmethod
    def process_parameters(
        cls, request: Request
    ) -> Dict[str, list[str]]:
        """Convert Starlette Request QueryParams to a dictionary."""

        query = parse_qs(str(request.query_params))
        for key, param in cls.params.items():
            _ = query.pop(key, [""])
            _ = query.pop(param.alias, [""])
        return query