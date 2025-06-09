from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import parse_qs

from fastapi import Query, Request
from pydantic import BaseModel, Field

STAC_VERSION = "1.1.0"

CONFORMANCE_URLS = [
    "https://api.stacspec.org/v1.0.0/core",
    "https://api.stacspec.org/v1.0.0/item-search",
    "https://api.stacspec.org/v1.0.0/collections",
    "https://api.stacspec.org/v1.0.0/ogcapi-features",
    "https://api.stacspec.org/v1.0.0-rc.1/item-search#free-text",
    "https://api.stacspec.org/v1.0.0-rc.1/ogcapi-features#free-text",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
    "http://www.opengis.net/spec/cql2/1.0/conf/basic-cql2",
    "http://www.opengis.net/spec/cql2/1.0/conf/cql2-json",
    "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text",
]


class STACLinks(BaseModel):
    """STAC Links model"""
    href: str = Field(..., description="Link URL")
    rel: str = Field(..., description="Link relation")
    type: Optional[str] = Field(None, description="Media type")
    title: Optional[str] = Field(None, description="Link title")
    method: Optional[str] = Field(None, description="HTTP method")
    merge: Optional[bool] = Field(None, description="Merge query parameters")
    body: Optional[Dict[str, Any]] = Field(
        None, description="Request body for POST links"
    )


class STACExtent(BaseModel):
    """STAC Extent model."""
    spatial: Dict[str, Any] = Field(..., description="Spatial extent")
    temporal: Dict[str, Any] = Field(..., description="Temporal extent")


class STACProvider(BaseModel):
    """STAC Provider model."""
    name: str = Field(..., description="Provider name")
    description: Optional[str] = Field(None, description="Provider description")
    roles: Optional[List[str]] = Field(None, description="Provider roles")
    url: Optional[str] = Field(None, description="Provider URL")


class LandingPageResponse(BaseModel):
    """STAC API Landing Page response model."""
    type: str = Field("Catalog", description="Type of STAC object")
    stac_version: str = Field(STAC_VERSION, description="STAC version")
    id: str = Field(..., description="Catalog identifier")
    title: Optional[str] = Field(None, description="Catalog title")
    description: str = Field(..., description="Catalog description")
    links: List[STACLinks] = Field(..., description="Navigation links")
    conformsTo: Optional[List[str]] = Field(None, description="Conformance URLs")

    class Config:
        json_schema_extra = {
            "example": {
                "type": "Catalog",
                "stac_version": "1.1.0",
                "id": "freva",
                "title": "FREVA STAC-API",
                "description": "FAIR data for Freva STAC-API",
                "links": [
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": "/api/freva-nextgen/stacapi/"
                    },
                    {
                        "rel": "conformance",
                        "type": "application/json",
                        "href": "/api/freva-nextgen/stacapi/conformance"
                    },
                    {
                        "rel": "collections",
                        "type": "application/json",
                        "href": "/api/freva-nextgen/stacapi/collections"
                    },
                    {
                        "rel": "search",
                        "type": "application/geo+json",
                        "href": "/api/freva-nextgen/stacapi/search",
                        "method": "GET"
                    },
                    {
                        "rel": "search",
                        "type": "application/geo+json",
                        "href": "/api/freva-nextgen/stacapi/search",
                        "method": "POST"
                    }
                ]
            }
        }


class ConformanceResponse(BaseModel):
    """STAC API Conformance response model."""
    conformsTo: List[str] = Field(
        default=CONFORMANCE_URLS,
        description="List of conformance URLs that this API implementation conforms to"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "conformsTo": CONFORMANCE_URLS
            }
        }


class STACCollection(BaseModel):
    """STAC Collection response model."""
    type: str = Field("Collection", description="Type of STAC object")
    stac_version: str = Field(STAC_VERSION, description="STAC version")
    id: str = Field(..., description="Collection identifier")
    title: Optional[str] = Field(None, description="Collection title")
    description: str = Field(..., description="Collection description")
    keywords: Optional[List[str]] = Field(None, description="Collection keywords")
    license: str = Field("proprietary", description="Collection license")
    providers: Optional[List[STACProvider]] = Field(
        None, description="Collection providers"
    )
    extent: STACExtent = Field(
        ..., description="Collection spatial and temporal extent"
    )
    links: List[STACLinks] = Field(..., description="Collection links")
    summaries: Optional[Dict[str, Any]] = Field(
        None, description="Collection summaries"
    )
    assets: Optional[Dict[str, Any]] = Field(None, description="Collection assets")

    class Config:
        json_schema_extra = {
            "example": {
                "type": "Collection",
                "stac_version": "1.1.0",
                "id": "project",
                "title": "Freva project search parameters",
                "description": "A collection of data",
                "license": "proprietary",
                "extent": {
                    "spatial": {
                        "bbox": [[-180, -90, 180, 90]]
                    },
                    "temporal": {
                        "interval": [["2000-01-01T00:00:00Z", "2023-12-31T23:59:59Z"]]
                    }
                },
                "links": [
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": "/api/freva-nextgen/stacapi/collections/observations"
                    },
                    {
                        "rel": "items",
                        "type": "application/geo+json",
                        "href": (
                            "/api/freva-nextgen/stacapi/"
                            "collections/observations/items"
                        )
                    }
                ]
            }
        }


class CollectionsResponse(BaseModel):
    """STAC Collections list response model."""
    collections: List[STACCollection] = Field(..., description="List of collections")
    links: List[STACLinks] = Field(..., description="Navigation links")

    class Config:
        json_schema_extra = {
            "example": {
                "collections": [
                    {
                        "type": "Collection",
                        "stac_version": "1.1.0",
                        "id": "project",
                        "title": "Freva project search parameters",
                        "description": "FAIR data for Freva STAC-API",
                        "license": "proprietary",
                        "extent": {
                            "spatial": {"bbox": [[-180, -90, 180, 90]]},
                            "temporal": {
                                "interval": [
                                    ["2000-01-01T00:00:00Z", "2023-12-31T23:59:59Z"]
                                ]
                            }
                        },
                        "links": []
                    }
                ],
                "links": [
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": "/api/freva-nextgen/stacapi/collections"
                    }
                ]
            }
        }


class STACItem(BaseModel):
    """STAC Item response model."""
    type: str = Field("Feature", description="GeoJSON type")
    stac_version: str = Field(STAC_VERSION, description="STAC version")
    id: str = Field(..., description="Item identifier")
    geometry: Optional[Dict[str, Any]] = Field(
        None, description="GeoJSON geometry"
    )
    bbox: Optional[List[float]] = Field(None, description="Bounding box")
    properties: Dict[str, Any] = Field(
        ..., description="Item properties including datetime"
    )
    links: List[STACLinks] = Field(..., description="Item links")
    assets: Dict[str, Any] = Field(..., description="Item assets")
    collection: Optional[str] = Field(
        None, description="Collection this item belongs to"
    )
    stac_extensions: Optional[List[str]] = Field(
        None, description="STAC extensions used"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "type": "Feature",
                "stac_version": "1.1.0",
                "id": "12345678",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-180, -90],
                                     [180, -90],
                                     [180, 90],
                                     [-180, 90],
                                     [-180, -90]]]
                },
                "bbox": [-180, -90, 180, 90],
                "properties": {
                    "datetime": "2023-01-01T00:00:00Z",
                    "title": "cmip6 January 2023",
                    "description": "Monthly cmip data for January 2023"
                },
                "collection": "cmip6",
                "links": [
                    {
                        "rel": "self",
                        "type": "application/geo+json",
                        "href": (
                            "/api/freva-nextgen/stacapi/collections/"
                            "cmip6/items/12345678"
                        )
                    }
                ],
                "assets": {
                    "data": {
                        "href": "/path/to/data.nc",
                        "type": "application/netcdf",
                        "title": "data asset",
                    }
                }
            }
        }


class ItemCollectionResponse(BaseModel):
    """STAC Item Collection response model (FeatureCollection)."""
    type: str = Field("FeatureCollection", description="GeoJSON type")
    features: List[STACItem] = Field(
        ...,
        description="List of STAC items"
    )
    links: List[STACLinks] = Field(
        ...,
        description="Navigation links"
    )
    numberMatched: Optional[int] = Field(
        None,
        description="Total number of items that match the search criteria"
    )
    numberReturned: int = Field(
        ...,
        description="Number of items returned in this response"
    )
    timeStamp: Optional[str] = Field(
        None,
        description="Timestamp when the response was generated"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "stac_version": "1.1.0",
                        "id": "12345678",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [0, 0]
                        },
                        "properties": {
                            "datetime": "2023-01-01T00:00:00Z"
                        },
                        "collection": "cmip6",
                        "links": [],
                        "assets": {}
                    }
                ],
                "numberMatched": 100,
                "numberReturned": 10,
                "links": [
                    {
                        "rel": "self",
                        "type": "application/geo+json",
                        "href": "/api/freva-nextgen/stacapi/search?limit=10"
                    },
                    {
                        "rel": "next",
                        "type": "application/geo+json",
                        "href": (
                            "/api/freva-nextgen/stacapi/search?limit=10&"
                            "token=next:search:12345678"
                        )
                    }
                ]
            }
        }


class QueryablesResponse(BaseModel):
    """STAC Queryables response model (JSON Schema)."""
    schema_: str = Field(
        "https://json-schema.org/draft/2019-09/schema",
        alias="$schema", description="JSON Schema version"
    )
    type: str = Field("object", description="Schema type")
    title: Optional[str] = Field(None, description="Schema title")
    description: Optional[str] = Field(None, description="Schema description")
    properties: Dict[str, Any] = Field(..., description="Queryable properties")
    additional_properties: Optional[bool] = Field(
        None, alias="additionalProperties",
        description="Allow additional properties"
    )

    class Config:
        validate_by_name = True
        json_schema_extra = {
            "example": {
                "$schema": "https://json-schema.org/draft/2019-09/schema",
                "type": "object",
                "title": "Queryables",
                "description": "Queryable properties for STAC items",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Item identifier"
                    },
                    "collection": {
                        "type": "string",
                        "description": "Collection identifier"
                    },
                    "datetime": {
                        "type": "string",
                        "format": "date-time",
                        "description": "Item datetime"
                    },
                    "geometry": {
                        "type": "object",
                        "description": "Item geometry"
                    }
                }
            }
        }


class PingResponse(BaseModel):
    """Ping/Health check response model."""
    message: str = Field(..., description="Health check message")

    class Config:
        json_schema_extra = {
            "example": {
                "message": "PONG"
            }
        }


class SearchPostRequest(BaseModel):
    """STAC API search POST request model with enhanced validation."""

    collections: Optional[List[str]] = Field(
        None,
        description="Array of collection IDs to search"
    )
    ids: Optional[List[str]] = Field(
        None,
        description="Array of item IDs to search"
    )
    bbox: Optional[Tuple[float, float, float, float]] = Field(
        None,
        description="Bounding box [minx, miny, maxx, maxy]",
    )
    intersects: Optional[Dict[str, Any]] = Field(
        None,
        description="GeoJSON geometry for spatial intersection"
    )
    datetime: Optional[str] = Field(
        None,
        description="Datetime range in RFC 3339 format"
    )
    limit: Optional[int] = Field(
        10,
        description="Maximum number of items to return",
        ge=1,
        le=1000
    )
    token: Optional[str] = Field(None, description="Pagination token")

    q: Optional[Union[str, List[str]]] = Field(
        None,
        description=(
            "Free text search. String for GET requests "
            "(comma-separated), array for POST requests"
        ),
        examples=["cmip6", "amip", "temp"],
    )
    query: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional property-based queries"
    )
    sortby: Optional[List[Dict[str, str]]] = Field(
        None,
        description="Sort criteria with field and direction"
    )
    fields: Optional[Dict[str, List[str]]] = Field(
        None,
        description="Fields to include or exclude"
    )
    filter: Optional[Dict[str, Any]] = Field(
        None,
        description="CQL filter expression"
    )


class STACAPISchema:
    """Class holding all Apache Solr config parameters."""

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


class CONFORMANCE(BaseModel):
    """STAC Conformance response model (legacy)."""
    conformsTo: List[str] = Field(
        default=CONFORMANCE_URLS,
        description="List of conformance URLs"
    )


class STACConformance(BaseModel):
    """STAC Conformance response model (legacy)."""
    conformsTo: List[str] = Field(..., description="List of conformance URLs")


class STACCollections(BaseModel):
    """STAC Collections response model (legacy)."""
    collections: List[STACCollection] = Field(..., description="List of collections")
    links: List[STACLinks] = Field(..., description="Navigation links")


class STACItemCollection(BaseModel):
    """STAC Item Collection response model (legacy)."""
    type: str = Field("FeatureCollection", description="GeoJSON type")
    features: List[STACItem] = Field(..., description="List of STAC items")
    links: List[STACLinks] = Field(..., description="Navigation links")
