STAC API
========

The Freva STAC (SpatioTemporal Asset Catalog) API provides a standardized way to access climate datasets following the STAC specification. STAC is an open standard for geospatial data cataloguing, enabling consistent discovery and access of climate datasets, satellite imagery and spatiotemporal data.

The STAC API allows you to:

- Browse collections of climate datasets
- Search for specific data items across collections
- Access detailed metadata about datasets and their spatial/temporal coverage
- Use standardized filtering and pagination
- Integrate with STAC-compatible tools and libraries

Getting Started
---------------

The STAC API organizes data into **Collections** and **Items**:

- **Collections**: Groups of related datasets (e.g., "observations", "CMIP6")

- **Items**: Individual dataset files with geospatial and temporal metadata

Authentication is not required for read-only access to the STAC API.

---


.. _stacapi-landing-page:

Landing Page
~~~~~~~~~~~~~~~

.. http:get:: /api/freva-nextgen/stacapi/

    Get the STAC API landing page which provides information about the API version, 
    title, description, and links to collections and other resources. This serves 
    as the entry point for exploring available collections and items.

    :statuscode 200: STAC API landing page returned successfully
    :statuscode 503: Search backend error
    :resheader Content-Type: ``application/json``

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/stacapi/ HTTP/1.1
        Host: www.freva.dkrz.de

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "type": "Catalog",
            "id": "freva-nextgen",
            "title": "I'm a STAC API",
            "description": "FAIR data for the Freva NextGen",
            "stac_version": "1.1.0",
            "stac_extensions": ["https://api.stacspec.org/v1.0.0/core"],
            "conformsTo": [
                "https://api.stacspec.org/v1.0.0/core",
                "https://api.stacspec.org/v1.0.0/collections",
                "https://api.stacspec.org/v1.0.0/item-search"
            ],
            "links": [
                {
                    "rel": "self",
                    "href": "https://www.freva.dkrz.de/api/freva-nextgen/stacapi",
                    "type": "application/json",
                    "title": "Landing Page"
                },
                {
                    "rel": "data",
                    "href": "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/collections",
                    "type": "application/json",
                    "title": "Data Collections"
                }
            ]
        }

    Code examples
    ~~~~~~~~~~~~~

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X GET https://www.freva.dkrz.de/api/freva-nextgen/stacapi/

        .. code-tab:: python
            :caption: Python

            import requests
            response = requests.get("https://www.freva.dkrz.de/api/freva-nextgen/stacapi/")
            data = response.json()

        .. code-tab:: r
            :caption: gnuR

            library(httr)
            response <- GET("https://www.freva.dkrz.de/api/freva-nextgen/stacapi/")
            data <- jsonlite::fromJSON(content(response, as = "text", encoding = "utf-8"))

        .. code-tab:: julia
            :caption: Julia

            using HTTP, JSON
            response = HTTP.get("https://www.freva.dkrz.de/api/freva-nextgen/stacapi/")
            data = JSON.parse(String(HTTP.body(response)))

---

.. _stacapi-conformance:

Conformance Classes
~~~~~~~~~~~~~~~~~~~~~~~~

.. http:get:: /api/freva-nextgen/stacapi/conformance

    Get the conformance classes that the STAC API implementation conforms to. 
    This provides information about the supported features and capabilities of the API.

    :statuscode 200: Conformance classes returned successfully
    :statuscode 503: Search backend error
    :resheader Content-Type: ``application/json``

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/stacapi/conformance HTTP/1.1
        Host: www.freva.dkrz.de

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "conformsTo": [
                "https://api.stacspec.org/v1.0.0/core",
                "https://api.stacspec.org/v1.0.0/collections",
                "https://api.stacspec.org/v1.0.0/item-search"
            ]
        }

---

.. _stacapi-collections:

Collections
~~~~~~~~~~~~

.. http:get:: /api/freva-nextgen/stacapi/collections

    List all collections available in the STAC API. Each collection represents 
    a group of related items and provides metadata including ID, title, 
    description, and spatial/temporal extents.

    :statuscode 200: Collections list returned successfully
    :statuscode 503: Search backend error
    :resheader Content-Type: ``application/json``

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/stacapi/collections HTTP/1.1
        Host: www.freva.dkrz.de

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "collections": [
                {
                    "id": "observations",
                    "type": "Collection",
                    "stac_version": "1.1.0",
                    "title": "OBSERVATIONS",
                    "description": "Collection OBSERVATIONS",
                    "license": "proprietary",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[null, null]]}
                    },
                    "links": [
                        {
                            "rel": "items",
                            "href": "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/collections/observations/items",
                            "type": "application/geo+json",
                            "title": "Items"
                        }
                    ]
                }
            ]
        }

    Code examples
    ~~~~~~~~~~~~~

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X GET https://www.freva.dkrz.de/api/freva-nextgen/stacapi/collections

        .. code-tab:: python
            :caption: Python

            import requests
            response = requests.get("https://www.freva.dkrz.de/api/freva-nextgen/stacapi/collections")
            collections = response.json()["collections"]

---

.. _stacapi-collection-details:

Get Collection
~~~~~~~~~~~~~~~

.. http:get:: /api/freva-nextgen/stacapi/collections/(str:collection_id)

    Get a specific collection by its ID. Returns detailed metadata about 
    the collection including its extent, license, and available links.

    :param collection_id: The unique identifier for the collection
    :type collection_id: str

    :statuscode 200: Collection returned successfully
    :statuscode 404: Collection not found
    :statuscode 503: Search backend error
    :resheader Content-Type: ``application/json``

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/stacapi/collections/observations HTTP/1.1
        Host: www.freva.dkrz.de

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "id": "observations",
            "type": "Collection",
            "stac_version": "1.1.0",
            "title": "OBSERVATIONS",
            "description": "Collection OBSERVATIONS",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[null, null]]}
            },
            "links": [
                {
                    "rel": "items",
                    "href": "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/collections/observations/items",
                    "type": "application/geo+json",
                    "title": "Items"
                }
            ],
            "keywords": ["observations", "climate", "analysis", "freva"]
        }

---

.. _stacapi-collection-items:

Get Collection Items
~~~~~~~~~~~~~~~~~~~~~

.. http:get:: /api/freva-nextgen/stacapi/collections/(str:collection_id)/items

    Get items from a specific collection. Items can be filtered using various 
    query parameters such as limit, datetime range, and bounding box.

    :param collection_id: The unique identifier for the collection
    :type collection_id: str
    :query limit: Maximum number of items to return (1-1000)
    :type limit: int
    :query token: Pagination token in format "direction:collection_id:item_id"
    :type token: str
    :query datetime: Datetime range in RFC 3339 format (start-date/end-date or exact-date)
    :type datetime: str  
    :query bbox: Bounding box as "minx,miny,maxx,maxy"
    :type bbox: str

    :statuscode 200: Items returned successfully
    :statuscode 422: Invalid query parameters
    :statuscode 503: Search backend error
    :resheader Content-Type: ``application/geo+json``

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/stacapi/collections/observations/items?limit=2&bbox=-180,-90,180,90 HTTP/1.1
        Host: www.freva.dkrz.de

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/geo+json

        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "stac_version": "1.1.0",
                    "id": "1834103571652542466",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]
                    },
                    "properties": {
                        "variable": ["pr"],
                        "experiment": ["cmorph"],
                        "institute": ["CPC"],
                        "datetime": "2016-09-02T23:00:00Z"
                    },
                    "collection": "observations",
                    "bbox": [-180.0, -90.0, 180.0, 90.0]
                }
            ],
            "links": [
                {
                    "rel": "self",
                    "href": "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/collections/observations/items?limit=2",
                    "type": "application/geo+json"
                },
                {
                    "rel": "next",
                    "href": "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/collections/observations/items?limit=2&token=next%3Aobservations%3A1834103571652542467",
                    "type": "application/geo+json",
                    "method": "GET"
                }
            ]
        }

    Code examples
    ~~~~~~~~~~~~~

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X GET \
            'https://www.freva.dkrz.de/api/freva-nextgen/stacapi/collections/observations/items?limit=10&datetime=2016-01-01/2016-12-31'

        .. code-tab:: python
            :caption: Python

            import requests
            response = requests.get(
                "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/collections/observations/items",
                params={
                    "limit": 10,
                    "datetime": "2016-01-01/2016-12-31",
                    "bbox": "-10,40,10,60"
                }
            )
            items = response.json()

        .. code-tab:: r
            :caption: gnuR

            library(httr)
            response <- GET(
                "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/collections/observations/items",
                query = list(
                    limit = 10,
                    datetime = "2016-01-01/2016-12-31", 
                    bbox = "-10,40,10,60"
                )
            )
            data <- jsonlite::fromJSON(content(response, as = "text", encoding = "utf-8"))

---

.. _stacapi-collection-item-details:

Get Collection Item Details
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. http:get:: /api/freva-nextgen/stacapi/collections/(str:collection_id)/items/(str:item_id)

    Get a specific item from a collection. Returns detailed metadata about 
    the dataset including its geometry, properties, assets, and links.

    :param collection_id: The unique identifier for the collection
    :type collection_id: str
    :param item_id: The unique identifier for the item
    :type item_id: str

    :statuscode 200: Item returned successfully
    :statuscode 404: Item not found
    :statuscode 503: Search backend error
    :resheader Content-Type: ``application/json``

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/stacapi/collections/observations/items/1834103571652542466 HTTP/1.1
        Host: www.freva.dkrz.de

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "type": "Feature",
            "stac_version": "1.1.0",
            "id": "1834103571652542466",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]
            },
            "properties": {
                "variable": ["pr"],
                "experiment": ["cmorph"],
                "institute": ["CPC"],
                "datetime": "2016-09-02T23:00:00Z"
            },
            "collection": "observations",
            "bbox": [-180.0, -90.0, 180.0, 90.0],
            "assets": {
                "zarr-access": {
                    "href": "https://www.freva.dkrz.de/api/freva-nextgen/databrowser/load/freva?file=/path/to/file.nc",
                    "title": "Stream Zarr Data",
                    "type": "application/vnd+zarr",
                    "roles": ["data"]
                }
            }
        }

---

.. _stacapi-search:

Search (GET)
~~~~~~~~~~~~

.. http:get:: /api/freva-nextgen/stacapi/search

    Search for items across collections using query parameters. Supports spatial, 
    temporal, and property-based filtering with free text search capabilities.

    :query collections: Comma-separated list of collection IDs to search
    :type collections: str
    :query ids: Comma-separated list of item IDs to search
    :type ids: str
    :query bbox: Bounding box as "minx,miny,maxx,maxy"
    :type bbox: str
    :query datetime: Datetime range in RFC 3339 format
    :type datetime: str
    :query limit: Maximum number of items to return (1-1000)
    :type limit: int
    :query token: Pagination token for next/previous pages
    :type token: str
    :query q: Free text search query (comma-separated terms)
    :type q: str

    :statuscode 200: Search results returned successfully
    :statuscode 422: Invalid query parameters
    :statuscode 503: Search backend error
    :resheader Content-Type: ``application/geo+json``

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/stacapi/search?collections=observations&q=precipitation,temperature&limit=5 HTTP/1.1
        Host: www.freva.dkrz.de

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/geo+json

        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "stac_version": "1.1.0", 
                    "id": "1834103571652542466",
                    "properties": {
                        "variable": ["pr"],
                        "experiment": ["cmorph"]
                    },
                    "collection": "observations"
                }
            ],
            "links": [
                {
                    "rel": "self",
                    "href": "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/search?collections=observations&q=precipitation&limit=5",
                    "type": "application/geo+json"
                }
            ]
        }

    Code examples
    ~~~~~~~~~~~~~

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X GET \
            'https://www.freva.dkrz.de/api/freva-nextgen/stacapi/search?collections=observations&q=temperature&bbox=-180,-90,180,90&limit=10'

        .. code-tab:: python
            :caption: Python

            import requests
            response = requests.get(
                "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/search",
                params={
                    "collections": "observations,cmip6",
                    "q": "precipitation,temperature", 
                    "bbox": "-180,-90,180,90",
                    "datetime": "2020-01-01/2020-12-31",
                    "limit": 20
                }
            )
            results = response.json()

        .. code-tab:: r
            :caption: gnuR

            library(httr)
            response <- GET(
                "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/search",
                query = list(
                    collections = "observations",
                    q = "temperature",
                    bbox = "-180,-90,180,90",
                    limit = 10
                )
            )
            data <- jsonlite::fromJSON(content(response, as = "text", encoding = "utf-8"))

---

.. _stacapi-search-post:

Search (POST)
~~~~~~~~~~~~~

.. http:post:: /api/freva-nextgen/stacapi/search

    Search for items across collections using a JSON request body. Provides 
    the same functionality as the GET endpoint but allows for more complex 
    search parameters and supports arrays for certain fields.

    :reqbody collections: List of collection IDs to search
    :type collections: list[str]
    :reqbody ids: List of item IDs to search  
    :type ids: list[str]
    :reqbody bbox: Bounding box as [minx, miny, maxx, maxy]
    :type bbox: list[float]
    :reqbody datetime: Datetime range in RFC 3339 format
    :type datetime: str
    :reqbody limit: Maximum number of items to return
    :type limit: int
    :reqbody q: Free text search terms (string or array)
    :type q: str or list[str]

    :reqheader Content-Type: application/json

    :statuscode 200: Search results returned successfully
    :statuscode 422: Invalid request body
    :statuscode 503: Search backend error
    :resheader Content-Type: ``application/geo+json``

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        POST /api/freva-nextgen/stacapi/search HTTP/1.1
        Host: www.freva.dkrz.de
        Content-Type: application/json

        {
            "collections": ["observations", "cmip6"],
            "q": ["temperature", "precipitation"],
            "bbox": [-180, -90, 180, 90],
            "datetime": "2020-01-01/2020-12-31",
            "limit": 10
        }

    Code examples
    ~~~~~~~~~~~~~

    .. tabs::

        .. code-tab:: bash
            :caption: Shell

            curl -X POST \
            'https://www.freva.dkrz.de/api/freva-nextgen/stacapi/search' \
            -H "Content-Type: application/json" \
            -d '{
                "collections": ["observations"],
                "q": ["temperature"],
                "limit": 10
            }'

        .. code-tab:: python
            :caption: Python

            import requests
            response = requests.post(
                "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/search",
                json={
                    "collections": ["observations", "cmip6"],
                    "q": ["temperature", "precipitation"],
                    "bbox": [-180, -90, 180, 90],
                    "datetime": "2020-01-01/2020-12-31",
                    "limit": 20
                }
            )
            results = response.json()

---

.. _stacapi-queryables:

Queryables
~~~~~~~~~~~~

.. http:get:: /api/freva-nextgen/stacapi/queryables

    Get global queryables schema. Returns a JSON Schema document describing 
    the properties that can be used in filter expressions across all collections.

    :statuscode 200: Queryables schema returned successfully
    :statuscode 503: Search backend error
    :resheader Content-Type: ``application/schema+json``

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/stacapi/queryables HTTP/1.1
        Host: www.freva.dkrz.de

    Example Response
    ~~~~~~~~~~~~~~~~

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/schema+json

        {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": "https://www.freva.dkrz.de/api/freva-nextgen/stacapi/queryables",
            "type": "object",
            "title": "Queryables for Freva NextGen STAC API",
            "properties": {
                "id": {
                    "description": "Item identifier",
                    "type": "string"
                },
                "datetime": {
                    "description": "Item datetime",
                    "type": "string", 
                    "format": "date-time"
                },
                "variable": {
                    "description": "Climate variable",
                    "type": "string"
                }
            }
        }

---

.. _stacapi-collection-queryables:

Collection Queryables
~~~~~~~~~~~~~~~~~~~~~~

.. http:get:: /api/freva-nextgen/stacapi/collections/(str:collection_id)/queryables

    Get collection-specific queryables schema. Returns a JSON Schema document 
    describing the properties available for filtering items in a specific collection.

    :param collection_id: The unique identifier for the collection
    :type collection_id: str

    :statuscode 200: Collection queryables returned successfully
    :statuscode 404: Collection not found
    :statuscode 503: Search backend error
    :resheader Content-Type: ``application/schema+json``

    Example Request
    ~~~~~~~~~~~~~~~

    .. sourcecode:: http

        GET /api/freva-nextgen/stacapi/collections/observations/queryables HTTP/1.1
        Host: www.freva.dkrz.de



---


STAC-API Integration
====================

The Freva STAC API is fully compatible with STAC-compliant tools and libraries. You can use popular tools like:

- **Python**: `pystac-client`, `pystac`, `stackstac`
- **R**: `rstac`
- **JavaScript**: `@stac/client`

Python Example with `pystac-client`
--------------------------------------

.. code-tab:: python

    from pystac_client import Client

    # Connect to the STAC API
    catalog = Client.open("https://www.freva.dkrz.de/api/freva-nextgen/stacapi")

    # Search for items
    search = catalog.search(
        collections=["observations"],
        datetime="2020-01-01/2020-12-31",
        bbox=[-10, 40, 10, 60]
    )

    # Get items
    items = list(search.get_items())
    print(f"Found {len(items)} items")

---


.. note::
   Please note that in these examples, "https://www.freva.dkrz.de" is used as a placeholder URL. You should replace it with the actual URL of your Freva STAC API instance.

.. note::
   The STAC API follows cursor-based pagination. Use the `token` parameter with values from the `next` and `previous` links in responses to navigate through result pages.

.. note::
   Free text search (`q` parameter) searches across relevant metadata fields including variable names, experiments, models, and institutes. Multiple terms are combined with OR logic.

.. important::
   Data transaction and ingestion into the Freva STAC-API is managed by administrators using the `data-crawler <https://freva.gitlab-pages.dkrz.de/metadata-crawler-source/docs/>`_ tool. This has nothing to do with the STAC API itself, which is primarily focused on data discovery and access.

