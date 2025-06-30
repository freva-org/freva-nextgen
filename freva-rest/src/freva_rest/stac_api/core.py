import json
import sys
from textwrap import dedent
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
)
from urllib.parse import urlencode

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder

from freva_rest.config import ServerConfig
from freva_rest.databrowser_api import Solr
from freva_rest.logger import logger
from freva_rest.utils.stac_utils import (
    Asset,
    Item,
    Link,
    parse_bbox,
    parse_datetime,
)
from freva_rest.utils.stats_utils import store_api_statistics

from .schema import (
    CONFORMANCE_URLS,
    STAC_VERSION,
    STACCollection,
    STACExtent,
    STACLinks,
    STACProvider,
)


class STACAPI:
    """STAC API implementation for the Freva and at
    the moment only with Solr Backend.

    Explanation about the structure:
    In this implementation we consider the `project`
    as `collection` name and each file under each
    project as an `item`.
    """

    def __init__(
        self,
        config: ServerConfig,
        *,
        limit: int = 12,
        token: Optional[str] = None,
        datetime: Optional[str] = None,
        bbox: Optional[str] = None,
        uniuq_key: Literal["file", "uri"] = "file",
        **query: list[str],
    ) -> None:
        self.config = config
        self.uniq_key = uniuq_key
        self.solr_object = Solr(config, multi_version=False)
        self.stacapi_query = query
        self.limit = limit
        self.token = token
        self.datetime = datetime
        self.bbox = bbox
        self.batch_size = 150

    @classmethod
    async def validate_parameters(
        cls,
        config: ServerConfig,
        *,
        limit: int = 12,
        token: Optional[str] = None,
        datetime: Optional[str] = None,
        bbox: Optional[str] = None,
        uniuq_key: Literal["file", "uri"] = "file",
        **query: list[str],
    ) -> "STACAPI":
        """
        Validate the parameters for the STAC API.
        Parameters
        ----------
        config : ServerConfig
            Server configuration object.
        limit : int, optional
            Limit for the number of items to return.
        token : str, optional
            Token for authentication.
        datetime : str, optional
            Datetime range for filtering items.
        bbox : list[float], optional
            Bounding box for filtering items.
        uniuq_key : str, optional
            Unique key for the items.
        query : list[str], optional
            Additional query parameters.
        Returns
        -------
        STACAPI
            STACAPI object with validated parameters.
        """

        caller_name = sys._getframe(1).f_code.co_name

        for key in query:
            if (
                key not in ["datetime", "bbox", "limit", "token", "q"]
            ) and caller_name == "collection_items":
                raise HTTPException(status_code=422, detail="Could not validate input.")

        return cls(
            config=config,
            limit=limit,
            token=token,
            datetime=datetime,
            bbox=bbox,
            uniuq_key=uniuq_key,
            **query,
        )

    async def _set_solr_query(self) -> None:
        """
        Set the Solr query for the STAC API.
        """
        self.solr_object.configure_base_search()

    async def get_all_project_facets(self) -> List[str]:
        """Get all project facets from Solr."""
        await self._set_solr_query()
        self.solr_object.set_query_params(
            facet_field=["project"],
            rows=self.batch_size
        )
        async with self.solr_object._session_get() as res:
            _, search = res
        project_facets = (
            search.get("facet_counts", {}).get("facet_fields", {}).get("project", [])
        )
        if project_facets == []:  # pragma: no cover
            logger.error("No project facets found in Solr response.")
            return []
        return cast(
            List[str],
            (
                search.get("facet_counts", {})
                .get("facet_fields", {})
                .get("project", [])[::2]
            ),
        )

    async def store_results(
        self,
        num_results: int,
        status: int,
        endpoint: str,
        query_params: Optional[Dict[str, Any]] = None
    ) -> None:
        """Store STAC API query statistics.

        Parameters
        ----------
        num_results: int
            The number of results returned
        status: int
            The HTTP request status
        endpoint: str
            The STAC API endpoint name
        query_params: Optional[Dict[str, Any]]
            Query parameters used in the request
        """
        await store_api_statistics(
            config=self.config,
            num_results=num_results,
            status=status,
            api_type="stacapi",
            endpoint=endpoint,
            query_params=query_params or {},
            uniq_key=self.uniq_key,
            limit=self.limit
        )

    async def get_landing_page(self) -> Dict[str, Any]:
        """Get the STAC API landing page."""
        # TODO: We need to outsource the harcoded detail
        # and description to somewhere else
        collection_ids = await self.get_all_project_facets()
        response = {
            "type": "Catalog",
            "id": "freva",
            "title": "Freva STAC-API",
            "description": "FAIR data for the Freva",
            "stac_version": STAC_VERSION,
            "stac_extensions": ["https://api.stacspec.org/v1.0.0/core"],
            "conformsTo": CONFORMANCE_URLS,
            "links": [
                {
                    "rel": "self",
                    "href": self.config.proxy + "/api/freva-nextgen/stacapi",
                    "type": "application/json",
                    "title": "Landing Page",
                },
                {
                    "rel": "conformance",
                    "href": (
                        self.config.proxy + "/api/freva-nextgen/stacapi/conformance"
                    ),
                    "type": "application/json",
                    "title": "Conformance Classes",
                },
                {
                    "rel": "data",
                    "href": (
                        self.config.proxy + "/api/freva-nextgen/stacapi/collections"
                    ),
                    "type": "application/json",
                    "title": "Data Collections",
                },
                {
                    "rel": "search",
                    "href": self.config.proxy + "/api/freva-nextgen/stacapi/search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "method": "POST",
                },
                {
                    "rel": "search",
                    "href": self.config.proxy + "/api/freva-nextgen/stacapi/search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "method": "GET",
                },
                {
                    "rel": "queryables",
                    "href": self.config.proxy + "/api/freva-nextgen/stacapi/queryables",
                    "type": "application/schema+json",
                    "title": "Queryables",
                },
                {
                    "rel": "service-desc",
                    "type": "application/vnd.oai.openapi+json;version=3.0",
                    "title": "OpenAPI service description",
                    "href": self.config.proxy + "/api/freva-nextgen/help/openapi.json",
                },
                {
                    "rel": "service-doc",
                    "type": "text/html",
                    "title": "OpenAPI service documentation",
                    "href": self.config.proxy + "/api/freva-nextgen/help#tag/STAC-API",
                },
            ],
        }
        # Add child links for each collection (based on the STAC-API SPEC)
        if collection_ids != []:
            for collection_id in collection_ids:
                cast(List[Dict[str, str]], response["links"]).append(
                    {
                        "rel": "child",
                        "href": (
                            self.config.proxy
                            + "/api/freva-nextgen/stacapi/collections/"
                            + collection_id
                        ),
                        "type": "application/json",
                    }
                )
        return response

    async def get_collection(self, collection_id: str) -> STACCollection:
        """Get a specific collection."""
        # TODO: We need to define a new core in Solr which contains the
        # description of each collection and all other metadata we need
        # for constructing this. For time being we define them all as
        # constants, since we don't have any usecase for this yet.
        # TODO: We need to add assets to the collections

        collection_ids = await self.get_all_project_facets()
        if collection_id not in collection_ids:
            raise HTTPException(
                status_code=404, detail=f"Collection {collection_id} not found"
            )
        return STACCollection(
            id=collection_id,
            type="Collection",
            stac_version="1.1.0",
            title=collection_id.upper(),
            description=f"Collection {collection_id.upper()}",
            license="proprietary",
            summaries=None,
            # TODO: we need to take care of extend somehow
            # it seems with None it's still valid
            extent=STACExtent(
                spatial={"bbox": [[-180, -90, 180, 90]]},
                temporal={"interval": [[None, None]]}
            ),
            links=[
                STACLinks(
                    rel="self",
                    href=(
                        self.config.proxy
                        + "/api/freva-nextgen/stacapi"
                        + "/collections/"
                        + collection_id
                    ),
                    type="application/json",
                    title="Collection",
                    method="GET",
                    merge=True,
                    body=None
                ),
                STACLinks(
                    rel="parent",
                    href=self.config.proxy + "/api/freva-nextgen/stacapi/",
                    type="application/json",
                    title="Landing Page",
                    method="GET",
                    merge=True,
                    body=None,
                ),
                STACLinks(
                    rel="root",
                    href=self.config.proxy + "/api/freva-nextgen/stacapi/",
                    type="application/json",
                    title="Root",
                    method="GET",
                    merge=True,
                    body=None,
                ),
                STACLinks(
                    rel="items",
                    href=(
                        self.config.proxy
                        + "/api/freva-nextgen/stacapi"
                        + "/collections/"
                        + collection_id
                        + "/items"
                    ),
                    type="application/geo+json",
                    title="Items",
                    method="GET",
                    merge=True,
                    body=None,
                ),
                STACLinks(
                    rel="queryables",
                    href=(
                        self.config.proxy
                        + "/api/freva-nextgen/stacapi"
                        + "/collections/"
                        + collection_id
                        + "/queryables"
                    ),
                    type="application/schema+json",
                    title="Queryables",
                    method="GET",
                    merge=True,
                    body=None,
                ),
                STACLinks(
                    rel="license",
                    href="https://opensource.org/license/bsd-3-clause",
                    title="BSD 3-Clause 'New' or 'Revised' License",
                    type="text/html",
                    method="GET",
                    merge=True,
                    body=None,
                ),
            ],
            keywords=[collection_id, "climate", "analysis", "freva"],
            providers=[
                STACProvider(
                    name="Freva",
                    description=(
                        "The Freva is a platform for climate data analysis and "
                        "evaluation, providing access to various datasets and tools."
                    ),
                    roles=["producer", "processor", "host"],
                    url=self.config.proxy + "/api/freva-nextgen/stacapi"
                )
            ],
            assets=None
        )

    async def get_collections(self) -> AsyncGenerator[str, None]:
        """Get all collections (as STAC Collections)."""
        collection_ids = await self.get_all_project_facets()
        yield '{"collections": ['
        first_item = True

        for collection_id in collection_ids:
            collection = await self.get_collection(collection_id)
            if not first_item:
                yield ","
            else:
                first_item = False
            yield collection.json(exclude_none=True)
        links = [
            STACLinks(
                rel="self",
                href=f"{self.config.proxy}/api/freva-nextgen/stacapi/collections",
                type="application/json",
                title="Collections",
                method="GET",
                merge=True,
                body=None,
            ),
            STACLinks(
                rel="parent",
                href=f"{self.config.proxy}/api/freva-nextgen/stacapi",
                type="application/json",
                title="Landing Page",
                method="GET",
                merge=True,
                body=None,
            ),
            STACLinks(
                rel="root",
                href=f"{self.config.proxy}/api/freva-nextgen/stacapi",
                type="application/json",
                title="Root",
                method="GET",
                merge=True,
                body=None
            ),
        ]
        yield f'], "links": {json.dumps(jsonable_encoder(links))}}}'

    async def create_stac_item(
        self,
        result: Dict[str, Any],
        collection_id: str,
    ) -> Item:
        """Create a STAC item from the Solr doc."""

        id = result.get(self.uniq_key, "")
        zarr_desc = dedent(
            f"""
            # Accessing Zarr Data
            1. Install freva-client
            ```bash
            pip install freva-client
            ```
            2. (Python) Get the auth token and access the zarr data - recommended
            ```python
            from freva_client import databrowser
            import xarray as xr
            db = databrowser({self.uniq_key}='{id}', \\
                            stream_zarr=True, host='{self.config.proxy}')
            xarray_dataset = xr.open_mfdataset(
            list(db),
            chunks="auto",
            engine="zarr",
            storage_options={{"headers":
                    {{
                    "Authorization": f"Bearer {{db.auth_token['access_token']}}"
                    }}
            }}
            )
            ```
            3. (CLI) Get token then access:
            ```bash
            freva-client databrowser data-search {self.uniq_key}={id} \\
                --zarr --host {self.config.proxy}
            ```
            4. Access the zarr data directly (API - language agnostic)
            ```bash
            curl -X GET {self.config.proxy}api/ \\
            freva-nextgen/databrowser/load/\\
            freva?\\
            {self.uniq_key}={id} \\
              -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
            ```
            💡: Read more about the
            [freva-client](https://freva-clint.github.io/freva-nextgen/)
            """
        )  # noqa: E501
        local_access_desc = dedent(
            f"""
            # Accessing data locally where the data is stored
            ```python
            import xarray as xr
            ds = xr.open_mfdataset('{id}')
            ```
            💡: Ensure required xarray packages are installed.
            """
        )
        item_id = str(result.get("_version_"))
        bbox = result.get("bbox")
        if bbox:
            try:
                bbox = parse_bbox(bbox)
            except ValueError as e:  # pragma: no cover
                logger.warning(f"Invalid bbox for {id}: {e}")
                bbox = None

        time = result.get("time")
        start_time = end_time = None
        if time:
            try:
                start_time, end_time = parse_datetime(time)
            except ValueError as e:  # pragma: no cover
                logger.warning(f"Invalid datetime for {id}: {e}")

        geometry = None
        if bbox:
            geometry = {
                "type": "Polygon",
                "coordinates": [
                    [
                        [bbox[0], bbox[1]],
                        [bbox[2], bbox[1]],
                        [bbox[2], bbox[3]],
                        [bbox[0], bbox[3]],
                        [bbox[0], bbox[1]],
                    ]
                ],
            }

        properties = {
            **{
                k: result.get(k)
                for k in self.config.solr_fields
                if k in result and result.get(k) is not None
            },
            "title": id,
        }
        item = Item(
            id=item_id,
            collection=collection_id,
            geometry=geometry,
            properties=properties,
            bbox=bbox,
        )
        if start_time and end_time:
            item.properties["start_datetime"] = start_time.isoformat() + "Z"
            item.properties["end_datetime"] = end_time.isoformat() + "Z"
            item.properties["datetime"] = start_time.isoformat() + "Z"
        base_url = f"{self.config.proxy}/api/freva-nextgen/stacapi"
        links_to_add = [
            {
                "rel": "self",
                "target": f"{base_url}/collections/{collection_id}/items/{item_id}",
                "media_type": "application/json",
            },
            {
                "rel": "root", "target": base_url + "/",
                "media_type": "application/json"},
            {
                "rel": "parent",
                "target": f"{base_url}/collections/{collection_id}",
                "media_type": "application/json",
            },
            {
                "rel": "collection",
                "target": f"{base_url}/collections/{collection_id}",
                "media_type": "application/json",
            },
        ]
        for link_info in links_to_add:
            if not any(
                link.rel == link_info["rel"] for link in item.links
            ):
                link = Link(
                    rel=link_info["rel"],
                    href=link_info["target"],
                    type=link_info["media_type"],
                    extra_fields={"noresolve": True},
                )
                item.add_link(link)

        assets = {
            "freva-databrowser": Asset(
                href=(f"{self.config.proxy}/databrowser/?" f"{self.uniq_key}={id}"),
                media_type="text/html",
                title="Freva Web DataBrowser",
                description=(
                    "Access the Freva web interface for data exploration and analysis"
                ),
                roles=["overview"],
            ),
            "zarr-access": Asset(
                href=(
                    f"{self.config.proxy}/api/freva-nextgen/"
                    f"databrowser/load/freva?"
                    f"{self.uniq_key}={id}"
                ),
                media_type="application/vnd+zarr",
                title="Stream Zarr Data",
                description=zarr_desc,
                roles=["data"],
                extra_fields={
                    "requires": ["oauth2"],
                    "authentication": {
                        "type": "oauth2",
                        "description": (
                            "Authentication using your Freva credentials is required."
                        ),
                    },
                },
            ),
            "local-access": Asset(
                href=(
                    f"{self.config.proxy}/api/freva-nextgen/"
                    f"databrowser/data-search/freva/"
                    f"{self.uniq_key}?"
                    f"{self.uniq_key}={id}"
                ),
                title="Access data locally",
                description=local_access_desc,
                roles=["data"],
                media_type="application/netcdf",
            ),
        }
        for key, asset in assets.items():
            item.add_asset(key, asset)

        return item

    async def _paginated_items_search(
        self,
        filters: List[str],
        limit: int,
        token: Optional[str],
        base_url: str,
        base_params: Dict[str, Any],
        context_id: str = "collection"
    ) -> AsyncGenerator[str, None]:
        """Shared pagination logic for both collection items and search."""
        direction = "next"
        first_loop = True
        items_returned = 0
        first_item_id = None
        last_item_id = None
        curr_query_count = 0
        has_prev = False
        has_next = False

        await self._set_solr_query()

        # Setup basic query parameters
        self.solr_object.set_query_params(
            facet_field=self.config.solr_fields + ["time", "bbox"],
            fl=[self.uniq_key] + self.config.solr_fields + [
                "time", "bbox", "_version_"
            ],
            sort="_version_ asc,file asc",
            fq=filters,
            rows=0
        )

        # Get total count before pagination
        async with self.solr_object._session_get() as res:
            _, search = res
        before_pagination_count = search.get("response", {}).get("numFound", 0)

        # Handle pagination token
        if token and ":" in token:
            direction, _, pivot_id = token.split(":", 2)
            if direction == "next":
                filters.append(f"_version_:{{{pivot_id} TO *}}")
            if direction == "prev":
                print(f"Pivot ID: {pivot_id}", file=sys.stderr)
                filters.append(f"_version_:{{* TO {pivot_id}}}")
                self.solr_object.set_query_params(sort="_version_ desc,file asc")

        # Update query with pagination filters
        self.solr_object.set_query_params(
            fq=filters,
            cursorMark="*",
            rows=self.batch_size
        )

        yield '{"type":"FeatureCollection","features":['
        print(f"{items_returned=}, {limit=}, {direction=}", file=sys.stderr)

        while items_returned < limit:
            async with self.solr_object._session_get() as res:
                _, results = res

            docs = results.get("response", {}).get("docs", [])
            if not docs:
                break

            for doc in docs:
                if items_returned >= limit:
                    break
                item_id = str(doc.get("_version_"))
                if items_returned == 0:
                    first_item_id = item_id
                    print(f"First item ID: {first_item_id}", file=sys.stderr)
                last_item_id = item_id

                project_value = doc.get("project", context_id)
                if isinstance(project_value, list) and project_value:
                    collection_id_for_item = project_value[0]
                else:
                    collection_id_for_item = project_value  # pragma: no cover
                item = await self.create_stac_item(doc, collection_id_for_item)
                text = json.dumps(item.to_dict(), default=str)

                if not first_loop:
                    yield ","
                else:
                    first_loop = False
                yield text
                items_returned += 1

            next_cursor_mark = results.get("nextCursorMark")
            if next_cursor_mark is None:
                break  # pragma: no cover
            self.solr_object.set_query_params(cursorMark=str(next_cursor_mark))
            curr_query_count = results.get("response", {}).get("numFound", 0)

        yield '],"links":['

        yield json.dumps(
            {
                "rel": "self",
                "href": f"{base_url}?{urlencode(base_params)}",
                "type": "application/geo+json",
            }
        )

        #####################################################################
        # illustration of the pagination links variables:
        # We consider we have 3 items with limit=1 which we would have 3
        # pages in total as a result.
        # Each Item has a unique _version_ field which is used to
        # as pivot for the pagination.
        #
        # Next links: forward paging
        # ┌────────────────────┐ ┌────────────────────┐ ┌────────────────────┐
        # │ Page 1             │ │ Page 2             │ │ Page 3             │
        # │ direction: next    │ │ direction: next    │ │ direction: next    │
        # │ item_id: None      │ │ item_id: 3         │ │ item_id: 5         │
        # │ more_beyond: True  │ │ more_beyond: True  │ │ more_beyond: False │
        # │ has_next:  True    │ │ has_next:  True    │ │ has_next:  False   │
        # │ has_prev:  False   │ │ has_prev:  True    │ │ has_prev:  True    │
        # └────────────────────┘ └────────────────────┘ └────────────────────┘
        # Previous links: backward paging
        # ┌────────────────────┐ ┌────────────────────┐ ┌────────────────────┐
        # │ Page 3             │ │ Page 2             │ │ Page 1             │
        # │ direction: prev    │ │ direction: prev    │ │ direction: prev    │
        # │ item_id: 7         │ │ item_id: 5         │ │ item_id: 3         │
        # │ more_beyond: True  │ │ more_beyond: True  │ │ more_beyond: False │
        # │ has_prev:  True    │ │ has_prev:  True    │ │ has_prev:  False   │
        # │ has_next:  False   │ │ has_next:  True    │ │ has_next:  True    │
        # └────────────────────┘ └────────────────────┘ └────────────────────┘
        #####################################################################

        more_beyond_pivot = curr_query_count > limit
        pivot_at_collection_start = (
            direction == "next" and curr_query_count == before_pagination_count
        )
        pivot_at_collection_end = (
            direction == "prev" and curr_query_count == before_pagination_count
        )

        if direction == "next":
            has_prev = not pivot_at_collection_start
            has_next = more_beyond_pivot
            token__next = last_item_id
            token__prev = first_item_id
        else:  # direction == "prev"
            has_prev = more_beyond_pivot
            has_next = not pivot_at_collection_end
            token__next = first_item_id
            token__prev = last_item_id

        if has_prev:
            prev_params = {
                **base_params,
                "token": f"prev:{context_id}:{token__prev}",
            }
            yield ","
            yield json.dumps(
                {
                    "rel": "previous",
                    "href": f"{base_url}?{urlencode(prev_params)}",
                    "type": "application/geo+json",
                    "method": "GET",
                }
            )
        if has_next:
            next_params = {
                **base_params,
                "token": f"next:{context_id}:{token__next}",
            }
            yield ","
            yield json.dumps(
                {
                    "rel": "next",
                    "href": f"{base_url}?{urlencode(next_params)}",
                    "type": "application/geo+json",
                    "method": "GET",
                }
            )
        yield "]}"

    async def get_collection_items(
        self,
        collection_id: str,
        limit: int = 10,
        token: Optional[str] = None,
        datetime: Optional[str] = None,
        bbox: Optional[str] = None,
    ) -> AsyncGenerator[str, None]:
        """Get a all items of a specific collection."""
        base_params: Dict[str, Any] = {"limit": limit}
        if datetime:
            base_params["datetime"] = datetime
        if bbox:
            base_params["bbox"] = bbox
        base_url = (
            f"{self.config.proxy}/api/freva-nextgen/"
            f"stacapi/collections/{collection_id}/items"
        )

        filters = [f"project:{collection_id}"]

        # handle bbox and datetime parameters:
        if datetime:
            if "/" in datetime:
                start, end = datetime.split("/", 1)
                if start and end:
                    filters.append(f"time:[{start} TO {end}]")
            else:
                filters.append(f"time:[{datetime} TO *]")
        if bbox:
            coords = [float(c) for c in bbox.split(",")]
            minx, miny, maxx, maxy = coords
            bbox_fq = (
                "{{!field f=bbox}}" "Intersects(ENVELOPE({minx},{maxx},{maxy},{miny}))"
            ).format(minx=minx, maxx=maxx, maxy=maxy, miny=miny)
            filters.append(bbox_fq)

        async for chunk in self._paginated_items_search(
            filters, limit, token, base_url, base_params, collection_id
        ):
            yield chunk

    async def get_collection_item(self, collection_id: str, item_id: str) -> Item:
        """Get a specific item from a collection."""
        await self._set_solr_query()

        # Set all parameters at once
        self.solr_object.set_query_params(
            facet_field=self.config.solr_fields + ["time", "bbox"],
            fl=[self.uniq_key] + self.config.solr_fields + [
                "time", "bbox", "_version_"
            ],
            sort="_version_ asc,file asc",
            fq=[f"project:{collection_id}", f"_version_:{item_id}"],
            rows=1
        )

        async with self.solr_object._session_get() as res:
            _, search = res
        docs = search.get("response", {}).get("docs", [])
        if not docs:
            raise HTTPException(
                status_code=404,
                detail=f"Item {item_id} not found in collection {collection_id}",
            )
        item = await self.create_stac_item(docs[0], collection_id)
        return item

    async def get_search(
        self,
        collections: Optional[str] = None,
        ids: Optional[str] = None,
        bbox: Optional[str] = None,
        datetime: Optional[str] = None,
        limit: int = 10,
        token: Optional[str] = None,
        q: Optional[str] = None,
        query: Optional[str] = None,
        sortby: Optional[str] = None,
        fields: Optional[str] = None,
        filter: Optional[str] = None,
    ) -> AsyncGenerator[str, None]:
        """Execute GET search across collections."""

        collection_list = collections.split(",") if collections else None
        ids_list = ids.split(",") if ids else None
        q_terms: List[str] = []
        if q:
            q_terms = [term.strip() for term in q.split(",") if term.strip()]

        base_url = f"{self.config.proxy}/api/freva-nextgen/stacapi/search"
        base_params: Dict[str, Any] = {"limit": limit}
        if collections:
            base_params["collections"] = collections
        if datetime:
            base_params["datetime"] = datetime
        if bbox:
            base_params["bbox"] = bbox
        if q:
            base_params["q"] = q

        filters: List[str] = []

        # Collection filter
        if collection_list:
            collection_filter = " OR ".join(
                [f"project:{coll}" for coll in collection_list]
            )
            filters.append(f"({collection_filter})")

        # IDs filter
        if ids_list:
            ids_filter = " OR ".join(
                [f'{self.uniq_key}:"{item_id}"' for item_id in ids_list]
            )
            filters.append(f"({ids_filter})")

        # Free text search filter
        if q_terms:
            text_fields = self.config.solr_fields
            print(f"Text fields: {text_fields}", file=sys.stderr)
            q_filters: List[str] = []
            for term in q_terms:
                field_queries: List[str] = []
                for field in text_fields:
                    escaped_term = term.replace(":", "\\:").replace(" ", "\\ ")
                    field_queries.append(f"{field}:*{escaped_term}*")

                if field_queries:
                    q_filters.append(f"({' OR '.join(field_queries)})")

            if q_filters:
                filters.append(f"({' OR '.join(q_filters)})")

        # Datetime filter
        if datetime:
            if "/" in datetime:
                start, end = datetime.split("/", 1)
                if start and end:
                    filters.append(f"time:[{start} TO {end}]")
            else:
                filters.append(f"time:[{datetime} TO *]")  # pragma: no cover

        # Bbox filter
        if bbox:
            coords = [float(c) for c in bbox.split(",")]
            minx, miny, maxx, maxy = coords
            bbox_fq = (
                "{{!field f=bbox}}"
                "Intersects(ENVELOPE({minx},{maxx},{maxy},{miny}))"
            ).format(minx=minx, maxx=maxx, maxy=maxy, miny=miny)
            filters.append(bbox_fq)

        if not filters:
            filters = ["*:*"]

        async for chunk in self._paginated_items_search(
            filters, limit, token, base_url, base_params, "search"
        ):
            yield chunk

    async def post_search(
        self,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        intersects: Optional[Dict[str, str]] = None,
        datetime: Optional[str] = None,
        limit: int = 10,
        token: Optional[str] = None,
        q: Optional[Union[str, List[str]]] = None,
        query: Optional[Dict[str, str]] = None,
        sortby: Optional[List[Dict[str, str]]] = None,
        fields: Optional[dict[str, list[str]]] = None,
        filter: Optional[Dict[str, str]] = None,
    ) -> AsyncGenerator[str, None]:
        """Execute POST search across collections."""

        # Convert POST parameters to GET format for reuse
        collections_str = ",".join(collections) if collections else None
        ids_str = ",".join(ids) if ids else None
        bbox_str = ",".join(map(str, bbox)) if bbox else None

        # Handle free text search - POST requests can have array of terms
        q_str = None
        if q:
            if isinstance(q, list):
                q_str = ",".join(q)  # pragma: no cover
            else:
                q_str = q

        # Delegate to get_search with converted parameters
        async for chunk in self.get_search(
            collections=collections_str,
            ids=ids_str,
            bbox=bbox_str,
            datetime=datetime,
            limit=limit,
            token=token,
            q=q_str,
            query=json.dumps(query) if query else None,
            sortby=json.dumps(sortby) if sortby else None,
            fields=json.dumps(fields) if fields else None,
            filter=json.dumps(filter) if filter else None,
        ):
            yield chunk

    async def get_queryables(self) -> Dict[str, Any]:
        """Get global queryables schema."""
        properties = {
            "id": {
                "description": "Item identifier",
                "type": "string"
            },
            "collection": {
                "description": "Collection identifier",
                "type": "string"
            },
            "geometry": {
                "description": "Item geometry",
                "$ref": "https://geojson.org/schema/Geometry.json"
            },
            "datetime": {
                "description": "Item datetime",
                "type": "string",
                "format": "date-time",
                "pattern": r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)$"
            },
            "bbox": {
                "description": "Bounding box of the item",
                "type": "array",
                "items": {"type": "number"},
                "minItems": 4,
                "maxItems": 6
            }
        }

        if hasattr(self.config, 'solr_fields') and self.config.solr_fields:
            for field in self.config.solr_fields:
                if field not in properties:
                    properties[field] = {
                        "description": f"Custom field: {field}",
                        # Generic type for flexibility
                        "type": ["string", "number", "null"]
                    }

        queryables_schema = {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": f"{self.config.proxy}/api/freva-nextgen/stacapi/queryables",
            "type": "object",
            "title": "Queryables for Freva STAC-API",
            "description": (
                "Queryable properties available for"
                " filtering items across all collections"
            ),
            "properties": properties,
            "additionalProperties": True
        }

        return queryables_schema

    async def get_collection_queryables(self, collection_id: str) -> Dict[str, Any]:
        """Get collection-specific queryables schema."""
        collection_ids = await self.get_all_project_facets()
        if collection_id not in collection_ids:
            raise HTTPException(
                status_code=404,
                detail=f"Collection {collection_id} not found"
            )

        global_queryables = await self.get_queryables()

        # Update the schema ID and title for this specific collection
        collection_queryables = global_queryables.copy()
        collection_queryables.update({
            "$id": (
                f"{self.config.proxy}/api/freva-nextgen/"
                f"stacapi/collections/{collection_id}/queryables"
            ),
            "title": f"Queryables for Collection {collection_id}",
            "description": (
                "Queryable properties available for"
                f" filtering items in collection {collection_id}"
            )
        })

        return collection_queryables
