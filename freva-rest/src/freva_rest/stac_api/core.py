import json
import sys
from textwrap import dedent
from typing import Any, AsyncGenerator, Dict, List, Literal, Optional, cast
from urllib.parse import urlencode

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder

from freva_rest.config import ServerConfig
from freva_rest.databrowser_api import STAC, Asset, Item, Link, Solr
from freva_rest.logger import logger

from .schema import (
    CONFORMANCE_URLS,
    STAC_VERSION,
    STACCollection,
    STACItemCollection,
    STACLinks,
)


class STACAPI:
    """STAC API implementation for the Freva with Solr Backend."""

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
        self.solr_object = Solr(config)
        self.stac_object = STAC(config)
        self.solr_query = self.solr_object.query
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
                key not in ["datetime", "bbox", "limit", "token"]
            ) and caller_name == "collection_items":
                raise HTTPException(status_code=422, detail="Could not validate input.")

        return STACAPI(
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
        endpoints
        ----------
        query : str
            Solr query string.
        """
        self.solr_query["wt"] = "json"
        self.solr_query["facet"] = "true"
        self.solr_query["facet.sort"] = "index"
        self.solr_query["facet.mincount"] = "1"
        self.solr_query["facet.limit"] = "-1"

    async def get_all_project_facets(self) -> List[str]:
        """Get all project facets from Solr."""
        await self._set_solr_query()
        self.solr_query["facet.field"] = "project"
        self.solr_query["rows"] = self.batch_size
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

    async def get_landing_page(self) -> Dict[str, Any]:
        """Get the STAC API landing page."""
        # TODO: adding Collection queryable endpoint and find out where do we need this
        # endpoint
        # TODO: We need to outsource the description to somewere else
        collection_ids = await self.get_all_project_facets()
        response = {
            "type": "Catalog",
            "id": "freva-nextgen",
            "title": "I'm a STAC API",
            "description": "FAIR data for the Freva NextGen",
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
                    "type": "application/json",
                    "title": "Post Search Method",
                    "method": "POST",
                },
                {
                    "rel": "search",
                    "href": self.config.proxy + "/api/freva-nextgen/stacapi/search",
                    "type": "application/json",
                    "title": "GET Search Method",
                    "method": "GET",
                },
                {
                    "rel": "queryable",
                    "href": self.config.proxy + "/api/freva-nextgen/stacapi/queryable",
                    "type": "application/json",
                    "title": "Queryable API",
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
        # for constructing this. For now we define them all as constants.
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
            description=collection_id.upper(),
            license="proprietary",
            extent={
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {
                    "interval": [["1800-01-01T12:00:00Z", "2025-12-30T12:00:00Z"]]
                },
            },
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
                ),
                STACLinks(
                    rel="parent",
                    href=self.config.proxy + "/api/freva-nextgen/stacapi/",
                    type="application/json",
                    title="Landing Page",
                ),
                STACLinks(
                    rel="root",
                    href=self.config.proxy + "/api/freva-nextgen/stacapi/",
                    type="application/json",
                    title="Root",
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
                ),
                STACLinks(
                    rel="license",
                    href="https://opensource.org/license/bsd-3-clause",
                    title="BSD 3-Clause 'New' or 'Revised' License",
                    type="text/html",
                ),
            ],
            keywords=["climate", "analysis", "freva"],
            providers=[
                {"name": "Freva NextGen", "roles": ["producer", "processor", "host"]}
            ],
        )

    async def get_collections(self) -> AsyncGenerator[str, None]:
        """Get all collections as a streaming JSON response."""
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
            ),
            STACLinks(
                rel="parent",
                href=f"{self.config.proxy}/api/freva-nextgen/stacapi",
                type="application/json",
                title="Landing Page",
            ),
            STACLinks(
                rel="root",
                href=f"{self.config.proxy}/api/freva-nextgen/stacapi",
                type="application/json",
                title="Root",
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
            from freva_client import authenticate, databrowser
            import xarray as xr
            token_info = authenticate(username=<your_username>, \\
                host='{self.config.proxy}')
            db = databrowser({self.uniq_key}='{id}', \\
                            stream_zarr=True, host='{self.config.proxy}')
            xarray_dataset = xr.open_mfdataset(list(db))
            ```
            3. (CLI) Get token then access:
            ```bash
            # Attention: jq has to be installed beforehand
            token=$(freva-client auth -u <username> --host {self.config.proxy}\\
                                                        |jq -r .access_token)
            freva-client databrowser data-search {self.uniq_key}={id} \\
                --zarr --host {self.config.proxy} --access-token $token
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
                bbox = self.stac_object.parse_bbox(bbox)
            except ValueError as e:  # pragma: no cover
                logger.warning(f"Invalid bbox for {id}: {e}")
                bbox = None

        time = result.get("time")
        start_time = end_time = None
        if time:
            try:
                start_time, end_time = self.stac_object.parse_datetime(time)
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
            {"rel": "root", "target": base_url + "/", "media_type": "application/json"},
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
            if not any(link.rel == link_info["rel"] for link in item.links):
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

    async def get_collection_items(
        self,
        collection_id: str,
        limit: int = 10,
        token: Optional[str] = None,
        datetime: Optional[str] = None,
        bbox: Optional[str] = None,
    ) -> AsyncGenerator[str, None]:
        """Get a all items of a specific collection
        based on the collection_id abd the provided parameters.

        Parameters
        ----------
        collection_id : str
            The collection ID to filter the items.
        limit : int, optional
            The maximum number of items to return. Default is 10.
        token : str, optional
            The token for pagination. The format is "direction:collection_id:item_id".
            The direction can be "next" or "prev".
        datetime : str, optional
            The datetime range to filter the items. The format is "start/end".
        bbox : str, optional
            The bounding box to filter the items. The format is "minx,miny,maxx,maxy".

        Yields
        -------
        str
            The items in JSON format.
        """
        base_params: Dict[str, Any] = {"limit": limit}
        if datetime:
            base_params["datetime"] = datetime
        if bbox:
            base_params["bbox"] = bbox
        base_url = (
            f"{self.config.proxy}/api/freva-nextgen/"
            f"stacapi/collections/{collection_id}/items"
        )
        direction = "next"
        first_loop = True
        items_returned = 0
        first_item_id = None
        last_item_id = None
        curr_query_count = 0
        has_prev = False
        has_next = False
        await self._set_solr_query()
        self.solr_query["facet.field"] = self.config.solr_fields + ["time", "bbox"]
        self.solr_query["fl"] = (
            [self.uniq_key] + self.config.solr_fields + ["time", "bbox", "_version_"]
        )
        # IMPORTANT: by having `file asc`  we can sort the items (_version_)
        # and simultaneously take advantage of the cursorMark pagination. Otherwise
        self.solr_query["sort"] = "_version_ asc,file asc"

        filters = [f"project:{collection_id}"]
        # Manage parameters:
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

        # A light query to get the number of all items in the collection
        self.solr_query["fq"] = filters
        self.solr_query["rows"] = 0
        async with self.solr_object._session_get() as res:
            _, search = res
        before_pagination_count = search.get("response", {}).get("numFound", 0)

        # If the token is provided in the param, we need to narrow down the query
        # via the _version_ field (item_id)
        if token and ":" in token:
            direction, _, pivot_id = token.split(":", 2)
            if direction == "next":
                self.solr_query["fq"].append(f"_version_:{{{pivot_id} TO *}}")
            if direction == "prev":
                self.solr_query["fq"].append(f"_version_:{{* TO {pivot_id}}}")

        self.solr_query["cursorMark"] = "*"
        self.collection_id = collection_id
        self.solr_query["rows"] = self.batch_size

        yield '{"type":"FeatureCollection","features":['
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
                last_item_id = item_id

                item = await self.create_stac_item(doc, self.collection_id)
                text = json.dumps(item.to_dict(), default=str)

                if not first_loop:
                    yield ","
                else:
                    first_loop = False
                yield text
                items_returned += 1
            next_cursor_mark = results.get("nextCursorMark", None)
            self.solr_query["cursorMark"] = next_cursor_mark
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

        else:  # direction == "prev"
            has_prev = more_beyond_pivot
            has_next = not pivot_at_collection_end

        if has_prev:
            prev_params = {
                **base_params,
                "token": f"prev:{collection_id}:{first_item_id}",
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
                "token": f"next:{collection_id}:{last_item_id}",
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

    async def get_collection_item(self, collection_id: str, item_id: str) -> Item:
        """Get a specific item from a collection."""
        await self._set_solr_query()
        self.solr_query["facet.field"] = self.config.solr_fields + ["time", "bbox"]
        self.solr_query["fl"] = (
            [self.uniq_key] + self.config.solr_fields + ["time", "bbox", "_version_"]
        )
        self.solr_query["sort"] = "_version_ asc,file asc"
        filters = [f"project:{collection_id}", f"_version_:{item_id}"]
        self.solr_query["fq"] = filters
        self.solr_query["rows"] = 1
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

    async def queryable(self) -> Dict[str, Any]:  # type: ignore
        pass  # pragma: no cover

    async def get_search(self) -> STACItemCollection:  # type: ignore
        pass  # pragma: no cover

    async def post_search(self) -> STACItemCollection:  # type: ignore
        pass  # pragma: no cover
