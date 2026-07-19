import fnmatch
import json
import sys
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    Final,
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
from freva_rest.utils.stac_assets import (
    AssetContext,
    build_collection_assets,
    build_item_assets,
)
from freva_rest.utils.stac_utils import (
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

# Ordered hierarchy of facets that may define a STAC collection level,
# coarsest first.
COLLECTION_AXIS_HIERARCHY: Final[Tuple[str, ...]] = (
    "project",
    "product",
    "institute",
    "model",
    "experiment",
    "time_frequency",
    "realm",
    "variable",
    "ensemble",
    "cmor_table",
    "fs_type",
    "grid_label",
    "grid_id",
    "format",
)

DEFAULT_COLLECTION_AXIS: Final[str] = COLLECTION_AXIS_HIERARCHY[0]


class STACAPI:
    """STAC API implementation for the Freva and at
    the moment only with Solr Backend.

    Explanation about the structure:
    In this implementation we consider a configurable facet (by default
    ``project``) as the ``collection`` name and each file under each
    collection as an ``item``. The collection axis can be selected per
    request via an ``{axis}``-scoped path; when no axis is given it
    defaults to ``project`` for backwards compatibility.
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
        collection_axis: Optional[str] = None,
        visible_collections: Optional[List[str]] = None,
        axis_in_path: bool = False,
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
        # The facet that defines the collection level. ``None`` means the
        # request came in on a legacy (unscoped) route, which defaults to
        # ``project``.
        self.collection_axis = self._validate_axis(collection_axis)
        self.axis_in_path = axis_in_path
        # vendor parameter: Optional per-request visibility filter
        self.visible_collections: Optional[List[str]] = (
            [c for c in visible_collections if c] or None
            if visible_collections
            else None
        )

    def _validate_axis(self, axis: Optional[str]) -> str:
        """
        Resolve and validate the collection axis.
        """
        resolved = axis or DEFAULT_COLLECTION_AXIS
        if resolved not in COLLECTION_AXIS_HIERARCHY:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown STAC collection axis: {resolved}",
            )
        if resolved not in self.config.solr_fields:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Collection axis {resolved} is not available "
                    "in the search backend."
                ),
            )
        return resolved

    def _stac_base(self) -> str:
        """Base URL of the STAC API"""
        base = f"{self.config.proxy}/api/freva-nextgen/stacapi"
        if self.axis_in_path:
            return f"{base}/{self.collection_axis}"
        return base

    def _scoping_params(self) -> Dict[str, Any]:
        """
        Query parameters that scope a request and must survive navigation.
        """
        params: Dict[str, Any] = dict(self.stacapi_query or {})
        if self.visible_collections:
            params["visible_collections"] = ",".join(self.visible_collections)
        return params

    def _href(self, path: str = "", *, navigational: bool = True) -> str:
        """
        Build a STAC link href.
        """
        href = self._stac_base() + path
        if navigational:
            scoping = self._scoping_params()
            if scoping:
                # doseq=True preserves repeated/multi-valued params exactly.
                query = urlencode(scoping, doseq=True)
                if query:
                    sep = "&" if "?" in href else "?"
                    href = href + sep + query
        return href

    def _asset_context(self) -> AssetContext:
        """Context shared by all asset builders for this request."""
        return AssetContext(
            str(self.config.proxy),
            flavour="freva",
            uniq_key=self.uniq_key,
        )

    def _map_collection_field(self, field: str) -> str:
        """
        Map a STAC queryable property name to its Solr field.
        """
        if field == "collection":
            return self.collection_axis
        if field == "id":
            return self.uniq_key
        return field

    def _escape_solr_value(self, value: str) -> str:
        """Escape Solr query-syntax characters in a literal value."""
        escaped = value
        for char in self.solr_object.escape_chars:
            escaped = escaped.replace(char, f"\\{char}")
        return escaped

    def _collection_fq(self, collection_id: str) -> str:
        """Build a quoted, escaped Solr filter for one collection id on the
        active axis."""
        return (
            f'{self.collection_axis}:"{self._escape_solr_value(collection_id)}"'
        )

    async def _resolved_visible(self) -> Optional[List[str]]:
        """
        Expand the visibility glob patterns into concrete collection ids.
        """
        if not self.visible_collections:
            return None
        # get_all_collection_facets already applies _apply_visibility, which
        # both expands the globs and validates no-match
        return await self.get_all_collection_facets()

    # IMPORTANT: Upper bound on how many concrete collections a visibility filter may
    # expand to, to avoid building a huge Solr OR filter (and very long pagination URLs)
    # from a broad glob.
    MAX_VISIBLE_COLLECTIONS_EXPANSION: Final[int] = 1000

    def _apply_visibility(self, collection_ids: List[str]) -> List[str]:
        """
        Filter enumerated collection ids by the visibility filter.
        """
        if not self.visible_collections:
            return collection_ids
        # A bare "*" means "everything" -> equivalent to no filter.
        if any(p == "*" for p in self.visible_collections):
            return collection_ids  # pragma: no cover
        selected: List[str] = []
        seen = set()
        for pattern in self.visible_collections:
            matches = fnmatch.filter(collection_ids, pattern)
            if not matches:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"visible_collections pattern '{pattern}' matched no "
                        "collections"
                    ),
                )
            for cid in matches:
                if cid not in seen:
                    seen.add(cid)
                    selected.append(cid)
        if len(selected) > self.MAX_VISIBLE_COLLECTIONS_EXPANSION:
            raise HTTPException(
                status_code=400,
                detail=(
                    "visible_collections expands to too many collections "
                    f"({len(selected)} > "
                    f"{self.MAX_VISIBLE_COLLECTIONS_EXPANSION}); "
                    "use a narrower pattern."
                ),
            )
        return selected

    async def _assert_collection_visible(self, collection_id: str) -> None:
        """
        Raise 404 if the collection does not exist or is hidden by the
        active view.
        """
        collection_ids = await self.get_all_collection_facets()
        if collection_id not in collection_ids:
            raise HTTPException(
                status_code=404,
                detail=f"Collection {collection_id} not found",
            )

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
        collection_axis: Optional[str] = None,
        visible_collections: Optional[List[str]] = None,
        axis_in_path: bool = False,
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
        collection_axis : str, optional
            Facet that defines the collection level (default ``project``).
        visible_collections : list[str], optional
            Vendor visibility filter restricting exposed collections.
        axis_in_path : bool, optional
            Whether the axis was provided in the URL path (affects the
            style of generated links).
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
                raise HTTPException(
                    status_code=400,
                    detail="Unknown or invalid query parameter.",
                )

        return cls(
            config=config,
            limit=limit,
            token=token,
            datetime=datetime,
            bbox=bbox,
            uniuq_key=uniuq_key,
            collection_axis=collection_axis,
            visible_collections=visible_collections,
            axis_in_path=axis_in_path,
            **query,
        )

    async def _set_solr_query(self) -> None:
        """
        Set the Solr query for the STAC API.
        """
        self.solr_object.configure_base_search()

    async def get_all_collection_facets(self) -> List[str]:
        """
        Get all collection-defining facet values from Solr.
        """
        await self._set_solr_query()
        self.solr_object.set_query_params(
            facet_field=[self.collection_axis], rows=self.batch_size
        )
        async with self.solr_object._session_get() as res:
            _, search = res
        facets = (
            search.get("facet_counts", {})
            .get("facet_fields", {})
            .get(self.collection_axis, [])
        )
        if facets == []:  # pragma: no cover
            logger.error("No collection facets found in Solr response.")
            return []
        return self._apply_visibility(cast(List[str], facets[::2]))

    async def store_results(
        self,
        num_results: int,
        status: int,
        endpoint: str,
        query_params: Optional[Dict[str, Any]] = None,
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
            limit=self.limit,
        )

    async def get_landing_page(self) -> Dict[str, Any]:
        """Get the STAC API landing page."""
        # TODO: We need to outsource the hardcoded detail
        # and description to somewhere else
        collection_ids = await self.get_all_collection_facets()
        response = {
            "type": "Catalog",
            "id": "freva",
            "title": "Freva STAC-API",
            "description": "FAIR data for the Freva",
            "stac_version": STAC_VERSION,
            "stac_extensions": [],
            "conformsTo": CONFORMANCE_URLS,
            "links": [
                {
                    "rel": "self",
                    "href": self._href(""),
                    "type": "application/json",
                    "title": "Landing Page",
                },
                {
                    "rel": "conformance",
                    "href": self._href("/conformance", navigational=False),
                    "type": "application/json",
                    "title": "Conformance Classes",
                },
                {
                    "rel": "data",
                    "href": self._href("/collections"),
                    "type": "application/json",
                    "title": "Data Collections",
                },
                {
                    "rel": "search",
                    "href": self._href("/search"),
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "method": "POST",
                },
                {
                    "rel": "search",
                    "href": self._href("/search"),
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "method": "GET",
                },
                {
                    "rel": "http://www.opengis.net/def/rel/ogc/1.0/queryables",
                    "type": "application/schema+json",
                    "title": "Queryables",
                    "href": self._href("/queryables"),
                    "method": "GET",
                },
                {
                    "rel": "service-desc",
                    "type": "application/vnd.oai.openapi+json;version=3.0",
                    "title": "OpenAPI service description",
                    "href": self.config.proxy
                    + "/api/freva-nextgen/help/openapi.json",
                },
                {
                    "rel": "service-doc",
                    "type": "text/html",
                    "title": "OpenAPI service documentation",
                    "href": self.config.proxy
                    + "/api/freva-nextgen/help#tag/STAC-API",
                },
            ],
        }
        # Add child links for each collection (based on the STAC-API SPEC)
        if collection_ids != []:
            for collection_id in collection_ids:
                cast(List[Dict[str, str]], response["links"]).append(
                    {
                        "rel": "child",
                        "href": self._href("/collections/" + collection_id),
                        "type": "application/json",
                    }
                )
        return response

    # Optional STAC collection-level metadata fields. Every field
    # is a multi-valued, axis-tagged string: each entry is "<axis>|<value>".
    _COLLECTION_META_FIELDS: Final[Tuple[str, ...]] = (
        "stac_collection_title",
        "stac_collection_description",
        "stac_collection_license",
        "stac_collection_license_url",
        "stac_collection_keywords",
        "stac_collection_thumbnail_url",
        "stac_collection_thumbnail_type",
        "stac_collection_documentation_url",
        "stac_collection_bbox",
        "stac_collection_time_start",
        "stac_collection_time_end",
    )

    def _tagged_values(self, raw: Any) -> List[str]:
        """Extract the values of axis-tagged entries matching the active axis.

        Each stored entry is ``"<axis>|<value>"``. The entry is split on the
        FIRST ``|`` only, so a value may itself contain ``|``
        """
        if raw is None:
            return []
        entries = raw if isinstance(raw, list) else [raw]
        values: List[str] = []
        for entry in entries:
            if not isinstance(entry, str):
                continue  # pragma: no cover
            tag, sep, value = entry.partition("|")
            if not sep:
                continue  # no tag; ignore
            if tag not in COLLECTION_AXIS_HIERARCHY:
                continue  # unknown tag; ignore
            if tag == self.collection_axis:
                values.append(value)
        return values

    def _tagged_single(self, raw: Any) -> Optional[str]:
        """
        First active-axis value for a single-valued
        tagged field, or None.
        """
        values = self._tagged_values(raw)
        return values[0] if values else None

    async def _get_collection_metadata(
        self, collection_id: str
    ) -> Dict[str, Any]:
        """
        Fetch optional collection-level metadata from one representative
        file document of the collection.
        """
        await self._set_solr_query()
        self.solr_object.set_query_params(
            fl=list(self._COLLECTION_META_FIELDS),
            fq=[self._collection_fq(collection_id)],
            sort="_version_ desc",
            rows=1,
        )
        async with self.solr_object._session_get() as res:
            _, search = res
        docs = search.get("response", {}).get("docs", [])
        return docs[0] if docs else {}

    async def get_collection(
        self, collection_id: str, *, verify_exists: bool = True
    ) -> STACCollection:
        """
        Get a specific collection.
        """
        collection_id = collection_id.lower()
        if verify_exists:
            collection_ids = await self.get_all_collection_facets()
            if collection_id not in collection_ids:
                raise HTTPException(
                    status_code=404,
                    detail=f"Collection {collection_id} not found",
                )

        meta = await self._get_collection_metadata(collection_id)

        # Per-field values for the ACTIVE axis (tag-parsed), with fallbacks to
        # the historic generated constants when no entry matches the axis.
        title = (
            self._tagged_single(meta.get("stac_collection_title"))
            or collection_id.upper()
        )
        description = (
            self._tagged_single(meta.get("stac_collection_description"))
            or f"Collection {collection_id.upper()}"
        )
        license_ = (
            self._tagged_single(meta.get("stac_collection_license"))
            or "proprietary"
        )
        keywords = self._tagged_values(meta.get("stac_collection_keywords")) or [
            collection_id,
            "climate",
            "analysis",
            "freva",
        ]

        bbox = [[-180.0, -90.0, 180.0, 90.0]]
        bbox_raw = self._tagged_single(meta.get("stac_collection_bbox"))
        if bbox_raw:
            try:
                parts = [float(x) for x in bbox_raw.split(",")]
                if len(parts) >= 4:
                    bbox = [parts[:4]]
            except ValueError:
                logger.warning(
                    "Invalid collection_bbox for %s:%s -> %r",
                    self.collection_axis,
                    collection_id,
                    bbox_raw,
                )  # malformed; keep global default

        # Temporal extent: stored start/end for the active axis
        temporal_start = self._tagged_single(meta.get("stac_collection_time_start"))
        temporal_end = self._tagged_single(meta.get("stac_collection_time_end"))
        interval = [[temporal_start, temporal_end]]

        # License link: stored url for the active axis or the BSD default.
        license_url = self._tagged_single(meta.get("stac_collection_license_url"))
        license_link = STACLinks(
            rel="license",
            href=license_url or "https://opensource.org/license/bsd-3-clause",
            title=(
                "License"
                if license_url
                else "BSD 3-Clause 'New' or 'Revised' License"
            ),
            type="text/html",
            method="GET",
            merge=True,
            body=None,
        )

        links = [
            STACLinks(
                rel="self",
                href=self._href("/collections/" + collection_id),
                type="application/json",
                title="Collection",
                method="GET",
                merge=True,
                body=None,
            ),
            STACLinks(
                rel="parent",
                href=self._href(""),
                type="application/json",
                title="Landing Page",
                method="GET",
                merge=True,
                body=None,
            ),
            STACLinks(
                rel="root",
                href=self._href(""),
                type="application/json",
                title="Root",
                method="GET",
                merge=True,
                body=None,
            ),
            STACLinks(
                rel="items",
                href=self._href(
                    "/collections/" + collection_id + "/items"
                ),
                type="application/geo+json",
                title="Items",
                method="GET",
                merge=True,
                body=None,
            ),
            STACLinks(
                rel="queryables",
                href=self._href(
                    "/collections/" + collection_id + "/queryables"
                ),
                type="application/schema+json",
                title="Queryables",
                method="GET",
                merge=True,
                body=None,
            ),
            license_link,
        ]

        # Optional documentation link
        documentation_url = self._tagged_single(
            meta.get("stac_collection_documentation_url")
        )
        if documentation_url:
            links.append(
                STACLinks(
                    rel="describedby",
                    href=documentation_url,
                    title="Documentation",
                    type="text/html",
                    method="GET",
                    merge=True,
                    body=None,
                )
            )

        assets: Dict[str, Any] = {
            key: asset.to_dict()
            for key, asset in build_collection_assets(
                self._asset_context(),
                facet=self.collection_axis,
                value=collection_id,
            ).items()
        }

        # Optional thumbnail asset
        thumbnail_url = self._tagged_single(meta.get("stac_collection_thumbnail_url"))
        if thumbnail_url:
            assets["thumbnail"] = {
                "href": thumbnail_url,
                "type": self._tagged_single(
                    meta.get("stac_collection_thumbnail_type")
                )
                or "image/png",
                "roles": ["thumbnail"],
                "title": "Thumbnail",
            }

        return STACCollection(
            id=collection_id,
            type="Collection",
            stac_version=STAC_VERSION,
            title=title,
            description=description,
            license=license_,
            summaries=None,
            extent=STACExtent(
                spatial={"bbox": bbox},
                temporal={"interval": interval},
            ),
            links=links,
            keywords=keywords,
            providers=[
                STACProvider(
                    name="Freva",
                    description=(
                        "The Freva is a platform for climate data analysis and "
                        "evaluation, providing access to various datasets and tools."
                    ),
                    roles=["producer", "processor", "host"],
                    url=self._href("", navigational=False),
                )
            ],
            assets=assets,
        )

    async def get_collections(self) -> AsyncGenerator[str, None]:
        """Get all collections (as STAC Collections)."""
        collection_ids = await self.get_all_collection_facets()
        yield '{"collections": ['
        first_item = True

        for collection_id in collection_ids:
            collection = await self.get_collection(
                collection_id, verify_exists=False
            )
            if not first_item:
                yield ","
            else:
                first_item = False
            yield collection.model_dump_json(exclude_none=True)
        links = [
            STACLinks(
                rel="self",
                href=self._href("/collections"),
                type="application/json",
                title="Collections",
                method="GET",
                merge=True,
                body=None,
            ),
            STACLinks(
                rel="parent",
                href=self._href(""),
                type="application/json",
                title="Landing Page",
                method="GET",
                merge=True,
                body=None,
            ),
            STACLinks(
                rel="root",
                href=self._href(""),
                type="application/json",
                title="Root",
                method="GET",
                merge=True,
                body=None,
            ),
        ]
        yield f'], "links": {json.dumps(jsonable_encoder(links))}}}'

    async def create_stac_item(
        self,
        result: Dict[str, Any],
        collection_id: str,
    ) -> Item:
        """Create a STAC item from the Solr doc."""
        collection_id = collection_id.lower()
        id = result.get(self.uniq_key, "")
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
                ("freva:data_type" if k == "data_type" else k): result.get(k)
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
        links_to_add = [
            {
                "rel": "self",
                "target": self._href(
                    f"/collections/{collection_id}/items/{item_id}"
                ),
                "media_type": "application/json",
            },
            {
                "rel": "root",
                "target": self._href(""),
                "media_type": "application/json",
            },
            {
                "rel": "parent",
                "target": self._href(f"/collections/{collection_id}"),
                "media_type": "application/json",
            },
            {
                "rel": "collection",
                "target": self._href(f"/collections/{collection_id}"),
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

        assets = build_item_assets(
            self._asset_context(),
            id,
            fs_type=result.get("fs_type"),
        )
        for key, asset in assets.items():
            item.add_asset(key, asset)

        return item

    def _validate_pagination_token(
        self, token: Optional[str], context_id: str
    ) -> None:
        """Validate a pagination token's context without touching Solr.

        A token is ``"<direction>:<context>:<pivot>"``. Reject tokens minted
        for a different scope (collection id or ``"search"``) so a copied or
        stale token cannot page the wrong set.
        """
        if token and ":" in token:
            _, token_context, _ = token.split(":", 2)
            if token_context != context_id:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Pagination token does not match this request "
                        "context."
                    ),
                )

    async def _paginated_items_search(
        self,
        filters: List[str],
        limit: int,
        token: Optional[str],
        base_url: str,
        base_params: Dict[str, Any],
        context_id: str = "collection",
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
            fl=[self.uniq_key]
            + self.config.solr_fields
            + ["time", "bbox", "_version_", "fs_type"],
            sort="_version_ asc,file asc",
            fq=filters,
            rows=0,
        )

        # Get total count before pagination
        async with self.solr_object._session_get() as res:
            _, search = res
        before_pagination_count = search.get("response", {}).get("numFound", 0)

        # Handle pagination token
        if token and ":" in token:
            direction, token_context, pivot_id = token.split(":", 2)
            # re-check defensively
            self._validate_pagination_token(token, context_id)
            if direction == "next":
                filters.append(f"_version_:{{{pivot_id} TO *}}")
            if direction == "prev":
                filters.append(f"_version_:{{* TO {pivot_id}}}")
                self.solr_object.set_query_params(sort="_version_ desc,file asc")

        # Update query with pagination filters
        self.solr_object.set_query_params(
            fq=filters, cursorMark="*", rows=self.batch_size
        )

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

                axis_value = doc.get(self.collection_axis, context_id)
                if isinstance(axis_value, list) and axis_value:
                    collection_id_for_item = axis_value[0]
                else:
                    collection_id_for_item = axis_value  # pragma: no cover
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

    async def prepare_collection_items(self, collection_id: str) -> None:
        """
        Validate a collection-items request before streaming begins.
        """
        await self._assert_collection_visible(collection_id.lower())

    async def get_collection_items(
        self,
        collection_id: str,
        limit: int = 10,
        token: Optional[str] = None,
        datetime: Optional[str] = None,
        bbox: Optional[str] = None,
    ) -> AsyncGenerator[str, None]:
        """
        Stream all items of a specific collection.
        """
        base_params: Dict[str, Any] = {"limit": limit}
        collection_id = collection_id.lower()
        if datetime:
            base_params["datetime"] = datetime
        if bbox:
            base_params["bbox"] = bbox
        # Preserve scoping parameters (visible_collections) on the
        # self/next/prev pagination links so the scope survives paging.
        base_params.update(self._scoping_params())
        base_url = self._stac_base() + (
            f"/collections/{collection_id}/items"
        )

        filters = [self._collection_fq(collection_id)]

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
                "{{!field f=bbox}}"
                "Intersects(ENVELOPE({minx},{maxx},{maxy},{miny}))"
            ).format(minx=minx, maxx=maxx, maxy=maxy, miny=miny)
            filters.append(bbox_fq)

        async for chunk in self._paginated_items_search(
            filters, limit, token, base_url, base_params, collection_id
        ):
            yield chunk

    async def get_collection_item(self, collection_id: str, item_id: str) -> Item:
        """Get a specific item from a collection."""
        await self._assert_collection_visible(collection_id)
        await self._set_solr_query()

        # Set all parameters at once
        self.solr_object.set_query_params(
            facet_field=self.config.solr_fields + ["time", "bbox"],
            fl=[self.uniq_key]
            + self.config.solr_fields
            + ["time", "bbox", "_version_", "fs_type"],
            sort="_version_ asc,file asc",
            fq=[
                self._collection_fq(collection_id),
                f"_version_:{item_id}",
            ],
            rows=1,
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

    def _parse_cql2_filter(self, filter_expr: Dict[str, Any]) -> List[str]:
        """
        Parse CQL2-JSON filter expression and convert to Solr query filters.
        TODO: when we have multiple backend support, we need to
        implement this method for each backend.

        Attention: All of the operators are coming from the STAC API
        CQL2-JSON specification, see:
        https://github.com/stac-api-extensions/filter

        Parameters
        ----------
        filter_expr : Dict[str, Any]
            CQL2-JSON filter expression

        Returns
        -------
        List[str]
            List of Solr filter queries
        """
        if not isinstance(filter_expr, dict) or "op" not in filter_expr:
            return []

        op = filter_expr["op"]
        args = filter_expr.get("args", [])

        # Logical operators
        if op == "and":
            sub_filters = []
            for arg in args:
                if isinstance(arg, dict) and "op" in arg:
                    sub_filters.extend(self._parse_cql2_filter(arg))
            return [f"({' AND '.join(sub_filters)})"] if sub_filters else []

        elif op == "or":
            sub_filters = []
            for arg in args:
                if isinstance(arg, dict) and "op" in arg:
                    sub_filters.extend(self._parse_cql2_filter(arg))
            return [f"({' OR '.join(sub_filters)})"] if sub_filters else []

        elif op == "not":
            if len(args) == 1 and isinstance(args[0], dict) and "op" in args[0]:
                sub_filter = self._parse_cql2_filter(args[0])
                if sub_filter:
                    return [f"-({sub_filter[0]})"]
            return []

        # Comparison operators
        elif op in ["=", "eq"]:
            if len(args) == 2:
                prop = args[0]
                value = args[1]
                if isinstance(prop, dict) and prop.get("property"):
                    field = self._map_collection_field(prop["property"])

                    # Escape special characters in value
                    if isinstance(value, str):
                        escaped_value = value
                        for char in self.solr_object.escape_chars:
                            escaped_value = escaped_value.replace(
                                char, f"\\{char}"
                            )
                        return [f'{field}:"{escaped_value}"']
                    else:
                        return [f"{field}:{value}"]
            return []

        elif op in ["<>", "!=", "neq"]:
            if len(args) == 2:
                prop = args[0]
                value = args[1]
                if isinstance(prop, dict) and prop.get("property"):
                    field = self._map_collection_field(prop["property"])

                    if isinstance(value, str):
                        escaped_value = value
                        for char in self.solr_object.escape_chars:
                            escaped_value = escaped_value.replace(
                                char, f"\\{char}"
                            )
                        return [f'-{field}:"{escaped_value}"']
                    else:
                        return [f"-{field}:{value}"]
            return []

        elif op in ["<", "lt"]:
            if len(args) == 2:
                prop = args[0]
                value = args[1]
                if isinstance(prop, dict) and prop.get("property"):
                    field = self._map_collection_field(prop["property"])
                    return [f"{field}:{{* TO {value}}}"]
            return []

        elif op in ["<=", "lte"]:
            if len(args) == 2:
                prop = args[0]
                value = args[1]
                if isinstance(prop, dict) and prop.get("property"):
                    field = self._map_collection_field(prop["property"])
                    return [f"{field}:[* TO {value}]"]
            return []

        elif op in [">", "gt"]:
            if len(args) == 2:
                prop = args[0]
                value = args[1]
                if isinstance(prop, dict) and prop.get("property"):
                    field = self._map_collection_field(prop["property"])
                    return [f"{field}:{{{value} TO *}}"]
            return []

        elif op in [">=", "gte"]:
            if len(args) == 2:
                prop = args[0]
                value = args[1]
                if isinstance(prop, dict) and prop.get("property"):
                    field = self._map_collection_field(prop["property"])
                    return [f"{field}:[{value} TO *]"]
            return []

        elif op == "isNull":
            if len(args) == 1:
                prop = args[0]
                if isinstance(prop, dict) and prop.get("property"):
                    field = self._map_collection_field(
                        prop["property"]
                    )  # pragma: no cover
                    return [f"-{field}:[* TO *]"]
            return []

        # Spatial operators
        elif op == "s_intersects":
            if len(args) == 2:
                prop = args[0]
                geom = args[1]
                if isinstance(prop, dict) and prop.get("property") == "geometry":
                    if isinstance(geom, dict) and geom.get("type") == "Polygon":
                        coordinates = geom.get("coordinates") or [[]]
                        coords = coordinates[0] if coordinates else []
                        if len(coords) >= 4:
                            # Convert to bbox for Solr
                            lons = [c[0] for c in coords[:-1]]
                            lats = [c[1] for c in coords[:-1]]
                            minx, maxx = min(lons), max(lons)
                            miny, maxy = min(lats), max(lats)
                            bbox_str = f"ENVELOPE({minx},{maxx},{maxy},{miny})"
                            return [f"{{!field f=bbox}}Intersects({bbox_str})"]
            return []

        # Temporal operators
        elif op == "t_after":
            if len(args) == 2:
                prop = args[0]
                timestamp = args[1]
                if isinstance(prop, dict) and prop.get("property") == "datetime":
                    if isinstance(timestamp, dict) and "timestamp" in timestamp:
                        ts = timestamp["timestamp"]
                        return [f"time:{{{ts} TO *}}"]
                    elif isinstance(timestamp, str):
                        return [f"time:{{{timestamp} TO *}}"]
            return []

        elif op == "t_before":
            if len(args) == 2:
                prop = args[0]
                timestamp = args[1]
                if isinstance(prop, dict) and prop.get("property") == "datetime":
                    if isinstance(timestamp, dict) and "timestamp" in timestamp:
                        ts = timestamp["timestamp"]
                        return [f"time:{{* TO {ts}}}"]
                    elif isinstance(timestamp, str):
                        return [f"time:{{* TO {timestamp}}}"]
            return []

        elif op == "t_during":
            if len(args) == 2:
                prop = args[0]
                interval = args[1]
                if isinstance(prop, dict) and prop.get("property") == "datetime":
                    if isinstance(interval, dict) and "interval" in interval:
                        start, end = interval["interval"]
                        return [
                            f"{{!field f=time op=Intersects}}[{start} TO {end}]"
                        ]
                    elif isinstance(interval, list) and len(interval) == 2:
                        start, end = interval
                        return [
                            f"{{!field f=time op=Intersects}}[{start} TO {end}]"
                        ]
            return []

        return []

    async def prepare_search(
        self,
        collections: Optional[str] = None,
        filter: Optional[str] = None,
    ) -> None:
        """
        Validate a search request before streaming begins.
        """
        collection_list = collections.split(",") if collections else None

        # Reject explicit collections outside the visible view.
        resolved_visible = await self._resolved_visible()
        if collection_list and resolved_visible is not None:
            allowed = set(resolved_visible)
            outside = [c for c in collection_list if c not in allowed]
            if outside:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Requested collections are not visible in this "
                        f"view: {', '.join(outside)}"
                    ),
                )

        # Validate the CQL2 filter to match the lenient CQL2 contract
        # 1. A filter that is not valid JSON is a 400.
        # 2. A filter that is valid JSON but does not map to a usable predicate
        # (unknown operator, missing args, unknown property, ...) is silently
        # ignored by _parse_cql2_filter (returns no Solr filter), so such a
        # request still succeeds
        if filter:
            if isinstance(filter, str):
                try:
                    json.loads(filter)
                except json.JSONDecodeError as e:
                    raise HTTPException(
                        status_code=400, detail=f"Invalid CQL2 JSON: {e}"
                    )

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

        base_url = self._stac_base() + "/search"
        base_params: Dict[str, Any] = {"limit": limit}
        if collections:
            base_params["collections"] = collections
        if datetime:
            base_params["datetime"] = datetime
        if bbox:
            base_params["bbox"] = bbox
        if q:
            base_params["q"] = q
        if filter:
            base_params["filter"] = filter
        # Preserve scoping parameters (visible_collections) on the
        # self/next/prev pagination links so the scope survives paging.
        base_params.update(self._scoping_params())

        # Reject explicit collections that fall outside the visible view.
        resolved_visible = await self._resolved_visible()
        if collection_list and resolved_visible is not None:
            allowed = set(resolved_visible)
            outside = [c for c in collection_list if c not in allowed]
            if outside:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Requested collections are not visible in this "
                        f"view: {', '.join(outside)}"
                    ),
                )

        filters: List[str] = []

        # CQL2 filter handling
        if filter:
            try:
                filter_obj = (
                    json.loads(filter) if isinstance(filter, str) else filter
                )
                cql2_filters = self._parse_cql2_filter(filter_obj)
                filters.extend(cql2_filters)
            except HTTPException:  # pragma: no cover
                raise
            except json.JSONDecodeError as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid CQL2 JSON: {e}"
                )
            except Exception as e:
                logger.error(f"Failed to parse CQL2 filter: {e}")
                raise HTTPException(
                    status_code=400, detail=f"Invalid CQL2 filter: {e}"
                )

        # standard filters if CQL2 didn't handle them
        axis_prefix = f"{self.collection_axis}:"
        has_collection_filter = any(axis_prefix in f for f in filters)
        has_id_filter = any(f"{self.uniq_key}:" in f for f in filters)
        has_time_filter = any("time:" in f for f in filters)
        has_bbox_filter = any("bbox" in f for f in filters)

        # Collection filter
        if collection_list and not has_collection_filter:
            collection_filter = " OR ".join(
                [self._collection_fq(coll) for coll in collection_list]
            )
            filters.append(f"({collection_filter})")

        # Visibility filter
        if resolved_visible and not collection_list:
            visible_filter = " OR ".join(
                [self._collection_fq(coll) for coll in resolved_visible]
            )
            filters.append(f"({visible_filter})")

        # IDs filter
        if ids_list and not has_id_filter:
            ids_filter = " OR ".join(
                [f'{self.uniq_key}:"{item_id}"' for item_id in ids_list]
            )
            filters.append(f"({ids_filter})")

        # Free text search filter
        if q_terms:
            text_fields = self.config.solr_fields
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
        if datetime and not has_time_filter:
            if "/" in datetime:
                start, end = datetime.split("/", 1)
                if start and end:
                    filters.append(f"time:[{start} TO {end}]")
            else:
                filters.append(f"time:[{datetime} TO *]")  # pragma: no cover

        # Bbox filter
        if bbox and not has_bbox_filter:
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
        filter: Optional[Dict[str, Any]] = None,
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

        # Convert filter dict to JSON string for processing req
        filter_str = None
        if filter:
            filter_str = (
                json.dumps(filter) if isinstance(filter, dict) else str(filter)
            )

        # execute get_search with converted parameters
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
            filter=filter_str,
        ):
            yield chunk

    async def _fetch_facets(self) -> Dict[str, List[str]]:
        """
        Enumerate facet values for the queryables schema.
        """
        try:
            await self._set_solr_query()

            # Scope the facet enumeration to the visible collections, mirroring
            # the filter the search endpoint applies to items.
            resolved_visible = await self._resolved_visible()
            fq: List[str] = []
            if resolved_visible:
                fq.append(
                    "("
                    + " OR ".join(
                        self._collection_fq(coll) for coll in resolved_visible
                    )
                    + ")"
                )

            self.solr_object.set_query_params(
                facet_field=self.config.solr_fields,
                fq=fq,
                rows=0,
            )
            async with self.solr_object._session_get() as res:
                _, search = res

            facet_fields = (
                search.get("facet_counts", {}).get("facet_fields", {})
            )
            facet_values: Dict[str, List[str]] = {}
            for facet_name, facet_data in facet_fields.items():
                if isinstance(facet_data, list) and len(facet_data) > 1:
                    values = [
                        str(facet_data[i])
                        for i in range(0, len(facet_data), 2)
                        if facet_data[i + 1] > 0
                    ]
                    if values:
                        facet_values[facet_name] = values

            return facet_values
        except Exception as e:  # pragma: no cover
            logger.error(f"Error fetching facets: {e}")
            return {}

    async def get_queryables(self) -> Dict[str, Any]:
        """Get global queryables schema."""
        collection_values = await self.get_all_collection_facets()
        collection_prop: Dict[str, Any] = {
            "description": (
                "STAC collection identifier "
                f"(collection axis: {self.collection_axis})"
            ),
            "type": "string",
        }
        if collection_values:
            collection_prop["enum"] = collection_values
        properties = {
            "id": {"description": "Item identifier", "type": "string"},
            "collection": collection_prop,
            "geometry": {
                "description": "Item geometry",
                "$ref": "https://geojson.org/schema/Geometry.json",
            },
            "datetime": {
                "description": "Item datetime",
                "type": "string",
                "format": "date-time",
                "pattern": r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)$",
            },
            "bbox": {
                "description": "Bounding box of the item",
                "type": "array",
                "items": {"type": "number"},
                "minItems": 4,
                "maxItems": 6,
            },
        }

        # Fetch dynamic facets and add them as properties
        facets = await self._fetch_facets()
        for facet_name, facet_values in facets.items():
            if facet_name not in properties:
                properties[facet_name] = {
                    "description": f"Search facet: {facet_name}",
                    "type": "string",
                    "enum": facet_values,
                }

        queryables_schema = {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": self._href("/queryables"),
            "type": "object",
            "title": "Queryables for Freva STAC-API",
            "description": (
                "Queryable properties available for"
                " filtering items across all collections"
            ),
            "properties": properties,
            "additionalProperties": True,
        }

        return queryables_schema

    async def get_collection_queryables(
        self, collection_id: str
    ) -> Dict[str, Any]:
        """Get collection-specific queryables schema."""
        collection_ids = await self.get_all_collection_facets()
        if collection_id not in collection_ids:
            raise HTTPException(
                status_code=404, detail=f"Collection {collection_id} not found"
            )

        global_queryables = await self.get_queryables()

        # Update the schema ID and title for this specific collection
        collection_queryables = global_queryables.copy()
        collection_queryables.update(
            {
                "$id": self._href(
                    f"/collections/{collection_id}/queryables",
                ),
                "title": f"Queryables for Collection {collection_id}",
                "description": (
                    "Queryable properties available for"
                    f" filtering items in collection {collection_id}"
                ),
            }
        )

        return collection_queryables
