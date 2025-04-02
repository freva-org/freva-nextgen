"""STAC class for generating the STATIC STAC catalog."""

import ast
import io
import json
from datetime import datetime
from textwrap import dedent
from typing import (
    Any,
    AsyncIterator,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pystac
from dateutil import parser
from fastapi import Request

from freva_rest.config import ServerConfig
from freva_rest.logger import logger

from .core import FlavourType, Solr, Translator

import io
from datetime import datetime
from zipfile import ZipFile, ZipInfo, ZIP_DEFLATED

class ZipStream(io.RawIOBase):
    """Custom unseekable stream that buffers writes and 
    can flush its content.
    
    In spite of having simillar libraries for this purpose,
    we designed this lean class to have a better control
    on memory usage and to have a better understanding
    of the underlying mechanism. Most of existing libraries
    due to the nature of their design, they consume a certain
    amount of memory which doesn't make them suitable for 
    Freva case.
    """
    def __init__(self):
        self._buffer = bytearray()
        self._closed = False

    def close(self):
        self._closed = True

    def write(self, b):
        """ Write the small buffer into the stream """
        if self._closed:
            raise ValueError("Can't write to a closed stream")
        self._buffer += b
        return len(b)

    def flush_and_read(self):
        """ Flush the buffer and return the chunk """
        chunk = bytes(self._buffer)
        self._buffer.clear()
        return chunk

# TODO: drop pystac completely and use the custom implementation
class STAC(Solr):
    """STAC class to create static STAC catalogues."""

    def __init__(
        self,
        config: ServerConfig,
        *,
        uniq_key: Literal["file", "uri"] = "file",
        flavour: FlavourType = "freva",
        start: int = 0,
        multi_version: bool = True,
        translate: bool = True,
        _translator: Union[None, Translator] = None,
        **query: list[str],
    ):
        super().__init__(
            config,
            uniq_key=uniq_key,
            flavour=flavour,
            start=start,
            multi_version=multi_version,
            translate=translate,
            _translator=_translator,
            **query
        )
        self.config = config
        self._zip_stream = Stream()
        self._zip = ZipFile(self._zip_stream, mode="w", compression=ZIP_DEFLATED)
        self.spatial_extent = {
            "minx": float("-180"),
            "miny": float("-90"),
            "maxx": float("180"),
            "maxy": float("90"),
        }
        self.temporal_extent: dict[str, Optional[datetime]] = {
            "start": None,
            "end": None,
        }
        self.assets_prereqs: Dict[str, Union[str, int]] = {}

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
    ) -> "STAC":
        """Use Solr validate_parameters and return a STAC validated_parameters
        for validating the search params through Solr in STAC inheritated cls."""
        solr_instance = await super().validate_parameters(
            config,
            uniq_key=uniq_key,
            flavour=flavour,
            start=start,
            multi_version=multi_version,
            translate=translate,
            **query
        )

        return cls(
            config,
            uniq_key=uniq_key,
            flavour=flavour,
            start=start,
            multi_version=multi_version,
            translate=translate,
            _translator=solr_instance.translator,
            **query
        )

    async def _create_stac_collection(self, collection_id: str) -> pystac.Collection:
        """Create a STAC collection from the Solr search results."""

        intake_desc = dedent(
            f"""
            # Installing Intake-ESM
            ```bash
            # Method 1: Using pip
            pip install intake-esm
            # Method 2: Using conda (recommended)
            conda install -c conda-forge intake-esm
            ```
            # Quick Guide: INTAKE-ESM Catalog on Levante (Python)
            ```python
            import intake
            # create a catalog object from a EMS JSON file containing dataset metadata
            cat = intake.open_esm_datastore(
            '{str(self.assets_prereqs.get('full_endpoint')).replace(
                "stac-catalogue", "intake-catalogue")}')
            ```
        """
        )

        stac_static_desc = dedent(
            f"""
            # STAC Static Catalog Setup
            ```bash
            pip install pystac
            ```
            # Load the STAC Catalog
            ```python
            import pystac
            import tarfile
            import tempfile
            import os
            temp_dir = tempfile.mkdtemp(dir='/tmp')
            with tarfile.open('stac-catalog-{collection_id}-{self.uniq_key}.tar.gz',
                            mode='r:gz') as tar:
                tar.extractall(path=temp_dir)
            cat = pystac.Catalog.from_file(os.path.join(temp_dir, 'catalog.json'))
            ```
            💡: This has been desigend to work with the data locally. So you
            can copy the catalog link from here and download and load the catalog
            locally via the provided script.
            """
        )

        params_dict = (
            ast.literal_eval(str(self.assets_prereqs.get("only_params")))
            if self.assets_prereqs.get("only_params", "")
            else {}
        )
        python_params = " ".join(
            f"{k}='{v}','"
            for k, v in params_dict.items()
            if k not in ("translate")
        )
        cli_params = " ".join(
            f"{k}={v}" for k, v in params_dict.items()
            if k not in ("translate")
        )
        api_params = "&".join(
            f"{k}={v}" for k, v in params_dict.items()
            if k not in ("translate")
        )

        zarr_desc = dedent(
            f"""
            # Accessing Zarr Data
            1. Install freva-client
            ```bash
            pip install freva-client
            ```
            2. Get the auth token and access the zarr data (Python) - recommended
            ```python
            from freva_client import authenticate, databrowser
            import xarray as xr
            token_info = authenticate(username=<your_username>,\\
                                    host='{self.config.proxy}')
            db = databrowser({python_params} stream_zarr=True,\\
                                host='{self.config.proxy}')
            xarray_dataset = xr.open_mfdataset(list(db))
            ```
            3. Get the auth token and access the zarr data (CLI)
            ```bash
            # Attention: jq has to be installed beforehand
            token=$(freva-client auth -u <username> --host {self.config.proxy}\\
                                                        |jq -r .access_token)
            freva-client databrowser data-search {cli_params} --zarr\\
                            --host {self.config.proxy} --access-token $token
            ```
            4. Access the zarr data directly (API - language agnostic)
            ```bash
            curl -X GET {self.assets_prereqs.get('base_url')}api/ \\
            freva-nextgen/databrowser/load/\\
            {self.translator.flavour}?{api_params} \\
            -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
            ```
            💡: Read more about the
            [freva-client](https://freva-clint.github.io/freva-nextgen/)
            """
        )

        local_access_desc = dedent(
            f"""
            # Accessing data locally where the data is stored

            ```python
            import xarray as xr
            ds = xr.open_mfdataset(list('{str(self.assets_prereqs.get("full_endpoint"))
                                          .replace("stac-catalogue", "data-search")}'))
            ```

            💡: Please make sure to have the required xarray packages installed.
            """
        )
        collection = pystac.Collection(
            id=collection_id,
            title=collection_id,
            description="",
            extent=pystac.Extent(
                # We need to define an initial temporal and spatial extent,
                # and then we update those values as we iterate over the items
                spatial=pystac.SpatialExtent([[-180.0, -90.0, 180.0, 90.0]]),
                temporal=pystac.TemporalExtent([[None, None]]),  # type: ignore
            ),
        )
        assets = {
            "freva-databrowser": pystac.Asset(
                href=(
                    f"{self.assets_prereqs.get('base_url')}databrowser/?"
                    f"{api_params}"
                ),
                title="Freva Web Data-Browser",
                description=(
                    "Interactive web interface for data exploration and analysis. "
                    "Access through any browser."
                ),
                roles=["overview"],
                media_type="text/html",
            ),
            "intake-catalogue": pystac.Asset(
                href=str(self.assets_prereqs.get("full_endpoint")).replace(
                    "stac-catalogue", "intake-catalogue"
                ),
                title="Intake-ESM Catalogue",
                description=intake_desc,
                roles=["metadata"],
                media_type="application/json",
            ),
            "stac-static-catalogue": pystac.Asset(
                href=str(self.assets_prereqs.get("full_endpoint")),
                title="STAC Static Catalogue",
                description=stac_static_desc,
                roles=["metadata"],
                media_type="application/geopackage+sqlite3",
            ),
            "local-access": pystac.Asset(
                href=str(self.assets_prereqs.get("full_endpoint")).replace(
                    "stac-catalogue", "data-search"
                ),
                title="Access data locally",
                description=local_access_desc,
                roles=["data"],
                media_type="application/netcdf",
            ),
            "zarr-access": pystac.Asset(
                href=(
                    f"{self.assets_prereqs.get('base_url')}api/freva-nextgen/"
                    f"databrowser/load/{self.translator.flavour}?"
                    f"{api_params}"
                ),
                title="Stream Zarr Dataset",
                description=zarr_desc,
                roles=["data"],
                media_type="application/vnd+zarr",
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
        }

        for key, asset in assets.items():
            collection.add_asset(key, asset)

        if self.facets:
            collection.extra_fields["search_keys"] = {
                key: values for key, values in self.facets.items()
            }
        else:
            logger.info("No search keys found for collection")

        collection.providers = [
            pystac.Provider(
                name="Freva DataBrowser",
                url=f"{self.config.proxy}",
            )
        ]
        return collection

    async def _iter_stac_items(self) -> AsyncIterator[List[pystac.Item]]:
        self.query["cursorMark"] = "*"
        items_batch = []
        while True:
            async with self._session_get() as res:
                _, results = res
            for result in results.get("response", {}).get("docs", [{}]):
                item = await self._create_stac_item(result)
                items_batch.append(item)

                if len(items_batch) >= self.batch_size:
                    yield items_batch
                    items_batch = []

            if items_batch:
                yield items_batch
            next_cursor_mark = results.get("nextCursorMark", None)
            if next_cursor_mark == self.query["cursorMark"] or not results:
                break
            self.query["cursorMark"] = next_cursor_mark

    def parse_datetime(self, time_str: str) -> Tuple[datetime, datetime]:
        """
        Parse a time range string into start and end datetimes.

        Parameters
        ----------
        time_str : str
            Time range string in rdate format '[start_time TO end_time]'

        Returns
        -------
        Tuple[datetime, datetime]
            Start and end datetime objects
        """
        clean_start_time = time_str.replace("[", "").split(" TO ")[0]
        clean_end_time = time_str.replace("]", "").split(" TO ")[1]
        return parser.parse(clean_start_time), parser.parse(clean_end_time)

    def parse_bbox(self, bbox_str: Union[str, List[str]]) -> List[float]:
        """
        Parse a bounding box string into coordinates.

        Parameters
        ----------
        bbox_str : Union[str, List[str]]
            Bounding box in ENVELOPE format: 'ENVELOPE(west,east,north,south)'
            or as a list with one element: ['ENVELOPE(west,east,north,south)']

        Returns
        -------
        List[float]
            Coordinates as [minx, miny, maxx, maxy]
        """
        bbox = bbox_str[0] if isinstance(bbox_str, list) else bbox_str

        nums = [
            float(x)
            for x in bbox.replace("ENVELOPE(", "").replace(")", "").split(",")
        ]
        return [nums[0], nums[3], nums[1], nums[2]]

    def _update_spatial_extent(self, bbox: List[float]) -> None:
        """
        Update collection's spatial extent based on item bbox.

        Parameters
        ----------
        bbox : List[float]
            Bounding box coordinates [minx, miny, maxx, maxy]
        """
        self.spatial_extent["minx"] = min(self.spatial_extent["minx"], bbox[0])
        self.spatial_extent["miny"] = min(self.spatial_extent["miny"], bbox[1])
        self.spatial_extent["maxx"] = max(self.spatial_extent["maxx"], bbox[2])
        self.spatial_extent["maxy"] = max(self.spatial_extent["maxy"], bbox[3])

    def _update_temporal_extent(self, start_time: datetime, end_time: datetime) -> None:
        """Update collection's temporal extent based on item timerange.

        Parameters
        ----------
        start_time : datetime
            Item's start datetime
        end_time : datetime
            Item's end datetime
        """
        if self.temporal_extent["start"] is None:
            self.temporal_extent["start"] = start_time
        elif start_time < self.temporal_extent["start"]:
            self.temporal_extent["start"] = start_time

        if self.temporal_extent["end"] is None:
            self.temporal_extent["end"] = end_time
        elif end_time > self.temporal_extent["end"]:
            self.temporal_extent["end"] = end_time

    async def _create_stac_item(self, result: Dict[str, Any]) -> pystac.Item:
        """Create a STAC Item from a result dictionary.

        Parameters
        ----------
            result (Dict[str, Any]): Dictionary containing item metadata

        Returns
        --------
            pystac.Item: Created STAC item
        """
        id = result.get(self.uniq_key, "")
        params_dict = (
            ast.literal_eval(str(self.assets_prereqs.get("only_params")))
            if self.assets_prereqs.get("only_params", "")
            else {}
        )
        python_params = " ".join(
            f"{k}='{v}',"
            for k, v in params_dict.items()
            if k not in ("translate", "start")
        )

        cli_params = " ".join(
            f"{k}={v}" for k, v in params_dict.items()
            if k not in ("translate", "start")
        )

        api_params = "&".join(
            f"{k}={v}" for k, v in params_dict.items()
            if k not in ("translate", "start")
        )
        intake_desc = dedent(
            f"""
            # Installing Intake-ESM
            ```bash
            # Method 1: Using pip
            pip install intake-esm
            # Method 2: Using conda (recommended)
            conda install -c conda-forge intake-esm
            ```
            # Quick Guide: INTAKE-ESM Catalog (Python)
            ```python
            import intake
            # create a catalog object from a EMS JSON file containing dataset metadata
            cat = intake.open_esm_datastore('{
                str(self.assets_prereqs.get("full_endpoint")).replace(
                    "stac-catalogue",
                    "intake-catalogue"
                ) + f"&{self.uniq_key}={id}"
            }')
            ```
            """
        )

        zarr_desc = dedent(
            f"""
            # Accessing Zarr Data
            1. Install freva-client
            ```bash
            pip install freva-client
            ```
            2. Get the auth token and access the zarr data (Python) - recommended
            ```python
            from freva_client import authenticate, databrowser
            import xarray as xr
            token_info = authenticate(username=<your_username>, \\
                host='{self.config.proxy}')
            db = databrowser({python_params} {self.uniq_key}='{id}', \\
                            stream_zarr=True, host='{self.config.proxy}')
            xarray_dataset = xr.open_mfdataset(list(db))
            ```
            3. Get the auth token and access the zarr data (CLI)
            ```bash
            # Attention: jq has to be installed beforehand
            token=$(freva-client auth -u <username> --host {self.config.proxy}\\
                                                        |jq -r .access_token)
            freva-client databrowser data-search {cli_params} {self.uniq_key}={id} \\
                --zarr --host {self.config.proxy} --access-token $token
            ```
            4. Access the zarr data directly (API - language agnostic)
            ```bash
            curl -X GET {self.assets_prereqs.get('base_url')}api/ \\
            freva-nextgen/databrowser/load/\\
            {self.translator.flavour}?{api_params}\\
            &{self.uniq_key}={id} \\
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
            💡: Please make sure to have the required xarray packages installed.
            """
        )

        normalized_id = (
            id.replace("https://", "")
            .replace("http://", "")
            .replace("/", "-")
            .replace(".", "-")
            .lower()
            .strip()
        )
        bbox = result.get("bbox")
        if bbox:
            try:
                bbox = self.parse_bbox(bbox)
                self._update_spatial_extent(bbox)
            except ValueError as e:  # pragma: no cover
                logger.warning(f"Invalid bbox for {id}: {e}")
                bbox = None

        time = result.get("time")
        start_time = end_time = None
        if time:
            try:
                start_time, end_time = self.parse_datetime(time)
                self._update_temporal_extent(start_time, end_time)
            except ValueError as e:
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
                for k in self._config.solr_fields
                if k in result and result.get(k) is not None
            },
            "title": id,
        }
        item = pystac.Item(
            id=normalized_id,
            collection=self.collection.id,
            geometry=geometry,
            bbox=bbox,
            datetime=start_time or datetime.now(),
            properties=properties,
        )

        if start_time and end_time:
            item.common_metadata.start_datetime = start_time
            item.common_metadata.end_datetime = end_time

        # Necessary item links to make the STAC item valid
        links_to_add = [
            {
                "rel": "self",
                "target": f"./{normalized_id}.json",
                "media_type": "application/json"
            },
            {
                "rel": "root",
                "target": "../../catalog.json",
                "media_type": "application/json"
            },
            {
                "rel": "parent",
                "target": "../../catalog.json",
                "media_type": "application/json"
            },
            {
                "rel": "collection",
                "target": "../collection.json",
                "media_type": "application/json"
            }
        ]

        for link_info in links_to_add:
            # Since in some cases it adds `rel`, so we need to check if
            # it's already there
            if not any(link.rel == link_info["rel"] for link in item.links):
                link = pystac.Link(
                    rel=link_info["rel"],
                    target=link_info["target"],
                    media_type=link_info["media_type"]
                )
                # Set this link to not auto-resolve
                link.extra_fields["noresolve"] = True
                # Add to the item
                item.links.append(link)

        assets = {
            "freva-databrowser": pystac.Asset(
                href=(
                    f"{self.assets_prereqs.get('base_url')}databrowser/?"
                    f"{api_params}"
                    f"&{self.uniq_key}={id}"
                ),
                title="Freva Web DataBrowser",
                description=(
                    "Access the Freva web interface for data exploration and analysis"
                ),
                roles=["overview"],
                media_type="text/html",
            ),
            "intake-catalogue": pystac.Asset(
                href=(
                    str(self.assets_prereqs.get("full_endpoint")).replace(
                        "stac-catalogue", "intake-catalogue"
                    )
                    + f"&{self.uniq_key}={id}"
                ),
                title="Intake Catalogue",
                description=intake_desc,
                roles=["metadata"],
                media_type="application/json",
            ),
            "zarr-access": pystac.Asset(
                href=(
                    f"{self.assets_prereqs.get('base_url')}api/freva-nextgen/"
                    f"databrowser/load/{self.translator.flavour}?"
                    f"{api_params}&{self.uniq_key}={id}"
                ),
                title="Stream Zarr Data",
                description=zarr_desc,
                roles=["data"],
                media_type="application/vnd+zarr",
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
            "local-access": pystac.Asset(
                href=(
                    f"{self.assets_prereqs.get('base_url')}api/freva-nextgen/"
                    f"databrowser/data-search/{self.translator.flavour}/"
                    f"{self.uniq_key}?"
                    f"{api_params}"
                    f"&{self.uniq_key}={id}"
                ),
                title="Access data locally",
                description=local_access_desc,
                roles=["data"],
                media_type="application/netcdf"
            ),
        }

        for key, asset in assets.items():
            item.add_asset(key, asset)

        return item

    def finalize_stac_collection(self) -> None:
        """
        Finalize STAC collection by updating spatial and temporal extents.

        Raises
        ------
        Exception
            If collection validation fails
        """
        collection_desc = dedent(
            f"""
            ## {self.translator.flavour.upper()} Flavour Dataset Collection

            A curated climate datasets STAC Collection from the
            `{self.translator.flavour.upper()}` flavour, and
            specific search parameters through the Freva
            databrowser. Includes standardized metadata and direct
            data access capabilities.

        """
        ).strip()
        if self.spatial_extent["minx"] != float("inf") and self.spatial_extent[
            "maxx"
        ] != float("-inf"):

            bbox = [
                self.spatial_extent["minx"],
                self.spatial_extent["miny"],
                self.spatial_extent["maxx"],
                self.spatial_extent["maxy"],
            ]
            self.collection.extent.spatial = pystac.SpatialExtent([bbox])

        if self.temporal_extent["start"] and self.temporal_extent["end"]:
            self.collection.extent.temporal = pystac.TemporalExtent(
                [[self.temporal_extent["start"], self.temporal_extent["end"]]]
            )
        self.collection.description = collection_desc
        try:
            self.collection.validate()
        except Exception as e:  # pragma: no cover
            logger.error(f"Collection validation failed: {e}")

    async def validate_stac(self) -> Tuple[int, int]:
        """Validate search and get result counts."""
        self._set_catalogue_queries()
        self.query["facet.field"] = self._config.solr_fields + ["time", "bbox"]
        self.query["fl"] = [self.uniq_key] + self._config.solr_fields + ["time", "bbox"]
        async with self._session_get() as res:
            search_status, search = res
        total_count = int(search.get("response", {}).get("numFound", 0))
        return search_status, total_count

    async def init_stac_catalogue(
        self,
        request: Request,
    ) -> None:
        filtered_params = dict(request.query_params)
        filtered_params.pop('translate', None)
        filtered_params.pop('start', None)

        self.assets_prereqs = {
            "base_url": str(self.config.proxy) + "/",
            "full_endpoint": (
                f"{self.config.proxy}/"
                f"{str(request.url).split(str(request.base_url))[-1]}"
            ),
            "only_params": str(filtered_params) if filtered_params != {} else "",
        }

    def _add_object(self, name: str, content: str, mtime=None) -> bytes:
        content_bytes = content.encode("utf-8")
        info = ZipInfo(filename=name)
        ts = datetime.fromtimestamp(mtime) if mtime else datetime.now()
        info.date_time = ts.timetuple()[:6]
        info.compress_type = ZIP_DEFLATED

        with self._zip.open(info, mode="w") as fp:
            fp.write(content_bytes)
        return self._zip_stream.flush_and_read()

    async def add_object_async(self, name: str, content: str, mtime=None) -> AsyncIterator[bytes]:
        chunk = self._add_object(name, content, mtime)
        yield chunk

    async def finalize_zip(self) -> AsyncIterator[bytes]:
        self._zip.close()
        while True:
            chunk = self._zip_stream.flush_and_read()
            if not chunk:
                break
            yield chunk

    async def stream_catalog(self, collection_id: str) -> AsyncIterator[bytes]:
        catalog = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "static-catalog",
            "description": "Static STAC catalog for Freva databrowser search",
            "links": [
                {"rel": "root", "href": "./catalog.json", "type": "application/json"},
                {"rel": "child", "href": f"./collections/{collection_id}/collection.json", "type": "application/json"},
            ]
        }
        async for chunk in self.add_object_async("stac-catalog/catalog.json", json.dumps(catalog, indent=2)):
            yield chunk

    async def stream_collection(self, collection: Dict[str, Any], collection_id: str) -> AsyncIterator[bytes]:
        collection["links"] = [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "items", "href": "./items/*.json", "type": "application/json"}
        ]
        async for chunk in self.add_object_async(f"stac-catalog/collections/{collection_id}/collection.json",
                                                  json.dumps(collection, indent=2)):
            yield chunk

    async def stream_item(self, item: Dict[str, Any], collection_id: str) -> AsyncIterator[bytes]:
        async for chunk in self.add_object_async(f"stac-catalog/collections/{collection_id}/items/{item['id']}.json",
                                                  json.dumps(item, indent=2)):
            yield chunk

    async def close(self) -> AsyncIterator[bytes]:
        async for chunk in self.finalize_zip():
            yield chunk

    async def stream_stac_catalogue(self, collection_id: str) -> AsyncIterator[bytes]:
        logger.info("Streaming STAC Catalogue for %s", collection_id)
        try:
            async for chunk in self.stream_catalog(collection_id):
                yield chunk

            self.collection = await self._create_stac_collection(collection_id)

            async for item_batch in self._iter_stac_items():
                for item in item_batch:
                    async for chunk in self.stream_item(item.to_dict(), collection_id):
                        yield chunk

            self.finalize_stac_collection()
            async for chunk in self.stream_collection(self.collection.to_dict(), collection_id):
                yield chunk

            async for chunk in self.close():
                yield chunk

        except Exception as e:
            logger.error(f"STAC collection creation failed for {collection_id}: {e}")
            raise