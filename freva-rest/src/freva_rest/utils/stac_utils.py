""" utilities for the STAC."""

import re
from datetime import MAXYEAR, MINYEAR, datetime
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple, Union

import fsspec.core
from dateutil import parser

YEAR_ONLY = re.compile(r"^\d{1,4}$")


class Item:
    """ Item class which is compatible with pySTAC
     Item object """
    def __init__(
        self,
        id: str,
        collection: str,
        geometry: Optional[Dict[str, Any]],
        properties: Dict[str, Any],
        bbox: Optional[List[float]] = None,
    ):
        self.id = id
        self.collection = collection
        self.geometry = geometry
        self.properties = properties
        self.bbox = bbox
        self.stac_version = "1.1.0"
        self.stac_extensions: List[Any] = []
        self.links: List[Link] = []
        self.assets: Dict[str, Asset] = {}

    def add_link(self, link: "Link") -> None:
        self.links.append(link)

    def add_asset(self, key: str, asset: "Asset") -> None:
        self.assets[key] = asset

    def to_dict(self) -> Dict[str, Any]:
        item = {
            "type": "Feature",
            "stac_version": self.stac_version,
            "stac_extensions": self.stac_extensions,
            "id": self.id,
            "geometry": self.geometry,
            "properties": self.properties,
            "links": [link.to_dict() for link in self.links],
            "assets": {k: asset.to_dict() for k, asset in self.assets.items()},
            "collection": self.collection,
        }
        if self.bbox:
            item["bbox"] = self.bbox
        return item


class Link:
    """ Link class which is compatible with pySTAC
        Link object """
    def __init__(
        self,
        rel: str,
        href: str,
        type: str,
        noresolve: bool = False,
        extra_fields: Optional[Dict[str, Any]] = None,
    ):
        self.rel = rel
        self.href = href
        self.type = type
        self.noresolve = noresolve
        self.extra_fields = extra_fields or {}

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "rel": self.rel,
            "href": self.href,
            "type": self.type,
        }
        if self.noresolve:  # pragma: no cover
            d["noresolve"] = True
        d.update(self.extra_fields)
        return d


class Asset:
    """ Asset class which is compatible with pySTAC
        Asset object """
    def __init__(
        self,
        href: str,
        title: str,
        description: str,
        roles: List[str],
        media_type: str,
        extra_fields: Optional[Dict[str, Any]] = None,
    ):
        self.href = href
        self.title = title
        self.description = description
        self.roles = roles
        self.media_type = media_type
        self.extra_fields = extra_fields or {}

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "href": self.href,
            "title": self.title,
            "description": self.description,
            "roles": self.roles,
            "type": self.media_type,
        }
        d.update(self.extra_fields)
        return d


def parse_datetime(time_str: str) -> Tuple[datetime, datetime]:
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
    start_str, end_str = time_str.strip("[]").split(" TO ")

    try:
        if YEAR_ONLY.fullmatch(start_str) and YEAR_ONLY.fullmatch(end_str):
            sy, ey = int(start_str), int(end_str)
            sy = max(MINYEAR, min(sy, MAXYEAR))
            ey = max(MINYEAR, min(ey, MAXYEAR))
            start_dt = datetime(sy, 1, 1)
            end_dt = datetime(ey, 12, 31, 23, 59, 59)
        else:
            start_dt = parser.parse(start_str)
            end_dt = parser.parse(end_str)
    except Exception:
        start_dt, end_dt = datetime.min, datetime.max

    return start_dt, end_dt


def parse_bbox(bbox_str: Union[str, List[str]]) -> List[float]:
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


def generate_local_access_desc(file_id: str) -> str:
    """
    Generate local access description based on file type and protocol.

    Parameters
    ----------
    file_id : str
        The file identifier/path

    Returns
    -------
    str
        Formatted description with appropriate access code
    """
    protocol, _ = fsspec.core.split_protocol(file_id)
    is_remote = protocol in ("s3", "gs", "gcs", "azure", "abfs", "http", "https")
    is_zarr = file_id.endswith(".zarr")

    if is_remote and is_zarr:
        # Remote zarr file
        desc = dedent(
            f"""
            # Accessing remote Zarr data
            ```python
            import fsspec
            import xarray as xr

            file = "{file_id}"
            protocol, _ = fsspec.core.split_protocol(file)
            anon_access = protocol in ("s3", "gs")
            mapper = fsspec.get_mapper(file, anon=anon_access)
            ds = xr.open_dataset(mapper, engine="zarr")
            ```
            💡: Requires fsspec and zarr packages: `pip install fsspec zarr`
            """
        )
    elif is_remote and not is_zarr:
        # Remote non-zarr file
        desc = dedent(
            f"""
            # Accessing remote data
            ```python
            import fsspec
            import xarray as xr

            file = "{file_id}"
            # For remote files, you might need authentication
            with fsspec.open(file) as f:
                ds = xr.open_dataset(f)
            # Or: direct access if supported
            # ds = xr.open_dataset(file)
            ```
            💡: Requires fsspec package: `pip install fsspec`
            💡: Some protocols may require authentication credentials
            """
        )
    elif not is_remote and is_zarr:
        # local zarr file
        desc = dedent(
            f"""
            # Accessing local Zarr data
            ```python
            import xarray as xr

            ds = xr.open_dataset("{file_id}", engine="zarr")
            ```
            💡: Requires zarr package: `pip install zarr`
            """
        )
    else:
        # local non-zarr file
        desc = dedent(
            f"""
            # Accessing data locally where the data is stored
            ```python
            import xarray as xr
            ds = xr.open_mfdataset('{file_id}')
            ```
            💡: Ensure required xarray packages are installed.
            """
        )

    return desc
