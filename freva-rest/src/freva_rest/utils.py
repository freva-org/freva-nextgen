"""Various utilities for the restAPI."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import redis.asyncio as redis
from dateutil import parser
from fastapi import HTTPException, status

from freva_rest.config import ServerConfig
from freva_rest.logger import logger

REDIS_CACHE: Optional[redis.Redis] = None
CACHING_SERVICES = set(("zarr-stream",))
"""All the services that need the redis cache."""
CONFIG = ServerConfig()


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
    clean_start_time = time_str.replace("[", "").split(" TO ")[0]
    clean_end_time = time_str.replace("]", "").split(" TO ")[1]
    return parser.parse(clean_start_time), parser.parse(clean_end_time)


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


def get_userinfo(user_info: Dict[str, str]) -> Dict[str, str]:
    """Convert a user_info dictionary to the UserInfo Model."""
    output = {}
    keys = {
        "email": ("mail", "email"),
        "username": ("preferred-username", "user-name", "uid"),
        "last_name": ("last-name", "family-name", "name", "surname"),
        "first_name": ("first-name", "given-name"),
    }
    for key, entries in keys.items():
        for entry in entries:
            if user_info.get(entry):
                output[key] = user_info[entry]
                break
            if user_info.get(entry.replace("-", "_")):
                output[key] = user_info[entry.replace("-", "_")]
                break
            if user_info.get(entry.replace("-", "")):
                output[key] = user_info[entry.replace("-", "")]
                break
    # Strip all the middle names
    name = output.get("first_name", "") + " " + output.get("last_name", "")
    output["first_name"] = name.partition(" ")[0]
    output["last_name"] = name.rpartition(" ")[-1]
    return output


async def create_redis_connection(
    cache: Optional[redis.Redis] = REDIS_CACHE,
) -> redis.Redis:
    """Reuse a potentially created redis connection."""
    kwargs = dict(
        host=CONFIG.redis_url,
        port=CONFIG.redis_port,
        username=CONFIG.redis_user or None,
        password=CONFIG.redis_password or None,
        ssl=CONFIG.redis_ssl_certfile is not None,
        ssl_certfile=CONFIG.redis_ssl_certfile or None,
        ssl_keyfile=CONFIG.redis_ssl_keyfile or None,
        ssl_ca_certs=CONFIG.redis_ssl_certfile or None,
        db=0,
    )
    if CACHING_SERVICES - CONFIG.services == CACHING_SERVICES:
        # All services that would need caching are disabled.
        # If this is the case and we ended up here, we shouldn't be here.
        # tell the users.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not enabled.",
        )

    if cache is None:
        logger.info("Creating redis connection using: %s", kwargs)
    cache = cache or redis.Redis(
        host=CONFIG.redis_url,
        port=CONFIG.redis_port,
        username=CONFIG.redis_user or None,
        password=CONFIG.redis_password or None,
        ssl=CONFIG.redis_ssl_certfile is not None,
        ssl_certfile=CONFIG.redis_ssl_certfile or None,
        ssl_keyfile=CONFIG.redis_ssl_keyfile or None,
        ssl_ca_certs=CONFIG.redis_ssl_certfile or None,
        db=0,
    )
    try:
        await cache.ping()
    except Exception as error:
        logger.error("Cloud not connect to redis cache: %s", error)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cache gone.",
        ) from None
    return cache


def str_to_int(inp_str: Optional[str], default: int) -> int:
    """Convert an integer from a string. If it's not working return default."""
    inp_str = inp_str or ""
    try:
        return int(inp_str)
    except ValueError:
        return default
