"""Query utilities for handling time and spatial queries."""

from datetime import datetime
from typing import Dict, List, Tuple, Union, Any

from dateutil.parser import ParserError, parse

from freva_rest.exceptions import ValidationError


def adjust_time_string(
    time: str,
    time_select: str = "flexible",
    backend_type: str = "solr",
    lookuptable: Dict[str, str] = None,
) -> Union[List[str], Tuple[str, Dict[str, datetime]], Dict[str, Any]]:
    """Adjust the time select keys to a backend-specific time query.

    Parameters
    ----------
    time: str, default: ""
        Special search facet to refine/subset search results by time.
    time_select: str, default: flexible
        Operator that specifies how the time period is selected.
    backend_type: str, default: solr
        The type of backend being used.
    lookuptable: Dict[str, str], optional
        Lookup table for field mappings.

    Returns
    -------
    Union[List[str], Tuple[str, Dict], Dict[str, Any]]:
        Backend-specific time query format.

    Raises
    ------
    ValidationError: If parsing the dates failed or if time_select is invalid.
    """
    if not time:
        return []

    time = "".join(time.split())
    select_methods: dict[str, str] = {
        "strict": "Within",
        "flexible": "Intersects",
        "file": "Contains",
    }
    
    try:
        solr_select = select_methods[time_select]
    except KeyError as exc:
        methods = ", ".join(select_methods.keys())
        raise ValidationError(f"Choose `time_select` from {methods}") from exc

    start, _, end = time.lower().partition("to")
    try:
        start_dt = parse(start or "1", default=datetime(1, 1, 1, 0, 0, 0))
        end_dt = parse(end or "9999", default=datetime(9999, 12, 31, 23, 59, 59))
        start = start_dt.isoformat()
        end = end_dt.isoformat()
    except ParserError as exc:
        raise ValueError(exc) from exc

    if backend_type == "RDBMS":
        lookuptable = lookuptable or {}
        time_conditions = {
            "flexible": f"""
                CAST({lookuptable.get('time_max')} AS timestamp) >= CAST(:start_ts AS timestamp) 
                AND CAST({lookuptable.get('time_min')} AS timestamp) <= CAST(:end_ts AS timestamp)
            """,
            "strict": f"""
                CAST({lookuptable.get('time_min')} AS timestamp) <= CAST(:start_ts AS timestamp) 
                AND CAST({lookuptable.get('time_max')} AS timestamp) >= CAST(:end_ts AS timestamp)
            """,
            "file": f"""
                CAST({lookuptable.get('time_min')} AS timestamp) >= CAST(:start_ts AS timestamp) 
                AND CAST({lookuptable.get('time_max')} AS timestamp) <= CAST(:end_ts AS timestamp)
            """
        }
        return time_conditions.get(time_select, time_conditions["flexible"]), {
            "start_ts": start_dt, 
            "end_ts": end_dt
        }
    
    if backend_type == "SE":
        lookuptable = lookuptable or {}
        time_min_field = lookuptable.get('time_min')
        time_max_field = lookuptable.get('time_max')
        
        time_conditions = {
            "flexible": {
                "bool": {
                    "must": [
                        {"range": {time_max_field: {"gte": start}}},
                        {"range": {time_min_field: {"lte": end}}}
                    ]
                }
            },
            "strict": {
                "bool": {
                    "must": [
                        {"range": {time_min_field: {"lte": start}}},
                        {"range": {time_max_field: {"gte": end}}}
                    ]
                }
            },
            "file": {
                "bool": {
                    "must": [
                        {"range": {time_min_field: {"gte": start}}},
                        {"range": {time_max_field: {"lte": end}}}
                    ]
                }
            }
        }
        return time_conditions.get(time_select, time_conditions["flexible"])
    
    # Solr format
    return [f"{{!field f=time op={solr_select}}}[{start} TO {end}]"]


def adjust_bbox_string(
    bbox: str,
    bbox_select: str = "flexible",
) -> List[str]:
    """Adjust the bbox select keys to a solr spatial query using RPT.

    Parameters
    ----------
    bbox: str, default: ""
        Special search facet to refine/subset search results by spatial extent.
    bbox_select: str, default: flexible
        Operator that specifies how the spatial extent is selected.

    Returns
    -------
    List[str]: Solr spatial query format.

    Raises
    ------
    ValidationError: If parsing failed or if bbox_select is invalid.
    """
    if not bbox:
        return []
    
    bbox = "".join(part.strip() for part in bbox.split())
    select_methods: dict[str, str] = {
        "strict": "Within",
        "flexible": "Intersects",
        "file": "Contains",
    }
    
    try:
        solr_select = select_methods[bbox_select.lower()]
    except KeyError as exc:
        methods = ", ".join(select_methods.keys())
        raise ValidationError(f"Choose `bbox_select` from {methods}") from exc

    try:
        min_lon, max_lon, min_lat, max_lat = bbox.split(",")

        if not (-180 <= float(min_lon) <= 180 and -180 <= float(max_lon) <= 180):
            raise ValidationError("Longitude must be between -180 and 180")
        if not (-90 <= float(min_lat) <= 90 and -90 <= float(max_lat) <= 90):
            raise ValidationError("Latitude must be between -90 and 90")

        bbox_str = f"ENVELOPE({min_lon},{max_lon},{max_lat},{min_lat})"
        return [f'bbox:"{solr_select}({bbox_str})"']
    except ValueError as exc:
        raise ValidationError(f"Failed to parse bbox string: {exc}") from exc


def join_facet_queries(key: str, facets: List[str], uniq_keys: Tuple[str, str]) -> Tuple[str, str]:
    """Create lucene search contain and NOT contain search queries."""
    escape_chars = (
        "+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "~", ":", "/",
    )
    
    negative, positive = [], []
    for search_value in facets:
        if key not in uniq_keys:
            search_value = search_value.lower()
        if search_value.lower().startswith("not "):
            negative.append(search_value[4:])
        elif search_value[0] in ("!", "-"):
            negative.append(search_value[1:])
        elif "_not_" in key:
            negative.append(search_value)
        else:
            positive.append(search_value)
    
    search_value_pos = " OR ".join(positive)
    search_value_neg = " OR ".join(negative)
    
    for char in escape_chars:
        search_value_pos = search_value_pos.replace(char, "\\" + char)
        search_value_neg = search_value_neg.replace(char, "\\" + char)
    
    return search_value_pos, search_value_neg