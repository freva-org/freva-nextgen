"""Helper functions for zarr utilities."""

from typing import Dict, List, Literal, Optional, TypedDict, Union

from .auth import Auth
from .utils import do_request
from .utils.auth_utils import choose_token_strategy, load_token
from .utils.databrowser_utils import Config


class Status(TypedDict):
    """Representation of the status of a zarr store."""

    status: int
    reason: str


def convert(
    *paths: str,
    aggregate: Optional[Literal["auto", "merge", "concat"]] = None,
    host: Optional[str] = None,
    join: Literal["outer", "inner", "exact", "left", "right"] = "outer",
    compat: Literal["no_conflicts", "equals", "override"] = "override",
    data_vars: Literal["minimal", "different", "all"] = "minimal",
    coords: Literal["minimal", "different", "all"] = "minimal",
    dim: Optional[str] = None,
    group_by: Optional[str] = None,
    zarr_options: Optional[Dict[str, Union[bool, float]]] = None,
) -> List[str]:
    """Convert data files to a zarr store in the cloud.

    This method lets you convert data files in netCDF, hdf5, geotiff etc.
    to zarr-stores that are avialale via http.

    I can either directly map one input file to a zarr store or aggregate the
    files into one single zarr-store. There are three main aggregation modes.
    (``auto``, ``merge`` or ``concat``). Once you've chosen the main aggregation mode.
    You can fine tune the
    aggregation using the ``join``, ``compat``, ``data_vars``, ``coords``
    ``dim`` and ``group_by`` parameters to fine the the aggregation.

    Parameters
    ----------
    paths: str
        Collection of paths that are converted to zarr.
    aggregate: str, choices: None, auto, merge, concat
        None will not aggregate data. The string indicating how the
        aggregation should be done:
        - "auto": let the system choose if the datasets should be concatenated
          or mereged.
        - "merge": merge datasets as `variables`
        - "concat": concatenated datasets along a `dimension`

        This option is only taken into account it ``aggregate`` is not None.
    host: str, default: None
        Override the host name of the databrowser server. This is usually the
        url where the freva web site can be found. Such as www.freva.dkrz.de.
        By default no host name is given and the host name will be taken from
        the freva config file.

    join: str, choices: outer, inner, exact, left, right
        String indicating how to combine differing indexes
        - "outer": use the union of object indexes.
        - "inner": use the intersection of object indexes.
        - "left": use indexes from the first object with each dimension.
        - "right": use indexes from the last object with each dimension.
        - "exact": instead of aligning, errors when indexes to be aligned
                   are not equal.

        This option is only taken into account it ``aggregate`` is not None.
    compat: str, choices: no_conflicts, equals, override
        String indicating how to compare non-concatenated variables of the
        same name for:

        - "equals": all values and dimensions must be the same.
        - "no_conflicts": only values which are not null in both datasets
          must be equal. The returned dataset then contains the combination
          of all non-null values.
        - "override": skip comparing and pick variable from first dataset

        This option is only taken into account it ``aggregate`` is not None.
    data_vars: str, choices: minimal, different, all
        These data variables will be combined together:

        - "minimal": Only data variables in which the dimension already
          appears are included.
        - "different":  Data variables which are not equal (ignoring
           attributes) across all datasets are also concatenated (as well as
           all for which dimension already appears).
        - "all": All data variables will be concatenated.

        This option is only taken into account it ``aggregate`` is not None.
    coords: str, choices: minimal, different, all
        These coordinate variables will be combined together:

        - "minimal": Only coordinates in which the dimension already
           appears are included.
        - "different":  Coordinates which are not equal (ignoring
           attributes) across all datasets are also concatenated (as well as
           all for which dimension already appears).
        - "all": All coordinates will be concatenated.

        This option is only taken into account it ``aggregate`` is not None.
    dim: str
        Name of the dimension to concatenate along. This can either be a new
        dimension name, in which case it is added along axis=0, or an
        existing dimension name, in which case the location of the
        dimension is unchanged.

        This option is only taken into account it ``aggregate`` is not None.
    group_by: str
        If set, forces grouping by a signature key. Otherwise grouping
        is attempted only when direct combine fails.

        This option is only taken into account it ``aggregate`` is not None.
    zarr_options: dict, default: None
        Set additional options for creating the dynamic zarr streams. For
        example if you which to create public instead of a private url that
        expires in one hour you can set the the following options:
        ``zarr_options={"public": True, "ttl_seconds": 3600}``.


    Examples
    --------

    .. code-block: python

        from freva_client import authenticate
        from freva_client.zarr_utils import convert
        storage_options = authenticate()["headers"]

        urls = convert("/mnt/data/test1.nc", "/mnt/data/test2.nc")
        dset = xr.open_zarr(
            url,
            storage_options=storage_options
        )

    You can also create zarr stores that are public. For example creating a
    `temporary` public store that is valid for one day.

    .. code-block: python

        from freva_client import authenticate
        from freva_client.zarr_utils import convert
        storage_options = authenticate()["headers"]

        urls = convert(
            "/mnt/data/test1.nc",
            "/mnt/data/test2.nc",
            zarr_options={public: True, ttl_seconds: 86400}
        )
        dset = xr.open_zarr(urls[0])



    You can also be more specific on the aggregation operation

    .. code-block: python

        from freva_client import authenticate
        storage_options = authenticate()["headers"]
        url = convert(
            "/mnt/data/test1.nc", "/mnt/data/test2.nc",
            aggregate="concat",
            join="inner",
            dim="ensemble",
        )
        dset = xr.open_zarr(
            url,
            storage_options=storage_options
        )

        The ``zarr_options`` dictionary can be used to request public zarr
        stores:

    .. code-block: python

        from freva_client import authenticate
        _ = authenticate()
        url = convert(
            "/mnt/data/test1.nc", "/mnt/data/test2.nc",
            aggregate="concat",
            join="inner",
            dim="ensemble",
            zarr_options={"public": True, ttl_seconds: 86400}
        )
        dset = xr.open_zarr(url)



    """
    data: Dict[str, Optional[Union[str, bool, float, int, List[str]]]] = {
        "aggregate": aggregate,
        "join": join,
        "compat": compat,
        "data-vars": data_vars,
        "coords": coords,
        "dim": dim,
        "group_by": group_by,
        "path": list(paths),
    }
    zarr_options = zarr_options or {}
    _zarr_options = {
        "public": bool(zarr_options.get("public", False)),
        "ttl_seconds": float(zarr_options.get("ttl_seconds", 86400.0)),
    }
    data.update(_zarr_options)
    _cfg = Config(host)
    token = Auth().authenticate(config=_cfg)
    headers = {"Authorization": f"{token['token_type']} {token['access_token']}"}
    res = do_request(
        "POST",
        f"{_cfg.data_portal_url}/zarr/convert",
        data=data,
        headers=headers,
        fail_on_error=True,
    )
    if res and "urls" in res.json():
        urls: List[str] = res.json()["urls"]
        return urls
    raise ValueError("Cloud not aggregate data: {res.json()}")


def status(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    host: Optional[str] = None,
) -> Status:
    """Query the status of a pre signed zarr store.

    This method can be useful to check the state of a zarr store if clients
    like ``xarray`` file to load the data.


    Parameters
    ----------
    url: str
        The url of the zarr store that is should be checked.
    headers: Dict[str, str]
        Non-Public zarr stores will need a valid OAuth2 token to query the
        status.
    host: str, default: None
        Override the host name of the databrowser server. This is usually the
        url where the freva web site can be found. Such as www.freva.dkrz.de.
        By default no host name is given and the host name will be taken from
        the freva config file.


    Returns
    -------
    Status: Dict(status=0, reason="")

    The status `status` is a int between 0 `reason` represents a human readale
    reason of the status.

    """
    _cfg = Config(host)
    auth = Auth()
    token = load_token(auth.token_file)
    if not headers and choose_token_strategy(token) in (
        "use_token",
        "refresh_token",
    ):
        headers = auth.authenticate(host, _cfg)["headers"]
    res = do_request(
        "GET",
        f"{_cfg.data_portal_url}/zarr-utils/status",
        params={"url": url},
        headers=headers,
        fail_on_error=True,
    )
    stat = Status(status=5, reason="Unknown")
    if res:
        stat = res.json()
    return Status(status=stat["status"], reason=stat["reason"])
