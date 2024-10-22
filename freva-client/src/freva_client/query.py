"""Query climate data sets by using-key value pair search queries."""

import asyncio
import concurrent.futures
import multiprocessing as mp
import os
import sys
from collections import defaultdict
from fnmatch import fnmatch
from functools import cached_property
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Collection,
    Coroutine,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import intake
import intake_esm
import numpy as np
import requests
import xarray as xr
import yaml
from rich import print as pprint

from .auth import Auth
from .utils import logger
from .utils.databrowser_utils import Config

__all__ = ["databrowser"]


def run_async_in_thread(
    coro_func: Callable[..., Coroutine[Any, Any, Any]], *args: Any, **kwargs: Any
) -> Any:
    """Run an async function in a new event loop in a thread."""
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    try:
        result = new_loop.run_until_complete(coro_func(*args, **kwargs))
    finally:
        new_loop.close()
    return result  # pragma: no cover


class databrowser:
    """Find data in the system.

    You can either search for files or uri's. Uri's give you an information
    on the storage system where the files or objects you are looking for are
    located. The query is of the form ``key=value``. For ``value`` you might
    use wild cards such as \\*, ? or any regular expression.

    Parameters
    ~~~~~~~~~~

    *facets: str
        If you are not sure about the correct search key's you can use
        positional arguments to search of any matching entries. For example
        'era5' would allow you to search for any entries
        containing era5, regardless of project, product etc.
    **search_keys: str
        The search constraints applied in the data search. If not given
        the whole dataset will be queried.
    flavour: str, default: freva
        The Data Reference Syntax (DRS) standard specifying the type of climate
        datasets to query. You can get an overview by using the
        :py:meth:databrowser.overview class method to retrieve information
        on the available search flavours and their different search keys.
    time: str, default: ""
        Special search key to refine/subset search results by time.
        This can be a string representation of a time range or a single
        timestamp. The timestamps has to follow ISO-8601. Valid strings are
        ``%Y-%m-%dT%H:%M to %Y-%m-%dT%H:%M`` for time ranges or
        ``%Y-%m-%dT%H:%M`` for single time stamps.
        **Note**: You don't have to give the full string format to subset
        time steps: `%Y`, `%Y-%m` etc are also valid.
    time_select: str, default: flexible
        Operator that specifies how the time period is selected. Choose from
        flexible (default), strict or file. ``strict`` returns only those files
        that have the `entire` time period covered. The time search ``2000 to
        2012`` will not select files containing data from 2010 to 2020 with
        the ``strict`` method. ``flexible`` will select those files as
        ``flexible`` returns those files that have either start or end period
        covered. ``file`` will only return files where the entire time
        period is contained within `one single` file.
    uniq_key: str, default: file
        Chose if the solr search query should return paths to files or
        uris, uris will have the file path along with protocol of the storage
        system. Uris can be useful if the search query result should be
        used libraries like fsspec.
    host: str, default: None
        Override the host name of the databrowser server. This is usually the
        url where the freva web site can be found. Such as www.freva.dkrz.de.
        By default no host name is given and the host name will be taken from
        the freva config file.
    stream_zarr: bool, default: False
        Create a zarr stream for all search results. When set to true the
        files are served in zarr format and can be opened from anywhere.
    multiversion: bool, default: False
        Select all versions and not just the latest version (default).
    fail_on_error: bool, default: False
        Make the call fail if the connection to the databrowser could not
        be established.


    Attributes
    ~~~~~~~~~~

    url: str
        the url of the currently selected databrowser api server
    metadata: dict[str, str]
        The available search keys, or metadata, found for the applied search
        constraints. This can be useful for reverse searches.


    Example
    ~~~~~~~

    Search for the cmorph datasets. Suppose we know that the experiment name
    of this dataset is cmorph therefore we can create in instance of the
    databrowser class using the ``experiment`` search constraint.
    If you just 'print' the created object you will get a quick overview:

    .. execute_code::

        from freva_client import databrowser
        db = databrowser(experiment="cmorph", uniq_key="uri")
        print(db)

    After having created the search object you can acquire different kinds of
    information like the number of found objects:

    .. execute_code::

        from freva_client import databrowser
        db = databrowser(experiment="cmorph", uniq_key="uri")
        print(len(db))
        # Get all the search keys associated with this search

    Or you can retrieve the combined metadata of the search objects.

    .. execute_code::

        from freva_client import databrowser
        db = databrowser(experiment="cmorph", uniq_key="uri")
        print(db.metadata)

    Most importantly you can retrieve the locations of all encountered objects

    .. execute_code::

        from freva_client import databrowser
        db = databrowser(experiment="cmorph", uniq_key="uri")
        for file in db:
            pass
        all_files = sorted(db)
        print(all_files[0])


    You can also set a different flavour, for example according to cmip6
    standard:

    .. execute_code::

        from freva_client import databrowser
        db = databrowser(flavour="cmip6", experiment_id="cmorph")
        print(db.metadata)


    Sometimes you don't exactly know the exact names of the search keys and
    want retrieve all file objects that match a certain category. For example
    for getting all ocean reanalysis datasets you can apply the 'reana*'
    search key as a positional argument:

    .. execute_code::

        from freva_client import databrowser
        db = databrowser("reana*", realm="ocean", flavour="cmip6")
        for file in db:
            print(file)

    If you don't have direct access to the data, for example because you are
    not directly logged in to the computer where the data is stored you can
    set ``stream_zarr=True``. The data will then be
    provisioned in zarr format and can be opened from anywhere. But bear in
    mind that zarr streams if not accessed in time will expire. Since the
    data can be accessed from anywhere you will also have to authenticate
    before you are able to access the data. Refer also to the
    :py:meth:`freva_client.authenticate` method.

    .. execute_code::

        from freva_client import authenticate, databrowser
        token_info = authenticate(username="janedoe")
        db = databrowser(dataset="cmip6-fs", stream_zarr=True)
        zarr_files = list(db)
        print(zarr_files)

    After you have created the paths to the zarr files you can open them

    ::

        import xarray as xr
        dset = xr.open_dataset(
           zarr_files[0],
           chunks="auto",
           engine="zarr",
           storage_options={"header":
                {"Authorization": f"Bearer {token_info['access_token']}"}
           }
        )


    """

    def __init__(
        self,
        *facets: str,
        uniq_key: Literal["file", "uri"] = "file",
        flavour: Literal[
            "freva", "cmip6", "cmip5", "cordex", "nextgems", "user"
        ] = "freva",
        time: Optional[str] = None,
        host: Optional[str] = None,
        time_select: Literal["flexible", "strict", "file"] = "flexible",
        stream_zarr: bool = False,
        multiversion: bool = False,
        fail_on_error: bool = False,
        **search_keys: Union[str, List[str]],
    ) -> None:
        self._auth = Auth()
        self._fail_on_error = fail_on_error
        self._cfg = Config(host, uniq_key=uniq_key, flavour=flavour)
        self._flavour = flavour
        self._stream_zarr = stream_zarr
        self.payload_metadata: dict[str, Collection[Collection[str]]] = {}
        self.suffixes = [".nc", ".nc4", ".grb", ".grib", ".zarr", "zar"]
        self.batch_size = 150
        self._lock = Lock()
        self.user_metadata: List[Dict[str, Union[str, List[str], Dict[str, str]]]] = []
        facet_search: Dict[str, List[str]] = defaultdict(list)
        for key, value in search_keys.items():
            if isinstance(value, str):
                facet_search[key] = [value]
            else:
                facet_search[key] = value
        self._params: Dict[str, Union[str, bool, List[str]]] = {
            **{"multi-version": multiversion},
            **search_keys,
        }

        if time:
            self._params["time"] = time
            self._params["time_select"] = time_select
        if facets:
            self._add_search_keyword_args_from_facet(facets, facet_search)

    def _add_search_keyword_args_from_facet(
        self, facets: Tuple[str, ...], search_kw: Dict[str, List[str]]
    ) -> None:
        metadata = {
            k: v[::2] for (k, v) in self._facet_search(extended_search=True).items()
        }
        primary_key = list(metadata.keys() or ["project"])[0]
        num_facets = 0
        for facet in facets:
            for key, values in metadata.items():
                for value in values:
                    if fnmatch(value, facet):
                        num_facets += 1
                        search_kw[key].append(value)

        if facets and num_facets == 0:
            # TODO: This isn't pretty, but if a user requested a search
            # string that doesn't exist than we have to somehow make the search
            # return nothing.
            search_kw = {primary_key: ["NotAvailable"]}
        self._params.update(search_kw)

    def __iter__(self) -> Iterator[str]:
        query_url = self._cfg.search_url
        headers = {}
        if self._stream_zarr:
            query_url = self._cfg.zarr_loader_url
            token = self._auth.check_authentication(auth_url=self._cfg.auth_url)
            headers = {"Authorization": f"Bearer {token['access_token']}"}
        result = self._get(query_url, headers=headers, stream=True)
        if result is not None:
            try:
                for res in result.iter_lines():
                    yield res.decode("utf-8")
            except KeyboardInterrupt:
                pprint("[red][b]User interrupt: Exit[/red][/b]", file=sys.stderr)

    def __repr__(self) -> str:
        params = ", ".join(
            [f"{k.replace('-', '_')}={v}" for (k, v) in self._params.items()]
        )
        return (
            f"{self.__class__.__name__}(flavour={self._flavour}, "
            f"host={self.url}, {params})"
        )

    def _repr_html_(self) -> str:
        params = ", ".join(
            [f"{k.replace('-', '_')}={v}" for (k, v) in self._params.items()]
        )

        found_objects_count = len(self)

        available_flavours = ", ".join(
            flavour for flavour in self._cfg.overview["flavours"]
        )
        available_search_facets = ", ".join(
            facet for facet in self._cfg.overview["attributes"][self._flavour]
        )

        # Create a table-like structure for available flavors and search facets
        style = 'style="text-align: left"'
        facet_heading = f"Available search facets for <em>{self._flavour}</em> flavour"
        html_repr = (
            "<table>"
            f"<tr><th colspan='2' {style}>{self.__class__.__name__}"
            f"(flavour={self._flavour}, host={self.url}, "
            f"{params})</th></tr>"
            f"<tr><td><b># objects</b></td><td {style}>{found_objects_count}"
            "</td></tr>"
            f"<tr><td valign='top'><b>{facet_heading}</b></td>"
            f"<td {style}>{available_search_facets}</td></tr>"
            "<tr><td valign='top'><b>Available flavours</b></td>"
            f"<td {style}>{available_flavours}</td></tr>"
            "</table>"
        )

        return html_repr

    def __len__(self) -> int:
        """Query the total number of found objects.

        Example
        ~~~~~~~
        .. execute_code::

            from freva_client import databrowser
            print(len(databrowser(experiment="cmorph")))


        """
        result = self._get(self._cfg.metadata_url)
        if result:
            return cast(int, result.json().get("total_count", 0))
        return 0

    def _create_intake_catalogue_file(self, filename: str) -> None:
        """Create an intake catalogue file."""
        kwargs: Dict[str, Any] = {"stream": True}
        url = self._cfg.intake_url
        if self._stream_zarr:
            token = self._auth.check_authentication(auth_url=self._cfg.auth_url)
            url = self._cfg.zarr_loader_url
            kwargs["headers"] = {"Authorization": f"Bearer {token['access_token']}"}
            kwargs["params"] = {"catalogue-type": "intake"}
        result = self._get(url, **kwargs)
        if result is None:
            raise ValueError("No results found")

        try:
            Path(filename).parent.mkdir(exist_ok=True, parents=True)
            with open(filename, "bw") as stream:
                for content in result.iter_content(decode_unicode=False):
                    stream.write(content)
        except Exception as error:
            raise ValueError(f"Couldn't write catalogue content: {error}") from None

    def intake_catalogue(self) -> intake_esm.core.esm_datastore:
        """Create an intake esm catalogue object from the search.

        This method creates a intake-esm catalogue from the current object
        search. Instead of having the original files as target objects you can
        also choose to stream the files via zarr.

        Returns
        ~~~~~~~
        intake_esm.core.esm_datastore: intake-esm catalogue.

        Raises
        ~~~~~~
        ValueError: If user is not authenticated or catalogue creation failed.

        Example
        ~~~~~~~
        Let's create an intake-esm catalogue that points points allows for
        streaming the target data as zarr:

        .. execute_code::

            from freva_client import databrowser
            db = databrowser(dataset="cmip6-hsm", stream_zarr=True)
            cat = db.intake_catalogue()
            print(cat.df)
        """
        with NamedTemporaryFile(suffix=".json") as temp_f:
            self._create_intake_catalogue_file(temp_f.name)
            return intake.open_esm_datastore(temp_f.name)

    @classmethod
    def count_values(
        cls,
        *facets: str,
        flavour: Literal[
            "freva", "cmip6", "cmip5", "cordex", "nextgems", "user"
        ] = "freva",
        time: Optional[str] = None,
        host: Optional[str] = None,
        time_select: Literal["flexible", "strict", "file"] = "flexible",
        multiversion: bool = False,
        fail_on_error: bool = False,
        extended_search: bool = False,
        **search_keys: Union[str, List[str]],
    ) -> Dict[str, Dict[str, int]]:
        """Count the number of objects in the databrowser.

        Parameters
        ~~~~~~~~~~

        *facets: str
            If you are not sure about the correct search key's you can use
            positional arguments to search of any matching entries. For example
            'era5' would allow you to search for any entries
            containing era5, regardless of project, product etc.
        flavour: str, default: freva
            The Data Reference Syntax (DRS) standard specifying the type of climate
            datasets to query.
        time: str, default: ""
            Special search facet to refine/subset search results by time.
            This can be a string representation of a time range or a single
            timestamp. The timestamp has to follow ISO-8601. Valid strings are
            ``%Y-%m-%dT%H:%M`` to ``%Y-%m-%dT%H:%M`` for time ranges and
            ``%Y-%m-%dT%H:%M``. **Note**: You don't have to give the full string
            format to subset time steps ``%Y``, ``%Y-%m`` etc are also valid.
        time_select: str, default: flexible
            Operator that specifies how the time period is selected. Choose from
            flexible (default), strict or file. ``strict`` returns only those files
            that have the *entire* time period covered. The time search ``2000 to
            2012`` will not select files containing data from 2010 to 2020 with
            the ``strict`` method. ``flexible`` will select those files as
            ``flexible`` returns those files that have either start or end period
            covered. ``file`` will only return files where the entire time
            period is contained within `one single` file.
        extended_search: bool, default: False
            Retrieve information on additional search keys.
        host: str, default: None
            Override the host name of the databrowser server. This is usually
            the url where the freva web site can be found. Such as
            www.freva.dkrz.de. By default no host name is given and the host
            name will be taken from the freva config file.
        multiversion: bool, default: False
            Select all versions and not just the latest version (default).
        fail_on_error: bool, default: False
            Make the call fail if the connection to the databrowser could not
        **search_keys: str
            The search constraints to be applied in the data search. If not given
            the whole dataset will be queried.

        Returns
        ~~~~~~~
        dict[str, int]:
            Dictionary with the number of objects for each search facet/key
            is given.

        Example
        ~~~~~~~

        .. execute_code::

            from freva_client import databrowser
            print(databrowser.count_values(experiment="cmorph"))

        .. execute_code::

            from freva_client import databrowser
            print(databrowser.count_values("model"))

        Sometimes you don't exactly know the exact names of the search keys and
        want retrieve all file objects that match a certain category. For
        example for getting all ocean reanalysis datasets you can apply the
        'reana*' search key as a positional argument:

        .. execute_code::

            from freva_client import databrowser
            print(databrowser.count_values("reana*", realm="ocean", flavour="cmip6"))

        """
        this = cls(
            *facets,
            flavour=flavour,
            time=time,
            time_select=time_select,
            host=host,
            multiversion=multiversion,
            fail_on_error=fail_on_error,
            uniq_key="file",
            stream_zarr=False,
            **search_keys,
        )
        result = this._facet_search(extended_search=extended_search)
        counts = {}
        for facet, value_counts in result.items():
            counts[facet] = dict(zip(value_counts[::2], map(int, value_counts[1::2])))
        return counts

    @cached_property
    def metadata(self) -> Dict[str, List[str]]:
        """Get the metadata (facets) for the current databrowser query.

        You can retrieve all information that is associated with your current
        databrowser search. This can be useful for reverse searches for example
        for retrieving metadata of object stores or file/directory names.

        Example
        ~~~~~~~

        Reverse search: retrieving meta data from a known file

        .. execute_code::

            from freva_client import databrowser
            db = databrowser(uri="slk:///arch/*/CPC/*")
            print(db.metadata)


        """
        return {
            k: v[::2] for (k, v) in self._facet_search(extended_search=True).items()
        }

    @classmethod
    def metadata_search(
        cls,
        *facets: str,
        flavour: Literal[
            "freva", "cmip6", "cmip5", "cordex", "nextgems", "user"
        ] = "freva",
        time: Optional[str] = None,
        host: Optional[str] = None,
        time_select: Literal["flexible", "strict", "file"] = "flexible",
        multiversion: bool = False,
        fail_on_error: bool = False,
        extended_search: bool = False,
        **search_keys: Union[str, List[str]],
    ) -> Dict[str, List[str]]:
        """Search for data attributes (facets) in the databrowser.

        The method queries the databrowser for available search facets (keys)
        like model, experiment etc.

        Parameters
        ~~~~~~~~~~

        *facets: str
            If you are not sure about the correct search key's you can use
            positional arguments to search of any matching entries. For example
            'era5' would allow you to search for any entries
            containing era5, regardless of project, product etc.
        flavour: str, default: freva
            The Data Reference Syntax (DRS) standard specifying the type of climate
            datasets to query.
        time: str, default: ""
            Special search facet to refine/subset search results by time.
            This can be a string representation of a time range or a single
            timestamp. The timestamp has to follow ISO-8601. Valid strings are
            ``%Y-%m-%dT%H:%M`` to ``%Y-%m-%dT%H:%M`` for time ranges and
            ``%Y-%m-%dT%H:%M``. **Note**: You don't have to give the full string
            format to subset time steps ``%Y``, ``%Y-%m`` etc are also valid.
        time_select: str, default: flexible
            Operator that specifies how the time period is selected. Choose from
            flexible (default), strict or file. ``strict`` returns only those files
            that have the *entire* time period covered. The time search ``2000 to
            2012`` will not select files containing data from 2010 to 2020 with
            the ``strict`` method. ``flexible`` will select those files as
            ``flexible`` returns those files that have either start or end period
            covered. ``file`` will only return files where the entire time
            period is contained within *one single* file.
        extended_search: bool, default: False
            Retrieve information on additional search keys.
        multiversion: bool, default: False
            Select all versions and not just the latest version (default).
        host: str, default: None
            Override the host name of the databrowser server. This is usually
            the url where the freva web site can be found. Such as
            www.freva.dkrz.de. By default no host name is given and the host
            name will be taken from the freva config file.
        fail_on_error: bool, default: False
            Make the call fail if the connection to the databrowser could not
        **search_keys: str, list[str]
            The facets to be applied in the data search. If not given
            the whole dataset will be queried.

        Returns
        ~~~~~~~
        dict[str, list[str]]:
            Dictionary with a list search facet values for each search facet key


        Example
        ~~~~~~~

        .. execute_code::

            from freva_client import databrowser
            all_facets = databrowser.metadata_search(project='obs*')
            print(all_facets)

        You can also search for all metadata matching a search string:

        .. execute_code::

            from freva_client import databrowser
            spec_facets = databrowser.metadata_search("obs*")
            print(spec_facets)

        Get all models that have a given time step:

        .. execute_code::

            from freva_client import databrowser
            model = databrowser.metadata_search(
                project="obs*",
                time="2016-09-02T22:10"
            )
            print(model)

        Reverse search: retrieving meta data from a known file

        .. execute_code::

            from freva_client import databrowser
            res = databrowser.metadata_search(file="/arch/*CPC/*")
            print(res)

        Sometimes you don't exactly know the exact names of the search keys and
        want retrieve all file objects that match a certain category. For
        example for getting all ocean reanalysis datasets you can apply the
        'reana*' search key as a positional argument:

        .. execute_code::

            from freva_client import databrowser
            print(databrowser.metadata_search("reana*", realm="ocean", flavour="cmip6"))

        """
        this = cls(
            *facets,
            flavour=flavour,
            time=time,
            time_select=time_select,
            host=host,
            multiversion=multiversion,
            fail_on_error=fail_on_error,
            uniq_key="file",
            stream_zarr=False,
            **search_keys,
        )
        return {
            k: v[::2]
            for (k, v) in this._facet_search(extended_search=extended_search).items()
        }

    @classmethod
    def overview(cls, host: Optional[str] = None) -> str:
        """Get an overview over the available search options.

        If you don't know what search flavours or search keys you can use
        for searching the data you can use this method to get an overview
        over what is available.

        Parameters
        ~~~~~~~~~~

        host: str, default None
            Override the host name of the databrowser server. This is usually
            the url where the freva web site can be found. Such as
            www.freva.dkrz.de. By default no host name is given and the host
            name will be taken from the freva config file.

        Returns
        ~~~~~~~
        str: A string representation over what is available.

        Example
        ~~~~~~~

        .. execute_code::

            from freva_client import databrowser
            print(databrowser.overview())
        """
        overview = Config(host).overview.copy()
        overview["Available search flavours"] = overview.pop("flavours")
        overview["Search attributes by flavour"] = overview.pop("attributes")
        return yaml.safe_dump(overview)

    @property
    def url(self) -> str:
        """Get the url of the databrowser API.

        Example
        ~~~~~~~

        .. execute_code::

            from freva_client import databrowser
            db = databrowser()
            print(db.url)

        """
        return self._cfg.databrowser_url

    def _facet_search(
        self,
        extended_search: bool = False,
    ) -> Dict[str, List[str]]:
        result = self._get(self._cfg.metadata_url)
        if result is None:
            return {}
        data = result.json()
        if extended_search:
            constraints = data["facets"].keys()
        else:
            constraints = data["primary_facets"]
        return {f: v for f, v in data["facets"].items() if f in constraints}

    def userdata(
        self,
        action: Literal["add", "delete"],
        userdata_items: List[Union[str, xr.Dataset]] = [],
        metadata: Dict[str, str] = {},
    ) -> None:
        """Add or delete user data in the databrowser system.

        Manage user data in the databrowser system by adding new data or
        deleting existing data.

        For the "add" action, the user can provide data items (file paths
        or xarray datasets)
        along with metadata (key-value pairs) to categorize and organize
        the data.

        For the "delete" action, the user provides metadata as search
        criteria to identify and remove the existing data from the
        system.

        Parameters
        ~~~~~~~~~~
        action : Literal["add", "delete"]
            The action to perform: "add" to add new data, or "delete"
            to remove existing data.
        userdata_items : List[Union[str, xr.Dataset]], optional
            A list of user file paths or xarray datasets to add to the
            databrowser (required for "add").
        metadata : Dict[str, str], optional
            Key-value metadata pairs to categorize the data (for "add")
            or search and identify data for
            deletion (for "delete").

        Raises
        ~~~~~~
        ValueError
            If the operation fails or required parameters are missing
            for the specified action.
        FileNotFoundError
            If no user data is provided for the "add" action.

        Example
        ~~~~~~~

        Adding user data:

        .. execute_code::
            from freva_client import authenticate, databrowser
            import xarray as xr
            token_info = authenticate(username="janedoe")
            filename1 = (
                "./freva-rest/src/databrowser_api/mock/data/model/regional/cordex/output/EUR-11/"
                "GERICS/NCC-NorESM1-M/rcp85/r1i1p1/GERICS-REMO2015/v1/3hr/pr/v20181212/"
                "pr_EUR-11_NCC-NorESM1-M_rcp85_r1i1p1_GERICS-REMO2015_v2_3hr_200701020130-200701020430.nc"
            )
            filename2 = (
                "./freva-rest/src/databrowser_api/mock/data/model/regional/cordex/output/EUR-11/"
                "CLMcom/MPI-M-MPI-ESM-LR/historical/r0i0p0/CLMcom-CCLM4-8-17/v1/fx/orog/v20140515/"
                "orog_EUR-11_MPI-M-MPI-ESM-LR_historical_r1i1p1_CLMcom-CCLM4-8-17_v1_fx.nc"
            xarray_data = xr.open_dataset(filename1)
            db = databrowser()
            db.userdata(
                action="add",
                userdata_items=[xarray_data, filename2],
                metadata={"project": "cmip5", "experiment": "myFavExp"}
        )

        Deleting user data:

        .. execute_code::
            from freva_client import authenticate, databrowser
            token_info = authenticate(username="janedoe")
            db = databrowser()
            db.userdata(
                action="delete",
                metadata={"project": "cmip5", "experiment": "myFavExp"}
            )
        """
        url = f"{self._cfg.userdata_url}"
        token = self._auth.check_authentication(auth_url=self._cfg.auth_url)
        headers = {"Authorization": f"Bearer {token['access_token']}"}

        if action == "add":
            if not userdata_items:
                raise FileNotFoundError(
                    "No data items found. Please provide data to add."
                )

            self.metadata_collection: List[
                Dict[str, Union[str, List[str], Dict[str, str]]]
            ] = []
            self.executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=min(mp.cpu_count(), 15)
            )
            try:
                self.validated_userdata = self._validate_user_data(userdata_items)
                asyncio.run(self._process_user_data())
            finally:
                self.executor.shutdown(wait=True)
                logger.info("Executor shutdown completed successfully.")

            if self.user_metadata:
                self.payload_metadata = {
                    "user_metadata": self.user_metadata,
                    "facets": metadata,
                }
                result = self._post(url, data=self.payload_metadata, headers=headers)
                if result is None:
                    raise ValueError("Failed to add user data")
            else:
                raise ValueError("No metadata generated from the input data.")

        if action == "delete":
            if userdata_items:
                logger.info(
                    "'userdata_items' are not needed for the 'delete'"
                    "action and will be ignored."
                )

            result = self._delete(url, headers=headers, json=metadata)
            if result is None:
                raise ValueError("Failed to delete user data")

    def _get(self, url: str, **kwargs: Any) -> Optional[requests.models.Response]:
        """Apply the get method to the databrowser."""
        logger.debug("Searching %s with parameters: %s", url, self._params)
        params = kwargs.pop("params", {})
        kwargs.setdefault("timeout", 30)
        try:
            res = requests.get(url, params={**self._params, **params}, **kwargs)
            res.raise_for_status()
            return res
        except KeyboardInterrupt:
            pprint("[red][b]User interrupt: Exit[/red][/b]", file=sys.stderr)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
        ) as error:
            msg = f"Search request failed with {error}"
            if self._fail_on_error:
                raise ValueError(msg) from None
            logger.warning(msg)
        return None

    def _post(
        self, url: str, data: Dict[str, Any], **kwargs: Any
    ) -> Optional[requests.models.Response]:
        """Apply the POST method to the databrowser."""
        logger.debug(
            "POST request to %s with data: %s and parameters: %s",
            url,
            data,
            self._params,
        )
        kwargs.setdefault("timeout", 30)
        try:
            res = requests.post(url, json=data, **kwargs)
            res.raise_for_status()
            return res
        except KeyboardInterrupt:
            pprint("[red][b]User interrupt: Exit[/red][/b]", file=sys.stderr)

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
        ) as error:
            msg = f"adding user data request failed with {error}"
            if self._fail_on_error:
                raise ValueError(msg) from None
            logger.warning(msg)
        return None

    def _delete(self, url: str, **kwargs: Any) -> Optional[requests.models.Response]:
        """Apply the DELETE method to the databrowser."""
        logger.debug("DELETE request to %s with parameters: %s", url, self._params)
        params = kwargs.pop("params", {})
        kwargs.setdefault("timeout", 30)
        try:
            res = requests.delete(url, params={**self._params, **params}, **kwargs)
            res.raise_for_status()
            return res
        except KeyboardInterrupt:
            pprint("[red][b]User interrupt: Exit[/red][/b]", file=sys.stderr)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
        ) as error:
            msg = f"DELETE request failed with {error}"
            if self._fail_on_error:
                raise ValueError(msg) from None
            logger.warning(msg)
        return None

    def _validate_user_data(
        self,
        user_data: Sequence[Union[str, xr.Dataset]],
    ) -> Tuple[List[os.PathLike[str]], List[xr.Dataset]]:

        validated_paths: List[os.PathLike[str]] = []
        validated_xarray_datasets: List[xr.Dataset] = []

        for data in user_data:
            if isinstance(data, str):
                path = Path(data)
                if not path.exists():
                    logger.warning(f"File path does not exist: {data}")
                    continue
                validated_paths.append(path)
            if isinstance(data, xr.Dataset):
                validated_xarray_datasets.append(data)

        if not validated_paths and not validated_xarray_datasets:
            raise FileNotFoundError("No valid file paths or xarray datasets found.")

        return validated_paths, validated_xarray_datasets

    async def _process_user_data(self) -> None:
        tasks: List[asyncio.Future[None]] = []
        tasks.extend(
            [
                asyncio.ensure_future(
                    self._submit_xarray_datasets(self.validated_userdata[1])
                )
            ]
        )
        tasks.extend(
            [
                asyncio.ensure_future(self._submit_paths(path))
                for path in self.validated_userdata[0]
            ]
        )
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _submit_xarray_datasets(
        self, xarray_datasets: list[xr.Dataset]
    ) -> None:
        xarray_datasets_to_process = []
        for xarray_dataset in xarray_datasets:
            xarray_datasets_to_process.append(xarray_dataset)
            if len(xarray_datasets_to_process) >= self.batch_size:
                self.executor.submit(
                    run_async_in_thread,
                    self._process_userdata_in_executor,
                    xarray_datasets_to_process,
                )
                xarray_datasets_to_process = []
        if xarray_datasets_to_process:
            self.executor.submit(
                run_async_in_thread,
                self._process_userdata_in_executor,
                xarray_datasets_to_process,
            )

    async def _submit_paths(self, path: os.PathLike[str]) -> None:
        paths_to_process = []
        if Path(path).is_file() and any(
            Path(path).suffix == suffix for suffix in self.suffixes
        ):
            paths_to_process.append(path)
        elif Path(path).is_dir():
            async for file_path in self._gather_files_from_dir(path):
                paths_to_process.append(file_path)
                if len(paths_to_process) >= self.batch_size:
                    self.executor.submit(
                        run_async_in_thread,
                        self._process_userdata_in_executor,
                        paths_to_process,
                    )
                    paths_to_process = []
        if paths_to_process:
            self.executor.submit(
                run_async_in_thread,
                self._process_userdata_in_executor,
                paths_to_process,
            )

    async def _gather_files_from_dir(
        self, path: os.PathLike[str]
    ) -> AsyncIterator[Path]:
        for item in Path(path).rglob("*"):
            if Path(item).is_file() and Path(item).suffix in self.suffixes:
                yield item

    async def _process_userdata_in_executor(
        self, user_data: Union[List[os.PathLike[str]], List[xr.Dataset]]
    ) -> None:
        for data in user_data:
            metadata = await self._get_metadata(data)
            if isinstance(metadata, Exception) or metadata == {}:
                logger.warning("Error getting metadata: %s", metadata)
            else:
                self.user_metadata.append(metadata)

    def _timedelta_to_cmor_frequency(self, dt: float) -> str:
        for total_seconds, frequency in self._time_table.items():
            if dt >= total_seconds:
                return frequency
        return "fx"  # pragma: no cover

    @property
    def _time_table(self) -> dict[int, str]:
        return {
            315360000: "dec",  # Decade
            31104000: "yr",  # Year
            2538000: "mon",  # Month
            1296000: "sem",  # Seasonal (half-year)
            84600: "day",  # Day
            21600: "6h",  # Six-hourly
            10800: "3h",  # Three-hourly
            3600: "hr",  # Hourly
            1: "subhr",  # Sub-hourly
        }

    def _get_time_frequency(self, time_delta: int, freq_attr: str = "") -> str:
        if freq_attr in self._time_table.values():
            return freq_attr
        return self._timedelta_to_cmor_frequency(time_delta)

    async def _get_metadata(
        self, path: Union[os.PathLike[str], xr.Dataset]
    ) -> Dict[str, Union[str, List[str], Dict[str, str]]]:
        loop = asyncio.get_running_loop()

        def open_dataset_with_lock() -> xr.Dataset:
            with self._lock:
                with xr.open_mfdataset(
                    str(path), parallel=False, use_cftime=True, lock=False
                ) as dset:
                    return dset

        try:
            if isinstance(path, xr.Dataset):
                dset = path
            if isinstance(path, Path):
                dset = await loop.run_in_executor(None, open_dataset_with_lock)
            time_freq = dset.attrs.get("frequency", "")
            data_vars = list(map(str, dset.data_vars))
            coords = list(map(str, dset.coords))
            try:
                times = dset["time"].values[:]
            except (KeyError, IndexError, TypeError):
                times = np.array([])

        except Exception as error:
            logger.warning("Failed to open data file %s: %s", str(path), error)
            return {}
        if len(times) > 0:
            try:
                time_str = f"[{times[0].isoformat()}Z TO {times[-1].isoformat()}Z]"
                dt = abs((times[1] - times[0]).total_seconds()) if len(times) > 1 else 0
            except Exception as non_cftime:
                logger.info("The time var is not based on the cftime: %s", non_cftime)
                time_str = (
                    f"[{np.datetime_as_string(times[0], unit='s')}Z TO "
                    f"{np.datetime_as_string(times[-1], unit='s')}Z]"
                )
                dt = (
                    abs((times[1] - times[0]).astype("timedelta64[s]").astype(int))
                    if len(times) > 1
                    else 0
                )
        else:
            time_str = "fx"
            dt = 0

        variables = [
            var
            for var in data_vars
            if var not in coords
            and not any(
                term in var.lower() for term in ["lon", "lat", "bnds", "x", "y"]
            )
            and var.lower() not in ["rotated_pole", "rot_pole"]
        ]
        # TODO: we need to find out why we are not getting more than one variable!
        if len(variables) != 1:
            logger.warning("Only one data variable allowed, found: %s", variables)

        _data: Dict[str, Union[str, List[str], Dict[str, str]]] = {}
        _data.setdefault("variable", variables[0])
        _data.setdefault("time_frequency", self._get_time_frequency(dt, time_freq))
        _data["time"] = time_str
        _data.setdefault("cmor_table", _data["time_frequency"])
        _data.setdefault("version", "")
        if isinstance(path, Path):
            _data["file"] = str(path)
        if isinstance(path, xr.Dataset):
            _data["file"] = str(dset.encoding["source"])
        return _data
