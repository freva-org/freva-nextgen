"""Shared construction of STAC assets.

Every item carries five assets:
freva-databrowser
freva-data-viewer
access-data
intake-catalogue
stac-catalogue

Note: Collections deliberately carry far less; because a collection can hold
millions of items, so a one-click "download the whole catalogue" asset is
a trap rather than a feature.
"""

from textwrap import dedent, indent
from typing import Dict, Literal, Mapping, Optional, Sequence
from urllib.parse import quote

from .stac_utils import Asset, split_protocol

StorageKind = Literal["local", "remote"]

LOCAL_FS_TYPES = frozenset({"posix", "local", "nfs", "lustre"})
LOCAL_PROTOCOLS = frozenset({"file", "local"})

# eliminate the irrelevant keys from search scopes
NON_SEARCH_KEYS = frozenset({"translate", "start", "multi_version"})


class AssetContext:
    """Everything the asset builders need to know about the request.

    Parameters
    ----------
    base_url:
        Public base URL of the Freva deployment, without a trailing slash.
    flavour:
        The active databrowser flavour, used in the API routes.
    uniq_key:
        Either ``file`` or ``uri``.
    params:
        The search facets that scoped this request. Used to rebuild the
        equivalent query for the databrowser, the CLI and the client.
    """

    def __init__(
        self,
        base_url: str,
        *,
        flavour: str = "freva",
        uniq_key: Literal["file", "uri"] = "file",
        params: Optional[Mapping[str, object]] = None,
    ) -> None:
        self.base_url = str(base_url).rstrip("/")
        self.flavour = flavour
        self.uniq_key = uniq_key
        self.params = {
            k: v
            for k, v in (params or {}).items()
            if k not in NON_SEARCH_KEYS
        }

    # URL / snippet fragments
    @property
    def api_params(self) -> str:
        """The search facets as a URL query fragment, no leading ``&``."""
        return "&".join(f"{k}={quote(str(v), safe='')}" for k, v in self.params.items())

    @property
    def cli_params(self) -> str:
        """The search facets as ``freva-client`` CLI arguments."""
        return " ".join(f"{k}={v}" for k, v in self.params.items())

    @property
    def python_params(self) -> str:
        """The search facets as ``databrowser()`` keyword arguments."""
        return "".join(f"{k}='{v}', " for k, v in self.params.items())

    def _query(self, *extra: str) -> str:
        """Join the scoping facets with any additional query fragments."""
        return "&".join(part for part in (self.api_params, *extra) if part)

    def item_query(self, item_id: str) -> str:
        """Query fragment that selects exactly one file."""
        return self._query(f"{self.uniq_key}={quote(item_id, safe='')}")

    # endpoints build
    def databrowser_url(self, item_id: Optional[str] = None) -> str:
        query = self.item_query(item_id) if item_id is not None else self.api_params
        return f"{self.base_url}/databrowser/?{query}"

    def viewer_url(self, item_id: str) -> str:
        return (
            f"{self.base_url}/inspect/?"
            f"{self.uniq_key}={quote(item_id, safe='')}"
        )

    def zarr_url(self, item_id: Optional[str] = None) -> str:
        query = self.item_query(item_id) if item_id is not None else self.api_params
        return (
            f"{self.base_url}/api/freva-nextgen/databrowser/load/"
            f"{self.flavour}?{query}"
        )

    def intake_url(self, item_id: Optional[str] = None) -> str:
        query = self.item_query(item_id) if item_id is not None else self.api_params
        return (
            f"{self.base_url}/api/freva-nextgen/databrowser/intake-catalogue/"
            f"{self.flavour}/{self.uniq_key}?{query}"
        )

    def stac_url(self, item_id: Optional[str] = None) -> str:
        query = self.item_query(item_id) if item_id is not None else self.api_params
        return (
            f"{self.base_url}/api/freva-nextgen/databrowser/stac-catalogue/"
            f"{self.flavour}/{self.uniq_key}?{query}"
        )


# Classification helpers
def classify_storage(
    file_id: str, fs_type: Optional[str] = None
) -> StorageKind:
    """
    Decide whether a file is reachable from outside the HPC.
    """
    if fs_type:
        return "local" if fs_type.strip().lower() in LOCAL_FS_TYPES else "remote"
    protocol, _ = split_protocol(file_id)
    if protocol is None or protocol.lower() in LOCAL_PROTOCOLS:
        return "local"
    return "remote"


def guess_media_type(file_id: str) -> str:
    """Best-effort media type for a data file."""
    path = file_id.rstrip("/").lower()
    if path.endswith(".zarr"):
        return "application/vnd+zarr"
    if path.endswith((".nc", ".nc4", ".cdf", ".netcdf")):
        return "application/netcdf"
    if path.endswith((".grb", ".grib", ".grb2", ".grib2")):
        return "application/wmo-GRIB2"
    if path.endswith((".h5", ".hdf5", ".hdf")):
        return "application/x-hdf5"
    if path.endswith((".tif", ".tiff")):
        return "image/tiff; application=geotiff"
    return "application/octet-stream"


def guess_engine(file_id: str) -> str:
    """The ``xarray`` engine that most likely opens this file."""
    path = file_id.rstrip("/").lower()
    if path.endswith(".zarr"):
        return "zarr"
    if path.endswith((".grb", ".grib", ".grb2", ".grib2")):
        return "cfgrib"
    return "h5netcdf"


# Descriptions
_DOCS = "https://freva-org.github.io/freva-nextgen/"


def streamed_access_desc(ctx: AssetContext, item_id: str) -> str:
    """Access instructions for data held on a local (POSIX) filesystem."""
    return dedent(
        f"""
        # Access this dataset

        This file lives on the compute/data centre's own filesystem, so the path
        below is **not** something you can open from outside. Freva instead
        streams the dataset to you as **Zarr over HTTP**, authenticated with
        your Freva credentials. Pick whichever of the following fits your
        workflow - they all do the same thing.

        💡 **Just want a look?** The **Freva Data Viewer** asset on this item
        opens this very same Zarr stream in your browser; it is the web
        front-end of the streaming service described below, pointing at the
        same endpoint. Your web session stands in for the access token, so
        there is nothing to install and no token to create. Use the options
        below when you want the data inside your own code.

        ## 1. Install the client

        ```bash
        pip install freva-client
        # or
        conda install -c conda-forge freva-client
        ```

        ## 2. Python (recommended)

        Note: `authenticate()` opens a browser for the login flow and caches the
        result, so it is only interactive the first time. It returns the
        request headers ready to hand to `xarray`.

        ```python
        import xarray as xr
        from freva_client import authenticate, databrowser

        storage_options = authenticate(host="{ctx.base_url}")["headers"]

        db = databrowser(
            {ctx.python_params}{ctx.uniq_key}="{item_id}",
            stream_zarr=True,
            host="{ctx.base_url}",
        )
        dset = xr.open_mfdataset(
            list(db),
            chunks="auto",
            engine="zarr",
            storage_options=storage_options,
        )
        ```

        ## 3. Command line

        Create a token once and keep it in a file, then point the search at
        it. `--token-file` is what makes this work unattended, in a batch
        job or a cron entry:

        ```bash
        freva-client auth --host {ctx.base_url} > ~/.freva-token.json
        chmod 600 ~/.freva-token.json

        freva-client databrowser data-search \\
            {ctx.cli_params} {ctx.uniq_key}={item_id} \\
            --zarr --host {ctx.base_url} \\
            --token-file ~/.freva-token.json
        ```

        No browser on that machine? Log in to the
        [Freva web portal]({ctx.base_url}) and download a token file from
        there instead, then copy it across; `--token-file` does not care
        how the file was produced.

        ## 4. Plain HTTP (language agnostic)

        ```bash
        ACCESS_TOKEN=$(jq -r .access_token ~/.freva-token.json)
        curl -H "Authorization: Bearer $ACCESS_TOKEN" \\
            "{ctx.zarr_url(item_id)}"
        ```

        ---
        ⚠️ Treat the token file like a password: `chmod 600`, never commit
        it, and refresh it rather than re-using an expired one.
        💡 Three ways to get one, all interchangeable: `authenticate()` in
        Python, `freva-client auth` on the command line, or a download from
        the [Freva web portal]({ctx.base_url}). On a machine that cannot
        open a browser, pass an existing refresh token with
        `authenticate(token_file="~/.freva-token.json")` or
        `freva-client auth --token-file ...`.
        💡 Zarr streams expire if they are not read in time. See the
        [freva-client documentation]({_DOCS}).
        """
    ).strip()


def remote_access_desc(item_id: str) -> str:
    """Access instructions for data held on remote storage."""
    engine = guess_engine(item_id)
    if engine == "zarr":
        open_call = dedent(
            f"""
            store = fsspec.get_mapper("{item_id}", anon=True)
            dset = xr.open_dataset(store, engine="zarr")
            """
        ).strip()
        extras = "zarr"
    else:
        open_call = dedent(
            f"""
            with fsspec.open("{item_id}", anon=True) as fobj:
                dset = xr.open_dataset(fobj, engine="{engine}")
            """
        ).strip()
        extras = "h5netcdf" if engine == "h5netcdf" else engine

    open_call = indent(open_call, " " * 8).lstrip()

    return dedent(
        f"""
        # Access this dataset

        This dataset is hosted on remote storage and can be opened directly
        with `xarray` no Freva login required, and no need to copy it
        anywhere first.

        ## 1. Install the dependencies

        ```bash
        pip install xarray fsspec {extras}
        ```

        ## 2. Open it in Python

        ```python
        import fsspec
        import xarray as xr

        {open_call}
        ```

        ---
        ⚠️ **Access to this dataset may be restricted.** Freva indexes both
        public and non-public collections and cannot always tell them apart,
        so this link is not a guarantee of access. If the call above fails
        with an authentication or permission error (HTTP 401 / 403), drop
        `anon=True` and pass your own credentials for that storage backend.
        If you do not have credentials, please contact the **data provider**
        or your **project coordinator** to request access.
        """
    ).strip()


def intake_desc(url: str) -> str:
    return dedent(
        f"""
        # Intake-ESM catalogue

        ## 1. Install intake-esm

        ```bash
        pip install intake-esm
        # or
        conda install -c conda-forge intake-esm
        ```

        ## 2. Open the catalogue

        ```python
        import intake

        cat = intake.open_esm_datastore("{url}")
        dsets = cat.to_dataset_dict()
        ```

        ---
        💡 The catalogue is generated on request, so it always reflects the
        current state of the index.
        """
    ).strip()


def stac_download_desc(url: str) -> str:
    return dedent(
        f"""
        # Static STAC catalogue (ZIP)

        Downloads this search as a self-contained, offline STAC catalogue.
        The archive is streamed and generated on the fly, so nothing is
        staged on the server.

        ## 1. Download and unpack

        ```bash
        curl -L -o stac-catalog.zip "{url}"
        unzip stac-catalog.zip -d stac-catalog
        ```

        ## 2. Open it with pystac

        ```bash
        pip install pystac
        ```

        ```python
        import pystac

        catalog = pystac.Catalog.from_file("stac-catalog/stac-catalog/catalog.json")
        print(catalog.describe())
        ```

        ---
        💡 The catalogue is fully self-contained: point a static web server
        at the unpacked directory to serve it, or browse it locally.
        """
    ).strip()


VIEWER_DESC = (
    "Open this dataset in the Freva data viewer for a quick look at its "
    "variables, dimensions, attributes and a preview plot, straight in "
    "the browser, no download and no local environment needed. This is the "
    "web front-end of the same Zarr streaming service the `access-data` "
    "asset describes: it opens the identical stream, with your web session "
    "standing in for the access token."
)

DATABROWSER_ITEM_DESC = (
    "Open this dataset in the Freva web databrowser to inspect its "
    "metadata and explore neighbouring datasets."
)


def databrowser_collection_desc(facet: str, value: str) -> str:
    return (
        "Browse this collection in the Freva web databrowser, pre-filtered "
        f"to `{facet} = {value}`. From there you can narrow the search "
        "further and export the result as an intake or STAC catalogue of a "
        "size that suits you."
    )


def collection_streamed_access_desc(ctx: AssetContext) -> str:
    """Open*everything in this collection as one dataset."""
    return dedent(
        f"""
        # Access the whole collection

        Rather than opening the files one by one, you can have Freva stream
        the entire search to you as a single Zarr-backed `xarray` dataset.
        Files held on the compute/data centre's own filesystem are served over
        HTTP, so this works from anywhere you can log in to Freva.

        💡 **Just want a look at one dataset?** The **Freva Data Viewer**
        asset on any item opens that item's Zarr stream in your browser - it
        is the web front-end of this same streaming service. Your web session
        stands in for the access token, so there is nothing to install.

        ## 1. Install the client

        ```bash
        pip install freva-client
        # or
        conda install -c conda-forge freva-client
        ```

        ## 2. Python (recommended)

        `authenticate()` opens a browser for the login flow and caches the
        result, so it is only interactive the first time. It returns the
        request headers ready to hand to `xarray`.

        ```python
        import xarray as xr
        from freva_client import authenticate, databrowser

        storage_options = authenticate(host="{ctx.base_url}")["headers"]

        db = databrowser(
            {ctx.python_params}stream_zarr=True,
            host="{ctx.base_url}",
        )
        dset = xr.open_mfdataset(
            list(db),
            chunks="auto",
            engine="zarr",
            storage_options=storage_options,
        )
        ```

        ## 3. Command line

        Create a token once and keep it in a file, then point the search at
        it. `--token-file` is what makes this work unattended, in a batch
        job or a cron entry:

        ```bash
        freva-client auth --host {ctx.base_url} > ~/.freva-token.json
        chmod 600 ~/.freva-token.json

        freva-client databrowser data-search {ctx.cli_params} \\
            --zarr --host {ctx.base_url} \\
            --token-file ~/.freva-token.json
        ```

        No browser on that machine? Log in to the
        [Freva web portal]({ctx.base_url}) and download a token file from
        there instead, then copy it across - `--token-file` does not care
        how the file was produced.

        ---
        ⚠️ This opens **every** file in the collection at once. For a large
        collection, narrow the search down first - the databrowser asset is
        a good starting point.
        ⚠️ Treat the token file like a password: `chmod 600`, never commit
        it, and refresh it rather than re-using an expired one.
        💡 Three ways to get one, all interchangeable: `authenticate()` in
        Python, `freva-client auth` on the command line, or a download from
        the [Freva web portal]({ctx.base_url}). On a machine that cannot
        open a browser, pass an existing refresh token with
        `authenticate(token_file="~/.freva-token.json")` or
        `freva-client auth --token-file ...`.
        💡 Zarr streams expire if they are not read in time - see the
        [freva-client documentation]({_DOCS}).
        """
    ).strip()


def collection_intake_desc(url: str) -> str:
    """The same search, in intake-esm form."""
    return dedent(
        f"""
        # The same collection as an Intake-ESM catalogue

        If you would rather work with `intake-esm` than with STAC, this is
        the identical search expressed as an ESM datastore - handy for
        aggregating the collection into a handful of datasets by facet.

        ## 1. Install intake-esm

        ```bash
        pip install intake-esm
        # or
        conda install -c conda-forge intake-esm
        ```

        ## 2. Open the catalogue

        ```python
        import intake

        cat = intake.open_esm_datastore("{url}")
        print(cat.df.head())
        dsets = cat.to_dataset_dict()
        ```

        ---
        ⚠️ The catalogue is built on request and covers the whole
        collection, so it can be large. Narrow the search first if you only
        need part of it.
        """
    ).strip()


def static_archive_desc(url: str) -> str:
    """How to use the archive the user is already holding.

    This sits on the *static* catalogue's collection, which by definition
    is only ever read from inside a ZIP the user has already downloaded.
    So it is a "how do I open this thing" note first, and a link to
    regenerate a fresh copy second.
    """
    return dedent(
        f"""
        # Using this catalogue

        You are reading this from inside a static STAC catalogue that has
        already been downloaded, so there is nothing further to fetch. The
        archive is self-contained: `catalog.json` at the top level, one
        `collection.json` per collection, and one JSON file per item.

        ## Open it with pystac

        ```bash
        pip install pystac
        ```

        ```python
        import pystac

        catalog = pystac.Catalog.from_file("stac-catalog/catalog.json")
        print(catalog.describe())

        for item in catalog.get_items(recursive=True):
            print(item.id, item.assets["access-data"].href)
        ```

        ## Or serve it

        Point any static web server at the unpacked directory and it
        becomes a browsable STAC catalogue:

        ```bash
        python -m http.server --directory stac-catalog 8000
        ```

        ---
        💡 The archive is a snapshot. To pick up newly indexed data, fetch a
        fresh copy from [the same search]({url}) - the catalogue is streamed
        and generated on the fly, so nothing is ever stale on the server.
        """
    ).strip()


# Builders
def build_item_assets(
    ctx: AssetContext,
    item_id: str,
    *,
    fs_type: Optional[str] = None,
    include: Optional[Sequence[str]] = None,
) -> Dict[str, Asset]:
    """Build the five standard assets for a single STAC item.

    Parameters
    ----------
    ctx:
        Request context, see :class:`AssetContext`.
    item_id:
        The ``file`` or ``uri`` value of this item.
    fs_type:
        The backend's ``fs_type`` for this document, when known.
    include:
        Restrict the result to these asset keys. Used by the static
        catalogue, which drops ``stac-catalogue`` because the item is
        already *inside* a downloaded STAC catalogue.
    """
    storage = classify_storage(item_id, fs_type)

    if storage == "local":
        access = Asset(
            href=ctx.zarr_url(item_id),
            title="Access data (Zarr stream)",
            description=streamed_access_desc(ctx, item_id),
            roles=["data"],
            media_type="application/vnd+zarr",
            extra_fields={
                "requires": ["oauth2"],
                "authentication": {
                    "type": "oauth2",
                    "description": (
                        "Authentication with your Freva credentials is required."
                    ),
                },
                "freva:storage": "local",
            },
        )
    else:
        access = Asset(
            href=item_id,
            title="Access data",
            description=remote_access_desc(item_id),
            roles=["data"],
            media_type=guess_media_type(item_id),
            extra_fields={"freva:storage": "remote"},
        )

    assets: Dict[str, Asset] = {
        "freva-databrowser": Asset(
            href=ctx.databrowser_url(item_id),
            title="Freva Web DataBrowser",
            description=DATABROWSER_ITEM_DESC,
            roles=["overview"],
            media_type="text/html",
        ),
        "freva-data-viewer": Asset(
            href=ctx.viewer_url(item_id),
            title="Freva Data Viewer",
            description=VIEWER_DESC,
            roles=["overview", "visual"],
            media_type="text/html",
        ),
        "access-data": access,
        "intake-catalogue": Asset(
            href=ctx.intake_url(item_id),
            title="Intake-ESM Catalogue",
            description=intake_desc(ctx.intake_url(item_id)),
            roles=["metadata"],
            media_type="application/json",
        ),
        "stac-catalogue": Asset(
            href=ctx.stac_url(item_id),
            title="Static STAC Catalogue (ZIP)",
            description=stac_download_desc(ctx.stac_url(item_id)),
            roles=["metadata"],
            media_type="application/zip",
        ),
    }
    if include is not None:
        assets = {k: v for k, v in assets.items() if k in include}
    return assets


STATIC_COLLECTION_ASSETS = (
    "freva-databrowser",
    "access-data",
    "intake-catalogue",
    "stac-catalogue",
)

API_COLLECTION_ASSETS = ("freva-databrowser",)


def build_collection_assets(
    ctx: AssetContext,
    *,
    facet: Optional[str] = None,
    value: Optional[str] = None,
    include: Sequence[str] = API_COLLECTION_ASSETS,
) -> Dict[str, Asset]:
    """Build the assets for a STAC collection.

    The two callers want genuinely different things here, which is why
    ``include`` exists rather than a single fixed set.

    Parameters
    ----------
    facet, value:
        The facet/value pair that defines the collection, when there is
        one.
    include:
        Which asset keys to emit, see the two module constants above.
    """
    if facet and value:
        databrowser_href = (
            f"{ctx.base_url}/databrowser/?{facet}={quote(value, safe='')}"
        )
        databrowser_description = databrowser_collection_desc(facet, value)
    else:
        databrowser_href = ctx.databrowser_url()
        databrowser_description = (
            "Browse this collection in the Freva web databrowser, "
            "pre-filtered to the search that produced it. From there you "
            "can refine the search and regenerate this catalogue for a "
            "narrower or broader selection."
        )

    assets: Dict[str, Asset] = {
        "freva-databrowser": Asset(
            href=databrowser_href,
            title="Freva Web DataBrowser",
            description=databrowser_description,
            roles=["overview"],
            media_type="text/html",
        ),
        "access-data": Asset(
            href=ctx.zarr_url(),
            title="Access the whole collection (Zarr stream)",
            description=collection_streamed_access_desc(ctx),
            roles=["data"],
            media_type="application/vnd+zarr",
            extra_fields={
                "requires": ["oauth2"],
                "authentication": {
                    "type": "oauth2",
                    "description": (
                        "Authentication with your Freva credentials is required."
                    ),
                },
            },
        ),
        "intake-catalogue": Asset(
            href=ctx.intake_url(),
            title="Intake-ESM Catalogue",
            description=collection_intake_desc(ctx.intake_url()),
            roles=["metadata"],
            media_type="application/json",
        ),
        "stac-catalogue": Asset(
            href=ctx.stac_url(),
            title="This STAC Catalogue",
            description=static_archive_desc(ctx.stac_url()),
            roles=["metadata"],
            media_type="application/zip",
        ),
    }
    return {key: assets[key] for key in include if key in assets}
