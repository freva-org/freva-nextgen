"""Definition of special types."""

from dataclasses import dataclass, fields
from typing import Dict, Literal, Optional, Union

from typing_extensions import TypeAlias

ZarrOptionsDict: TypeAlias = Dict[str, Optional[Union[str, int, float, bool]]]


@dataclass
class ZarrOptions:
    """Options controlling how the service builds a Zarr store.

    Every entry point that produces a Zarr URL accepts these as a plain
    dict via ``zarr_options``: :func:`freva_client.zarr_utils.convert`,
    :meth:`freva_client.databrowser.aggregate`, and
    :class:`freva_client.databrowser` with ``stream_zarr=True``. Only the
    keys you set need to be given; unknown keys are ignored.

    .. code-block:: python

        from freva_client import databrowser
        from freva_client.zarr_utils import convert

        urls = convert("/work/data/tas_day.nc",
                       zarr_options={"time_freq": "monthly"})

    The options fall into four groups.

    **URL and lifetime** -- ``public`` decides whether the URL carries its
    own signature and can be handed to someone else, and ``ttl_seconds``
    how long it stays valid.

    **Chunking** -- ``access_pattern``, ``chunk_size`` and
    ``map_primary_chunksize`` shape the chunk grid. They change how many
    requests a client makes and how large each one is, never the values.
    Pick ``"map"`` when readers want whole fields a step at a time, and
    ``"time_series"`` when they want long series at a few points.

    **Caching** -- stores are cached server side and keyed on the request,
    so the same paths and the same options resolve to the same URL. Set
    ``reload`` to force a rebuild.

    **Dimension reduction** -- ``time_freq``, ``time_method``,
    ``climatology``, ``min_coverage`` and ``dtype`` make the service
    collapse the time dimension before serving, so that only the reduced
    data crosses the wire. Reduction is off unless ``time_freq`` is set.

    Parameters
    ----------
    public: bool, default: False
        Generate a pre-signed URL that anyone can open, rather than one
        requiring the caller's token.
    ttl_seconds: float, default: 86400.0
        Lifetime of the generated URL, in seconds. One day by default.
    access_pattern: str, default: "map"
        ``"map"`` chunks along time, so one request returns one full
        field. ``"time_series"`` chunks along the geographical
        dimensions, so one request returns a long series at few points.
    chunk_size: float, default: 16.0
        Target size of a single chunk in megabytes.
    map_primary_chunksize: int, default: 1
        Number of steps along the primary (usually time) dimension per
        chunk, when ``access_pattern="map"``.
    reload: bool, default: False
        Bypass the server-side cache and rebuild the store.
    time_freq: str or None, default: None
        Target frequency for temporal reduction, one of ``"hourly"``,
        ``"3hourly"``, ``"6hourly"``, ``"daily"``, ``"monthly"``,
        ``"seasonal"``, ``"yearly"``, ``"decadal"``. ``None`` applies no
        reduction. Every other reduction option is ignored without it.
    time_method: str or None, default: None
        How each group is reduced: ``"mean"`` (the default applied
        server side), ``"sum"``, ``"min"``, ``"max"``, ``"std"``,
        ``"var"``, ``"median"`` or ``"count"``.
    climatology: bool, default: False
        Whether ``time_freq`` means a climatology instead of a
        resampling. See the example below -- the two are easy to confuse
        and give very different results.
    min_coverage: float, default: 0.0
        Fraction between 0 and 1. An output step whose group contained a
        smaller fraction of valid (non-missing) source steps is masked.
    dtype: str, default: "float32"
        Output precision of reduced variables: ``"float32"``,
        ``"float64"``, or ``"keep"`` to leave decoding's own result.

    Examples
    --------
    Serve monthly means of a daily dataset:

    .. code-block:: python

        convert("/work/data/tas_day.nc",
                zarr_options={"time_freq": "monthly"})

    ``time_freq`` on its own **resamples**: one value per calendar period
    in the record. Set ``climatology`` for the other reading of "monthly
    mean", one value per period of the year across the whole record. Ten
    years of daily data gives:

    .. code-block:: python

        # 120 steps, one per month of the record
        {"time_freq": "monthly"}

        # 12 steps, one per month of the year
        {"time_freq": "monthly", "climatology": True}

    Guard against groups built from too little data. A monthly mean
    computed from two valid days is usually worse than no value at all:

    .. code-block:: python

        {"time_freq": "monthly", "min_coverage": 0.8}

    Reduce with something other than a mean, and keep full precision:

    .. code-block:: python

        {"time_freq": "yearly", "time_method": "max", "dtype": "float64"}

    Combine reduction with chunking suited to point extraction:

    .. code-block:: python

        db = databrowser(dataset="cmip6-fs")
        url = db.aggregate("concat", dim="time", zarr_options={
            "time_freq": "monthly",
            "access_pattern": "time_series",
            "ttl_seconds": 3600,
        })

    Notes
    -----
    Reduced stores are always CF-decoded: packed integers are unpacked,
    ``scale_factor``/``add_offset`` are applied, ``_FillValue`` becomes
    NaN, and the result is floating point. Unreduced stores are served
    raw. The two therefore differ in dtype by design -- decoding is not
    optional for a reduction, since averaging packed integers that still
    carry an in-band fill value produces nonsense.

    ``dtype`` defaults to ``"float32"`` because decoding packed data
    promotes to ``float64`` whenever ``scale_factor`` is stored as a
    double, which is the common case. That quadruples the transferred
    bytes relative to the source for no gain in precision.

    Reduction only ever makes data coarser. Asking for a frequency finer
    than the source is rejected: it is not a reduction but an
    interpolation onto a mostly-empty axis, and would inflate the store
    by orders of magnitude.

    Time-invariant variables such as ``orog``, and CF grid-mapping
    containers such as ``rotated_pole``, are carried through a reduction
    unchanged rather than being averaged or broadcast onto the new time
    axis.
    """

    public: bool = False
    """Whether to generate a publicly accessible Zarr URL."""

    ttl_seconds: float = 86400.0
    """Time-to-live for the generated URL in seconds."""

    access_pattern: Literal["map", "time_series"] = "map"
    """Data access pattern for chunk size optimization.

    - ``"map"``: Optimizes for spatial access by chunking along the time dimension.
    - ``"time_series"``: Optimizes for temporal access by chunking along
      geographical dimensions.
    """

    chunk_size: float = 16.0
    """Target chunk size in megabytes."""

    map_primary_chunksize: int = 1
    """Chunk size for primary dimensions (e.g., time) when using
    ``"map"`` access pattern."""

    reload: bool = False
    """Force a server-side cache refresh.

    By default, data store requests are cached to improve performance.
    Set to ``True`` to bypass the cache and fetch fresh data.
    """

    time_freq: Optional[
        Literal[
            "hourly",
            "3hourly",
            "6hourly",
            "daily",
            "monthly",
            "seasonal",
            "yearly",
            "decadal",
        ]
    ] = None
    """Reduce the time dimension to this target frequency.

    Set this to have the service serve, for example, monthly means instead
    of the full daily data. ``None`` (default) applies no reduction.

    Reduced stores are always CF-decoded: packed integers are unpacked,
    ``_FillValue`` is honoured and the result is floating point.
    Unreduced stores are served raw, so the two differ in dtype by design.
    """

    time_method: Optional[
        Literal["mean", "sum", "min", "max", "std", "var", "median", "count"]
    ] = None
    """How to reduce each time group. Requires ``time_freq``.

    Defaults to ``mean`` server side.
    """

    climatology: bool = False
    """Whether ``time_freq`` means a climatology rather than a resampling.

    - ``False`` (default): resample, i.e. one value per calendar period in
      the record. ``monthly`` over ten years gives 120 steps.
    - ``True``: climatology, i.e. group across the whole record.
      ``monthly`` over ten years gives 12 steps.

    Only ``hourly``, ``daily``, ``monthly`` and ``seasonal`` are meaningful
    as climatologies.
    """

    min_coverage: float = 0.0
    """Minimum fraction of valid source steps required per output step.

    Output steps whose group contained fewer than this fraction of valid
    (non-missing) source steps are masked. A monthly mean built from two
    valid days is usually worse than no value at all. ``0`` (default) keeps
    every group with at least one valid point.
    """

    dtype: Literal["float32", "float64", "keep"] = "float32"
    """Output dtype of *reduced* variables. Ignored without ``time_freq``.

    CF-decoding packed data promotes to ``float64`` whenever
    ``scale_factor`` is stored as a double, which quadruples the transferred
    bytes for no gain in precision. Use ``keep`` to leave whatever decoding
    produced.
    """

    @classmethod
    def from_dict(
        cls,
        options: Optional[ZarrOptionsDict] = None,
    ) -> "ZarrOptions":
        """Create a ZarrOptions instance from a dictionary.

        Parameters
        ----------
        options: dict, default: None
            Dictionary of options. Unknown keys are ignored.
            If None or empty, returns instance with all defaults.

        Returns
        -------
        ZarrOptions
            Configured instance.
        """
        options = options or {}
        valid_keys = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in options.items() if k in valid_keys}
        return cls(**filtered)  # type: ignore[arg-type]
