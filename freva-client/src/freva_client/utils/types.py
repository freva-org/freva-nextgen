"""Definition of special types."""

from dataclasses import dataclass, fields
from typing import Dict, Literal, Optional, Union

from typing_extensions import TypeAlias

ZarrOptionsDict: TypeAlias = Dict[str, Optional[Union[str, int, float, bool]]]


@dataclass
class ZarrOptions:
    """Configuration options for Zarr URL requests.

    Controls URL generation, caching behavior, and chunk size optimization
    for different data access patterns.
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
