"""Definition of special types."""

from typing_extensions import TypedDict


class ZarrOptionsDict(TypedDict):
    """This dict repr holds information on how to handle zarr url requests."""

    public: bool
    """Flag indicating whether or not we need to create a public zarr ulr."""

    ttl_seconds: int
    """TTL of the link in seconds."""
