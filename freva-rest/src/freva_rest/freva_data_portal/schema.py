"""Schema definitions for the data loader."""

from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, Field

from freva_rest.auth.presign import MAX_TTL_SECONDS, MIN_TTL_SECONDS


class ZarrConversion(BaseModel):
    """Payload for converting zarr datasets."""

    path: Annotated[
        Union[str, List[str]],
        Field(
            title="Path to data.",
            description=(
                "Absolute or object‑store path(s) to the data files to "
                "convert. You can add multiple files if you wish to aggregate"
                " data."
            ),
            examples=["/work/abc1234/myuser/my-data.nc"],
        ),
    ]
    aggregate: Annotated[
        Optional[Literal["auto", "merge", "concat"]],
        Field(
            title="Aggregte Data",
            description=(
                "If data needs to be aggregated, "
                "instruct which aggregation method should be used  "
                "(auto, merge or concat). If set to auto the system will "
                "try to infer a plan."
            ),
            examples=["concat"],
        ),
    ] = None
    join: Annotated[
        Optional[Literal["outer", "inner", "exact", "left", "right"]],
        Field(
            title="Join Mode",
            description=(
                "How to align coordinate indexes across inputs:\n "
                "  - outer: use the union of object indexes\n"
                "  - inner: use the intersection of object indexes\n"
                "  - left: use indexes from the first object with each dimension\n"
                "  - right: use indexes from the last object with each dimension\n"
                "  - exact: errors when indexes to be aligned are not equal."
            ),
            examples=["inner"],
        ),
    ] = None
    compat: Annotated[
        Optional[Literal["no_conflicts", "equals", "override"]],
        Field(
            title="Compat Mode",
            description=(
                "How to treat variables with same name:\n"
                "  - equals: all values and dimensions must be the same.\n"
                "  - no_conflicts: only values which are not null in both datasets\n"
                "must be equal. The returned dataset then contains the "
                "combination of all non-null values.\n"
                "   - override: skip comparing and pick variable from first "
                "dataset"
            ),
            examples=["no_conflicts"],
        ),
    ] = None
    data_vars: Annotated[
        Optional[Literal["minimal", "different", "all"]],
        Field(
            title="Variable concat.",
            description=(
                "These data variables will be combined together:\n"
                "  - minimal: Only data variables in which the dimension "
                "already appears are included.\n"
                "  - different: Data variables which are not equal (ignoring "
                "attributes) across all datasets are also concatenated "
                "(as well as all for which dimension already appears).\n"
                "  - all: All data variables will be concatenated."
            ),
            examples=["minimal"],
        ),
    ] = None
    coords: Annotated[
        Optional[Literal["minimal", "different", "all"]],
        Field(
            title="Coordinate concat.",
            description=(
                "These data coordinates will be combined together:\n"
                "  - minimal: Only data variables in which the dimension "
                "already appears are included.\n"
                "  - different: Data variables which are not equal (ignoring "
                "attributes) across all datasets are also concatenated "
                "(as well as all for which dimension already appears).\n"
                "  - all: All data variables will be concatenated."
            ),
            examples=["minimal"],
        ),
    ] = None
    dim: Annotated[
        Optional[str],
        Field(
            title="Dim",
            description=(
                "Name of the dimension to concatenate along. This can either "
                "be a new dimension name, in which case it is added along "
                "``axis=0``, or an existing dimension name, in which case "
                "the location of the dimension is unchanged."
            ),
            examples=["time"],
        ),
    ] = None
    group_by: Annotated[
        Optional[str],
        Field(
            title="Group by",
            description=(
                "If set, forces grouping by a signature key. "
                "Otherwise grouping is attempted only when direct combine "
                "fails."
            ),
            examples=["ensemble"],
        ),
    ] = None
    public: Annotated[
        bool,
        Field(
            title="Public Zarr.",
            description="Create a pre-signed zarr url that is public",
        ),
    ] = False
    ttl_seconds: Annotated[
        int,
        Field(
            title="TTL of Public Zarr.",
            description="Time in seconds the public zarr url is valid for.",
            ge=MIN_TTL_SECONDS,
            le=MAX_TTL_SECONDS,
        ),
    ] = 86400
