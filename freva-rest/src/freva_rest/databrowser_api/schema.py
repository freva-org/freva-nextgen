"""Schema definitions for the FastAPI endpoints."""

from typing import Any, Dict, List, Literal, Optional, Union
from urllib.parse import parse_qs

from fastapi import Path, Query, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing_extensions import TypedDict

from freva_rest.rest import server_config

Required: Any = Ellipsis
FlavourType = Literal["freva", "cmip6", "cmip5", "cordex", "user"]


class LoadFiles(BaseModel):
    """Schema for the load file endpoint response."""

    urls: List[str]


class SolrSchema:
    """Class holding all apache solr config parameters."""

    params: Dict[str, Any] = {
        "batch_size": Query(
            alias="max-results",
            title="Max. results",
            description="Control the number of maximum result items returned.",
            ge=0,
            le=1500,
        ),
        "start": Query(
            alias="start",
            title="Start",
            description="Specify the starting point for receiving results.",
            ge=0,
        ),
        "multi_version": Query(
            alias="multi-version",
            title="Multi Version",
            description="Use versioned datasets instead of latest versions.",
        ),
        "translate": Query(
            title="Translate",
            alias="translate",
            description="Translate the output to the required DRS flavour.",
        ),
        "facets": Query(
            title="Facets",
            alias="facets",
            description=(
                "The facets that should be part of the output, "
                "by default all facets will be returned."
            ),
        ),
        "max_results": Query(
            title="Max. Results",
            alias="max-results",
            description=(
                "Raise an Error if more results are found than that"
                "number, -1 for all results."
            ),
        ),
    }

    path_params: Dict[str, Any] = {
        "flavour": Path(
            ...,
            description=(
                "DRS flavour: built-in (freva, cmip6, cmip5, cordex, user)"
                " or custom flavours."
                " For custom flavours, use '`flavour_name`' or "
                "'`username:flavour_name`' format when conflicts exist"
                " with the global flavours."
            ),
            examples=[
                "freva",
                "nextgem",
                "flavour_name",
                "user_name:flavour_name",
            ],
        ),
        "uniq_key": Path(..., description="Core type"),
    }

    @classmethod
    def process_parameters(
        cls, request: Request, *parameters_not_to_process: str
    ) -> Dict[str, list[str]]:
        """Convert Starlette Request QueryParams to a dictionary."""

        query = parse_qs(str(request.query_params))
        for key in ("uniq_key", "flavour") + parameters_not_to_process:
            _ = query.pop(key, [""])
        for key, param in cls.params.items():
            _ = query.pop(key, [""])
            _ = query.pop(param.alias, [""])
        return query


class FacetResults(BaseModel):
    """Schema for facets results."""

    facets: Dict[str, List[Union[str, int]]]


class SearchFlavours(BaseModel):
    """Schema for search flavours."""

    flavours: List[Union[FlavourType, str]]
    attributes: Dict[Union[FlavourType, str], List[str]]


class FlavourDefinition(BaseModel):
    """Schema for flavour definition."""

    flavour_name: str = Field(
        ...,
        description="Name of the custom flavour",
        examples=["nextgem", "custom_project"],
    )
    mapping: Dict[str, str] = Field(
        ...,
        description="Facet mapping dictionary",
        examples=[
            {
                "project": "mip_era",
                "model": "source_id",
                "experiment": "experiment_id",
                "variable": "variable_id",
            }
        ],
    )
    is_global: bool = Field(
        False,
        description="Make flavour available to all users",
        examples=[False, True],
    )
    model_config = ConfigDict(extra="forbid")

    @field_validator("flavour_name")
    @classmethod
    def validate_flavour_name(cls, v: str) -> str:
        import re

        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError(
                "Flavour name can only contain letters, "
                "numbers, underscores, and hyphens"
            )
        return v

    @field_validator("mapping")
    @classmethod
    def validate_mapping_keys(cls, v: Dict[str, str]) -> Dict[str, str]:
        """Validate that mapping keys are valid freva facets."""
        valid_facets = set(server_config.solr_fields)
        valid_facets.update({"time", "bbox", "user"})
        invalid_keys = set(v.keys()) - valid_facets
        if invalid_keys:  # pragma: no cover
            raise ValueError(
                f"Invalid mapping keys: {sorted(invalid_keys)}. "
                f"Valid freva facets are: {sorted(valid_facets)}"
            )
        return v


class FlavourUpdateDefinition(BaseModel):
    """Schema for partial flavour definition updates.
    Only `mapping` is required, `flavour_name` and `is_global`
    are optional.
    """

    flavour_name: Optional[str] = Field(
        None,
        description="Name of the flavour (must don't exist for new flavours)",
        examples=["nextgem", "custom_project"],
    )
    mapping: Dict[str, str] = Field(
        ...,
        description="Partial facet mapping dictionary to merge with existing mapping",
        examples=[{"model": "updated_model", "experiment": "updated_experiment"}],
    )
    is_global: bool = Field(False, description="Whether this is a global flavour")
    model_config = ConfigDict(extra="forbid")

    @field_validator("flavour_name")
    @classmethod
    def validate_flavour_name(cls, v: str) -> str:
        import re

        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError(
                "Flavour name can only contain letters, "
                "numbers, underscores, and hyphens"
            )
        return v


class FlavourResponse(BaseModel):
    """Response schema for flavour queries."""

    flavour_name: str = Field(examples=["nextgem", "cordex"])
    mapping: Dict[str, str] = Field(
        examples=[{"project": "mip_era", "model": "source_id"}]
    )
    owner: str = Field(examples=["global", "john_doe"])
    who_created: str = Field(examples=["john_doe", "admin"])
    ctime: str = Field(examples=["2024-01-15T10:30:00"])
    mtime: str = Field(examples=["2024-02-20T14:45:00"])


class FlavourListResponse(BaseModel):
    total: int = Field(examples=[1])
    flavours: List[FlavourResponse]


class FlavourDeleteResponse(BaseModel):
    """Response schema for flavour deletion."""

    status: str = Field(
        examples=[
            "Personal flavour 'nextgem' deleted successfully",
            "Global flavour 'custom_project' deleted successfully",
        ]
    )


class AddUserDataRequestBody(BaseModel):
    """Request body schema for adding user data."""

    user_metadata: List[Dict[str, str]] = Field(
        ...,
        description="List of user metadata objects or strings to be"
        " added to the databrowser.",
        examples=[
            [
                {
                    "variable": "tas",
                    "time_frequency": "mon",
                    "time": "[1979-01-16T12:00:00Z TO 1979-11-16T00:00:00Z]",
                    "file": "path of the file",
                },
            ]
        ],
    )
    facets: Dict[str, Any] = Field(
        ...,
        description="Key-value pairs representing metadata search attributes.",
        examples=[
            {"project": "user-data", "product": "new", "institute": "globe"}
        ],
    )


IntakeType = TypedDict(
    "IntakeType",
    {
        "esmcat_version": str,
        "attributes": List[Dict[str, str]],
        "assets": Dict[str, str],
        "id": str,
        "description": str,
        "title": str,
        "last_updated": str,
        "aggregation_control": Dict[str, Any],
    },
)


class SearchResult(BaseModel):
    """Return Model of a uniq key search."""

    total_count: int
    facets: Dict[str, List[Union[str, int]]]
    search_results: List[Dict[str, Union[str, float, List[str]]]]
    facet_mapping: Dict[str, str]
    primary_facets: List[str]


class IntakeCatalogue(BaseModel):
    """Return Model of a uniq key search."""

    catalogue: IntakeType
    total_count: int
