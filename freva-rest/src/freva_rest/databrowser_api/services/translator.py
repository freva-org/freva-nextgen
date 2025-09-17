"""The core functionality to interact with the apache solr search system."""

from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    cast,
)

from fastapi import HTTPException, Request

from freva_rest.config import ServerConfig
from freva_rest.logger import logger

from ..schema import (
    FlavourDefinition,
    FlavourDeleteResponse,
    FlavourResponse,
    FlavourType,
)

BUILTIN_FLAVOURS = ["freva", "cmip6", "cmip5", "cordex", "user"]


@dataclass
class Translator:
    """Class that defines the flavour translation.

    Parameters
    ----------
    flavour: str
        The target flavour, the facet names should be translated to.
    translate: bool, default: True
        Translate the search keys. Not translating (default: True) can be
        useful if the actual translation of the facets should be done on the
        client side.

    Attributes
    ----------
    """

    flavour: str
    translate: bool = True
    config: Optional['ServerConfig'] = None
    flavours: tuple[FlavourType, ...] = (
        "freva",
        "cmip6",
        "cmip5",
        "cordex",
        "user",
    )

    @property
    def facet_hierarchy(self) -> list[str]:
        """Define the hierarchy of facets that define a dataset."""
        return [
            "project",
            "product",
            "institute",
            "model",
            "experiment",
            "time_frequency",
            "realm",
            "variable",
            "ensemble",
            "cmor_table",
            "fs_type",
            "grid_label",
            "grid_id",
            "format",
        ]

    @property
    def _freva_facets(self) -> Dict[str, str]:
        """Define the freva search facets and their relevance"""
        return {
            "project": "primary",
            "product": "primary",
            "institute": "primary",
            "model": "primary",
            "experiment": "primary",
            "time_frequency": "primary",
            "realm": "primary",
            "variable": "primary",
            "ensemble": "primary",
            "time_aggregation": "primary",
            "fs_type": "secondary",
            "grid_label": "secondary",
            "cmor_table": "secondary",
            "driving_model": "secondary",
            "format": "secondary",
            "grid_id": "secondary",
            "level_type": "secondary",
            "rcm_name": "secondary",
            "rcm_version": "secondary",
            "dataset": "secondary",
            "time": "secondary",
            "bbox": "secondary",
            "user": "secondary",
            "version": "secondary",
        }

    @property
    def _cmip5_lookup(self) -> Dict[str, str]:
        """Define the search facets for the cmip5 standard."""
        return {
            "experiment": "experiment",
            "ensemble": "member_id",
            "fs_type": "fs_type",
            "grid_label": "grid_label",
            "institute": "institution_id",
            "model": "model_id",
            "project": "project",
            "product": "product",
            "realm": "realm",
            "variable": "variable",
            "time": "time",
            "bbox": "bbox",
            "time_aggregation": "time_aggregation",
            "time_frequency": "time_frequency",
            "cmor_table": "cmor_table",
            "dataset": "dataset",
            "driving_model": "driving_model",
            "format": "format",
            "grid_id": "grid_id",
            "level_type": "level_type",
            "rcm_name": "rcm_name",
            "rcm_version": "rcm_version",
        }

    @property
    def _cmip6_lookup(self) -> Dict[str, str]:
        """Define the search facets for the cmip6 standard."""
        return {
            "experiment": "experiment_id",
            "ensemble": "member_id",
            "fs_type": "fs_type",
            "grid_label": "grid_label",
            "institute": "institution_id",
            "model": "source_id",
            "project": "mip_era",
            "product": "activity_id",
            "realm": "realm",
            "variable": "variable_id",
            "time": "time",
            "bbox": "bbox",
            "time_aggregation": "time_aggregation",
            "time_frequency": "frequency",
            "cmor_table": "table_id",
            "dataset": "dataset",
            "driving_model": "driving_model",
            "format": "format",
            "grid_id": "grid_id",
            "level_type": "level_type",
            "rcm_name": "rcm_name",
            "rcm_version": "rcm_version",
        }

    @property
    def _cordex_lookup(self) -> Dict[str, str]:
        """Define the search facets for the cordex5 standard."""
        return {
            "experiment": "experiment",
            "ensemble": "ensemble",
            "fs_type": "fs_type",
            "grid_label": "grid_label",
            "institute": "institution",
            "model": "model",
            "project": "project",
            "product": "domain",
            "realm": "realm",
            "variable": "variable",
            "time": "time",
            "bbox": "bbox",
            "time_aggregation": "time_aggregation",
            "time_frequency": "time_frequency",
            "cmor_table": "cmor_table",
            "dataset": "dataset",
            "driving_model": "driving_model",
            "format": "format",
            "grid_id": "grid_id",
            "level_type": "level_type",
            "rcm_name": "rcm_name",
            "rcm_version": "rcm_version",
        }

    @cached_property
    def forward_lookup(self) -> Dict[str, str]:
        """Define how things get translated from the freva standard"""

        builtin_mappings = {
            "freva": {k: k for k in self._freva_facets},
            "cmip6": self._cmip6_lookup,
            "cmip5": self._cmip5_lookup,
            "cordex": self._cordex_lookup,
            "user": {k: k for k in self._freva_facets},
        }

        base_mapping = builtin_mappings.get(
            self.flavour, {k: k for k in self._freva_facets}
        ).copy()
        return base_mapping

    @cached_property
    def valid_facets(self) -> list[str]:
        """Get all valid facets for a flavour."""
        if self.translate:
            return list(self.forward_lookup.values())
        return list(self.forward_lookup.keys())

    @property
    def cordex_keys(self) -> Tuple[str, ...]:
        """Define the keys that make a cordex dataset."""
        return ("rcm_name", "driving_model", "rcm_version")

    @cached_property
    def primary_keys(self) -> list[str]:
        """Define which search facets are primary for which standard."""
        if self.translate:
            _keys = [
                self.forward_lookup[k]
                for (k, v) in self._freva_facets.items()
                if v == "primary"
            ]
        else:
            _keys = [k for (k, v) in self._freva_facets.items() if v == "primary"]
        if self.flavour in ("cordex",):
            for key in self.cordex_keys:
                _keys.append(key)
        return _keys

    @cached_property
    def backward_lookup(self) -> Dict[str, str]:
        """Translate the schema to the freva standard."""
        return {v: k for (k, v) in self.forward_lookup.items()}

    def translate_facets(
        self,
        facets: Iterable[str],
        backwards: bool = False,
    ) -> List[str]:
        """Translate the facets names to a given flavour."""
        if self.translate:
            if backwards:
                return [self.backward_lookup.get(f, f) for f in facets]
            return [self.forward_lookup.get(f, f) for f in facets]
        return list(facets)

    def translate_query(
        self,
        query: Dict[str, Any],
        backwards: bool = False,
    ) -> Dict[str, Any]:
        """Translate the queries names to a given flavour."""
        return dict(
            zip(
                self.translate_facets(query.keys(), backwards=backwards),
                query.values(),
            )
        )


class Flavour:
    def __init__(self, config: ServerConfig):
        self._config = config

    allowed_flavour_query_params = {"flavour_name", "owner", "multi_version"}
    """Set of allowed query parameters for flavour queries."""
    async def query_flavour_mongo(
        self,
        user_name: Optional[str] = None,
        flavour_name: Optional[str] = None,
    ) -> List[FlavourResponse]:
        """
        Query flavours from MongoDB for both global and user-specific flavours.

        This method retrieves flavour definitions from the MongoDB collection,
        filtering by owner (global and/or specific user) and optionally by
        flavour name.

        Parameters
        ----------
        user_name: Optional[str], default: None
            The username to include user-specific flavours for. If None,
            only global flavours are returned.
        flavour_name: Optional[str], default: None
            Filter results to only include this specific flavour name.
            If None, all matching flavours are returned.

        Returns
        -------
        List[FlavourResponse]
            A list of flavour response objects containing flavour definitions
            that match the query criteria. Returns empty list if no matches found.
        """
        try:
            or_clauses = [{"owner": "global"}]
            if user_name:
                or_clauses.append({"owner": user_name})
            mongo_filter: Dict[str, Any] = {"$or": or_clauses}
            if flavour_name:
                mongo_filter["flavour_name"] = flavour_name

            cursor = self._config.mongo_collection_flavours.find(mongo_filter)
            docs = await cursor.to_list(length=None)
            if not docs:
                return []

            return [
                FlavourResponse(
                    flavour_name=doc["flavour_name"],
                    mapping=doc["mapping"],
                    owner=doc["owner"],
                    who_created=doc.get("who_created", ""),
                    created_at=doc.get("created_at", "")
                )
                for doc in docs
            ]
        except Exception as error:
            logger.warning("MongoDB unavailable for flavour queries: %s", error)
            return []

    async def add_flavour(
        self,
        user_name: str,
        flavour_def: FlavourDefinition,
    ) -> Dict[str, str]:
        """
        Add a new custom flavour definition to MongoDB.

        This method validates the flavour definition against existing flavours
        and built-in flavours, then stores it in the MongoDB collection for
        future use in search operations.

        Parameters
        ----------
        user_name: str
            The username of the user creating the flavour. Used as owner
            unless flavour_def.is_global is True.
        flavour_def: FlavourDefinition
            The flavour definition object containing the flavour name,
            mapping, and global flag.

        Returns
        -------
        Dict[str, str]
            A dictionary containing the status message of the operation.

        Raises
        ------
        HTTPException
            Status 409 if the flavour name conflicts with global flavours
            or already exists for the same owner(Other users can define
            the same name and it doesn't conflict).
        HTTPException
            Status 500 if there's an error inserting into MongoDB.
        """
        effective_owner = "global" if flavour_def.is_global else user_name
        if (
            flavour_def.is_global
            and flavour_def.flavour_name.lower()
            in [f.lower() for f in BUILTIN_FLAVOURS]
        ):
            logger.warning(
                ("'%s' has chosen '%s' as flavour name, "
                 "but this conflicts with a global flavours."),
                user_name,
                flavour_def.flavour_name
            )
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Flavour name '{flavour_def.flavour_name}'"
                    f"conflicts with global flavours. "
                    f"Please choose another `falvour_name`."
                )
            )

        try:
            existing = await self.query_flavour_mongo(
                effective_owner, flavour_def.flavour_name
            )

            same_owner_existing = [f for f in existing if f.owner == effective_owner]

            if same_owner_existing:
                owner_type = "global" if flavour_def.is_global else "personal"
                logger.warning(
                    "'%s' tried to add flavour '%s', but it already exists.",
                    user_name,
                    flavour_def.flavour_name
                )
                raise HTTPException(
                    409,
                    (
                        f"{owner_type.capitalize()} flavour "
                        f"'{flavour_def.flavour_name}' already exists"
                    )
                )
        except HTTPException as e:
            if e.status_code != 404:
                raise

        flavour_doc = {
            "flavour_name": flavour_def.flavour_name,
            "mapping": flavour_def.mapping,
            "owner": effective_owner,
            "who_created": user_name,
            "created_at": datetime.now().isoformat()
        }

        try:
            await self._config.mongo_collection_flavours.insert_one(flavour_doc)
            logger.info(
                "Added flavour '%s' for user '%s' with mapping: %s",
                flavour_def.flavour_name,
                user_name,
                flavour_def.mapping
            )
            return {
                "status": f"Flavour '{flavour_def.flavour_name}' added successfully"
            }
        except Exception as error:
            logger.error(
                "%s's attempt to add flavour '%s' failed: %s",
                user_name,
                flavour_def.flavour_name,
                error
            )
            raise HTTPException(status_code=500, detail="Failed to add flavour")

    async def delete_flavour(
        self,
        user_name: str,
        input_flavour_name: str,
        is_global: bool = False,
    ) -> FlavourDeleteResponse:
        """
        Delete a custom flavour definition from MongoDB.

        This method removes a flavour definition from the MongoDB collection
        after validating that it exists and the user has permission to delete it.

        Parameters
        ----------
        user_name: str
            The username of the user requesting the deletion. Used to determine
            effective owner unless is_global is True.
        flavour_name: str
            The name of the flavour to delete.
        is_global: bool, default: False
            Whether to delete a global flavour. Only admin users can delete
            global flavours.

        Returns
        -------
        Dict[str, str]
            A dictionary containing the status message of the deletion operation.

        Raises
        ------
        HTTPException
            Status 404 if the flavour is not found for the specified owner.
        HTTPException
            Status 500 if there's an error deleting from MongoDB.
        """
        translator = await self.validate_and_get_flavour(
            self._config, input_flavour_name, user_name
        )
        flavour_name = translator.flavour
        if flavour_name.lower() in BUILTIN_FLAVOURS and is_global:
            logger.error(
                "Attempt to delete built-in flavour: %s by user: %s",
                flavour_name,
                user_name
            )
            raise HTTPException(
                status_code=422,
                detail=f"Cannot delete built-in flavour '{flavour_name}'"
            )
        effective_user = "global" if is_global else user_name
        try:
            result = await self._config.mongo_collection_flavours.delete_one({
                "flavour_name": flavour_name,
                "owner": effective_user
            })
            if result.deleted_count == 0:
                logger.error(
                    "user '%s' tried to delete %s flavour '%s'",
                    user_name,
                    flavour_name,
                    effective_user
                )
                raise HTTPException(
                    status_code=422,
                    detail=f"Flavour '{flavour_name}' is built-in or does not exist"
                )
            flavour_type = "global" if is_global else "personal"
            logger.info(
                "Deleted %s flavour '%s' for user '%s'",
                flavour_type,
                flavour_name,
                effective_user
            )
            return cast(FlavourDeleteResponse, {
                "status": (
                    f"{flavour_type.capitalize()} flavour "
                    f"'{flavour_name}' deleted successfully"
                )
            })
        except HTTPException:
            raise
        except Exception as error:
            logger.error(
                "user '%s' failed to delete flavour '%s': %s",
                user_name,
                flavour_name,
                error
            )
            raise HTTPException(500, "Failed to delete flavour")

    @classmethod
    async def validate_and_get_flavour(
        cls, config: ServerConfig, flavour: str, user_name: str
    ) -> Translator:
        """Validate flavour exists and return configured translator."""
        temp_flavour = Flavour(config)
        original_flavour = flavour
        owner = None

        async def get_error_details() -> str:
            all_available = [
                f.flavour_name for f in await temp_flavour.get_all_flavours(user_name)
            ]
            suggested = [f for f in all_available if flavour in f]
            message_parts = [
                (
                    f"Invalid flavour '{original_flavour}'. "
                    f"Available flavours: {all_available}"
                )
            ]

            if suggested:
                message_parts.append(f"Did you mean: {suggested}")

            if ":" in original_flavour:
                message_parts.append(
                    "For personal flavours, use either directly "
                    "'<YOUR PERSONAL FLAVOUR>' or namespaced "
                    f"'{user_name}:<YOUR PERSONAL FLAVOUR>' as flavour."
                )
            return ". ".join(message_parts) + "."

        if ":" in flavour:
            input_username, flavour = flavour.split(":", 1)
            if input_username != user_name:
                logger.error(
                    (
                        "'%s' attempted to access flavour '%s' "
                        "of user '%s' which is not allowed."
                    ),
                    user_name, flavour, input_username
                )
                raise HTTPException(status_code=422, detail=await get_error_details())
            owner = user_name

        all_flavours = await temp_flavour.get_all_flavours(
            user_name,
            flavour_name=flavour,
            owner=owner
        )
        if not any(f.flavour_name == flavour for f in all_flavours):
            logger.error(
                "%s attempted to use invalid flavour '%s'",
                user_name,
                original_flavour
            )
            raise HTTPException(status_code=422, detail=await get_error_details())

        translator = Translator(flavour, translate=True, config=config)
        custom_flavour = next(
            (f for f in all_flavours if f.flavour_name == flavour), None
        )
        if custom_flavour:
            translator.forward_lookup.update(custom_flavour.mapping)
        return translator

    @classmethod
    def validate_flavour_parameters(
        cls,
        config: ServerConfig,
        request: Request,
        **params: Any
    ) -> "Flavour":
        """Validate flavour query parameters and return Solr instance.

        Parameters
        ----------
        config: ServerConfig
            Server configuration instance
        request: Request
            The FastAPI request object containing query parameters
        **params: Any
            Additional parameters passed to the function

        Returns
        -------
        Solr
            A configured Solr instance for flavour operations

        Raises
        ------
        HTTPException
            If invalid query parameters are found (status 422)
        """
        query_params = dict(request.query_params)

        for param in list(query_params.keys()):
            if param.lower().replace("-", "_") not in cls.allowed_flavour_query_params:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"Invalid parameter '{param}'. Valid parameters: "
                        f"{list(cls.allowed_flavour_query_params)}"
                    )
                )
        return cls(config=config)

    @staticmethod
    async def list_builtin_flavours() -> List[FlavourResponse]:
        """
        Retrieve all built-in flavour definitions as FlavourResponse objects.

        This method returns the standard flavour definitions that are built into
        the system (freva, cmip5, cmip6, cordex, user). These flavours are always
        available and owned by 'global'.

        Returns
        -------
        List[FlavourResponse]
            A list of FlavourResponse objects representing all built-in flavours.
            Each response contains the flavour name, its facet mapping, owner as
            'global'.
        """
        results: List[FlavourResponse] = []
        for name in BUILTIN_FLAVOURS:
            mapping = Translator(name, translate=True).forward_lookup
            results.append(
                FlavourResponse(
                    flavour_name=name,
                    mapping=mapping,
                    owner="global",
                    who_created="freva",
                    # TODO: any better way to set current time?
                    created_at=datetime.now().isoformat()
                )
            )
        return results

    async def get_all_flavours(
        self,
        user_name: Optional[str] = None,
        flavour_name: Optional[str] = None,
        owner: Optional[str] = None
    ) -> List[FlavourResponse]:
        """
        Get all available flavours (built-in + custom) with optional filtering.

        This method combines built-in flavours with custom flavours from MongoDB,
        applying optional filters for flavour name and owner. Built-in flavours
        are always included unless explicitly filtered out by owner.

        Parameters
        ----------
        user_name : Optional[str], default: None
            Username to get user-specific flavours for. If provided, includes
            both global and user-specific custom flavours.
        flavour_name : Optional[str], default: None
            Filter by specific flavour name. If provided, only flavours with
            this exact name are returned.
        owner : Optional[str], default: None
            Filter by owner ('global' or username). If provided, only flavours
            owned by this entity are returned.

        Returns
        -------
        List[FlavourResponse]
            Combined list of built-in and custom flavours that match the
            specified filter criteria.
        """
        raw_custom = await self.query_flavour_mongo(user_name, flavour_name)
        custom = [f for f in raw_custom if (owner is None or f.owner == owner)]
        all_builtins = await self.list_builtin_flavours()
        builtins = [
            f for f in all_builtins
            if (flavour_name is None or f.flavour_name == flavour_name)
            and (owner is None or f.owner == owner)
        ]
        from collections import Counter
        all_flavours = builtins + custom
        name_counts = Counter(f.flavour_name for f in all_flavours)

        for flavour_response in all_flavours:
            original_name = flavour_response.flavour_name
            if name_counts[original_name] > 1 and flavour_response.owner != "global":
                flavour_response.flavour_name = (
                    f"{flavour_response.owner}"
                    f":{original_name}"
                )
        return all_flavours
