"""
The registry of settings resources.
"""

from typing import Dict, NamedTuple, Tuple, Type

from pydantic import BaseModel

from .core import check_reserved_fields
from .schema import UiConfig, UiConfigUpdate


class Resource(NamedTuple):
    model: Type[BaseModel]
    update_model: Type[BaseModel]
    open_maps: Tuple[str, ...] = ()
    """Dotted paths of the fields that are open maps; a user-defined set of
    keys rather than a fixed model.
    """


REGISTRY: Dict[str, Resource] = {
    "ui": Resource(
        model=UiConfig,
        update_model=UiConfigUpdate,
        open_maps=(
            "extra_colors",
            "public_extensions",
            "features.databrowser.fixed_facets",
        ),
    ),
}
"""The single place that knows which settings resources exist."""


for _name, _resource in REGISTRY.items():
    # At import, so a model colliding with the storage layer's own fields is a
    # startup error naming the field, not a record that silently becomes
    # unwritable the first time someone patches it.
    check_reserved_fields(_name, _resource.model, _resource.update_model)
