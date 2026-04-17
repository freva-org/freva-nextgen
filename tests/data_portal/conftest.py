"""Common fixtures for the data portal rest api service."""

import pathlib
from typing import List

import pytest


@pytest.fixture(scope="session")
def aggregation_files() -> List[str]:
    """Define the files that are good for aggregating."""
    from freva_rest.databrowser_api import mock

    file_dir = pathlib.Path(mock.__file__).parent / "data" / "model"
    return sorted(map(str, file_dir.glob("*.nc")))
