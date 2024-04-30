"""Load backend for reading different datasets."""

from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
from urllib.parse import urlparse
from typing import Optional, cast
import uuid

import cloudpickle
import xarray as xr
import zarr

from freva_rest.utils import str_to_int, RedisCache
from freva_rest import CACHE_EXP, REST_URL
from .schema import LoadStatus, LoadDict


class DataLoadFactory:
    """Class to load data object and convert them to zarr.

    The class defines different staticmethods that load datasets for different
    storage systems.

    Currently implemented are:
        from_posix: loads a dataset from a posix file system

    Parameters
    ----------
    scheme: str
    the url prefix of the object path that holds the
    dataset. For example the scheme of hsm://arch/foo.nc would
    indicate that the data is stored on an hsm tape archive.
    A posix file system if assumed if scheme is empty, e.g /home/bar/foo.nc
    """

    def __init__(self, scheme: str) -> None:
        implemented_methods = {"file": self.from_posix, "": self.from_posix}
        try:
            self.read = implemented_methods[scheme.lower()]
        except KeyError:
            raise NotImplementedError(f"datasets on {scheme} can't be loaded")

    @staticmethod
    def from_posix(input_path: str) -> zarr.storage.BaseStore:
        """Open a dataset with xarray."""
        return (
            xr.open_dataset(input_path, encode_cf=False, chunks="auto")
            .to_zarr()
            .ds
        )

    @classmethod
    def from_object_path(cls, input_path: str, path_id: str) -> None:
        """Create a zarr object from an input path."""
        parsed_url = urlparse(input_path)
        status_dict = LoadStatus.from_dict(
            cast(
                Optional[LoadDict],
                cloudpickle.loads(RedisCache.get(path_id)),
            )
        ).dict()
        expires_in = str_to_int(CACHE_EXP, 3600)
        status_dict["status"] = 3
        RedisCache.setex(path_id, expires_in, cloudpickle.dumps(status_dict))
        try:
            status_dict["obj"] = cloudpickle.dumps(
                cls(parsed_url.scheme).read(input_path)
            )
            status_dict["status"] = 0
        except Exception as error:
            status_dict["status"] = 1
            status_dict["reason"] = str(error)
        RedisCache.setex(
            path_id,
            expires_in,
            cloudpickle.dumps(status_dict),
        )


class ProcessQueue(ProcessPoolExecutor):
    """Class that can load datasets on different object stores."""

    def __init__(self) -> None:
        context = mp.get_context(method="spawn")
        super().__init__(max_workers=18, mp_context=context)

    async def spawn(self, inp_obj: str) -> str:
        """Subumit a new data loading task to the process pool."""
        uuid5 = str(uuid.uuid5(uuid.NAMESPACE_URL, inp_obj))
        cache: Optional[bytes] = RedisCache.get(uuid5)
        status_dict: LoadDict = {
            "status": 2,
            "url": f"{REST_URL}/api/zarr/{uuid5}",
            "obj": None,
            "reason": "",
        }
        if cache is None:
            RedisCache.setex(
                uuid5,
                str_to_int(CACHE_EXP, 3600),
                cloudpickle.dumps(status_dict),
            )
            self.submit(DataLoadFactory.from_object_path, inp_obj, uuid5)
        else:
            status_dict = cast(LoadDict, cloudpickle.loads(cache))
        return status_dict["url"]
