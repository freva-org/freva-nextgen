"""Load backend for reading different datasets."""

from dataclasses import dataclass
import multiprocessing as mp
import json
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Literal, Optional, TypedDict, Union, cast

import cloudpickle  # fades cloudpickle
from dask.distributed import LocalCluster  # fades distributed
from dask.distributed import Client
from dask.distributed.deploy.cluster import Cluster
import redis  # fades redis
import xarray as xr  # fades xarray
from xarray.backends.zarr import encode_zarr_variable
from xpublish.utils.zarr import (
    create_zmetadata,
    jsonify_zmetadata,
)  # fades xpublish
from xpublish.utils.zarr import get_data_chunk, encode_chunk
import zarr  # fades zarr
from zarr.meta import encode_array_metadata, encode_group_metadata
from zarr.storage import array_meta_key

from .utils import data_logger, str_to_int
from .backends import load_data

ZARR_CONSOLIDATED_FORMAT = 1
ZARR_FORMAT = 2

CLIENT: Optional[Client] = None
LoadDict = TypedDict(
    "LoadDict",
    {
        "status": Literal[0, 1, 2, 3],
        "url": str,
        "obj": Optional[xr.Dataset],
        "obj_path": str,
        "reason": str,
        "meta": Optional[Dict[str, Any]],
        "json_meta": Optional[Dict[str, Any]],
    },
)
RedisKw = TypedDict(
    "RedisKw", {"user": str, "passwd": str, "host": str, "port": int}
)


class RedisCacheFactory(redis.Redis):
    """Define a custom redis cache."""

    def __init__(self, db: int = 0) -> None:
        host, _, port = (
            (os.environ.get("REDIS_HOST") or "localhost")
            .replace("redis://", "")
            .partition(":")
        )
        port_i = int(port or "6379")
        super().__init__(
            host=host,
            port=port_i,
            db=db,
            username=os.getenv("REDIS_USER"),
            password=os.getenv("REDIS_PASS"),
            ssl=bool(len(os.getenv("REDIS_SSL_CERTFILE", ""))),
            ssl_certfile=os.getenv("REDIS_SSL_CERTFILE") or None,
            ssl_keyfile=os.getenv("REDIS_SSL_KEYFILE") or None,
            ssl_ca_certs=os.getenv("REDIS_SSL_CERTFILE") or None,
        )


@dataclass
class LoadStatus:
    """Schema defining the status of loading dataset."""

    status: Literal[0, 1, 2, 3]
    """Status of the submitted jobs:
        0: exit success
        1: exit failed
        2: in queue (submitted)
        3: in progress
    """
    obj_path: str
    """url of the zarr dataset once finished."""
    obj: Optional[xr.Dataset]
    """pickled memory view of the opened dataset."""
    reason: str
    """if status = 1 reasone why opening the dataset failed."""
    meta: Optional[Dict[str, Any]] = None
    """Meta data of the zarr store"""
    url: str = ""
    """Url of the machine that loads the zarr store."""
    json_meta: Optional[Dict[str, Any]] = None
    """Json representation of the zarr metadata."""

    @staticmethod
    def lookup(status: int) -> str:
        """Translate a status integer to a human readable status."""
        _lookup = {
            0: "finished, ok",
            1: "finished, failed",
            2: "waiting",
            3: "processing",
        }
        return _lookup.get(status, "unkown")

    def dict(self) -> LoadDict:
        """Convert object to dict."""
        return {
            "status": self.status,
            "obj": self.obj,
            "obj_path": self.obj_path,
            "reason": self.reason,
            "meta": self.meta,
            "url": self.url,
            "json_meta": self.json_meta,
        }

    @classmethod
    def from_dict(cls, load_dict: Optional[LoadDict]) -> "LoadStatus":
        """Create an instance of the class from a normal python dict."""
        _dict = load_dict or {
            "status": 2,
            "obj": None,
            "reason": "",
            "url": "",
            "obj_path": "",
            "meta": None,
            "json_meta": None,
        }
        return cls(**_dict)


def get_dask_client(client: Optional[Client] = CLIENT) -> Client:
    """Get or create a cached dask cluster."""
    if client is None:
        client = Client(
            LocalCluster(scheduler_port=int(os.getenv("DASK_PORT", "40000")))
        )
        data_logger.info("Created new cluster: %s", client.dashboard_link)
    else:
        data_logger.debug("recycling dask cluster.")
    return client


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

    def __init__(self, cache: Optional[redis.Redis] = None) -> None:
        self.cache = cache or RedisCacheFactory(0)

    def read(self, input_path: str) -> xr.Dataset:
        """Open the dataset."""
        return load_data(inp_data)

    @staticmethod
    def from_posix(input_path: str) -> xr.Dataset:
        """Open a dataset with xarray."""

        return xr.open_dataset(
            input_path,
            decode_cf=False,
            use_cftime=False,
            chunks="auto",
            cache=False,
            decode_coords=False,
        )

    @classmethod
    def from_object_path(
        cls, input_path: str, path_id: str, cache: Optional[redis.Redis] = None
    ) -> None:
        """Create a zarr object from an input path."""
        cache = cache or RedisCacheFactory(0)
        status_dict = LoadStatus.from_dict(
            cast(
                Optional[LoadDict],
                cloudpickle.loads(cache.get(path_id)),
            )
        ).dict()
        expires_in = str_to_int(os.environ.get("API_CACHE_EXP"), 3600)
        status_dict["status"] = 3
        cache.setex(path_id, expires_in, cloudpickle.dumps(status_dict))
        data_logger.debug("Reading %s", input_path)
        try:
            data_logger.info(input_path)
            dset = load_data(input_path)
            metadata = create_zmetadata(dset)
            status_dict["json_meta"] = jsonify_zmetadata(dset, metadata)
            status_dict["obj"] = dset
            status_dict["meta"] = metadata
            status_dict["status"] = 0
        except Exception as error:
            data_logger.exception("Could not process %s: %s", path_id, error)
            status_dict["status"] = 1
            status_dict["reason"] = str(error)
        cache.setex(
            path_id,
            expires_in,
            cloudpickle.dumps(status_dict),
        )

    @classmethod
    def get_zarr_chunk(
        cls,
        key: str,
        chunk: str,
        variable: str,
        cache: Optional[redis.Redis] = None,
    ) -> None:
        """Read the zarr metadata from the cache."""
        cache = cache or RedisCacheFactory(0)
        pickle_data = cls.load_object(key, cache)
        dset = cast(xr.Dataset, pickle_data["obj"])
        meta = cast(Dict[str, Any], pickle_data["meta"])
        arr_meta = meta["metadata"][f"{variable}/{array_meta_key}"]
        data = encode_chunk(
            get_data_chunk(
                encode_zarr_variable(
                    dset.variables[variable], name=variable
                ).data,
                chunk,
                out_shape=arr_meta["chunks"],
            ).tobytes(),
            filters=arr_meta["filters"],
            compressor=arr_meta["compressor"],
        )
        cache.setex(f"{key}-{variable}-{chunk}", 360, data)

    @staticmethod
    def load_object(key: str, cache: Optional[redis.Redis] = None) -> LoadDict:
        """Load a cached dataset.

        Parameters
        ----------
        key: str, The cache key.
        cache: The redis cache object, if None (default) a new redis isntance
               is created.

        Returns
        -------
        The data that was stored under that key.

        Raises
        ------
        RuntimeError: If the cache key exists but the data could not be loaded,
                      which means that there is a load status != 0
        KeyError: If the key doesn't exist in the cache (anymore).
        """
        cache = cache or RedisCacheFactory(0)
        data_cache = cache.get(key)
        if data_cache is None:
            raise KeyError(f"{key} uuid does not exist (anymore).")
        pickle_data: LoadDict = cloudpickle.loads(data_cache)
        task_status = pickle_data.get("status", 1)
        if task_status != 0:
            raise RuntimeError(LoadStatus.lookup(task_status))
        return pickle_data


class ProcessQueue:
    """Class that can load datasets on different object stores."""

    def __init__(
        self,
        redis_cache: Optional[redis.Redis] = None,
    ) -> None:
        self.redis_cache = redis_cache or RedisCacheFactory(0)
        self.client = get_dask_client()

    def run_for_ever(
        self,
        channel: str,
    ) -> None:
        """Start the listner deamon."""
        data_logger.info("Starting data-loading deamon")
        pubsub = self.redis_cache.pubsub()
        pubsub.subscribe(channel)
        data_logger.info("Broker will listen for messages now")
        while True:
            message = pubsub.get_message()
            if message and message["type"] == "message":
                self.redis_callback(message["data"])
            else:
                time.sleep(0.1)

    def redis_callback(
        self,
        body: bytes,
    ) -> None:
        """Callback method to recieve rabbit mq messages."""
        try:
            message = json.loads(body)
            if "uri" in message:
                self.spawn(message["uri"]["path"], message["uri"]["uuid"])
            elif "chunk" in message:
                DataLoadFactory.get_zarr_chunk(
                    message["chunk"]["uuid"],
                    message["chunk"]["chunk"],
                    message["chunk"]["variable"],
                    self.redis_cache,
                )
        except json.JSONDecodeError:
            data_logger.warning("could not decode message")

    def spawn(self, inp_obj: str, uuid5: str) -> str:
        """Subumit a new data loading task to the process pool."""
        data_logger.debug(
            "Assigning %s to %s for future processing", inp_obj, uuid5
        )
        cache: Optional[bytes] = self.redis_cache.get(uuid5)
        status_dict: LoadDict = {
            "status": 2,
            "obj_path": f"/api/freva-data-portal/zarr/{uuid5}.zarr",
            "obj": None,
            "reason": "",
            "url": "",
            "meta": None,
            "json_meta": None,
        }
        if cache is None:
            self.redis_cache.setex(
                uuid5,
                str_to_int(os.environ.get("API_CACHE_EXP"), 3600),
                cloudpickle.dumps(status_dict),
            )
            DataLoadFactory.from_object_path(inp_obj, uuid5, self.redis_cache)
        else:
            status_dict = cast(LoadDict, cloudpickle.loads(cache))
            if status_dict["status"] in (1, 2):
                # Failed job, let's retry
                # self.client.submit(
                DataLoadFactory.from_object_path(
                    inp_obj, uuid5, self.redis_cache
                )
                # )

        return status_dict["obj_path"]
