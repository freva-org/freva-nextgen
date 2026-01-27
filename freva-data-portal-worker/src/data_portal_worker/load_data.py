"""Load backend for reading different datasets."""

import json
import os
import ssl
import time
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypedDict,
    Union,
    cast,
)

import cloudpickle
import numcodecs
import xarray as xr
from dask.distributed import Client, LocalCluster
from redis.backoff import ExponentialBackoff
from redis.client import PubSub, Redis
from redis.exceptions import RedisError
from redis.retry import Retry
from xarray.backends.zarr import encode_zarr_variable

from .aggregator import DatasetAggregator, write_grouped_zarr
from .backends import load_data
from .rechunker import ChunkOptimizer
from .utils import JSONObject, data_logger, str_to_int, xr_repr_html
from .zarr_utils import (
    encode_chunk,
    get_data_chunk,
)

ZARR_CONSOLIDATED_FORMAT = 1
ZARR_FORMAT = 2
ZARRAY_JSON = ".zarray"

CLIENT: Optional[Client] = None


class StateEnum(Enum):

    finished_ok = 0
    finished_failed = 1
    finished_not_found = 2
    waiting = 3
    processing = 4
    ukown = 5

    @classmethod
    def from_exception(cls, error: Exception) -> int:
        """Define which error state we should assign."""
        if isinstance(error, (FileNotFoundError, KeyError)):
            return cls.finished_not_found.value
        return cls.finished_failed.value


class LoadDict(TypedDict, total=False):
    """Definition of the job payload."""

    status: int
    url: str
    obj_path: str
    reason: str
    data: Optional[Union[bytes, JSONObject]]
    repr_html: str


class RedisKw(TypedDict, total=False):
    """Essential arguments for creating a redis connection."""

    user: str
    passwd: str
    host: str
    port: int
    ssl_cert: str
    ssl_key: str


class RedisConnectionDict(TypedDict, total=False):
    """Connection arguments for the Redis connection."""

    host: str
    port: int
    db: int
    username: Optional[str]
    password: Optional[str]
    ssl: bool
    ssl_certfile: Optional[str]
    ssl_keyfile: Optional[str]
    ssl_ca_certs: Optional[str]
    ssl_cert_reqs: ssl.VerifyMode
    health_check_interval: int
    retry: Retry
    retry_on_error: List[Type[Exception]]
    retry_on_timeout: bool
    socket_keepalive: bool


class RedisCacheFactory(Redis):
    """Define a custom redis cache."""

    def __init__(self, db: int = 0, retry_interval: int = 30) -> None:
        self._db = db
        self._retry = Retry(ExponentialBackoff(cap=10, base=0.1), retries=25)
        self._retry_interval = retry_interval
        conn = self.connection_args
        data_logger.info("Creating redis connection using: %s", conn)

        super().__init__(**conn)

    @property
    def connection_args(self) -> RedisConnectionDict:
        """Define the arguments for the redis connection."""
        host, _, port = (
            (os.environ.get("API_REDIS_HOST") or "localhost")
            .replace("redis://", "")
            .partition(":")
        )
        port_i = int(port or "6379")
        return RedisConnectionDict(
            host=host,
            port=port_i,
            db=self._db,
            username=os.getenv("API_REDIS_USER") or None,
            password=os.getenv("API_REDIS_PASSWORD") or None,
            ssl_certfile=os.getenv("API_REDIS_SSL_CERTFILE") or None,
            ssl_keyfile=os.getenv("API_REDIS_SSL_KEYFILE") or None,
            ssl_ca_certs=os.getenv("API_REDIS_SSL_CERTFILE") or None,
            ssl=os.getenv("API_REDIS_SSL_CERTFILE") is not None,
            ssl_cert_reqs=ssl.CERT_NONE,
            health_check_interval=self._retry_interval,
            socket_keepalive=True,
            retry=self._retry,
            retry_on_error=[RedisError, OSError],
            retry_on_timeout=True,
        )


@dataclass
class LoadStatus:
    """Schema defining the status of loading dataset."""

    status: int
    """Status of the submitted jobs:
        0: exit success
        1: exit failed
        2: exit, file not found
        3: in queue (submitted)
        4: in progress
        5: gone
    """
    obj_path: str
    """url of the zarr dataset once finished."""
    reason: str
    """eason why opening the dataset failed."""
    url: str = ""
    """Url of the machine that loads the zarr store."""
    data: Optional[Union[bytes, JSONObject]] = None
    """Json representation of the zarr metadata or the zarr chunk."""
    repr_html: str = "<b>No data could be loaded.</b>"
    """Html representation of the zarr metadata."""

    def dict(self) -> LoadDict:
        """Convert object to dict."""
        return {
            "status": self.status,
            "obj_path": self.obj_path,
            "url": self.url,
            "data": self.data,
            "repr_html": self.repr_html,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, load_dict: LoadDict) -> "LoadStatus":
        """Create an instance of the class from a normal python dict."""
        _dict = LoadDict(
            status=load_dict.get("status") or StateEnum.waiting.value,
            reason=load_dict.get("reason", ""),
            url=load_dict.get("url", ""),
            obj_path=load_dict.get("obj_path", ""),
            data=load_dict.get("data"),
            repr_html=load_dict.get("repr_html", cls.repr_html),
        )
        return cls(**_dict)


def get_dask_client(
    client: Optional[Client] = CLIENT, dev_mode: bool = False
) -> Optional[Client]:
    """Get or create a cached dask cluster."""
    if client is None and dev_mode is False:
        client = Client(
            LocalCluster(
                host="0.0.0.0",
                scheduler_port=int(os.getenv("DASK_PORT", "40000")),
            )
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

    def __init__(self) -> None:
        self._cache: Optional[Redis] = None

    @property
    def cache(self) -> Redis:
        """Get or create the cache."""
        if self._cache is None:
            self._cache = RedisCacheFactory()
        return self._cache

    def from_object_path(
        self,
        input_paths: List[str],
        path_id: str,
        assembly: Optional[Dict[str, Optional[str]]] = None,
        map_primary_chunksize: int = 1,
        access_pattern: Literal["time_series", "map"] = "map",
        chunk_size: float = 16.0,
    ) -> None:
        """Create a zarr object from an input path."""
        agg = DatasetAggregator()
        opt = ChunkOptimizer(
            access_pattern=access_pattern,
            target=f"{chunk_size}MiB",
            map_primary_chunksize=map_primary_chunksize,
        )

        data = cast(
            LoadDict,
            cloudpickle.loads(self.cache.get(path_id) or b"\x80\x05}\x94."),
        )
        data["status"] = StateEnum.processing.value
        data.setdefault("obj_path", f"/api/freva-data-portal/zarr/{path_id}.zarr")
        data.setdefault("repr_html", "<b>Data hasn't been loaded.</b>")
        status_dict = LoadStatus.from_dict(data).dict()
        expires_in = str_to_int(os.environ.get("API_CACHE_EXP"), 3600)
        status_dict["status"] = StateEnum.processing.value
        self.cache.setex(path_id, expires_in, cloudpickle.dumps(status_dict))
        data_logger.debug("Reading %s", ",".join(input_paths))
        try:
            dsets = {
                k: opt.opening(d)
                for k, d in agg.aggregate(
                    [load_data(p) for p in input_paths],
                    job_id=path_id,
                    plan=assembly,
                    access_pattern=access_pattern,
                    map_primary_chunksize=map_primary_chunksize,
                    chunk_size=f"{chunk_size}MiB",
                ).items()
            }
            combined_meta = write_grouped_zarr(dsets)
            status_dict["data"] = combined_meta
            status_dict["repr_html"] = xr_repr_html(dsets)
            pkls = cloudpickle.dumps(dsets)
            # We need to add the xr dataset to an extra cache entry because the
            # status_dict will be loaded by the rest-api, if the xarray dataset
            # would be present in the status_dict the rest-api code would attempt
            # to instantiate the pickled dataset object and that might fail
            # because we might or might not have xarray and all the backends
            # instantiated. Since the xarray dataset object isn't needed
            # anyway byt the rest-api we simply add it to a cache entry of its
            # own.
            status_dict["status"] = StateEnum.finished_ok.value
            self.cache.setex(f"{path_id}-dset", expires_in, pkls)
        except Exception as error:
            data_logger.exception("Could not process %s: %s", path_id, error)
            status_dict["status"] = StateEnum.from_exception(error)
            status_dict["reason"] = str(error)
        self.cache.setex(
            path_id,
            expires_in,
            cloudpickle.dumps(status_dict),
        )

    def get_zarr_chunk(
        self,
        key: str,
        chunk: str,
        var_group: str,
    ) -> None:
        """Read the zarr metadata from the cache."""
        group, _, variable = var_group.rpartition("/")
        group = group or "root"
        try:
            meta, dsets = self.load_object(key)
            arr_meta = meta["metadata"][f"{var_group}/{ZARRAY_JSON}"]
            data = encode_chunk(
                get_data_chunk(
                    encode_zarr_variable(
                        dsets[group].variables[variable], name=variable
                    ).data,
                    chunk,
                    out_shape=arr_meta["chunks"],
                ).tobytes(),
                filters=arr_meta["filters"],
                compressor=numcodecs.get_codec(arr_meta["compressor"]),
            )
            package = LoadDict(data=data, status=0, reason="")
        except Exception as error:
            data_logger.exception(error)
            package = dict(
                reason=str(error), status=StateEnum.from_exception(error)
            )
        self.cache.setex(
            f"{key}-{var_group}-{chunk}", 360, cloudpickle.dumps(package)
        )

    def load_object(
        self, key: str
    ) -> Tuple[Dict[str, Any], Dict[str, xr.Dataset]]:
        """Load a cached dataset.

        Parameters
        ----------
        key: str, The cache key.

        Returns
        -------
        The data that was stored under that key.

        Raises
        ------
        RuntimeError: If the cache key exists but the data could not be loaded,
                      which means that there is a load status != 0
        KeyError: If the key doesn't exist in the cache (anymore).
        """
        metadata_cache = cast(Optional[bytes], self.cache.get(key))
        dset_cache = self.cache.get(f"{key}-dset")
        if metadata_cache is None or dset_cache is None:
            raise KeyError(f"{key} uuid does not exist (anymore).")
        load_dict: LoadDict = cloudpickle.loads(metadata_cache)
        dsets = cast(Dict[str, xr.Dataset], cloudpickle.loads(dset_cache))
        return cast(Dict[str, Any], load_dict["data"]), dsets


class ProcessQueue(DataLoadFactory):
    """Class that can load datasets on different object stores."""

    def __init__(
        self,
        dev_mode: bool = False,
        backoff_sec: float = 0.2,
        max_backoff_sec: float = 5.0,
    ) -> None:
        super().__init__()
        self._backoff_sec = backoff_sec
        self._max_backoff_sec = max_backoff_sec
        self.client = get_dask_client(dev_mode=dev_mode)
        self.dev_mode = dev_mode

    def _close_pubsub(
        self, pubsub: Optional[PubSub], recycle: bool = False
    ) -> None:
        if pubsub is not None:
            self.backoff_sec = min(self._max_backoff_sec, self._backoff_sec * 2)
            try:
                pubsub.close()
            except Exception:
                pass
            time.sleep(self.backoff_sec)

    def run_for_ever(self, channel: str) -> None:
        """Start the listener daemon."""
        data_logger.info("Starting data-loading daemon")
        pubsub: Optional[PubSub] = None
        data_logger.info("Broker will listen for messages now")
        while True:
            try:
                if pubsub is None:
                    pubsub = self.cache.pubsub()
                    pubsub.subscribe(channel)
                message = pubsub.get_message()
                if message and message["type"] == "message":
                    self.redis_callback(message["data"])
                else:
                    time.sleep(0.1)
            except KeyboardInterrupt:
                self._close_pubsub(pubsub)
                raise KeyboardInterrupt("Exiting")
            except RedisError:  # pragma: no cover
                self._close_pubsub(pubsub)  # pragma: no cover
                pubsub = None  # pragma: no cover
            except Exception as error:
                data_logger.exception(error)

    def redis_callback(
        self,
        body: bytes,
    ) -> None:
        """Callback method to receive rabbit mq messages."""
        try:
            message = json.loads(body)
        except json.JSONDecodeError:
            data_logger.warning("could not decode message")
            return
        if "uri" in message:
            self.spawn(
                message["uri"]["path"],
                message["uri"]["uuid"],
                assembly=message["uri"].get("assembly"),
                access_pattern=message["uri"].get("access_pattern", "map"),
                map_primary_chunksize=message["uri"].get(
                    "map_primary_chunksize", 1
                ),
                reload=message["uri"].get("reload", False),
                chunk_size=message["uri"].get("chunk_size", 16.0),
            )
        elif "chunk" in message:
            self.get_zarr_chunk(
                message["chunk"]["uuid"],
                message["chunk"]["chunk"],
                message["chunk"]["variable"],
            )
        elif "shutdown" in message:
            if message["shutdown"] is True and self.dev_mode is True:
                raise KeyboardInterrupt("Shutdown client")

    def spawn(
        self,
        inp_objs: List[str],
        uuid5: str,
        assembly: Optional[Dict[str, Optional[str]]] = None,
        access_pattern: Literal["map", "time_series"] = "map",
        map_primary_chunksize: int = 1,
        reload: bool = False,
        chunk_size: float = 16.0,
    ) -> None:
        """Submit a new data loading task to the process pool."""
        data_logger.debug(
            "Assigning %s to %s for future processing", inp_objs, uuid5
        )
        data_cache = cast(
            LoadDict,
            cloudpickle.loads(self.cache.get(uuid5) or b"\x80\x05}\x94."),
        )
        if data_cache.get("status") in (None, 1, 2) or reload:
            self.from_object_path(
                inp_objs,
                uuid5,
                assembly=assembly,
                access_pattern=access_pattern,
                map_primary_chunksize=map_primary_chunksize,
                chunk_size=chunk_size,
            )
