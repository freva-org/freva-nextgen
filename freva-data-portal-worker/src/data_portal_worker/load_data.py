"""Load backend for reading different datasets."""

import itertools
import json
import os
import ssl
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
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
from cachetools import TTLCache
from redis import BlockingConnectionPool, Connection, SSLConnection
from redis.backoff import ExponentialBackoff
from redis.client import Redis
from redis.exceptions import RedisError
from redis.retry import Retry
from xarray.backends.zarr import encode_zarr_variable

from ._cache_manager import CacheScheduler
from .aggregator import DatasetAggregator, write_grouped_zarr
from .backends import load_data
from .rechunker import ChunkOptimizer
from .sanitizer import sanitize_message
from .utils import (
    JSONObject,
    background_task,
    data_logger,
    str_to_int,
    user_can_read,
    xr_repr_html,
)
from .zarr_utils import (
    encode_chunk,
    get_data_chunk,
)

ZARR_CONSOLIDATED_FORMAT = 1
ZARR_FORMAT = 2
ZARRAY_JSON = ".zarray"


class StateEnum(Enum):
    finished_ok = 0
    finished_failed = 1
    finished_not_found = 2
    waiting = 3
    processing = 4
    ukown = 5
    finished_permission_denied = 6

    @classmethod
    def from_exception(cls, error: Exception) -> int:
        """Define which error state we should assign."""
        if isinstance(error, (FileNotFoundError, KeyError)):
            return cls.finished_not_found.value
        if isinstance(error, (PermissionError,)):
            return cls.finished_permission_denied.value
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
    max_connections: int


class RedisCacheFactory(Redis):
    """Define a custom redis cache."""

    def __init__(
        self,
        db: int = 0,
        retry_interval: int = 30,
        timeout: int = 5,
        username: Optional[str] = None,
        password: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_keyfile: Optional[str] = None,
        hostname: Optional[str] = None,
    ) -> None:
        self._db = db
        self._retry = Retry(ExponentialBackoff(cap=10, base=0.1), retries=25)
        self._retry_interval = retry_interval
        self._kwargs: Dict[str, Optional[str]] = {
            "username": username,
            "password": password,
            "ssl_certfile": ssl_certfile,
            "ssl_keyfile": ssl_keyfile,
            "hostname": hostname,
        }
        conn = self.connection_args
        obscure = (
            "username",
            "password",
            "ssl_certfile",
            "ssl_keyfile",
            "ssl_ca_certs",
        )
        conn_info = [
            f"{k}=***" if k in obscure and s else f"{k}={s}" for (k, s) in conn.items()
        ]
        connection_class = Connection if self._use_ssl is False else SSLConnection
        data_logger.info(
            "Creating redis connection pool using: %s via %s",
            " ".join(conn_info),
            connection_class,
        )

        pool = BlockingConnectionPool(
            timeout=timeout, connection_class=connection_class, **conn
        )
        super().__init__(connection_pool=pool)

    @property
    def _use_ssl(self) -> bool:
        return (
            self._kwargs.get("ssl_certfile", os.getenv("API_REDIS_SSL_CERTFILE"))
            is not None
        )

    @property
    def connection_args(self) -> RedisConnectionDict:
        """Define the arguments for the redis connection."""
        hostname = (
            self._kwargs.get("hostname", os.getenv("API_REDIS_HOST")) or "localhost"
        ).split("://")[-1]
        host, _, port = hostname.partition(":")
        port_i = int(port or "6379")
        kwargs = RedisConnectionDict(
            host=host,
            port=port_i,
            db=self._db,
            username=self._kwargs["username"] or os.getenv("API_REDIS_USER"),
            password=self._kwargs["password"] or os.getenv("API_REDIS_PASSWORD"),
            health_check_interval=self._retry_interval,
            socket_keepalive=True,
            retry=self._retry,
            retry_on_error=[RedisError, OSError],
            retry_on_timeout=True,
            max_connections=50,
        )
        ssl_args: RedisConnectionDict = {
            "ssl_certfile": self._kwargs["ssl_certfile"]
            or os.getenv("API_REDIS_SSL_CERTFILE"),
            "ssl_keyfile": self._kwargs["ssl_keyfile"]
            or os.getenv("API_REDIS_SSL_KEYFILE"),
            "ssl_ca_certs": self._kwargs["ssl_certfile"]
            or os.getenv("API_REDIS_SSL_CERTFILE"),
            "ssl_cert_reqs": ssl.CERT_NONE,
        }
        if self._use_ssl:
            kwargs.update(ssl_args)
        return kwargs


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

    def __init__(
        self,
        hostname: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
    ) -> None:
        self._cache: Optional[Redis] = None
        # Per-process, per-instance cache: avoids re-deserialising the
        # cloudpickle blob for every concurrent get_zarr_chunk thread.
        self._connection_args: Dict[str, Optional[str]] = {
            "hostname": hostname,
            "username": username,
            "password": password,
            "ssl_keyfile": ssl_keyfile,
            "ssl_certfile": ssl_certfile,
        }
        self._object_cache: TTLCache[
            str, Optional[Tuple[Dict[str, Any], Dict[str, xr.Dataset]]]
        ] = TTLCache(
            maxsize=int(os.environ.get("API_OBJECT_CACHE_SIZE", "32")),
            ttl=int(os.environ.get("API_CACHE_EXP", "3600")),
        )
        self._object_cache_lock = threading.Lock()

    def _evict_object_cache(self, key: str) -> None:
        """Remove *key* from the in-memory cache (call before a reload)."""
        with self._object_cache_lock:
            self._object_cache.pop(key, None)

    def _preload_coordinate_chunks(
        self,
        token: str,
        meta: Dict[str, Any],
        dsets: Dict[str, xr.Dataset],
        ttl: int = 360,
    ) -> None:
        """Pre-populate coordinate chunks in the cache."""
        for group_name, ds in dsets.items():
            group_prefix = "" if group_name == "root" else f"{group_name}/"
            for coord_name in ds.coords:
                var_key = f"{group_prefix}{coord_name}"
                arr_meta_key = f"{var_key}/{ZARRAY_JSON}"
                if arr_meta_key not in meta["metadata"]:
                    continue
                arr_meta = meta["metadata"][arr_meta_key]
                chunk_shape = arr_meta["chunks"]
                total_shape = arr_meta["shape"]
                compressor = (
                    numcodecs.get_codec(arr_meta["compressor"])
                    if arr_meta.get("compressor")
                    else None
                )
                filters = arr_meta["filters"]
                values = ds.variables[coord_name].values

                chunk_ranges = [
                    range(-(-s // c)) for s, c in zip(total_shape, chunk_shape)
                ]

                for idx in itertools.product(*chunk_ranges):
                    chunk_id = ".".join(map(str, idx))
                    try:
                        slices = tuple(
                            slice(i * c, min((i + 1) * c, s))
                            for i, c, s in zip(idx, chunk_shape, total_shape)
                        )
                        chunk_data = values[slices]
                        raw = encode_chunk(
                            chunk_data.tobytes(),
                            filters=filters,
                            compressor=compressor,
                        )
                        package = cloudpickle.dumps(
                            LoadDict(data=raw, status=0, reason="")
                        )
                        self.cache.setex(f"{token}-{var_key}-{chunk_id}", ttl, package)
                    except Exception as error:
                        data_logger.warning(
                            "Failed to preload %s chunk %s: %s",
                            var_key,
                            chunk_id,
                            error,
                        )

    @property
    def cache(self) -> Redis:
        """Get or create the cache."""
        if self._cache is None:
            self._cache = RedisCacheFactory(
                hostname=self._connection_args["hostname"],
                username=self._connection_args["username"],
                password=self._connection_args["password"],
                ssl_certfile=self._connection_args["ssl_certfile"],
                ssl_keyfile=self._connection_args["ssl_keyfile"],
            )
        return self._cache

    @background_task
    def from_object_path(
        self,
        input_paths: List[str],
        path_id: str,
        assembly: Optional[Dict[str, Optional[str]]] = None,
        map_primary_chunksize: int = 1,
        access_pattern: Literal["time_series", "map"] = "map",
        chunk_size: float = 16.0,
        username: Optional[str] = None,
    ) -> None:
        """Create a zarr object from an input path."""
        start = time.time()
        data_logger.info("Registering serialisation task ...")
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
        self._evict_object_cache(path_id)
        data["status"] = StateEnum.processing.value
        data.setdefault("obj_path", f"/api/freva-data-portal/zarr/{path_id}.zarr")
        data.setdefault("repr_html", "<b>Data hasn't been loaded.</b>")
        status_dict = LoadStatus.from_dict(data).dict()
        expires_in = str_to_int(os.environ.get("API_CACHE_EXP"), 3600)
        status_dict["status"] = StateEnum.processing.value
        self.cache.setex(path_id, expires_in, cloudpickle.dumps(status_dict))
        data_logger.info("%s", ",".join(input_paths))
        try:
            ProcessQueue.check_for_access_permissions(username, input_paths)
            dsets = {
                k: opt.apply(d)
                for k, d in agg.aggregate(
                    [load_data(p) for p in input_paths],
                    job_id=path_id,
                    plan=assembly,
                ).items()
            }
            step = time.time()
            data_logger.info("Reading done within %.2f sec", step - start)
            data_logger.info("Serialising data")
            combined_meta = write_grouped_zarr(dsets)
            status_dict["data"] = combined_meta
            status_dict["repr_html"] = xr_repr_html(dsets)
            try:
                self._preload_coordinate_chunks(
                    path_id, combined_meta, dsets, ttl=expires_in
                )
            except Exception as error:
                data_logger.warning("Couldn't preload coords: %s", error)
            pkls = cloudpickle.dumps(dsets)
            step2 = time.time()
            data_logger.info("Serialisation data done within %.2f sec", step2 - step)
            step = time.time()
            data_logger.info("Ceching data")
            # We need to add the xr dataset to an extra cache entry because
            # the status_dict will be loaded by the rest-api, if the xarray
            # dataset would be present in the status_dict the rest-api
            # code would attempt to instantiate the pickled dataset object
            # and that might fail because we might or might not have xarray
            # and all the backends instantiated. Since the xarray dataset
            # object isn't needed anyway but the rest-api we simply add it
            # to a cache entry of its own.
            status_dict["status"] = StateEnum.finished_ok.value
            self.cache.setex(f"{path_id}-dset", expires_in, pkls)
            data_logger.info("Caching done within %.2f sec", time.time() - step)
        except Exception as error:
            data_logger.exception("Could not process %s: %s", path_id, error)
            status_dict["status"] = StateEnum.from_exception(error)
            status_dict["reason"] = str(error)
        self.cache.setex(
            path_id,
            expires_in,
            cloudpickle.dumps(status_dict),
        )
        data_logger.info("Task done within %.2f sec", time.time() - start)

    @background_task
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
            data_logger.debug("Encoding data for variable %s  ... ", variable)
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
            data_logger.debug("Encoding data for variable %s ... done", variable)
        except Exception as error:
            data_logger.exception(error)
            package = dict(reason=str(error), status=StateEnum.from_exception(error))
        self.cache.setex(f"{key}-{var_group}-{chunk}", 360, cloudpickle.dumps(package))

    def _cache_lookup(
        self, key: str
    ) -> Optional[Tuple[Dict[str, Any], Dict[str, xr.Dataset]]]:
        for _ in range(2):
            with self._object_cache_lock:
                cached = self._object_cache.get(key)
                if cached is not None:
                    return cached
        return None

    def load_object(self, key: str) -> Tuple[Dict[str, Any], Dict[str, xr.Dataset]]:
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
        result = self._cache_lookup(key)
        if result is not None:
            return result
        with self._object_cache_lock:
            data_logger.debug("Loading %s ...", key)
            metadata_cache = cast(Optional[bytes], self.cache.get(key))
            dset_cache = self.cache.get(f"{key}-dset")
            if metadata_cache is None or dset_cache is None:
                raise KeyError(f"{key} uuid does not exist (anymore).")
            load_dict: LoadDict = cloudpickle.loads(metadata_cache)
            dsets = cast(Dict[str, xr.Dataset], cloudpickle.loads(dset_cache))
            data_logger.debug("Loading %s ... done", key)
            result = cast(Dict[str, Any], load_dict["data"]), dsets
            self._object_cache[key] = result
        return result


class ProcessQueue(DataLoadFactory):
    """Class that can load datasets on different object stores."""

    def __init__(
        self,
        backoff_sec: float = 0.2,
        max_backoff_sec: float = 5.0,
        hostname: str = "localhost",
        **kwargs: Optional[str],
    ) -> None:
        super().__init__(hostname=hostname, **kwargs)
        self._backoff_sec = backoff_sec
        self._max_backoff_sec = max_backoff_sec

    def __enter__(self) -> "ProcessQueue":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[Any],
    ) -> None:
        self.cache.close()
        return None

    @staticmethod
    def check_for_access_permissions(username: Optional[str], paths: List[str]) -> None:
        """Check if a user is allowed to read *all* paths on the file system."""
        data_logger.debug(
            "Checking read permissions for user %s on path %s",
            username or "guest",
            ",".join(paths),
        )
        allowed = False
        try:
            allowed = all(
                [user_can_read(p, username) for p in paths if Path(p).exists()]
            )
        except Exception as error:
            data_logger.warning(
                "Could not determine file permissions for %s:\n%s",
                ",".join(paths),
                error,
            )
        if allowed is False:
            _paths = " ,".join(paths)
            raise PermissionError(f"Permission denied for{_paths}")

    def _handle_access_check(self, data: Dict[str, Any]) -> None:
        """Publish the result of a fs access check for a user."""
        try:
            self.check_for_access_permissions(
                username=data.get("username") or None, paths=data.get("paths", [])
            )
            allowed = True
        except PermissionError:
            allowed = False
        self.cache.lpush(
            f"access-reply:{data['request_id']}",
            json.dumps({"allowed": allowed}),
        )
        self.cache.expire(f"access-reply:{data['request_id']}", 30)

    def run_for_ever(self, channel: str) -> None:
        """Start the listener daemon."""
        data_logger.info("Starting data-loading daemon")
        cache_scheduler = CacheScheduler()
        data_logger.info("Broker will listen for messages now")
        while True:
            try:
                cache_scheduler.tick()
                result = cast(
                    Optional[Tuple[str, bytes]],
                    self.cache.brpop(channel, timeout=1),
                )
                if result:
                    _, body = result
                    self.redis_callback(body)
            except KeyboardInterrupt:
                break
            except RedisError:  # pragma: no cover
                time.sleep(1)  # back off and retry
            except Exception as error:
                data_logger.exception(error)

    def redis_callback(
        self,
        body: bytes,
    ) -> None:
        """Callback method to receive rabbit mq messages."""
        try:
            message = sanitize_message(json.loads(body))
        except (json.JSONDecodeError, ValueError) as exc:
            data_logger.warning("Rejected broker message: %s", exc)
            return

        if "uri" in message:
            self.spawn(
                message["uri"]["path"],
                message["uri"]["uuid"],
                assembly=message["uri"].get("assembly"),
                username=message["uri"].get("username"),
                access_pattern=message["uri"].get("access_pattern", "map"),
                map_primary_chunksize=message["uri"].get("map_primary_chunksize", 1),
                reload=message["uri"].get("reload", False),
                chunk_size=message["uri"].get("chunk_size", 16.0),
            )
        elif "chunk" in message:
            self.get_zarr_chunk(
                message["chunk"]["uuid"],
                message["chunk"]["chunk"],
                message["chunk"]["variable"],
            )
        elif "access_check" in message:
            self._handle_access_check(message["access_check"])

    def spawn(
        self,
        inp_objs: List[str],
        uuid5: str,
        assembly: Optional[Dict[str, Optional[str]]] = None,
        username: Optional[str] = None,
        access_pattern: Literal["map", "time_series"] = "map",
        map_primary_chunksize: int = 1,
        reload: bool = False,
        chunk_size: float = 16.0,
    ) -> None:
        """Submit a new data loading task to the process pool."""
        data_logger.debug("Assigning %s to %s for future processing", inp_objs, uuid5)
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
                username=username,
            )
