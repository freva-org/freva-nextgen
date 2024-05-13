"""Load backend for reading different datasets."""

from base64 import b64decode
import argparse
import logging
import multiprocessing as mp
import json
import os
from socket import gethostname
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Dict, List, Literal, Optional, TypedDict, Union, cast

import appdirs  # fades appdirs
import cloudpickle  # fades cloudpickle
from dask.distributed import SSHCluster  # fades distributed
from dask.distributed import Client
from dask.distributed.deploy.cluster import Cluster
import pika  # fades pika
from pydantic import BaseModel  # fades pydantic
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

ZARR_CONSOLIDATED_FORMAT = 1
ZARR_FORMAT = 2

LoadDict = TypedDict(
    "LoadDict",
    {
        "status": Literal[0, 1, 2],
        "url": str,
        "obj": Optional[bytes],
        "obj_path": str,
        "reason": str,
        "meta": Optional[bytes],
        "json_meta": Optional[bytes],
    },
)
ClusterKw = TypedDict(
    "ClusterKw",
    {
        "hosts": List[str],
        "connect_options": List[Dict[str, str]],
    },
)
BrokerKw = TypedDict(
    "BrokerKw", {"user": str, "passwd": str, "host": str, "port": int}
)

DataLoaderConfig = TypedDict(
    "DataLoaderConfig", {"ssh_config": ClusterKw, "broker_config": BrokerKw}
)

logging.basicConfig(
    level="ERROR",
    format="%(name)s - %(asctime)s - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

data_logger = logging.getLogger(f"data-loader @ {gethostname()}")
data_logger.setLevel(logging.INFO)

CLIENT: Optional[Client] = None


class RedisCacheFactory(redis.Redis):
    """Define a custom redis cache."""

    def __init__(self, db: int = 0) -> None:
        host, _, port = (
            (os.environ.get("REDIS_HOST") or "localhost")
            .replace("redis://", "")
            .partition(":")
        )
        port_i = int(port or "6379")
        super().__init__(host=host, port=port_i, db=db)


class LoadStatus(BaseModel):
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
    obj: Optional[bytes]
    """pickled memory view of the opened dataset."""
    reason: str
    """if status = 1 reasone why opening the dataset failed."""
    meta: Optional[bytes] = None
    """Meta data of the zarr store"""
    url: str = ""
    """Url of the machine that loads the zarr store."""

    @staticmethod
    def lookup(status: int) -> str:
        _lookup = {
            0: "finished, ok",
            1: "finished, failed",
            2: "waiting",
            3: "processing",
        }
        return _lookup.get(status, "unkown")

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


def get_dask_client(
    cluster_kw: ClusterKw, client: Optional[Client] = CLIENT
) -> Client:
    """Get or create a cached dask cluster."""
    if client is None:
        print_kw = cluster_kw.copy()
        for num, opt in enumerate(cluster_kw["connect_options"]):
            print_kw["connect_options"][num]["password"] = "***"
        data_logger.debug("setting up ssh cluster with %s", print_kw)
        client = Client(SSHCluster(**cluster_kw))
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

    def __init__(
        self, scheme: str, cache: Optional[redis.Redis] = None
    ) -> None:
        self.cache = cache or RedisCacheFactory(0)
        implemented_methods = {"file": self.from_posix, "": self.from_posix}
        try:
            self.read = implemented_methods[scheme.lower()]
        except KeyError:
            raise NotImplementedError(f"datasets on {scheme} can't be loaded")

    @staticmethod
    def from_posix(input_path: str) -> zarr.storage.BaseStore:
        """Open a dataset with xarray."""
        return xr.open_dataset(
            input_path,
            decode_cf=False,
            use_cftime=False,
            chunks="auto",
            cache="False",
            decode_coords=False,
        )

    @classmethod
    def from_object_path(
        cls, input_path: str, path_id: str, cache: Optional[redis.Redis]
    ) -> None:
        """Create a zarr object from an input path."""
        cache = cache or RedisCacheFactory(0)
        parsed_url = urlparse(input_path)
        status_dict = LoadStatus.from_dict(
            cast(
                Optional[LoadDict],
                cloudpickle.loads(cache.get(path_id)),
            )
        ).dict()
        expires_in = int(os.environ.get("API_CACHE_EXP") or "3600")
        status_dict["status"] = 3
        cache.setex(path_id, expires_in, cloudpickle.dumps(status_dict))
        data_logger.debug("Reading %s", input_path)
        try:
            read_instance = cls(parsed_url.scheme, cache=cache)
            dset = read_instance.read(input_path)
            metadata = create_zmetadata(dset)
            status_dict["obj"] = cloudpickle.dumps(dset)
            status_dict["meta"] = cloudpickle.dumps(create_zmetadata(dset))
            status_dict["json_meta"] = cloudpickle.dumps(
                jsonify_zmetadata(dset, metadata)
            )
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

    @staticmethod
    def get_zarr_chunk(
        key: str,
        chunk: str,
        variable: str,
        cache: Optional[redis.Redis] = None,
    ) -> bytes:
        """Read the zarr metadata from the cache."""

        cache = cache or RedisCacheFactory(0)
        data_cache = cache.get(key)
        if data_cache is None:
            raise KeyError(f"{key} uuid does not exist (anymore).")
        pickle_data: LoadDict = cloudpickle.loads(data_cache)
        task_status = pickle_data.get("status", 1)
        if task_status != 0:
            raise RuntimeError(LoadStatus.lookup(task_status))
        dset = cloudpickle.loads(pickle_data["obj"])
        meta = cloudpickle.loads(pickle_data["meta"])
        arr_meta = meta["metadata"][f"{variable}/{array_meta_key}"]
        data = encode_chunk(
            get_data_chunk(
                encode_zarr_variable(
                    dset.variables[variable], name=variable
                ).data,
                chunk,
                out_shape=arr_meta["chunks"],
            ).tobytes(),
            filters=meta["filters"],
            compressor=meta["compressor"],
        )
        cache.setex(f"{key}-{variable}-{chunk}", data, 60)

    @staticmethod
    def load_dataset(
        key: str, cache: Optional[redis.Redis] = None
    ) -> xr.Dataset:
        """Look up a zarr store in the redis cache."""
        cache = cache or RedisCacheFactory(0)
        data_cache = RedisCache.get(key)
        if data_cache is None:
            raise KeyError(f"{key} uuid does not exist (anymore).")
        pickle_data: LoadDict = cloudpickle.loads(cache)
        task_status = pickle_data.get("status", 1)
        if task_status != 0:
            raise RuntimeError(LoadStatus.lookup(task_status))
        return cloudpickle.loads(pickle_data["obj"])


class ProcessQueue:
    """Class that can load datasets on different object stores."""

    def __init__(
        self,
        rest_url: str,
        cluster_kw: ClusterKw,
        redis_cache: Optional[redis.Redis] = None,
    ) -> None:
        self.redis_cache = redis_cache or RedisCacheFactory(0)
        self.client = get_dask_client(cluster_kw)
        self.rest_url = rest_url
        self.cluster_kw = cluster_kw

    def run_for_ever(
        self,
        queue: str,
        config: BrokerKw,
    ) -> None:
        """Start the listner deamon."""
        data_logger.info("Starting data-loading deamon")
        data_logger.debug(
            "Connecting to broker on host %s via port %i",
            config["host"],
            config["port"],
        )
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=config["host"],
                port=config["port"],
                credentials=pika.PlainCredentials(
                    username=config["user"], password=config["passwd"]
                ),
            )
        )
        self.channel = connection.channel()
        self.channel.queue_declare(queue=queue)
        self.channel.basic_consume(
            queue=queue,
            on_message_callback=self.rabbit_mq_callback,
            auto_ack=True,
        )
        data_logger.info("Broker will listen for messages now")
        self.channel.start_consuming()

    def rabbit_mq_callback(
        self,
        ch: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        """Callback method to recieve rabbit mq messages."""
        try:
            message = json.loads(body)
            if "uri" in message:
                self.spawn(message["uri"]["path"], message["uri"]["uuid"])
            elif "chunk" in message:
                DataLoadFactory.get_zarr_chunk(
                    message["chunk"]["uuid"], message["chunk"]["chunk_key"]
                )
        except json.JSONDecodeError:
            data_logger.warning("could not decode message")
            pass

    def spawn(self, inp_obj: str, uuid5: str) -> str:
        """Subumit a new data loading task to the process pool."""
        data_logger.debug(
            "Assigning %s to %s for future processing", inp_obj, uuid5
        )
        cache: Optional[bytes] = self.redis_cache.get(uuid5)
        status_dict: LoadDict = {
            "status": 2,
            "obj_path": f"{self.rest_url}/api/freva-data-portal/zarr/{uuid5}",
            "obj": None,
            "reason": "",
            "url": "",
            "meta": None,
            "json_meta": None,
        }
        if cache is None:
            self.redis_cache.setex(
                uuid5,
                os.environ.get("API_CACHE_EXP") or 3600,
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


def cli(config_file: Path, argv: Optional[List[str]] = None) -> None:
    """Command line interface for starting the data loader."""
    redis_host, _, redis_port = (
        (os.environ.get("REDIS_HOST") or "localhost")
        .replace("redis://", "")
        .partition(":")
    )
    redis_port = redis_port or "6379"
    parser = argparse.ArgumentParser(
        prog="Data Loder",
        description=("Starts the data loading service."),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-e",
        "--exp",
        type=int,
        help="Set the expiry time of the redis cache.",
        default=os.environ.get("API_CACHE_EXP") or "3600",
    )
    parser.add_argument(
        "-r",
        "--redis-host",
        type=str,
        help="Host:Port of the redis cache.",
        default=f"redis://{redis_host}:{redis_port}",
    )
    parser.add_argument(
        "-a",
        "--api-url",
        type=str,
        help="Host:Port for the databrowser api",
        default=os.environ.get("API_URL"),
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        help="API port, only used if --api-host not set.",
        default=8080,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Display debug messages.",
        default=False,
    )
    args = parser.parse_args(argv)
    if args.verbose is True:
        data_logger.setLevel(logging.DEBUG)
    data_logger.debug("Loading cluster config from %s", config_file)
    config: DataLoaderConfig = json.loads(b64decode(config_file.read_bytes()))
    data_logger.debug("Deleting cluster config file %s", config_file)
    env = os.environ.copy()
    try:
        os.environ["API_CACHE_EXP"] = str(args.exp)
        os.environ["REDIS_HOST"] = args.redis_host
        data_logger.debug("Starting data-loader process")
        broker = ProcessQueue(
            args.api_url or f"http://localhost:{args.port}",
            config["ssh_config"],
        )
        broker.run_for_ever(
            "data-portal",
            config["broker_config"],
        )
    except KeyboardInterrupt:
        pass
    finally:
        if CLIENT is not None:
            CLIENT.shutdown()
        os.environ = env


if __name__ == "__main__":
    config_file = (
        Path(appdirs.user_cache_dir()) / "data-portal-cluster-config.json"
    )
    cli(config_file)
