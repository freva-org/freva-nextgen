"""Definitions to setup dask clusters."""

from typing import Optional

from dask.distributed import LocalCluster
from freva_rest import DASK_CLUSTER
from freva_utils import RedisCache
from dask.distributed import Client
from dask.distributed.deploy.cluster import Cluster


async def get_or_create_cluster() -> Client:
    """Get or create a cached dask cluster."""
    cluster: Optional[Cluster] = RedisCache.get("cluster")
    if cluster is None:
        cluster = LocalCluster()
        RedisCache.set("cluster", cluster, nx=True)
    return Client(cluster)
