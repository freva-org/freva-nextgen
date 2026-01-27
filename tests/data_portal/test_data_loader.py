"""Tests for the data loader."""

import time
from base64 import b64encode
from pathlib import Path
from tempfile import TemporaryDirectory

from distributed import Client
from pytest import LogCaptureFixture

from data_portal_worker.cli import get_redis_config
from data_portal_worker.load_data import RedisCacheFactory, get_dask_client


def test_get_client() -> None:
    """Test the dask client creation."""

    assert get_dask_client(None, True) is None
    client = get_dask_client()
    assert isinstance(client, Client)
    client.shutdown()


def test_broker(caplog: LogCaptureFixture) -> None:
    """Test seding messags to the broker."""
    caplog.clear()
    cache = RedisCacheFactory()
    cache.publish("data-portal", "foo")
    time.sleep(0.5)
    assert caplog.records


def test_read_config() -> None:
    """Test the redis cache reading functionality."""
    with TemporaryDirectory() as temp_dir:
        config = get_redis_config(Path(temp_dir))
        for value in config.values():
            assert not value
        config_file = Path(temp_dir) / "foo"
        config_file.write_text("")
        config = get_redis_config(config_file)
        for value in config.values():
            assert not value
        config_file.write_bytes(b64encode('{"user": "foo"}'.encode()))
        config = get_redis_config(
            config_file, redis_user="bar", redis_password="bar"
        )
        assert config["user"] == "foo"
        assert config["passwd"] == "bar"
