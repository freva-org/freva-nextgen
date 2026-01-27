import os
from typing import Annotated, Dict, List, Optional, Union

import mock
from pytest_mock import MockerFixture

from freva_rest.cli import cli, get_cert_file


def test_get_cert_file() -> None:
    """Test getting the cerfiles."""
    out1, out2 = get_cert_file(None, None, None)
    assert out1 == out2 == ""
    out1, out2 = get_cert_file("foo", None, None)
    assert out1.startswith("foo")
    assert out2.startswith("foo")
    out1, out2 = get_cert_file("foo", None, "bar")
    assert out1.startswith("foo")
    assert out2.startswith("bar")
    out1, out2 = get_cert_file("foo", "bar", "baz")
    assert out1.startswith("bar")
    assert out2.startswith("baz")


def test_rest_cli_dev_mode(mocker: MockerFixture) -> None:
    """Test the rest api command line interface in dev mode."""
    mock_run = mocker.patch("uvicorn.run")
    cli(["--dev"])
    mock_run.assert_called_once()
    kwargs = mock_run.call_args[1]
    assert kwargs.get("log_level") == 20
    assert kwargs.get("reload") is True


def test_rest_cli_debug_mode(mocker: MockerFixture) -> None:
    """Test the rest api command line interface in debug."""
    mock_run = mocker.patch("uvicorn.run")
    cli(["--debug"])
    mock_run.assert_called_once()
    kwargs = mock_run.call_args[1]
    assert kwargs.get("reload") is False
    assert kwargs.get("log_level") == 10
    assert kwargs.get("workers") == 8


def test_cli_utils() -> None:
    """Test functionality of some cli related utils."""
    from freva_rest.cli import _dict_to_defaults, _is_type_annotation
    from freva_rest.config import env_to_int

    with mock.patch.dict(os.environ, {"DEBUG": "1"}, clear=False):
        assert env_to_int("DEBUG", 0) == 1
    assert env_to_int("DEBUG", 0) == 0

    assert _dict_to_defaults({}) == []
    assert _dict_to_defaults({"foo": "bar"}) == [("foo", "bar")]
    assert _dict_to_defaults({"foo": ["1", "2"]}) == [("foo", "1"), ("foo", "2")]

    inner_type = Union[Dict[str, str], List[str]]
    assert _is_type_annotation(inner_type, dict) is True
    assert (
        _is_type_annotation(Annotated[Optional[inner_type], "foo"], dict) is True
    )
