import logging
import os
import sys
from typing import Annotated, Dict, List, Optional, Union

import mock
import pytest
import rich.logging
from pytest_mock import MockerFixture

from freva_rest.cli import cli, get_cert_file
from freva_rest.logger import isatty, make_log_handler


class TestLogger:
    """Tests for logger helper functions."""

    def test_isatty_uses_stdout_when_env_is_unset(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Without API_USE_TTY, sys.stdout.isatty() decides."""
        monkeypatch.delenv("API_USE_TTY", raising=False)
        monkeypatch.setattr(sys.stdout, "isatty", lambda: True, raising=False)

        assert isatty() is True

        monkeypatch.setattr(sys.stdout, "isatty", lambda: False, raising=False)

        assert isatty() is False

    def test_isatty_env_overrides_stdout(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """API_USE_TTY overrides sys.stdout.isatty()."""
        monkeypatch.setattr(sys.stdout, "isatty", lambda: False, raising=False)
        monkeypatch.setenv("API_USE_TTY", "1")

        assert isatty() is True

        monkeypatch.setattr(sys.stdout, "isatty", lambda: True, raising=False)
        monkeypatch.setenv("API_USE_TTY", "0")

        assert isatty() is False

    def test_make_log_handler_returns_plain_handler_in_pytest(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """PYTEST_CURRENT_TEST=1 forces plain logs."""
        monkeypatch.setenv("PYTEST_CURRENT_TEST", "1")
        monkeypatch.delenv("API_USE_TTY", raising=False)

        handler = make_log_handler()

        assert isinstance(handler, logging.StreamHandler)
        assert not isinstance(handler, rich.logging.RichHandler)
        assert handler.formatter is not None

    def test_make_log_handler_returns_plain_handler_when_tty_is_disabled(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """API_USE_TTY=0 forces plain logs."""
        monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
        monkeypatch.setenv("API_USE_TTY", "0")

        handler = make_log_handler()

        assert isinstance(handler, logging.StreamHandler)
        assert not isinstance(handler, rich.logging.RichHandler)
        assert handler.formatter is not None

    def test_make_log_handler_plain_formatter(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Plain handler uses the expected non-Rich formatter."""
        monkeypatch.setenv("PYTEST_CURRENT_TEST", "1")

        handler = make_log_handler()
        assert handler.formatter is not None

        record = logging.LogRecord(
            name="freva-rest",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="boom",
            args=(),
            exc_info=None,
        )
        formatted = handler.formatter.format(record)

        assert "ERROR" in formatted
        assert "freva-rest - boom" in formatted

    def test_make_log_handler_returns_rich_handler_when_tty_is_enabled(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Interactive mode returns a RichHandler."""
        monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
        monkeypatch.setenv("API_USE_TTY", "1")
        monkeypatch.setattr(sys.stdout, "isatty", lambda: True, raising=False)

        handler = make_log_handler()

        assert isinstance(handler, rich.logging.RichHandler)


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
    assert _is_type_annotation(Annotated[Optional[inner_type], "foo"], dict) is True
