"""Tests for data_portal_worker.cli."""

import json
import logging
import os
import signal
from base64 import b64encode
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from data_portal_worker.cli import (
    _load_wrapper,
    _main,
    _set_loglevel_from_verbosity,
    _sigterm_handler,
    daemon,
    get_redis_config,
    read_file_content,
    run_data_loader,
)
from data_portal_worker.utils import DEFAULT_LOG_LEVEL

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_b64_config(
    user: str = "ruser",
    passwd: str = "rpass",
    ssl_cert: str = "",
    ssl_key: str = "",
) -> bytes:
    """Return a valid base64-encoded JSON loader config."""
    return b64encode(
        json.dumps(
            {"user": user, "passwd": passwd, "ssl_cert": ssl_cert, "ssl_key": ssl_key}
        ).encode()
    )


def _mock_queue(mocker: MockerFixture) -> MagicMock:
    """Patch ProcessQueue and return the object bound by ``as queue``."""
    mock_cls = mocker.patch("data_portal_worker.cli.ProcessQueue")
    return mock_cls.return_value.__enter__.return_value


# ---------------------------------------------------------------------------
# _sigterm_handler
# ---------------------------------------------------------------------------


class TestSigtermHandler:
    def test_raises_keyboard_interrupt(self) -> None:
        with pytest.raises(KeyboardInterrupt, match="SIGTERM received"):
            _sigterm_handler(signal.SIGTERM, None)

    def test_message_contains_sigterm_received(self) -> None:
        with pytest.raises(KeyboardInterrupt, match="SIGTERM received"):
            _sigterm_handler(signal.SIGTERM, None)


# ---------------------------------------------------------------------------
# read_file_content
# ---------------------------------------------------------------------------


class TestReadFileContent:
    def test_none_returns_empty_string(self) -> None:
        assert read_file_content(None) == ""

    def test_reads_existing_file(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.txt"
        f.write_text("hello")
        assert read_file_content(f) == "hello"

    def test_missing_file_returns_empty_string(self, tmp_path: Path) -> None:
        assert read_file_content(tmp_path / "ghost.txt") == ""

    def test_permission_error_returns_empty_string(self, tmp_path: Path) -> None:
        f = tmp_path / "locked.txt"
        f.write_text("secret")
        f.chmod(0o000)
        try:
            assert read_file_content(f) == ""
        finally:
            f.chmod(0o644)


# ---------------------------------------------------------------------------
# get_redis_config
# ---------------------------------------------------------------------------


class TestGetRedisConfig:
    def test_reads_user_and_passwd_from_config_file(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.json"
        f.write_bytes(_make_b64_config(user="alice", passwd="s3cr3t"))
        cfg = get_redis_config(config_file=f)
        assert cfg["user"] == "alice"
        assert cfg["passwd"] == "s3cr3t"

    def test_falls_back_to_kwargs_when_no_file(self) -> None:
        cfg = get_redis_config(redis_user="bob", redis_password="pw")
        assert cfg["user"] == "bob"
        assert cfg["passwd"] == "pw"

    def test_config_file_takes_precedence_over_kwargs(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.json"
        f.write_bytes(_make_b64_config(user="from_file"))
        cfg = get_redis_config(config_file=f, redis_user="from_kwarg")
        assert cfg["user"] == "from_file"

    def test_falls_back_to_kwargs_on_invalid_base64(self, tmp_path: Path) -> None:
        f = tmp_path / "cfg.json"
        f.write_text("not-valid-base64!!!")
        cfg = get_redis_config(config_file=f, redis_user="fallback")
        assert cfg["user"] == "fallback"

    def test_ssl_cert_content_read_from_path(self, tmp_path: Path) -> None:
        cert = tmp_path / "cert.pem"
        cert.write_text("CERT_DATA")
        cfg = get_redis_config(redis_ssl_certfile=str(cert))
        assert cfg["ssl_cert"] == "CERT_DATA"

    def test_missing_config_file_yields_empty_credentials(self, tmp_path: Path) -> None:
        cfg = get_redis_config(config_file=tmp_path / "missing.json")
        assert cfg["user"] == ""
        assert cfg["passwd"] == ""

    def test_ssl_cert_in_config_file_not_overwritten_by_kwarg(
        self, tmp_path: Path
    ) -> None:
        f = tmp_path / "cfg.json"
        f.write_bytes(_make_b64_config(ssl_cert="FILE_CERT"))
        kwarg_cert = tmp_path / "other.pem"
        kwarg_cert.write_text("KWARG_CERT")
        cfg = get_redis_config(config_file=f, redis_ssl_certfile=str(kwarg_cert))
        assert cfg["ssl_cert"] == "FILE_CERT"


# ---------------------------------------------------------------------------
# _set_loglevel_from_verbosity
# ---------------------------------------------------------------------------


class TestSetLoglevelFromVerbosity:
    def test_zero_returns_default_level(self) -> None:
        result = _set_loglevel_from_verbosity(0)
        assert result == logging.getLevelName(DEFAULT_LOG_LEVEL)

    def test_one_returns_info(self) -> None:
        assert _set_loglevel_from_verbosity(1) == "INFO"

    def test_two_returns_debug(self) -> None:
        assert _set_loglevel_from_verbosity(2) == "DEBUG"

    def test_above_two_clamped_to_debug(self) -> None:
        assert _set_loglevel_from_verbosity(99) == "DEBUG"


# ---------------------------------------------------------------------------
# _main
# ---------------------------------------------------------------------------


class TestMain:
    def test_installs_sigterm_handler(self, mocker: MockerFixture) -> None:
        mock_signal = mocker.patch("data_portal_worker.cli.signal.signal")
        _mock_queue(mocker)

        _main()

        mock_signal.assert_called_once_with(signal.SIGTERM, _sigterm_handler)

    def test_redis_env_vars_set_from_arguments(self, mocker: MockerFixture) -> None:
        mocker.patch("data_portal_worker.cli.signal.signal")
        _mock_queue(mocker)
        mocker.patch.dict(os.environ, {})

        _main(redis_host="redis://testhost:9999", exp=7200)

        assert os.environ["API_REDIS_HOST"] == "redis://testhost:9999"
        assert os.environ["API_CACHE_EXP"] == "7200"

    def test_process_queue_created_with_correct_dev_mode(
        self, mocker: MockerFixture
    ) -> None:
        mocker.patch("data_portal_worker.cli.signal.signal")
        mock_cls = mocker.patch("data_portal_worker.cli.ProcessQueue")
        mock_cls.return_value.__enter__.return_value = MagicMock()

        _main()

        mock_cls.assert_called_once_with(
            **{
                "hostname": "localhost",
                "password": None,
                "ssl_certfile": None,
                "ssl_keyfile": None,
                "username": None,
            }
        )

    def test_run_for_ever_called_with_data_portal_channel(
        self, mocker: MockerFixture
    ) -> None:
        mocker.patch("data_portal_worker.cli.signal.signal")
        queue = _mock_queue(mocker)

        _main()

        queue.run_for_ever.assert_called_once_with("data-portal")

    def test_ssl_env_vars_set_when_certs_provided(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        mocker.patch("data_portal_worker.cli.signal.signal")
        _mock_queue(mocker)
        mocker.patch.dict(os.environ, {})

        cert = tmp_path / "cert.pem"
        key = tmp_path / "key.pem"
        cert.write_text("CERT")
        key.write_text("KEY")

        _main(redis_ssl_certfile=str(cert), redis_ssl_keyfile=str(key))

        assert "API_REDIS_SSL_CERTFILE" in os.environ
        assert "API_REDIS_SSL_KEYFILE" in os.environ

    def test_ssl_cert_files_written_with_mode_600(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        mocker.patch("data_portal_worker.cli.signal.signal")
        _mock_queue(mocker)

        cert = tmp_path / "cert.pem"
        key = tmp_path / "key.pem"
        cert.write_text("CERT")
        key.write_text("KEY")

        chmod_calls: list[int] = []
        real_chmod = Path.chmod

        def tracking_chmod(self: Path, mode: int, **kw: object) -> None:
            chmod_calls.append(mode)
            real_chmod(self, mode, **kw)

        mocker.patch.object(Path, "chmod", tracking_chmod)
        _main(redis_ssl_certfile=str(cert), redis_ssl_keyfile=str(key))

        assert chmod_calls, "chmod was never called"
        assert all(m == 0o600 for m in chmod_calls)

    def test_ssl_branch_skipped_when_no_certs(self, mocker: MockerFixture) -> None:
        mocker.patch("data_portal_worker.cli.signal.signal")
        _mock_queue(mocker)
        mock_chmod = mocker.patch.object(Path, "chmod")

        _main()

        mock_chmod.assert_not_called()


# ---------------------------------------------------------------------------
# _load_wrapper
# ---------------------------------------------------------------------------


class TestLoadWrapper:
    def test_calls_watchfiles_run_process(self, mocker: MockerFixture) -> None:
        mock_run_process = mocker.patch("watchfiles.run_process")

        _load_wrapper(Path("/cfg"), dev=True, log_level="DEBUG")

        mock_run_process.assert_called_once()

    def test_passes_config_file_as_arg(self, mocker: MockerFixture) -> None:
        mock_run_process = mocker.patch("watchfiles.run_process")

        _load_wrapper(Path("/some/config.json"), dev=True)

        _, call_kwargs = mock_run_process.call_args
        assert call_kwargs["args"] == (Path("/some/config.json"),)

    def test_passes_kwargs_through(self, mocker: MockerFixture) -> None:
        mock_run_process = mocker.patch("watchfiles.run_process")

        _load_wrapper(Path("/cfg"), dev=True, log_level="INFO", exp=7200)

        _, call_kwargs = mock_run_process.call_args
        assert call_kwargs["kwargs"]["log_level"] == "INFO"
        assert call_kwargs["kwargs"]["exp"] == 7200

    def test_flushes_logging_handlers_on_exit(self, mocker: MockerFixture) -> None:
        mocker.patch("watchfiles.run_process")

        # level must be a real int — logging machinery compares it numerically
        handler = MagicMock()
        handler.level = logging.WARNING

        logging.root.addHandler(handler)
        try:
            _load_wrapper(Path("/cfg"), dev=True)
        finally:
            if handler in logging.root.handlers:
                logging.root.removeHandler(handler)

        handler.flush.assert_called_once()
        handler.close.assert_called_once()


# ---------------------------------------------------------------------------
# daemon
# ---------------------------------------------------------------------------


class TestDaemon:
    def _make_proc(
        self, *, exitcode: int = 0, alive: bool = False, name: str = "worker-0"
    ) -> MagicMock:
        p = MagicMock()
        p.exitcode = exitcode
        p.is_alive.return_value = alive
        p.name = name
        return p

    def test_all_clean_exits_terminates_loop(self, mocker: MockerFixture) -> None:
        """Every slot becomes None → while condition is False → loop exits."""
        mocker.patch("data_portal_worker.cli.time.sleep")
        clean = self._make_proc(exitcode=0, alive=False)
        mocker.patch("data_portal_worker.cli.Process", return_value=clean)

        daemon(Path("/cfg"), 2)

        assert clean.start.call_count == 2

    def test_crashed_worker_is_restarted(self, mocker: MockerFixture) -> None:
        """Non-zero exit code triggers a new Process in the same slot."""
        mocker.patch("data_portal_worker.cli.time.sleep")

        crashed = self._make_proc(exitcode=1, alive=False, name="worker-0")
        replacement = self._make_proc(exitcode=0, alive=False, name="worker-0")

        mock_cls = mocker.patch(
            "data_portal_worker.cli.Process", side_effect=[crashed, replacement]
        )

        daemon(Path("/cfg"), 1)

        assert mock_cls.call_count == 2
        crashed.start.assert_called_once()
        replacement.start.assert_called_once()

    def test_clean_worker_not_restarted(self, mocker: MockerFixture) -> None:
        """exitcode=0 sets the slot to None; no new Process is ever created."""
        mocker.patch("data_portal_worker.cli.time.sleep")
        clean = self._make_proc(exitcode=0, alive=False)
        mock_cls = mocker.patch("data_portal_worker.cli.Process", return_value=clean)

        daemon(Path("/cfg"), 1)

        assert mock_cls.call_count == 1

    def test_keyboard_interrupt_terminates_and_joins_workers(
        self, mocker: MockerFixture
    ) -> None:
        """KeyboardInterrupt causes terminate() + join() on every live process."""
        alive = self._make_proc(exitcode=None, alive=True)
        # call 1: in loop body (True → skip); call 2: after join (False → no kill)
        alive.is_alive.side_effect = [True, False]

        mocker.patch("data_portal_worker.cli.Process", return_value=alive)
        mocker.patch("data_portal_worker.cli.time.sleep", side_effect=KeyboardInterrupt)

        daemon(Path("/cfg"), 1)

        alive.terminate.assert_called_once()
        alive.join.assert_called()
        alive.kill.assert_not_called()

    def test_stuck_process_is_killed_after_join_timeout(
        self, mocker: MockerFixture
    ) -> None:
        """Process still alive after join(timeout=10) receives kill()."""
        stuck = self._make_proc(exitcode=None, alive=True)
        stuck.is_alive.side_effect = [True, True]

        mocker.patch("data_portal_worker.cli.Process", return_value=stuck)
        mocker.patch("data_portal_worker.cli.time.sleep", side_effect=KeyboardInterrupt)

        daemon(Path("/cfg"), 1)

        stuck.kill.assert_called_once()

    def test_none_slot_not_terminated_on_interrupt(self, mocker: MockerFixture) -> None:
        """A slot set to None (clean exit) is never passed to terminate()."""
        mocker.patch("data_portal_worker.cli.time.sleep", side_effect=KeyboardInterrupt)
        clean = self._make_proc(exitcode=0, alive=False)
        mocker.patch("data_portal_worker.cli.Process", return_value=clean)

        daemon(Path("/cfg"), 1)

        clean.terminate.assert_not_called()

    def test_logging_handlers_flushed_in_finally(self, mocker: MockerFixture) -> None:
        """finally block flushes and removes every root logging handler."""
        mocker.patch("data_portal_worker.cli.time.sleep")
        clean = self._make_proc(exitcode=0, alive=False)
        mocker.patch("data_portal_worker.cli.Process", return_value=clean)

        # level must be a real int — logging machinery compares it numerically
        handler = MagicMock()
        handler.level = logging.WARNING

        logging.root.addHandler(handler)
        try:
            daemon(Path("/cfg"), 1)
        finally:
            if handler in logging.root.handlers:
                logging.root.removeHandler(handler)

        handler.flush.assert_called_once()
        handler.close.assert_called_once()


# ---------------------------------------------------------------------------
# run_data_loader
# ---------------------------------------------------------------------------


class TestRunDataLoader:
    def test_dev_mode_calls_load_wrapper(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        mock_wrapper = mocker.patch("data_portal_worker.cli._load_wrapper")
        mocker.patch("data_portal_worker.cli.daemon")

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(["-c", tmp.name, "--dev"])

        mock_wrapper.assert_called_once()

    def test_dev_mode_does_not_call_daemon(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        mocker.patch("data_portal_worker.cli._load_wrapper")
        mock_daemon = mocker.patch("data_portal_worker.cli.daemon")

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(["-c", tmp.name, "--dev"])

        mock_daemon.assert_not_called()

    def test_non_dev_mode_calls_daemon(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        mock_daemon = mocker.patch("data_portal_worker.cli.daemon")
        mocker.patch("data_portal_worker.cli._load_wrapper")

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(["-c", tmp.name])

        mock_daemon.assert_called_once()

    def test_non_dev_mode_does_not_call_load_wrapper(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        mocker.patch("data_portal_worker.cli.daemon")
        mock_wrapper = mocker.patch("data_portal_worker.cli._load_wrapper")

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(["-c", tmp.name])

        mock_wrapper.assert_not_called()

    def test_num_proc_passed_as_positional_to_daemon(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        mock_daemon = mocker.patch("data_portal_worker.cli.daemon")

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(["-c", tmp.name, "--num-proc", "3"])

        _cfg, num_proc = mock_daemon.call_args[0]
        assert num_proc == 3

    def test_exp_and_redis_host_passed_as_kwargs(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        mock_daemon = mocker.patch("data_portal_worker.cli.daemon")

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(
                ["-c", tmp.name, "-e", "10", "-r", "redis://example.com:1234"]
            )

        kwargs = mock_daemon.call_args[1]
        assert kwargs["exp"] == 10
        assert kwargs["redis_host"] == "redis://example.com:1234"

    def test_single_verbose_flag_produces_info_level(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        mock_daemon = mocker.patch("data_portal_worker.cli.daemon")

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(["-c", tmp.name, "-v"])

        assert mock_daemon.call_args[1]["log_level"] == "INFO"

    def test_double_verbose_flag_produces_debug_level(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        mock_daemon = mocker.patch("data_portal_worker.cli.daemon")

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(["-c", tmp.name, "-vv"])

        assert mock_daemon.call_args[1]["log_level"] == "DEBUG"

    def test_num_proc_default_capped_at_eight(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        mock_daemon = mocker.patch("data_portal_worker.cli.daemon")
        mocker.patch("os.cpu_count", return_value=64)

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(["-c", tmp.name])

        _cfg, num_proc = mock_daemon.call_args[0]
        assert num_proc <= 8

    def test_dev_mode_forces_single_process(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        """--dev always routes to _load_wrapper (single process), never daemon."""
        mock_wrapper = mocker.patch("data_portal_worker.cli._load_wrapper")
        mock_daemon = mocker.patch("data_portal_worker.cli.daemon")

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(["-c", tmp.name, "--dev", "--num-proc", "8"])

        mock_wrapper.assert_called_once()
        mock_daemon.assert_not_called()

    def test_redis_credentials_forwarded_to_daemon(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        mock_daemon = mocker.patch("data_portal_worker.cli.daemon")

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(
                [
                    "-c",
                    tmp.name,
                    "--redis-username",
                    "alice",
                    "--redis-password",
                    "pw",
                ]
            )

        kwargs = mock_daemon.call_args[1]
        assert kwargs["redis_user"] == "alice"
        assert kwargs["redis_password"] == "pw"

    def test_dev_false_forwarded_in_kwargs(
        self, mocker: MockerFixture, loader_config: bytes
    ) -> None:
        mock_daemon = mocker.patch("data_portal_worker.cli.daemon")

        with NamedTemporaryFile(suffix=".json") as tmp:
            Path(tmp.name).write_bytes(loader_config)
            run_data_loader(["-c", tmp.name])

        assert "dev" not in mock_daemon.call_args[1]
