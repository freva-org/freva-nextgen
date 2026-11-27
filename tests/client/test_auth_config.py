"""Unit tests for the client's authentication settings (no services needed)."""

from pathlib import Path
from typing import Any, Dict, List
from unittest import mock

import pytest
from typer.testing import CliRunner

from freva_client.cli.cli_app import app as main_app
from freva_client.utils import AuthConfig

HOST = "https://freva.example.org"


def _ports_response(ports: List[int]) -> mock.MagicMock:
    resp = mock.MagicMock()
    resp.json.return_value = {"valid_ports": ports}
    return resp


class TestRedirectPorts:
    """The server's callback ports reach the login."""

    def test_ports_from_server(self) -> None:
        with mock.patch("freva_client.utils.requests.get") as get:
            get.return_value = _ports_response([8091, 8092])
            cfg = AuthConfig(HOST)
        get.assert_called_once_with(f"{HOST}/api/freva-nextgen/auth/v2/auth-ports")
        assert cfg.config.redirect_ports == [8091, 8092]

    def test_ports_do_not_pile_up(self) -> None:
        """Each instance gets its own list, not a shared default."""
        with mock.patch("freva_client.utils.requests.get") as get:
            get.return_value = _ports_response([8091])
            AuthConfig(HOST)
            get.return_value = _ports_response([9001])
            cfg = AuthConfig(HOST)
        assert cfg.config.redirect_ports == [9001]

    def test_given_ports_skip_the_request(self) -> None:
        given = [7000]
        with mock.patch("freva_client.utils.requests.get") as get:
            cfg = AuthConfig(HOST, _redirect_ports=given)
        get.assert_not_called()
        assert cfg.config.redirect_ports == [7000]
        assert given == [7000]

    def test_unreachable_server_keeps_defaults(self) -> None:
        with mock.patch("freva_client.utils.requests.get", side_effect=OSError):
            cfg = AuthConfig(HOST)
        assert cfg.config.redirect_ports  # the client library's defaults

    def test_authenticate_passes_ports(self) -> None:
        from freva_client import authenticate

        seen: Dict[str, Any] = {}

        def fake_authenticate(host: str, **kwargs: Any) -> Dict[str, str]:
            seen.update(kwargs, host=host)
            return {"access_token": "t"}

        with mock.patch("freva_client.utils.requests.get") as get:
            get.return_value = _ports_response([8091, 8092])
            with mock.patch("py_oidc_auth_client.authenticate", fake_authenticate):
                authenticate(host=HOST)
        assert seen["redirect_ports"] == [8091, 8092]
        assert seen["host"] == f"{HOST}/api/freva-nextgen"


class TestAuthCli:
    """freva-client auth options reach authenticate()."""

    @pytest.fixture()
    def runner(self) -> CliRunner:
        return CliRunner()

    def _invoke(self, runner: CliRunner, args: List[str], **env: str) -> Dict[str, Any]:
        with mock.patch(
            "freva_client.cli.auth_cli.authenticate", return_value={"access_token": "t"}
        ) as auth:
            res = runner.invoke(main_app, ["auth", "--host", HOST, *args], env=env)
        assert res.exit_code == 0, res.output
        return dict(auth.call_args.kwargs)

    def test_token_file_option(self, runner: CliRunner, tmp_path: Path) -> None:
        token_file = tmp_path / "token.json"
        kwargs = self._invoke(runner, ["--token-file", str(token_file)])
        assert kwargs["token_file"] == str(token_file)

    def test_token_file_from_env(self, runner: CliRunner, tmp_path: Path) -> None:
        token_file = str(tmp_path / "token.json")
        kwargs = self._invoke(runner, [], OIDC_TOKEN_FILE=token_file)
        assert kwargs["token_file"] == token_file

    def test_no_token_file(self, runner: CliRunner) -> None:
        kwargs = self._invoke(runner, [], OIDC_TOKEN_FILE="")
        assert kwargs["token_file"] is None

    def test_zero_timeout_waits_indefinitely(self, runner: CliRunner) -> None:
        assert self._invoke(runner, ["--timeout", "0"])["timeout"] is None
        assert self._invoke(runner, ["--timeout", "45"])["timeout"] == 45


class TestScopesAndFlow:
    """authenticate(scopes=..., flow=...) and the matching CLI options."""

    @pytest.fixture(autouse=True)
    def _ports(self) -> Any:
        with mock.patch("freva_client.utils.requests.get") as get:
            get.return_value = _ports_response([8091])
            yield

    def test_auto_passes_scopes(self) -> None:
        from freva_client import authenticate

        with mock.patch(
            "py_oidc_auth_client.authenticate", return_value={"access_token": "t"}
        ) as auth:
            authenticate(host=HOST, scopes="openid email")
        assert auth.call_args.kwargs["scopes"] == "openid email"

    @pytest.mark.parametrize("flow", ["device", "code"])
    def test_forced_flow(self, flow: str) -> None:
        from py_oidc_auth_client import CodeFlow, DeviceFlow

        from freva_client import authenticate

        cls = DeviceFlow if flow == "device" else CodeFlow
        run = mock.AsyncMock(return_value={"access_token": "t"})
        with mock.patch.object(cls, "authenticate", run):
            with mock.patch("py_oidc_auth_client.authenticate") as auto:
                token = authenticate(
                    host=HOST, scopes=["openid"], flow=flow, force=True  # type: ignore
                )
        assert token == {"access_token": "t"}
        run.assert_awaited_once_with(force=True)
        auto.assert_not_called()

    def test_forced_flow_uses_scopes_and_ports(self) -> None:
        from py_oidc_auth_client import CodeFlow

        from freva_client import authenticate

        seen: Dict[str, Any] = {}

        async def fake(self: CodeFlow, *, force: bool = False) -> Dict[str, str]:
            seen["scopes"] = self.identity.scopes
            seen["ports"] = self.provider.config.redirect_ports
            return {"access_token": "t"}

        with mock.patch.object(CodeFlow, "authenticate", fake):
            authenticate(host=HOST, scopes="email openid", flow="code")
        assert seen == {"scopes": ("email", "openid"), "ports": [8091]}

    def test_unknown_flow(self) -> None:
        from freva_client import authenticate

        with pytest.raises(ValueError, match="Unknown login flow"):
            authenticate(host=HOST, flow="exchange")  # type: ignore[arg-type]

    def test_token_strategy_is_a_strategy_name(self) -> None:
        assert AuthConfig(HOST).token_strategy in (
            "use_token",
            "refresh_token",
            "interactive_auth",
            "fail",
        )

    def test_cli_scope_and_flow(self) -> None:
        with mock.patch(
            "freva_client.cli.auth_cli.authenticate",
            return_value={"access_token": "t"},
        ) as auth:
            res = CliRunner().invoke(
                main_app,
                [
                    "auth",
                    "--host",
                    HOST,
                    "-s",
                    "openid",
                    "--scope",
                    "email profile",
                    "--flow",
                    "CODE",
                ],
            )
        assert res.exit_code == 0, res.output
        assert auth.call_args.kwargs["scopes"] == "openid email profile"
        assert auth.call_args.kwargs["flow"] == "code"

    def test_cli_defaults(self) -> None:
        with mock.patch(
            "freva_client.cli.auth_cli.authenticate",
            return_value={"access_token": "t"},
        ) as auth:
            res = CliRunner().invoke(main_app, ["auth", "--host", HOST])
        assert res.exit_code == 0, res.output
        assert auth.call_args.kwargs["scopes"] is None
        assert auth.call_args.kwargs["flow"] == "auto"
