"""Tests for the databrowser command line interface.

These tests exercise the CLI commands for data search, metadata search,
data count, intake/STAC catalogues, and custom flavour management.

Authentication is handled via ``--token-file`` in CLI commands, which
loads tokens from a JSON file and puts them into the TokenStore. For
operations that also need the databrowser internals to be authenticated,
the ``mock_authenticate`` fixture patches ``py_oidc_auth_client.authenticate``.
"""

import json
import subprocess
import zipfile
from pathlib import Path
from tempfile import NamedTemporaryFile

from py_oidc_auth_client import Token
from pytest import LogCaptureFixture
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from freva_client.cli.cli_app import app as main_app
from freva_client.cli.databrowser_cli import databrowser_app as app


class TestOverview:
    """Tests for the data-overview CLI sub-command."""

    def test_overview(self, cli_runner: CliRunner, test_server: str) -> None:
        """The data-overview command should succeed and produce output."""
        res = cli_runner.invoke(app, ["data-overview", "--host", test_server])
        assert res.exit_code == 0
        assert res.stdout


class TestDataSearch:
    """Tests for the data-search CLI sub-command."""

    def test_search_files_normal(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """Searching for files without zarr should succeed."""
        res = cli_runner.invoke(app, ["data-search", "--host", test_server])
        assert res.exit_code == 0
        assert res.stdout

    def test_search_no_results(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """Searching with non-matching constraints gives no output."""
        res = cli_runner.invoke(
            app,
            [
                "data-search",
                "--host",
                test_server,
                "project=cmip6",
                "project=bar",
                "model=foo",
            ],
        )
        assert res.exit_code == 0
        assert not res.stdout

    def test_search_json_output(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """JSON output should be a valid list."""
        res = cli_runner.invoke(
            app, ["data-search", "--host", test_server, "--json"]
        )
        assert res.exit_code == 0
        assert isinstance(json.loads(res.stdout), list)

    def test_search_zarr_without_auth_fails(
        self,
        cli_runner: CliRunner,
        test_server: str,
        mock_authenticate_fail: None,
    ) -> None:
        """Zarr search without authentication should fail."""
        res = cli_runner.invoke(
            app, ["data-search", "--host", test_server, "--zar"]
        )
        assert res.exit_code > 0

    def test_search_zarr_with_token_file(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """Zarr search with a token file should succeed."""
        res = cli_runner.invoke(
            app,
            [
                "data-search",
                "--host",
                test_server,
                "--zarr",
                "--token-file",
                str(token_file),
                "dataset=cmip6-fs",
                "--json",
            ],
        )
        assert res.exit_code == 0
        assert res.stdout
        assert isinstance(json.loads(res.stdout), list)

    def test_search_zarr_without_token_file_fails(
        self,
        cli_runner: CliRunner,
        test_server: str,
        mock_authenticate_fail: None,
    ) -> None:
        """Zarr search without a token file should fail."""
        res = cli_runner.invoke(
            app,
            [
                "data-search",
                "--host",
                test_server,
                "--zarr",
                "dataset=cmip6-fs",
                "--json",
            ],
        )
        assert res.exit_code != 0


class TestIntakeCatalogue:
    """Tests for the intake-catalogue CLI sub-command."""

    def test_intake_no_zarr(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """intake-catalogue without zarr should produce valid JSON."""
        res = cli_runner.invoke(app, ["intake-catalogue", "--host", test_server])
        assert res.exit_code == 0
        assert res.stdout
        assert isinstance(json.loads(res.stdout), dict)

    def test_intake_to_file(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """intake-catalogue with -f flag should write a JSON file."""
        with NamedTemporaryFile(suffix=".json") as temp_f:
            res = cli_runner.invoke(
                app,
                ["intake-catalogue", "--host", test_server, "-f", temp_f.name],
            )
            assert res.exit_code == 0
            with open(temp_f.name, "r") as stream:
                assert isinstance(json.load(stream), dict)

    def test_intake_zarr_with_token(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """intake-catalogue with zarr and token-file should succeed."""
        res = cli_runner.invoke(
            app,
            [
                "intake-catalogue",
                "--host",
                test_server,
                "--zarr",
                "--token-file",
                str(token_file),
                "dataset=cmip6-fs",
            ],
        )
        assert res.exit_code == 0
        assert res.stdout
        assert isinstance(json.loads(res.stdout), dict)


class TestStacCatalogue:
    """Tests for the stac-catalogue CLI sub-command."""

    def test_stac_catalogue_to_file(
        self,
        cli_runner: CliRunner,
        test_server: str,
        temp_dir: Path,
        mock_authenticate: Token,
    ) -> None:
        """Creating a STAC catalogue to a zip should validate items."""
        output_file = temp_dir / "something.zip"
        res = cli_runner.invoke(
            app,
            [
                "stac-catalogue",
                "--host",
                test_server,
                "--filename",
                output_file,
            ],
        )
        assert res.exit_code == 0
        with zipfile.ZipFile(output_file, "r") as zip_file:
            for member in zip_file.namelist():
                if "/items/" in member and member.endswith(".json"):
                    item_content = zip_file.read(member)
                    temp_item_path = temp_dir / "test_item.json"
                    temp_item_path.write_bytes(item_content)
                    res_stac = subprocess.run(
                        ["stac-check", str(temp_item_path)],
                        check=True,
                        capture_output=True,
                    )
                    assert "ITEM Passed: True" in res_stac.stdout.decode("utf-8")
                    break

    def test_stac_bad_output_path(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """STAC catalogue with an invalid output path should fail."""
        res = cli_runner.invoke(
            app,
            [
                "stac-catalogue",
                "--host",
                test_server,
                "--filename",
                "/foo/bar",
            ],
        )
        assert res.exit_code == 1

    def test_stac_bad_search_params(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """STAC catalogue with wrong search params should fail."""
        res = cli_runner.invoke(
            app,
            [
                "stac-catalogue",
                "--host",
                test_server,
                "--filename",
                "/foo/bar" "foo=b",
            ],
        )
        assert res.exit_code == 1

    def test_stac_no_filename(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """STAC catalogue without explicit filename should succeed."""
        res = cli_runner.invoke(app, ["stac-catalogue", "--host", test_server])
        assert res.exit_code == 0
        assert (
            "Downloading the STAC catalog started ...\nSTAC catalog saved to: "
            in res.stdout
        )

    def test_stac_with_nonexistent_flavour(
        self,
        cli_runner: CliRunner,
        test_server: str,
        temp_dir: Path,
        mock_authenticate: Token,
    ) -> None:
        """STAC catalogue with a non-existent flavour should fail."""
        output_file = temp_dir / "something.zip"
        res = cli_runner.invoke(
            app,
            [
                "stac-catalogue",
                "--host",
                test_server,
                "--filename",
                output_file,
                "--flavour",
                "cmip69",
            ],
        )
        assert res.exit_code == 1


class TestMetadataSearch:
    """Tests for the metadata-search CLI sub-command."""

    def test_metadata_search_basic(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """metadata-search should produce output."""
        res = cli_runner.invoke(app, ["metadata-search", "--host", test_server])
        assert res.exit_code == 0
        assert res.stdout

    def test_metadata_search_with_filter(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """metadata-search with a model filter."""
        res = cli_runner.invoke(
            app, ["metadata-search", "--host", test_server, "model=bar"]
        )
        assert res.exit_code == 0
        assert res.stdout

    def test_metadata_search_json(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """JSON output should be a valid dict."""
        res = cli_runner.invoke(
            app, ["metadata-search", "--host", test_server, "--json"]
        )
        assert res.exit_code == 0
        output = json.loads(res.stdout)
        assert isinstance(output, dict)

    def test_metadata_search_json_filtered(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """JSON output with filter should be a dict."""
        res = cli_runner.invoke(
            app,
            ["metadata-search", "--host", test_server, "--json", "model=b"],
        )
        assert res.exit_code == 0
        assert isinstance(json.loads(res.stdout), dict)

    def test_metadata_search_cmor_table(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """metadata-search with cmor_table facet."""
        res = cli_runner.invoke(
            app,
            ["metadata-search", "--host", test_server, "cmor_table=inst"],
        )
        assert res.exit_code == 0

    def test_metadata_search_with_custom_flavour(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """metadata-search with a custom flavour should work."""
        # add the flavour
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "add",
                "test_cli_flavour",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--map",
                "project=projekt",
                "--map",
                "variable=var",
            ],
        )
        assert res.exit_code == 0

        # use it in metadata-search
        res = cli_runner.invoke(
            app,
            [
                "metadata-search",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--flavour",
                "test_cli_flavour",
                "--json",
            ],
        )
        assert res.exit_code == 0

        # wrong username:flavour should produce stderr
        res = cli_runner.invoke(
            app,
            [
                "metadata-search",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--flavour",
                "janedoexx:test_cli_fla",
                "--json",
            ],
        )
        assert res.stderr

        # right username:flavour
        res = cli_runner.invoke(
            app,
            [
                "metadata-search",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--flavour",
                "janedoe:test_cli_flavour",
                "--json",
            ],
        )
        assert res.exit_code == 0
        assert isinstance(json.loads(res.stdout), dict)

        # cleanup
        cli_runner.invoke(
            app,
            [
                "flavour",
                "delete",
                "test_cli_flavour",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )


class TestCountValues:
    """Tests for the data-count CLI sub-command."""

    def test_count_basic(self, cli_runner: CliRunner, test_server: str) -> None:
        """data-count should succeed and produce output."""
        res = cli_runner.invoke(app, ["data-count", "--host", test_server])
        assert res.exit_code == 0
        assert res.stdout

    def test_count_json(self, cli_runner: CliRunner, test_server: str) -> None:
        """data-count --json should return an integer."""
        res = cli_runner.invoke(
            app, ["data-count", "--host", test_server, "--json"]
        )
        assert res.exit_code == 0
        assert isinstance(json.loads(res.stdout), int)

    def test_count_wildcard(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """data-count '*' should produce output."""
        res = cli_runner.invoke(app, ["data-count", "*", "--host", test_server])
        assert res.exit_code == 0
        assert res.stdout

    def test_count_facet_json(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """data-count with --facet and --json should return a dict."""
        res = cli_runner.invoke(
            app,
            [
                "data-count",
                "--facet",
                "ocean",
                "--host",
                test_server,
                "--json",
                "-d",
            ],
        )
        assert res.exit_code == 0
        assert isinstance(json.loads(res.stdout), dict)

    def test_count_facet_display(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """data-count with --facet should produce output."""
        res = cli_runner.invoke(
            app,
            ["data-count", "--facet", "ocean", "--host", test_server, "-d"],
        )
        assert res.exit_code == 0
        assert res.stdout

    def test_count_facet_with_filter(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """data-count with facet and realm filter gives 0."""
        res = cli_runner.invoke(
            app,
            [
                "data-count",
                "--facet",
                "ocean",
                "--host",
                test_server,
                "realm=atmos",
                "--json",
            ],
        )
        assert res.exit_code == 0
        assert json.loads(res.stdout) == 0

    def test_count_with_custom_flavour(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """data-count with a custom flavour should work."""
        # add the flavour first
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "add",
                "test_cli_flavour",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--map",
                "project=projekt",
                "--map",
                "variable=var",
            ],
        )
        assert res.exit_code == 0

        # use it in data-count
        res = cli_runner.invoke(
            app,
            [
                "data-count",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--flavour",
                "test_cli_flavour",
                "--json",
            ],
        )
        assert res.exit_code == 0

        # cleanup
        cli_runner.invoke(
            app,
            [
                "flavour",
                "delete",
                "test_cli_flavour",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )
        assert res.exit_code == 0


class TestFailedCommands:
    """Tests for handling bad commands and parameters."""

    def test_unknown_facet_warns(
        self,
        cli_runner: CliRunner,
        caplog: LogCaptureFixture,
        test_server: str,
    ) -> None:
        """Passing an unknown facet should log a WARNING."""
        for cmd in ("data-count", "data-search", "metadata-search"):
            caplog.clear()
            res = cli_runner.invoke(app, [cmd, "--host", test_server, "foo=b"])
            assert res.exit_code == 0
            assert caplog.records
            assert caplog.records[-1].levelname == "WARNING"

    def test_invalid_flavour_flag(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """Using an invalid flavour flag value should fail."""
        for cmd in ("data-count", "data-search", "metadata-search"):
            res = cli_runner.invoke(
                app, [cmd, "--host", test_server, "-f", "foo"]
            )
            assert res.exit_code != 0

    def test_bad_host_errors(
        self,
        cli_runner: CliRunner,
        caplog: LogCaptureFixture,
        test_server: str,
    ) -> None:
        """Using a non-reachable host should log an ERROR."""
        for cmd in ("data-count", "data-search", "metadata-search"):
            caplog.clear()
            res = cli_runner.invoke(app, [cmd, "--host", "foo"])
            assert res.exit_code != 0
            assert caplog.records
            assert caplog.records[-1].levelname == "ERROR"

    def test_bad_host_verbose(
        self,
        cli_runner: CliRunner,
        caplog: LogCaptureFixture,
        test_server: str,
    ) -> None:
        """Verbose mode with a bad host should still error."""
        for cmd in ("data-count", "data-search", "metadata-search"):
            caplog.clear()
            res = cli_runner.invoke(app, [cmd, "--host", "foo", "-vvvvv"])
            assert res.exit_code != 0
            assert caplog.records
            assert caplog.records[-1].levelname == "ERROR"


class TestVersionFlags:
    """Tests for the --version flag on sub-commands."""

    def test_version_flag(self, cli_runner: CliRunner) -> None:
        """All data sub-commands should support -V."""
        for cmd in ("data-count", "data-search", "metadata-search"):
            res = cli_runner.invoke(app, [cmd, "-V"])
            assert res.exit_code == 0


class TestAuthenticate:
    """Test the authentication cli."""

    def test_auth_cli(
        self, cli_runner: CliRunner, test_server: str, mock_authenticate: Token
    ) -> None:
        """Test the authenticate command."""
        res = cli_runner.invoke(
            main_app,
            [
                "auth",
                "--host",
                test_server,
            ],
        )
        assert res.exit_code == 0


class TestFlavourCommands:
    """Tests for the flavour CLI sub-commands (list, add, update, delete)."""

    def test_flavour_full_lifecycle(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
        mocker: MockerFixture,
    ) -> None:
        """Test the full flavour lifecycle through the CLI."""
        # list with auth

        mocker.patch("freva_client.utils.choose_token_strategy").return_value = (
            "use_token"
        )
        from freva_rest.utils.namegenerator import generate_names
        flavour_name = generate_names()
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "list",
                "--host",
                test_server,
            ],
        )
        assert res.exit_code == 0
        assert (
            "Available Data Reference Syntax" in res.stdout
            or "No custom flavours found" in res.stdout
        )

        # list JSON
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "list",
                "--host",
                test_server,
                "--json",
            ],
        )
        assert res.exit_code == 0
        flavours_data = json.loads(res.stdout)
        assert isinstance(flavours_data, list)
        initial_count = len(flavours_data)

        # add
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "add",
                flavour_name,
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--map",
                "project=projekt",
                "--map",
                "variable=var",
            ],
        )
        assert res.exit_code == 0

        # verify add
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "list",
                "--host",
                test_server,
                "--json",
            ],
        )
        assert res.exit_code == 0
        flavours_after = json.loads(res.stdout)
        assert len(flavours_after) > initial_count
        assert flavour_name in [f["flavour_name"] for f in flavours_after]

        # metadata-search with custom flavour
        res = cli_runner.invoke(
            app,
            [
                "metadata-search",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--flavour",
                flavour_name,
                "--json",
            ],
        )
        assert res.exit_code == 0

        # data-count with custom flavour
        res = cli_runner.invoke(
            app,
            [
                "data-count",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--flavour",
                flavour_name,
                "--json",
            ],
        )
        assert res.exit_code == 0

        # update
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "update",
                flavour_name,
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--map",
                "experiment=exp_new",
            ],
        )
        assert res.exit_code == 0

        # rename
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "update",
                flavour_name,
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--new-name",
                "test_cli_flavour_renamed",
            ],
        )
        assert res.exit_code == 0

        # verify rename
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "list",
                "--host",
                test_server,
                "--json",
            ],
        )
        assert res.exit_code == 0
        flavour_names = [f["flavour_name"] for f in json.loads(res.stdout)]
        assert "test_cli_flavour_renamed" in flavour_names

        # delete
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "delete",
                "test_cli_flavour_renamed",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )

        # verify delete
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "list",
                "--host",
                test_server,
                "--json",
            ],
        )
        assert res.exit_code == 0
        flavours_final = json.loads(res.stdout)
        final_names = [f["flavour_name"] for f in flavours_final]
        assert "test_cli_flavour_renamed" not in final_names
        assert len(flavours_final) == initial_count


class TestFlavourErrorCasesCli:
    """Tests for flavour CLI error handling."""

    def test_add_without_mapping(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """Adding a flavour without mapping should fail."""
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "add",
                "test_flavour",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
            ],
        )
        assert res.exit_code == 1

    def test_add_with_invalid_mapping(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """Adding with invalid mapping format should fail."""
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "add",
                "test_flavour",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--map",
                "invalid_format",
            ],
        )
        assert res.exit_code == 1

    def test_update_with_invalid_mapping(
        self,
        cli_runner: CliRunner,
        test_server: str,
        token_file: Path,
        mock_authenticate: Token,
    ) -> None:
        """Updating with invalid mapping format should fail."""
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "update",
                "test_flavour",
                "--host",
                test_server,
                "--token-file",
                str(token_file),
                "--map",
                "invalid_format",
            ],
        )
        assert res.exit_code == 1

    def test_list_without_auth(
        self, cli_runner: CliRunner, test_server: str
    ) -> None:
        """Listing without authentication should succeed."""
        res = cli_runner.invoke(app, ["flavour", "list", "--host", test_server])
        assert res.exit_code == 0

    def test_add_without_auth_fails(
        self,
        cli_runner: CliRunner,
        test_server: str,
        mock_authenticate_fail: None,
    ) -> None:
        """Adding without authentication should fail."""
        res = cli_runner.invoke(
            app,
            [
                "flavour",
                "add",
                "test",
                "--host",
                test_server,
                "--map",
                "a=b",
            ],
        )
        assert res.exit_code != 0

    def test_delete_without_auth_fails(
        self,
        cli_runner: CliRunner,
        test_server: str,
        mock_authenticate_fail: Token,
    ) -> None:
        """Deleting without authentication should fail."""
        res = cli_runner.invoke(
            app,
            ["flavour", "delete", "test", "--host", test_server],
        )
        assert res.exit_code != 0
