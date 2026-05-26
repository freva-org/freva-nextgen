"""Tests for data_portal_worker.sanitizer.

All tests are pure unit tests — no Redis, no filesystem, no threads.
Each test builds a minimal valid message, mutates exactly one field to an
invalid value, and asserts that ``sanitize_message`` raises ``ValueError``
with a message that is safe to log.

A complementary ``TestValidMessages`` class verifies that every valid
message type passes through unchanged (or with correct normalisation).
"""

from __future__ import annotations

import pytest

from data_portal_worker.sanitizer import (_MAX_CHUNK_SIZE_MIB,
                                          _MAX_MAP_PRIMARY,
                                          _MIN_CHUNK_SIZE_MIB,
                                          _MIN_MAP_PRIMARY, sanitize_message)

# ---------------------------------------------------------------------------
# Minimal valid payloads — reused across test classes
# ---------------------------------------------------------------------------

_VALID_URI: dict = {
    "uri": {
        "path": ["/lustre/data/cmip6/tas.nc"],
        "uuid": "abc123",
        "username": "k204230",
        "assembly": None,
        "access_pattern": "map",
        "map_primary_chunksize": 1,
        "reload": False,
        "chunk_size": 16.0,
    }
}

_VALID_CHUNK: dict = {
    "chunk": {
        "uuid": "abc123",
        "chunk": "0.1.2",
        "variable": "tas",
    }
}

_VALID_ACCESS_CHECK: dict = {
    "access_check": {
        "request_id": "req-uuid-001",
        "username": None,
        "paths": ["/lustre/data/cmip6/tas.nc"],
    }
}

_VALID_SHUTDOWN: dict = {"shutdown": True}


# ---------------------------------------------------------------------------
# Valid messages round-trip correctly
# ---------------------------------------------------------------------------


class TestValidMessages:
    """Every recognised message type passes sanitisation without error."""

    def test_uri_message_passes(self) -> None:
        out = sanitize_message(_VALID_URI)
        assert out["uri"]["path"] == ["/lustre/data/cmip6/tas.nc"]
        assert out["uri"]["username"] == "k204230"
        assert out["uri"]["access_pattern"] == "map"
        assert out["uri"]["chunk_size"] == 16.0

    def test_chunk_message_passes(self) -> None:
        out = sanitize_message(_VALID_CHUNK)
        assert out["chunk"]["chunk"] == "0.1.2"
        assert out["chunk"]["variable"] == "tas"

    def test_access_check_passes(self) -> None:
        out = sanitize_message(_VALID_ACCESS_CHECK)
        assert out["access_check"]["username"] is None
        assert "/lustre/data/cmip6/tas.nc" in out["access_check"]["paths"]

    def test_shutdown_passes(self) -> None:
        out = sanitize_message(_VALID_SHUTDOWN)
        assert out["shutdown"] is True

    def test_uri_without_optional_fields_uses_defaults(self) -> None:
        minimal = {
            "uri": {
                "path": ["/data/f.nc"],
                "uuid": "x",
            }
        }
        out = sanitize_message(minimal)
        assert out["uri"]["access_pattern"] == "map"
        assert out["uri"]["reload"] is False
        assert out["uri"]["chunk_size"] == 16.0
        assert out["uri"]["map_primary_chunksize"] == 1
        assert out["uri"]["username"] is None
        assert out["uri"]["assembly"] is None

    def test_blank_username_is_normalised_to_none(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "username": "   "}}
        assert sanitize_message(msg)["uri"]["username"] is None

    def test_time_series_access_pattern_passes(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "access_pattern": "time_series"}}
        assert sanitize_message(msg)["uri"]["access_pattern"] == "time_series"

    def test_path_with_double_slashes_is_normalised(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": ["/data//cmip6/f.nc"]}}
        out = sanitize_message(msg)
        # PurePosixPath collapses double slashes.
        assert "//" not in out["uri"]["path"][0]

    def test_grouped_variable_path_passes(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "variable": "group1/tas"}}
        assert sanitize_message(msg)["chunk"]["variable"] == "group1/tas"

    def test_assembly_with_valid_keys_passes(self) -> None:
        msg = {
            "uri": {
                **_VALID_URI["uri"],
                "assembly": {"mode": "by_coords", "dim": "time"},
            }
        }
        out = sanitize_message(msg)
        assert out["uri"]["assembly"]["mode"] == "by_coords"

    def test_reload_true_passes(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "reload": True}}
        assert sanitize_message(msg)["uri"]["reload"] is True

    def test_chunk_size_at_minimum_boundary_passes(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "chunk_size": _MIN_CHUNK_SIZE_MIB}}
        assert sanitize_message(msg)["uri"]["chunk_size"] == _MIN_CHUNK_SIZE_MIB

    def test_chunk_size_at_maximum_boundary_passes(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "chunk_size": _MAX_CHUNK_SIZE_MIB}}
        assert sanitize_message(msg)["uri"]["chunk_size"] == _MAX_CHUNK_SIZE_MIB

    def test_map_primary_chunksize_at_boundaries_passes(self) -> None:
        for v in (_MIN_MAP_PRIMARY, _MAX_MAP_PRIMARY):
            msg = {"uri": {**_VALID_URI["uri"], "map_primary_chunksize": v}}
            assert sanitize_message(msg)["uri"]["map_primary_chunksize"] == v

    def test_multi_path_uri_passes(self) -> None:
        msg = {
            "uri": {
                **_VALID_URI["uri"],
                "path": ["/data/a.nc", "/data/b.nc", "/data/c.nc"],
            }
        }
        assert len(sanitize_message(msg)["uri"]["path"]) == 3

    def test_1d_chunk_id_passes(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "chunk": "7"}}
        assert sanitize_message(msg)["chunk"]["chunk"] == "7"

    def test_s3_url_passes(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": ["s3://my-bucket/cmip6/tas.nc"]}}
        assert sanitize_message(msg)["uri"]["path"] == ["s3://my-bucket/cmip6/tas.nc"]

    def test_https_url_passes(self) -> None:
        msg = {
            "uri": {**_VALID_URI["uri"], "path": ["https://data.dkrz.de/cmip6/tas.nc"]}
        }
        assert sanitize_message(msg)["uri"]["path"] == [
            "https://data.dkrz.de/cmip6/tas.nc"
        ]

    def test_hsm_url_passes(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": ["hsm://hsmarchive/cmip6/tas.nc"]}}
        assert sanitize_message(msg)["uri"]["path"] == ["hsm://hsmarchive/cmip6/tas.nc"]


# ---------------------------------------------------------------------------
# Top-level message structure
# ---------------------------------------------------------------------------


class TestTopLevelStructure:
    def test_non_dict_message_rejected(self) -> None:
        for bad in ([], "uri", 42, None, True):
            with pytest.raises(ValueError, match="JSON object"):
                sanitize_message(bad)

    def test_unknown_key_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="no recognised key"):
            sanitize_message({"totally_unknown": 1})

    def test_empty_dict_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="no recognised key"):
            sanitize_message({})


# ---------------------------------------------------------------------------
# URI message validation
# ---------------------------------------------------------------------------


class TestUriPaths:
    """Path field: absolute, no traversal, no null bytes, non-empty list."""

    def test_relative_path_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": ["relative/path.nc"]}}
        with pytest.raises(ValueError, match="absolute"):
            sanitize_message(msg)

    def test_path_traversal_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": ["/data/../etc/passwd"]}}
        with pytest.raises(ValueError, match=r"\.\."):
            sanitize_message(msg)

    def test_null_byte_in_path_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": ["/data/fi\x00le.nc"]}}
        with pytest.raises(ValueError, match="null byte"):
            sanitize_message(msg)

    def test_empty_path_string_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": [""]}}
        with pytest.raises(ValueError):
            sanitize_message(msg)

    def test_empty_path_list_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": []}}
        with pytest.raises(ValueError, match="non-empty list"):
            sanitize_message(msg)

    def test_non_list_path_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": "/data/f.nc"}}
        with pytest.raises(ValueError, match="non-empty list"):
            sanitize_message(msg)

    def test_list_with_non_string_entry_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": ["/data/f.nc", 42]}}
        with pytest.raises(ValueError, match="string"):
            sanitize_message(msg)

    def test_path_with_only_traversal_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": ["/../secret"]}}
        with pytest.raises(ValueError, match=r"\.\."):
            sanitize_message(msg)

    def test_no_json(self) -> None:
        with pytest.raises(ValueError, match="must be a JSON object"):
            sanitize_message({"uri": ["foo"]})


class TestUriUsername:
    def test_username_with_shell_metachar_is_rejected(self) -> None:
        for bad in ("user;id", "$(id)", "user name", "user\nname", "user|name"):
            msg = {"uri": {**_VALID_URI["uri"], "username": bad}}
            with pytest.raises(ValueError, match="username"):
                sanitize_message(msg)

    def test_username_too_long_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "username": "a" * 65}}
        with pytest.raises(ValueError, match="username"):
            sanitize_message(msg)

    def test_non_string_username_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "username": 1234}}
        with pytest.raises(ValueError, match="username"):
            sanitize_message(msg)

    def test_username_with_dots_and_hyphens_passes(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "username": "jane.doe-2"}}
        assert sanitize_message(msg)["uri"]["username"] == "jane.doe-2"


class TestUriAccessPattern:
    def test_invalid_access_pattern_is_rejected(self) -> None:
        for bad in ("MAP", "timeseries", "glob", "", "  map  "):
            msg = {"uri": {**_VALID_URI["uri"], "access_pattern": bad}}
            with pytest.raises(ValueError, match="access_pattern"):
                sanitize_message(msg)

    def test_non_string_access_pattern_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "access_pattern": None}}
        with pytest.raises(ValueError):
            sanitize_message(msg)


class TestUriChunkSize:
    def test_chunk_size_below_minimum_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "chunk_size": _MIN_CHUNK_SIZE_MIB - 0.01}}
        with pytest.raises(ValueError, match="chunk_size"):
            sanitize_message(msg)

    def test_chunk_size_above_maximum_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "chunk_size": _MAX_CHUNK_SIZE_MIB + 1}}
        with pytest.raises(ValueError, match="chunk_size"):
            sanitize_message(msg)

    def test_chunk_size_zero_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "chunk_size": 0}}
        with pytest.raises(ValueError, match="chunk_size"):
            sanitize_message(msg)

    def test_negative_chunk_size_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "chunk_size": -16.0}}
        with pytest.raises(ValueError, match="chunk_size"):
            sanitize_message(msg)

    def test_string_chunk_size_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "chunk_size": "16"}}
        with pytest.raises(ValueError, match="chunk_size"):
            sanitize_message(msg)


class TestUriMapPrimaryChunksize:
    def test_below_minimum_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "map_primary_chunksize": 0}}
        with pytest.raises(ValueError, match="map_primary_chunksize"):
            sanitize_message(msg)

    def test_above_maximum_is_rejected(self) -> None:
        msg = {
            "uri": {**_VALID_URI["uri"], "map_primary_chunksize": _MAX_MAP_PRIMARY + 1}
        }
        with pytest.raises(ValueError, match="map_primary_chunksize"):
            sanitize_message(msg)

    def test_float_is_rejected(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "map_primary_chunksize": 1.5}}
        with pytest.raises(ValueError, match="map_primary_chunksize"):
            sanitize_message(msg)

    def test_bool_is_rejected(self) -> None:
        # bool is a subclass of int in Python — must be explicitly excluded.
        msg = {"uri": {**_VALID_URI["uri"], "map_primary_chunksize": True}}
        with pytest.raises(ValueError, match="map_primary_chunksize"):
            sanitize_message(msg)


class TestUriAssembly:
    def test_non_dict_assembly_is_rejected(self) -> None:
        for bad in ("by_coords", [], 42):
            msg = {"uri": {**_VALID_URI["uri"], "assembly": bad}}
            with pytest.raises(ValueError, match="assembly"):
                sanitize_message(msg)

    def test_unknown_assembly_key_is_rejected(self) -> None:
        msg = {
            "uri": {
                **_VALID_URI["uri"],
                "assembly": {"__proto__": "evil", "mode": "by_coords"},
            }
        }
        with pytest.raises(ValueError, match="unexpected key"):
            sanitize_message(msg)

    def test_non_string_assembly_value_is_rejected(self) -> None:
        msg = {
            "uri": {
                **_VALID_URI["uri"],
                "assembly": {"mode": ["by_coords"]},
            }
        }
        with pytest.raises(ValueError, match="assembly"):
            sanitize_message(msg)

    def test_empty_assembly_dict_normalised_to_none(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "assembly": {}}}
        assert sanitize_message(msg)["uri"]["assembly"] is None

    def test_wrong_assembly_dict_type(self) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "assembly": {1: "foo"}}}
        with pytest.raises(ValueError, match="must be a string"):
            sanitize_message(msg)


# ---------------------------------------------------------------------------
# Chunk message validation
# ---------------------------------------------------------------------------


class TestChunkMessage:
    def test_non_dict_payload_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="JSON object"):
            sanitize_message({"chunk": "0.1.2"})

    def test_chunk_id_with_letters_is_rejected(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "chunk": "a.b.c"}}
        with pytest.raises(ValueError, match="chunk"):
            sanitize_message(msg)

    def test_chunk_id_with_unicode_digits_is_rejected(self) -> None:
        # '²' and '٣' are decimal/digit in Unicode but NOT ASCII —
        # isascii() fast-fail rejects them before isdecimal() is even called.
        for bad in ("².0", "0.٣", "\u0660"):
            msg = {"chunk": {**_VALID_CHUNK["chunk"], "chunk": bad}}
            with pytest.raises(ValueError, match="chunk"):
                sanitize_message(msg)

    def test_chunk_id_with_negative_index_is_rejected(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "chunk": "-1.0"}}
        with pytest.raises(ValueError, match="chunk"):
            sanitize_message(msg)

    def test_empty_chunk_id_is_rejected(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "chunk": ""}}
        with pytest.raises(ValueError, match="chunk"):
            sanitize_message(msg)

    def test_chunk_id_with_spaces_is_rejected(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "chunk": "0 1"}}
        with pytest.raises(ValueError, match="chunk"):
            sanitize_message(msg)

    def test_chunk_id_with_leading_dot_is_rejected(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "chunk": ".0"}}
        with pytest.raises(ValueError, match="chunk"):
            sanitize_message(msg)

    def test_chunk_id_with_trailing_dot_is_rejected(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "chunk": "0."}}
        with pytest.raises(ValueError, match="chunk"):
            sanitize_message(msg)

    def test_chunk_id_with_double_dot_is_rejected(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "chunk": "0..1"}}
        with pytest.raises(ValueError, match="chunk"):
            sanitize_message(msg)

    def test_variable_with_path_traversal_is_rejected(self) -> None:
        for bad in ("../tas", "group/../tas", "tas/../../../etc/passwd"):
            msg = {"chunk": {**_VALID_CHUNK["chunk"], "variable": bad}}
            with pytest.raises(ValueError, match="variable"):
                sanitize_message(msg)

    def test_variable_with_leading_slash_is_rejected(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "variable": "/tas"}}
        with pytest.raises(ValueError, match="variable"):
            sanitize_message(msg)

    def test_variable_with_trailing_slash_is_rejected(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "variable": "group/"}}
        with pytest.raises(ValueError, match="variable"):
            sanitize_message(msg)

    def test_variable_too_long_is_rejected(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "variable": "a" * 257}}
        with pytest.raises(ValueError, match="variable"):
            sanitize_message(msg)

    def test_variable_with_shell_chars_is_rejected(self) -> None:
        for bad in ("tas;id", "$(tas)", "tas\nvar"):
            msg = {"chunk": {**_VALID_CHUNK["chunk"], "variable": bad}}
            with pytest.raises(ValueError, match="variable"):
                sanitize_message(msg)

    def test_non_string_uuid_is_rejected(self) -> None:
        msg = {"chunk": {**_VALID_CHUNK["chunk"], "uuid": 42}}
        with pytest.raises(ValueError, match="string"):
            sanitize_message(msg)


# ---------------------------------------------------------------------------
# access_check message validation
# ---------------------------------------------------------------------------


class TestAccessCheckMessage:
    def test_non_dict_payload_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="JSON object"):
            sanitize_message({"access_check": "r1"})

    def test_relative_path_is_rejected(self) -> None:
        msg = {
            "access_check": {
                **_VALID_ACCESS_CHECK["access_check"],
                "paths": ["relative/path.nc"],
            }
        }
        with pytest.raises(ValueError, match="absolute"):
            sanitize_message(msg)

    def test_path_traversal_is_rejected(self) -> None:
        msg = {
            "access_check": {
                **_VALID_ACCESS_CHECK["access_check"],
                "paths": ["/data/../etc/shadow"],
            }
        }
        with pytest.raises(ValueError, match=r"\.\."):
            sanitize_message(msg)

    def test_empty_paths_list_is_rejected(self) -> None:
        msg = {
            "access_check": {
                **_VALID_ACCESS_CHECK["access_check"],
                "paths": [],
            }
        }
        with pytest.raises(ValueError, match="non-empty list"):
            sanitize_message(msg)

    def test_invalid_request_id_with_spaces_is_rejected(self) -> None:
        msg = {
            "access_check": {
                **_VALID_ACCESS_CHECK["access_check"],
                "request_id": "bad id with spaces",
            }
        }
        with pytest.raises(ValueError, match="request_id"):
            sanitize_message(msg)

    def test_request_id_with_tab_is_rejected(self) -> None:
        msg = {
            "access_check": {
                **_VALID_ACCESS_CHECK["access_check"],
                "request_id": "req\t001",
            }
        }
        with pytest.raises(ValueError, match="request_id"):
            sanitize_message(msg)

    def test_request_id_with_newline_is_rejected(self) -> None:
        msg = {
            "access_check": {
                **_VALID_ACCESS_CHECK["access_check"],
                "request_id": "req\n001",
            }
        }
        with pytest.raises(ValueError, match="request_id"):
            sanitize_message(msg)

    def test_request_id_too_long_is_rejected(self) -> None:
        msg = {
            "access_check": {
                **_VALID_ACCESS_CHECK["access_check"],
                "request_id": "x" * 129,
            }
        }
        with pytest.raises(ValueError, match="request_id"):
            sanitize_message(msg)

    def test_non_string_request_id_is_rejected(self) -> None:
        msg = {
            "access_check": {
                **_VALID_ACCESS_CHECK["access_check"],
                "request_id": 12345,
            }
        }
        with pytest.raises(ValueError, match="string"):
            sanitize_message(msg)

    def test_invalid_username_is_rejected(self) -> None:
        msg = {
            "access_check": {
                **_VALID_ACCESS_CHECK["access_check"],
                "username": "bad;user",
            }
        }
        with pytest.raises(ValueError, match="username"):
            sanitize_message(msg)

    def test_null_username_passes(self) -> None:
        msg = {
            "access_check": {
                **_VALID_ACCESS_CHECK["access_check"],
                "username": None,
            }
        }
        assert sanitize_message(msg)["access_check"]["username"] is None


# ---------------------------------------------------------------------------
# _has_url_scheme detection
# ---------------------------------------------------------------------------


class TestHasUrlScheme:
    """Unit tests for the internal URL-scheme detector."""

    from data_portal_worker.sanitizer import _has_url_scheme

    @pytest.mark.parametrize(
        "raw",
        [
            "s3://bucket/key.nc",
            "s3a://bucket/key.nc",
            "https://host/path",
            "http://host/path",
            "hsm://archive/file.nc",
            "gs://bucket/key",
            "ftp://host/path",
            "sftp://host/path",
            "swift://container/obj",
            "abfs://container@account.dfs.core.windows.net/path",
        ],
    )
    def test_url_detected(self, raw: str) -> None:
        from data_portal_worker.sanitizer import _has_url_scheme

        assert _has_url_scheme(raw) is True

    @pytest.mark.parametrize(
        "raw",
        [
            "/lustre/path/file.nc",
            "/",
            "relative/path.nc",
            "",
            "://missing-scheme",
            "no-sep-at-all",
        ],
    )
    def test_posix_not_detected_as_url(self, raw: str) -> None:
        from data_portal_worker.sanitizer import _has_url_scheme

        assert _has_url_scheme(raw) is False


# ---------------------------------------------------------------------------
# URL-scheme paths
# ---------------------------------------------------------------------------


class TestUrlPaths:
    """Validation of s3://, https://, hsm://, and other URL-scheme paths."""

    # -- valid URLs ----------------------------------------------------------

    @pytest.mark.parametrize(
        "url",
        [
            "s3://my-bucket/cmip6/tas.nc",
            "s3a://my-bucket/cmip6/tas.nc",
            "s3n://my-bucket/cmip6/tas.nc",
            "https://data.example.org/cmip6/tas.nc",
            "http://internal.dkrz.de/cmip6/tas.nc",
            "hsm://hsmarchive/cmip6/project/tas.nc",
            "gs://gcs-bucket/path/to/file.nc",
            "swift://container/object.nc",
            "ftp://ftp.example.org/data/file.nc",
            "sftp://hpc.dkrz.de/data/file.nc",
        ],
    )
    def test_valid_url_passes(self, url: str) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": [url]}}
        out = sanitize_message(msg)
        assert out["uri"]["path"] == [url]

    def test_url_is_returned_unchanged(self) -> None:
        """URL-scheme paths are NOT normalised — object-store keys are case-sensitive."""
        url = "s3://My-Bucket/Path//with//double//slashes.nc"
        msg = {"uri": {**_VALID_URI["uri"], "path": [url]}}
        assert sanitize_message(msg)["uri"]["path"] == [url]

    def test_mixed_posix_and_url_list_passes(self) -> None:
        msg = {
            "uri": {
                **_VALID_URI["uri"],
                "path": [
                    "/lustre/data/local.nc",
                    "s3://bucket/remote.nc",
                    "hsm://archive/tape.nc",
                ],
            }
        }
        out = sanitize_message(msg)["uri"]["path"]
        assert len(out) == 3

    def test_url_in_access_check_passes(self) -> None:
        msg = {
            "access_check": {
                **_VALID_ACCESS_CHECK["access_check"],
                "paths": ["s3://bucket/key.nc", "https://host/data.nc"],
            }
        }
        out = sanitize_message(msg)["access_check"]["paths"]
        assert "s3://bucket/key.nc" in out

    # -- disallowed schemes --------------------------------------------------

    @pytest.mark.parametrize(
        "url",
        [
            "file:///etc/passwd",
            "file://localhost/etc/passwd",
            "ssh://host/path",
            "ldap://host/path",
            "javascript://host/path",
            "gopher://host/path",
            "smb://server/share",
        ],
    )
    def test_disallowed_scheme_is_rejected(self, url: str) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": [url]}}
        with pytest.raises(ValueError, match="disallowed URL scheme"):
            sanitize_message(msg)

    # -- embedded credentials ------------------------------------------------

    @pytest.mark.parametrize(
        "url",
        [
            "s3://AKIAIOSFODNN7:wJalrXUtnFEMI@bucket/key.nc",
            "https://user:password@host/path",
            "ftp://anonymous:guest@ftp.example.org/data.nc",
        ],
    )
    def test_embedded_credentials_are_rejected(self, url: str) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": [url]}}
        with pytest.raises(ValueError, match="credentials"):
            sanitize_message(msg)

    # -- missing host / bucket -----------------------------------------------

    @pytest.mark.parametrize(
        "url",
        [
            "s3:///key-with-no-bucket.nc",
            "https:///path-with-no-host",
        ],
    )
    def test_missing_bucket_or_host_is_rejected(self, url: str) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": [url]}}
        with pytest.raises(ValueError, match="host or bucket"):
            sanitize_message(msg)

    # -- path traversal in URL -----------------------------------------------

    @pytest.mark.parametrize(
        "url",
        [
            "s3://bucket/../../../etc/passwd",
            "https://host/data/../../../etc/shadow",
            "hsm://archive/path/../../secret",
        ],
    )
    def test_traversal_in_url_path_is_rejected(self, url: str) -> None:
        msg = {"uri": {**_VALID_URI["uri"], "path": [url]}}
        with pytest.raises(ValueError, match=r"\.\."):
            sanitize_message(msg)

    # -- null bytes ----------------------------------------------------------

    def test_null_byte_in_url_is_rejected(self) -> None:
        url = "s3://bucket/fi\x00le.nc"
        msg = {"uri": {**_VALID_URI["uri"], "path": [url]}}
        with pytest.raises(ValueError, match="null byte"):
            sanitize_message(msg)
