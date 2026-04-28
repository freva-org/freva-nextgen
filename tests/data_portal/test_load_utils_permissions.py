"""Tests for filesystem permission checks in data_portal_worker.utils."""

import os
import stat
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from data_portal_worker.utils import (
    _can_read_stat,
    _can_read_su,
    user_can_read,
)


def _make_stat(mode: int, uid: int = 1000, gid: int = 1000) -> os.stat_result:
    """Create a fake os.stat_result with the given mode, uid, gid."""
    # os.stat_result expects a 10-tuple:
    # (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
    return os.stat_result((mode, 0, 0, 1, uid, gid, 0, 0, 0, 0))


def _make_passwd(uid: int = 1000, gid: int = 1000) -> MagicMock:
    """Create a fake pwd struct."""
    pw = MagicMock()
    pw.pw_uid = uid
    pw.pw_gid = gid
    return pw


def _make_group(gid: int, members: List[str]) -> MagicMock:
    """Create a fake grp struct."""
    g = MagicMock()
    g.gr_gid = gid
    g.gr_mem = members
    return g


class TestCanReadStat:
    """Tests for the stat-based permission check."""

    @patch("data_portal_worker.utils.grp.getgrall")
    @patch("data_portal_worker.utils.os.stat")
    @patch("data_portal_worker.utils.pwd.getpwnam")
    def test_owner_can_read(self, mock_pwd, mock_stat, mock_grp) -> None:
        """File owner with read bit set can read."""
        mock_pwd.return_value = _make_passwd(uid=1000, gid=1000)
        mock_stat.return_value = _make_stat(
            stat.S_IRUSR | stat.S_IFREG, uid=1000, gid=2000
        )
        mock_grp.return_value = []
        assert _can_read_stat("/data/file.nc", "testuser") is True

    @patch("data_portal_worker.utils.grp.getgrall")
    @patch("data_portal_worker.utils.os.stat")
    @patch("data_portal_worker.utils.pwd.getpwnam")
    def test_owner_cannot_read(self, mock_pwd, mock_stat, mock_grp) -> None:
        """File owner without read bit cannot read."""
        mock_pwd.return_value = _make_passwd(uid=1000, gid=1000)
        mock_stat.return_value = _make_stat(
            stat.S_IWUSR | stat.S_IFREG, uid=1000, gid=2000
        )
        mock_grp.return_value = []
        assert _can_read_stat("/data/file.nc", "testuser") is False

    @patch("data_portal_worker.utils.grp.getgrall")
    @patch("data_portal_worker.utils.os.stat")
    @patch("data_portal_worker.utils.pwd.getpwnam")
    def test_group_can_read(self, mock_pwd, mock_stat, mock_grp) -> None:
        """User in file's group with group read bit can read."""
        mock_pwd.return_value = _make_passwd(uid=1000, gid=2000)
        mock_stat.return_value = _make_stat(
            stat.S_IRGRP | stat.S_IFREG, uid=9999, gid=2000
        )
        mock_grp.return_value = []
        assert _can_read_stat("/data/file.nc", "testuser") is True

    @patch("data_portal_worker.utils.grp.getgrall")
    @patch("data_portal_worker.utils.os.stat")
    @patch("data_portal_worker.utils.pwd.getpwnam")
    def test_supplementary_group_can_read(self, mock_pwd, mock_stat, mock_grp) -> None:
        """User in a supplementary group matching the file's group can read."""
        mock_pwd.return_value = _make_passwd(uid=1000, gid=1000)
        mock_stat.return_value = _make_stat(
            stat.S_IRGRP | stat.S_IFREG, uid=9999, gid=3000
        )
        mock_grp.return_value = [_make_group(3000, ["testuser"])]
        assert _can_read_stat("/data/file.nc", "testuser") is True

    @patch("data_portal_worker.utils.grp.getgrall")
    @patch("data_portal_worker.utils.os.stat")
    @patch("data_portal_worker.utils.pwd.getpwnam")
    def test_group_no_read_bit(self, mock_pwd, mock_stat, mock_grp) -> None:
        """User in file's group but no group read bit cannot read."""
        mock_pwd.return_value = _make_passwd(uid=1000, gid=2000)
        mock_stat.return_value = _make_stat(
            stat.S_IWGRP | stat.S_IFREG, uid=9999, gid=2000
        )
        mock_grp.return_value = []
        assert _can_read_stat("/data/file.nc", "testuser") is False

    @patch("data_portal_worker.utils.grp.getgrall")
    @patch("data_portal_worker.utils.os.stat")
    @patch("data_portal_worker.utils.pwd.getpwnam")
    def test_other_can_read(self, mock_pwd, mock_stat, mock_grp) -> None:
        """Non-owner, non-group user with world-read bit can read."""
        mock_pwd.return_value = _make_passwd(uid=1000, gid=1000)
        mock_stat.return_value = _make_stat(
            stat.S_IROTH | stat.S_IFREG, uid=9999, gid=9999
        )
        mock_grp.return_value = []
        assert _can_read_stat("/data/file.nc", "testuser") is True

    @patch("data_portal_worker.utils.grp.getgrall")
    @patch("data_portal_worker.utils.os.stat")
    @patch("data_portal_worker.utils.pwd.getpwnam")
    def test_no_permissions(self, mock_pwd, mock_stat, mock_grp) -> None:
        """Non-owner, non-group user with no read bits cannot read."""
        mock_pwd.return_value = _make_passwd(uid=1000, gid=1000)
        mock_stat.return_value = _make_stat(stat.S_IFREG, uid=9999, gid=9999)
        mock_grp.return_value = []
        assert _can_read_stat("/data/file.nc", "testuser") is False

    @patch("data_portal_worker.utils.grp.getgrall")
    @patch("data_portal_worker.utils.os.stat")
    @patch("data_portal_worker.utils.pwd.getpwnam")
    def test_unknown_user_raises(self, mock_pwd, mock_stat, mock_grp) -> None:
        """Unknown username raises KeyError from pwd."""
        mock_pwd.side_effect = KeyError("getpwnam(): name not found")
        with pytest.raises(KeyError):
            _can_read_stat("/data/file.nc", "nonexistent")


class TestCanReadSu:
    """Tests for the su-based permission check."""

    @patch("data_portal_worker.utils.subprocess.run")
    def test_allowed(self, mock_run) -> None:
        """test -r returning 0 means allowed."""
        mock_run.return_value = MagicMock(returncode=0)
        assert _can_read_su("/data/file.nc", "testuser") is True

    @patch("data_portal_worker.utils.subprocess.run")
    def test_denied(self, mock_run) -> None:
        """test -r returning 1 means denied."""
        mock_run.return_value = MagicMock(returncode=1)
        assert _can_read_su("/data/file.nc", "testuser") is False

    @patch("data_portal_worker.utils.subprocess.run")
    def test_command_structure(self, mock_run) -> None:
        """Verify the su command is constructed correctly."""
        mock_run.return_value = MagicMock(returncode=0)
        _can_read_su("/data/my file.nc", "k204230")
        args = mock_run.call_args
        cmd = args[0][0]
        assert cmd[:4] == ["su", "-s", "/bin/sh", "k204230"]
        # Path should be shell-quoted
        assert "my file.nc" in cmd[5] or "'my file.nc'" in cmd[5]


class TestUserCanRead:
    """Tests for the top-level user_can_read dispatcher."""

    @patch("data_portal_worker.utils.os.stat")
    def test_guest_world_readable(self, mock_stat) -> None:
        """Guest (no username) can read world-readable files."""
        mock_stat.return_value = _make_stat(stat.S_IROTH | stat.S_IFREG)
        assert user_can_read("/data/file.nc") is True

    @patch("data_portal_worker.utils.os.stat")
    def test_guest_not_world_readable(self, mock_stat) -> None:
        """Guest (no username) cannot read non-world-readable files."""
        mock_stat.return_value = _make_stat(stat.S_IRUSR | stat.S_IFREG)
        assert user_can_read("/data/file.nc") is False

    @patch("data_portal_worker.utils.os.stat")
    def test_empty_username_is_guest(self, mock_stat) -> None:
        """Empty string username is treated as guest."""
        mock_stat.return_value = _make_stat(stat.S_IROTH | stat.S_IFREG)
        assert user_can_read("/data/file.nc", "") is True

    @patch("data_portal_worker.utils.os.stat")
    def test_whitespace_username_is_guest(self, mock_stat) -> None:
        """Whitespace-only username is treated as guest."""
        mock_stat.return_value = _make_stat(stat.S_IROTH | stat.S_IFREG)
        assert user_can_read("/data/file.nc", "  ") is True

    @patch("data_portal_worker.utils._can_read_su")
    @patch("data_portal_worker.utils.os.getuid", return_value=0)
    def test_root_delegates_to_su(self, mock_getuid, mock_su) -> None:
        """When running as root, delegates to su-based check."""
        mock_su.return_value = True
        assert user_can_read("/data/file.nc", "testuser") is True
        mock_su.assert_called_once_with("/data/file.nc", "testuser")

    @patch("data_portal_worker.utils._can_read_stat")
    @patch("data_portal_worker.utils.os.getuid", return_value=1000)
    def test_non_root_delegates_to_stat(self, mock_getuid, mock_stat_fn) -> None:
        """When not root, delegates to stat-based check."""
        mock_stat_fn.return_value = True
        assert user_can_read("/data/file.nc", "testuser") is True
        mock_stat_fn.assert_called_once_with("/data/file.nc", "testuser")
