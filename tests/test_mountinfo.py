"""Unit tests for youvegotdata.mountinfo."""

import io
import logging
from unittest.mock import mock_open, patch

import pytest

from youvegotdata import parse_mountinfo, parse_mountinfo_alike

from conftest import (
    CEPH_ROCKY_8_7_MOUNTINFO_LINE,
    CEPH_ROCKY_9_4_MOUNTINFO_LINE,
    CEPH_ROCKY_9_4_NO_MON_ADDR_MOUNTINFO_LINE,
    LOCAL_MOUNTINFO_LINE,
    NFS_MOUNTINFO_LINE,
    ROOT_MOUNTINFO_LINE,
    _lines,
)

# ---------------------------------------------------------------------------
# parse_mountinfo_alike
# ---------------------------------------------------------------------------


class TestParseMountinfoAlike:
    def test_local_mount_parsed_correctly(self):
        entries = parse_mountinfo_alike(_lines(LOCAL_MOUNTINFO_LINE))
        assert len(entries) == 1
        e = entries[0]
        assert e["mount_id"] == 23
        assert e["parent_id"] == 1
        assert e["major_minor"] == "8:1"
        assert e["root"] == "/"
        assert e["mount_point"] == "/data"
        assert e["mount_options"] == ["rw", "relatime"]
        assert e["filesystem_type"] == "ext4"
        assert e["mount_source"] == "/dev/sda1"
        assert "rw" in e["super_options"]
        assert e["raw_line"] == LOCAL_MOUNTINFO_LINE.strip()

    def test_ceph_rocky_8_7_mount_parsed_correctly(self):
        entries = parse_mountinfo_alike(_lines(CEPH_ROCKY_8_7_MOUNTINFO_LINE))
        e = entries[0]
        assert e["mount_point"] == "/mnt/data2"
        assert e["filesystem_type"] == "ceph"
        assert e["mount_source"] == (
            "168.10.10.9:5789,168.10.10.10:5789,168.10.10.11:5789,"
            "168.10.10.12:5789,168.10.10.13:5789:/"
        )

    def test_ceph_rocky_9_4_mount_parsed_correctly(self):
        entries = parse_mountinfo_alike(_lines(CEPH_ROCKY_9_4_MOUNTINFO_LINE))
        e = entries[0]
        assert e["mount_point"] == "/mnt/data3"
        assert e["filesystem_type"] == "ceph"
        assert e["mount_source"] == "admin@39bde8b9-3ab3-22f1-0b76-4cecefb8c925.data3=/"
        assert "mon_addr=" in ",".join(e["super_options"])

    def test_ceph_rocky_9_4_no_mon_addr_mount_parsed_correctly(self):
        entries = parse_mountinfo_alike(
            _lines(CEPH_ROCKY_9_4_NO_MON_ADDR_MOUNTINFO_LINE)
        )
        e = entries[0]
        assert e["mount_point"] == "/mnt/data3"
        assert e["filesystem_type"] == "ceph"
        assert e["mount_source"] == "admin@39bde8b9-3ab3-22f1-0b76-4cecefb8c925.data3=/"
        assert "mon_addr=" not in ",".join(e["super_options"])

    def test_nfs_mount_parsed_correctly(self):
        entries = parse_mountinfo_alike(_lines(NFS_MOUNTINFO_LINE))
        e = entries[0]
        assert e["mount_point"] == "/mnt/nfs"
        assert e["filesystem_type"] == "nfs4"
        assert e["mount_source"] == "nfsserver:/exports"

    def test_empty_input_returns_empty_list(self):
        entries = parse_mountinfo_alike(io.StringIO(""))
        assert entries == []

    def test_multiple_lines_parsed(self):
        entries = parse_mountinfo_alike(
            _lines(
                LOCAL_MOUNTINFO_LINE,
                NFS_MOUNTINFO_LINE,
                CEPH_ROCKY_8_7_MOUNTINFO_LINE,
                CEPH_ROCKY_9_4_MOUNTINFO_LINE,
                ROOT_MOUNTINFO_LINE,
            )
        )
        assert len(entries) == 5

    def test_no_super_options(self):
        # A line where the last_part has only two fields (no super options).
        line = "10 1 8:2 / /tmp rw - tmpfs tmpfs"
        entries = parse_mountinfo_alike(io.StringIO(line + "\n"))
        assert len(entries) == 1
        assert entries[0]["super_options"] == []

    def test_mount_options_split_into_list(self):
        entries = parse_mountinfo_alike(_lines(LOCAL_MOUNTINFO_LINE))
        opts = entries[0]["mount_options"]
        assert isinstance(opts, list)
        assert "rw" in opts
        assert "relatime" in opts

    def test_blank_lines_are_ignored(self):
        entries = parse_mountinfo_alike(
            _lines("", LOCAL_MOUNTINFO_LINE, "", "   ", NFS_MOUNTINFO_LINE)
        )
        assert len(entries) == 2
        assert entries[0]["mount_point"] == "/data"

    def test_octal_escapes_in_mount_point_are_unescaped(self):
        line = (
            "23 1 8:1 / /mnt/my\\040data rw,relatime shared:1"
            " - ext4 /dev/sda1 rw,errors=remount-ro"
        )
        entries = parse_mountinfo_alike(_lines(line))
        assert entries[0]["mount_point"] == "/mnt/my data"

    def test_malformed_line_is_skipped_with_warning(self, caplog):
        line = "this line has no separator"
        with caplog.at_level(logging.WARNING):
            entries = parse_mountinfo_alike(_lines(line))
        assert entries == []
        assert any("no '- ' separator" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# parse_mountinfo
# ---------------------------------------------------------------------------


class TestParseMountinfo:
    def test_reads_proc_self_mountinfo(self):
        data = LOCAL_MOUNTINFO_LINE + "\n"
        with patch("builtins.open", mock_open(read_data=data)) as mocked:
            entries = parse_mountinfo()
        assert mocked.call_args.args[0] == "/proc/self/mountinfo"
        assert len(entries) == 1

    def test_falls_back_to_proc_mountinfo_when_self_missing(self):
        data = LOCAL_MOUNTINFO_LINE + "\n"

        def side_effect(path, mode):
            if path == "/proc/self/mountinfo":
                raise FileNotFoundError
            return mock_open(read_data=data)()

        with patch("builtins.open", side_effect=side_effect):
            entries = parse_mountinfo()
        assert len(entries) == 1

    def test_raises_when_both_files_missing(self):
        with patch("builtins.open", side_effect=FileNotFoundError):
            with pytest.raises(FileNotFoundError):
                parse_mountinfo()

    def test_fallback_logs_warning(self, caplog):
        data = LOCAL_MOUNTINFO_LINE + "\n"

        def side_effect(path, mode):
            if path == "/proc/self/mountinfo":
                raise FileNotFoundError
            return mock_open(read_data=data)()

        with caplog.at_level(logging.WARNING):
            with patch("builtins.open", side_effect=side_effect):
                parse_mountinfo()
        assert any("/proc/self/mountinfo" in r.message for r in caplog.records)
