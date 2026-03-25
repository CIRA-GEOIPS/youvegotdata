"""Unit tests for youvegotdata.youvegotdata."""

import configparser
import io
import json
import logging
from unittest.mock import MagicMock, mock_open, patch

import pytest

from youvegotdata.youvegotdata import (
    parse_mountinfo,
    parse_mountinfo_alike,
    produce_notification,
    resolve_data_store,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# A realistic /proc/self/mountinfo line for a local ext4 filesystem.
# Format: mount_id parent_id major:minor root mount_point mount_options
#         [optional-fields] - filesystem_type mount_source super_options
LOCAL_MOUNTINFO_LINE = (
    "23 1 8:1 / /data rw,relatime shared:1 - ext4 /dev/sda1 rw,errors=remount-ro"
)

# An NFS mount where mount_source contains a host:path pair.
NFS_MOUNTINFO_LINE = (
    "42 1 0:35 / /mnt/nfs rw,relatime shared:2 - nfs4 nfsserver:/exports rw,vers=4"
)

# The root mount (should be skipped by resolve_data_store).
ROOT_MOUNTINFO_LINE = (
    "1 0 8:0 / / rw,relatime shared:0 - ext4 /dev/sda rw"
)


def _lines(*lines):
    """Return a file-like object containing the given lines."""
    return io.StringIO("\n".join(lines) + "\n")


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
            _lines(LOCAL_MOUNTINFO_LINE, NFS_MOUNTINFO_LINE, ROOT_MOUNTINFO_LINE)
        )
        assert len(entries) == 3

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


# ---------------------------------------------------------------------------
# parse_mountinfo
# ---------------------------------------------------------------------------


class TestParseMountinfo:
    def test_reads_proc_self_mountinfo(self):
        data = LOCAL_MOUNTINFO_LINE + "\n"
        with patch("builtins.open", mock_open(read_data=data)) as mocked:
            entries = parse_mountinfo()
        mocked.assert_called_once_with("/proc/self/mountinfo", "r")
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


# ---------------------------------------------------------------------------
# resolve_data_store
# ---------------------------------------------------------------------------


class TestResolveDataStore:
    def _mock_parse(self, *lines):
        """Return a list of mount entries parsed from the given lines."""
        return parse_mountinfo_alike(_lines(*lines))

    def test_local_mount_returns_device_and_filepath(self):
        mounts = self._mock_parse(LOCAL_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/data/subdir/file.hdf")
        assert data_store == "/dev/sda1"
        assert fpath == "/data/subdir/file.hdf"

    def test_nfs_mount_returns_server_and_remote_path(self):
        mounts = self._mock_parse(NFS_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/mnt/nfs/subdir/file.hdf")
        assert data_store == "nfsserver"
        # The mount_source path (/exports) should replace the mount point prefix
        assert fpath == "/exports/subdir/file.hdf"

    def test_no_matching_mount_returns_none(self):
        # Only the root mount, which is skipped.
        mounts = self._mock_parse(ROOT_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/unrelated/file.hdf")
        assert data_store is None
        assert fpath is None

    def test_root_mount_is_skipped(self):
        # Even though "/" prefix-matches everything, it must be skipped.
        mounts = self._mock_parse(ROOT_MOUNTINFO_LINE, LOCAL_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, _ = resolve_data_store("/data/file.hdf")
        # /data mount should win, not the root mount
        assert data_store == "/dev/sda1"

    def test_longest_prefix_mount_wins(self):
        # /data and /data/archive are both valid prefixes; /data/archive is longer.
        archive_line = (
            "24 23 8:2 / /data/archive rw,relatime - ext4 /dev/sdb1 rw"
        )
        mounts = self._mock_parse(LOCAL_MOUNTINFO_LINE, archive_line)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/data/archive/file.hdf")
        assert data_store == "/dev/sdb1"
        assert fpath == "/data/archive/file.hdf"


# ---------------------------------------------------------------------------
# produce_notification
# ---------------------------------------------------------------------------


class TestProduceNotification:
    def _make_config(self, host="rmq.example.com"):
        config = configparser.ConfigParser()
        config["Settings"] = {"RMQ_HOST": host}
        return config

    def _run(self, **kwargs):
        """Run produce_notification with sensible defaults, mocking pika."""
        defaults = dict(
            config=self._make_config(),
            filepath="/data/file.hdf",
            product="VIIRS",
            version="1.0",
            start_time="2024-01-01T00:00:00",
            end_time="2024-01-01T01:00:00",
            length=1024,
            checksum="abc123",
            checksum_type="md5",
        )
        defaults.update(kwargs)

        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel

        with patch(
            "youvegotdata.youvegotdata.resolve_data_store",
            return_value=("/dev/sda1", "/data/file.hdf"),
        ):
            with patch(
                "youvegotdata.youvegotdata.pika.BlockingConnection",
                return_value=mock_connection,
            ) as mock_bc:
                produce_notification(**defaults)

        return mock_bc, mock_connection, mock_channel

    def test_connection_opened_with_correct_host(self):
        mock_bc, _, _ = self._run()
        call_args = mock_bc.call_args
        conn_params = call_args[0][0]
        assert conn_params.host == "rmq.example.com"

    def test_queue_declared_durable(self):
        _, _, mock_channel = self._run()
        mock_channel.queue_declare.assert_called_once_with(
            queue="file_notif_queue", durable=True
        )

    def test_message_published_to_correct_queue(self):
        _, _, mock_channel = self._run()
        mock_channel.basic_publish.assert_called_once()
        kwargs = mock_channel.basic_publish.call_args.kwargs
        assert kwargs["routing_key"] == "file_notif_queue"
        assert kwargs["exchange"] == ""

    def test_message_body_is_valid_json(self):
        _, _, mock_channel = self._run()
        body = mock_channel.basic_publish.call_args.kwargs["body"]
        msg = json.loads(body)
        assert isinstance(msg, dict)

    def test_message_contains_expected_fields(self):
        _, _, mock_channel = self._run()
        body = mock_channel.basic_publish.call_args.kwargs["body"]
        msg = json.loads(body)
        assert msg["data_store"] == "/dev/sda1"
        assert msg["filepath"] == "/data/file.hdf"
        assert msg["product"] == "VIIRS"
        assert msg["version"] == "1.0"
        assert msg["checksum"] == "abc123"
        assert msg["checksum_type"] == "md5"

    def test_optional_fields_default_to_none(self):
        _, _, mock_channel = self._run(
            start_time=None,
            end_time=None,
            length=None,
            checksum=None,
            checksum_type=None,
        )
        body = mock_channel.basic_publish.call_args.kwargs["body"]
        msg = json.loads(body)
        assert msg["start_time"] is None
        assert msg["end_time"] is None
        assert msg["length"] is None

    def test_connection_closed_after_publish(self):
        _, mock_connection, _ = self._run()
        mock_connection.close.assert_called_once()

    def test_message_delivery_mode_is_persistent(self):
        import pika as pika_mod

        _, _, mock_channel = self._run()
        props = mock_channel.basic_publish.call_args.kwargs["properties"]
        # BasicProperties stores delivery_mode as an integer; compare via .value
        assert props.delivery_mode == pika_mod.DeliveryMode.Persistent.value
