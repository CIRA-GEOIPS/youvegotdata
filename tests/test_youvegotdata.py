"""Unit tests for youvegotdata.youvegotdata."""

import configparser
import io
import json
import ast
import logging
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest
import pika

from youvegotdata.youvegotdata import (
    main,
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

# A Ceph mount where mount_source is a Rocky 8.7 type of mount.
CEPH_ROCKY_8_7_MOUNTINFO_LINE = ( "469 200 0:55 / /mnt/data2 rw,relatime shared:332 - ceph"
    " 168.10.10.9:5789,168.10.10.10:5789,168.10.10.11:5789,168.10.10.12:5789,168.10.10.13:5789:/"
    " rw,name=admin,secret=<hidden>,acl"
)

# A Ceph mount where mount_source is a Rocky 9.4 type of mount.
CEPH_ROCKY_9_4_MOUNTINFO_LINE = (
    "828 76 0:61 / /mnt/data3 rw,relatime shared:436 - ceph"
    " admin@39bde8b9-3ab3-22f1-0b76-4cecefb8c925.data3=/"
    " rw,name=admin,secret=<hidden>,ms_mode=prefer-crc,acl,"
    "mon_addr=10.168.42.192:4400/10.168.42.193:4400/168.10.10.9:4400/"
    "168.10.10.11:4400/168.10.10.12:4400"
)

# A Ceph mount where mount_source is a Rocky 9.4 type of mount, but with no
# mon_addr.
CEPH_ROCKY_9_4_NO_MON_ADDR_MOUNTINFO_LINE = (
    "828 76 0:61 / /mnt/data3 rw,relatime shared:436 - ceph"
    " admin@39bde8b9-3ab3-22f1-0b76-4cecefb8c925.data3=/"
    " rw,name=admin,secret=<hidden>,ms_mode=prefer-crc,acl"
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

    def test_ceph_rocky_8_7_mount_parsed_correctly(self):
        entries = parse_mountinfo_alike(_lines(CEPH_ROCKY_8_7_MOUNTINFO_LINE))
        e = entries[0]
        assert e["mount_point"] == "/mnt/data2"
        assert e["filesystem_type"] == "ceph"
        assert e["mount_source"] == "168.10.10.9:5789,168.10.10.10:5789,168.10.10.11:5789,168.10.10.12:5789,168.10.10.13:5789:/"

    def test_ceph_rocky_9_4_mount_parsed_correctly(self):
        entries = parse_mountinfo_alike(_lines(CEPH_ROCKY_9_4_MOUNTINFO_LINE))
        e = entries[0]
        assert e["mount_point"] == "/mnt/data3"
        assert e["filesystem_type"] == "ceph"
        assert e["mount_source"] == "admin@39bde8b9-3ab3-22f1-0b76-4cecefb8c925.data3=/"
        assert "mon_addr=" in ",".join(e["super_options"])

    def test_ceph_rocky_9_4_no_mon_addr_mount_parsed_correctly(self):
        entries = parse_mountinfo_alike(_lines(CEPH_ROCKY_9_4_NO_MON_ADDR_MOUNTINFO_LINE))
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
            _lines(LOCAL_MOUNTINFO_LINE, NFS_MOUNTINFO_LINE, CEPH_ROCKY_8_7_MOUNTINFO_LINE, CEPH_ROCKY_9_4_MOUNTINFO_LINE, ROOT_MOUNTINFO_LINE)
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
    def _make_ceph_mapping(self,
      ip_mapping='{"/mnt/data2": ["168.10.10.9", "168.10.10.11", "168.10.10.12"]}'):
        ceph_ips = ast.literal_eval(ip_mapping)
        return ceph_ips

    def _mock_parse(self, *lines):
        """Return a list of mount entries parsed from the given lines."""
        return parse_mountinfo_alike(_lines(*lines))

    def test_local_mount_returns_device_and_filepath(self):
        ceph_ips = self._make_ceph_mapping()
        mounts = self._mock_parse(LOCAL_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/data/subdir/file.hdf", ceph_ips)
        assert data_store == "/dev/sda1"
        assert fpath == "/data/subdir/file.hdf"

    def test_ceph_rocky_8_7_mount_returns_server_and_remote_path(self):
        ceph_ips = self._make_ceph_mapping()
        mounts = self._mock_parse(CEPH_ROCKY_8_7_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/mnt/data2/subdir/file.hdf", ceph_ips)
        assert data_store == "ceph-IPs:168.10.10.9,168.10.10.11,168.10.10.12"
        # The mount_source path (/exports) should replace the mount point prefix
        assert fpath == "/mnt/data2/subdir/file.hdf"

    def test_ceph_rocky_9_4_mount_returns_server_and_remote_path(self):
        ceph_ips = self._make_ceph_mapping()
        mounts = self._mock_parse(CEPH_ROCKY_9_4_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/mnt/data3/subdir/file.hdf", ceph_ips)
        assert data_store == "ceph-IPs:168.10.10.9,168.10.10.11,168.10.10.12"
        # The mount_source path (/exports) should replace the mount point prefix
        assert fpath == "/mnt/data3/subdir/file.hdf"

    def test_ceph_rocky_9_4_no_mon_addr_mount_returns_server_and_remote_path(self):
        ceph_ips = self._make_ceph_mapping()
        mounts = self._mock_parse(CEPH_ROCKY_9_4_NO_MON_ADDR_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/mnt/data3/subdir/file.hdf", ceph_ips)
        assert data_store is None
        assert fpath is None

    def test_nfs_mount_returns_server_and_remote_path(self):
        ceph_ips = self._make_ceph_mapping()
        mounts = self._mock_parse(NFS_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/mnt/nfs/subdir/file.hdf", ceph_ips)
        assert data_store == "nfsserver"
        # The mount_source path (/exports) should replace the mount point prefix
        assert fpath == "/exports/subdir/file.hdf"

    def test_no_matching_mount_returns_none(self):
        # Only the root mount, which is skipped.
        ceph_ips = self._make_ceph_mapping()
        mounts = self._mock_parse(ROOT_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/unrelated/file.hdf", ceph_ips)
        assert data_store is None
        assert fpath is None

    def test_root_mount_is_skipped(self):
        # Even though "/" prefix-matches everything, it must be skipped.
        ceph_ips = self._make_ceph_mapping()
        mounts = self._mock_parse(ROOT_MOUNTINFO_LINE, LOCAL_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, _ = resolve_data_store("/data/file.hdf", ceph_ips)
        # /data mount should win, not the root mount
        assert data_store == "/dev/sda1"

    def test_longest_prefix_mount_wins(self):
        # /data and /data/archive are both valid prefixes; /data/archive is longer.
        ceph_ips = self._make_ceph_mapping()
        archive_line = (
            "24 23 8:2 / /data/archive rw,relatime - ext4 /dev/sdb1 rw"
        )
        mounts = self._mock_parse(LOCAL_MOUNTINFO_LINE, archive_line)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/data/archive/file.hdf", ceph_ips)
        assert data_store == "/dev/sdb1"
        assert fpath == "/data/archive/file.hdf"

    @staticmethod
    def _ceph_line(mount_point, ips):
        """Build a Rocky 8.7-style ceph mountinfo line."""
        source = ",".join(f"{ip}:6789" for ip in ips) + ":/"
        return (
            "900 76 0:55 / " + mount_point + " rw,relatime shared:436 - ceph"
            " " + source + " rw,name=admin,secret=<hidden>,acl"
        )

    def test_path_boundary_must_be_respected(self):
        # /data must not match a sibling like /datafoo
        ceph_ips = self._make_ceph_mapping()
        mounts = self._mock_parse(LOCAL_MOUNTINFO_LINE)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/datafoo/file.hdf", ceph_ips)
        assert data_store is None
        assert fpath is None

    def test_longest_ceph_mount_wins_regardless_of_order(self):
        # The shorter ceph mount appears first, but the longest prefix must win.
        short_ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
        long_ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"]
        ceph_mapping = {"short": short_ips, "long": long_ips}
        mounts = self._mock_parse(
            self._ceph_line("/mnt/data2", short_ips),
            self._ceph_line("/mnt/data2/archive", long_ips),
        )
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store(
                "/mnt/data2/archive/file.hdf", ceph_mapping
            )
        assert data_store == "ceph-IPs:10.0.0.1,10.0.0.2,10.0.0.3,10.0.0.4"
        assert fpath == "/mnt/data2/archive/file.hdf"

    def test_longer_non_ceph_mount_beats_shorter_ceph_mount(self):
        short_ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
        ceph_mapping = {"short": short_ips}
        ceph_line = self._ceph_line("/mnt/data2", short_ips)
        ext_deep_line = "24 23 8:2 / /mnt/data2/deep rw,relatime - ext4 /dev/sdb2 rw"
        mounts = self._mock_parse(ceph_line, ext_deep_line)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store(
                "/mnt/data2/deep/file.hdf", ceph_mapping
            )
        assert data_store == "/dev/sdb2"
        assert fpath == "/mnt/data2/deep/file.hdf"

    def test_overmounted_path_uses_later_entry(self):
        # Same mount point twice: the later (topmost) mount is the active one.
        bottom = "23 1 8:1 / /mnt/data rw,relatime shared:1 - ext4 /dev/sda1 rw"
        top = "24 23 8:2 / /mnt/data rw,relatime shared:2 - ext4 /dev/sdb1 rw"
        mounts = self._mock_parse(bottom, top)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, _ = resolve_data_store("/mnt/data/file.hdf", {})
        assert data_store == "/dev/sdb1"

    def test_octal_escaped_mount_point_resolves(self):
        line = (
            "23 1 8:1 / /mnt/my\\040data rw,relatime shared:1"
            " - ext4 /dev/sda1 rw,errors=remount-ro"
        )
        mounts = self._mock_parse(line)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, _ = resolve_data_store("/mnt/my data/file.hdf", {})
        assert data_store == "/dev/sda1"

    def test_nfs_root_export_path_has_no_double_slash(self):
        line = "42 1 0:35 / /mnt/nfs rw,relatime shared:2 - nfs4 nfsserver:/ rw,vers=4"
        mounts = self._mock_parse(line)
        with patch(
            "youvegotdata.youvegotdata.parse_mountinfo", return_value=mounts
        ):
            data_store, fpath = resolve_data_store("/mnt/nfs/sub/file.hdf", {})
        assert data_store == "nfsserver"
        assert fpath == "/sub/file.hdf"


# ---------------------------------------------------------------------------
# produce_notification
# ---------------------------------------------------------------------------


class TestProduceNotification:
    def _make_config(self, host="rmq.example.com",
      ip_mapping='{"/mnt/data2": ["168.10.10.9", "168.10.10.11", "168.10.10.12"]}'):
        config = configparser.ConfigParser()
        config["Settings"] = {"RMQ_HOST": host}
        config["Data-store-mappings"] = {"CEPH_IPS": ip_mapping}
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
        logging.debug(f"defaults: {defaults}")
        ceph_ips = ast.literal_eval(defaults["config"]["Data-store-mappings"]["CEPH_IPS"])
        assert ceph_ips["/mnt/data2"] == ["168.10.10.9", "168.10.10.11", "168.10.10.12"]

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

    def test_returns_true_when_notification_sent(self):
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
            ):
                result = produce_notification(
                    config=self._make_config(),
                    filepath="/data/file.hdf",
                    product="VIIRS",
                    version="1.0",
                )
        assert result is True

    def test_bad_ceph_ips_json_returns_false(self):
        config = self._make_config(ip_mapping='{"/mnt/data2": ["10.0.0.1",]}')
        with patch("youvegotdata.youvegotdata.resolve_data_store") as mock_resolve:
            result = produce_notification(
                config=config,
                filepath="/data/file.hdf",
                product=None,
                version=None,
            )
        assert result is False
        mock_resolve.assert_not_called()

    def test_unresolved_store_returns_false(self):
        with patch(
            "youvegotdata.youvegotdata.resolve_data_store",
            return_value=(None, None),
        ):
            result = produce_notification(
                config=self._make_config(),
                filepath="/data/file.hdf",
                product=None,
                version=None,
            )
        assert result is False

    def test_pika_failure_raises_and_logs_error(self, caplog):
        with patch(
            "youvegotdata.youvegotdata.resolve_data_store",
            return_value=("/dev/sda1", "/data/file.hdf"),
        ):
            with patch(
                "youvegotdata.youvegotdata.pika.BlockingConnection",
                side_effect=pika.exceptions.AMQPConnectionError("down"),
            ):
                with caplog.at_level(logging.ERROR):
                    with pytest.raises(pika.exceptions.AMQPConnectionError):
                        produce_notification(
                            config=self._make_config(),
                            filepath="/data/file.hdf",
                            product=None,
                            version=None,
                        )
        assert any("RabbitMQ error" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


class TestMain:
    _VALID_CFG = (
        "[Settings]\nRMQ_HOST = rmq.example.com\n"
        '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1"]}\n'
    )

    def _invoke(self, tmp_path, monkeypatch, config_text=_VALID_CFG, create_file=True):
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir(exist_ok=True)
        if config_text is not None:
            (cfg_dir / "config.ini").write_text(config_text)
        monkeypatch.setattr(
            "youvegotdata.youvegotdata.user_config_dir", lambda app: str(cfg_dir)
        )
        filepath = str(tmp_path / "data" / "file.hdf")
        if create_file:
            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            Path(filepath).touch()
        monkeypatch.setattr("sys.argv", ["ygd", filepath])
        return filepath

    def test_help_shows_prog_and_description(self, capsys):
        with patch("sys.argv", ["ygd", "--help"]):
            with pytest.raises(SystemExit) as exc:
                main()
        assert exc.value.code == 0
        out = capsys.readouterr().out
        assert "usage: ygd" in out
        assert "new file notification" in out

    def test_missing_config_exits_1(self, tmp_path, monkeypatch):
        self._invoke(tmp_path, monkeypatch, config_text=None, create_file=False)
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1

    def test_missing_rmq_host_exits_1(self, tmp_path, monkeypatch):
        cfg = '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1"]}\n'
        self._invoke(tmp_path, monkeypatch, config_text=cfg)
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1

    def test_missing_ceph_ips_exits_1(self, tmp_path, monkeypatch):
        cfg = "[Settings]\nRMQ_HOST = rmq.example.com\n"
        self._invoke(tmp_path, monkeypatch, config_text=cfg)
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1

    def test_missing_file_exits_1(self, tmp_path, monkeypatch):
        self._invoke(tmp_path, monkeypatch, create_file=False)
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1

    def test_bad_ceph_ips_json_exits_1(self, tmp_path, monkeypatch):
        cfg = (
            "[Settings]\nRMQ_HOST = rmq.example.com\n"
            '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1",]}\n'
        )
        self._invoke(tmp_path, monkeypatch, config_text=cfg)
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1

    def test_unresolved_store_exits_1(self, tmp_path, monkeypatch):
        self._invoke(tmp_path, monkeypatch)
        with patch(
            "youvegotdata.youvegotdata.resolve_data_store",
            return_value=(None, None),
        ):
            with pytest.raises(SystemExit) as exc:
                main()
        assert exc.value.code == 1

    def test_percent_in_config_value_is_not_interpolated(self, tmp_path, monkeypatch):
        # '%' must not trigger configparser interpolation.
        cfg = (
            "[Settings]\nRMQ_HOST = rmq-%s.example.com\n"
            '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1"]}\n'
        )
        self._invoke(tmp_path, monkeypatch, config_text=cfg)
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
            ):
                main()
        # main() exits 0 without raising InterpolationMissingOptionError
