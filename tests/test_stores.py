"""Unit tests for youvegotdata.stores."""

import json
from unittest.mock import patch

from youvegotdata import parse_mountinfo_alike, resolve_data_store

from conftest import (
    CEPH_ROCKY_8_7_MOUNTINFO_LINE,
    CEPH_ROCKY_9_4_MOUNTINFO_LINE,
    CEPH_ROCKY_9_4_NO_MON_ADDR_MOUNTINFO_LINE,
    LOCAL_MOUNTINFO_LINE,
    NFS_MOUNTINFO_LINE,
    ROOT_MOUNTINFO_LINE,
    _lines,
)

CEPH_MAPPING = {"/mnt/data2": ["168.10.10.9", "168.10.10.11", "168.10.10.12"]}


class TestResolveDataStore:
    def _mock_parse(self, *lines):
        """Return a list of mount entries parsed from the given lines."""
        return parse_mountinfo_alike(_lines(*lines))

    def test_local_mount_returns_device_and_filepath(self):
        mounts = self._mock_parse(LOCAL_MOUNTINFO_LINE)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, fpath = resolve_data_store(
                "/data/subdir/file.hdf", CEPH_MAPPING
            )
        assert data_store == "/dev/sda1"
        assert fpath == "/data/subdir/file.hdf"

    def test_ceph_rocky_8_7_mount_returns_server_and_remote_path(self):
        mounts = self._mock_parse(CEPH_ROCKY_8_7_MOUNTINFO_LINE)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, fpath = resolve_data_store(
                "/mnt/data2/subdir/file.hdf", CEPH_MAPPING
            )
        assert data_store == "ceph-IPs:168.10.10.9,168.10.10.11,168.10.10.12"
        assert fpath == "/mnt/data2/subdir/file.hdf"

    def test_ceph_rocky_9_4_mount_returns_server_and_remote_path(self):
        mounts = self._mock_parse(CEPH_ROCKY_9_4_MOUNTINFO_LINE)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, fpath = resolve_data_store(
                "/mnt/data3/subdir/file.hdf", CEPH_MAPPING
            )
        assert data_store == "ceph-IPs:168.10.10.9,168.10.10.11,168.10.10.12"
        assert fpath == "/mnt/data3/subdir/file.hdf"

    def test_ceph_rocky_9_4_no_mon_addr_mount_returns_none(self):
        mounts = self._mock_parse(CEPH_ROCKY_9_4_NO_MON_ADDR_MOUNTINFO_LINE)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, fpath = resolve_data_store(
                "/mnt/data3/subdir/file.hdf", CEPH_MAPPING
            )
        assert data_store is None
        assert fpath is None

    def test_nfs_mount_returns_server_and_remote_path(self):
        mounts = self._mock_parse(NFS_MOUNTINFO_LINE)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, fpath = resolve_data_store(
                "/mnt/nfs/subdir/file.hdf", CEPH_MAPPING
            )
        assert data_store == "nfsserver"
        # The mount_source path (/exports) should replace the mount point prefix
        assert fpath == "/exports/subdir/file.hdf"

    def test_no_matching_mount_returns_none(self):
        # Only the root mount, which is skipped.
        mounts = self._mock_parse(ROOT_MOUNTINFO_LINE)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, fpath = resolve_data_store("/unrelated/file.hdf", CEPH_MAPPING)
        assert data_store is None
        assert fpath is None

    def test_root_mount_is_skipped(self):
        # Even though "/" prefix-matches everything, it must be skipped.
        mounts = self._mock_parse(ROOT_MOUNTINFO_LINE, LOCAL_MOUNTINFO_LINE)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, _ = resolve_data_store("/data/file.hdf", CEPH_MAPPING)
        # /data mount should win, not the root mount
        assert data_store == "/dev/sda1"

    def test_longest_prefix_mount_wins(self):
        # /data and /data/archive are both valid prefixes; /data/archive is longer.
        archive_line = "24 23 8:2 / /data/archive rw,relatime - ext4 /dev/sdb1 rw"
        mounts = self._mock_parse(LOCAL_MOUNTINFO_LINE, archive_line)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, fpath = resolve_data_store(
                "/data/archive/file.hdf", CEPH_MAPPING
            )
        assert data_store == "/dev/sdb1"
        assert fpath == "/data/archive/file.hdf"

    @staticmethod
    def _ceph_line(mount_point, ips):
        """Build a Rocky 8.7-style ceph mountinfo line."""
        source = ",".join(f"{ip}:6789" for ip in ips) + ":/"
        return (
            "900 76 0:55 / "
            + mount_point
            + " rw,relatime shared:436 - ceph"
            + " "
            + source
            + " rw,name=admin,secret=<hidden>,acl"
        )

    def test_path_boundary_must_be_respected(self):
        # /data must not match a sibling like /datafoo
        mounts = self._mock_parse(LOCAL_MOUNTINFO_LINE)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, fpath = resolve_data_store("/datafoo/file.hdf", CEPH_MAPPING)
        assert data_store is None
        assert fpath is None

    def test_longest_ceph_mount_wins_regardless_of_order(self):
        short_ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
        long_ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"]
        ceph_mapping = {"short": short_ips, "long": long_ips}
        mounts = self._mock_parse(
            self._ceph_line("/mnt/data2", short_ips),
            self._ceph_line("/mnt/data2/archive", long_ips),
        )
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
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
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
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
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, _ = resolve_data_store("/mnt/data/file.hdf", {})
        assert data_store == "/dev/sdb1"

    def test_octal_escaped_mount_point_resolves(self):
        line = (
            "23 1 8:1 / /mnt/my\\040data rw,relatime shared:1"
            " - ext4 /dev/sda1 rw,errors=remount-ro"
        )
        mounts = self._mock_parse(line)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, _ = resolve_data_store("/mnt/my data/file.hdf", {})
        assert data_store == "/dev/sda1"

    def test_nfs_root_export_path_has_no_double_slash(self):
        line = "42 1 0:35 / /mnt/nfs rw,relatime shared:2 - nfs4 nfsserver:/ rw,vers=4"
        mounts = self._mock_parse(line)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, fpath = resolve_data_store("/mnt/nfs/sub/file.hdf", {})
        assert data_store == "nfsserver"
        assert fpath == "/sub/file.hdf"

    def test_ceph_mapping_accepts_config_dataclass_dict(self, config):
        # The Config.ceph_ips dict is a plain dict; ensure it works directly.
        mounts = self._mock_parse(CEPH_ROCKY_8_7_MOUNTINFO_LINE)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, _ = resolve_data_store("/mnt/data2/x.hdf", config.ceph_ips)
        assert data_store == "ceph-IPs:168.10.10.9,168.10.10.11,168.10.10.12"

    def test_ceph_ips_loaded_via_json_in_config(self):
        # Build the mapping exactly as load_config normalizes it.
        raw = '{"storeA": ["10.1.1.1", "10.1.1.2", "10.1.1.3"]}'
        ceph_mapping = json.loads(raw)
        line = self._ceph_line("/mnt/ceph", ["10.1.1.1", "10.1.1.2", "10.1.1.3"])
        mounts = self._mock_parse(line)
        with patch("youvegotdata.stores.parse_mountinfo", return_value=mounts):
            data_store, _ = resolve_data_store("/mnt/ceph/f.hdf", ceph_mapping)
        assert data_store == "ceph-IPs:10.1.1.1,10.1.1.2,10.1.1.3"
