"""Shared fixtures and helpers for the youvegotdata test suite."""

import configparser
import io

import pytest

from youvegotdata import Config

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
CEPH_ROCKY_8_7_MOUNTINFO_LINE = (
    "469 200 0:55 / /mnt/data2 rw,relatime shared:332 - ceph"
    " 168.10.10.9:5789,168.10.10.10:5789,168.10.10.11:5789,168.10.10.12:5789,"
    "168.10.10.13:5789:/ rw,name=admin,secret=<hidden>,acl"
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
ROOT_MOUNTINFO_LINE = "1 0 8:0 / / rw,relatime shared:0 - ext4 /dev/sda rw"


def _lines(*lines):
    """Return a file-like object containing the given lines."""
    return io.StringIO("\n".join(lines) + "\n")


def make_configparser(
    host="rmq.example.com",
    ip_mapping='{"/mnt/data2": ["168.10.10.9", "168.10.10.11", "168.10.10.12"]}',
):
    """Build a legacy raw configparser.ConfigParser for shim tests."""
    conf = configparser.ConfigParser(interpolation=None)
    conf["Settings"] = {"RMQ_HOST": host}
    conf["Data-store-mappings"] = {"CEPH_IPS": ip_mapping}
    return conf


def make_config(
    host="rmq.example.com",
    ip_mapping='{"/mnt/data2": ["168.10.10.9", "168.10.10.11", "168.10.10.12"]}',
):
    """Build a Config dataclass (as load_config would return)."""
    import json

    return Config(
        rmq_host=host,
        ceph_ips=json.loads(ip_mapping),
    )


@pytest.fixture
def config():
    return make_config()


@pytest.fixture
def configparser_config():
    return make_configparser()
