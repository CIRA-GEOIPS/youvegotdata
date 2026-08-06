"""Resolve a local filepath to a data store name and a store-relative path.

Mapping a filepath to a data store requires the mounted filesystem table on the
Linux host, so this module (like :mod:`youvegotdata.mountinfo`) is only useful
on Linux. The resolution logic itself is pure and unit-tested via mocked mount
tables.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from .mountinfo import MountEntry, parse_mountinfo

log = logging.getLogger(__name__)

CephMapping = Mapping[str, Sequence[str]]


def _mount_matches(filepath: str, mount_point: str) -> bool:
    """Check that ``filepath`` lives under ``mount_point`` (path-boundary aware)."""
    mp = mount_point.rstrip("/") or "/"
    if mp == "/":
        return filepath.startswith("/")
    return filepath == mp or filepath.startswith(mp + "/")


def _join_store_path(source_path: str, remainder: str) -> str:
    """Join a mount source's path with the filepath remainder, avoiding ``//``."""
    if not remainder:
        return source_path
    return os.path.join(source_path, remainder.lstrip("/"))


def resolve_ceph_store(
    filepath: str, mount: MountEntry, ceph_mapping: CephMapping
) -> Tuple[Optional[str], Optional[str]]:
    """Map a filepath on a Ceph mount to a store name and remote path.

    Ceph mounts differ by OS. On Rocky 8.7 the monitor IPs are embedded in
    ``mount_source`` (a comma-separated ``host:port`` list); on Rocky 9.4 they
    live in the ``mon_addr=`` super option. The ``ceph_mapping`` maps store
    names to the IP lists that uniquely identify them.

    Parameters
    ----------
    filepath : str
        The local path of the file.
    mount : dict
        A mount entry as produced by :func:`parse_mountinfo`.
    ceph_mapping : mapping
        Store name to list of IP addresses (as read from the config).

    Returns
    -------
    (str, str) or (None, None)
        The matching store name and the filepath, or ``(None, None)`` if no
        mapping matched.
    """
    log.debug(
        "Ceph Source: %-20s Mount Point: %-20s FS Type: %-10s Options: %s",
        mount["mount_source"],
        mount["mount_point"],
        mount["filesystem_type"],
        mount["super_options"],
    )

    data_store = None
    fpath = None
    iplist: Optional[List[str]] = None
    if "," in mount["mount_source"]:
        # The list of IPs is in the "mount_source".
        iplist = mount["mount_source"].split(",")

        dev_dir = iplist[-1].split(":")[-1]
        if dev_dir != "/":
            log.error("Found a ceph 'mount_source' that has a device dir: %s", dev_dir)

        # But trim off the ports.
        iplist = [ip.split(":")[0] for ip in iplist]
    else:
        # The list of IPs is in the "super_options" "mon_addr".
        ips_str = None
        for opt in mount["super_options"]:
            if opt.startswith("mon_addr="):
                ips_str = opt[len("mon_addr=") :]
                break

        if ips_str:
            iplist = ips_str.split("/")

            # Still trim off the ports.
            iplist = [ip.split(":")[0] for ip in iplist]

            if mount["mount_source"][-2:] != "=/":
                log.error(
                    "Found a ceph 'mount_source': %r that has a device dir",
                    mount["mount_source"],
                )

    if iplist:
        # Evaluate the most-specific mappings first so that a store whose IP
        # list is a subset of another's resolves deterministically.
        for store_name, ips_map in sorted(
            ceph_mapping.items(), key=lambda item: len(item[1]), reverse=True
        ):
            log.debug(
                "Trying ceph mapping %s=%s against mount IPs %s",
                store_name,
                list(ips_map),
                iplist,
            )
            # The ips in ips_map must all be in the "mount_source" list.
            if all(item in iplist for item in ips_map):
                # This is the needed mapping.
                data_store = "ceph-IPs:{}".format(",".join(ips_map))
                fpath = filepath
                log.info(
                    "Matched ceph store %r for filepath %r (mapping %s=%s)",
                    data_store,
                    filepath,
                    store_name,
                    list(ips_map),
                )
                break
    else:
        log.warning(
            "Ceph mount at %r has no IPs in mount_source or mon_addr; "
            "cannot resolve a data store.",
            mount["mount_point"],
        )

    # TODO: handle device dirs for ceph mounts.

    return data_store, fpath


def resolve_data_store(
    filepath: str, ceph_mapping: CephMapping
) -> Tuple[Optional[str], Optional[str]]:
    """Get the data store name and the absolute path from the data store.

    Symbolic links in ``filepath`` are resolved first. The mounted filesystem
    whose mount point is the longest path-boundary-aware prefix of the filepath
    wins; on equal lengths the later (topmost, active) mount entry wins. Ceph
    mounts take part in the same comparison but require a ``ceph_mapping`` IP
    match to resolve.

    Parameters
    ----------
    filepath : str
        The filepath argument given to the program. Must exist on the local
        machine.
    ceph_mapping : mapping
        Store name to list of IP addresses (as read from the config).

    Returns
    -------
    (str, str) or (None, None)
        The data store name and the filepath relative to that store. If no
        mount or Ceph mapping matches, ``(None, None)`` is returned.
    """
    # Any symbolic links in the filepath need to be resolved first.
    resolved_path = str(Path(filepath).resolve())
    if filepath != resolved_path:
        log.info("Symbolic links in filepath resolved. New path = %s", resolved_path)
        filepath = resolved_path

    # Read the /proc/self/mountinfo file to get the data store and mount point.
    # The mount point with the longest prefix match wins (ceph mounts take part
    # in the same comparison); on equal-length matches the later entry in
    # mountinfo is the active, topmost overmount and wins.
    data_store: Optional[str] = None
    fpath: Optional[str] = None
    best_mount: Optional[MountEntry] = None
    best_mp_len = -1
    log.debug("Currently mounted filesystems:")
    for mount in parse_mountinfo():
        log.debug(
            "Source: %-20s Mount Point: %-20s FS Type: %-10s Options: %s",
            mount["mount_source"],
            mount["mount_point"],
            mount["filesystem_type"],
            mount["super_options"],
        )
        if mount["mount_point"] == "/":
            # Skip this - every path will match it.
            continue
        if not _mount_matches(filepath, mount["mount_point"]):
            continue
        mp_len = len(mount["mount_point"])
        if mp_len < best_mp_len:
            continue
        # Longest match wins; ties go to the later (active/topmost) mount.
        best_mp_len = mp_len
        best_mount = mount

    if best_mount is None:
        log.warning(
            "No mounted filesystem contains the resolved filepath %r. "
            "No data store can be resolved.",
            filepath,
        )
        return data_store, fpath

    log.debug(
        "Best matching mount: %s (%s)",
        best_mount["mount_point"],
        best_mount["filesystem_type"],
    )

    if best_mount["filesystem_type"] == "ceph":
        return resolve_ceph_store(filepath, best_mount, ceph_mapping)

    dev_dir = best_mount["mount_source"].split(":")
    data_store = dev_dir[0]
    if len(dev_dir) == 2:
        # There is a path associated with the mount_source. Replace the mount
        # point with this path.
        remainder = filepath[len(best_mount["mount_point"]) :]
        fpath = _join_store_path(dev_dir[1], remainder)
    else:
        fpath = filepath

    log.info(
        "Resolved filepath %r to data_store %r, store path %r (mount %r)",
        filepath,
        data_store,
        fpath,
        best_mount["mount_point"],
    )
    return data_store, fpath


__all__ = [
    "resolve_data_store",
    "resolve_ceph_store",
    "_mount_matches",
    "_join_store_path",
]
