"""Parsing of the Linux ``/proc/self/mountinfo`` file.

This module is Linux-only: the mount table is read from ``/proc``. On other
platforms (e.g. macOS) ``parse_mountinfo`` will raise ``FileNotFoundError``.
The pure parsing helper :func:`parse_mountinfo_alike` works on any platform and
is what the unit tests exercise.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, IO, List

log = logging.getLogger(__name__)

MountEntry = Dict[str, Any]


def _unescape_mountinfo(value: str) -> str:
    """Undo the kernel's octal escapes used in ``/proc/mountinfo``.

    The kernel escapes spaces, tabs, newlines and backslashes as ``\\040``,
    ``\\011``, ``\\012`` and ``\\134`` respectively when writing the mountinfo
    fields.
    """
    return (
        value.replace("\\134", "\\")
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
    )


def parse_mountinfo_alike(fobj: IO[str]) -> List[MountEntry]:
    """Parse mountinfo-formatted lines into a list of mount entry dictionaries.

    This is the pure re-implementation of the ``/proc/self/mountinfo`` parser.
    It is tolerant of blank lines, and skips lines that cannot be split at the
    ``" - "`` separator or that are missing a mount source.

    Parameters
    ----------
    fobj : file-like
        An iterable of text lines, each formatted like a mountinfo line.

    Returns
    -------
    list of dict
        One dictionary per valid line. Keys: ``mount_id``, ``parent_id``,
        ``major_minor``, ``root``, ``mount_point``, ``mount_options``,
        ``filesystem_type``, ``mount_source``, ``super_options``, and the
        original ``raw_line``.
    """
    mount_entries: List[MountEntry] = []
    for line_no, line in enumerate(fobj, start=1):
        # Each line in /proc/mountinfo has a specific format. The fields are
        # space-separated, but some fields can contain spaces. The separator
        # between the optional fields and the rest is '- '.
        raw_line = line.strip()
        if not raw_line:
            continue

        parts = raw_line.split(" - ")
        if len(parts) < 2:
            log.warning(
                "Skipping malformed mountinfo line %d (no '- ' separator): %s",
                line_no,
                raw_line,
            )
            continue

        # Extract the first part (non-optional fields).
        first_part_fields = parts[0].split(" ")

        # Extract the last part (optional fields and remaining fields).
        last_part_fields = parts[1].split(" ")
        if len(last_part_fields) < 2:
            log.warning(
                "Skipping malformed mountinfo line %d (missing source): %s",
                line_no,
                raw_line,
            )
            continue

        # Example of extracting common fields. Adjust indices based on the
        # specific fields you need.
        mount_entry = {
            "mount_id": int(first_part_fields[0]),
            "parent_id": int(first_part_fields[1]),
            "major_minor": first_part_fields[2],
            "root": _unescape_mountinfo(first_part_fields[3]),
            "mount_point": _unescape_mountinfo(first_part_fields[4]),
            "mount_options": first_part_fields[5].split(","),
            # Filesystem type, mount source, and super options are in the
            # last part.
            "filesystem_type": last_part_fields[0],
            "mount_source": _unescape_mountinfo(last_part_fields[1]),
            "super_options": (
                last_part_fields[2].split(",") if len(last_part_fields) > 2 else []
            ),
            "raw_line": raw_line,
        }
        log.debug(
            "Parsed mountinfo line %d: mount_point=%s fstype=%s source=%s",
            line_no,
            mount_entry["mount_point"],
            mount_entry["filesystem_type"],
            mount_entry["mount_source"],
        )
        mount_entries.append(mount_entry)

    log.debug("Parsed %d mount entries from mountinfo input", len(mount_entries))
    return mount_entries


def parse_mountinfo() -> List[MountEntry]:
    """Parse ``/proc`` mountinfo and return one dict per mount entry.

    Reads ``/proc/self/mountinfo`` first and falls back to
    ``/proc/mountinfo``. Raises ``FileNotFoundError`` if neither is readable.

    Returns
    -------
    list of dict
        See :func:`parse_mountinfo_alike` for the entry shape.
    """
    try:
        with open("/proc/self/mountinfo", "r") as fobj:
            mount_entries = parse_mountinfo_alike(fobj)
        log.debug("Read mount table from /proc/self/mountinfo")
    except FileNotFoundError:
        log.warning("/proc/self/mountinfo not found. Trying /proc/mountinfo.")
        try:
            with open("/proc/mountinfo", "r") as fobj:
                mount_entries = parse_mountinfo_alike(fobj)
            log.debug("Read mount table from /proc/mountinfo")
        except FileNotFoundError:
            log.error("Could not open /proc/self/mountinfo nor /proc/mountinfo")
            raise

    return mount_entries
