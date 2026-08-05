#!/usr/bin/env python
# coding: utf-8

# Stock modules
import os
import sys
import logging
import argparse
import pika
import json
import configparser
from platformdirs import user_config_dir
from pathlib import Path

DESCRIPTION = """
Allows a data ingest process to send a new file notification to the Data
Inventory RabbitMQ server. The notification will ultimately be used to add the
file metadata to the Data Inventory DB.
"""

log = logging.getLogger(__name__)

APP_NAME = "youvegotdata"

def _unescape_mountinfo(value):
    """Undo the kernel's octal escapes used in /proc/mountinfo.

    The kernel escapes spaces, tabs, newlines and backslashes as \\040, \\011,
    \\012 and \\134 respectively when writing the mountinfo fields.
    """
    return (
        value.replace("\\134", "\\")
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
    )


def parse_mountinfo_alike(fobj):
    mount_entries = []
    for line in fobj:
        # Each line in /proc/mountinfo has a specific format
        # The fields are space-separated, but some fields can contain spaces
        # The separator between the optional fields and the rest is '- '
        raw_line = line.strip()
        if not raw_line:
            continue

        parts = raw_line.split(' - ')
        if len(parts) < 2:
            log.warning(f"Skipping malformed mountinfo line (no '- ' separator): {raw_line}")
            continue

        # Extract the first part (non-optional fields)
        first_part_fields = parts[0].split(' ')

        # Extract the last part (optional fields and remaining fields)
        last_part_fields = parts[1].split(' ')
        if len(last_part_fields) < 2:
            log.warning(f"Skipping malformed mountinfo line (missing source): {raw_line}")
            continue

        # Example of extracting common fields
        # Adjust indices based on the specific fields you need
        mount_id = int(first_part_fields[0])
        parent_id = int(first_part_fields[1])
        major_minor = first_part_fields[2]
        root = _unescape_mountinfo(first_part_fields[3])
        mount_point = _unescape_mountinfo(first_part_fields[4])
        mount_options = first_part_fields[5].split(',')

        # Filesystem type, mount source, and super options are in the last part
        filesystem_type = last_part_fields[0]
        mount_source = _unescape_mountinfo(last_part_fields[1])
        super_options = last_part_fields[2].split(',') if len(last_part_fields) > 2 else []

        mount_entry = {
            "mount_id": mount_id,
            "parent_id": parent_id,
            "major_minor": major_minor,
            "root": root,
            "mount_point": mount_point,
            "mount_options": mount_options,
            "filesystem_type": filesystem_type,
            "mount_source": mount_source,
            "super_options": super_options,
            "raw_line": raw_line
        }
        mount_entries.append(mount_entry)

    return mount_entries


def parse_mountinfo():
    """
    Parses /proc/self/mountinfo (or /proc/mountinfo) returns a dictionary for each entry.
    """
    try:
        with open("/proc/self/mountinfo", "r") as fobj:
            mount_entries = parse_mountinfo_alike(fobj)
    except FileNotFoundError:
        log.warning("/proc/self/mountinfo not found. Trying /proc/mountinfo.")
        try:
            with open("/proc/mountinfo", "r") as fobj:
                mount_entries = parse_mountinfo_alike(fobj)
        except FileNotFoundError:
            log.error("Could not open /proc/self/mountinfo nor /proc/mountinfo")
            raise

    return mount_entries


def resolve_ceph_store(filepath, mount, ceph_mapping):
    log.debug(
        f"Ceph Source: {mount['mount_source']:<20} Mount Point: {mount['mount_point']:<20} FS Type: {mount['filesystem_type']:<10} Options: {mount['super_options']}"
    )

    data_store = None
    fpath = None
    iplist = None
    if "," in mount['mount_source']:
        # The list of IPs is in the "mount_source"
        iplist = mount['mount_source'].split(",")

        dev_dir = iplist[-1].split(":")[-1]
        if dev_dir != "/":
            log.error(
                f"Found a ceph 'mount_source' that has a device dir:"
                f" {dev_dir}"
            )

        # But trim off the ports
        iplist = [ip.split(":")[0] for ip in iplist]
    else:
        # The list of IPs is in the "super_options" "mon_addr"
        ips_str = None
        for opt in mount["super_options"]:
            if opt.startswith("mon_addr="):
                ips_str = opt[len("mon_addr="):]
                break

        if ips_str:
            iplist = ips_str.split("/")

            # Still trim off the ports
            iplist = [ip.split(":")[0] for ip in iplist]

            if mount['mount_source'][-2:] != "=/":
                log.error(
                    f"Found a ceph 'mount_source': \"{mount['mount_source']}\""
                    f" that has a device dir"
                )

    if iplist:
        # Evaluate the most-specific mappings first so that a store whose IP
        # list is a subset of another's resolves deterministically.
        for ips_map in sorted(ceph_mapping.values(), key=len, reverse=True):
            # The ips in ips_map must all be in the "mount_source" list
            if all(item in iplist for item in ips_map):
                # This is the needed mapping
                data_store = f"ceph-IPs:{','.join(ips_map)}"
                fpath = filepath
                break

    # TODO, handle device dirs for ceph mounts

    return data_store, fpath


def _mount_matches(filepath, mount_point):
    """Check that filepath lives under mount_point (path-boundary aware)."""
    mp = mount_point.rstrip("/") or "/"
    if mp == "/":
        return filepath.startswith("/")
    return filepath == mp or filepath.startswith(mp + "/")


def _join_store_path(source_path, remainder):
    """Join a mount source's path with the filepath remainder, avoiding `//`."""
    if not remainder:
        return source_path
    return os.path.join(source_path, remainder.lstrip("/"))


def resolve_data_store(filepath, ceph_mapping):
    """
    Get the data store name and the absolute path from the data store.
    filepath: The filepath argument given to the program
    """
    # Any symbolic links in the filepath need to be resolved first
    resolved_path = str(Path(filepath).resolve())
    if filepath != resolved_path:
        log.info(
          f"Symbolic links in filepath resolved. New path = {resolved_path}"
        )
        filepath = resolved_path

    # Read the /proc/self/mountinfo file to get the data store and mount point.
    # The mount point with the longest prefix match wins (ceph mounts take part
    # in the same comparison); on equal-length matches the later entry in
    # mountinfo is the active, topmost overmount and wins.
    data_store = None
    fpath = None
    best_mount = None
    best_mp_len = -1
    log.debug("Currently mounted filesystems:")
    for mount in parse_mountinfo():
        log.debug(
            f"Source: {mount['mount_source']:<20} Mount Point: {mount['mount_point']:<20} FS Type: {mount['filesystem_type']:<10} Options: {mount['super_options']}"
        )
        if mount["mount_point"] == "/":
            # Skip this - every path will match it
            continue
        if not _mount_matches(filepath, mount["mount_point"]):
            continue
        mp_len = len(mount["mount_point"])
        if mp_len < best_mp_len:
            continue
        # Longest match wins; ties go to the later (active/topmost) mount
        best_mp_len = mp_len
        best_mount = mount

    if best_mount is None:
        return data_store, fpath

    if best_mount["filesystem_type"] == "ceph":
        return resolve_ceph_store(filepath, best_mount, ceph_mapping)

    dev_dir = best_mount["mount_source"].split(":")
    data_store = dev_dir[0]
    if len(dev_dir) == 2:
        # There is a path associated with the mount_source. Replace the mount
        # point with this path.
        remainder = filepath[len(best_mount["mount_point"]):]
        fpath = _join_store_path(dev_dir[1], remainder)
    else:
        fpath = filepath

    return data_store, fpath


def produce_notification(
    config,
    filepath,
    product,
    version,
    start_time=None,
    end_time=None,
    length=None,
    checksum=None,
    checksum_type=None,
):
    """
    Send a "Fair Dispatch" message via RabbitMQ.

    Returns True if the notification was sent, False otherwise.
    """

    log.info(f'RMQ_HOST:  {config["Settings"]["RMQ_HOST"]}')
    log.info(f'CEPH_IPS:  {config["Data-store-mappings"]["CEPH_IPS"]}')

    try:
        ceph_ips = json.loads(config["Data-store-mappings"]["CEPH_IPS"])
    except (ValueError, TypeError) as exc:
        log.error(
            "The CEPH_IPS setting must be valid JSON (double quotes and no"
            f" trailing commas). Got: {config['Data-store-mappings']['CEPH_IPS']}."
            f" Error: {exc}"
        )
        return False
    log.debug(f'ceph_ips:  {ceph_ips}')

    # Get the data store name and the absolute path from the data store
    data_store, fpath = resolve_data_store(filepath, ceph_ips)
    log.info(f"data_store: {data_store}, fpath: {fpath}")

    if data_store is None or fpath is None:
        log.error(
          f"Could not resolve the data store: {data_store} and the file path:"
          f" {fpath} from the data store for: {filepath}. No notification was"
          f" sent!"
        )
        return False

    try:
        # Establish connection and create a channel on that connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=config["Settings"]["RMQ_HOST"])
        )
        channel = connection.channel()

        # Ensure the durable file_notif_queue exists
        channel.queue_declare(queue="file_notif_queue", durable=True)

        # Put the message data in a dictionary for conversion to JSON
        msg_dict = {
            "data_store": data_store,
            "filepath": fpath,
            "product": product,
            "version": version,
            "start_time": start_time,
            "end_time": end_time,
            "length": length,
            "checksum": checksum,
            "checksum_type": checksum_type,
        }

        msg_json = json.dumps(msg_dict)

        # Send the JSON formatted message
        channel.basic_publish(
            exchange="",
            routing_key="file_notif_queue",
            body=msg_json,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        log.debug(f" [x] Sent {msg_json}")

        # Close the connection to make sure the message actually gets sent - buffers
        # are flushed
        connection.close()
    except pika.exceptions.AMQPError as exc:
        log.error(f"RabbitMQ error while sending the notification: {exc}")
        raise

    return True


def main():

    # Parse the arguments
    parser = argparse.ArgumentParser(prog="ygd", description=DESCRIPTION.strip())

    # Add the positional argument(s?)
    parser.add_argument(
        "filepath", type=str, help="Send a notification for the file with this path."
    )

    # Add the flags
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output - set log level to DEBUG",
    )

    parser.add_argument(
        "-p",
        "--product",
        default=None,
        help="The file's product",
    )

    parser.add_argument(
        "-r",
        "--version",
        default=None,
        help="The file's version",
    )

    parser.add_argument(
        "-s",
        "--start_time",
        default=None,
        help="The first date and time for which the file has data",
    )

    parser.add_argument(
        "-e",
        "--end_time",
        default=None,
        help="The last date and time for which the file has data",
    )

    parser.add_argument(
        "-l", "--length", default=None, help="The length(size) of the file"
    )

    parser.add_argument("-c", "--checksum", default=None, help="The file's checksum")

    parser.add_argument(
        "-t",
        "--checksum_type",
        default=None,
        help="The type of the checksum - its algorithm",
    )

    pargs = parser.parse_args()

    # Setup logging.
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s%(name)s: %(message)s",
        level="DEBUG" if pargs.verbose else "INFO",
    )

    # Reduce pika logging
    logging.getLogger("pika").setLevel(logging.WARNING)

    # Read the configuration file
    config_dpath = user_config_dir(APP_NAME)
    config_fpath = os.path.join(config_dpath, "config.ini")
    log.debug(f"config_fpath = {config_fpath}")

    config = configparser.ConfigParser(interpolation=None)
    files_read = config.read(config_fpath)
    if not files_read:
        log.error(f"{config_fpath} not found. Please ensure the file exists.")
        sys.exit(1)

    if not config.has_option("Settings", "RMQ_HOST"):
        log.error(f"Missing `[Settings] RMQ_HOST` in {config_fpath}.")
        sys.exit(1)
    if not config.has_option("Data-store-mappings", "CEPH_IPS"):
        log.error(f"Missing `[Data-store-mappings] CEPH_IPS` in {config_fpath}.")
        sys.exit(1)

    # The filepath file must exist on the local machine
    resolved_path = str(Path(pargs.filepath).resolve())
    if not os.path.exists(resolved_path):
        log.error(f"The file {pargs.filepath} does not exist. No notification was sent.")
        sys.exit(1)

    log.info(f"Sending a new file notification for {pargs.filepath}")

    sent = produce_notification(
        config,
        pargs.filepath,
        pargs.product,
        pargs.version,
        pargs.start_time,
        pargs.end_time,
        pargs.length,
        pargs.checksum,
        pargs.checksum_type,
    )
    if not sent:
        sys.exit(1)


if __name__ == "__main__":
    main()
