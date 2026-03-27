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

DESCRIPTION = """
Allows a data ingest process to send a new file notification to the Data
Inventory RabbitMQ server. The notification will ultimately be used to add the
file metadata to the Data Inventory DB.
"""

log = logging.getLogger(__name__)

APP_NAME = "youvegotdata"

def parse_mountinfo_alike(fobj):
    mount_entries = []
    for line in fobj:
        # Each line in /proc/mountinfo has a specific format
        # The fields are space-separated, but some fields can contain spaces
        # The separator between the optional fields and the rest is '- '
        parts = line.strip().split(' - ')

        # Extract the first part (non-optional fields)
        first_part_fields = parts[0].split(' ')

        # Extract the last part (optional fields and remaining fields)
        last_part_fields = parts[1].split(' ') if len(parts) > 1 else []

        # Example of extracting common fields
        # Adjust indices based on the specific fields you need
        mount_id = int(first_part_fields[0])
        parent_id = int(first_part_fields[1])
        major_minor = first_part_fields[2]
        root = first_part_fields[3]
        mount_point = first_part_fields[4]
        mount_options = first_part_fields[5].split(',')

        # Filesystem type, mount source, and super options are in the last part
        filesystem_type = last_part_fields[0]
        mount_source = last_part_fields[1]
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
            "raw_line": line.strip()
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


def resolve_data_store(filepath):
    """
    Get the data store name and the absolute path from the data store.
    filepath: The filepath argument given to the program
    """
    # Read the /proc/self/mountinfo file to get the data store and mount point
    # Example usage
    data_store = None
    fpath = None
    mp_match_len = 0
    log.info("Currently mounted filesystems:")
    for mount in parse_mountinfo():
        log.debug(
            f"Source: {mount['mount_source']:<20} Mount Point: {mount['mount_point']:<20} FS Type: {mount['filesystem_type']:<10} Options: {mount['super_options']}"
        )
        if mount["mount_point"] == "/":
            # Skip this - every path will match it
            continue
        # Check all the mount points and use the one with the longest match
        if (
            filepath.startswith(mount["mount_point"])
            and len(mount["mount_point"]) > mp_match_len
        ):
            mp_match_len = len(mount["mount_point"])
            dev_dir = mount["mount_source"].split(":")
            data_store = dev_dir[0]
            if len(dev_dir) == 2:
                # There is a path associated with the mount_source. Replace the mount point with this path.
                fpath = dev_dir[1] + filepath[mp_match_len:]
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
    Send a "Fair Dispatch" message via RabbitMQ
    """

    # Get the data store name and the absolute path from the data store
    data_store, fpath = resolve_data_store(filepath)
    log.info(f"data_store: {data_store}, fpath: {fpath}")

    log.info(f'RMQ_HOST:  {config["Settings"]["RMQ_HOST"]}')

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


def main():

    # Parse the arguments
    parser = argparse.ArgumentParser(f"{DESCRIPTION}python youvegotdata.py")

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

    config = configparser.ConfigParser()
    files_read = config.read(config_fpath)
    if not files_read:
        log.error(f"{config_fpath} not found. Please ensure the file exists.")
        exit()

    log.info("Sending a new file notification")

    produce_notification(
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


if __name__ == "__main__":
    main()
