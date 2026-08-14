# youvegotdata

Uses RabbitMQ to send new file notifications, with the ultimate purpose of
getting the file metadata into the Data Inventory Database.

The "producer" `youvegotdata.py` will usually be called by the CIRA data
ingest scripts when a new file is added to the CIRA data stores, and will send
a message through RabbitMQ to the consumers with the file's metadata.

Message "consumers" will be running to receive the file metadata and insert it
into the database. It is expected that multiple consumers process will be
accepting messages in RabbitMQ's "fair dispatch" configuration. A given
notification will be received by one consumer.

## Installing youvegotdata
This module requires Python 3.8 or greater.
It is deployed to PyPi, so it can be installed with:
```
pip install youvegotdata
```
Which will install the `ygd` CLI command in your current Python environment.

## Library usage
`youvegotdata` is also importable as a plain Python package so that ingest
scripts can send notifications programmatically instead of shelling out to the
`ygd` CLI. The public API is re-exported from the top-level package:

```python
from youvegotdata import (
    Config,
    ConfigError,
    Notification,
    load_config,
    resolve_data_store,
    send_notification,
)
```

### Loading configuration
`load_config()` reads and validates `~/.config/youvegotdata/config.ini` (the
same file the CLI uses) and returns a frozen `Config` dataclass. Validation
errors raise `ConfigError`:

```python
try:
    config = load_config()
except ConfigError as exc:
    print(f"configuration problem: {exc}")
    raise
```

To point at a specific file (e.g. for tests), pass an explicit path:

```python
config = load_config("/path/to/config.ini")
```

The returned `Config` exposes `rmq_host` (str) and `ceph_ips`
(dict[str, list[str]]).

### Resolving a data store from a filepath
`resolve_data_store` maps a local path to its data store name and a
store-relative path. It reads `/proc/self/mountinfo`, so it only works on
Linux:

```python
data_store, store_path = resolve_data_store("/data/file.hdf", config.ceph_ips)
```

### Sending a notification
Build a `Notification` and pass it (plus a `Config`) to `send_notification`.
It publishes one durable JSON message to the `file_notif_queue` queue and
returns `True` on success or `False` if the data store could not be resolved.
RabbitMQ errors are logged and re-raised as `pika.exceptions.AMQPError`:

```python
notification = Notification(
    filepath="/data/file.hdf",
    product="L2_VIS",
    version="1.0",
    start_time="2024-01-01T00:00:00",
    end_time="2024-01-01T01:00:00",
    size=1024,
    checksum="abc123",
    platform_name="VIIRS",
    source_name="imager",
    addl_metadata={
        "geoips_variables": {
            "variables": ["red", "green", "blue"],
            "SSP_spatial_resolution": "0.02 km",
        }
    },
)
send_notification(notification, config)
```

Only `filepath` is required; the rest default to `None`. The `checksum` must be
an xxhash value if it is included.

### Backward compatibility
The v1.x function `produce_notification(config, filepath, ...)` is kept as a
deprecated shim that emits a `DeprecationWarning` and forwards to
`send_notification`. It still accepts a raw `configparser.ConfigParser` or a
`Config`. New code should use `send_notification` instead.

## Running youvegotdata.py as ygd
Create the ~/.config/youvegotdata/ directory if it does not already exist.
Create a `config.ini` file in this directory that looks like:
```
[Settings]
RMQ_HOST = <host of the RabbitMQ server>

[Data-store-mappings]
# The list of common Ceph IPs for every Ceph data store
# A dictionary of lists. This must be valid JSON: double quotes, no trailing commas.
CEPH_IPS = {"name": ["IP", "IP", "IP"], "name2": ["IP", "IP", "IP"]}
```
And fill it in with the RabbitMQ server's host name, and the IP address
mappings across different Linux versions for the Ceph data stores.

Run the code with:
```
ygd [-h] [-v] [-p PRODUCT] [-r VERSION] [-s START_TIME] [-e END_TIME] [-l LENGTH] [-c CHECKSUM] [-t PLATFORM_NAME] [-o SOURCE_NAME] [-a ADDL_METADATA] filepath
```
Run this with the -h (--help) argument to see the available flagged arguments.

This will usually be run with just the `filepath` argument. An example is:
```
ygd /full/path/to/local/file/data_file.hdf
```

The `filepath` file must exist on the local machine.
