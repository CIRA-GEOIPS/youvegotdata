"""youvegotdata - send new file notifications via RabbitMQ.

Public API
----------
Resolve a filepath to a data store::

    from youvegotdata import Config, load_config, resolve_data_store

    config = load_config()                      # ~/.config/youvegotdata/config.ini
    store, fpath = resolve_data_store("/data/file.hdf", config.ceph_ips)

Or publish a notification directly::

    from youvegotdata import Config, Notification, send_notification

    config = load_config()
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
"""

import logging

try:
    import importlib.metadata

    __version__ = importlib.metadata.version(__package__ or __name__)
except Exception:
    __version__ = "unknown"
    logging.getLogger(__name__).debug(
        "Could not determine __version__ - the package is not installed as a "
        "distribution (install it or run `pip install -e .`)."
    )

from .cli import build_parser, main
from .config import Config, ConfigError, config_file_path, load_config
from .mountinfo import parse_mountinfo, parse_mountinfo_alike
from .notify import Notification, QUEUE_NAME, produce_notification, send_notification
from .stores import resolve_ceph_store, resolve_data_store

__all__ = [
    # Version
    "__version__",
    # Config
    "Config",
    "ConfigError",
    "load_config",
    "config_file_path",
    # Mount table
    "parse_mountinfo",
    "parse_mountinfo_alike",
    # Data store resolution
    "resolve_data_store",
    "resolve_ceph_store",
    # Notification
    "Notification",
    "send_notification",
    "produce_notification",
    "QUEUE_NAME",
    # CLI
    "build_parser",
    "main",
]
