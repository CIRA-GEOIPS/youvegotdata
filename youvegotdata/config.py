"""Configuration loading and validation for youvegotdata.

Configuration is read from an INI file at
``~/.config/youvegotdata/config.ini`` (platform-dependent, via
:func:`platformdirs.user_config_dir`) and exposed as a :class:`Config`
dataclass so that library callers never touch ``ConfigParser`` directly.
"""

from __future__ import annotations

import configparser
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Union

from platformdirs import user_config_dir

log = logging.getLogger(__name__)

APP_NAME = "youvegotdata"


class ConfigError(Exception):
    """Raised when the config file is missing, unreadable, or invalid."""


@dataclass(frozen=True)
class Config:
    """Validated application configuration.

    Attributes
    ----------
    rmq_host : str
        Host of the RabbitMQ server.
    ceph_ips : dict of str to list of str
        Mapping of Ceph data store names to the IP lists that identify them.
    config_path : str
        Absolute path of the config file this was loaded from.
    """

    rmq_host: str
    ceph_ips: Dict[str, List[str]] = field(default_factory=dict)
    config_path: str = ""


def config_file_path() -> str:
    """Return the default config file path for this app.

    Returns
    -------
    str
        ``<user_config_dir>/config.ini``, where ``user_config_dir`` comes from
        platformdirs and is app-specific.
    """
    config_dpath = user_config_dir(APP_NAME)
    return os.path.join(config_dpath, "config.ini")


def load_config(path: Optional[Union[str, os.PathLike]] = None) -> Config:
    """Load and validate the youvegotdata configuration.

    Parameters
    ----------
    path : str or os.PathLike, optional
        Explicit path to a config INI file. Defaults to the app config
        location (:func:`config_file_path`).

    Returns
    -------
    Config
        The validated configuration.

    Raises
    ------
    ConfigError
        If the file is missing, an option is missing, or ``CEPH_IPS`` is not
        valid JSON.
    """
    if path is None:
        path = config_file_path()
    config_fpath = str(path)
    log.debug("Reading config from %s", config_fpath)

    if not os.path.isfile(config_fpath):
        raise ConfigError(f"{config_fpath} not found. Please ensure the file exists.")

    config = configparser.ConfigParser(interpolation=None)
    files_read = config.read([config_fpath])
    if not files_read:
        raise ConfigError(f"{config_fpath} could not be read.")

    if not config.has_option("Settings", "RMQ_HOST"):
        raise ConfigError(f"Missing `[Settings] RMQ_HOST` in {config_fpath}.")
    if not config.has_option("Data-store-mappings", "CEPH_IPS"):
        raise ConfigError(
            f"Missing `[Data-store-mappings] CEPH_IPS` in {config_fpath}."
        )

    rmq_host = config.get("Settings", "RMQ_HOST").strip()
    if not rmq_host:
        raise ConfigError(f"`[Settings] RMQ_HOST` is empty in {config_fpath}.")

    raw_ceph_ips = config.get("Data-store-mappings", "CEPH_IPS")
    try:
        ceph_ips = json.loads(raw_ceph_ips)
    except (ValueError, TypeError) as exc:
        raise ConfigError(
            "The CEPH_IPS setting must be valid JSON (double quotes and no "
            f"trailing commas). Got: {raw_ceph_ips}. Error: {exc}"
        ) from exc

    if not isinstance(ceph_ips, dict):
        raise ConfigError(
            "The CEPH_IPS setting must be a JSON object mapping store names to "
            f"lists of IPs. Got: {raw_ceph_ips}"
        )

    normalized: Dict[str, List[str]] = {}
    for store_name, ips in ceph_ips.items():
        if not isinstance(ips, list) or not all(isinstance(ip, str) for ip in ips):
            raise ConfigError(
                f"The CEPH_IPS store {store_name!r} must map to a list of IP "
                f"strings. Got: {ips!r}"
            )
        normalized[str(store_name)] = [str(ip) for ip in ips]

    config_obj = Config(
        rmq_host=rmq_host,
        ceph_ips=normalized,
        config_path=config_fpath,
    )
    log.info(
        "Loaded config from %s (RMQ_HOST=%s, %d Ceph store mappings)",
        config_fpath,
        rmq_host,
        len(normalized),
    )
    log.debug("CEPH_IPS mappings: %s", {k: list(v) for k, v in normalized.items()})
    return config_obj


__all__ = ["Config", "ConfigError", "load_config", "config_file_path", "APP_NAME"]
