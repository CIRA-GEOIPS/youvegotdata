"""Publishing file notifications to RabbitMQ.

The core public function is :func:`send_notification`, which takes a
:class:`Notification` and a :class:`~youvegotdata.config.Config` and publishes
one JSON message to the durable ``file_notif_queue``. The legacy
:func:`produce_notification` is kept as a deprecated compatibility shim.
"""

from __future__ import annotations

import json
import logging
import warnings
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

import pika

from .config import Config, ConfigError
from .stores import resolve_data_store

log = logging.getLogger(__name__)

QUEUE_NAME = "file_notif_queue"


@dataclass
class Notification:
    """A new-file notification to publish.

    Attributes
    ----------
    filepath : str
        The local path of the new file. The file must exist on the local
        machine and be inside a resolvable data store.
    product : str, optional
        The file's product.
    version : str, optional
        The file's version.
    start_time : str, optional
        The first date and time for which the file has data.
    end_time : str, optional
        The last date and time for which the file has data.
    length : int, optional
        The length (size) of the file.
    checksum : str, optional
        The file's checksum.
    checksum_type : str, optional
        The type (algorithm) of the checksum.
    """

    filepath: str
    product: Optional[str] = None
    version: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    length: Optional[int] = None
    checksum: Optional[str] = None
    checksum_type: Optional[str] = None

    def to_message(self) -> Dict[str, Any]:
        """Return the message as a JSON-serializable dictionary."""
        return asdict(self)


def send_notification(notification: Notification, config: Config) -> bool:
    """Send a "Fair Dispatch" notification message via RabbitMQ.

    The notification is published to the durable ``file_notif_queue`` on
    ``config.rmq_host`` and the connection is closed to flush buffers. An
    empty message has ``data_store``/``filepath`` filled in by resolving the
    filepath against the mounted filesystems.

    Parameters
    ----------
    notification : Notification
        The notification to send. ``filepath`` must exist on the local
        machine.
    config : Config
        Validated configuration (RabbitMQ host + Ceph IP mappings).

    Returns
    -------
    bool
        ``True`` if the notification was published, ``False`` if the data
        store could not be resolved.

    Raises
    ------
    pika.exceptions.AMQPError
        If RabbitMQ reports an error while sending.
    """
    log.info("Sending a new file notification for %s", notification.filepath)
    log.debug("RMQ_HOST: %s", config.rmq_host)

    # Get the data store name and the absolute path from the data store.
    data_store, fpath = resolve_data_store(notification.filepath, config.ceph_ips)
    log.info("data_store: %s, fpath: %s", data_store, fpath)

    if data_store is None or fpath is None:
        log.error(
            "Could not resolve the data store for: %s. No notification was sent!",
            notification.filepath,
        )
        return False

    message_dict = notification.to_message()
    message_dict["data_store"] = data_store
    message_dict["filepath"] = fpath

    msg_json = json.dumps(message_dict)
    log.debug("Message payload: %s", msg_json)

    try:
        # Establish connection and create a channel on that connection.
        log.debug("Connecting to RabbitMQ host %r ...", config.rmq_host)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=config.rmq_host)
        )
        channel = connection.channel()

        # Ensure the durable file_notif_queue exists.
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        log.debug("Queue %r is declared durable on %r", QUEUE_NAME, config.rmq_host)

        # Send the JSON formatted message.
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=msg_json,
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        log.debug(" [x] Sent %s", msg_json)

        # Close the connection to make sure the message actually gets sent -
        # buffers are flushed.
        connection.close()
        log.debug("Connection closed; buffers flushed.")
    except pika.exceptions.AMQPError as exc:
        log.error("RabbitMQ error while sending the notification: %s", exc)
        raise

    log.info(
        "Notification sent for %s (store %s) to %s/%s",
        notification.filepath,
        data_store,
        config.rmq_host,
        QUEUE_NAME,
    )
    return True


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
) -> bool:
    """Deprecated compatibility shim for :func:`send_notification`.

    .. deprecated:: 2.0
       Use :func:`send_notification` with a :class:`Notification` and a
       :class:`~youvegotdata.config.Config` instead.

    ``config`` may be a raw :class:`configparser.ConfigParser` (as in v1.x) or
    a :class:`~youvegotdata.config.Config`; both are converted transparently.
    All arguments are passed through to the new API.

    Returns
    -------
    bool
        See :func:`send_notification`.
    """
    warnings.warn(
        "produce_notification() is deprecated; use send_notification() with a "
        "Notification and Config instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    from .config import Config as _Config

    if isinstance(config, _Config):
        cfg = config
    else:
        # Legacy raw ConfigParser: build a Config out of it.
        try:
            ceph_ips = json.loads(config["Data-store-mappings"]["CEPH_IPS"])
        except (KeyError, ValueError, TypeError) as exc:
            raise ConfigError(
                "The CEPH_IPS setting must be valid JSON and present in the "
                f"config. Error: {exc}"
            ) from exc
        cfg = _Config(
            rmq_host=config["Settings"]["RMQ_HOST"],
            ceph_ips=ceph_ips,
        )

    notification = Notification(
        filepath=filepath,
        product=product,
        version=version,
        start_time=start_time,
        end_time=end_time,
        length=length,
        checksum=checksum,
        checksum_type=checksum_type,
    )
    return send_notification(notification, cfg)


__all__ = ["Notification", "send_notification", "produce_notification", "QUEUE_NAME"]
