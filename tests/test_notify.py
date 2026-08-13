"""Unit tests for youvegotdata.notify."""

import json
import datetime
import logging
from unittest.mock import MagicMock, patch

import pika
import pytest

from youvegotdata import (
    Notification,
    produce_notification,
    send_notification,
)

from conftest import make_config, make_configparser


def _default_notification(**kwargs):
    defaults = dict(
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
                "SSP_spatial_resolution": "0.02",
            }
        },
    )
    defaults.update(kwargs)
    return Notification(**defaults)


def _run(config=None, resolve_return=("/dev/sda1", "/data/file.hdf"), **kwargs):
    """Run send_notification with sensible defaults, mocking pika."""
    config = config if config is not None else make_config()
    notification = _default_notification(**kwargs)

    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with patch("youvegotdata.notify.resolve_data_store", return_value=resolve_return):
        with patch(
            "youvegotdata.notify.pika.BlockingConnection",
            return_value=mock_connection,
        ) as mock_bc:
            send_notification(notification, config)

    return mock_bc, mock_connection, mock_channel


class TestSendNotification:
    def test_connection_opened_with_correct_host(self):
        mock_bc, _, _ = _run()
        conn_params = mock_bc.call_args[0][0]
        assert conn_params.host == "rmq.example.com"

    def test_queue_declared_durable(self):
        _, _, mock_channel = _run()
        mock_channel.queue_declare.assert_called_once_with(
            queue="file_notif_queue", durable=True
        )

    def test_message_published_to_correct_queue(self):
        _, _, mock_channel = _run()
        mock_channel.basic_publish.assert_called_once()
        kwargs = mock_channel.basic_publish.call_args.kwargs
        assert kwargs["routing_key"] == "file_notif_queue"
        assert kwargs["exchange"] == ""

    def test_message_body_is_valid_json(self):
        _, _, mock_channel = _run()
        body = mock_channel.basic_publish.call_args.kwargs["body"]
        msg = json.loads(body)
        assert isinstance(msg, dict)

    def test_message_contains_expected_fields(self):
        _, _, mock_channel = _run()
        body = mock_channel.basic_publish.call_args.kwargs["body"]
        msg = json.loads(body)
        assert msg["data_store"] == "/dev/sda1"
        assert msg["filepath"] == "/data/file.hdf"
        assert msg["product"] == "L2_VIS"
        assert msg["version"] == "1.0"
        assert msg["checksum"] == "abc123"
        assert msg["platform_name"] == "VIIRS"
        assert msg["source_name"] == "imager"
        assert msg["addl_metadata"] == {
            "geoips_variables": {
                "variables": ["red", "green", "blue"],
                "SSP_spatial_resolution": "0.02",
            }
        }


    def test_optional_fields_default_to_none(self):
        _, _, mock_channel = _run(
            start_time=None,
            end_time=None,
            size=None,
            checksum=None,
        )
        body = mock_channel.basic_publish.call_args.kwargs["body"]
        msg = json.loads(body)
        assert msg["start_time"] is None
        assert msg["end_time"] is None
        assert msg["size"] is None

    def test_connection_closed_after_publish(self):
        _, mock_connection, _ = _run()
        mock_connection.close.assert_called_once()

    def test_message_delivery_mode_is_persistent(self):
        _, _, mock_channel = _run()
        props = mock_channel.basic_publish.call_args.kwargs["properties"]
        assert props.delivery_mode == pika.DeliveryMode.Persistent.value

    def test_notification_to_message_is_json_serializable(self):
        message = _default_notification().to_message()
        assert json.loads(json.dumps(message)) == message

    def test_returns_true_when_notification_sent(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        with patch(
            "youvegotdata.notify.resolve_data_store",
            return_value=("/dev/sda1", "/data/file.hdf"),
        ):
            with patch(
                "youvegotdata.notify.pika.BlockingConnection",
                return_value=mock_connection,
            ):
                result = send_notification(
                    _default_notification(),
                    make_config(),
                )
        assert result is True

    def test_unresolved_store_returns_false(self):
        with patch(
            "youvegotdata.notify.resolve_data_store",
            return_value=(None, None),
        ):
            result = send_notification(
                _default_notification(),
                make_config(),
            )
        assert result is False

    def test_pika_failure_raises_and_logs_error(self, caplog):
        with patch(
            "youvegotdata.notify.resolve_data_store",
            return_value=("/dev/sda1", "/data/file.hdf"),
        ):
            with patch(
                "youvegotdata.notify.pika.BlockingConnection",
                side_effect=pika.exceptions.AMQPConnectionError("down"),
            ):
                with caplog.at_level(logging.ERROR):
                    with pytest.raises(pika.exceptions.AMQPConnectionError):
                        send_notification(
                            _default_notification(),
                            make_config(),
                        )
        assert any("RabbitMQ error" in r.message for r in caplog.records)


class TestProduceNotificationShim:
    def test_deprecation_warning_raised(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        with patch(
            "youvegotdata.notify.resolve_data_store",
            return_value=("/dev/sda1", "/data/file.hdf"),
        ):
            with patch(
                "youvegotdata.notify.pika.BlockingConnection",
                return_value=mock_connection,
            ):
                with pytest.warns(DeprecationWarning):
                    result = produce_notification(
                        config=make_configparser(),
                        filepath="/data/file.hdf",
                        product="L2_VIS",
                        version="1.0",
                    )
        assert result is True

    def test_still_accepts_legacy_configparser(self):
        import configparser

        raw = configparser.ConfigParser(interpolation=None)
        raw["Settings"] = {"RMQ_HOST": "legacy.example.com"}
        raw["Data-store-mappings"] = {"CEPH_IPS": '{"/mnt/data2": ["168.10.10.9"]}'}
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        with patch(
            "youvegotdata.notify.resolve_data_store",
            return_value=("/dev/sda1", "/data/file.hdf"),
        ):
            with patch(
                "youvegotdata.notify.pika.BlockingConnection",
                return_value=mock_connection,
            ) as mock_bc:
                with pytest.warns(DeprecationWarning):
                    result = produce_notification(
                        config=raw,
                        filepath="/data/file.hdf",
                        product=None,
                        version=None,
                    )
        assert result is True
        conn_params = mock_bc.call_args[0][0]
        assert conn_params.host == "legacy.example.com"

    def test_shim_accepts_config_dataclass(self):
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        with patch(
            "youvegotdata.notify.resolve_data_store",
            return_value=("/dev/sda1", "/data/file.hdf"),
        ):
            with patch(
                "youvegotdata.notify.pika.BlockingConnection",
                return_value=mock_connection,
            ):
                with pytest.warns(DeprecationWarning):
                    result = produce_notification(
                        config=make_config(),
                        filepath="/data/file.hdf",
                        product="L2_VIS",
                        version="1.0",
                    )
        assert result is True

    def test_shim_bad_ceph_ips_json_raises_configerror(self):
        raw = make_configparser(ip_mapping='{"/d": ["10.0.0.1",]}')
        from youvegotdata import ConfigError

        with pytest.raises(ConfigError):
            with pytest.warns(DeprecationWarning):
                produce_notification(
                    config=raw,
                    filepath="/data/file.hdf",
                    product=None,
                    version=None,
                )
