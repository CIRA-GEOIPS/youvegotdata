"""Unit tests for the youvegotdata CLI (youvegotdata.cli)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from youvegotdata import main

VALID_CFG = (
    "[Settings]\nRMQ_HOST = rmq.example.com\n"
    '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1"]}\n'
)


def _setup(tmp_path, monkeypatch, config_text=VALID_CFG, create_file=True):
    """Patch config location + sys.argv; return the filepath used."""
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(exist_ok=True)
    if config_text is not None:
        (cfg_dir / "config.ini").write_text(config_text)
    monkeypatch.setattr("youvegotdata.config.user_config_dir", lambda app: str(cfg_dir))
    filepath = str(tmp_path / "data" / "file.hdf")
    if create_file:
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        Path(filepath).touch()
    return filepath


class TestMain:
    def test_help_shows_prog_and_description(self, capsys):
        with pytest.raises(SystemExit) as exc:
            main(["--help"])
        assert exc.value.code == 0
        out = capsys.readouterr().out
        assert "usage: ygd" in out
        assert "new file notification" in out

    def test_missing_config_returns_1(self, tmp_path, monkeypatch):
        _setup(tmp_path, monkeypatch, config_text=None, create_file=False)
        assert main(["/data/file.hdf"]) == 1

    def test_missing_rmq_host_returns_1(self, tmp_path, monkeypatch):
        cfg = '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1"]}\n'
        filepath = _setup(tmp_path, monkeypatch, config_text=cfg)
        assert main([filepath]) == 1

    def test_missing_ceph_ips_returns_1(self, tmp_path, monkeypatch):
        cfg = "[Settings]\nRMQ_HOST = rmq.example.com\n"
        filepath = _setup(tmp_path, monkeypatch, config_text=cfg)
        assert main([filepath]) == 1

    def test_bad_ceph_ips_json_returns_1(self, tmp_path, monkeypatch):
        cfg = (
            "[Settings]\nRMQ_HOST = rmq.example.com\n"
            '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1",]}\n'
        )
        filepath = _setup(tmp_path, monkeypatch, config_text=cfg)
        assert main([filepath]) == 1

    def test_missing_file_returns_1(self, tmp_path, monkeypatch):
        _setup(tmp_path, monkeypatch, create_file=False)
        assert main(["/data/file.hdf"]) == 1

    def test_unresolved_store_returns_1(self, tmp_path, monkeypatch):
        filepath = _setup(tmp_path, monkeypatch)
        with patch(
            "youvegotdata.notify.resolve_data_store",
            return_value=(None, None),
        ):
            assert main([filepath]) == 1

    def test_success_returns_0(self, tmp_path, monkeypatch):
        filepath = _setup(tmp_path, monkeypatch)
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        with patch(
            "youvegotdata.notify.resolve_data_store",
            return_value=("/dev/sda1", filepath),
        ):
            with patch(
                "youvegotdata.notify.pika.BlockingConnection",
                return_value=mock_connection,
            ):
                assert main([filepath]) == 0

    def test_percent_in_config_value_is_not_interpolated(self, tmp_path, monkeypatch):
        # '%' must not trigger configparser interpolation.
        cfg = (
            "[Settings]\nRMQ_HOST = rmq-%s.example.com\n"
            '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1"]}\n'
        )
        filepath = _setup(tmp_path, monkeypatch, config_text=cfg)
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        with patch(
            "youvegotdata.notify.resolve_data_store",
            return_value=("/dev/sda1", filepath),
        ):
            with patch(
                "youvegotdata.notify.pika.BlockingConnection",
                return_value=mock_connection,
            ):
                # main() returns 0 without raising InterpolationMissingOptionError
                assert main([filepath]) == 0

    def test_custom_host_in_config_is_used(self, tmp_path, monkeypatch):
        cfg = (
            "[Settings]\nRMQ_HOST = custom.example.com\n"
            '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1"]}\n'
        )
        filepath = _setup(tmp_path, monkeypatch, config_text=cfg)
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.channel.return_value = mock_channel
        with patch(
            "youvegotdata.notify.resolve_data_store",
            return_value=("/dev/sda1", filepath),
        ):
            with patch(
                "youvegotdata.notify.pika.BlockingConnection",
                return_value=mock_connection,
            ) as mock_bc:
                assert main([filepath]) == 0
        conn_params = mock_bc.call_args[0][0]
        assert conn_params.host == "custom.example.com"
