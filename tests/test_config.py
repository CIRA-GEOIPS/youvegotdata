"""Unit tests for youvegotdata.config."""

import pytest

from youvegotdata import ConfigError, config_file_path, load_config

VALID_CFG = (
    "[Settings]\nRMQ_HOST = rmq.example.com\n"
    '[Data-store-mappings]\nCEPH_IPS = {"storeA": ["10.0.0.1", "10.0.0.2"]}\n'
)


def _write(tmp_path, text=VALID_CFG):
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(text)
    return str(cfg_path)


class TestLoadConfig:
    def test_loads_valid_config(self, tmp_path):
        cfg = load_config(_write(tmp_path))
        assert cfg.rmq_host == "rmq.example.com"
        assert cfg.ceph_ips == {"storeA": ["10.0.0.1", "10.0.0.2"]}
        assert cfg.config_path.endswith("config.ini")

    def test_default_path_uses_user_config_dir(self, tmp_path, monkeypatch):
        (tmp_path / "config.ini").write_text(VALID_CFG)
        monkeypatch.setattr(
            "youvegotdata.config.user_config_dir", lambda app: str(tmp_path)
        )
        cfg = load_config()
        assert cfg.config_path == str(tmp_path / "config.ini")
        assert cfg.rmq_host == "rmq.example.com"

    def test_config_file_path_uses_app_name(self, monkeypatch):
        monkeypatch.setattr(
            "youvegotdata.config.user_config_dir", lambda app: "/tmp/cfg"
        )
        assert config_file_path() == "/tmp/cfg/config.ini"

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(ConfigError):
            load_config(str(tmp_path / "nope" / "config.ini"))

    def test_missing_rmq_host_raises(self, tmp_path):
        cfg = '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1"]}\n'
        with pytest.raises(ConfigError) as exc:
            load_config(_write(tmp_path, cfg))
        assert "RMQ_HOST" in str(exc.value)

    def test_missing_ceph_ips_raises(self, tmp_path):
        cfg = "[Settings]\nRMQ_HOST = rmq.example.com\n"
        with pytest.raises(ConfigError) as exc:
            load_config(_write(tmp_path, cfg))
        assert "CEPH_IPS" in str(exc.value)

    def test_bad_ceph_ips_json_raises(self, tmp_path):
        cfg = (
            "[Settings]\nRMQ_HOST = rmq.example.com\n"
            '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1",]}\n'
        )
        with pytest.raises(ConfigError) as exc:
            load_config(_write(tmp_path, cfg))
        assert "valid JSON" in str(exc.value)

    def test_ceph_ips_not_object_raises(self, tmp_path):
        cfg = (
            "[Settings]\nRMQ_HOST = rmq.example.com\n"
            '[Data-store-mappings]\nCEPH_IPS = ["a", "b"]\n'
        )
        with pytest.raises(ConfigError) as exc:
            load_config(_write(tmp_path, cfg))
        assert "JSON object" in str(exc.value)

    def test_ceph_ips_not_list_of_str_raises(self, tmp_path):
        cfg = (
            "[Settings]\nRMQ_HOST = rmq.example.com\n"
            '[Data-store-mappings]\nCEPH_IPS = {"s": "not-a-list"}\n'
        )
        with pytest.raises(ConfigError):
            load_config(_write(tmp_path, cfg))

    def test_ceph_ips_list_of_non_strings_raises(self, tmp_path):
        cfg = (
            "[Settings]\nRMQ_HOST = rmq.example.com\n"
            '[Data-store-mappings]\nCEPH_IPS = {"s": [1, 2]}\n'
        )
        with pytest.raises(ConfigError):
            load_config(_write(tmp_path, cfg))

    def test_empty_rmq_host_raises(self, tmp_path):
        cfg = (
            "[Settings]\nRMQ_HOST = \n"
            '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1"]}\n'
        )
        with pytest.raises(ConfigError) as exc:
            load_config(_write(tmp_path, cfg))
        assert "empty" in str(exc.value)

    def test_percent_in_config_value_is_not_interpolated(self, tmp_path):
        cfg = (
            "[Settings]\nRMQ_HOST = rmq-%s.example.com\n"
            '[Data-store-mappings]\nCEPH_IPS = {"s": ["10.0.0.1"]}\n'
        )
        loaded = load_config(_write(tmp_path, cfg))
        assert loaded.rmq_host == "rmq-%s.example.com"

    def test_config_is_immutable(self, tmp_path):
        cfg = load_config(_write(tmp_path))
        with pytest.raises(Exception):
            cfg.rmq_host = "other"
