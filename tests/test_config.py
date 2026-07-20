from __future__ import annotations

import argparse
import stat

import pytest

from diskblaze import cli, config


@pytest.fixture
def isolated_config(tmp_path, monkeypatch):
    """Point credential storage at a temp dir and clear auth env vars."""
    monkeypatch.setenv("DISKBLAZE_CONFIG_DIR", str(tmp_path / "cfg"))
    monkeypatch.delenv("DISKBLAZE_TOKEN", raising=False)
    monkeypatch.delenv("DISKBLAZE_API_KEY", raising=False)
    monkeypatch.delenv("DISKBLAZE_URL", raising=False)
    monkeypatch.delenv("DISKBLAZE_GQL_URL", raising=False)
    return tmp_path


def test_save_and_load_credentials_roundtrip(isolated_config):
    assert config.stored_token() is None
    path = config.save_credentials("db_secret", "https://example.com/graphql")
    assert config.stored_token() == "db_secret"
    assert config.stored_endpoint() == "https://example.com/graphql"

    # File must not be world/group readable.
    mode = stat.S_IMODE(path.stat().st_mode)
    assert mode == 0o600


def test_clear_credentials(isolated_config):
    config.save_credentials("db_secret")
    assert config.clear_credentials() is True
    assert config.stored_token() is None
    # Clearing again when nothing is stored is a no-op.
    assert config.clear_credentials() is False


def _ns(**kw):
    base = {"endpoint": None, "token": None}
    base.update(kw)
    return argparse.Namespace(**base)


def test_resolve_token_precedence(isolated_config, monkeypatch):
    config.save_credentials("from_file")
    # Saved file is the fallback.
    assert cli.resolve_token(_ns()) == "from_file"
    # Env beats file.
    monkeypatch.setenv("DISKBLAZE_TOKEN", "from_env")
    assert cli.resolve_token(_ns()) == "from_env"
    # Explicit flag beats everything.
    assert cli.resolve_token(_ns(token="from_flag")) == "from_flag"


def test_resolve_endpoint_uses_saved_then_default(isolated_config):
    assert cli.resolve_endpoint(_ns()) == "https://diskblaze.com/graphql"
    config.save_credentials("t", "https://saved.example/graphql")
    assert cli.resolve_endpoint(_ns()) == "https://saved.example/graphql"
    # A flag overrides the saved endpoint (bare host gets /graphql appended).
    assert (
        cli.resolve_endpoint(_ns(endpoint="https://flag.example")) == "https://flag.example/graphql"
    )


def test_logout_command_reports_state(isolated_config, capsys):
    assert cli.command_logout(_ns()) == 0
    assert "not logged in" in capsys.readouterr().out
    config.save_credentials("t")
    assert cli.command_logout(_ns()) == 0
    assert "logged out" in capsys.readouterr().out
