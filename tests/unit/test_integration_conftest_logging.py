import importlib.util
import logging
from pathlib import Path

import pytest


def _load_integration_conftest():
    path = Path(__file__).resolve().parents[1] / "integration" / "conftest.py"
    spec = importlib.util.spec_from_file_location(
        "integration_conftest_under_test", path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def integration_conftest():
    return _load_integration_conftest()


def test_must_env_logs_only_allowlisted_env_vars(
    monkeypatch, caplog, integration_conftest
):
    monkeypatch.setenv("CLIENT_ID", "id-should-not-appear")
    monkeypatch.setenv("CLIENT_SECRET", "secret-should-not-appear")
    monkeypatch.setenv("SERVICE_SECRET", "renamed-secret-should-not-appear")
    monkeypatch.setenv("ENGINE_NAME", "engine-ok")
    monkeypatch.setenv("DATABASE_NAME", "database-ok")
    monkeypatch.setenv("ACCOUNT_NAME", "account-ok")

    with caplog.at_level(logging.INFO, logger=integration_conftest.LOGGER.name):
        assert integration_conftest.must_env("CLIENT_ID") == "id-should-not-appear"
        assert (
            integration_conftest.must_env("CLIENT_SECRET") == "secret-should-not-appear"
        )
        assert (
            integration_conftest.must_env("SERVICE_SECRET")
            == "renamed-secret-should-not-appear"
        )
        assert integration_conftest.must_env("ENGINE_NAME") == "engine-ok"
        assert integration_conftest.must_env("DATABASE_NAME") == "database-ok"
        assert integration_conftest.must_env("ACCOUNT_NAME") == "account-ok"

    assert "id-should-not-appear" not in caplog.text
    assert "secret-should-not-appear" not in caplog.text
    assert "renamed-secret-should-not-appear" not in caplog.text
    assert "CLIENT_ID" not in caplog.text
    assert "CLIENT_SECRET" not in caplog.text
    assert "SERVICE_SECRET" not in caplog.text
    assert "ENGINE_NAME: engine-ok" in caplog.text
    assert "DATABASE_NAME: database-ok" in caplog.text
    assert "ACCOUNT_NAME: account-ok" in caplog.text
