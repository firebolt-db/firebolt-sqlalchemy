import pytest
from firebolt.client.auth import FireboltCore
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Connection, Engine


@pytest.mark.core
class TestFireboltCoreIntegration:
    def test_core_connection(self, core_connection: Connection):
        """Test that Core connection can be established."""
        result = core_connection.execute(text("SELECT 1"))
        assert result.fetchall() == [(1,)]

    def test_core_engine_auth(self, core_engine: Engine):
        """Test that Core engine uses FireboltCore authentication."""
        connect_args = core_engine.dialect.create_connect_args(core_engine.url)
        auth = connect_args[1]["auth"]
        assert isinstance(auth, FireboltCore)

    def test_core_simple_query(self, core_connection: Connection):
        """Test executing a simple query against Core."""
        result = core_connection.execute(text("SELECT 'Hello Core' as message"))
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "Hello Core"

    def test_core_no_credentials_required(self, core_engine: Engine):
        """Test that Core connection doesn't require traditional credentials."""
        connect_args = core_engine.dialect.create_connect_args(core_engine.url)
        result_dict = connect_args[1]

        assert "url" in result_dict
        assert result_dict["url"] == "http://localhost:3473"
        assert isinstance(result_dict["auth"], FireboltCore)
        assert result_dict["engine_name"] is None
        assert "account_name" not in result_dict

    def test_core_sdk_validation_with_engine_name(self, core_url: str):
        """Test that SDK handles engine_name parameter for Core connections."""
        engine = create_engine(f"firebolt://test_db/test_engine?url={core_url}")

        connect_args = engine.dialect.create_connect_args(engine.url)
        result_dict = connect_args[1]

        assert result_dict["engine_name"] == "test_engine"
        assert result_dict["database"] == "test_db"
        assert result_dict["url"] == core_url
        assert isinstance(result_dict["auth"], FireboltCore)

    def test_core_sdk_validation_with_account_name(self, core_url: str):
        """Test that SDK handles account_name parameter for Core connections."""
        engine = create_engine(
            f"firebolt://test_db?url={core_url}&account_name=test_account"
        )

        connect_args = engine.dialect.create_connect_args(engine.url)
        result_dict = connect_args[1]

        assert result_dict["account_name"] == "test_account"
        assert result_dict["database"] == "test_db"
        assert result_dict["url"] == core_url
        assert isinstance(result_dict["auth"], FireboltCore)

    def test_core_sdk_validation_with_invalid_url(self, core_url: str):
        """Test that SDK handles invalid URL parameter for Core connections."""
        engine = create_engine("firebolt://test_db?url=invalid://localhost:9999")

        connect_args = engine.dialect.create_connect_args(engine.url)
        result_dict = connect_args[1]

        assert result_dict["url"] == "invalid://localhost:9999"
        assert result_dict["database"] == "test_db"
        assert isinstance(result_dict["auth"], FireboltCore)
