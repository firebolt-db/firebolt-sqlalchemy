import os
from unittest import mock

import sqlalchemy
import sqlalchemy.types as sqltypes
from conftest import MockCursor, MockDBApi
from firebolt.client.auth import FireboltCore
from pytest import mark, raises
from sqlalchemy.engine import url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql import text

import firebolt_db  # SQLAlchemy package
from firebolt_db.firebolt_dialect import (
    FireboltCompiler,
    FireboltDialect,
    FireboltIdentifierPreparer,
    FireboltTypeCompiler,
)
from firebolt_db.firebolt_dialect import dialect as dialect_definition
from firebolt_db.firebolt_dialect import resolve_type


class TestFireboltDialect:
    def test_create_dialect(self, dialect: FireboltDialect):
        assert issubclass(dialect_definition, FireboltDialect)
        assert isinstance(FireboltDialect.dbapi(), type(firebolt_db))
        assert dialect.name == "firebolt"
        assert dialect.driver == "firebolt"
        assert issubclass(dialect.preparer, FireboltIdentifierPreparer)
        assert issubclass(dialect.statement_compiler, FireboltCompiler)
        # SQLAlchemy's DefaultDialect creates an instance of
        # type_compiler behind the scenes
        assert isinstance(dialect.type_compiler, FireboltTypeCompiler)
        assert dialect.context == {}

    def test_create_connect_args_user_password(self, dialect: FireboltDialect):
        u = url.make_url(
            "test_engine://test-sa@user.com:test_password@test_db_name/test_engine_name"
        )
        with mock.patch.dict(os.environ, {"FIREBOLT_BASE_URL": "test_url"}):
            result_list, result_dict = dialect.create_connect_args(u)
            assert result_dict["engine_name"] == "test_engine_name"
            assert result_dict["auth"].username == "test-sa@user.com"
            assert result_dict["auth"].password == "test_password"
            assert result_dict["auth"]._use_token_cache is True
            assert result_dict["database"] == "test_db_name"
            assert result_dict["api_endpoint"] == "test_url"
            assert "username" not in result_dict
            assert "password" not in result_dict
            assert result_list == []

    def test_create_connect_no_account(self, dialect: FireboltDialect):
        u = url.make_url(
            "test_engine://test-sa-user-key:test_password@test_db_name/test_engine_name"
        )
        with raises(ArgumentError):
            dialect.create_connect_args(u)

    def test_create_connect_args(self, dialect: FireboltDialect):
        connection_url = (
            "test_engine://aabbb2bccc-kkkn3nbbb-iii4lll:test_password@"
            + "test_db_name/test_engine_name?"
            "account_name=dummy"
        )
        u = url.make_url(connection_url)
        with mock.patch.dict(os.environ, {"FIREBOLT_BASE_URL": "test_url"}):
            result_list, result_dict = dialect.create_connect_args(u)
            assert result_dict["engine_name"] == "test_engine_name"
            assert result_dict["account_name"] == "dummy"
            assert result_dict["auth"].client_id == "aabbb2bccc-kkkn3nbbb-iii4lll"
            assert result_dict["auth"].client_secret == "test_password"
            assert result_dict["auth"]._use_token_cache is True
            assert result_dict["database"] == "test_db_name"
            assert result_dict["api_endpoint"] == "test_url"
            assert "username" not in result_dict
            assert "password" not in result_dict
            assert result_list == []
        # No endpoint override
        with mock.patch.dict(os.environ, {}, clear=True):
            result_list, result_dict = dialect.create_connect_args(u)
            assert "api_endpoint" not in result_dict
            assert "use_token_cache" not in result_dict
            assert result_list == []

    def test_create_connect_args_set_params(self, dialect: FireboltDialect):
        connection_url = (
            "test_engine://aabbb2bccc-kkkn3nbbb-iii4ll:test_password@"
            "test_db_name/test_engine_name"
            "?account_name=dummy&param1=1&param2=2"
        )
        u = url.make_url(connection_url)
        result_list, result_dict = dialect.create_connect_args(u)
        assert (
            "account_name" in result_dict
        ), "account_name was not parsed correctly from connection string"
        assert dialect._set_parameters == {"param1": "1", "param2": "2"}

    def test_create_connect_args_driver_override(self, dialect: FireboltDialect):
        connection_url = (
            "test_engine://aabbb2bccc-kkkn3nbbb-iii4ll:test_password@"
            "test_db_name/test_engine_name"
            "?account_name=dummy"
            "&user_drivers=DriverA:1.0.2&user_clients=ClientB:2.0.9"
        )
        u = url.make_url(connection_url)
        result_list, result_dict = dialect.create_connect_args(u)
        assert (
            "additional_parameters" in result_dict
        ), "additional_parameters were not parsed correctly from connection string"
        assert (
            result_dict["additional_parameters"].get("user_drivers") == "DriverA:1.0.2"
        )
        assert (
            result_dict["additional_parameters"].get("user_clients") == "ClientB:2.0.9"
        )

    @mark.parametrize(
        "token,expected", [("false", False), ("0", False), ("true", True)]
    )
    def test_create_connect_args_token_cache(
        self, token, expected, dialect: FireboltDialect
    ):
        connection_url = (
            "test_engine://aabbb2bccc-kkkn3nbbb-iii4ll:test_password@"
            "test_db_name/test_engine_name"
            "?account_name=dummy"
            f"&use_token_cache={token}&param1=1&param2=2"
        )
        u = url.make_url(connection_url)
        result_list, result_dict = dialect.create_connect_args(u)
        assert result_dict["auth"].client_id == "aabbb2bccc-kkkn3nbbb-iii4ll"
        assert result_dict["auth"].client_secret == "test_password"
        assert result_dict["auth"]._use_token_cache == expected
        assert dialect._set_parameters == {"param1": "1", "param2": "2"}

    def test_do_execute(
        self, dialect: FireboltDialect, cursor: mock.Mock(spec=MockCursor)
    ):
        dialect._set_parameters = {"a": "b"}
        dialect.do_execute(cursor, "SELECT *", None)
        cursor.execute.assert_called_once_with("SELECT *", parameters=None)
        assert cursor._set_parameters == {"a": "b"}, "Set parameters were not set"
        cursor._set_parameters = {}
        cursor.execute.reset_mock()
        dialect.do_execute(cursor, "SELECT *", (1, 22), None)
        cursor.execute.assert_called_once_with("SELECT *", parameters=(1, 22))
        assert cursor._set_parameters == {"a": "b"}, "Set parameters were not set"

    def test_schema_names(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        result = dialect.get_schema_names(connection)
        assert result == ["public"]

    def test_table_names(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        def row_with_table_name(name):
            return mock.Mock(table_name=name)

        connection.execute.return_value = [
            row_with_table_name("table1"),
            row_with_table_name("table2"),
        ]

        result = dialect.get_table_names(connection)
        assert result == ["table1", "table2"]
        connection.execute.assert_called_once()
        assert str(connection.execute.call_args[0][0].compile()) == str(
            text("select table_name from information_schema.tables").compile()
        )
        connection.execute.reset_mock()
        result = dialect.get_table_names(connection, schema="schema")
        assert result == ["table1", "table2"]
        connection.execute.assert_called_once()
        assert str(connection.execute.call_args[0][0].compile()) == str(
            text(
                "select table_name from information_schema.tables"
                " where table_schema = 'schema'"
            ).compile()
        )

    def test_view_names(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        assert dialect.get_view_names(connection) == []

    def test_table_options(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        assert dialect.get_table_options(connection, "table") == {}

    def test_columns(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        def multi_column_row(columns):
            def getitem(self, idx):
                for i, result in enumerate(columns):
                    if idx == i:
                        return result

            return mock.Mock(__getitem__=getitem)

        connection.execute.return_value = [
            multi_column_row(["name1", "INT", 1]),
            multi_column_row(["name2", "date", 0]),
        ]

        expected_query = """
            select column_name,
                   data_type,
                   is_nullable
              from information_schema.columns
             where table_name = 'table'
        """

        expected_query_schema = expected_query + " and table_schema = 'schema'"

        for call, expected_query in (
            (lambda: dialect.get_columns(connection, "table"), expected_query),
            (
                lambda: dialect.get_columns(connection, "table", "schema"),
                expected_query_schema,
            ),
        ):
            assert call() == [
                {
                    "name": "name1",
                    "type": sqlalchemy.types.INTEGER,
                    "nullable": True,
                    "default": None,
                },
                {
                    "name": "name2",
                    "type": sqlalchemy.types.DATE,
                    "nullable": False,
                    "default": None,
                },
            ]
            connection.execute.assert_called_once()
            assert str(connection.execute.call_args[0][0].compile()) == str(
                text(expected_query).compile()
            )
            connection.execute.reset_mock()

    def test_has_table(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        connection.execute.return_value.fetchone.return_value.exists_ = True
        assert dialect.has_table(connection, "dummy")
        assert "dummy" in str(connection.execute.call_args[0][0].compile())

    def test_noop(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        dialect.get_view_definition(connection, "dummy")
        dialect.do_rollback(connection)
        dialect.do_commit(connection)
        connection.assert_not_called()
        connection.execute.assert_not_called()

    def test_pk_constraint(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        assert dialect.get_pk_constraint(connection, "table") == {
            "constrained_columns": [],
            "name": None,
        }

    def test_foreign_keys(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        assert dialect.get_foreign_keys(connection, "table") == []

    def test_check_constraints(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        assert dialect.get_check_constraints(connection, "table") == []

    def test_table_comment(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        assert dialect.get_table_comment(connection, "table") == {"text": ""}

    def test_indexes(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        assert dialect.get_indexes(connection, "table") == []

    def test_unique_constraints(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        assert dialect.get_unique_constraints(connection, "table") == []

    def test_unicode_returns(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        assert dialect._check_unicode_returns(connection)

    def test_unicode_description(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        assert dialect._check_unicode_description(connection)

    def test_create_connect_args_core_connection(self, dialect: FireboltDialect):
        connection_url = "test_engine://test_db_name?url=http://localhost:8080"
        u = url.make_url(connection_url)
        result_list, result_dict = dialect.create_connect_args(u)

        assert result_dict["engine_name"] is None
        assert result_dict["database"] == "test_db_name"
        assert result_dict["url"] == "http://localhost:8080"
        assert isinstance(result_dict["auth"], FireboltCore)
        assert "account_name" not in result_dict
        assert result_list == []

    def test_create_connect_args_core_connection_with_database(
        self, dialect: FireboltDialect
    ):
        connection_url = "test_engine://test_db_name?" "url=http://localhost:8080"
        u = url.make_url(connection_url)
        result_list, result_dict = dialect.create_connect_args(u)

        assert result_dict["database"] == "test_db_name"
        assert result_dict["url"] == "http://localhost:8080"
        assert isinstance(result_dict["auth"], FireboltCore)
        assert result_dict["engine_name"] is None
        assert result_list == []

    def test_create_connect_args_core_no_credentials_required(
        self, dialect: FireboltDialect
    ):
        connection_url = "test_engine://test_db_name?url=http://localhost:8080"
        u = url.make_url(connection_url)

        result_list, result_dict = dialect.create_connect_args(u)
        assert isinstance(result_dict["auth"], FireboltCore)
        assert "account_name" not in result_dict

    def test_create_connect_args_core_with_credentials_error(
        self, dialect: FireboltDialect
    ):
        connection_url = (
            "test_engine://user:pass@test_db_name?url=http://localhost:8080"
        )
        u = url.make_url(connection_url)

        with raises(
            ArgumentError,
            match="Core connections do not support username/password authentication",
        ):
            dialect.create_connect_args(u)

    def test_create_connect_args_core_with_engine_allowed(self, dialect: FireboltDialect):
        """Test that Core connections now allow engine_name parameter."""
        connection_url = (
            "test_engine://test_db_name/test_engine?url=http://localhost:8080"
        )
        u = url.make_url(connection_url)
        
        result_list, result_dict = dialect.create_connect_args(u)
        assert result_dict["engine_name"] == "test_engine"
        assert result_dict["url"] == "http://localhost:8080"

    def test_create_connect_args_core_with_account_allowed(
        self, dialect: FireboltDialect
    ):
        """Test that Core connections now allow account_name parameter."""
        connection_url = (
            "test_engine://test_db_name?url=http://localhost:8080&account_name=test"
        )
        u = url.make_url(connection_url)
        
        result_list, result_dict = dialect.create_connect_args(u)
        assert result_dict["account_name"] == "test"
        assert result_dict["url"] == "http://localhost:8080"


def test_get_is_nullable():
    assert firebolt_db.firebolt_dialect.get_is_nullable(1)
    assert not firebolt_db.firebolt_dialect.get_is_nullable(0)


def test_types():
    assert firebolt_db.firebolt_dialect.DATE is sqlalchemy.sql.sqltypes.DATE
    assert firebolt_db.firebolt_dialect.DATETIME is sqlalchemy.sql.sqltypes.DATETIME
    assert firebolt_db.firebolt_dialect.INTEGER is sqlalchemy.sql.sqltypes.INTEGER
    assert firebolt_db.firebolt_dialect.BIGINT is sqlalchemy.sql.sqltypes.BIGINT
    assert firebolt_db.firebolt_dialect.TIMESTAMP is sqlalchemy.sql.sqltypes.TIMESTAMP
    assert firebolt_db.firebolt_dialect.TEXT is sqlalchemy.sql.sqltypes.TEXT
    assert firebolt_db.firebolt_dialect.BOOLEAN is sqlalchemy.sql.sqltypes.BOOLEAN
    assert firebolt_db.firebolt_dialect.REAL is sqlalchemy.sql.sqltypes.REAL
    assert issubclass(firebolt_db.firebolt_dialect.ARRAY, sqlalchemy.types.TypeEngine)


@mark.parametrize(
    ["firebolt_type", "alchemy_type"],
    [
        ("TEXT", sqltypes.TEXT),
        ("LONG", sqltypes.BIGINT),
        ("DECIMAL", sqltypes.NUMERIC),
        ("INT", sqltypes.INTEGER),
        ("TIMESTAMP", sqltypes.TIMESTAMP),
        ("TIMESTAMPTZ", sqltypes.TIMESTAMP),
        ("TIMESTAMPNTZ", sqltypes.TIMESTAMP),
    ],
)
def test_resolve_type(firebolt_type: str, alchemy_type: sqltypes.TypeEngine):
    assert resolve_type(firebolt_type.lower()) == alchemy_type


@mark.parametrize(
    ["firebolt_type", "item_type", "dimensions"],
    [
        ("ARRAY(INT NOT NULL)", sqltypes.INTEGER, 1),
        ("ARRAY(INT NULL)", sqltypes.INTEGER, 1),
        ("ARRAY(ARRAY(INT NULL))", sqltypes.INTEGER, 2),
    ],
)
def test_resolve_array_type(
    firebolt_type: str, item_type: sqltypes.TypeEngine, dimensions: int
):
    resolved_type = resolve_type(firebolt_type.lower())
    assert type(resolved_type.item_type) == item_type
    assert resolved_type.dimensions == dimensions
