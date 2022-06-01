import os
from unittest import mock

import sqlalchemy
from conftest import MockCursor, MockDBApi
from sqlalchemy.engine import url
from sqlalchemy.sql import text

import firebolt_db  # SQLAlchemy package
from firebolt_db.firebolt_dialect import (
    FireboltCompiler,
    FireboltDialect,
    FireboltIdentifierPreparer,
    FireboltTypeCompiler,
)
from firebolt_db.firebolt_dialect import dialect as dialect_definition


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

    def test_create_connect_args(self, dialect: FireboltDialect):
        connection_url = (
            "test_engine://test_user@email:test_password@test_db_name/test_engine_name"
        )
        u = url.make_url(connection_url)
        with mock.patch.dict(os.environ, {"FIREBOLT_BASE_URL": "test_url"}):
            result_list, result_dict = dialect.create_connect_args(u)
            assert result_dict["engine_name"] == "test_engine_name"
            assert result_dict["username"] == "test_user@email"
            assert result_dict["password"] == "test_password"
            assert result_dict["database"] == "test_db_name"
            assert result_dict["api_endpoint"] == "test_url"
            assert result_list == []
        # No endpoint override
        with mock.patch.dict(os.environ, {}, clear=True):
            result_list, result_dict = dialect.create_connect_args(u)
            assert "api_endpoint" not in result_dict
            assert result_list == []

    def test_create_connect_args_set_params(self, dialect: FireboltDialect):
        connection_url = (
            "test_engine://test_user@email:test_password@test_db_name/test_engine_name"
            "?account_name=FB&param1=1&param2=2"
        )
        u = url.make_url(connection_url)
        result_list, result_dict = dialect.create_connect_args(u)
        assert (
            "account_name" in result_dict
        ), "account_name was not parsed correctly from connection string"
        assert dialect._set_parameters == {"param1": "1", "param2": "2"}

    def test_do_execute(
        self, dialect: FireboltDialect, cursor: mock.Mock(spec=MockCursor)
    ):
        dialect._set_parameters = {"a": "b"}
        dialect.do_execute(cursor, "SELECT *", (1, 22), None)
        cursor.execute.assert_called_once_with(
            "SELECT *", parameters=(1, 22), set_parameters={"a": "b"}
        )

    def test_schema_names(
        self, dialect: FireboltDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        def row_with_schema(name):
            return mock.Mock(schema_name=name)

        connection.execute.return_value = [
            row_with_schema("schema1"),
            row_with_schema("schema2"),
        ]
        result = dialect.get_schema_names(connection)
        assert result == ["schema1", "schema2"]
        connection.execute.assert_called_once()
        assert str(connection.execute.call_args[0][0].compile()) == str(
            text("select schema_name from information_schema.databases").compile()
        )

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
            multi_column_row(["name1", "INT", "YES"]),
            multi_column_row(["name2", "date", "no"]),
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


def test_get_is_nullable():
    assert firebolt_db.firebolt_dialect.get_is_nullable("YES")
    assert firebolt_db.firebolt_dialect.get_is_nullable("yes")
    assert not firebolt_db.firebolt_dialect.get_is_nullable("NO")
    assert not firebolt_db.firebolt_dialect.get_is_nullable("no")
    assert not firebolt_db.firebolt_dialect.get_is_nullable("ABC")


def test_types():
    assert firebolt_db.firebolt_dialect.CHAR is sqlalchemy.sql.sqltypes.CHAR
    assert firebolt_db.firebolt_dialect.DATE is sqlalchemy.sql.sqltypes.DATE
    assert firebolt_db.firebolt_dialect.DATETIME is sqlalchemy.sql.sqltypes.DATETIME
    assert firebolt_db.firebolt_dialect.INTEGER is sqlalchemy.sql.sqltypes.INTEGER
    assert firebolt_db.firebolt_dialect.BIGINT is sqlalchemy.sql.sqltypes.BIGINT
    assert firebolt_db.firebolt_dialect.TIMESTAMP is sqlalchemy.sql.sqltypes.TIMESTAMP
    assert firebolt_db.firebolt_dialect.VARCHAR is sqlalchemy.sql.sqltypes.VARCHAR
    assert firebolt_db.firebolt_dialect.BOOLEAN is sqlalchemy.sql.sqltypes.BOOLEAN
    assert firebolt_db.firebolt_dialect.FLOAT is sqlalchemy.sql.sqltypes.FLOAT
    assert issubclass(firebolt_db.firebolt_dialect.ARRAY, sqlalchemy.types.TypeEngine)
