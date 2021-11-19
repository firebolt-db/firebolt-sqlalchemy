import os
from unittest import mock

import sqlalchemy
from sqlalchemy.engine import url

import firebolt_db
from firebolt_db import firebolt_dialect


class TestFireboltDialect:
    def test_create_dialect(self, dialect):
        assert issubclass(firebolt_dialect.dialect, firebolt_dialect.FireboltDialect)
        assert isinstance(firebolt_dialect.FireboltDialect.dbapi(), type(firebolt_db))
        assert dialect.name == "firebolt"
        assert dialect.driver == "firebolt"
        assert issubclass(dialect.preparer, firebolt_dialect.FireboltIdentifierPreparer)
        assert issubclass(dialect.statement_compiler, firebolt_dialect.FireboltCompiler)
        # SQLAlchemy's DefaultDialect creates an instance of type_compiler behind the scenes
        assert isinstance(dialect.type_compiler, firebolt_dialect.FireboltTypeCompiler)
        assert dialect.context == {}

    def test_create_connect_args(self, dialect):
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

    def test_schema_names(self, dialect, connection):
        def row_with_schema(name):
            return mock.Mock(schema_name=name)

        connection.execute.return_value = [
            row_with_schema("schema1"),
            row_with_schema("schema2"),
        ]
        result = dialect.get_schema_names(connection)
        assert result == ["schema1", "schema2"]
        connection.execute.assert_called_once_with(
            "select schema_name from information_schema.databases"
        )

    def test_table_names(self, dialect, connection):
        def row_with_table_name(name):
            return mock.Mock(table_name=name)

        connection.execute.return_value = [
            row_with_table_name("table1"),
            row_with_table_name("table2"),
        ]

        result = dialect.get_table_names(connection)
        assert result == ["table1", "table2"]
        connection.execute.assert_called_once_with(
            "select table_name from information_schema.tables"
        )
        connection.execute.reset_mock()
        result = dialect.get_table_names(connection, schema="schema")
        assert result == ["table1", "table2"]
        connection.execute.assert_called_once_with(
            "select table_name from information_schema.tables"
            " where table_schema = 'schema'"
        )

    def test_view_names(self, dialect, connection):
        assert dialect.get_view_names(connection) == []

    def test_table_options(self, dialect, connection):
        assert dialect.get_table_options(connection, "table") == {}

    def test_columns(self, dialect, connection):
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
            connection.execute.assert_called_once_with(expected_query)
            connection.execute.reset_mock()

    def test_pk_constraint(self, dialect, connection):
        assert dialect.get_pk_constraint(connection, "table") == {
            "constrained_columns": [],
            "name": None,
        }

    def test_foreign_keys(self, dialect, connection):
        assert dialect.get_foreign_keys(connection, "table") == []

    def test_check_constraints(self, dialect, connection):
        assert dialect.get_check_constraints(connection, "table") == []

    def test_table_comment(self, dialect, connection):
        assert dialect.get_table_comment(connection, "table") == {"text": ""}

    def test_indexes(self, dialect, connection):
        assert dialect.get_indexes(connection, "table") == []

    def test_unique_constraints(self, dialect, connection):
        assert dialect.get_unique_constraints(connection, "table") == []

    def test_unicode_returns(self, dialect, connection):
        assert dialect._check_unicode_returns(connection)

    def test_unicode_description(self, dialect, connection):
        assert dialect._check_unicode_description(connection)


def test_get_is_nullable():
    assert firebolt_dialect.get_is_nullable("YES")
    assert firebolt_dialect.get_is_nullable("yes")
    assert not firebolt_dialect.get_is_nullable("NO")
    assert not firebolt_dialect.get_is_nullable("no")
    assert not firebolt_dialect.get_is_nullable("ABC")


def test_types():
    assert firebolt_dialect.CHAR is sqlalchemy.sql.sqltypes.CHAR
    assert firebolt_dialect.DATE is sqlalchemy.sql.sqltypes.DATE
    assert firebolt_dialect.DATETIME is sqlalchemy.sql.sqltypes.DATETIME
    assert firebolt_dialect.INTEGER is sqlalchemy.sql.sqltypes.INTEGER
    assert firebolt_dialect.BIGINT is sqlalchemy.sql.sqltypes.BIGINT
    assert firebolt_dialect.TIMESTAMP is sqlalchemy.sql.sqltypes.TIMESTAMP
    assert firebolt_dialect.VARCHAR is sqlalchemy.sql.sqltypes.VARCHAR
    assert firebolt_dialect.BOOLEAN is sqlalchemy.sql.sqltypes.BOOLEAN
    assert firebolt_dialect.FLOAT is sqlalchemy.sql.sqltypes.FLOAT
    assert issubclass(firebolt_dialect.ARRAY, sqlalchemy.types.TypeEngine)
