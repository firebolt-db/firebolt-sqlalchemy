import os
from unittest import mock

import pytest
import sqlalchemy
from conftest import MockAsyncDBApi, MockDBApi
from sqlalchemy.engine import url
from sqlalchemy.util import await_only, greenlet_spawn

import firebolt_db  # SQLAlchemy package
from firebolt_db.firebolt_async_dialect import (
    AsyncAPIWrapper,
    AsyncConnectionWrapper,
    AsyncCursorWrapper,
    AsyncFireboltDialect,
)
from firebolt_db.firebolt_async_dialect import (
    dialect as async_dialect_definition,
)
from firebolt_db.firebolt_dialect import (
    FireboltCompiler,
    FireboltDialect,
    FireboltIdentifierPreparer,
    FireboltTypeCompiler,
)
from firebolt_db.firebolt_dialect import dialect as dialect_definition


class TestAsyncFireboltDialect:
    def test_create_dialect(self, async_dialect: AsyncFireboltDialect):
        assert issubclass(async_dialect_definition, AsyncFireboltDialect)
        assert type(AsyncFireboltDialect.dbapi()) == AsyncAPIWrapper
        assert async_dialect.name == "firebolt"
        assert async_dialect.driver == "firebolt_aio"
        assert issubclass(async_dialect.preparer, FireboltIdentifierPreparer)
        assert issubclass(async_dialect.statement_compiler, FireboltCompiler)
        # SQLAlchemy's DefaultDialect creates an instance of
        # type_compiler behind the scenes
        assert isinstance(async_dialect.type_compiler, FireboltTypeCompiler)
        assert async_dialect.context == {}

    @pytest.mark.asyncio
    async def test_create_wrapper(self, async_api: mock.AsyncMock(spec=MockAsyncDBApi)):
        def test_wrapper():
            async_api.paramstyle = "quoted"
            wrapper = AsyncAPIWrapper(async_api)
            assert wrapper.dbapi == async_api
            assert wrapper.paramstyle == "quoted"
            wrapper.connect("test arg")

        await greenlet_spawn(test_wrapper)
        async_api.connect.assert_called_once_with("test arg")

    @pytest.mark.asyncio
    async def test_connection_wrapper(
        self, async_api: mock.AsyncMock(spec=MockAsyncDBApi)
    ):
        def test_connection():
            wrapper = AsyncConnectionWrapper(async_api, await_only(async_api.connect()))
            # Check call propagation
            wrapper.commit()
            wrapper.rollback()
            wrapper.close()
            assert isinstance(wrapper.cursor(), AsyncCursorWrapper)

        await greenlet_spawn(test_connection)
        async_api.connect.return_value.commit.assert_awaited_once()
        async_api.connect.return_value.rollback.assert_awaited_once()
        async_api.connect.return_value._aclose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cursor_execute(
        self,
        async_api: mock.AsyncMock(spec=MockAsyncDBApi),
        async_connection,
        async_cursor,
    ):
        def test_cursor():
            async_connection.cursor.return_value = async_cursor
            conn_wrapper = AsyncConnectionWrapper(async_api, async_connection)
            wrapper = AsyncCursorWrapper(conn_wrapper)
            wrapper.execute("INSERT INTO test(a, b) VALUES (?, ?)", [(1, "a")])
            assert wrapper.description == async_cursor.description
            assert wrapper.rowcount == async_cursor.rowcount

        async_cursor.rowcount = -1
        await greenlet_spawn(test_cursor)
        async_cursor.execute.assert_awaited_once_with(
            "INSERT INTO test(a, b) VALUES (?, ?)", [(1, "a")]
        )
        async_cursor.fetchall.assert_awaited_once()
        async_cursor.close.assert_called_once()

        async_cursor.execute.reset_mock()
        async_cursor.fetchall.reset_mock()
        async_cursor.close.reset_mock()
        async_cursor.description = None
        async_cursor.rowcount = 100

        await greenlet_spawn(test_cursor)
        async_cursor.execute.assert_awaited_once_with(
            "INSERT INTO test(a, b) VALUES (?, ?)", [(1, "a")]
        )
        async_cursor.fetchall.assert_not_awaited()
        async_cursor.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_cursor_close(
        self,
        async_api: mock.AsyncMock(spec=MockAsyncDBApi),
        async_connection,
        async_cursor,
    ):
        def test_cursor():
            async_connection.cursor.return_value = async_cursor
            conn_wrapper = AsyncConnectionWrapper(async_api, async_connection)
            wrapper = AsyncCursorWrapper(conn_wrapper)
            wrapper._rows = [1, 2, 3]
            wrapper.close()
            assert wrapper._rows == []

        await greenlet_spawn(test_cursor)

    @pytest.mark.asyncio
    async def test_cursor_executemany(
        self,
        async_api: mock.AsyncMock(spec=MockAsyncDBApi),
        async_connection,
        async_cursor,
    ):
        def test_cursor():
            async_connection.cursor.return_value = async_cursor
            conn_wrapper = AsyncConnectionWrapper(async_api, async_connection)
            wrapper = AsyncCursorWrapper(conn_wrapper)
            with pytest.raises(NotImplementedError):
                wrapper.executemany(
                    "INSERT INTO test(a, b) VALUES (?, ?)", [(1, "a"), (2, "b")]
                )

        await greenlet_spawn(test_cursor)

    @pytest.mark.asyncio
    async def test_cursor_fetch(
        self,
        async_api: mock.AsyncMock(spec=MockAsyncDBApi),
        async_connection,
        async_cursor,
    ):
        def test_cursor():
            async_connection.cursor.return_value = async_cursor
            conn_wrapper = AsyncConnectionWrapper(async_api, async_connection)
            wrapper = AsyncCursorWrapper(conn_wrapper)
            wrapper._rows = [1, 2, 3, 4, 5, 6]
            assert wrapper.fetchone() == 1
            assert wrapper.fetchmany() == [2]
            assert wrapper.fetchmany(2) == [3, 4]
            assert wrapper.fetchall() == [5, 6]

        await greenlet_spawn(test_cursor)


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
        connection.execute.assert_called_once_with(
            "select schema_name from information_schema.databases"
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
            connection.execute.assert_called_once_with(expected_query)
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
