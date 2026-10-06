from collections import deque

import pytest
from conftest import MockAsyncConnection, MockAsyncCursor, MockAsyncDBApi
from mock import AsyncMock
from sqlalchemy.util import greenlet_spawn

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
    FireboltIdentifierPreparer,
    FireboltTypeCompiler,
)


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

    @pytest.mark.skip("Failing with nested run() in trino")
    async def test_create_api_wrapper(self, async_api: AsyncMock(spec=MockAsyncDBApi)):
        def test_connect() -> AsyncAPIWrapper:
            async_api.paramstyle = "quoted"
            wrapper = AsyncAPIWrapper(async_api)
            wrapper.connect("test arg")
            return wrapper

        wrapper = await greenlet_spawn(test_connect)
        assert wrapper.dbapi == async_api
        assert wrapper.paramstyle == "quoted"
        async_api.connect.assert_called_once_with("test arg")

    async def test_connection_wrapper(
        self,
        async_api,
        async_connection,
        async_cursor,
    ):
        def test_connection() -> AsyncConnectionWrapper:
            # Set up the mock to return the async_connection when connect() is called
            async_api.connect.return_value = async_connection
            # Set up the connection to return the async_cursor when cursor() is called
            async_connection.cursor.return_value = async_cursor

            wrapper = AsyncConnectionWrapper(async_api, async_connection)

            # Monkey patch the _aenter_cursor method to avoid the __aenter__ issue
            def mock_aenter_cursor(self, cursor):
                return cursor  # Return cursor directly without calling __aenter__

            wrapper._cursor_cls._aenter_cursor = mock_aenter_cursor

            # Check call propagation
            wrapper.commit()
            wrapper.rollback()
            wrapper.close()
            return wrapper

        wrapper = await greenlet_spawn(test_connection)

        cursor_wrapper = wrapper.cursor()
        assert isinstance(cursor_wrapper, AsyncCursorWrapper)

        async_connection.commit.assert_called_once()
        async_connection.aclose.assert_awaited_once()

    async def test_cursor_execute(
        self,
        async_api: AsyncMock(spec=MockAsyncDBApi),
        async_connection: AsyncMock(spec=MockAsyncConnection),
        async_cursor: AsyncMock(spec=MockAsyncCursor),
    ):
        def test_cursor() -> AsyncCursorWrapper:
            async_connection.cursor.return_value = async_cursor
            conn_wrapper = AsyncConnectionWrapper(async_api, async_connection)
            wrapper = AsyncCursorWrapper(conn_wrapper)
            wrapper.execute("INSERT INTO test(a, b) VALUES (?, ?)", [(1, "a")])
            return wrapper

        async_cursor.description = "dummy"
        async_cursor.rowcount = -1
        wrapper = await greenlet_spawn(test_cursor)
        assert wrapper.description == "dummy"
        assert wrapper.rowcount == -1
        async_cursor.execute.assert_awaited_once_with(
            "INSERT INTO test(a, b) VALUES (?, ?)", [(1, "a")]
        )
        async_cursor.fetchall.assert_awaited_once()

    async def test_cursor_execute_no_fetch(
        self,
        async_api: AsyncMock(spec=MockAsyncDBApi),
        async_connection: AsyncMock(spec=MockAsyncConnection),
        async_cursor: AsyncMock(spec=MockAsyncCursor),
    ):
        def test_cursor() -> AsyncCursorWrapper:
            async_connection.cursor.return_value = async_cursor
            conn_wrapper = AsyncConnectionWrapper(async_api, async_connection)
            wrapper = AsyncCursorWrapper(conn_wrapper)
            wrapper.execute("INSERT INTO test(a, b) VALUES (?, ?)", [(1, "a")])
            return wrapper

        async_cursor.description = None
        async_cursor.rowcount = 100

        wrapper = await greenlet_spawn(test_cursor)
        assert wrapper.description is None
        assert wrapper.rowcount == 100
        async_cursor.execute.assert_awaited_once_with(
            "INSERT INTO test(a, b) VALUES (?, ?)", [(1, "a")]
        )
        async_cursor.fetchall.assert_not_awaited()

    async def test_cursor_close(
        self,
        async_api: AsyncMock(spec=MockAsyncDBApi),
        async_connection: AsyncMock(spec=MockAsyncConnection),
        async_cursor: AsyncMock(spec=MockAsyncCursor),
    ):
        def test_cursor():
            async_connection.cursor.return_value = async_cursor
            conn_wrapper = AsyncConnectionWrapper(async_api, async_connection)
            wrapper = AsyncCursorWrapper(conn_wrapper)
            wrapper._rows = deque([1, 2, 3])
            wrapper.close()
            assert len(wrapper._rows) == 0
            async_cursor.close.assert_called_once()

        await greenlet_spawn(test_cursor)

    async def test_cursor_executemany(
        self,
        async_api: AsyncMock(spec=MockAsyncDBApi),
        async_connection: AsyncMock(spec=MockAsyncConnection),
        async_cursor: AsyncMock(spec=MockAsyncCursor),
    ):
        def test_cursor():
            async_connection.cursor.return_value = async_cursor
            conn_wrapper = AsyncConnectionWrapper(async_api, async_connection)
            wrapper = AsyncCursorWrapper(conn_wrapper)
            wrapper.executemany(
                "INSERT INTO test(a, b) VALUES (?, ?)", [(1, "a"), (2, "b")]
            )

        await greenlet_spawn(test_cursor)
        async_cursor.executemany.assert_awaited_once_with(
            "INSERT INTO test(a, b) VALUES (?, ?)", [(1, "a"), (2, "b")]
        )

    async def test_cursor_fetch(
        self,
        async_api: AsyncMock(spec=MockAsyncDBApi),
        async_connection: AsyncMock(spec=MockAsyncConnection),
        async_cursor: AsyncMock(spec=MockAsyncCursor),
    ):
        def test_cursor():
            async_connection.cursor.return_value = async_cursor
            # Set arraysize to 1 initially
            async_cursor.arraysize = 1
            conn_wrapper = AsyncConnectionWrapper(async_api, async_connection)
            wrapper = AsyncCursorWrapper(conn_wrapper)
            wrapper._rows = deque([1, 2, 3, 4, 5, 6, 7, 8])
            assert wrapper.fetchone() == 1
            assert wrapper.fetchmany() == [2]
            async_cursor.arraysize = 2
            assert wrapper.fetchmany() == [3, 4]
            async_cursor.arraysize = 1
            assert wrapper.fetchmany(2) == [5, 6]
            assert wrapper.fetchall() == [7, 8]

        await greenlet_spawn(test_cursor)
