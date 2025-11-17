from unittest import mock

from mock import AsyncMock
from pytest import fixture

from firebolt_db import firebolt_async_dialect, firebolt_dialect


class MockDBApi:
    class DatabaseError:
        pass

    class Error:
        pass

    class IntegrityError:
        pass

    class NotSupportedError:
        pass

    class OperationalError:
        pass

    class ProgrammingError:
        pass

    paramstyle = ""

    def execute():
        pass

    def executemany():
        pass

    def connect():
        pass


class MockCursor:
    def execute():
        pass

    def executemany():
        pass

    def fetchall():
        pass

    def close():
        pass


class MockAsyncDBApi:
    class DatabaseError:
        pass

    class Error:
        pass

    class IntegrityError:
        pass

    class NotSupportedError:
        pass

    class OperationalError:
        pass

    class ProgrammingError:
        pass

    paramstyle = ""

    async def connect():
        pass


class MockAsyncConnection:
    def cursor(self):
        # Mock implementation for cursor creation
        pass

    def commit(self):
        # Mock implementation for commit
        pass

    def rollback(self):
        # Mock implementation for rollback
        pass

    async def aclose(self):
        # Mock implementation for async close
        pass


class MockAsyncCursor:
    description = ""
    rowcount = -1
    arraysize = 1

    async def execute(self):
        # Mock implementation for async execute
        pass

    async def executemany(self, **kwargs):
        # Mock implementation for async executemany
        pass

    async def fetchall(self):
        # Mock implementation for async fetchall
        pass

    def close(self):
        # Mock implementation for close
        pass

    async def aclose(self):
        # Mock implementation for async close
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Mock implementation for async context manager exit
        pass


@fixture
def dialect() -> firebolt_dialect.FireboltDialect:
    return firebolt_dialect.FireboltDialect()


@fixture
def async_dialect() -> firebolt_async_dialect.AsyncFireboltDialect:
    return firebolt_async_dialect.AsyncFireboltDialect()


@fixture
def connection() -> mock.Mock(spec=MockDBApi):
    return mock.Mock(spec=MockDBApi)


@fixture
def cursor() -> mock.Mock(spec=MockCursor):
    return mock.Mock(spec=MockCursor)


@fixture
def async_api() -> AsyncMock(spec=MockAsyncDBApi):
    return AsyncMock(spec=MockAsyncDBApi)


@fixture
def async_connection() -> AsyncMock(spec=MockAsyncConnection):
    return AsyncMock(spec=MockAsyncConnection)


@fixture
def async_cursor() -> AsyncMock(spec=MockAsyncCursor):
    mock = AsyncMock(spec=MockAsyncCursor)
    # Make sure the async context manager methods return the mock itself

    async def aenter():
        # Return the mock cursor for async context manager entry
        return mock

    async def aexit(*args):
        # Mock implementation for async context manager exit
        pass

    # Make sure close() returns a coroutine that can be awaited
    async def close_coro():
        # Mock implementation for async close
        pass

    mock.__aenter__ = AsyncMock(side_effect=aenter)
    mock.__aexit__ = AsyncMock(side_effect=aexit)
    mock.close.return_value = close_coro()
    return mock
