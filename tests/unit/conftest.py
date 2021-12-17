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
    def cursor():
        pass


class MockAsyncCursor:
    description = ""
    rowcount = -1
    arraysize = 1

    async def execute():
        pass

    async def executemany():
        pass

    async def fetchall():
        pass

    def close():
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
def async_api() -> AsyncMock(spec=MockAsyncDBApi):
    return AsyncMock(spec=MockAsyncDBApi)


@fixture
def async_connection() -> AsyncMock(spec=MockAsyncConnection):
    return AsyncMock(spec=MockAsyncConnection)


@fixture
def async_cursor() -> AsyncMock(spec=MockAsyncCursor):
    return AsyncMock(spec=MockAsyncCursor)
