from unittest import mock

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


@fixture
def dialect() -> firebolt_dialect.FireboltDialect:
    return firebolt_dialect.FireboltDialect()


@fixture
def async_dialect() -> firebolt_async_dialect.AsyncFireboltDialect:
    return firebolt_async_dialect.AsyncFireboltDialect()


@fixture
def connection() -> mock.Mock(spec=MockDBApi):
    return mock.Mock(spec=MockDBApi)


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


@fixture
def async_api() -> mock.AsyncMock(spec=MockAsyncDBApi):
    return mock.AsyncMock(spec=MockAsyncDBApi)


class MockAsyncConnection:
    def cursor():
        pass


class MockAsyncCursor:
    description = ""
    rowcount = -1

    async def execute():
        pass

    async def executemany():
        pass

    async def fetchall():
        pass

    def close():
        pass


@fixture
def async_connection() -> mock.AsyncMock(spec=MockAsyncConnection):
    return mock.AsyncMock(spec=MockAsyncConnection)


@fixture
def async_cursor() -> mock.AsyncMock(spec=MockAsyncCursor):
    return mock.AsyncMock(spec=MockAsyncCursor)
