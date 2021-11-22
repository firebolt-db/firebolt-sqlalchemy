from unittest import mock

from pytest import fixture

from firebolt_db import firebolt_dialect


class MockDBApi:
    def execute():
        pass

    def executemany():
        pass


@fixture
def dialect() -> firebolt_dialect.FireboltDialect:
    return firebolt_dialect.FireboltDialect()


@fixture
def connection() -> mock.Mock(spec=MockDBApi):
    return mock.Mock(spec=MockDBApi)
