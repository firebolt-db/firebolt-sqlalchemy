from unittest import mock

from pytest import fixture

from firebolt_db import firebolt_dialect


class DBApi:
    def execute():
        pass

    def executemany():
        pass


@fixture
def dialect():
    return firebolt_dialect.FireboltDialect()


@fixture
def connection():
    return mock.Mock(spec=DBApi)
