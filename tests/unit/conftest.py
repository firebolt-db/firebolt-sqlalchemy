

from pytest import fixture
from firebolt_db import firebolt_dialect
from unittest import mock


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
