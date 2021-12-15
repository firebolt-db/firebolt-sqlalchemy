import asyncio
from logging import getLogger
from os import environ

import nest_asyncio
from pytest import fixture
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.ext.asyncio import create_async_engine

nest_asyncio.apply()

LOGGER = getLogger(__name__)

ENGINE_NAME_ENV = "engine_name"
DATABASE_NAME_ENV = "database_name"
USERNAME_ENV = "username"
PASSWORD_ENV = "password"


def must_env(var_name: str) -> str:
    assert var_name in environ, f"Expected {var_name} to be provided in environment"
    LOGGER.info(f"{var_name}: {environ[var_name]}")
    return environ[var_name]


@fixture(scope="session")
def engine_name() -> str:
    return must_env(ENGINE_NAME_ENV)


@fixture(scope="session")
def database_name() -> str:
    return must_env(DATABASE_NAME_ENV)


@fixture(scope="session")
def username() -> str:
    return must_env(USERNAME_ENV)


@fixture(scope="session")
def password() -> str:
    return must_env(PASSWORD_ENV)


@fixture(scope="session")
def engine(
    username: str, password: str, database_name: str, engine_name: str
) -> Engine:
    registry.register("firebolt", "src.firebolt_db.firebolt_dialect", "FireboltDialect")
    return create_engine(
        f"firebolt://{username}:{password}@{database_name}/{engine_name}"
    )


@fixture(scope="session")
def connection(engine: Engine) -> Connection:
    return engine.connect()


@fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@fixture(scope="session")
def async_engine(
    username: str, password: str, database_name: str, engine_name: str
) -> Engine:
    registry.register(
        "firebolt_aio", "src.firebolt_db.firebolt_async_dialect", "AsyncFireboltDialect"
    )
    return create_async_engine(
        f"firebolt_aio://{username}:{password}@{database_name}/{engine_name}"
    )


@fixture(scope="session")
async def async_connection(async_engine: Engine, event_loop) -> Connection:
    return await async_engine.connect()


@fixture
def ex_table_name() -> str:
    return "ex_lineitem_alchemy"


@fixture
def ex_table_query(ex_table_name) -> str:
    return f"""
            CREATE EXTERNAL TABLE {ex_table_name}
            (       l_orderkey              LONG,
                    l_partkey               LONG,
                    l_suppkey               LONG,
                    l_linenumber            INT,
                    l_quantity              LONG,
                    l_extendedprice         LONG,
                    l_discount              LONG,
                    l_tax                   LONG,
                    l_returnflag            TEXT,
                    l_linestatus            TEXT,
                    l_shipdate              TEXT,
                    l_commitdate            TEXT,
                    l_receiptdate           TEXT,
                    l_shipinstruct          TEXT,
                    l_shipmode              TEXT,
                    l_comment               TEXT
            )
            URL = 's3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/'
            OBJECT_PATTERN = '*.parquet'
            TYPE = (PARQUET);
            """


@fixture(scope="class")
def fact_table_name() -> str:
    return "test_alchemy"


@fixture(scope="class", autouse=True)
def setup_test_tables(connection: Connection, engine: Engine, fact_table_name: str):
    connection.execute(
        f"""
        CREATE FACT TABLE IF NOT EXISTS {fact_table_name}
        (
            idx INT,
            dummy TEXT
        ) PRIMARY INDEX idx;
        """
    )
    assert engine.dialect.has_table(engine, fact_table_name)
    yield
    # Teardown
    connection.execute(f"DROP TABLE IF EXISTS {fact_table_name}")
    assert not engine.dialect.has_table(engine, fact_table_name)
