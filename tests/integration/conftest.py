import urllib.parse
from logging import getLogger
from os import environ
from typing import List

from pytest import fixture
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.ext.asyncio import create_async_engine

LOGGER = getLogger(__name__)

ENGINE_NAME_ENV = "ENGINE_NAME"
DATABASE_NAME_ENV = "DATABASE_NAME"
ACCOUNT_NAME_ENV = "ACCOUNT_NAME"
CLIENT_ID_ENV = "CLIENT_ID"
CLIENT_KEY_ENV = "CLIENT_SECRET"


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
def client_id() -> str:
    return must_env(CLIENT_ID_ENV)


@fixture(scope="session")
def client_key() -> str:
    return urllib.parse.quote_plus(must_env(CLIENT_KEY_ENV))


@fixture(scope="session")
def account_name() -> str:
    return must_env(ACCOUNT_NAME_ENV)


@fixture(scope="session")
def engine(
    client_id: str,
    client_key: str,
    database_name: str,
    engine_name: str,
    account_name: str,
) -> Engine:
    return create_engine(
        f"firebolt://{client_id}:{client_key}@{database_name}/{engine_name}"
        + f"?account_name={account_name}"
    )


@fixture(scope="session")
def engine_service_account(
    client_id: str,
    client_key: str,
    database_name: str,
    engine_name: str,
    account_name: str,
) -> Engine:
    return create_engine(
        f"firebolt://{client_id}:{client_key}@{database_name}/{engine_name}"
        + f"?account_name={account_name}"
    )


@fixture(scope="session")
def connection(engine: Engine) -> Connection:
    with engine.connect() as c:
        yield c


@fixture(scope="session")
def connection_service_account(engine_service_account: Engine) -> Connection:
    with engine_service_account.connect() as c:
        yield c


@fixture()
def async_engine(
    client_id: str,
    client_key: str,
    database_name: str,
    engine_name: str,
    account_name: str,
) -> Engine:
    return create_async_engine(
        f"asyncio+firebolt://{client_id}:{client_key}@{database_name}/{engine_name}"
        + f"?account_name={account_name}"
    )


@fixture()
async def async_connection(
    async_engine: Engine,
) -> Connection:
    async with async_engine.connect() as c:
        yield c


@fixture
def ex_table_name() -> str:
    return "ex_lineitem_alchemy"


@fixture
def ex_table_query(ex_table_name: str) -> str:
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
def type_table_name() -> str:
    return "types_alchemy"


@fixture(scope="class")
def firebolt_columns() -> List[str]:
    return [
        "INTEGER",
        "NUMERIC",
        "BIGINT",
        "REAL",
        "DOUBLE PRECISION",
        "TEXT",
        "TIMESTAMPNTZ",
        "TIMESTAMPTZ",
        "DATE",
        "TIMESTAMP",
        "BOOLEAN",
        "BYTEA",
    ]


@fixture(scope="class")
def type_table_query(firebolt_columns: List[str], type_table_name: str) -> str:
    col_names = [c.replace(" ", "_").lower() for c in firebolt_columns]
    cols = ",\n".join(
        [f"c_{name} {c_type}" for name, c_type in zip(col_names, firebolt_columns)]
    )
    return f"""
        CREATE DIMENSION TABLE {type_table_name}
        (
            {cols},
            c_array ARRAY(ARRAY(INTEGER))
        );
    """


@fixture(scope="class")
def fact_table_name() -> str:
    return "test_alchemy"


@fixture(scope="class")
def dimension_table_name() -> str:
    return "test_alchemy_dimension"


@fixture(scope="class", autouse=True)
def setup_test_tables(
    connection: Connection,
    engine: Engine,
    fact_table_name: str,
    dimension_table_name: str,
    type_table_query: str,
    type_table_name: str,
):
    connection.execute(
        text(
            f"""
        CREATE FACT TABLE IF NOT EXISTS {fact_table_name}
        (
            idx INT,
            dummy TEXT
        ) PRIMARY INDEX idx;
        """
        )
    )
    connection.execute(
        text(
            f"""
        CREATE DIMENSION TABLE IF NOT EXISTS {dimension_table_name}
        (
            idx INT,
            dummy TEXT
        );
        """
        )
    )
    connection.execute(text(type_table_query))
    assert engine.dialect.has_table(connection, fact_table_name)
    assert engine.dialect.has_table(connection, dimension_table_name)
    assert engine.dialect.has_table(connection, type_table_name)
    yield
    # Teardown
    connection.execute(text(f"DROP TABLE IF EXISTS {fact_table_name} CASCADE;"))
    connection.execute(text(f"DROP TABLE IF EXISTS {dimension_table_name} CASCADE;"))
    connection.execute(text(f"DROP TABLE IF EXISTS {type_table_name} CASCADE;"))
    assert not engine.dialect.has_table(connection, fact_table_name)
    assert not engine.dialect.has_table(connection, dimension_table_name)
    assert not engine.dialect.has_table(connection, type_table_name)
