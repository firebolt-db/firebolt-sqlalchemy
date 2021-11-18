from _pytest.fixtures import fixture
import pytest
import os

import sqlalchemy

from firebolt_db import firebolt_dialect

from sqlalchemy import create_engine
from sqlalchemy.dialects import registry


test_username = os.environ["username"]
test_password = os.environ["password"]
test_engine_name = os.environ["engine_name"]
test_db_name = os.environ["db_name"]


@pytest.fixture(scope="class")
def get_engine():
    registry.register("firebolt", "src.firebolt_db.firebolt_dialect", "FireboltDialect")
    return create_engine(f"firebolt://{test_username}:{test_password}@{test_db_name}/{test_engine_name}")


@pytest.fixture(scope="class")
def get_connection(get_engine):
    engine = get_engine
    # TODO: once commit is implemented remove execution options
    return engine.connect().execution_options(autocommit=False)


dialect = firebolt_dialect.FireboltDialect()


class TestFireboltDialect:

    test_table = "test_alchemy"

    def create_test_table(self, get_connection, get_engine, table):
        connection = get_connection
        connection.commit = lambda x: x
        connection.execute(f"""
        CREATE FACT TABLE IF NOT EXISTS {table}
        (
            dummy TEXT
        ) PRIMARY INDEX dummy;
        """)
        assert get_engine.dialect.has_table(get_engine, table)

    def drop_test_table(self, get_connection, get_engine, table):
        connection = get_connection
        connection.commit = lambda x: x
        connection.execute(f"DROP TABLE IF EXISTS {table}")
        assert not get_engine.dialect.has_table(get_engine, table)

    @pytest.fixture(scope="class", autouse=True)
    def setup_test_tables(self, get_connection, get_engine):
        self.create_test_table(get_connection, get_engine, self.test_table)
        yield
        self.drop_test_table(get_connection, get_engine, self.test_table)

    @pytest.mark.skip(reason="Commit not implemented in sdk")
    def test_create_ex_table(self, get_engine, get_connection):
        engine = get_engine
        connection = get_connection
        connection.execute("""
        CREATE EXTERNAL TABLE ex_lineitem_alchemy
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
        )
        assert engine.dialect.has_table(engine, "ex_lineitem_alchemy")
        # Cleanup
        connection.execute("DROP TABLE ex_lineitem_alchemy;")
        assert not engine.dialect.has_table(engine, "ex_lineitem_alchemy")

    @pytest.mark.skip(reason="Commit not implemented in sdk")
    def test_data_write(self, get_connection):
        connection = get_connection
        connection.execute(
            "INSERT INTO test_alchemy(dummy) VALUES ('some_text')"
        )
        result = connection.execute("SELECT * FROM lineitem")
        assert result.rowcount == 1
        connection.execute(
            "DELETE FROM lineitem WHERE l_orderkey=10"
        )
        result = connection.execute("SELECT * FROM lineitem")
        assert result.rowcount == 0

    def test_get_schema_names(self, get_engine):
        engine = get_engine
        results = dialect.get_schema_names(engine)
        assert test_db_name in results

    def test_has_table(self, get_engine):
        schema = test_db_name
        engine = get_engine
        results = dialect.has_table(engine, self.test_table, schema)
        assert results == 1

    def test_get_table_names(self, get_engine):
        schema = test_db_name
        engine = get_engine
        results = dialect.get_table_names(engine, schema)
        assert len(results) > 0

    def test_get_columns(self, get_engine):
        schema = test_db_name
        engine = get_engine
        results = dialect.get_columns(engine, self.test_table, schema)
        assert len(results) > 0
        row = results[0]
        assert isinstance(row, dict)
        row_keys = list(row.keys())
        assert row_keys[0] == "name"
        assert row_keys[1] == "type"
        assert row_keys[2] == "nullable"
        assert row_keys[3] == "default"