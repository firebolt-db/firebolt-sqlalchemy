import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import OperationalError


class TestFireboltDialect:

    test_table = "test_alchemy"

    def create_test_table(self, connection: Connection, engine: Engine, table: str):
        connection.execute(
            f"""
            CREATE FACT TABLE IF NOT EXISTS {table}
            (
                idx INT,
                dummy TEXT
            ) PRIMARY INDEX idx;
            """
        )
        assert engine.dialect.has_table(engine, table)

    def drop_test_table(self, connection: Connection, engine: Engine, table: str):
        connection.execute(f"DROP TABLE IF EXISTS {table}")
        assert not engine.dialect.has_table(engine, table)

    @pytest.fixture(scope="class", autouse=True)
    def setup_test_tables(self, connection: Connection, engine: Engine):
        self.create_test_table(connection, engine, self.test_table)
        yield
        self.drop_test_table(connection, engine, self.test_table)

    def test_create_ex_table(self, connection: Connection, engine: Engine):
        connection.execute(
            """
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

    def test_data_write(self, connection: Connection):
        connection.execute(
            f"INSERT INTO {self.test_table}(idx, dummy) VALUES (1, 'some_text')"
        )
        result = connection.execute(f"SELECT * FROM {self.test_table}")
        assert len(result.fetchall()) == 1
        # Update not supported
        with pytest.raises(OperationalError):
            connection.execute(
                f"UPDATE {self.test_table} SET dummy='some_other_text' WHERE idx=1"
            )
        # Delete not supported
        with pytest.raises(OperationalError):
            connection.execute(f"DELETE FROM {self.test_table} WHERE idx=1")

    def test_get_schema_names(self, engine: Engine, database_name: str):
        results = engine.dialect.get_schema_names(engine)
        assert database_name in results

    def test_has_table(self, engine: Engine, database_name: str):
        results = engine.dialect.has_table(engine, self.test_table, database_name)
        assert results == 1

    def test_get_table_names(self, engine: Engine, database_name: str):
        results = engine.dialect.get_table_names(engine, database_name)
        assert len(results) > 0

    def test_get_columns(self, engine: Engine, database_name: str):
        results = engine.dialect.get_columns(engine, self.test_table, database_name)
        assert len(results) > 0
        row = results[0]
        assert isinstance(row, dict)
        row_keys = list(row.keys())
        assert row_keys[0] == "name"
        assert row_keys[1] == "type"
        assert row_keys[2] == "nullable"
        assert row_keys[3] == "default"


class TestAsyncFireboltDialect:

    test_table = "test_alchemy_async"

    def create_test_table(self, connection: Connection, engine: Engine, table: str):
        connection.execute(
            f"""
            CREATE FACT TABLE IF NOT EXISTS {table}
            (
                idx INT,
                dummy TEXT
            ) PRIMARY INDEX idx;
            """
        )
        assert engine.dialect.has_table(engine, table)

    def drop_test_table(self, connection: Connection, engine: Engine, table: str):
        connection.execute(f"DROP TABLE IF EXISTS {table}")
        assert not engine.dialect.has_table(engine, table)

    @pytest.fixture(scope="class", autouse=True)
    async def setup_test_tables(self, connection: Connection, engine: Engine):
        self.create_test_table(connection, engine, self.test_table)
        yield
        self.drop_test_table(connection, engine, self.test_table)

    @pytest.mark.asyncio
    async def test_create_ex_table(
        self, async_connection: Connection, async_engine: Engine
    ):
        await async_connection.execute(
            text(
                """
            CREATE EXTERNAL TABLE ex_lineitem_alchemy_async
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
        )

        def has_test_table(conn):
            inspector = inspect(conn)
            return inspector.has_table("ex_lineitem_alchemy_async")

        assert await async_connection.run_sync(has_test_table)
        # Cleanup
        await async_connection.execute(text("DROP TABLE ex_lineitem_alchemy_async;"))
        assert not await async_connection.run_sync(has_test_table)

    @pytest.mark.asyncio
    async def test_data_write(self, async_connection: Connection):
        await async_connection.execute(
            text("INSERT INTO test_alchemy_async(idx, dummy) VALUES (1, 'some_text')")
        )
        result = await async_connection.execute(
            text("SELECT * FROM test_alchemy_async")
        )
        assert len(result.fetchall()) == 1
        # Update not supported
        with pytest.raises(OperationalError):
            await async_connection.execute(
                text(
                    "UPDATE test_alchemy_async SET dummy='some_other_text' WHERE idx=1"
                )
            )
        # Delete not supported
        with pytest.raises(OperationalError):
            await async_connection.execute(
                text("DELETE FROM test_alchemy_async WHERE idx=1")
            )

    @pytest.mark.asyncio
    async def test_get_table_names(
        self, async_connection: Connection, database_name: str
    ):
        def get_table_names(conn):
            inspector = inspect(conn)
            return inspector.get_table_names(database_name)

        results = await async_connection.run_sync(get_table_names)
        assert len(results) > 0

    @pytest.mark.asyncio
    async def test_get_columns(self, async_connection: Connection, database_name: str):
        def get_columns(conn):
            inspector = inspect(conn)
            return inspector.get_columns(self.test_table, database_name)

        results = await async_connection.run_sync(get_columns)
        assert len(results) > 0
        row = results[0]
        assert isinstance(row, dict)
        row_keys = list(row.keys())
        assert row_keys[0] == "name"
        assert row_keys[1] == "type"
        assert row_keys[2] == "nullable"
        assert row_keys[3] == "default"
