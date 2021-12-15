import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import OperationalError


class TestAsyncFireboltDialect:
    @pytest.mark.asyncio
    async def test_create_ex_table(
        self,
        async_connection: Connection,
        async_engine: Engine,
        ex_table_query: str,
        ex_table_name: str,
    ):
        await async_connection.execute(text(ex_table_query))

        def has_test_table(conn):
            inspector = inspect(conn)
            return inspector.has_table(ex_table_name)

        assert await async_connection.run_sync(has_test_table)
        # Cleanup
        await async_connection.execute(text(f"DROP TABLE {ex_table_name}"))
        assert not await async_connection.run_sync(has_test_table)

    @pytest.mark.asyncio
    async def test_data_write(self, async_connection: Connection, fact_table_name: str):
        await async_connection.execute(
            text(f"INSERT INTO {fact_table_name}(idx, dummy) VALUES (1, 'some_text')")
        )
        result = await async_connection.execute(
            text(f"SELECT * FROM {fact_table_name}")
        )
        assert len(result.fetchall()) == 1
        # Update not supported
        with pytest.raises(OperationalError):
            await async_connection.execute(
                text(
                    f"UPDATE {fact_table_name} SET dummy='some_other_text' WHERE idx=1"
                )
            )
        # Delete not supported
        with pytest.raises(OperationalError):
            await async_connection.execute(
                text(f"DELETE FROM {fact_table_name} WHERE idx=1")
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
    async def test_get_columns(
        self, async_connection: Connection, database_name: str, fact_table_name: str
    ):
        def get_columns(conn):
            inspector = inspect(conn)
            return inspector.get_columns(fact_table_name, database_name)

        results = await async_connection.run_sync(get_columns)
        assert len(results) > 0
        row = results[0]
        assert isinstance(row, dict)
        row_keys = list(row.keys())
        assert row_keys[0] == "name"
        assert row_keys[1] == "type"
        assert row_keys[2] == "nullable"
        assert row_keys[3] == "default"
