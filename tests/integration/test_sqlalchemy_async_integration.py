from typing import Dict, List

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine.base import Connection, Engine


@pytest.mark.usefixtures("setup_test_tables")
class TestAsyncFireboltDialect:
    async def test_create_ex_table(
        self,
        async_connection: Connection,
        ex_table_query: str,
        ex_table_name: str,
    ):
        await async_connection.execute(text(ex_table_query))

        def has_test_table(conn: Connection) -> bool:
            inspector = inspect(conn)
            return inspector.has_table(ex_table_name)

        assert await async_connection.run_sync(has_test_table)
        # Cleanup
        await async_connection.execute(text(f"DROP TABLE {ex_table_name}"))
        assert not await async_connection.run_sync(has_test_table)

    async def test_data_write(self, async_connection: Connection, fact_table_name: str):
        result = await async_connection.execute(
            text(f"INSERT INTO {fact_table_name}(idx, dummy) VALUES (1, 'some_text')")
        )
        result = await async_connection.execute(
            text(f"SELECT * FROM {fact_table_name}")
        )
        assert result.rowcount == 1
        assert len(result.fetchall()) == 1

    async def test_set_params(self, async_connection: Engine):
        await async_connection.execute(text("SET advanced_mode=1"))
        result = await async_connection.execute(text("SELECT 1"))
        assert len(result.fetchall()) == 1
        await async_connection.execute(text("SET advanced_mode=0"))

    async def test_get_table_names(self, async_connection: Connection):
        def get_table_names(conn: Connection) -> bool:
            inspector = inspect(conn)
            return inspector.get_table_names()

        results = await async_connection.run_sync(get_table_names)
        assert len(results) > 0

    async def test_get_columns(
        self, async_connection: Connection, fact_table_name: str
    ):
        def get_columns(conn: Connection) -> List[Dict]:
            inspector = inspect(conn)
            return inspector.get_columns(fact_table_name)

        results = await async_connection.run_sync(get_columns)
        assert len(results) > 0
        row = results[0]
        assert isinstance(row, dict)
        row_keys = list(row.keys())
        assert row_keys[0] == "name"
        assert row_keys[1] == "type"
        assert row_keys[2] == "nullable"
        assert row_keys[3] == "default"
