from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import OperationalError


class TestFireboltDialect:
    def test_create_ex_table(
        self,
        connection: Connection,
        engine: Engine,
        ex_table_query: str,
        ex_table_name: str,
    ):
        connection.execute(ex_table_query)
        assert engine.dialect.has_table(engine, ex_table_name)
        # Cleanup
        connection.execute(f"DROP TABLE {ex_table_name}")
        assert not engine.dialect.has_table(engine, ex_table_name)

    def test_set_params(
        self, username: str, password: str, database_name: str, engine_name: str
    ):
        engine = create_engine(
            f"firebolt://{username}:{password}@{database_name}/{engine_name}"
        )
        with engine.connect() as connection:
            connection.execute("SET advanced_mode=1")
            connection.execute("SET use_standard_sql=0")
            result = connection.execute("SELECT sleepEachRow(1) from numbers(1)")
            assert len(result.fetchall()) == 1
        engine.dispose()

    def test_data_write(self, connection: Connection, fact_table_name: str):
        connection.execute(
            f"INSERT INTO {fact_table_name}(idx, dummy) VALUES (1, 'some_text')"
        )
        result = connection.execute(f"SELECT * FROM {fact_table_name} WHERE idx=?", 1)
        assert result.fetchall() == [(1, "some_text")]
        result = connection.execute(f"SELECT * FROM {fact_table_name}")
        assert len(result.fetchall()) == 1
        # Update not supported
        with pytest.raises(OperationalError):
            connection.execute(
                f"UPDATE {fact_table_name} SET dummy='some_other_text' WHERE idx=1"
            )
        # Delete works but is not officially supported yet
        # with pytest.raises(OperationalError):
        #     connection.execute(f"DELETE FROM {fact_table_name} WHERE idx=1")

    def test_firebolt_types(self, connection: Connection):
        result = connection.execute("SELECT '1896-01-01' :: DATE_EXT")
        assert result.fetchall() == [(date(1896, 1, 1),)]
        result = connection.execute("SELECT '1896-01-01 00:01:00' :: TIMESTAMP_EXT")
        assert result.fetchall() == [(datetime(1896, 1, 1, 0, 1, 0, 0),)]
        result = connection.execute("SELECT 100.76 :: DECIMAL(5, 2)")
        assert result.fetchall() == [(Decimal("100.76"),)]

    def test_agg_index(self, connection: Connection, fact_table_name: str):
        # Test if sql parsing allows it
        agg_index = "idx_agg_max"
        connection.execute(
            f"""
            CREATE AGGREGATING INDEX {agg_index} ON {fact_table_name} (
                dummy,
                max(idx)
            );
            """
        )
        connection.execute(f"DROP AGGREGATING INDEX {agg_index}")

    def test_join_index(self, connection: Connection, dimension_table_name: str):
        # Test if sql parsing allows it
        join_index = "idx_join"
        connection.execute(
            f"""
            CREATE JOIN INDEX {join_index} ON {dimension_table_name} (
                idx,
                dummy
            );
            """
        )
        connection.execute(f"DROP JOIN INDEX {join_index}")

    def test_get_schema_names(self, engine: Engine, database_name: str):
        results = engine.dialect.get_schema_names(engine)
        assert "public" in results

    def test_has_table(self, engine: Engine, fact_table_name: str):
        results = engine.dialect.has_table(engine, fact_table_name)
        assert results == 1

    def test_get_table_names(self, engine: Engine):
        results = engine.dialect.get_table_names(engine)
        assert len(results) > 0
        results = engine.dialect.get_table_names(engine, "public")
        assert len(results) > 0
        results = engine.dialect.get_table_names(engine, "non_existing_schema")
        assert len(results) == 0

    def test_get_columns(self, engine: Engine, fact_table_name: str):
        results = engine.dialect.get_columns(engine, fact_table_name)
        assert len(results) > 0
        row = results[0]
        assert isinstance(row, dict)
        row_keys = list(row.keys())
        assert row_keys[0] == "name"
        assert row_keys[1] == "type"
        assert row_keys[2] == "nullable"
        assert row_keys[3] == "default"
