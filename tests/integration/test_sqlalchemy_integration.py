from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.types import ARRAY, INTEGER, TypeEngine


class TestFireboltDialect:
    def test_create_ex_table(
        self,
        connection: Connection,
        engine: Engine,
        ex_table_query: str,
        ex_table_name: str,
    ):
        connection.execute(text(ex_table_query))
        assert engine.dialect.has_table(connection, ex_table_name)
        # Cleanup
        connection.execute(text(f"DROP TABLE {ex_table_name}"))
        assert not engine.dialect.has_table(connection, ex_table_name)

    def test_set_params(
        self,
        client_id: str,
        client_key: str,
        database_name: str,
        engine_name: str,
        account_name: str,
    ):
        engine = create_engine(
            f"firebolt://{client_id}:{client_key}@{database_name}/{engine_name}"
            + f"?account_name={account_name}"
        )
        with engine.connect() as connection:
            connection.execute(text("SET advanced_mode=1"))
            result = connection.execute(text("SELECT 1"))
            assert len(result.fetchall()) == 1
        engine.dispose()

    def test_data_write(self, connection: Connection, fact_table_name: str):
        connection.execute(
            text(f"INSERT INTO {fact_table_name}(idx, dummy) VALUES (1, 'some_text')")
        )
        result = connection.execute(
            text(f"SELECT * FROM {fact_table_name} WHERE idx=1")
        )
        assert result.fetchall() == [(1, "some_text")]
        result = connection.execute(text(f"SELECT * FROM {fact_table_name}"))
        assert len(result.fetchall()) == 1

    def test_firebolt_types(self, connection: Connection):
        result = connection.execute(text("SELECT '1896-01-01' :: PGDATE"))
        assert result.fetchall() == [(date(1896, 1, 1),)]
        result = connection.execute(
            text("SELECT '1896-01-01 00:01:00' :: TIMESTAMPNTZ")
        )
        assert result.fetchall() == [(datetime(1896, 1, 1, 0, 1, 0, 0),)]
        result = connection.execute(text("SELECT 100.76 :: DECIMAL(5, 2)"))
        assert result.fetchall() == [(Decimal("100.76"),)]

    def test_agg_index(self, connection: Connection, fact_table_name: str):
        # Test if sql parsing allows it
        agg_index = "idx_agg_max"
        connection.execute(
            text(
                f"""
            CREATE AGGREGATING INDEX {agg_index} ON {fact_table_name} (
                dummy,
                max(idx)
            );
            """
            )
        )
        connection.execute(text(f"DROP AGGREGATING INDEX {agg_index}"))

    def test_get_schema_names(self, engine: Engine, database_name: str):
        results = engine.dialect.get_schema_names(engine)
        assert "public" in results

    def test_has_table(
        self, engine: Engine, connection: Connection, fact_table_name: str
    ):
        results = engine.dialect.has_table(connection, fact_table_name)
        assert results == 1

    def test_get_table_names(self, engine: Engine, connection: Connection):
        results = engine.dialect.get_table_names(connection)
        assert len(results) > 0
        results = engine.dialect.get_table_names(connection, "public")
        assert len(results) > 0
        results = engine.dialect.get_table_names(connection, "non_existing_schema")
        assert len(results) == 0

    def test_get_columns(
        self, engine: Engine, connection: Connection, type_table_name: str
    ):
        results = engine.dialect.get_columns(connection, type_table_name)
        assert len(results) > 0
        for column in results:
            assert isinstance(column, dict)
            # Check only works for basic types
            if type(column["type"]) == ARRAY:
                # ARRAY[[INT]]
                assert column["type"].dimensions == 2
                assert type(column["type"].item_type) == INTEGER
            else:
                assert issubclass(column["type"], TypeEngine)

    def test_service_account_connect(self, connection_service_account: Connection):
        result = connection_service_account.execute(text("SELECT 1"))
        assert result.fetchall() == [(1,)]
