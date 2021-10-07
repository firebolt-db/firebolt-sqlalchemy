import pytest

from firebolt_db import firebolt_dialect
from sqlalchemy.engine import url
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry


@pytest.fixture
def get_engine():
    registry.register("firebolt", "src.firebolt_db.firebolt_dialect", "FireboltDialect")
    return create_engine("firebolt://aapurva@sigmoidanalytics.com:Apurva111@host/Sigmoid_Alchemy")


dialect = firebolt_dialect.FireboltDialect()


class TestFireboltDialect:

    def test_create_connect_args(self):
        connection_url = "test_engine://test_user@email:test_password@test_host_name/test_db_name"
        u = url.make_url(connection_url)
        result_list, result_dict = dialect.create_connect_args(u)
        assert result_dict["host"] == "test_host_name"
        assert result_dict["port"] == 5432
        assert result_dict["username"] == "test_user@email"
        assert result_dict["password"] == "test_password"
        assert result_dict["db_name"] == "test_db_name"
        assert result_dict["context"] == {}
        assert not result_dict["header"]

    def test_get_schema_names(self, get_engine):
        engine = get_engine
        results = dialect.get_schema_names(engine)
        assert 'Sigmoid_Alchemy' in results

    def test_has_table(self, get_engine):
        table = 'ci_fact_table'
        schema = 'Sigmoid_Alchemy'
        engine = get_engine
        results = dialect.has_table(engine, table, schema)
        assert results == 1

    def test_get_table_names(self, get_engine):
        schema = 'Sigmoid_Alchemy'
        engine = get_engine
        results = dialect.get_table_names(engine, schema)
        assert len(results) > 0

    def test_get_columns(self, get_engine):
        table = 'ci_fact_table'
        schema = 'Sigmoid_Alchemy'
        engine = get_engine
        results = dialect.get_columns(engine, table, schema)
        assert len(results) > 0
        row = results[0]
        assert isinstance(row, dict)
        row_keys = list(row.keys())
        assert row_keys[0] == "name"
        assert row_keys[1] == "type"
        assert row_keys[2] == "nullable"
        assert row_keys[3] == "default"


def test_get_is_nullable():
    assert firebolt_dialect.get_is_nullable("YES")
    assert firebolt_dialect.get_is_nullable("yes")
    assert not firebolt_dialect.get_is_nullable("NO")
    assert not firebolt_dialect.get_is_nullable("no")
    assert not firebolt_dialect.get_is_nullable("ABC")
