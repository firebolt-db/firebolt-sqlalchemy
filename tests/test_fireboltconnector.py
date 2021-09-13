import itertools

import pytest

from src.firebolt_db import firebolt_connector
from src.firebolt_db import exceptions


@pytest.fixture
def get_connection():
    return firebolt_connector.connect('aapurva@sigmoidanalytics.com', 'Apurva111', 'Sigmoid_Alchemy')


class TestConnect:

    def test_connect_success(self):
        user_email = "aapurva@sigmoidanalytics.com"
        password = "Apurva111"
        db_name = "Sigmoid_Alchemy"
        connection = firebolt_connector.connect(user_email, password, db_name)
        assert connection.access_token
        assert connection.engine_url

    def test_connect_invalid_credentials(self):
        user_email = "aapurva@sigmoidanalytics.com"
        password = "wrongpassword"
        db_name = "Sigmoid_Alchemy"
        with pytest.raises(exceptions.InvalidCredentialsError):
            firebolt_connector.connect(user_email, password, db_name)

    def test_connect_invalid_database(self):
        user_email = "aapurva@sigmoidanalytics.com"
        password = "Apurva111"
        db_name = "wrongdatabase"
        with pytest.raises(exceptions.SchemaNotFoundError):
            firebolt_connector.connect(user_email, password, db_name)


def test_get_description_from_row_valid_rows():
    row = {'id': 1, 'name': 'John', 'is_eligible': True}
    result = firebolt_connector.get_description_from_row(row)
    assert result[0][0] == 'id'
    assert result[0][1] == firebolt_connector.Type.NUMBER
    assert not result[0][6]
    assert result[1][0] == 'name'
    assert result[1][1] == firebolt_connector.Type.STRING
    assert result[1][6]
    assert result[2][0] == 'is_eligible'
    assert result[2][1] == firebolt_connector.Type.BOOLEAN
    assert not result[2][6]


def test_get_description_from_row_invalid_rows():
    row = {'id': []}
    with pytest.raises(Exception):
        firebolt_connector.get_description_from_row(row)


def test_get_type():
    value_1 = "String Value"
    value_2_1 = 5
    value_2_2 = 5.1
    value_3_1 = True
    value_3_2 = False
    value_4 = []
    assert firebolt_connector.get_type(value_1) == 1
    assert firebolt_connector.get_type(value_2_1) == 2
    assert firebolt_connector.get_type(value_2_2) == 2
    assert firebolt_connector.get_type(value_3_1) == 3
    assert firebolt_connector.get_type(value_3_2) == 3
    with pytest.raises(Exception):
        firebolt_connector.get_type(value_4)


class TestConnection:

    def test_cursor(self, get_connection):
        connection = get_connection
        assert len(connection.cursors) == 0
        cursor = connection.cursor()
        assert len(connection.cursors) > 0
        assert type(cursor) == firebolt_connector.Cursor

    def test_execute(self, get_connection):
        connection = get_connection
        query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.DATABASES"
        cursor = connection.execute(query)
        assert type(cursor._results) == itertools.chain

    def test_commit(self):
        pass

    def test_close(self, get_connection):
        connection = get_connection
        connection.cursor()
        connection.close()
        for cursor in connection.cursors:
            assert cursor.closed


class TestCursor:

    def test_rowcount(self, get_connection):
        connection = get_connection
        query = "select * from lineitem limit 10"
        cursor = connection.cursor().execute(query)
        assert cursor.rowcount == 10


    def test_close(self):
        pass

    def test_execute(self):
        pass

    def test_stream_query(self):
        pass

    def test_fetchone(self):
        pass

    def test_fetchmany(self):
        pass

    def test_fetchall(self):
        pass


def test_rows_from_chunks():
    pass
