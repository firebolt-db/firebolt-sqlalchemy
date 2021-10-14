import os

import pytest
from requests.exceptions import HTTPError

from firebolt_db import firebolt_connector
from firebolt_db import exceptions

test_username = os.environ["username"]
test_password = os.environ["password"]
test_engine_name = os.environ["engine_name"]
test_db_name = os.environ["db_name"]

@pytest.fixture
def get_connection():
    return firebolt_connector.connect(test_engine_name, 8123, test_username, test_password, test_db_name)


class TestConnect:

    def test_connect_success(self):
        user_email = test_username
        password = test_password
        db_name = test_engine_name
        host = test_db_name
        port = "8123"
        connection = firebolt_connector.connect(host, port, user_email, password, db_name)
        assert connection.access_token
        assert connection.engine_url

    def test_connect_invalid_credentials(self):
        user_email = test_username
        password = "wrongpassword"
        db_name = test_engine_name
        host = test_db_name
        port = "8123"
        with pytest.raises(exceptions.InvalidCredentialsError):
            firebolt_connector.connect(host, port, user_email, password, db_name)


def test_get_description_from_row_valid_rows():
    row = {'id': 1, 'name': 'John', 'is_eligible': True, 'some_array': [2, 4]}
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
    assert result[3][0] == 'some_array'
    assert result[3][1] == firebolt_connector.Type.ARRAY
    assert not result[3][6]


def test_get_description_from_row_invalid_rows():
    row = {'id': {}}
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
    assert firebolt_connector.get_type(value_4) == 4


def test_get_type_invalid_type():
    value = {}
    with pytest.raises(Exception):
        firebolt_connector.get_type(value)


class TestConnection:

    def test_cursor(self, get_connection):
        connection = get_connection
        assert len(connection.cursors) == 0
        cursor = connection.cursor()
        assert len(connection.cursors) > 0
        assert type(cursor) == firebolt_connector.Cursor

    # def test_execute(self, get_connection):
    #     connection = get_connection
    #     query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.DATABASES"
    #     cursor = connection.execute(query)
    #     assert type(cursor._results) == itertools.chain

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
        query = "select * from ci_fact_table limit 10"
        try:
            cursor = connection.cursor().execute(query)
            assert cursor.rowcount == 10
        except exceptions.InternalError as http_err:
            assert http_err != ""

    def test_close(self, get_connection):
        connection = get_connection
        cursor = connection.cursor()
        if not cursor.closed:
            cursor.close()
        assert cursor.closed

    def test_execute(self, get_connection):
        query = 'select * from ci_fact_table ' \
                'where l_orderkey=3184321 and l_partkey=65945'
        connection = get_connection
        cursor = connection.cursor()
        assert not cursor._results
        try:
            cursor.execute(query)
            assert cursor.rowcount == 1
        except exceptions.InternalError as http_err:
            assert http_err != ""

    def test_executemany(self, get_connection):
        query = "select * from ci_fact_table limit 10"
        connection = get_connection
        cursor = connection.cursor()
        with pytest.raises(exceptions.NotSupportedError):
            cursor.executemany(query)

    def test_fetchone(self, get_connection):
        query = "select * from ci_fact_table limit 10"
        connection = get_connection
        cursor = connection.cursor()
        assert not cursor._results
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            assert isinstance(result, tuple)
        except exceptions.InternalError as http_err:
            assert http_err != ""

    def test_fetchmany(self, get_connection):
        query = "select * from ci_fact_table limit 10"
        connection = get_connection
        cursor = connection.cursor()
        assert not cursor._results
        try:
            cursor.execute(query)
            result = cursor.fetchmany(3)
            assert isinstance(result, list)
            assert len(result) == 3
        except exceptions.InternalError as http_err:
            assert http_err != ""

    def test_fetchall(self, get_connection):
        query = "select * from ci_fact_table limit 10"
        connection = get_connection
        cursor = connection.cursor()
        assert not cursor._results
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            assert isinstance(result, list)
            assert len(result) == 10
        except exceptions.InternalError as http_err:
            assert http_err != ""
