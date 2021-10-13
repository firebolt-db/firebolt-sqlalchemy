from datetime import date

from firebolt_db.firebolt_api_service import FireboltApiService
from firebolt_db import exceptions
import pytest
import os

test_username = os.environ["username"]
test_password = os.environ["password"]
test_engine_url = os.environ["engine_url"]
test_db_name = os.environ["db_name"]
query = 'select * from ci_fact_table limit 1'


access_token = FireboltApiService.get_access_token(test_username, test_password, date.today())

class TestFireboltApiService:

    def test_get_access_token_success(self):
        assert access_token[0] != ""

    def test_get_access_token_invalid_credentials(self):
        with pytest.raises(Exception) as e_info:
            response = FireboltApiService.get_access_token('username', 'password', date.today())

    def test_get_access_token_via_refresh_success(self):
        assert FireboltApiService.get_access_token_via_refresh(access_token[1]) != ""

    def test_get_access_token_via_refresh_invalid_token(self):
        with pytest.raises(Exception) as e_info:
            response = FireboltApiService.get_access_token_via_refresh('refresh_token')

    def test_run_query_success(self):
        try:
            response = FireboltApiService.run_query(access_token[0], test_engine_url, test_db_name, query)
            assert response != ""
        except exceptions.InternalError as http_err:
            assert http_err != ""

    def test_run_query_invalid_url(self):
        with pytest.raises(Exception) as e_info:
            response = FireboltApiService.run_query(access_token[0], "", test_db_name, query) != {}

    def test_run_query_invalid_schema(self):
        with pytest.raises(Exception) as e_info:
            response = FireboltApiService.run_query(access_token[0], test_engine_url, 'db_name', query)

    def test_run_query_invalid_header(self):
        try:
            response = FireboltApiService.run_query('header', test_engine_url, test_db_name, query)
            assert response != ""
        except exceptions.InternalError as e_info:
            assert e_info != ""

    def test_run_query_invalid_query(self):
        with pytest.raises(Exception) as e_info:
            response = FireboltApiService.run_query(access_token[0], test_engine_url, test_db_name, 'query')
