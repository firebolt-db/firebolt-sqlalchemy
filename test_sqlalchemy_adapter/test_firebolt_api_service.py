from sqlalchemy_adapter.firebolt_api_service import FireboltApiService
from sqlalchemy_adapter.test_sqlalchemy_adapter import constants
from requests.exceptions import HTTPError

access_token = FireboltApiService.get_access_token({'username': constants.username,
                                                    'password': constants.password})
header = {'Authorization': "Bearer " + access_token["access_token"]}
engine_url = FireboltApiService.get_engine_url_by_db(constants.db_name, header)
query_url = "https://" + engine_url
query_file = {"query": (None, constants.query)}


class TestFireboltApiService:

    def test_get_connection_success(self):
        response = FireboltApiService.get_connection(constants.username, constants.password,  constants.db_name)
        if type(response) == HTTPError:
            assert response.response.status_code == 503
        else:
            assert response != ""

    def test_get_connection_invalid_credentials(self):
        assert FireboltApiService.get_connection('username', 'password',  constants.db_name)[0] \
                   .response.status_code == 403

    def test_get_connection_invalid_schema_name(self):
        assert FireboltApiService.get_connection(constants.username, constants.password, 'db_name')[1] \
                   .response.status_code == 404

    def test_get_access_token_success(self):
        assert access_token["access_token"] != ""

    def test_get_access_token_invalid_credentials(self):
        assert FireboltApiService.get_access_token({'username': 'username', 'password': 'password'}) \
                   .response.status_code == 403

    def test_get_access_token_via_refresh_success(self):
        assert FireboltApiService.get_access_token({'refresh_token': access_token["refresh_token"]}) != ""

    def test_get_access_token_via_refresh_invalid_token(self):
        assert FireboltApiService.get_access_token_via_refresh({'refresh_token': 'refresh_token'}) \
                   .response.status_code == 403

    def test_get_engine_url_by_db_success(self):
        assert engine_url != ""

    def test_get_engine_url_by_db_invalid_schema(self):
        assert FireboltApiService.get_engine_url_by_db('db_name', header) \
                   .response.status_code == 404

    def test_get_engine_url_by_db_invalid_header(self):
        assert FireboltApiService.get_engine_url_by_db(constants.db_name, 'header') != ""

    def test_run_query_success(self):
        response = FireboltApiService.run_query(access_token["access_token"], engine_url, constants.db_name, query_file)
        if type(response) == HTTPError:
            assert response.response.status_code == 503
        else:
            assert response != ""

    def test_run_query_invalid_url(self):
        assert FireboltApiService.run_query(access_token["access_token"], "",
                                            constants.db_name, query_file) != {}

    def test_run_query_invalid_schema(self):
        response = FireboltApiService.run_query(access_token["access_token"], engine_url, 'db_name', query_file)
        print(response)
        code = response.response.status_code
        if code == 503:
            assert code == 503
            return
        assert code == 403

    def test_run_query_invalid_header(self):
        assert FireboltApiService.run_query('header', engine_url, constants.db_name, query_file) != {}

    def test_run_query_invalid_query(self):
        response = FireboltApiService.run_query(access_token["access_token"], engine_url, constants.db_name,
                                                {"query": (None, 'query')})
        print(response)
        code = response.response.status_code
        if code == 503:
            assert code == 503
            return
        assert code == 500
