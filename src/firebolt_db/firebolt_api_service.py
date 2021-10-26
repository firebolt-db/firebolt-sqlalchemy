import json

import requests
from requests.exceptions import HTTPError

from . import exceptions
from . import constants
from .memoized import memoized


class FireboltApiService:

    @staticmethod
    @memoized
    def get_connection(user_email, password, engine_name, db_name, date):
        """
        Retrieve Authorisation details for connection
        This method internally calls methods to get access token, refresh token and engine URL.
        :input user-email, password, engine name, database name and date for memoization
        :returns access-token, refresh-token and engine url
        """
        # get access token
        token_json = FireboltApiService.get_access_token(user_email, password)
        access_token = token_json["access_token"]
        refresh_token = token_json["refresh_token"]

        # get engine url
        if engine_name is None or engine_name == '':
            engine_url = FireboltApiService.get_engine_url_by_db(db_name, access_token)
        else:
            engine_url = FireboltApiService.get_engine_url_by_engine(engine_name, access_token)
        return access_token, refresh_token, engine_url

    @staticmethod
    def get_access_token(user_email, password):
        """
        Retrieve authentication token
        This method uses the user email and the password to fire the API to generate access-token.
        :input user-email and password
        :returns access-token
        """
        data = {'username': user_email, 'password': password}
        json_data = {}  # base case
        payload = {}
        try:

            """
               General format of request:
              curl --request POST 'https://api.app.firebolt.io/auth/v1/login' --header 'Content-Type: application/json;charset=UTF-8' --data-binary '{"username":"username","password":"password"}'
            """
            token_response = requests.post(url=constants.token_url, data=json.dumps(data),
                                           headers=constants.token_header)
            token_response.raise_for_status()

            """
            General format of response:

            {  
              "access_token": "YOUR_ACCESS_TOKEN_VALUE",  
              "expires_in": 86400,  
              "refresh_token": "YOUR_REFRESH_TOKEN_VALUE",  
              "scope": "offline_access",  
              "token_type": "Bearer"  
            }
            """

            json_data = json.loads(token_response.text)

        except HTTPError as http_err:
            payload = {
                "error": "Access Token API Exception",
                "errorMessage": http_err.response.text,
            }
        except Exception as err:
            payload = {
                "error": "Access Token API Exception",
                "errorMessage": str(err),
            }

        if payload != {}:
            msg = "{error} : {errorMessage}".format(**payload)
            raise exceptions.InvalidCredentialsError(msg)

        return json_data

    @staticmethod
    def get_access_token_via_refresh(refresh_token):
        """
        Refresh access token
        In case the token expires or the API throws a 401 HTTP error, then this method generates a fresh token
        :input refresh token generated alongside the previous expired token
        :returns new access-token
        """
        refresh_access_token = ""
        payload = {}
        try:
            """
                Request:
                curl --request POST 'https://api.app.firebolt.io/auth/v1/refresh'
                --header 'Content-Type: application/json;charset=UTF-8'
                --data-binary '{"refresh_token":"YOUR_REFRESH_TOKEN_VALUE"}'
                """
            data = {'refresh_token': refresh_token}
            token_response = requests.post(url=constants.refresh_url, data=json.dumps(data),
                                           headers=constants.token_header)
            token_response.raise_for_status()

            """
            Response:
            {
                "access_token": "YOUR_REFRESHED_ACCESS_TOKEN_VALUE",
                "scope": "offline_access",
                "expires_in": 86400,
                "token_type": "Bearer"
            }
            """

            json_data = json.loads(token_response.text)
            refresh_access_token = json_data["access_token"]

        except HTTPError as http_err:
            payload = {
                "error": "Refresh Access Token API Exception",
                "errorMessage": http_err.response.text,
            }
        except Exception as err:
            payload = {
                "error": "Refresh Access Token API Exception",
                "errorMessage": str(err),
            }
        if payload != {}:
            msg = "{error} : {errorMessage}".format(**payload)
            raise exceptions.InternalError(msg)

        return refresh_access_token

    @staticmethod
    def get_engine_url_by_db(db_name, access_token):
        """
        Get engine url by db_name name
        This method generates engine url using engine name and access-token
        :input database name and access token
        :returns engine url
        """
        engine_url = ""  # base case
        payload = {}
        try:
            """
            Request:
            curl --request GET 'https://api.app.firebolt.io/core/v1/account/engines:getURLByDatabaseName?database_name=YOUR_DATABASE_NAME'
            --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE'
            """
            header = {'Authorization': "Bearer " + access_token}
            query_engine_response = requests.get(constants.query_engine_url, params={'database_name': db_name},
                                                 headers=header)
            query_engine_response.raise_for_status()

            """
            Response:
            {"engine_url": "YOUR_DATABASES_DEFAULT_ENGINE_URL"}
            """
            json_data = json.loads(query_engine_response.text)
            engine_url = json_data["engine_url"]

        except HTTPError as http_err:
            payload = {
                "error": "Engine Url API Exception",
                "errorMessage": http_err.response.text,
            }
        except Exception as err:
            payload = {
                "error": "Engine Url API Exception",
                "errorMessage": str(err),
            }
        if payload != {}:
            msg = "{error} : {errorMessage}".format(**payload)
            raise exceptions.SchemaNotFoundError(msg)

        return engine_url

    @staticmethod
    def get_engine_url_by_engine(engine_name, access_token):
        """
        Get engine url by engine name
        This method generates engine url using engine name and access-token
        :input engine name and access-token
        :returns engine url
        """
        engine_url = ""  # base case
        payload = {}
        try:
            """
            Request:
            curl --request GET 'https://api.app.firebolt.io/core/v1/account/engines?filter.name_contains=YOUR_ENGINE_NAME'
            --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE'
            """
            header = {'Authorization': "Bearer " + access_token}
            query_engine_response = requests.get(constants.query_engine_url_by_engine_name,
                                                 params={'filter.name_contains': engine_name},
                                                 headers=header)
            query_engine_response.raise_for_status()

            """
            Response:
            {
              "page": {
                ...
              },
              "edges": [
                {
                  ...
                    "endpoint": "YOUR_ENGINE_URL",
                  ...
                  }
                }
              ]
            }
            """
            json_data = json.loads(query_engine_response.text)
            engine_url = json_data["edges"][0]["node"]["endpoint"]

        except HTTPError as http_err:
            payload = {
                "error": "Engine Url API Exception",
                "errorMessage": http_err.response.text,
            }
        except Exception as err:
            payload = {
                "error": "Engine Url API Exception",
                "errorMessage": str(err),
            }
        if payload != {}:
            msg = "{error} : {errorMessage}".format(**payload)
            raise exceptions.EngineNotFoundError(msg)

        return engine_url

    @staticmethod
    def run_query(access_token, engine_url, db_name, query):
        """
        Run queries
        This method is used to submit a query to run to a running engine.
        You can specify multiple queries separated by a semicolon (;)..
        :input access token, engine url, database name, query
        :returns database metadata
        """
        query_response = {}     # base-case
        payload = {}

        try:

            """
            Request:
            --request POST 'https://YOUR_ENGINE_ENDPOINT/?database=YOUR_DATABASE_NAME' \
            --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE' \
            --data-binary @-
            """

            header = {'Authorization': "Bearer " + access_token}
            query_response = requests.post(url="https://" + engine_url, params={'database': db_name},
                                           headers=header, files={"query": (None, query)})
            query_response.raise_for_status()

        except HTTPError as http_err:
            payload = {
                    "error": "DB-API Exception",
                    "errorMessage": http_err.response.text,
                }
        except Exception as err:
            payload = {
                "error": "DB-API Exception",
                "errorMessage": str(err),
            }
        if payload != {}:
            msg = "{error} : {errorMessage}".format(**payload)
            raise exceptions.InternalError(msg)

        return query_response
