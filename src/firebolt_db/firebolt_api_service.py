import json

import requests
from requests.exceptions import HTTPError

from . import exceptions
from . import constants
from .memoized import memoized


class FireboltApiService:

    @staticmethod
    @memoized
    def get_access_token(user_email, password, date):
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
            access_token = json_data["access_token"]
            refresh_token = json_data["refresh_token"]

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

        return access_token, refresh_token

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
