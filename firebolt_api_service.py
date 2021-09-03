import json

import requests
from requests.exceptions import HTTPError

from sqlalchemy_adapter import constants


class FireboltApiService:

    @staticmethod
    def get_connection(user_email, password, db_name):
        # get access token
        token_json = FireboltApiService.get_access_token({'username': user_email, 'password': password})
        engine_url = ""
        refresh_token = ""
        if type(token_json) == dict:  # case when http error is not raised
            access_token = token_json["access_token"]
            refresh_token = token_json["refresh_token"]

            # get engine url
            engine_url = FireboltApiService.get_engine_url_by_db(db_name, access_token)
            # if type(engine_url) != str:  # case when http error is raised
            #     if type(engine_url) == HTTPError and \
            #             engine_url.response.status_code == 401:  # check for access token expiry
            #         access_token = FireboltApiService.get_access_token_via_refresh({'refresh_token': refresh_token})
            #         if type(access_token) == str:
            #             header = {'Authorization': "Bearer " + access_token}
            #             engine_url = FireboltApiService.get_engine_url_by_db(db_name, header)

            token_json = access_token

        # get db response using firebolt api
        # db_response = FireboltApiService.run_query("https://" + engine_url, db_name,
        #                                            header, {"query": (None, query)})
        # return db_response
        return token_json, engine_url, refresh_token

    # retrieve authentication token
    """
    This method uses the user email and the password to fire the API to generate access-token.
    :input dictionary containing user-email and password
    :returns access-token
    """

    @staticmethod
    def get_access_token(data):
        json_data = {}  # base case
        try:

            """
               General format of request:
              curl --request POST 'https://api.app.firebolt.io/auth/v1/login' --header 'Content-Type: application/json;charset=UTF-8' --data-binary '{"username":"raghavs@sigmoidanalytics.com","password":"Sharma%1"}'
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
            return http_err
        except Exception as err:
            return err

        return json_data

    # refresh access token
    """
    In case the token expires or the API throws a 401 HTTP error, then this method generates a fresh token
    :input refresh api url, request type, authentication header and 
    the refresh token generated alongside the previous expired token
    :returns new access-token
    """

    @staticmethod
    def get_access_token_via_refresh(data):
        refresh_access_token = ""
        try:
            """
                Request:
                curl --request POST 'https://api.app.firebolt.io/auth/v1/refresh' \  
                --header 'Content-Type: application/json;charset=UTF-8' \  
                --data-binary '{"refresh_token":"YOUR_REFRESH_TOKEN_VALUE"}'
                """
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
            return http_err
        except Exception as err:
            return err

        return refresh_access_token

    # get engine url by db name
    """
    This method generates engine url using db name and access-token
    :input api url, request type, authentication header and access-token
    :returns engine url
    """

    @staticmethod
    def get_engine_url_by_db(db_name, access_token):
        engine_url = ""  # base case
        try:
            """
            Request:
            curl --request GET 'https://api.app.firebolt.io/core/v1/account/engines:getURLByDatabaseName?database_name=YOUR_DATABASE_NAME' \  
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
            return http_err
        except Exception as err:
            return err

        return engine_url

    # run queries
    """
    This method is used to submit a query to run to a running engine. 
    You can specify multiple queries separated by a semicolon (;)..
    :input token url, request type of API and authentication header
    :returns access-token
    """

    @staticmethod
    def run_query(access_token, engine_url, db_name, query):
        json_data = {}  # base case
        try:

            """
            Request:
            echo "SELECT * FROM lineitem LIMIT 1000" | curl
            --request POST 'https://YOUR_ENGINE_ENDPOINT/?database=YOUR_DATABASE_NAME' \
            --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE' \
            --data-binary @-
            """
            if type(access_token) == str:
                header = {'Authorization': "Bearer " + access_token}
                if type(engine_url) == str:
                    query_response = requests.post(url="https://" + engine_url, params={'database': db_name},
                                                   headers=header, files={"query": (None, query)})
                    query_response.raise_for_status()
                    json_data = json.loads(query_response.text)
                else:
                    json_data = {"message": "Engine url is invalid", "attribute": engine_url}
            else:
                json_data = {"message": "Access token is invalid", "attribute": access_token}

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
            return http_err
        except Exception as err:
            print(f'Other error occurred: {err}')
            return err

        return json_data
