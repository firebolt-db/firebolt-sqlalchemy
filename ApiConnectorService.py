import requests
from requests.exceptions import HTTPError

"""
This class fires the firebolt apis to generate access-token and further engine url. The two attributes are then
shared with the dialect class to retrieve the metadata from Firebolt DB.
"""


class ApiConnectorService:

    # retrieve authentication token
    """
    This method uses the user email and the password to fire the API to generate access-token.
    :input token url, request type of API and authentication header
    :returns access-token
    """

    def get_access_token(self, token_url, request_type, header, user_email, password):
        json_response = ""  # base case
        try:

            """
               General format of request:

               curl --request POST 'https://api.app.firebolt.io/auth/v1/login' \
               --header 'Content-Type: application/json;charset=UTF-8' \
               --data-binary
               '{"username":"YOUR_USER_EMAIL","password":"YOUR_PASSWORD"}'
               """

            token_response = requests.get(
                "curl --request {0} \'{1}\' \\\n--header \'{2}\' \\\n--data-binary\n\'{\"username\":"
                "\"{3}\",\"password\":\"{4}\"}\'".format(
                    request_type, token_url, header, user_email, password))
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

            json_response = token_response.json()

            # print("Entire JSON response")
            # print(jsonResponse)

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

        return json_response

    # refresh access token
    """
    In case the token expires or the API throws a 401 HTTP error, then this method generates a fresh token
    :input refresh api url, request type, authentication header and 
    the refresh token generated alongside the previous expired token
    :returns new access-token
    """

    def get_access_token_via_refresh(self, refresh_url, request_type, header, refresh_token):
        refresh_access_token = 0
        try:
            """
                Request:
                curl --request POST 'https://api.app.firebolt.io/auth/v1/refresh' \  
                --header 'Content-Type: application/json;charset=UTF-8' \  
                --data-binary '{"refresh_token":"YOUR_REFRESH_TOKEN_VALUE"}'
                """
            token_response = requests.get(
                "curl --request {0} \'{1}\' \\\n--header \'{2}\' \\\n--data-binary\n\'{\"refresh_token\":"
                "\"{3}\"}\'".format(
                    request_type, refresh_url, header, refresh_token))
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

            jsonResponse = token_response.json()
            refresh_access_token = jsonResponse["access_token"]

            # print("Entire JSON response")
            # print(jsonResponse)

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

        return refresh_access_token

    # get engine url by db name
    """
    This method generates engine url using db name and access-token
    :input api url, request type, authentication header and access-token
    :returns engine url
    """

    def get_engine_url_by_db(self, engine_db_url, request_type, header):
        engine_url = ""     # base case
        try:
            """
                Request:

                curl --request GET 'https://api.app.firebolt.io/core/v1/account/engines:getURLByDatabaseName?database_name=YOUR_DATABASE_NAME' \  
                --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE'
                """
            query_engine_response = requests.get(
                "curl --request {0} \'{1}\' \\\n--header \'{2}\'".format(
                    request_type, engine_db_url, header))
            query_engine_response.raise_for_status()

            """
            Response:
            {"engine_url": "YOUR_DATABASES_DEFAULT_ENGINE_URL"}
            """
            jsonResponse = query_engine_response.json()
            engine_url = jsonResponse["engine_url"]

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

        return engine_url

    # get engine url by engine name
    """
    This method generates engine url using engine name and access-token
    :input api url, request type, authentication header and access-token
    :returns engine url
    """

    def get_engine_url_by_name(self, engine_name_url, request_type, engine_name, header, token):
        engine_url = ""     # base case
        try:
            """ Request:
                curl --request GET 'https://api.app.firebolt.io/core/v1/account/engines?filter.name_contains=YOUR_ENGINE_NAME' \  
                --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE'
                """
            query_engine_response = requests.get(
                "curl --request {0} \'{1}{2}\' \\\n--header \'{3}{4}\'".format(
                    request_type, engine_name_url, engine_name, header, token))
            query_engine_response.raise_for_status()
            # access JSOn content

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

            jsonResponse = query_engine_response.json()
            edges = jsonResponse['edges']
            engine_url = edges["endpoint"]

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

        return engine_url

    # run queries
    """
    This method is used to submit a query to run to a running engine. 
    You can specify multiple queries separated by a semicolon (;)..
    :input token url, request type of API and authentication header
    :returns access-token
    """

    def run_query(self, request_type, query_url, header):
        json_response = ""  # base case
        try:

            """
            Request:
            echo "SELECT_QUERY" | curl
            --request POST 'https://YOUR_ENGINE_ENDPOINT/?database=YOUR_DATABASE_NAME' \
            --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE' \
            --data-binary @-
            """

            query_response = requests.get(
                "echo \"SELECT_QUERY\" | curl\n--request {0} \'{1}\' \\\n--header \'{2}\' \\\n--data-binary @-".format(
                    request_type, query_url, header))
            query_response.raise_for_status()

            """
            Response:
            yet to receive firebolt access to test on local
            """

            json_response = query_response.json()

            # print("Entire JSON response")
            # print(jsonResponse)

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

        return json_response
