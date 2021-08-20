import requests
import smtplib
from requests.exceptions import HTTPError


class Sqlalchemy:

    def __init__(self, user_email, password):
        self.user_email = user_email  # provided by firebolt
        self.password = password  # provided by firebolt

    # refresh access token
    """
    Request:
    curl --request POST 'https://api.app.firebolt.io/auth/v1/refresh' \  
    --header 'Content-Type: application/json;charset=UTF-8' \  
    --data-binary '{"refresh_token":"YOUR_REFRESH_TOKEN_VALUE"}'
    """

    def get_access_token_via_refresh(self, refresh_url, request_type, header, refresh_token):
        refresh_access_token = 0
        try:
            token_response = requests.get(
                "curl --request {0} \'{1}\' \\\n--header \'{2}\' \\\n--data-binary\n\'{\"refresh_token\":"
                "\"{3}\"}\'".format(
                    request_type, refresh_url, header, refresh_token))
            token_response.raise_for_status()
            # access JSOn content

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
            scope = jsonResponse["scope"]
            expires_in = jsonResponse["expires_in"]
            token_type = jsonResponse["token_type"]

            print("Entire JSON response")
            print(jsonResponse)

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

        return refresh_access_token

    # generating access token
    """
    General format of request:

    curl --request POST 'https://api.app.firebolt.io/auth/v1/login' \
    --header 'Content-Type: application/json;charset=UTF-8' \
    --data-binary
    '{"username":"YOUR_USER_EMAIL","password":"YOUR_PASSWORD"}'
    """

    def get_access_token(self, token_url, request_type, header):
        access_token = 0
        try:
            token_response = requests.get(
                "curl --request {0} \'{1}\' \\\n--header \'{2}\' \\\n--data-binary\n\'{\"username\":"
                "\"{3}\",\"password\":\"{4}\"}\'".format(
                    request_type, token_url, header, user_email, password))
            token_response.raise_for_status()
            # access JSOn content
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

            jsonResponse = token_response.json()


            print("Entire JSON response")
            print(jsonResponse)

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

        return jsonResponse

    # get engine url by db
    """
    Request:

    curl --request GET 'https://api.app.firebolt.io/core/v1/account/engines:getURLByDatabaseName?database_name=YOUR_DATABASE_NAME' \  
    --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE'
    """

    def get_engine_url_by_db(self, engine_url, request_type, header, token):
        engine_url = ""
        try:
            query_engine_response = requests.get(
                "curl --request {0} \'{1}\' \\\n--header \'{2}{3}\'".format(
                    request_type, engine_url, header, token))
            query_engine_response.raise_for_status()
            # access JSOn content

            """
            Response:
            {"engine_url": "YOUR_DATABASES_DEFAULT_ENGINE_URL"}
            """
            jsonResponse = query_engine_response.json()
            engine_url = jsonResponse["engine_url"]

            print("Entire JSON response")
            print(jsonResponse)

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

        return engine_url

    # get engine url by name
    """
    curl --request GET 'https://api.app.firebolt.io/core/v1/account/engines?filter.name_contains=YOUR_ENGINE_NAME' \  
    --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE'
    """

    query_engine_url_name = 'https://api.app.firebolt.io/core/v1/account/engines?filter.name_contains=YOUR_ENGINE_NAME'

    def get_engine_url_by_name(self, engine_url, request_type, header, token):
        engine_url = ""
        try:
            query_engine_response = requests.get(
                "curl --request {0} \'{1}\' \\\n--header \'{2}{3}\'".format(
                    request_type, engine_url, header, token))
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

            print("Entire JSON response")
            print(jsonResponse)

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

        return engine_url


api_service = Sqlalchemy("","")  # provide user email and password here
token_url = "https://api.app.firebolt.io/auth/v1/login"
token_request_type = "POST"
token_header = "Content-Type: application/json;charset=UTF-8"
refresh_url = "https://api.app.firebolt.io/auth/v1/refresh"

token = api_service.get_access_token(token_url, token_request_type, token_header)
if token == 0:
    token = api_service.get_access_token_via_refresh(refresh_url, token_request_type, token_header, self.refresh_token)

query_engine_url = 'https://api.app.firebolt.io/core/v1/account/engines:getURLByDatabaseName?database_name=YOUR_DATABASE_NAME'
engine_request_type = 'GET'
engine_header = "Authorization: Bearer "
query_engine_url_name = 'https://api.app.firebolt.io/core/v1/account/engines?filter.name_contains=YOUR_ENGINE_NAME'

engine_url = api_service.get_engine_url_by_db(query_engine_url, engine_request_type, engine_header, token)
