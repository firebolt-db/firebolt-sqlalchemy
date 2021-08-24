import ApiConnectorService

"""
Utility class to interlink modules of Sqlalchemy Adapter
To be merged with AlchemyConnector
"""

class Controller:

    def __init__(self, user_email, password):
        self._user_email = user_email
        self._password = password
        self._token = 0;
        self._engine_url = ""

    """
    Method to call ApiConnectorService in order to fire respective Firebolt APIs 
    and retrieve data required for the dialect
    """
    def get_connection_details(self):
        api_service = ApiConnectorService(self._user_email, self._password)  # provide user email and password here
        token_url = "https://api.app.firebolt.io/auth/v1/login"
        token_request_type = "POST"
        token_header = "Content-Type: application/json;charset=UTF-8"
        refresh_url = "https://api.app.firebolt.io/auth/v1/refresh"

        token_json = api_service.get_access_token(token_url, token_request_type, token_header)
        access_token = token_json["access_token"]
        refresh_token = token_json["refresh_token"]
        if access_token == 0:    # flag check for token validity
            self._token = api_service.get_access_token_via_refresh(refresh_url, token_request_type, token_header,
                                                                  refresh_token)
        else:
            self._token = access_token

        query_engine_url = 'https://api.app.firebolt.io/core/v1/account/' \
                           'engines:getURLByDatabaseName?database_name=YOUR_DATABASE_NAME'
        engine_request_type = 'GET'
        engine_header = "Authorization: Bearer "

        # to get engine url using engine name, use below url
        # query_engine_url_name = "https://api.app.firebolt.io/core/v1/
        # account/engines?filter.name_contains=YOUR_ENGINE_NAME"

        self._engine_url = api_service.get_engine_url_by_db(query_engine_url, engine_request_type, engine_header, self._token)

        return self._token, self._url

"""
token_url = "https://api.app.firebolt.io/auth/v1/login"
token_request_type = "POST"
token_header = "Content-Type: application/json;charset=UTF-8"
refresh_url = "https://api.app.firebolt.io/auth/v1/refresh"

query_engine_url = 'https://api.app.firebolt.io/core/v1/account/engines:getURLByDatabaseName?database_name=YOUR_DATABASE_NAME'
engine_request_type = 'GET'
engine_header = "Authorization: Bearer "
query_engine_url_name = 'https://api.app.firebolt.io/core/v1/account/engines?filter.name_contains=YOUR_ENGINE_NAME'
"""
