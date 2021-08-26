import ApiConnectorService

"""
Utility class to interlink modules of Sqlalchemy Adapter
To be merged with AlchemyConnector
"""

class Controller:

    def __init__(self, user_email, password, db_name):
        self._user_email = user_email
        self._password = password
        self._db_name = db_name
        self._token = 0
        self._engine_url = ""

    """
    Method to call ApiConnectorService in order to fire respective Firebolt APIs 
    and retrieve data required for the dialect
    """
    def get_connection_details(self):
        token_url = "https://api.app.firebolt.io/auth/v1/login"
        token_request_type = "POST"
        token_header = "Content-Type: application/json;charset=UTF-8"
        refresh_url = "https://api.app.firebolt.io/auth/v1/refresh"

        # get access token
        token_json = ApiConnectorService.get_access_token(token_url, token_request_type, token_header, self._user_email, self._password)
        access_token = token_json["access_token"]
        refresh_token = token_json["refresh_token"]
        if access_token != 0:
            self._token = access_token
        else:  # flag check for token validity
            self._token = ApiConnectorService.get_access_token_via_refresh(refresh_url, token_request_type, token_header,
                                                                  refresh_token)

        query_engine_url = 'https://api.app.firebolt.io/core/v1/account/' \
                           'engines:getURLByDatabaseName?database_name='+self._db_name
        engine_request_type = 'GET'
        engine_header = "Authorization: Bearer "+self._token

        # to get engine url using engine name, use below url
        # query_engine_url_name = "https://api.app.firebolt.io/core/v1/
        # account/engines?filter.name_contains=YOUR_ENGINE_NAME"

        # get engine url
        self._engine_url = ApiConnectorService.get_engine_url_by_db(query_engine_url, engine_request_type, engine_header)

        get_db_data_url = "https://"+self._engine_url+"/?database="+self._db_name
        query_request_type = 'POST'
        query_header = "Authorization: Bearer "+self._token

        # get db metadata using firebolt api
        db_response = ApiConnectorService.run_query(query_request_type, get_db_data_url, query_header)
        return db_response

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
