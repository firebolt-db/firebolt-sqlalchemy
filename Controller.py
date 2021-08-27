from sqlalchemy_adapter.ApiConnectorService import ApiConnectorService

"""
Utility class to interlink modules of Sqlalchemy Adapter
To be merged with AlchemyConnector
"""


class Controller:

    def __init__(self, user_email, password, db_name, query):
        self._user_email = user_email
        self._password = password
        self._db_name = db_name
        self._query = query
        self._token = 0
        self._engine_url = ""

    """
    Method to call ApiConnectorService in order to fire respective Firebolt APIs 
    and retrieve data required for the dialect
    """

    def get_connection_details(self):
        api_service = ApiConnectorService()
        token_url = "https://api.app.firebolt.io/auth/v1/login"
        token_header = {"Content-Type": "application/json;charset=UTF-8"}
        refresh_url = "https://api.app.firebolt.io/auth/v1/refresh"

        # get access token
        token_api_response = api_service.get_access_token
        token_json = token_api_response(token_url, token_header,
                                        {'username': self._user_email, 'password': self._password})
        access_token = token_json["access_token"]
        refresh_token = token_json["refresh_token"]

        if access_token == "":
            refresh_api_response = api_service.get_access_token_via_refresh
            self._token = refresh_api_response(refresh_url, token_header, {'refresh_token': refresh_token})
        else:  # flag check for token validity
            self._token = access_token

        print(self._token)
        header = {'Authorization': "Bearer " + self._token}
        query_engine_url = 'https://api.app.firebolt.io/core/v1/account/engines:getURLByDatabaseName'

        # to get engine url using engine name, use below url
        # query_engine_url_name = "https://api.app.firebolt.io/core/v1/
        # account/engines?filter.name_contains=YOUR_ENGINE_NAME"

        # get engine url
        engine_api_response = api_service.get_engine_url_by_db
        self._engine_url = engine_api_response(query_engine_url, self._db_name, header)

        print(self._engine_url)
        # get db response using firebolt api
        db_response = api_service.run_query("https://" + self._engine_url, self._db_name,
                                            header, {"query": (None, self._query)})
        return

def get_connection(user_email, password, db_name):
    api_service = ApiConnectorService()
    token_url = "https://api.app.firebolt.io/auth/v1/login"
    token_header = {"Content-Type": "application/json;charset=UTF-8"}
    refresh_url = "https://api.app.firebolt.io/auth/v1/refresh"

    # get access token
    token_api_response = api_service.get_access_token
    token_json = token_api_response(token_url, token_header,
                                    {'username': user_email, 'password': password})
    new_access_token = token_json["access_token"]
    refresh_token = token_json["refresh_token"]

    if new_access_token == "":
        refresh_api_response = api_service.get_access_token_via_refresh
        access_token = refresh_api_response(refresh_url, token_header, {'refresh_token': refresh_token})
    else:  # flag check for token validity
        access_token = new_access_token

    print(access_token)
    header = {'Authorization': "Bearer " + access_token}
    query_engine_url = 'https://api.app.firebolt.io/core/v1/account/engines:getURLByDatabaseName'

    # get engine url
    engine_api_response = api_service.get_engine_url_by_db
    engine_url = engine_api_response(query_engine_url, db_name, header)

    print(engine_url)

    return (access_token, engine_url)

