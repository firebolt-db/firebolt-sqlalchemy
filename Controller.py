import Sqlalchemy


class Controller:

    def __init__(self, request):
        self.query = request
        self.token = token
        self.url = url

    def get_connection_details(self):
        api_service = Sqlalchemy()  # provide user email and password here
        token_url = "https://api.app.firebolt.io/auth/v1/login"
        token_request_type = "POST"
        token_header = "Content-Type: application/json;charset=UTF-8"
        refresh_url = "https://api.app.firebolt.io/auth/v1/refresh"

        token_json = api_service.get_access_token(token_url, token_request_type, token_header)
        access_token = token_json["access_token"]
        refresh_token = token_json["refresh_token"]
        if access_token == 0:
            self.token = api_service.get_access_token_via_refresh(refresh_url, token_request_type, token_header,
                                                                  refresh_token)
        else:
            self.token = access_token

        query_engine_url = 'https://api.app.firebolt.io/core/v1/account/' \
                           'engines:getURLByDatabaseName?database_name=YOUR_DATABASE_NAME'
        engine_request_type = 'GET'
        engine_header = "Authorization: Bearer "

        # to get engine url using engine name, use below url
        # query_engine_url_name = "https://api.app.firebolt.io/core/v1/
        # account/engines?filter.name_contains=YOUR_ENGINE_NAME"

        self.url = api_service.get_engine_url_by_db(query_engine_url, engine_request_type, engine_header, self.token)

        return self.token, self.url


controller = Controller(" ")
dialect = Dialect(controller)
