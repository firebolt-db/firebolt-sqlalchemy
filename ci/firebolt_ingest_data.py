import json
import sys

import requests
from requests.exceptions import HTTPError

from firebolt_db import exceptions
from firebolt_db import constants

# Arguments passed
user_email = sys.argv[1]
password = sys.argv[2]
db_name = sys.argv[3]


class IngestFireboltData:

    @staticmethod
    def create_tables():
        """
        Create tables for testing purpose.
        This method creates tables inside firebolt database to test for code changes
        It internally calls methods to get access token and engine URL.
        :input user-email, password, database name, external table name and fact table name
        """
        # get access token
        access_token = IngestFireboltData.get_access_token({'username': user_email, 'password': password})
        # get engine url
        engine_url = IngestFireboltData.get_engine_url_by_db(access_token)
        # create external table
        IngestFireboltData.create_external_table(engine_url, access_token)
        # create fact table
        IngestFireboltData.create_fact_table(engine_url, access_token)
        # ingest data into fact table
        IngestFireboltData.ingest_data(engine_url, access_token)

    @staticmethod
    def get_access_token(data):
        """
        Retrieve authentication token
        This method uses the user email and the password to fire the API to generate access-token.
        :input dictionary containing user-email and password
        :returns access-token
        """
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

        return access_token

    @staticmethod
    def get_engine_url_by_db(access_token):
        """
        Get engine url by db name
        This method generates engine url using db name and access-token
        :input api url, request type, authentication header and access-token
        :returns engine url
        """
        engine_url = ""  # base case
        payload = {}
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
    def create_external_table(engine_url, access_token):
        """
        This method is used to create an external table.
        :input engine url, db_name, access_token, table_name
        """
        payload = {}
        try:
            external_table_script = 'CREATE EXTERNAL TABLE IF NOT EXISTS test_external_table' \
                                    '(       l_orderkey              LONG,' \
                                    'l_partkey               LONG,' \
                                    'l_suppkey               LONG,' \
                                    'l_linenumber            INT,' \
                                    'l_quantity              LONG,' \
                                    'l_extendedprice         LONG,' \
                                    'l_discount              LONG,' \
                                    'l_tax                   LONG,' \
                                    'l_returnflag            TEXT,' \
                                    'l_linestatus            TEXT,' \
                                    'l_shipdate              TEXT,' \
                                    'l_commitdate            TEXT,' \
                                    'l_receiptdate           TEXT,' \
                                    'l_shipinstruct          TEXT,' \
                                    'l_shipmode              TEXT,' \
                                    'l_comment               TEXT' \
                                    ')' \
                                    'URL = \'s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/\'' \
                                    'OBJECT_PATTERN = \'*.parquet\'' \
                                    'TYPE = (PARQUET);'
            # '-- CREDENTIALS = ( AWS_KEY_ID = \'******\' AWS_SECRET_KEY = \'******\' )' \

            """
            General format of request:
            echo "CREATE_EXTERNAL_TABLE_SCRIPT" | curl \
            --request POST 'https://YOUR_ENGINE_URL/?database=YOUR_DATABASE_NAME' \
            --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE' \
            --data-binary @-
            """

            header = {'Authorization': "Bearer " + access_token}
            api_response = requests.post(url="https://" + engine_url + "/", params={'database': db_name},
                                         headers=header, files={"query": (None, external_table_script)})
            api_response.raise_for_status()

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

    @staticmethod
    def create_fact_table(engine_url, access_token):
        """
        Create a fact table
        This method is used to create a fact table.
        :input engine url, db_name, access_token, table_name
        """
        payload = {}
        try:
            fact_table_script = 'CREATE FACT TABLE IF NOT EXISTS ci_fact_table' \
                                '(       l_orderkey              LONG,' \
                                '        l_partkey               LONG,' \
                                '        l_suppkey               LONG,' \
                                '        l_linenumber            INT,' \
                                '        l_quantity              LONG,' \
                                '        l_extendedprice         LONG,' \
                                '        l_discount              LONG,' \
                                '        l_tax                   LONG,' \
                                '        l_returnflag            TEXT,' \
                                '        l_linestatus            TEXT,' \
                                '        l_shipdate              TEXT,' \
                                '        l_commitdate            TEXT,' \
                                '        l_receiptdate           TEXT,' \
                                '        l_shipinstruct          TEXT,' \
                                '        l_shipmode              TEXT,' \
                                '        l_comment               TEXT' \
                                ') PRIMARY INDEX l_orderkey, l_linenumber;'
            """
            General format of request:
            echo "CREATE_FACT_TABLE_SCRIPT" | curl \
            --request POST 'https://YOUR_ENGINE_URL/?database=YOUR_DATABASE_NAME' \
            --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE' \
            --data-binary @-
            """

            header = {'Authorization': "Bearer " + access_token}
            api_response = requests.post(url="https://" + engine_url, params={'database': db_name},
                                         headers=header, files={"query": (None, fact_table_script)})
            api_response.raise_for_status()

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

    @staticmethod
    def ingest_data(engine_url, access_token):
        """
        This method is used to ingest data into the fact table.
        :input engine url, db_name, access_token, external_table, fact_table
        """
        payload = {}
        try:
            import_script = 'INSERT INTO ci_fact_table\n' \
                            'SELECT *' \
                            'FROM test_external_table WHERE NOT EXISTS ' \
                            '(SELECT l_orderkey FROM ci_fact_table WHERE ci_fact_table.l_orderkey = ' \
                             'test_external_table.l_orderkey) ;'

            """
            General format of request:
            echo "IMPORT_SCRIPT" | curl \
            --request POST 'https://YOUR_ENGINE_URL/?database=YOUR_DATABASE_NAME' \
            --header 'Authorization: Bearer YOUR_ACCESS_TOKEN_VALUE' --data-binary @-
            """

            header = {'Authorization': "Bearer " + access_token}
            api_response = requests.post(url="https://" + engine_url, params={'database': db_name},
                                         headers=header, files={"query": (None, import_script)})
            api_response.raise_for_status()

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


IngestFireboltData.create_tables()
