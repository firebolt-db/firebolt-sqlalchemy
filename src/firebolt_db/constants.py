import os
from dotenv import load_dotenv,find_dotenv

found_dotenv = find_dotenv()

if found_dotenv:
    load_dotenv(found_dotenv)
    base_url = os.environ["BASE_URL"]
else:
    base_url = "https://api.app.firebolt.io"

token_url = f"{base_url}/auth/v1/login"
refresh_url = f"{base_url}/auth/v1/refresh"
query_engine_url = f"{base_url}/core/v1/account/engines:getURLByDatabaseName"
query_engine_url_by_engine_name = f"{base_url}/core/v1/account/engines"
engine_id_url = f"{base_url}/core/v1/account/engines:getIdbyName"
engine_start_url = f"{base_url}/core/v1/account/engines"
token_header = {"Content-Type": "application/json;charset=UTF-8"}
