import os
from dotenv import load_dotenv

load_dotenv()


def get_env_variable(var_name: str) -> str:
    """Get the environment variable or raise exception."""
    try:
        return os.environ[var_name]
    except KeyError:
        error_msg = "The environment variable {} was missing, abort...".format(
            var_name
        )
        raise EnvironmentError(error_msg)


DATABASE_DIALECT = get_env_variable("refresh_url")

base_url = "https://api.app.firebolt.io/"

token_url = f"{base_url}{get_env_variable('token_url')}"
refresh_url = f"{base_url}{get_env_variable('refresh_url')}"
query_engine_url = f"{base_url}{get_env_variable('query_engine_url')}:getURLByDatabaseName"
query_engine_url_by_engine_name = f"{base_url}{get_env_variable('query_engine_url')}"
engine_id_url = f"{base_url}{get_env_variable('engine_id_url')}:getIdbyName"
engine_start_url = f"{base_url}{get_env_variable('engine_start_url')}"
token_header = {"Content-Type": "application/json;charset=UTF-8"}
