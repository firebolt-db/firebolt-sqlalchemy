import configparser
import os

config = configparser.ConfigParser()
config.read(f"{os.getcwd()}/env-config.ini")

base_url = "https://api.app.firebolt.io/"

token_url = f"{base_url}{config['API']['token_url']}"
refresh_url = f"{base_url}{config['API']['refresh_url']}"
query_engine_url = f"{base_url}{config['API']['query_engine_url']}:getURLByDatabaseName"
engine_id_url = f"{base_url}{config['API']['engine_id_url']}:getIdbyName"
engine_start_url = f"{base_url}{config['API']['engine_start_url']}"
token_header = {"Content-Type": "application/json;charset=UTF-8"}
