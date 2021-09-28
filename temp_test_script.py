# Temporary file to share manual testing code
# This is a temporary test file to test end to end function of adapter
# To use this file, copy this file to a folder outside firebolt_db
# Comment out the type of test you want to run

# from firebolt_db.firebolt_connector import connect
from firebolt_db.firebolt_dialect import FireboltDialect

# connection = connect('localhost',8123,'aapurva@sigmoidanalytics.com', 'Apurva111', 'Sigmoid_Alchemy')

# if http_err.response.status_code == 401:
            #     access_token = FireboltApiService.get_access_token_via_refresh(refresh_token)
            #     header = {'Authorization': "Bearer " + access_token}
            #     query_response = requests.post(url="https://" + engine_url, params={'database': db_name},
            #                                    headers=header, files={"query": (None, query)})
            # else:

# Test for end to end
# query = 'select * from lineitem limit 10'
# cursor = connection.cursor()
# response = cursor.execute(query)
# # print(response.fetchmany(3))
# # print(response.fetchone())
# print(response.fetchall())

# Test for dialect
# dialect = FireboltDialect()

# schemas = dialect.get_schema_names(connection)
# print("Schema Names")
# print(schemas.fetchone())

# tables = dialect.get_table_names(connection,"Sigmoid_Alchemy").fetchall()
# print("Table names")
# print(tables)


from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
registry.register("firebolt", "src.firebolt_db.firebolt_dialect", "FireboltDialect")

engine = create_engine("firebolt://aapurva@sigmoidanalytics.com:Apurva111@host/Sigmoid_Alchemy")
print(type(engine))

dialect = FireboltDialect()

schemas = dialect.get_schema_names(engine)
print(schemas)
# print("Schema Names")
# print(schemas.fetchone())

schemas = dialect.get_table_names(engine,"Sigmoid_Alchemy")
print(schemas)

schemas = dialect.get_columns(engine,"lineitem","Sigmoid_Alchemy")
print(schemas)

connection = engine.connect()
schemas = dialect.get_schema_names(connection)
print(schemas)

