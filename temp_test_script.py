# Temporary file to share manual testing code
# This is a temporary test file to test end to end function of adapter
# To use this file, copy this file to a folder outside firebolt_db
# Comment out the type of test you want to run

# from firebolt_db.firebolt_connector import connect
# from firebolt_db.firebolt_dialect import FireboltDialect

# connection = connect('localhost',8123,'aapurva@sigmoidanalytics.com', 'Apurva111', 'Sigmoid_Alchemy')

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

