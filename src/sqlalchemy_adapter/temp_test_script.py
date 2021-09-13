# Temporary file to share manual testing code
# This is a temporary test file to test end to end function of adapter
# To use this file, copy this file to a folder outside sqlalchemy_adapter
# Comment out the type of test you want to run

from sqlalchemy_adapter.firebolt_connector import connect
from sqlalchemy_adapter.firebolt_dialect import FireboltDialect

connection = connect('aapurva@sigmoidanalytics.com', 'Apurva111', 'Sigmoid_Alchemy')

# Test for end to end
# query = 'select * from lineitem limit 10'
# cursor = connection.cursor()
# response = cursor.execute(query)
# # print(response.fetchmany(3))
# # print(response.fetchone())
# print(response.fetchall())

# Test for dialect
dialect = FireboltDialect()

schemas = dialect.get_schema_names(connection).fetchall()
print("Schema Names")
print(schemas)

tables = dialect.get_table_names(connection,"Sigmoid_Alchemy").fetchall()
print("Table names")
print(tables)

