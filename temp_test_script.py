# Temporary file to share manual testing code

from sqlalchemy_adapter.firebolt_connector import connect

query = 'select * from joining_details'
connection = connect('aapurva@sigmoidanalytics.com','Apurva111','Sigmoid_Alchemy')
cursor = connection.cursor()
cursor.execute('select * from joining_details')