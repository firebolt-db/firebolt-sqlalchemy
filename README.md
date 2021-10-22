# firebolt-sqlalchemy

```
This is the 'alpha' package. Expect updates in the future.
```

Firebolt is a Cloud Data Warehousing solution that helps its users streamline their Data Analytics and access to insights. It offers fast query performance and combines Elasticity, Simplicity, Low cost of the Cloud, and innovation in Analytics.

The SQLAlchemy Adapter will act as an interface for third-party applications with SQLAlchemy support (like Superset, Redash etc.) to communicate with Firebolt databases through REST APIs provided by Firebolt. The adapter is written in Python language using SQLAlchemy toolkit. It is built as per PEP 249 - Python Database API Specification v2.0 which specifies a set of standard interfaces for applications that wish to access a specific database.

## Goals:
1. Build Firebolt SQLAlchemy Adapter Python library: The aim is to package the Firebolt SQLAlchemy Adapter in a Python library which can be imported and used by third party applications.
2. SQLAlchemy Adapter connects with Firebolt Database: The adapter should be able to access Firebolt database to retrieve database metadata and table data.
3. Provide accessible methods for third party applications: The adapter should provide standard methods for third party applications to be able to use it as per its requirement.

## Technologies:
1. Python 3.*
2. SQLAlchemy 1.4/2.0
3. REST API
4. Git and Github
5. SQL


## Installation:
```bash
pip install firebolt-sqlalchemy
```


## Connection Method:
The recommended connection string is:
```
firebolt://{username}:{password}@{database}
or
firebolt://{username}:{password}@{database}/{engine_name}
```
Sample connection strings to connect to a Firebolt database:
```
firebolt://email@domain:password@sample_database
firebolt://email@domain:password@sample_database/sample_engine
```


## DB API

```python
from firebolt_db.firebolt_connector import connect

connection = connect('engine_name',8123,'email@domain', 'password', 'db_name')
query = 'select * from sample_table limit 10'
cursor = connection.cursor()

response = cursor.execute(query)
print(response.fetchmany(3))
```

## SQLAlchemy

```python
from sqlalchemy import create_engine
from firebolt_db.firebolt_dialect import FireboltDialect
from sqlalchemy.dialects import registry

registry.register("firebolt", "src.firebolt_db.firebolt_dialect", "FireboltDialect")
engine = create_engine("firebolt://email@domain:password@sample_database/sample_engine")

dialect = FireboltDialect()
schemas = dialect.get_schema_names(engine)

connection = engine.connect()
schemas = dialect.get_schema_names(connection)
```

## Components in the Adapter:
1. Firebolt Connector: This file is used to establish a connection to the Firebolt database from 3rd party applications. It provides a ‘connect’ method which accepts parameters like database name, username, password etc. from the connecting application to identify the database and authenticate the user credentials. It returns a database connection which is used to execute queries on the database.
2. API Service: The API Service is responsible for calling Firebolt REST APIs to establish connection with the database and fire SQL queries on it. It provides methods to get access token as per user credentials, get the specific engine URL and execute/run SQL queries. Executing queries need access token and engine URL as per the Firebolt REST API specifications.
3. Firebolt Dialect: It provides methods for retrieving metadata about databases like schema data, table names, column names etc. It also maps the data types between Firebolt and SQLAlchemy along with providing a data type compiler for complex data types.


## Testing Strategy:
1. Test firebolt database creation, data ingestion and select query using Firebolt manager.
2. Test getting access token and engine URL using Firebolt REST API through adapter API Service code.
3. Test running SQL queries on a database using Firebolt REST API through adapter API service code.
4. Test Firebolt Connector methods to create connection, get database cursor and execute SQL queries.
5. Integration testing with Superset and Redash.
6. End to end functionality testing through Superset and Redash.
7. Link for unit tests: [Unit-Testing Results](https://docs.google.com/spreadsheets/d/1uP49jjpwCzfYPeh9NIkm_BdsmIV1bSrJHsR4wk86zUE/edit#gid=1161341563)
8. Link for Query testing: [Query-Testing Results](https://docs.google.com/spreadsheets/d/1V0gw-Ke8m3bcGF4bs-SaTgnnO73Rxw8iZ15x5lMJJ0g/edit#gid=0)


## References:
1. PyPi - [Firebolt PyPi](https://pypi.org/project/firebolt-sqlalchemy/)
2. Important Firebolt URLs:
    1. [Rest API](https://docs.firebolt.io/integrations/connecting-via-rest-api)
    2. [Information schema](https://docs.firebolt.io/general-reference/information-schema)
    3. [Engine usage](https://docs.firebolt.io/working-with-engines)
3. SQLAlchemy: [Dialect](https://docs.sqlalchemy.org/en/14/dialects/)
5. DB-API Driver: [PEP 249](https://www.python.org/dev/peps/pep-0249/)
