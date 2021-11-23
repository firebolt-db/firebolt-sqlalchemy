# firebolt-sqlalchemy

[Firebolt](https://www.firebolt.io/) is a Cloud Data Warehousing solution that helps its users streamline their Data Analytics and access to insights. It offers fast query performance and combines Elasticity, Simplicity, Low cost of the Cloud, and innovation in Analytics.

The SQLAlchemy Adapter acts as an interface for third-party applications with SQLAlchemy support (like Superset, Redash etc.) to communicate with Firebolt databases through REST APIs provided by Firebolt.


## Installation:

Requires Python >=3.7

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

To override the API url (e.g. for dev testing)
```bash
export FIREBOLT_BASE_URL=<your_url>
```

## Quickstart

```python
from sqlalchemy import create_engine
from firebolt_db.firebolt_dialect import FireboltDialect
from sqlalchemy.dialects import registry

registry.register("firebolt", "src.firebolt_db.firebolt_dialect", "FireboltDialect")
engine = create_engine("firebolt://email@domain:password@sample_database/sample_engine")
connection = engine.connect()

connection.execute("CREATE FACT TABLE example(dummy int)")
connection.execute("INSERT INTO example(dummy) VALUES (11)")
result = connection.execute("SELECT * FROM example")
for item in result.fetchall():
    print(item)
```

## Limitations

1. Transactions are not supported since Firebolt database does not support them at this time.
1. [AsyncIO](https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html) is not yet implemented.

## Contributing

See: [CONTRIBUTING.MD](https://github.com/firebolt-db/firebolt-sqlalchemy/tree/master/CONTRIBUTING.MD)

## References:
1. PyPi - [Firebolt PyPi](https://pypi.org/project/firebolt-sqlalchemy/)
2. Important Firebolt URLs:
    1. [Rest API](https://docs.firebolt.io/integrations/connecting-via-rest-api)
    2. [Information schema](https://docs.firebolt.io/general-reference/information-schema)
    3. [Engine usage](https://docs.firebolt.io/working-with-engines)
3. SQLAlchemy: [Dialect](https://docs.sqlalchemy.org/en/14/dialects/)
5. DB-API Driver: [PEP 249](https://www.python.org/dev/peps/pep-0249/)
