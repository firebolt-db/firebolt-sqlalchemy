<p align="center">
    <img width="761" alt="SQLAlchemy and Firebolt" src="https://user-images.githubusercontent.com/7674553/145249436-534b3cc0-2350-4f7e-9c56-78ffbcc0f003.png">
</p>

# firebolt-sqlalchemy

The [Firebolt](https://www.firebolt.io/) dialect for [SQLAlchemy](https://www.sqlalchemy.org/). `firebolt-sqlalchemy` uses [Firebolt's Python SDK](https://github.com/firebolt-db/firebolt-python-sdk) which implements [PEP 249](https://www.python.org/dev/peps/pep-0249/).

* [SQLAlchemy Dialects](https://docs.sqlalchemy.org/en/14/dialects/index.html)
* [PyPI Package](https://pypi.org/project/firebolt-sqlalchemy/)

## Installation

Requires Python >=3.7.

```bash
pip install firebolt-sqlalchemy
```

## Connecting

Connection strings use the following structure:

```
firebolt://{username}:{password}@{database}[/{engine_name}]
```

`engine_name` is optional. If omitted, Firebolt will use the default engine for the database.

Examples:

```
firebolt://email@domain:password@sample_database
firebolt://email@domain:password@sample_database/sample_engine
```

To override the API URL (e.g. for dev testing):

```bash
export FIREBOLT_BASE_URL=<your_url>
```

## Quick Start

```python
from sqlalchemy import create_engine
from firebolt_db.firebolt_dialect import FireboltDialect
from sqlalchemy.dialects import registry

registry.register("firebolt", "src.firebolt_db.firebolt_dialect", "FireboltDialect")
engine = create_engine("firebolt://email@domain:password@sample_database/sample_engine")
connection = engine.connect()

connection.execute("CREATE FACT TABLE example(dummy int) PRIMARY INDEX dummy")
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
