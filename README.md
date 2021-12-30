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

engine = create_engine("firebolt://email@domain:password@sample_database/sample_engine")
connection = engine.connect()

connection.execute("CREATE FACT TABLE example(dummy int) PRIMARY INDEX dummy")
connection.execute("INSERT INTO example(dummy) VALUES (11)")
result = connection.execute("SELECT * FROM example")
for item in result.fetchall():
    print(item)
```

### [AsyncIO](https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html) extension

```python
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("asyncio+firebolt://email@domain:password@sample_database/sample_engine")

async with engine.connect() as conn:

    await conn.execute(
        text(f"INSERT INTO example(dummy) VALUES (11)")
    )

    result = await conn.execute(
        text(f"SELECT * FROM example")
    )
    print(result.fetchall())

await engine.dispose()
```


## Limitations

1. Transactions are not supported since Firebolt database does not support them at this time.
1. Parametrised calls to execute and executemany are not implemented.

## Contributing

See: [CONTRIBUTING.MD](https://github.com/firebolt-db/firebolt-sqlalchemy/tree/master/CONTRIBUTING.MD)
