<p align="center">
    <img width="761" alt="SQLAlchemy and Firebolt" src="https://user-images.githubusercontent.com/7674553/145249436-534b3cc0-2350-4f7e-9c56-78ffbcc0f003.png">
</p>

# firebolt-sqlalchemy

[![Unit tests](https://github.com/firebolt-db/firebolt-sqlalchemy/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/firebolt-db/firebolt-sqlalchemy/actions/workflows/unit-tests.yml)
[![Code quality checks](https://github.com/firebolt-db/firebolt-sqlalchemy/actions/workflows/code-check.yml/badge.svg)](https://github.com/firebolt-db/firebolt-sqlalchemy/actions/workflows/code-check.yml)
[![Firebolt Security Scan](https://github.com/firebolt-db/firebolt-sqlalchemy/actions/workflows/security-scan.yml/badge.svg)](https://github.com/firebolt-db/firebolt-sqlalchemy/actions/workflows/security-scan.yml)
[![Integration tests](https://github.com/firebolt-db/firebolt-sqlalchemy/actions/workflows/python-integration-tests.yml/badge.svg)](https://github.com/firebolt-db/firebolt-sqlalchemy/actions/workflows/python-integration-tests.yml)
![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/ptiurin/64f31d124b7249319234d247ade4a7db/raw/firebolt-sqlalchemy-coverage.json)



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
firebolt://{username}:{password}@{database}[/{engine_name}][?account_name={name}}]
```

`engine_name` is optional. If omitted, Firebolt will use the default engine for the database.

`account_name` is optional. If omitted a default account will be used for connection.

Examples:

```
firebolt://email@domain:password@sample_database
firebolt://email@domain:password@sample_database/sample_engine
```

If a different account name is required, it can be specified in the connection string

```
firebolt://email@domain:password@sample_database/sample_engine?account_name=my_account
```

To override the API URL (e.g. for dev testing):

```bash
export FIREBOLT_BASE_URL=<your_url>
```

If your password contains % or / characters they need to be sanitised as per https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
```python
my_pass = "0920%/2"
import urllib.parse
new_pass = urllib.parse.quote_plus(my_pass)
```

## Quick Start

```python
import urllib.parse
from sqlalchemy import create_engine

password = urllib.parse.quote_plus("your_password_here")
engine = create_engine("firebolt://email@domain:" + password + "@sample_database/sample_engine")
connection = engine.connect()

connection.execute("CREATE FACT TABLE example(dummy int) PRIMARY INDEX dummy")
connection.execute("INSERT INTO example(dummy) VALUES (11)")
result = connection.execute("SELECT * FROM example")
for item in result.fetchall():
    print(item)
```

### [AsyncIO](https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html) extension

```python
import urllib.parse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

password = urllib.parse.quote_plus("your_password_here")
engine = create_async_engine("asyncio+firebolt://email@domain:" + password + "@sample_database/sample_engine")

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
