### Links

* [SQL Alchemy docs](https://www.sqlalchemy.org/library.html#reference)
* [Dev Docs](https://api.dev.firebolt.io/devDocs)

### Development Setup

1. Clone this repo into a Python `3.7+` virtual environment.
1. `pip install .`
1. `pip install isort, autoflake, black`
1. If using PyCharm, set the test runner as `pytest` and mark `src` as a sources root.


### Before Committing

1. run `mypy src` to check for type errors
1. run `pytest tests/unit` to run unit tests
1. run `isort .` to sort the imports
1. run `black .` to fix formatting
1. run `autoflake -i -r --remove-all-unused-imports --ignore-init-module-imports --remove-duplicate-keys --remove-unused-variables .` to fix formatting

In the near future automatic linting and type checking will be added via pre-commit.

### Integration tests

This assumes you have a firebolt account and a database & engine setup. If not, follow the [Dev Docs](https://api.dev.firebolt.io/devDocs).

Make sure to set the following environment variables:
```bash
export engine_name=<your_engine>
export database_name=<your_db>
export username=<your_username>
export password=<your_password>
```

Run `pytest tests/integration` to run integration tests. The Firebolt Engine has to be running for those to succeed.


### Docstrings

Use the Google format for docstrings. Do not include types or an indication 
of "optional" in docstrings. Those should be captured in the function signature 
as type annotations; no need to repeat them in the docstring.

Public methods and functions should have docstrings. 
One-liners are fine for simple methods and functions.

For PyCharm Users:

1. Tools > Python Integrated Tools > Docstring Format: "Google"
2. Editor > General > Smart Keys > Check "Insert documentation comment stub"
3. Editor > General > Smart Keys > Python > Uncheck "Insert type placeholders..."

### Import style

In general, prefer `from typing import Optional, ...`, and not `import typing`.

### Method Order

In general, organize class internals in this order:

1. class attributes
2. `__init__()`
3. classmethods (`@classmethod`)
   * alternative constructors first
   * other classmethods next
4. properties (`@property`)
5. remaining methods 
   * put more important / broadly applicable functions first
   * group related functions together to minimize scrolling

Read more about this philosophy 
[here](https://softwareengineering.stackexchange.com/a/199317).

### Huge classes

If classes start to approach 1k lines, consider breaking them into parts, 
possibly like [this](https://stackoverflow.com/a/47562412).


### Versioning

Consider adopting: 
 * https://packboard.atlassian.net/wiki/x/AYC6aQ
 * https://python-semantic-release.readthedocs.io/en/latest/