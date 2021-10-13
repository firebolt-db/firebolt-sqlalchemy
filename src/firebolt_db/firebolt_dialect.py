import sqlalchemy.types as sqltypes
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy.types import (
    CHAR, DATE, DATETIME, INTEGER, BIGINT,
    TIMESTAMP, VARCHAR, BOOLEAN, FLOAT)

import firebolt_db


class ARRAY(sqltypes.TypeEngine):
    __visit_name__ = 'ARRAY'


# Firebolt data types compatibility with sqlalchemy.sql.types
type_map = {
    "char": CHAR,
    "text": VARCHAR,
    "varchar": VARCHAR,
    "string": VARCHAR,
    "float": FLOAT,
    "double": FLOAT,
    "double precision": FLOAT,
    "boolean": BOOLEAN,
    "int": INTEGER,
    "integer": INTEGER,
    "bigint": BIGINT,
    "long": BIGINT,
    "timestamp": TIMESTAMP,
    "datetime": DATETIME,
    "date": DATE,
    "array": ARRAY,
}


class UniversalSet(object):
    def __contains__(self, item):
        return True


class FireboltIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = UniversalSet()


class FireboltCompiler(compiler.SQLCompiler):
    pass


class FireboltTypeCompiler(compiler.GenericTypeCompiler):

    def visit_ARRAY(self, type, **kw):
        return "Array(%s)" % type


class FireboltDialect(default.DefaultDialect):
    """
    FireboltDialect defines the behavior of Firebolt database and DB-API combination.
    It is responsible for metadata definition and firing queries for receiving Database schema and table information.
    """

    name = "firebolt"
    driver = "firebolt"
    user = None
    password = None
    preparer = FireboltIdentifierPreparer
    statement_compiler = FireboltCompiler
    type_compiler = FireboltTypeCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    def __init__(self, context=None, *args, **kwargs):
        super(FireboltDialect, self).__init__(*args, **kwargs)
        self.context = context or {}

    @classmethod
    def dbapi(cls):
        return firebolt_db

    # Build DB-API compatible connection arguments.
    # URL format : firebolt://username:password@host:port/db_name
    def create_connect_args(self, url):
        kwargs = {
            "host": url.host or None,
            "port": url.port or 5432,
            "username": url.username or None,
            "password": url.password or None,
            "db_name": url.database,
            # "scheme": self.scheme,
            "context": self.context,
            "header": False,  # url.query.get("header") == "true",
        }
        return ([], kwargs)

    def get_schema_names(self, connection, **kwargs):
        query = "SELECT SCHEMA_NAME FROM information_schema.databases"
        result = connection.execute(query)
        return [
            row.schema_name for row in result
        ]

    def has_table(self, connection, table_name, schema=None):
        query = """
            SELECT COUNT(*) > 0 AS exists_
              FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_NAME = '{table_name}'
        """.format(
            table_name=table_name
        )

        result = connection.execute(query)
        return result.fetchone().exists_

    def get_table_names(self, connection, schema=None, **kwargs):
        query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
        if schema:
            query = "{query} WHERE TABLE_SCHEMA = '{schema}'".format(
                query=query, schema=schema
            )

        result = connection.execute(query)
        return [row.table_name for row in result]

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        query = """
            SELECT COLUMN_NAME,
                   DATA_TYPE,
                   IS_NULLABLE
              FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_NAME = '{table_name}'
        """.format(
            table_name=table_name
        )
        if schema:
            query = "{query} AND TABLE_SCHEMA = '{schema}'".format(
                query=query, schema=schema
            )

        result = connection.execute(query)

        return [
            {
                "name": row[0],
                "type": type_map[row[1].lower()],
                "nullable": get_is_nullable(row[2]),
                "default": None,
            }
            for row in result
        ]

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {"text": ""}

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kwargs):
        pass

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True


dialect = FireboltDialect


def get_is_nullable(column_is_nullable):
    return column_is_nullable.lower() == "yes"
