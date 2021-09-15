from sqlalchemy import types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler

# Firebolt data types compatibility with sqlalchemy.sql.types
import firebolt_db

type_map = {
    "char": types.String,
    "text": types.String,
    "varchar": types.String,
    "string": types.String,
    "float": types.Float,
    "double": types.Float,
    "double precision": types.Float,
    "boolean": types.Boolean,
    "int": types.BigInteger,
    "integer": types.BigInteger,
    "bigint": types.BigInteger,
    "long": types.BigInteger,
    "timestamp": types.TIMESTAMP,
    "datetime": types.TIMESTAMP,
    "date": types.DATE,
    "array": types.ARRAY,
}


class UniversalSet(object):
    def __contains__(self, item):
        return True


class FireboltIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = UniversalSet()


class FireboltCompiler(compiler.SQLCompiler):
    pass


class FireboltTypeCompiler(compiler.GenericTypeCompiler):
    def visit_REAL(self, type_, **kwargs):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kwargs):
        return "LONG"

    visit_DECIMAL = visit_NUMERIC
    visit_INTEGER = visit_NUMERIC
    visit_SMALLINT = visit_NUMERIC
    visit_BIGINT = visit_NUMERIC
    visit_BOOLEAN = visit_NUMERIC
    visit_TIMESTAMP = visit_NUMERIC
    visit_DATE = visit_NUMERIC

    def visit_CHAR(self, type_, **kwargs):
        return "STRING"

    visit_NCHAR = visit_CHAR
    visit_VARCHAR = visit_CHAR
    visit_NVARCHAR = visit_CHAR
    visit_TEXT = visit_CHAR

    def visit_DATETIME(self, type_, **kwargs):
        return "LONG"

    def visit_TIME(self, type_, **kwargs):
        return "LONG"

    def visit_BLOB(self, type_, **kwargs):
        return "COMPLEX"

    visit_CLOB = visit_BLOB
    visit_NCLOB = visit_BLOB
    visit_VARBINARY = visit_BLOB
    visit_BINARY = visit_BLOB


"""
FireboltDialect defines the behavior of Firebolt database and DB-API combination.
It is responsible for metadata definition and firing queries for receiving Database schema and table information.
"""


# TODO: check dialect attribute values

class FireboltDialect(default.DefaultDialect):
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
    #URL format : firebolt://username:password@host:port/db_name
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
        print("***Inside get_schema_names***")
        # result = connection.execute(
        #     "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.DATABASES"
        # )

        # print("***cursor type***")
        # print(type(result))
        # print("***result data***")
        # print(result)
        # print("***result.rowcount***")
        # print(result.rowcount)
        # print("***exiting schema names***")
        # return result


        print("Type Before execute")
        print(type(connection))
        con = connection.raw_connection()
        result = con.cursor().execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.DATABASES")
        print("Type after execute")
        print(type(result))
        # print(result.returns_rows)
        # print(result.rowcount)
        # return result
        return [
            row for row in result
        ]
        # return [
        #     row.schema_name for row in result
        # ]

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
        return result
        # return [row.table_name for row in result]

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        # TODO: Check alternative for column_default in below query
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
        # y = json.loads(result)
        result = result["data"]
        return [
            {
                "name": row['column_name'],
                "type": type_map[row['data_type'].lower()],
                "nullable": get_is_nullable(row['is_nullable'])
                # "default": get_default(row.COLUMN_DEFAULT),
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


# TODO check if this method is needed
def get_default(firebolt_column_default):
    # currently unused, returns ''
    return str(firebolt_column_default) if firebolt_column_default != "" else None
