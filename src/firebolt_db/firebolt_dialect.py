import os
from distutils.util import strtobool
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple, Union

import firebolt.db as dbapi
import sqlalchemy.types as sqltypes
from firebolt.db import Cursor
from sqlalchemy.engine import Connection as AlchemyConnection
from sqlalchemy.engine import ExecutionContext, default
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import compiler, text
from sqlalchemy.types import (
    BIGINT,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    FLOAT,
    INTEGER,
    TIMESTAMP,
    VARCHAR,
)


class ARRAY(sqltypes.TypeEngine):
    __visit_name__ = "ARRAY"


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


class UniversalSet(set):
    def __contains__(self, item: Any) -> bool:
        return True


class FireboltIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = UniversalSet()


class FireboltCompiler(compiler.SQLCompiler):
    pass


class FireboltTypeCompiler(compiler.GenericTypeCompiler):
    def visit_ARRAY(self, type: sqltypes.TypeEngine, **kw: int) -> str:
        return "Array(%s)" % type


class FireboltDialect(default.DefaultDialect):
    """
    FireboltDialect defines the behavior of Firebolt database and DB-API combination.
    It is responsible for metadata definition and firing queries for receiving Database
    schema and table information.
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
    _set_parameters: Optional[Dict[str, Any]] = None

    def __init__(
        self, context: Optional[ExecutionContext] = None, *args: Any, **kwargs: Any
    ):
        super(FireboltDialect, self).__init__(*args, **kwargs)
        self.context: Union[ExecutionContext, Dict] = context or {}

    @classmethod
    def dbapi(cls) -> ModuleType:
        return dbapi

    # Build firebolt-sdk compatible connection arguments.
    # URL format : firebolt://username:password@host:port/db_name
    def create_connect_args(self, url: URL) -> Tuple[List, Dict]:
        kwargs = {
            "database": url.host or None,
            "username": url.username or None,
            "password": url.password or None,
            "engine_name": url.database,
        }
        parameters = dict(url.query)
        if "account_name" in parameters:
            kwargs["account_name"] = parameters.pop("account_name")
        if "use_token_cache" in parameters:
            # parameters are all passed as a string, we need to convert it
            # to boolean for SDK compatibility
            kwargs["use_token_cache"] = bool(
                strtobool(parameters.pop("use_token_cache"))
            )
        self._set_parameters = parameters
        # If URL override is not provided leave it to the sdk to determine the endpoint
        if "FIREBOLT_BASE_URL" in os.environ:
            kwargs["api_endpoint"] = os.environ["FIREBOLT_BASE_URL"]
        return ([], kwargs)

    def get_schema_names(
        self, connection: AlchemyConnection, **kwargs: Any
    ) -> List[str]:
        query = "select schema_name from information_schema.databases"
        result = connection.execute(text(query))
        return [row.schema_name for row in result]

    def has_table(
        self,
        connection: AlchemyConnection,
        table_name: str,
        schema: Optional[str] = None,
    ) -> bool:
        query = """
            select count(*) > 0 as exists_
              from information_schema.tables
             where table_name = '{table_name}'
        """.format(
            table_name=table_name
        )
        result = connection.execute(text(query))
        return result.fetchone().exists_

    def get_table_names(
        self, connection: AlchemyConnection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        query = "select table_name from information_schema.tables"
        if schema:
            query = "{query} where table_schema = '{schema}'".format(
                query=query, schema=schema
            )

        result = connection.execute(text(query))
        return [row.table_name for row in result]

    def get_view_names(
        self, connection: AlchemyConnection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        return []

    def get_table_options(
        self,
        connection: AlchemyConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any
    ) -> Dict:
        return {}

    def get_columns(
        self,
        connection: AlchemyConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any
    ) -> List[Dict]:
        query = """
            select column_name,
                   data_type,
                   is_nullable
              from information_schema.columns
             where table_name = '{table_name}'
        """.format(
            table_name=table_name
        )
        if schema:
            query = "{query} and table_schema = '{schema}'".format(
                query=query, schema=schema
            )

        result = connection.execute(text(query))

        return [
            {
                "name": row[0],
                "type": type_map[row[1].lower()],
                "nullable": get_is_nullable(row[2]),
                "default": None,
            }
            for row in result
        ]

    def get_pk_constraint(
        self,
        connection: AlchemyConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any
    ) -> Dict:
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(
        self,
        connection: AlchemyConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any
    ) -> List[Dict]:
        return []

    def get_check_constraints(
        self,
        connection: AlchemyConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any
    ) -> List[Dict]:
        return []

    def get_table_comment(
        self,
        connection: AlchemyConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any
    ) -> Dict:
        return {"text": ""}

    def get_indexes(
        self,
        connection: AlchemyConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any
    ) -> List[Dict]:
        return []

    def get_unique_constraints(
        self,
        connection: AlchemyConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any
    ) -> List[Dict]:
        return []

    def get_view_definition(
        self,
        connection: AlchemyConnection,
        view_name: str,
        schema: Optional[str] = None,
        **kwargs: Any
    ) -> str:
        pass

    def do_execute(
        self,
        cursor: Cursor,
        statement: str,
        parameters: Tuple[str, Any],
        context: Optional[ExecutionContext] = None,
    ) -> None:
        cursor.execute(
            statement, parameters=parameters, set_parameters=self._set_parameters
        )

    def do_rollback(self, dbapi_connection: AlchemyConnection) -> None:
        pass

    def _check_unicode_returns(
        self,
        connection: AlchemyConnection,
        additional_tests: Optional[Any] = None,
    ) -> bool:
        """
        This might be redundant
        https://gerrit.sqlalchemy.org/c/sqlalchemy/sqlalchemy/+/1946
        """
        return True

    def _check_unicode_description(self, connection: AlchemyConnection) -> bool:
        """
        Same as _check_unicode_returns this might be redundant as there's
        no reference to it in the sqlalchemy repo.
        """
        return True

    def do_commit(self, dbapi_connection: AlchemyConnection) -> None:
        pass


dialect = FireboltDialect


def get_is_nullable(column_is_nullable: int) -> bool:
    return column_is_nullable == 1
