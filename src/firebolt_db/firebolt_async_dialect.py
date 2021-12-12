import firebolt.async_db as async_dbapi
from sqlalchemy.engine import AdaptedConnection, default
from sqlalchemy.util.concurrency import await_only

from firebolt_db.firebolt_dialect import FireboltDialect


class AsyncCursorWrapper:
    __slots__ = (
        "_adapt_connection",
        "_connection",
        "description",
        "await_",
        "_cursor",
        "_rows",
        # "arraysize",
        # "rowcount",
    )

    server_side = True

    def __init__(self, adapt_connection):
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.await_ = adapt_connection.await_
        # self.arraysize = 1
        # self.rowcount = -1
        self.description = None
        self._rows = []
        cursor = self._connection.cursor()
        self._cursor = cursor
        # self._cursor = self.await_(cursor.__enter__())
        # self._cursor = self.await_(self._connection.cursor())
        print(self._cursor)

    @property  # type: ignore
    def rowcount(self) -> int:
        """The number of rows produced by last query."""
        print(self._cursor.rowcount)
        return self._cursor.rowcount

    def close(self):
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None

    def execute(self, operation, parameters=None):
        result = self.await_(self._cursor.execute(operation, parameters))
        if self._cursor.description:
            self.description = self._cursor.description
            # self.lastrowid = self.rowcount = -1
        return result

    def executemany(self, operation, seq_of_parameters):
        return self.await_(self._cursor.executemany(operation, seq_of_parameters))

    def setinputsizes(self, *inputsizes):
        pass

    def __iter__(self):
        return self._cursor.__aiter__()

    #     while self._rows:
    #         yield self._rows.pop(0)

    def fetchone(self):
        return self.await_(self._cursor.fetchone())

    def fetchmany(self, size=None):
        return self.await_(self._cursor.fetchmany(size=size))

    def fetchall(self):
        # print(self._cursor)
        print("In my fetchall")
        return self.await_(self._cursor.fetchall())
        # res = self._cursor.fetchall()
        # print(res)
        # print(type(res))
        # return res


class AsyncConnectionWrapper(AdaptedConnection):
    await_ = staticmethod(await_only)
    __slots__ = ("dbapi", "_connection")

    def __init__(self, dbapi, connection):
        self.dbapi = dbapi
        self._connection = connection

    # @property
    # def isolation_level(self):
    #     return self._connection.isolation_level

    # @isolation_level.setter
    # def isolation_level(self, value):
    #     self._connection.isolation_level = value

    def create_function(self, *args, **kw):
        self.await_(self._connection.create_function(*args, **kw))

    def cursor(self, server_side=False):
        return AsyncCursorWrapper(self)

    def rollback(self):
        self.await_(self._connection.rollback())

    def commit(self):
        self.await_(self._connection.commit())

    def close(self):
        self.await_(self._connection._aclose())


class AsyncAPIWrapper:
    """Wrapper around Firebolt async dbapi that returns a similar wrapper for
    Cursor on connect()"""

    def __init__(self, dbapi):
        self.dbapi = dbapi
        self.paramstyle = dbapi.paramstyle

    def connect(self, *arg, **kw):

        connection = self.dbapi.connect(*arg, **kw)
        return AsyncConnectionWrapper(
            self,
            await_only(connection),
        )

    # TODO: this might need to have Error class defined


class MySQLExecutionContext_mysqldb(default.DefaultExecutionContext):
    @property
    def rowcount(self):
        if hasattr(self, "_rowcount"):
            return self._rowcount
        else:
            return self.cursor.rowcount


class AsyncFireboltDialect(FireboltDialect):
    driver = "firebolt_aio"
    supports_statement_cache = False
    supports_server_side_cursors = True
    is_async = True

    execution_ctx_cls = MySQLExecutionContext_mysqldb

    @classmethod
    def dbapi(cls):
        return AsyncAPIWrapper(async_dbapi)

    def get_driver_connection(self, connection):
        return connection._connection


dialect = AsyncFireboltDialect
