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
        "arraysize",
        "rowcount",
    )

    server_side = False

    def __init__(self, adapt_connection):
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.await_ = adapt_connection.await_
        self.arraysize = 1
        self.rowcount = -1
        self.description = None
        self._rows = []

    def close(self):
        self._rows[:] = []

    def execute(self, operation, parameters=None):
        _cursor = self._connection.cursor()
        self.await_(_cursor.execute(operation, parameters))
        if _cursor.description:
            self.description = _cursor.description
            self.rowcount = -1
            self._rows = self.await_(_cursor.fetchall())
        else:
            self.description = None
            self.rowcount = _cursor.rowcount

        _cursor.close()

    def executemany(self, operation, seq_of_parameters):
        _cursor = self._connection.cursor()
        self.await_(_cursor.executemany(operation, seq_of_parameters))
        self.description = None
        self.rowcount = _cursor.rowcount
        _cursor.close()

    def setinputsizes(self, *inputsizes):
        pass

    def __iter__(self):
        while self._rows:
            yield self._rows.pop(0)

    def fetchone(self):
        if self._rows:
            return self._rows.pop(0)
        else:
            return None

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize

        retval = self._rows[0:size]
        self._rows[:] = self._rows[size:]
        return retval

    def fetchall(self):
        retval = self._rows[:]
        self._rows[:] = []
        return retval


class AsyncConnectionWrapper(AdaptedConnection):
    await_ = staticmethod(await_only)
    __slots__ = ("dbapi", "_connection")

    def __init__(self, dbapi, connection):
        self.dbapi = dbapi
        self._connection = connection

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
        self._init_dbapi_attributes()

    def _init_dbapi_attributes(self):
        for name in (
            "DatabaseError",
            "Error",
            "IntegrityError",
            "NotSupportedError",
            "OperationalError",
            "ProgrammingError",
        ):
            setattr(self, name, getattr(self.dbapi, name))

    def connect(self, *arg, **kw):

        connection = self.dbapi.connect(*arg, **kw)
        return AsyncConnectionWrapper(
            self,
            await_only(connection),
        )


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
