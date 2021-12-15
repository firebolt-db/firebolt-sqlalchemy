from __future__ import annotations

from types import ModuleType
from typing import Any, Iterator, List, Optional, Tuple

import firebolt.async_db as async_dbapi
from firebolt.async_db import Connection
from sqlalchemy.engine import AdaptedConnection  # type: ignore[attr-defined]
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

    def __init__(self, adapt_connection: AsyncConnectionWrapper):
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.await_ = adapt_connection.await_
        self.arraysize = 1
        self.rowcount = -1
        self.description = None
        self._rows: List[Tuple] = []

    def close(self) -> None:
        self._rows[:] = []

    def execute(self, operation: str, parameters: Optional[Tuple] = None) -> None:
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

    def executemany(self, operation: str, seq_of_parameters: List[Tuple]) -> None:
        raise NotImplementedError("executemany is not supported yet")

    def __iter__(self) -> Iterator[Tuple]:
        while self._rows:
            yield self._rows.pop(0)

    def fetchone(self) -> Optional[Tuple]:
        if self._rows:
            return self._rows.pop(0)
        else:
            return None

    def fetchmany(self, size: int = None) -> List[Tuple]:
        if size is None:
            size = self.arraysize

        retval = self._rows[0:size]
        self._rows[:] = self._rows[size:]
        return retval

    def fetchall(self) -> List[Tuple]:
        retval = self._rows[:]
        self._rows[:] = []
        return retval


class AsyncConnectionWrapper(AdaptedConnection):
    await_ = staticmethod(await_only)
    __slots__ = ("dbapi", "_connection")

    def __init__(self, dbapi: AsyncAPIWrapper, connection: Connection):
        self.dbapi = dbapi
        self._connection = connection

    def cursor(self) -> AsyncCursorWrapper:
        return AsyncCursorWrapper(self)

    def rollback(self) -> None:
        pass

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self.await_(self._connection._aclose())


class AsyncAPIWrapper:
    """Wrapper around Firebolt async dbapi that returns a similar wrapper for
    Cursor on connect()"""

    def __init__(self, dbapi: ModuleType):
        self.dbapi = dbapi
        self.paramstyle = dbapi.paramstyle  # type: ignore[attr-defined] # noqa: F821
        self._init_dbapi_attributes()

    def _init_dbapi_attributes(self) -> None:
        for name in (
            "DatabaseError",
            "Error",
            "IntegrityError",
            "NotSupportedError",
            "OperationalError",
            "ProgrammingError",
        ):
            setattr(self, name, getattr(self.dbapi, name))

    def connect(self, *arg: Any, **kw: Any) -> AsyncConnectionWrapper:

        connection = self.dbapi.connect(*arg, **kw)  # type: ignore[attr-defined] # noqa: F821,E501
        return AsyncConnectionWrapper(
            self,
            await_only(connection),
        )


class AsyncFireboltDialect(FireboltDialect):
    driver = "firebolt_aio"
    supports_statement_cache = False
    supports_server_side_cursors = True
    is_async = True

    @classmethod
    def dbapi(cls) -> Any:
        return AsyncAPIWrapper(async_dbapi)


dialect = AsyncFireboltDialect