from __future__ import annotations

from asyncio import Lock
from types import ModuleType
from typing import Any, Dict, Iterator, List, Optional, Tuple

import firebolt.async_db as async_dbapi
from firebolt.async_db import Connection

# Ignoring type since sqlalchemy-stubs doesn't cover AdaptedConnection
# and util.concurrency
from sqlalchemy.engine import AdaptedConnection  # type: ignore[attr-defined]
from sqlalchemy.util.concurrency import await_only  # type: ignore[import]

from firebolt_db.firebolt_dialect import FireboltDialect


class AsyncCursorWrapper:
    __slots__ = (
        "_adapt_connection",
        "_connection",
        "await_",
        "_cursor",
        "_rows",
    )

    server_side = False

    def __init__(self, adapt_connection: AsyncConnectionWrapper):
        self._adapt_connection = adapt_connection
        self._connection = adapt_connection._connection
        self.await_ = adapt_connection.await_
        self._rows: List[List] = []
        self._cursor = self._connection.cursor()

    def close(self) -> None:
        self._rows[:] = []
        self._cursor.close()

    @property
    def description(self) -> str:
        return self._cursor.description

    @property
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._cursor.arraysize = value

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    def execute(
        self,
        operation: str,
        parameters: Optional[Tuple] = None,
    ) -> None:
        self.await_(self._execute(operation, parameters))

    async def _execute(
        self,
        operation: str,
        parameters: Optional[Tuple] = None,
    ) -> None:
        async with self._adapt_connection._execute_mutex:
            await self._cursor.execute(operation, parameters)
            if self._cursor.description:
                self._rows = await self._cursor.fetchall()
            else:
                self._rows = []

    def executemany(self, operation: str, seq_of_parameters: List[Tuple]) -> None:
        raise NotImplementedError("executemany is not supported yet")

    def __iter__(self) -> Iterator[List]:
        while self._rows:
            yield self._rows.pop(0)

    def fetchone(self) -> Optional[List]:
        if self._rows:
            return self._rows.pop(0)
        else:
            return None

    def fetchmany(self, size: int = None) -> List[List]:
        if size is None:
            size = self._cursor.arraysize

        retval = self._rows[0:size]
        self._rows[:] = self._rows[size:]
        return retval

    def fetchall(self) -> List[List]:
        retval = self._rows[:]
        self._rows[:] = []
        return retval

    @property
    def _set_parameters(self) -> Dict[str, Any]:
        return self._cursor._set_parameters

    @_set_parameters.setter
    def _set_parameters(self, value: Dict[str, Any]) -> None:
        self._cursor._set_parameters = value


class AsyncConnectionWrapper(AdaptedConnection):
    await_ = staticmethod(await_only)
    __slots__ = ("dbapi", "_connection", "_execute_mutex")

    def __init__(self, dbapi: AsyncAPIWrapper, connection: Connection):
        self.dbapi = dbapi
        self._connection = connection
        self._execute_mutex = Lock()

    def cursor(self) -> AsyncCursorWrapper:
        return AsyncCursorWrapper(self)

    def rollback(self) -> None:
        pass

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        self.await_(self._connection._aclose())


class AsyncAPIWrapper(ModuleType):
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

        connection = await_only(self.dbapi.connect(*arg, **kw))  # type: ignore[attr-defined] # noqa: F821,E501
        return AsyncConnectionWrapper(
            self,
            connection,
        )


class AsyncFireboltDialect(FireboltDialect):
    driver = "firebolt_aio"
    supports_statement_cache: bool = False
    supports_server_side_cursors: bool = False
    is_async: bool = True

    @classmethod
    def dbapi(cls) -> AsyncAPIWrapper:
        return AsyncAPIWrapper(async_dbapi)


dialect = AsyncFireboltDialect
