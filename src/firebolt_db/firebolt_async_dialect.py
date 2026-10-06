from __future__ import annotations

import asyncio
from functools import partial
from types import ModuleType
from typing import Any, Dict

import firebolt.async_db as async_dbapi
from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
    AsyncAdapt_dbapi_cursor,
)

# Ignoring type since sqlalchemy-stubs doesn't cover AdaptedConnection
# and util.concurrency
from sqlalchemy.pool import AsyncAdaptedQueuePool  # type: ignore[attr-defined]
from sqlalchemy.util.concurrency import await_fallback
from trio import run

from firebolt_db.firebolt_dialect import FireboltDialect


def _is_trio_context() -> bool:
    """Check if we're currently in a Trio async context."""
    try:
        import trio

        trio.lowlevel.current_task()
        return True
    except (ImportError, RuntimeError):
        return False


def _is_asyncio_context() -> bool:
    """Check if we're currently in an asyncio context."""
    try:
        loop = asyncio.get_running_loop()
        return loop.is_running()
    except RuntimeError:
        return False


class AsyncCursorWrapper(AsyncAdapt_dbapi_cursor):
    __slots__ = ()

    server_side = False

    @property
    def _set_parameters(self) -> Dict[str, Any]:
        return self._cursor._set_parameters

    @_set_parameters.setter
    def _set_parameters(self, value: Dict[str, Any]) -> None:
        self._cursor._set_parameters = value

    @property
    def rowcount(self) -> int:
        """Return the rowcount, using memoized value if cursor is closed."""
        # Use hasattr to check if attribute exists and has the value
        memoized = getattr(self, "_soft_closed_memoized", {})
        if "rowcount" in memoized:
            return memoized["rowcount"]  # type: ignore[return-value]
        return self._cursor.rowcount

    async def _async_soft_close(self) -> None:
        """close the cursor but keep the results pending, and memoize the
        description and rowcount.

        Copied from SQLAlchemy's AsyncAdapted_dbapi_connection to use aclose()
        instead of close().

        """
        # Check if cursor can be closed asynchronously
        awaitable_close = getattr(self, "_awaitable_cursor_close", False)
        if not awaitable_close or self.server_side:
            return

        # Get or initialize memoized data
        memoized = getattr(self, "_soft_closed_memoized", set())
        if not isinstance(memoized, dict):
            memoized = {}

        memoized.update(
            {
                "description": self._cursor.description,
                "rowcount": self._cursor.rowcount,  # Memoize rowcount before closing
            }
        )
        setattr(self, "_soft_closed_memoized", memoized)
        await self._cursor.aclose()


class AsyncConnectionWrapper(AsyncAdapt_dbapi_connection):
    _cursor_cls = AsyncCursorWrapper
    __slots__ = ("dbapi", "_connection", "_execute_mutex")

    def rollback(self) -> None:
        pass

    def commit(self) -> None:
        self._connection.commit()

    def close(self) -> None:
        if _is_trio_context() or _is_asyncio_context():
            await_fallback(self._connection.aclose())
        else:
            # Fall back to sync close
            self._connection.close()


class AsyncAPIWrapper:
    """Wrapper around Firebolt async dbapi that returns a similar wrapper for
    Cursor on connect()"""

    def __init__(self, dbapi: ModuleType):
        self.dbapi = dbapi
        self.paramstyle = dbapi.paramstyle  # type: ignore[attr-defined] # noqa: F821
        self._init_dbapi_attributes()
        self.Cursor = AsyncCursorWrapper

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
        """Create a connection, handling both sync and async contexts."""

        # Helper function to create async connection
        async def _create_async_connection() -> Any:
            return await self.dbapi.connect(*arg, **kw)  # type: ignore[attr-defined] # noqa: F821,E501

        # Check if we're in an async context
        if _is_trio_context() or _is_asyncio_context():
            connection = await_fallback(_create_async_connection())
            return AsyncConnectionWrapper(self, connection)

        # No async context detected, use trio.run for synchronous connection creation
        conn_func = partial(self.dbapi.connect, *arg, **kw)  # type: ignore[attr-defined] # noqa: F821,E501
        connection = run(conn_func)
        return AsyncConnectionWrapper(self, connection)


class AsyncFireboltDialect(FireboltDialect):
    driver = "firebolt_aio"
    supports_statement_cache: bool = False
    supports_server_side_cursors: bool = False
    is_async: bool = True
    poolclass = AsyncAdaptedQueuePool

    @classmethod
    def dbapi(cls) -> AsyncAPIWrapper:  # type: ignore[override]
        return AsyncAPIWrapper(async_dbapi)


dialect = AsyncFireboltDialect
