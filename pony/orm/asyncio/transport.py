"""Async transport primitives shared by Pony's native asyncio providers.

The ORM compiler, identity map and hydration machinery are deliberately kept
synchronous.  ``run_sync`` executes that CPU-only code in the event-loop thread
and suspends its complete call stack whenever a provider reaches ``await_io``.
Unlike the old implementation, no executor thread waits for database I/O and no
coroutine is synchronously submitted back to the event loop.
"""

import contextvars
import inspect
import sys
from dataclasses import dataclass

from greenlet import getcurrent, greenlet


@dataclass(frozen=True)
class AsyncResult:
    """Driver-neutral, fully materialized result of one database operation."""

    rows: tuple = ()
    description: tuple = ()
    rowcount: int = -1
    generated_id: object = None


class MaterializedCursor:
    """Read-only cursor view consumed by Pony's synchronous hydration code."""

    __slots__ = (
        "connection",
        "description",
        "lastrowid",
        "rowcount",
        "_position",
        "_result",
    )

    def __init__(self, connection):
        self.connection = connection
        self.description = ()
        self.lastrowid = None
        self.rowcount = -1
        self._position = 0
        self._result = AsyncResult()

    def set_result(self, result):
        self._result = result
        self._position = 0
        self.description = result.description
        self.lastrowid = result.generated_id
        self.rowcount = result.rowcount
        return self

    # A few schema/introspection routines still issue SQL through a cursor.  The
    # operation nevertheless awaits the provider directly through the same
    # stack-switching runtime; no DB-API driver cursor is exposed here.
    def execute(self, sql, arguments=None):
        self.connection.provider.execute_cursor(self, sql, arguments)
        return self

    def executemany(self, sql, arguments):
        self.connection.provider.executemany_cursor(self, sql, arguments)
        return self

    def fetchone(self):
        if self._position >= len(self._result.rows):
            return None
        row = self._result.rows[self._position]
        self._position += 1
        return row

    def fetchmany(self, size=None):
        if size is None:
            size = 1
        start = self._position
        stop = min(start + size, len(self._result.rows))
        self._position = stop
        return list(self._result.rows[start:stop])

    def fetchall(self):
        rows = self._result.rows[self._position :]
        self._position = len(self._result.rows)
        return list(rows)

    def close(self):
        self._result = AsyncResult()


class AsyncConnection:
    """Connection token used by the ORM core; ``raw`` stays provider-private."""

    __slots__ = (
        "provider",
        "raw",
        "server_version",
        "transaction",
        "released",
        "invalidated",
        "readonly",
    )

    def __init__(self, provider, raw, server_version=None):
        self.provider = provider
        self.raw = raw
        self.server_version = server_version
        self.transaction = None
        self.released = False
        self.invalidated = False
        self.readonly = False

    def cursor(self):
        return MaterializedCursor(self)

    def commit(self):
        return await_io(self.provider.commit_async(self))

    def rollback(self):
        return await_io(self.provider.rollback_async(self))

    def close(self):
        self.invalidated = True
        return await_io(self.provider.drop_async(self))


class _AsyncIoGreenlet(greenlet):
    pony_async_runtime = True

    def __init__(self, function, parent, before_await=None, after_await=None):
        super().__init__(function, parent=parent)
        self.before_await = before_await
        self.after_await = after_await
        # greenlet context propagation is explicit.  Sharing the driver's
        # context lets Pony's ContextVars retain task/session identity.
        if hasattr(parent, "gr_context"):
            self.gr_context = contextvars.copy_context()


def await_io(awaitable):
    """Suspend synchronous ORM code until *awaitable* completes.

    This function is valid only inside :func:`run_sync`.  It restores Pony's
    thread-local compatibility state before returning control to the event loop,
    then reapplies the owning session state when the operation resumes.
    """

    current = getcurrent()
    if not getattr(current, "pony_async_runtime", False):
        if inspect.iscoroutine(awaitable):
            awaitable.close()
        raise RuntimeError("Async database I/O was attempted outside Pony async runtime")
    before_await = current.before_await
    after_await = current.after_await
    if before_await is not None:
        before_await()
    try:
        return current.parent.switch(awaitable)
    finally:
        if after_await is not None:
            after_await()


async def run_sync(function, *args, before_await=None, after_await=None, **kwargs):
    """Run synchronous ORM machinery while natively awaiting provider I/O."""

    def invoke():
        return function(*args, **kwargs)

    child = _AsyncIoGreenlet(
        invoke, getcurrent(), before_await=before_await, after_await=after_await
    )
    result = child.switch()
    while not child.dead:
        try:
            value = await result
        except BaseException:
            result = child.throw(*sys.exc_info())
        else:
            result = child.switch(value)
    return result
