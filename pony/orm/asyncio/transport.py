"""Transport primitives used by Pony's native asyncio providers.

The ORM core consumes rows synchronously after an operation has completed.  Async
drivers, on the other hand, expose driver-specific cursor and result objects.  This
module is the small boundary between the two: providers materialize an
``AsyncResult`` and the shared hydration code consumes a ``MaterializedCursor``.
No driver cursor escapes this module.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class AsyncResult:
    """Driver-neutral result of one database operation."""

    rows: tuple = ()
    description: tuple = ()
    rowcount: int = -1
    generated_id: object = None


class MaterializedCursor:
    """Read-only cursor-shaped view over :class:`AsyncResult`.

    This is an internal compatibility boundary for Pony's existing pure hydration
    routines.  Database drivers themselves are never presented as DB-API cursors.
    """

    __slots__ = ("_connection", "_position", "description", "lastrowid", "rowcount")

    def __init__(self, connection):
        self._connection = connection
        self._position = 0
        self.description = ()
        self.lastrowid = None
        self.rowcount = -1

    def _set_result(self, result):
        self._position = 0
        self._result = result
        self.description = result.description
        self.lastrowid = result.generated_id
        self.rowcount = result.rowcount

    # _result cannot be declared as a class attribute when slots are used on all
    # supported Python versions, so keep it in the connection until first execute.
    @property
    def _result(self):
        return self._connection._cursor_results.get(id(self), AsyncResult())

    @_result.setter
    def _result(self, value):
        self._connection._cursor_results[id(self)] = value

    def execute(self, sql, arguments=None):
        self._set_result(self._connection._execute(sql, arguments))
        return self

    def executemany(self, sql, arguments):
        self._set_result(self._connection._execute_many(sql, arguments))
        return self

    def fetchone(self):
        rows = self._result.rows
        if self._position >= len(rows):
            return None
        row = rows[self._position]
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
        self._connection._cursor_results.pop(id(self), None)


class BridgedConnection:
    """Synchronous entry point used only inside Pony's ORM worker context."""

    __slots__ = ("_cursor_results", "_pool", "_raw", "server_version")

    def __init__(self, pool, raw, server_version=None):
        self._pool = pool
        self._raw = raw
        self._cursor_results = {}
        self.server_version = server_version

    def cursor(self):
        return MaterializedCursor(self)

    def _execute(self, sql, arguments=None):
        return self._pool._bridge(self._pool.execute(self._raw, sql, arguments))

    def _execute_many(self, sql, arguments):
        return self._pool._bridge(self._pool.executemany(self._raw, sql, arguments))

    def commit(self):
        return self._pool._bridge(self._pool.commit(self._raw))

    def rollback(self):
        return self._pool._bridge(self._pool.rollback(self._raw))

    def close(self):
        return self._pool._bridge(self._pool.drop(self._raw))


class BridgedPool:
    """Pool facade consumed by the existing provider/session machinery."""

    def __init__(self, async_pool, bridge):
        self.async_pool = async_pool
        self._bridge = bridge

    def connect(self):
        raw = self._bridge(self.async_pool.acquire())
        version = getattr(self.async_pool, "server_version", None)
        return BridgedConnection(self.async_pool, raw, version), False

    def release(self, connection):
        return self._bridge(self.async_pool.release(connection._raw))

    def drop(self, connection):
        return self._bridge(self.async_pool.drop(connection._raw))

    def disconnect(self):
        return self._bridge(self.async_pool.close())
