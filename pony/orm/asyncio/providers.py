"""Native asyncio transports and dialect adapters for SQLite and PostgreSQL."""

import asyncio
import os
import re
import sqlite3
from dataclasses import asdict, dataclass
from time import monotonic
from types import SimpleNamespace

from pony.orm import core, dbapiprovider
from pony.orm.asyncio.transport import AsyncConnection, AsyncResult, await_io
from pony.orm.dbproviders import sqlite as sqlite_provider


class AsyncPoolError(core.TransactionError):
    """Base class for bounded async pool failures."""


class AsyncPoolTimeout(AsyncPoolError):
    """The pool did not provide a connection before the acquire deadline."""


class AsyncPoolQueueFull(AsyncPoolError):
    """The configured number of connection waiters was exceeded."""


class AsyncQueryTimeout(core.TransactionError):
    """A database operation exceeded its query timeout."""


@dataclass
class PoolMetrics:
    max_size: int
    max_waiters: int
    open_connections: int = 0
    checked_out: int = 0
    waiting: int = 0
    total_acquires: int = 0
    total_timeouts: int = 0
    total_queue_full: int = 0
    total_dropped: int = 0
    acquire_wait_seconds: float = 0.0

    def snapshot(self):
        result = asdict(self)
        result["available"] = max(0, self.open_connections - self.checked_out)
        return result


async def _wait(awaitable, timeout, timeout_error):
    if timeout is None:
        return await awaitable
    try:
        return await asyncio.wait_for(awaitable, timeout)
    except asyncio.TimeoutError:
        raise timeout_error from None


def _positive_float(value, name, allow_none=True):
    if value is None and allow_none:
        return None
    value = float(value)
    if value <= 0:
        raise ValueError("%s must be positive" % name)
    return value


def _positive_int(value, name, allow_zero=False):
    value = int(value)
    if value < (0 if allow_zero else 1):
        raise ValueError("%s must be %s" % (
            name, "non-negative" if allow_zero else "positive"
        ))
    return value


def _description(names):
    return tuple((name, None, None, None, None, None, None) for name in names)


class _BoundedPool:
    """Admission control shared by the SQLite and asyncpg pools."""

    def __init__(self, max_size, max_waiters, acquire_timeout, shutdown_timeout):
        self.max_size = _positive_int(max_size, "max_pool_size")
        self.max_waiters = _positive_int(max_waiters, "max_waiters", allow_zero=True)
        self.acquire_timeout = _positive_float(acquire_timeout, "acquire_timeout")
        self.shutdown_timeout = _positive_float(shutdown_timeout, "shutdown_timeout")
        self.metrics = PoolMetrics(self.max_size, self.max_waiters)
        self._capacity = asyncio.Semaphore(self.max_size)
        self._closed = False
        self._drained = asyncio.Event()
        self._drained.set()

    async def _admit(self):
        if self._closed:
            raise core.ConnectionClosedError("Async database pool is closed")
        queued = False
        if self._capacity.locked():
            if self.metrics.waiting >= self.max_waiters:
                self.metrics.total_queue_full += 1
                raise AsyncPoolQueueFull(
                    "Async connection pool queue is full (max_waiters=%d)"
                    % self.max_waiters
                )
            self.metrics.waiting += 1
            queued = True
        started = monotonic()
        try:
            try:
                await _wait(
                    self._capacity.acquire(),
                    self.acquire_timeout,
                    AsyncPoolTimeout(
                        "Timed out waiting for an async database connection"
                    ),
                )
            except AsyncPoolTimeout:
                self.metrics.total_timeouts += 1
                raise
        finally:
            if queued:
                self.metrics.waiting -= 1
            self.metrics.acquire_wait_seconds += monotonic() - started
        if self._closed:
            self._capacity.release()
            raise core.ConnectionClosedError("Async database pool is closed")
        self.metrics.checked_out += 1
        self.metrics.total_acquires += 1
        self._drained.clear()

    def _returned(self):
        if self.metrics.checked_out:
            self.metrics.checked_out -= 1
            self._capacity.release()
        if not self.metrics.checked_out:
            self._drained.set()

    async def _wait_until_drained(self):
        if not self.metrics.checked_out:
            return True
        try:
            await _wait(
                self._drained.wait(), self.shutdown_timeout, asyncio.TimeoutError()
            )
        except asyncio.TimeoutError:
            return False
        return True


class _SQLitePool(_BoundedPool):
    def __init__(
        self,
        database,
        driver,
        filename,
        max_size,
        max_waiters,
        connect_timeout,
        acquire_timeout,
        query_timeout,
        shutdown_timeout,
        connect_kwargs,
    ):
        super().__init__(max_size, max_waiters, acquire_timeout, shutdown_timeout)
        self.database = database
        self.driver = driver
        self.filename = filename
        self.connect_timeout = _positive_float(connect_timeout, "connect_timeout")
        self.query_timeout = _positive_float(query_timeout, "query_timeout")
        self.connect_kwargs = connect_kwargs
        self._available = asyncio.LifoQueue()
        self._connections = set()

    async def start(self):
        connection = await self.acquire()
        await self.release(connection)

    async def _open(self):
        connection = await _wait(
            self.driver.connect(
                self.filename, isolation_level=None, **self.connect_kwargs
            ),
            self.connect_timeout,
            AsyncPoolTimeout("Timed out connecting to SQLite"),
        )
        self._connections.add(connection)
        self.metrics.open_connections = len(self._connections)
        try:
            connection.text_factory = sqlite_provider._text_factory
            functions = (
                ("power", 2, pow),
                ("rand", 0, sqlite_provider.random),
                ("py_upper", 1, sqlite_provider.py_upper),
                ("py_lower", 1, sqlite_provider.py_lower),
                ("py_json_unwrap", 1, sqlite_provider.py_json_unwrap),
                ("py_json_extract", -1, sqlite_provider.py_json_extract),
                ("py_json_contains", 3, sqlite_provider.py_json_contains),
                ("py_json_nonzero", 2, sqlite_provider.py_json_nonzero),
                ("py_json_array_length", -1, sqlite_provider.py_json_array_length),
                ("py_array_index", 2, sqlite_provider.py_array_index),
                ("py_array_contains", 2, sqlite_provider.py_array_contains),
                ("py_array_subset", 2, sqlite_provider.py_array_subset),
                ("py_array_length", 1, sqlite_provider.py_array_length),
                ("py_array_slice", 3, sqlite_provider.py_array_slice),
                ("py_make_array", -1, sqlite_provider.py_make_array),
                ("py_string_slice", 3, sqlite_provider.py_string_slice),
            )
            for name, num_params, function in functions:
                await connection.create_function(name, num_params, function)
            await connection.execute("PRAGMA foreign_keys = true")
            await connection.execute("PRAGMA case_sensitive_like = true")
            await self.database._run_on_connect(connection, "sqlite")
        except BaseException:
            self._connections.discard(connection)
            self.metrics.open_connections = len(self._connections)
            await connection.close()
            raise
        return connection

    async def acquire(self):
        await self._admit()
        try:
            try:
                return self._available.get_nowait()
            except asyncio.QueueEmpty:
                return await self._open()
        except BaseException:
            self._returned()
            raise

    async def release(self, connection):
        try:
            if self._closed:
                await asyncio.shield(self.drop(connection))
            else:
                try:
                    await connection.rollback()
                except BaseException:
                    try:
                        await asyncio.shield(self.drop(connection))
                    finally:
                        raise
                else:
                    self._available.put_nowait(connection)
        finally:
            self._returned()

    async def drop(self, connection):
        if connection in self._connections:
            self._connections.discard(connection)
            self.metrics.open_connections = len(self._connections)
            self.metrics.total_dropped += 1
            await connection.close()

    async def close(self):
        self._closed = True
        await self._wait_until_drained()
        connections = tuple(self._connections)
        self._connections.clear()
        self.metrics.open_connections = 0
        while not self._available.empty():
            self._available.get_nowait()
        for connection in connections:
            try:
                await connection.close()
            except BaseException:
                pass

    async def execute(self, connection, sql, arguments=None):
        async def operation():
            if arguments is None:
                cursor = await connection.execute(sql)
            else:
                cursor = await connection.execute(sql, arguments)
            try:
                description = tuple(cursor.description or ())
                rows = tuple(tuple(row) for row in await cursor.fetchall()) if description else ()
                return AsyncResult(
                    rows=rows,
                    description=description,
                    rowcount=cursor.rowcount,
                    generated_id=cursor.lastrowid,
                )
            finally:
                await cursor.close()

        started = monotonic()
        outcome = "ok"
        try:
            return await _wait(
                operation(), self.query_timeout,
                AsyncQueryTimeout("SQLite query timed out")
            )
        except AsyncQueryTimeout:
            outcome = "timeout"
            self.database._record_cancellation(timed_out=True)
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            self.database._record_cancellation()
            raise
        except BaseException:
            outcome = "error"
            raise
        finally:
            if self.database is not None:
                self.database._record_query(sql, monotonic() - started, outcome)

    async def executemany(self, connection, sql, arguments):
        async def operation():
            cursor = await connection.executemany(sql, arguments)
            try:
                return AsyncResult(
                    rowcount=cursor.rowcount, generated_id=cursor.lastrowid
                )
            finally:
                await cursor.close()

        started = monotonic()
        outcome = "ok"
        try:
            return await _wait(
                operation(), self.query_timeout,
                AsyncQueryTimeout("SQLite query timed out")
            )
        except AsyncQueryTimeout:
            outcome = "timeout"
            self.database._record_cancellation(timed_out=True)
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            self.database._record_cancellation()
            raise
        except BaseException:
            outcome = "error"
            raise
        finally:
            if self.database is not None:
                self.database._record_query(sql, monotonic() - started, outcome)


class _NativeProviderMixin:
    pool_transport = None

    @dbapiprovider.wrap_dbapi_exceptions
    def connect(self):
        raw = await_io(self.pool_transport.acquire())
        return AsyncConnection(self, raw, getattr(self, "server_version", None)), False

    @dbapiprovider.wrap_dbapi_exceptions
    def execute(self, cursor, sql, arguments=None, returning_id=False):
        if type(arguments) is list:
            result = await_io(self.executemany_async(cursor.connection, sql, arguments))
        else:
            result = await_io(self.execute_async(cursor.connection, sql, arguments))
        cursor.set_result(result)
        return result.generated_id if returning_id else None

    @dbapiprovider.wrap_dbapi_exceptions
    def execute_cursor(self, cursor, sql, arguments=None):
        result = await_io(self.execute_async(cursor.connection, sql, arguments))
        cursor.set_result(result)

    @dbapiprovider.wrap_dbapi_exceptions
    def executemany_cursor(self, cursor, sql, arguments):
        result = await_io(self.executemany_async(cursor.connection, sql, arguments))
        cursor.set_result(result)

    async def execute_async(self, connection, sql, arguments=None):
        try:
            return await self.pool_transport.execute(connection.raw, sql, arguments)
        except BaseException:
            is_closed = getattr(connection.raw, "is_closed", None)
            if is_closed is not None and is_closed():
                connection.invalidated = True
            raise

    async def executemany_async(self, connection, sql, arguments):
        try:
            return await self.pool_transport.executemany(
                connection.raw, sql, arguments
            )
        except BaseException:
            is_closed = getattr(connection.raw, "is_closed", None)
            if is_closed is not None and is_closed():
                connection.invalidated = True
            raise

    @dbapiprovider.wrap_dbapi_exceptions
    def commit(self, connection, cache=None):
        await_io(self.commit_async(connection))
        if cache is not None:
            cache.in_transaction = False

    async def commit_async(self, connection):
        raise NotImplementedError

    @dbapiprovider.wrap_dbapi_exceptions
    def rollback(self, connection, cache=None):
        await_io(self.rollback_async(connection))
        if cache is not None:
            cache.in_transaction = False

    async def rollback_async(self, connection):
        raise NotImplementedError

    @dbapiprovider.wrap_dbapi_exceptions
    def release(self, connection, cache=None):
        await_io(self.release_async(connection))

    async def release_async(self, connection):
        if connection.released:
            return
        connection.released = True
        if connection.invalidated:
            try:
                await self.pool_transport.drop(connection.raw)
            finally:
                self.pool_transport._returned()
        else:
            await self.pool_transport.release(connection.raw)

    @dbapiprovider.wrap_dbapi_exceptions
    def drop(self, connection, cache=None):
        connection.invalidated = True
        await_io(self.drop_async(connection))

    async def drop_async(self, connection):
        if connection.released:
            return
        connection.released = True
        try:
            await self.pool_transport.drop(connection.raw)
        finally:
            # ``drop`` is called while a pool permit is checked out.  Provider
            # pools release that permit from their public release path, so
            # explicitly mark it returned for direct invalidation.
            self.pool_transport._returned()

    @dbapiprovider.wrap_dbapi_exceptions
    def disconnect(self):
        await_io(self.async_disconnect())

    async def async_disconnect(self):
        if self.pool_transport is not None:
            await self.pool_transport.close()

    def should_reconnect(self, exc):
        # Replaying arbitrary ORM SQL after a lost connection is unsafe.  Retry
        # is performed only by the outer db_session retry contract.
        return False


class AsyncSQLiteProvider(_NativeProviderMixin, sqlite_provider.SQLiteProvider):
    """SQLite dialect backed by :mod:`aiosqlite`."""

    def __init__(self, database, filename, **kwargs):
        self.database = database
        self.pre_transaction_lock = sqlite_provider.Lock()
        self.transaction_lock = sqlite_provider.Lock()
        self.local_exceptions = sqlite_provider.local_exceptions
        self.json1_available = True

        is_shared_memory = filename == ":sharedmemory:"
        if is_shared_memory:
            filename = "file:memdb%d_%s?mode=memory&cache=shared" % (
                database.id, os.urandom(8).hex()
            )
            kwargs["uri"] = True
        if filename not in (":memory:",) and not is_shared_memory:
            filename = os.path.abspath(filename)
            create_db = kwargs.pop("create_db", False)
            if not create_db and not os.path.exists(filename):
                raise OSError("Database file is not found: %r" % filename)
        else:
            kwargs.pop("create_db", None)
        self.filename = filename
        self.is_shared_memory = is_shared_memory
        max_size = kwargs.pop("max_pool_size", kwargs.pop("pool_size", 5))
        if filename == ":memory:" or is_shared_memory:
            max_size = 1
        self.pool_options = dict(
            max_size=max_size,
            max_waiters=kwargs.pop("max_waiters", int(max_size) * 4),
            connect_timeout=kwargs.pop("connect_timeout", 10.0),
            acquire_timeout=kwargs.pop("acquire_timeout", 30.0),
            query_timeout=kwargs.pop("query_timeout", None),
            shutdown_timeout=kwargs.pop("shutdown_timeout", 30.0),
        )
        self.connect_kwargs = kwargs
        self.pool = self.pool_transport = None

    async def initialize(self):
        try:
            import aiosqlite
        except ImportError:
            raise ImportError(
                "Async SQLite support requires aiosqlite; install pony[async-sqlite]"
            ) from None
        pool = _SQLitePool(
            self.database, aiosqlite, self.filename,
            connect_kwargs=self.connect_kwargs, **self.pool_options
        )
        self.pool = self.pool_transport = pool
        await pool.start()
        connection = await pool.acquire()
        try:
            result = await pool.execute(connection, "select json('{\"pony\": true}')")
            self.json1_available = bool(result.rows)
        except sqlite3.OperationalError:
            self.json1_available = False
        finally:
            await pool.release(connection)

    @dbapiprovider.wrap_dbapi_exceptions
    def set_transaction_mode(self, connection, cache):
        if connection.transaction is not None:
            return
        session = cache.db_session
        if cache.db_session is not None and cache.db_session.ddl:
            cursor = connection.cursor()
            cursor.execute("PRAGMA foreign_keys")
            row = cursor.fetchone()
            cache.saved_fk_state = bool(row and row[0])
            if cache.saved_fk_state:
                cursor.execute("PRAGMA foreign_keys = false")
        if session is not None and getattr(session, "readonly", False):
            await_io(self.pool_transport.execute(connection.raw, "PRAGMA query_only = true"))
            connection.readonly = True
        if cache.immediate:
            begin_sql = (
                "BEGIN TRANSACTION" if connection.readonly
                else "BEGIN IMMEDIATE TRANSACTION"
            )
            await_io(self.pool_transport.execute(
                connection.raw, begin_sql
            ))
            connection.transaction = True
            cache.in_transaction = True

    async def commit_async(self, connection):
        if connection.transaction is not None:
            try:
                await connection.raw.commit()
            except BaseException:
                connection.invalidated = True
                raise
            finally:
                connection.transaction = None

    async def rollback_async(self, connection):
        if connection.transaction is not None:
            try:
                await connection.raw.rollback()
            except BaseException:
                connection.invalidated = True
                raise
            finally:
                connection.transaction = None

    async def release_async(self, connection):
        if connection.released:
            return
        try:
            if connection.transaction is not None:
                await self.rollback_async(connection)
            if connection.readonly:
                await self.pool_transport.execute(
                    connection.raw, "PRAGMA query_only = false"
                )
                connection.readonly = False
        except BaseException:
            connection.invalidated = True
            try:
                await super().release_async(connection)
            except BaseException:
                pass
            raise
        await super().release_async(connection)

    def release(self, connection, cache=None):
        try:
            if cache is not None and cache.saved_fk_state:
                cursor = connection.cursor()
                cursor.execute("PRAGMA foreign_keys = true")
        except BaseException:
            self.drop(connection, cache)
            raise
        super().release(connection, cache)


def _parse_dollar_quote(sql, offset):
    if sql[offset] != "$":
        return None
    end = offset + 1
    while end < len(sql) and (sql[end].isalnum() or sql[end] == "_"):
        end += 1
    if end < len(sql) and sql[end] == "$":
        return sql[offset : end + 1]
    return None


def _adapt_asyncpg_sql(sql, arguments):
    """Convert DB-API placeholders without touching SQL literals/comments."""

    if arguments is None:
        arguments = ()
    named = isinstance(arguments, dict)
    values = []
    positions = {}
    positional = iter(arguments) if not named else None
    output = []
    i = 0
    state = "sql"
    dollar_tag = None
    block_depth = 0
    escape_single = False
    while i < len(sql):
        char = sql[i]
        pair = sql[i : i + 2]
        if pair == "%%":
            output.append("%")
            i += 2
            continue
        if state == "line":
            output.append(char)
            i += 1
            if char in "\r\n":
                state = "sql"
            continue
        if state == "block":
            if pair == "/*":
                block_depth += 1
                output.append(pair)
                i += 2
            elif pair == "*/":
                block_depth -= 1
                output.append(pair)
                i += 2
                if not block_depth:
                    state = "sql"
            else:
                output.append(char)
                i += 1
            continue
        if state in ("single", "double"):
            quote = "'" if state == "single" else '"'
            output.append(char)
            i += 1
            if state == "single" and escape_single and char == "\\":
                if i < len(sql):
                    output.append(sql[i])
                    i += 1
                continue
            if char == quote:
                if i < len(sql) and sql[i] == quote:
                    output.append(sql[i])
                    i += 1
                else:
                    state = "sql"
            continue
        if state == "dollar":
            if sql.startswith(dollar_tag, i):
                output.append(dollar_tag)
                i += len(dollar_tag)
                state = "sql"
            else:
                output.append(char)
                i += 1
            continue

        if pair == "--":
            state = "line"
            output.append(pair)
            i += 2
        elif pair == "/*":
            state = "block"
            block_depth = 1
            output.append(pair)
            i += 2
        elif char == "'":
            state = "single"
            escape_single = (
                i > 0
                and sql[i - 1] in "eE"
                and (i < 2 or not (sql[i - 2].isalnum() or sql[i - 2] == "_"))
            )
            output.append(char)
            i += 1
        elif char == '"':
            state = "double"
            output.append(char)
            i += 1
        elif char == "$" and (tag := _parse_dollar_quote(sql, i)) is not None:
            state = "dollar"
            dollar_tag = tag
            output.append(tag)
            i += len(tag)
        elif pair == "%s":
            if named:
                raise TypeError("Positional placeholder used with mapping arguments")
            try:
                value = next(positional)
            except StopIteration:
                raise TypeError("Not enough arguments for SQL placeholders") from None
            values.append(value)
            output.append("$%d" % len(values))
            i += 2
        elif pair == "%(" and named:
            end = sql.find(")s", i + 2)
            if end < 0:
                raise ValueError("Malformed named SQL placeholder")
            name = sql[i + 2 : end]
            if not name:
                raise ValueError("Empty named SQL placeholder")
            try:
                position = positions[name]
            except KeyError:
                try:
                    value = arguments[name]
                except KeyError:
                    raise KeyError("Missing SQL argument %r" % name) from None
                values.append(value)
                position = positions[name] = len(values)
            output.append("$%d" % position)
            i = end + 2
        else:
            output.append(char)
            i += 1
    if state in ("single", "double", "block", "dollar"):
        # PostgreSQL will produce the canonical syntax error.  The important
        # invariant here is that no placeholder inside the unfinished region was
        # rewritten.
        pass
    if not named:
        try:
            next(positional)
        except StopIteration:
            pass
        else:
            raise TypeError("Too many arguments for SQL placeholders")
    return "".join(output), tuple(values)


_rowcount_re = re.compile(r"(?:^|\s)(\d+)$")


class _AsyncPGPool(_BoundedPool):
    def __init__(self, database, driver, args, kwargs):
        kwargs = dict(kwargs)
        max_size = kwargs.pop("max_pool_size", kwargs.pop("pool_size", 10))
        min_size = kwargs.pop("min_pool_size", 0)
        max_waiters = kwargs.pop("max_waiters", int(max_size) * 4)
        acquire_timeout = kwargs.pop("acquire_timeout", 30.0)
        shutdown_timeout = kwargs.pop("shutdown_timeout", 30.0)
        super().__init__(max_size, max_waiters, acquire_timeout, shutdown_timeout)
        self.database = database
        self.driver = driver
        self.args = args
        self.kwargs = kwargs
        self.min_size = _positive_int(min_size, "min_pool_size", allow_zero=True)
        driver_timeout = kwargs.pop("timeout", 10.0)
        self.connect_timeout = _positive_float(
            kwargs.pop("connect_timeout", driver_timeout), "connect_timeout"
        )
        self.query_timeout = _positive_float(
            kwargs.pop("query_timeout", None), "query_timeout"
        )
        self.user_setup = kwargs.pop("setup", None)
        self.pool = None
        self.server_version = None

    async def start(self):
        async def setup(connection):
            if self.user_setup is not None:
                result = self.user_setup(connection)
                if hasattr(result, "__await__"):
                    await result
            await self.database._run_on_connect(connection, "postgres")

        self.pool = await _wait(
            self.driver.create_pool(
                *self.args,
                min_size=self.min_size,
                max_size=self.max_size,
                setup=setup,
                timeout=self.connect_timeout,
                **self.kwargs,
            ),
            self.connect_timeout,
            AsyncPoolTimeout("Timed out connecting to PostgreSQL"),
        )
        connection = await _wait(
            self.pool.acquire(timeout=self.acquire_timeout),
            self.acquire_timeout,
            AsyncPoolTimeout("Timed out inspecting the PostgreSQL server"),
        )
        try:
            version = connection.get_server_version()
            self.server_version = version.major * 10000 + version.minor * 100 + version.micro
        finally:
            await self.pool.release(connection)
        self.metrics.open_connections = self.pool.get_size()

    async def acquire(self):
        await self._admit()
        try:
            connection = await _wait(
                self.pool.acquire(timeout=self.acquire_timeout),
                self.acquire_timeout,
                AsyncPoolTimeout("Timed out waiting for a PostgreSQL connection"),
            )
        except BaseException:
            self._returned()
            raise
        self.metrics.open_connections = self.pool.get_size()
        return connection

    async def release(self, connection):
        try:
            await self.pool.release(connection, timeout=self.shutdown_timeout)
        finally:
            self.metrics.open_connections = self.pool.get_size()
            self._returned()

    async def drop(self, connection):
        try:
            connection.terminate()
            await self.pool.release(connection, timeout=self.shutdown_timeout)
        finally:
            self.metrics.total_dropped += 1
            self.metrics.open_connections = self.pool.get_size()

    async def close(self):
        self._closed = True
        drained = await self._wait_until_drained()
        if self.pool is None:
            return
        if not drained:
            self.pool.terminate()
        else:
            try:
                await _wait(
                    self.pool.close(), self.shutdown_timeout, asyncio.TimeoutError()
                )
            except asyncio.TimeoutError:
                self.pool.terminate()
        self.metrics.open_connections = 0

    async def execute(self, connection, sql, arguments=None):
        adapted_sql, values = _adapt_asyncpg_sql(sql, arguments)

        async def operation():
            statement = await connection.prepare(adapted_sql)
            attributes = statement.get_attributes()
            if hasattr(statement, "get_statusmsg"):
                rows = await statement.fetch(*values)
                materialized = tuple(tuple(row) for row in rows)
                if not attributes:
                    status = statement.get_statusmsg() or ""
                    match = _rowcount_re.search(status)
                    return AsyncResult(
                        rowcount=int(match.group(1)) if match else -1
                    )
                return AsyncResult(
                    rows=materialized,
                    description=_description(attribute.name for attribute in attributes),
                    rowcount=len(materialized),
                    generated_id=materialized[0][0] if materialized else None,
                )
            # Small protocol fakes used by downstream tests may implement only
            # prepare/fetch. Keep that test seam out of the production branch.
            upper_sql = adapted_sql.lstrip().upper()
            if upper_sql.startswith(("SELECT", "WITH", "VALUES", "TABLE", "SHOW")):
                rows = await statement.fetch(*values)
                materialized = tuple(tuple(row) for row in rows)
                return AsyncResult(
                    rows=materialized,
                    description=_description(attribute.name for attribute in attributes),
                    rowcount=len(materialized),
                    generated_id=materialized[0][0] if materialized else None,
                )
            status = await connection.execute(adapted_sql, *values)
            match = _rowcount_re.search(status)
            return AsyncResult(rowcount=int(match.group(1)) if match else -1)

        started = monotonic()
        outcome = "ok"
        try:
            return await _wait(
                operation(), self.query_timeout,
                AsyncQueryTimeout("PostgreSQL query timed out")
            )
        except AsyncQueryTimeout:
            outcome = "timeout"
            if self.database is not None:
                self.database._record_cancellation(timed_out=True)
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            if self.database is not None:
                self.database._record_cancellation()
            raise
        except BaseException:
            outcome = "error"
            raise
        finally:
            if self.database is not None:
                self.database._record_query(sql, monotonic() - started, outcome)

    async def executemany(self, connection, sql, arguments):
        adapted = [_adapt_asyncpg_sql(sql, item) for item in arguments]
        if not adapted:
            return AsyncResult(rowcount=0)
        adapted_sql = adapted[0][0]
        if any(item_sql != adapted_sql for item_sql, _values in adapted[1:]):
            raise ValueError("executemany produced inconsistent SQL placeholders")

        async def operation():
            statement = await connection.prepare(adapted_sql)
            total = 0
            for _item_sql, values in adapted:
                await statement.fetch(*values)
                status = statement.get_statusmsg() or ""
                match = _rowcount_re.search(status)
                if match is None:
                    total = -1
                elif total >= 0:
                    total += int(match.group(1))
            return AsyncResult(rowcount=total)

        started = monotonic()
        outcome = "ok"
        try:
            return await _wait(
                operation(), self.query_timeout,
                AsyncQueryTimeout("PostgreSQL executemany timed out")
            )
        except AsyncQueryTimeout:
            outcome = "timeout"
            if self.database is not None:
                self.database._record_cancellation(timed_out=True)
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            if self.database is not None:
                self.database._record_cancellation()
            raise
        except BaseException:
            outcome = "error"
            raise
        finally:
            if self.database is not None:
                self.database._record_query(sql, monotonic() - started, outcome)


def _make_asyncpg_dbapi(driver):
    dummy = type("AsyncPGUnusedException", (Exception,), {})
    connection_errors = (
        getattr(driver, "PostgresConnectionError", dummy),
        getattr(driver, "ConnectionDoesNotExistError", dummy),
    )
    return SimpleNamespace(
        __name__="asyncpg",
        Warning=dummy,
        Error=(driver.PostgresError, driver.InterfaceError),
        InterfaceError=driver.InterfaceError,
        DatabaseError=driver.PostgresError,
        DataError=getattr(driver, "DataError", dummy),
        OperationalError=connection_errors + (
            getattr(driver, "SerializationError", dummy),
            getattr(driver, "DeadlockDetectedError", dummy),
        ),
        connection_errors=connection_errors,
        IntegrityError=driver.IntegrityConstraintViolationError,
        InternalError=getattr(driver, "InternalServerError", dummy),
        ProgrammingError=getattr(driver, "SyntaxOrAccessRuleViolationError", dummy),
        NotSupportedError=getattr(driver, "FeatureNotSupportedError", dummy),
    )


_asyncpg_provider_cls = None


def _get_asyncpg_provider_cls():
    global _asyncpg_provider_cls
    if _asyncpg_provider_cls is not None:
        return _asyncpg_provider_cls
    from pony.orm.dbproviders import postgres as postgres_provider

    class AsyncPGProvider(_NativeProviderMixin, postgres_provider.PGProvider):
        def __init__(self, database, *args, **kwargs):
            self.database = database
            self.args = args
            self.connect_kwargs = kwargs
            self.pool = self.pool_transport = None

        async def initialize(self):
            try:
                import asyncpg
            except ImportError:
                raise ImportError(
                    "Async PostgreSQL support requires asyncpg; "
                    "install pony[async-postgres]"
                ) from None
            self.dbapi_module = _make_asyncpg_dbapi(asyncpg)
            pool = _AsyncPGPool(self.database, asyncpg, self.args, self.connect_kwargs)
            self.pool = self.pool_transport = pool
            await pool.start()
            self.server_version = pool.server_version
            self.table_if_not_exists_syntax = self.server_version >= 90100

        @dbapiprovider.wrap_dbapi_exceptions
        def set_transaction_mode(self, connection, cache):
            if connection.transaction is not None or not cache.immediate:
                return
            session = cache.db_session
            isolation = getattr(session, "isolation", None)
            readonly = bool(getattr(session, "readonly", False))
            deferrable = bool(getattr(session, "deferrable", False))
            transaction = connection.raw.transaction(
                isolation=isolation, readonly=readonly, deferrable=deferrable
            )
            await_io(transaction.start())
            connection.transaction = transaction
            cache.in_transaction = True

        async def commit_async(self, connection):
            transaction = connection.transaction
            if transaction is not None:
                try:
                    await transaction.commit()
                except BaseException:
                    connection.invalidated = True
                    raise
                finally:
                    connection.transaction = None

        async def rollback_async(self, connection):
            transaction = connection.transaction
            if transaction is not None:
                try:
                    await transaction.rollback()
                except BaseException:
                    connection.invalidated = True
                    raise
                finally:
                    connection.transaction = None

        async def release_async(self, connection):
            if connection.released:
                return
            if connection.transaction is not None:
                await self.rollback_async(connection)
            await super().release_async(connection)

    _asyncpg_provider_cls = AsyncPGProvider
    return AsyncPGProvider


def make_provider(database, provider_name, args, kwargs):
    """Build an uninitialized native provider without importing its driver."""

    normalized = provider_name.lower()
    if normalized in ("sqlite", "aiosqlite"):
        if args:
            filename, args = args[0], args[1:]
            if args:
                raise TypeError("Too many positional arguments for SQLite provider")
        else:
            try:
                filename = kwargs.pop("filename")
            except KeyError:
                raise TypeError("SQLite filename is not specified") from None
        return "sqlite", AsyncSQLiteProvider(database, filename, **kwargs)
    if normalized in ("postgres", "postgresql", "asyncpg"):
        return "postgres", _get_asyncpg_provider_cls()(database, *args, **kwargs)
    raise TypeError(
        "pony.orm.asyncio.Database supports only SQLite and PostgreSQL providers; "
        "got %r" % provider_name
    )
