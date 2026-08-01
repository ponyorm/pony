"""Native asyncio transports for SQLite and PostgreSQL."""

import asyncio
import os
import re
import sqlite3
from types import SimpleNamespace

from pony.orm import dbapiprovider
from pony.orm.asyncio.transport import AsyncResult, BridgedPool
from pony.orm.dbproviders import sqlite as sqlite_provider


def _description(names):
    return tuple((name, None, None, None, None, None, None) for name in names)


class _SQLitePool:
    def __init__(self, database, driver, filename, max_size, connect_kwargs):
        self.database = database
        self.driver = driver
        self.filename = filename
        self.max_size = max_size
        self.connect_kwargs = connect_kwargs
        self._available = asyncio.LifoQueue()
        self._permits = asyncio.Semaphore(max_size)
        self._connections = set()
        self._closed = False

    def _bridge(self, awaitable):
        return self.database._bridge(awaitable)

    async def start(self):
        connection = await self.acquire()
        await self.release(connection)

    async def _open(self):
        connection = await self.driver.connect(
            self.filename, isolation_level=None, **self.connect_kwargs
        )
        self._connections.add(connection)
        try:
            connection.text_factory = sqlite_provider._text_factory

            async def create_function(name, num_params, func):
                await connection.create_function(name, num_params, func)

            await create_function("power", 2, pow)
            await create_function("rand", 0, sqlite_provider.random)
            await create_function("py_upper", 1, sqlite_provider.py_upper)
            await create_function("py_lower", 1, sqlite_provider.py_lower)
            await create_function("py_json_unwrap", 1, sqlite_provider.py_json_unwrap)
            await create_function(
                "py_json_extract", -1, sqlite_provider.py_json_extract
            )
            await create_function(
                "py_json_contains", 3, sqlite_provider.py_json_contains
            )
            await create_function("py_json_nonzero", 2, sqlite_provider.py_json_nonzero)
            await create_function(
                "py_json_array_length", -1, sqlite_provider.py_json_array_length
            )
            await create_function("py_array_index", 2, sqlite_provider.py_array_index)
            await create_function(
                "py_array_contains", 2, sqlite_provider.py_array_contains
            )
            await create_function("py_array_subset", 2, sqlite_provider.py_array_subset)
            await create_function("py_array_length", 1, sqlite_provider.py_array_length)
            await create_function("py_array_slice", 3, sqlite_provider.py_array_slice)
            await create_function("py_make_array", -1, sqlite_provider.py_make_array)
            await create_function("py_string_slice", 3, sqlite_provider.py_string_slice)
            await connection.execute("PRAGMA foreign_keys = true")
            await connection.execute("PRAGMA case_sensitive_like = true")
            await self.database._run_on_connect(connection, "sqlite")
        except BaseException:
            self._connections.discard(connection)
            await connection.close()
            raise
        return connection

    async def acquire(self):
        if self._closed:
            raise dbapiprovider.InterfaceError(Exception("Database pool is closed"))
        await self._permits.acquire()
        try:
            try:
                return self._available.get_nowait()
            except asyncio.QueueEmpty:
                return await self._open()
        except BaseException:
            self._permits.release()
            raise

    async def release(self, connection):
        if self._closed:
            await self.drop(connection, release_permit=False)
        else:
            try:
                await connection.rollback()
            except BaseException:
                await self.drop(connection, release_permit=False)
            else:
                self._available.put_nowait(connection)
        self._permits.release()

    async def drop(self, connection, release_permit=True):
        self._connections.discard(connection)
        try:
            await connection.close()
        finally:
            if release_permit:
                self._permits.release()

    async def close(self):
        self._closed = True
        connections = tuple(self._connections)
        self._connections.clear()
        for connection in connections:
            await connection.close()
        while not self._available.empty():
            self._available.get_nowait()

    async def execute(self, connection, sql, arguments=None):
        if arguments is None:
            cursor = await connection.execute(sql)
        else:
            cursor = await connection.execute(sql, arguments)
        try:
            description = tuple(cursor.description or ())
            rows = (
                tuple(tuple(row) for row in await cursor.fetchall())
                if description
                else ()
            )
            return AsyncResult(
                rows=rows,
                description=description,
                rowcount=cursor.rowcount,
                generated_id=cursor.lastrowid,
            )
        finally:
            await cursor.close()

    async def executemany(self, connection, sql, arguments):
        cursor = await connection.executemany(sql, arguments)
        try:
            return AsyncResult(rowcount=cursor.rowcount, generated_id=cursor.lastrowid)
        finally:
            await cursor.close()

    async def commit(self, connection):
        await connection.commit()

    async def rollback(self, connection):
        await connection.rollback()


class AsyncSQLiteProvider(sqlite_provider.SQLiteProvider):
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
                database.id,
                os.urandom(8).hex(),
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
        self.max_pool_size = int(
            kwargs.pop("max_pool_size", kwargs.pop("pool_size", 5))
        )
        if self.max_pool_size < 1:
            raise ValueError("max_pool_size must be a positive integer")
        if filename == ":memory:" or is_shared_memory:
            self.max_pool_size = 1
        self.connect_kwargs = kwargs
        self._async_pool = None
        self.pool = None

    async def initialize(self):
        try:
            import aiosqlite
        except ImportError:
            raise ImportError(
                "Async SQLite support requires aiosqlite; install pony[async-sqlite]"
            ) from None
        pool = _SQLitePool(
            self.database,
            aiosqlite,
            self.filename,
            self.max_pool_size,
            self.connect_kwargs,
        )
        self._async_pool = pool
        await pool.start()
        self.pool = BridgedPool(pool, self.database._bridge)
        connection = await pool.acquire()
        try:
            result = await pool.execute(connection, "select json('{\"pony\": true}')")
            self.json1_available = bool(result.rows)
        except sqlite3.OperationalError:
            self.json1_available = False
        finally:
            await pool.release(connection)

    async def async_disconnect(self):
        if self._async_pool is not None:
            await self._async_pool.close()

    @dbapiprovider.wrap_dbapi_exceptions
    def release(self, connection, cache=None):
        # The synchronous provider drops DDL connections in order to reset all
        # connection state.  An in-memory async database must retain its single
        # connection, and file pools are reset explicitly on release instead.
        if cache is not None:
            db_session = cache.db_session
            if db_session is not None and db_session.ddl and cache.saved_fk_state:
                cursor = connection.cursor()
                cursor.execute("PRAGMA foreign_keys = true")
        self.pool.release(connection)


_named_placeholder = re.compile(r"%\(([^)]+)\)s")
_positional_placeholder = re.compile(r"%s")
_rowcount_re = re.compile(r"(?:^|\s)(\d+)$")


def _adapt_asyncpg_sql(sql, arguments):
    if arguments is None:
        return sql, ()
    if isinstance(arguments, dict):
        positions = {}
        values = []

        def replace(match):
            name = match.group(1)
            position = positions.get(name)
            if position is None:
                position = positions[name] = len(values) + 1
                values.append(arguments[name])
            return "$%d" % position

        return _named_placeholder.sub(replace, sql), tuple(values)

    values = tuple(arguments)
    counter = iter(range(1, len(values) + 1))
    return _positional_placeholder.sub(lambda _m: "$%d" % next(counter), sql), values


class _AsyncPGPool:
    def __init__(self, database, driver, args, kwargs):
        self.database = database
        self.driver = driver
        self.args = args
        self.kwargs = kwargs
        self.pool = None
        self.server_version = None

    def _bridge(self, awaitable):
        return self.database._bridge(awaitable)

    async def start(self):
        kwargs = dict(self.kwargs)
        max_size = int(kwargs.pop("max_pool_size", kwargs.pop("pool_size", 10)))
        min_size = int(kwargs.pop("min_pool_size", 0))

        async def initialize(connection):
            await self.database._run_on_connect(connection, "postgres")

        self.pool = await self.driver.create_pool(
            *self.args,
            min_size=min_size,
            max_size=max_size,
            init=initialize,
            **kwargs,
        )
        connection = await self.pool.acquire()
        try:
            version = connection.get_server_version()
            self.server_version = (
                version.major * 10000 + version.minor * 100 + version.micro
            )
        finally:
            await self.pool.release(connection)

    async def acquire(self):
        return await self.pool.acquire()

    async def release(self, connection):
        await self.pool.release(connection)

    async def drop(self, connection):
        connection.terminate()
        await self.pool.release(connection)

    async def close(self):
        if self.pool is not None:
            await self.pool.close()

    async def execute(self, connection, sql, arguments=None):
        sql, values = _adapt_asyncpg_sql(sql, arguments)
        upper_sql = sql.lstrip().upper()
        returns_rows = upper_sql.startswith(("SELECT", "WITH")) or bool(
            re.search(r"\bRETURNING\b", upper_sql)
        )
        if returns_rows:
            statement = await connection.prepare(sql)
            rows = await statement.fetch(*values)
            names = tuple(attribute.name for attribute in statement.get_attributes())
            materialized = tuple(tuple(row) for row in rows)
            return AsyncResult(
                rows=materialized,
                description=_description(names),
                rowcount=len(materialized),
                generated_id=materialized[0][0] if materialized else None,
            )
        status = await connection.execute(sql, *values)
        match = _rowcount_re.search(status)
        return AsyncResult(rowcount=int(match.group(1)) if match else -1)

    async def executemany(self, connection, sql, arguments):
        adapted = []
        adapted_sql = sql
        for item in arguments:
            adapted_sql, values = _adapt_asyncpg_sql(sql, item)
            adapted.append(values)
        await connection.executemany(adapted_sql, adapted)
        return AsyncResult(rowcount=len(adapted))

    async def commit(self, connection):
        await connection.execute("COMMIT")

    async def rollback(self, connection):
        if connection.is_in_transaction():
            await connection.execute("ROLLBACK")


_asyncpg_provider_cls = None


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
        OperationalError=connection_errors
        + (
            getattr(driver, "SerializationError", dummy),
            getattr(driver, "DeadlockDetectedError", dummy),
        ),
        connection_errors=connection_errors,
        IntegrityError=driver.IntegrityConstraintViolationError,
        InternalError=getattr(driver, "InternalServerError", dummy),
        ProgrammingError=getattr(driver, "SyntaxOrAccessRuleViolationError", dummy),
        NotSupportedError=getattr(driver, "FeatureNotSupportedError", dummy),
    )


def _get_asyncpg_provider_cls():
    global _asyncpg_provider_cls
    if _asyncpg_provider_cls is not None:
        return _asyncpg_provider_cls

    # The synchronous PostgreSQL module contains the shared dialect, schema and
    # converters.  It can be imported without psycopg after the optional-driver
    # guard in that module; binding the synchronous provider still requires it.
    from pony.orm.dbproviders import postgres as postgres_provider

    class AsyncPGProvider(postgres_provider.PGProvider):
        def __init__(self, database, *args, **kwargs):
            self.database = database
            self.args = args
            self.connect_kwargs = kwargs
            self._async_pool = None
            self.pool = None

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
            self._async_pool = pool
            await pool.start()
            self.pool = BridgedPool(pool, self.database._bridge)
            self.server_version = pool.server_version
            self.table_if_not_exists_syntax = self.server_version >= 90100

        async def async_disconnect(self):
            if self._async_pool is not None:
                await self._async_pool.close()

        @dbapiprovider.wrap_dbapi_exceptions
        def set_transaction_mode(self, connection, cache):
            if cache.immediate:
                cursor = connection.cursor()
                db_session = cache.db_session
                if db_session is not None and db_session.serializable:
                    cursor.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
                else:
                    cursor.execute("BEGIN")
                cache.in_transaction = True

        @dbapiprovider.wrap_dbapi_exceptions
        def execute(self, cursor, sql, arguments=None, returning_id=False):
            if type(arguments) is list:
                cursor.executemany(sql, arguments)
            else:
                cursor.execute(sql, arguments)
            if returning_id:
                return cursor.lastrowid

        @dbapiprovider.wrap_dbapi_exceptions
        def release(self, connection, cache=None):
            self.pool.release(connection)

        def should_reconnect(self, exc):
            module = self.dbapi_module
            return isinstance(exc, module.connection_errors)

    _asyncpg_provider_cls = AsyncPGProvider
    return AsyncPGProvider


def make_provider(database, provider_name, args, kwargs):
    """Build an uninitialized provider without importing optional drivers."""

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
        provider_cls = _get_asyncpg_provider_cls()
        return "postgres", provider_cls(database, *args, **kwargs)
    raise TypeError(
        "pony.orm.asyncio.Database supports only SQLite and PostgreSQL "
        "providers; got %r" % provider_name
    )
