"""Native asyncio API for Pony ORM.

Query construction and entity manipulation stay synchronous.  Every operation
which can touch a database is explicit and awaitable.
"""

import asyncio as _asyncio
import contextvars
import inspect
import sys
import threading
import warnings
from functools import wraps

if sys.version_info < (3, 10):
    raise ImportError("pony.orm.asyncio requires Python 3.10 or newer")

from pony.orm import core
from pony.orm.asyncio.providers import make_provider

__all__ = [
    "Database",
    "AsyncImplicitIOError",
    "AsyncLazyLoadError",
    "AsyncSessionConcurrencyError",
    "commit",
    "db_session",
    "delete",
    "exists",
    "flush",
    "get",
    "rollback",
    "select",
]


class AsyncImplicitIOError(core.OrmError):
    """A synchronous operation attempted implicit database I/O."""


class AsyncLazyLoadError(AsyncImplicitIOError):
    """An unloaded async entity attribute or collection was accessed."""


class AsyncSessionConcurrencyError(core.TransactionError):
    """One async session was used by more than one asyncio task."""


_session_var = contextvars.ContextVar("pony_async_session", default=None)
_io_allowed = contextvars.ContextVar("pony_async_io_allowed", default=False)


def _current_task():
    try:
        return _asyncio.current_task()
    except RuntimeError:
        return None


class _SessionState:
    def __init__(self, options):
        self.options = options
        self.owner = _current_task()
        self.loop = _asyncio.get_running_loop()
        self.depth = 1
        self.db2cache = {}
        self.debug = options.sql_debug
        self.show_values = options.show_values
        self.debug_stack = []
        self.operation_lock = _asyncio.Lock()
        self.cleanup_tasks = set()
        self._pending = set()
        self._pending_lock = threading.Lock()

    def check_owner(self):
        task = _current_task()
        if (
            task is not None
            and task is not self.owner
            and task not in self.cleanup_tasks
        ):
            raise AsyncSessionConcurrencyError(
                "An asyncio db_session belongs to one asyncio task and cannot be "
                "inherited by create_task(); open a separate db_session "
                "inside the child task"
            )

    def add_pending(self, future):
        with self._pending_lock:
            self._pending.add(future)

    def remove_pending(self, future):
        with self._pending_lock:
            self._pending.discard(future)

    def cancel_pending(self):
        with self._pending_lock:
            pending = tuple(self._pending)
        for future in pending:
            future.cancel()


def _require_state():
    state = _session_var.get()
    if state is None:
        raise core.TransactionError(
            "pony.orm.asyncio.db_session is required when working with "
            "pony.orm.asyncio.Database"
        )
    state.check_owner()
    return state


def _call_in_orm_context(state, func, args, kwargs):
    local = core.local
    old_values = (
        local.db_session,
        local.db_context_counter,
        local.db2cache,
        local.debug,
        local.show_values,
    )
    local.db_session = state.options
    local.db_context_counter = state.depth
    local.db2cache = state.db2cache
    if state.debug is not None:
        local.debug = state.debug
        local.show_values = state.show_values
    token = _io_allowed.set(True)
    try:
        return func(*args, **kwargs)
    finally:
        _io_allowed.reset(token)
        (
            local.db_session,
            local.db_context_counter,
            local.db2cache,
            local.debug,
            local.show_values,
        ) = old_values


async def _run_sync_operation(state, func, *args, **kwargs):
    state.check_owner()
    async with state.operation_lock:
        context = contextvars.copy_context()
        loop = _asyncio.get_running_loop()
        worker = loop.run_in_executor(
            None, context.run, _call_in_orm_context, state, func, args, kwargs
        )
        try:
            await _asyncio.wait((worker,))
            return worker.result()
        except _asyncio.CancelledError:
            state.cancel_pending()
            try:
                await _asyncio.wait((worker,))
                worker.result()
            except BaseException:
                pass
            raise


def _release_all_sync():
    for cache in core._get_caches():
        cache.release()


def _rollback_remaining_sync():
    if core.local.db2cache:
        core.rollback()


async def _safe_cleanup(state, rollback_only):
    async def cleanup():
        if rollback_only:
            await _run_sync_operation(state, _rollback_remaining_sync)
            return
        try:
            await _run_sync_operation(state, core.commit)
            await _run_sync_operation(state, _release_all_sync)
        except BaseException:
            try:
                await _run_sync_operation(state, _rollback_remaining_sync)
            except BaseException:
                pass
            raise

    task = _asyncio.create_task(cleanup())
    state.cleanup_tasks.add(task)
    try:
        await _asyncio.wait((task,))
        return task.result()
    except _asyncio.CancelledError:
        try:
            await _asyncio.wait((task,))
            task.result()
        except BaseException:
            pass
        raise
    finally:
        state.cleanup_tasks.discard(task)


class AsyncDBSessionContextManager:
    __slots__ = (
        "allowed_exceptions",
        "ddl",
        "immediate",
        "optimistic",
        "retry",
        "retry_exceptions",
        "serializable",
        "show_values",
        "sql_debug",
        "strict",
    )

    def __init__(
        self,
        retry=0,
        immediate=False,
        ddl=False,
        serializable=False,
        strict=False,
        optimistic=True,
        retry_exceptions=(core.TransactionError,),
        allowed_exceptions=(),
        sql_debug=None,
        show_values=None,
    ):
        if type(retry) is not int or retry < 0:
            raise TypeError("retry must be a non-negative integer")
        if retry and ddl:
            raise TypeError("ddl and retry cannot be used together")
        if not callable(allowed_exceptions) and not callable(retry_exceptions):
            overlap = set(allowed_exceptions).intersection(retry_exceptions)
            if overlap:
                name = next(iter(overlap)).__name__
                raise TypeError(
                    "%s cannot be both an allowed and retry exception" % name
                )
        self.retry = retry
        self.ddl = ddl
        self.serializable = serializable
        self.immediate = immediate or ddl or serializable or not optimistic
        self.strict = strict
        self.optimistic = optimistic and not serializable
        self.retry_exceptions = retry_exceptions
        self.allowed_exceptions = allowed_exceptions
        self.sql_debug = sql_debug
        self.show_values = show_values

    def __call__(self, *args, **kwargs):
        if not args and not kwargs:
            return self
        if not args:
            return type(self)(**kwargs)
        if len(args) != 1 or kwargs:
            raise TypeError(
                "Pass keyword options to db_session or use it as a decorator"
            )
        function = args[0]
        if not inspect.iscoroutinefunction(function):
            raise TypeError("db_session can decorate only async functions")

        @wraps(function)
        async def wrapper(*f_args, **f_kwargs):
            state = _session_var.get()
            if state is not None:
                state.check_owner()
                if self.ddl:
                    raise core.TransactionError(
                        "A ddl db_session cannot be nested"
                    )
                if self.retry:
                    warnings.warn(
                        "retry is ignored for a nested db_session",
                        core.PonyRuntimeWarning,
                        stacklevel=2,
                    )
                async with self:
                    return await function(*f_args, **f_kwargs)

            last = None
            for attempt in range(self.retry + 1):
                try:
                    async with self:
                        return await function(*f_args, **f_kwargs)
                except _asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    last = exc
                    retry_exceptions = self.retry_exceptions
                    should_retry = (
                        retry_exceptions(exc)
                        if callable(retry_exceptions)
                        else isinstance(exc, tuple(retry_exceptions))
                    )
                    should_retry = should_retry or getattr(exc, "should_retry", False)
                    if not should_retry or attempt == self.retry:
                        raise
            raise last  # pragma: no cover

        return wrapper

    async def __aenter__(self):
        state = _session_var.get()
        if state is None:
            state = _SessionState(self)
            _session_var.set(state)
            return self
        state.check_owner()
        outer = state.options
        if self.ddl and not outer.ddl:
            raise core.TransactionError(
                "Cannot start ddl transaction inside non-ddl transaction"
            )
        if self.serializable and not outer.serializable:
            raise core.TransactionError(
                "Cannot start serializable transaction inside non-serializable transaction"
            )
        state.debug_stack.append((state.debug, state.show_values))
        if self.sql_debug is not None:
            state.debug = self.sql_debug
            state.show_values = self.show_values
        state.depth += 1
        return self

    async def __aexit__(self, exc_type=None, exc=None, tb=None):
        state = _require_state()
        state.depth -= 1
        if state.depth:
            state.debug, state.show_values = state.debug_stack.pop()
            return False
        try:
            try:
                if exc_type is None:
                    can_commit = True
                elif callable(self.allowed_exceptions):
                    can_commit = bool(self.allowed_exceptions(exc))
                else:
                    can_commit = issubclass(exc_type, tuple(self.allowed_exceptions))
            except BaseException:
                try:
                    await _safe_cleanup(state, rollback_only=True)
                except BaseException:
                    pass
                raise
            try:
                await _safe_cleanup(state, rollback_only=not can_commit)
            except BaseException:
                # Preserve the user's exception (especially CancelledError) after
                # rollback/drop cleanup, matching db_session semantics.
                if exc_type is None or can_commit:
                    raise
        finally:
            _session_var.set(None)
        return False


db_session = AsyncDBSessionContextManager()


class _AsyncQuery:
    """Awaitable terminal operations over Pony's shared query builder."""

    __slots__ = ("_limit", "_offset", "_query")
    _is_async_query_wrapper_ = True

    _query_methods = {
        "distinct",
        "filter",
        "for_update",
        "order_by",
        "prefetch",
        "sort_by",
        "where",
        "without_distinct",
    }

    def __init__(self, query, limit=None, offset=None):
        self._query = query
        self._limit = limit
        self._offset = offset

    @property
    def query(self):
        return self._query

    @property
    def _key(self):
        return self._query._key

    @property
    def _translator(self):
        return self._query._translator

    @property
    def _vars(self):
        return self._query._vars

    def _get_query(self):
        return self._query

    def _get_type_(self):
        return self._query._get_type_()

    def _normalize_var(self, query_type):
        return self._query._normalize_var(query_type)

    def _database(self):
        return self._query._database

    def _run(self, function, *args, **kwargs):
        return self._database()._run_sync(function, *args, **kwargs)

    async def fetch(self, limit=None, offset=None):
        if limit is None:
            limit = self._limit
        if offset is None:
            offset = self._offset
        result = await self._run(self._query.fetch, limit, offset)
        return list(result)

    async def get(self):
        return await self._run(self._query.get)

    async def first(self):
        return await self._run(self._query.first)

    async def exists(self):
        return await self._run(self._query.exists)

    async def delete(self, bulk=None):
        return await self._run(self._query.delete, bulk)

    async def sum(self, distinct=None):
        return await self._run(self._query.sum, distinct)

    async def avg(self, distinct=None):
        return await self._run(self._query.avg, distinct)

    async def min(self):
        return await self._run(self._query.min)

    async def max(self):
        return await self._run(self._query.max)

    async def count(self, distinct=None):
        return await self._run(self._query.count, distinct)

    async def group_concat(self, sep=None, distinct=None):
        return await self._run(self._query.group_concat, sep, distinct)

    async def show(self, width=None, stream=None):
        return await self._run(self._query.show, width, stream)

    async def to_json(self, *args, **kwargs):
        return await self._run(self._query.to_json, *args, **kwargs)

    async def random(self, limit):
        query = self._query.order_by("random()")
        result = await self._run(query.fetch, limit, None)
        return list(result)

    def limit(self, limit=None, offset=None):
        return type(self)(self._query, limit, offset)

    def page(self, pagenum, pagesize=10):
        return type(self)(self._query, pagesize, (pagenum - 1) * pagesize)

    def get_sql(self):
        return self._query.get_sql()

    def __getattr__(self, name):
        if name.startswith("_"):
            return getattr(self._query, name)
        if name not in self._query_methods:
            raise AttributeError(name)
        method = getattr(self._query, name)

        def invoke(*args, **kwargs):
            return type(self)(method(*args, **kwargs), self._limit, self._offset)

        return invoke

    @staticmethod
    def _implicit(method, terminal="fetch"):
        raise AsyncImplicitIOError(
            "Async queries do not perform I/O from %s; use `await query.%s()`"
            % (method, terminal)
        )

    def __iter__(self):
        self._implicit("iteration")

    def __len__(self):
        self._implicit("len()")

    def __bool__(self):
        self._implicit("bool()", "exists")

    def __getitem__(self, key):
        self._implicit("indexing or slicing")


class AsyncSetInstance(core.SetInstance):
    async def load(self):
        database = self._obj_._database_
        await database._run_sync(core.SetInstance.load, self)
        return self

    async def count(self):
        database = self._obj_._database_
        return await database._run_sync(core.SetInstance.count, self)

    async def is_empty(self):
        database = self._obj_._database_
        return await database._run_sync(core.SetInstance.is_empty, self)

    def select(self, *args, **kwargs):
        return _AsyncQuery(core.SetInstance.select(self, *args, **kwargs))

    filter = select


class AsyncEntityMeta(core.EntityMeta):
    def __getitem__(cls, key):  # noqa: N805 - metaclass method
        raise AsyncImplicitIOError(
            "Async entity lookup cannot use indexing; use `await Entity.get(...)`"
        )

    def select(cls, *args, **kwargs):  # noqa: N805 - metaclass method
        return _AsyncQuery(core.EntityMeta.select(cls, *args, **kwargs))

    async def get(cls, *args, **kwargs):  # noqa: N805 - metaclass method
        return await cls._database_._run_sync(core.EntityMeta.get, cls, *args, **kwargs)

    async def exists(cls, *args, **kwargs):  # noqa: N805 - metaclass method
        return await cls._database_._run_sync(
            core.EntityMeta.exists, cls, *args, **kwargs
        )

    async def get_for_update(cls, *args, **kwargs):  # noqa: N805
        return await cls._database_._run_sync(
            core.EntityMeta.get_for_update, cls, *args, **kwargs
        )

    def get_by_sql(cls, sql, globals=None, locals=None):  # noqa: N805
        if globals is None:
            frame = sys._getframe(1)
            globals, locals = frame.f_globals, frame.f_locals
        return cls._database_._run_sync(
            core.EntityMeta.get_by_sql, cls, sql, globals, locals
        )

    def select_by_sql(cls, sql, globals=None, locals=None):  # noqa: N805
        if globals is None:
            frame = sys._getframe(1)
            globals, locals = frame.f_globals, frame.f_locals
        return cls._database_._run_sync(
            core.EntityMeta.select_by_sql, cls, sql, globals, locals
        )

    async def select_random(cls, limit):  # noqa: N805 - metaclass method
        return await cls._database_._run_sync(core.EntityMeta.select_random, cls, limit)

    def _get_set_wrapper_subclass_(cls):  # noqa: N805 - metaclass method
        result_cls = cls._set_wrapper_subclass_
        if result_cls is None:
            mixin = cls._get_propagation_mixin_()
            cls_name = cls.__name__ + "Set"
            result_cls = type(cls_name, (AsyncSetInstance, mixin), {})
            cls._set_wrapper_subclass_ = result_cls
        return result_cls


# Pony's type normalizer deliberately avoids importing core.EntityMeta in order to
# break an import cycle and identifies entity metaclasses by this canonical name.
AsyncEntityMeta.__name__ = "EntityMeta"


class _AsyncEntityMixin:
    __slots__ = ()

    async def load(self, *attrs):
        await self._database_._run_sync(core.Entity.load, self, *attrs)
        return self

    async def delete(self):
        return await self._database_._run_sync(core.Entity.delete, self)

    async def set(self, **kwargs):
        return await self._database_._run_sync(core.Entity.set, self, **kwargs)

    async def flush(self):
        return await self._database_._run_sync(core.Entity.flush, self)

    async def find_updated_attributes(self):
        return await self._database_._run_sync(
            core.Entity.find_updated_attributes, self
        )


class Database(core.Database):
    """Pony database bound explicitly to an asyncio provider."""

    _is_async_ = True

    def __init__(self, *args, **kwargs):
        if args or kwargs:
            raise TypeError(
                "pony.orm.asyncio.Database does not accept connection arguments; "
                "create it first and then use `await db.bind(...)`"
            )
        super().__init__()
        self.Entity = type.__new__(
            AsyncEntityMeta, "Entity", (_AsyncEntityMixin, core.Entity), {}
        )
        self.Entity._database_ = self
        self._async_last_sql = None
        self._async_stats = {}
        self._async_loop = None

    async def bind(self, *args, **kwargs):
        if self.provider is not None:
            raise core.BindingError("Database object is already bound")
        if len(args) == 1 and not kwargs and hasattr(args[0], "keys"):
            args, kwargs = (), dict(args[0])
        else:
            kwargs = dict(kwargs)
        if args:
            provider_name, provider_args = args[0], args[1:]
        else:
            try:
                provider_name = kwargs.pop("provider")
            except KeyError:
                raise TypeError("Database provider is not specified") from None
            provider_args = ()
        if not isinstance(provider_name, str):
            raise TypeError("Async database provider name must be a string")
        self._async_loop = _asyncio.get_running_loop()
        provider_name, provider = make_provider(
            self, provider_name, provider_args, kwargs
        )
        try:
            await provider.initialize()
        except BaseException:
            try:
                await provider.async_disconnect()
            except BaseException:
                pass
            raise
        self.provider_name = provider_name
        self.provider = provider

    def _bridge(self, awaitable):
        loop = self._async_loop
        if loop is None or loop.is_closed():
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise core.ConnectionClosedError("Async database event loop is closed")
        if _current_task() is not None:
            if inspect.iscoroutine(awaitable):
                awaitable.close()
            raise AsyncImplicitIOError(
                "A synchronous ORM path attempted async I/O; use an awaitable terminal method"
            )
        future = _asyncio.run_coroutine_threadsafe(awaitable, loop)
        state = _session_var.get()
        if state is not None:
            state.add_pending(future)
        try:
            return future.result()
        finally:
            if state is not None:
                state.remove_pending(future)

    async def _run_on_connect(self, connection, provider_name):
        for function, selected_provider in self._on_connect_funcs:
            if selected_provider and selected_provider != provider_name:
                continue
            result = function(self, connection)
            if inspect.isawaitable(result):
                await result
        commit_method = getattr(connection, "commit", None)
        if commit_method is not None:
            result = commit_method()
            if inspect.isawaitable(result):
                await result

    def call_on_connect(self, connection):
        # Async pools execute callbacks before publishing a new connection.
        return None

    def _async_io_is_allowed(self):
        return _io_allowed.get()

    def _raise_implicit_lazy_load(self, subject):
        raise AsyncLazyLoadError(
            "Implicit lazy loading is disabled for pony.orm.asyncio.Database "
            "(%s); use "
            "`await entity.load(...)` or `await collection.load()`" % subject
        )

    def _get_cache(self):
        if self.provider is None:
            raise core.MappingError("Database object is not bound with a provider yet")
        state = _require_state()
        cache = state.db2cache.get(self)
        if cache is not None:
            return cache
        old_session = core.local.db_session
        try:
            core.local.db_session = state.options
            cache = core.SessionCache(self)
        finally:
            core.local.db_session = old_session
        state.db2cache[self] = cache
        return cache

    def _exec_sql(self, *args, **kwargs):
        if not _io_allowed.get():
            raise AsyncImplicitIOError(
                "Synchronous database I/O is disabled; use an awaitable ORM operation"
            )
        return core.Database._exec_sql(self, *args, **kwargs)

    def _exec_raw_sql(self, *args, **kwargs):
        if not _io_allowed.get():
            raise AsyncImplicitIOError(
                "Raw SQL on pony.orm.asyncio.Database must be awaited"
            )
        return core.Database._exec_raw_sql(self, *args, **kwargs)

    def _run_sync(self, function, *args, **kwargs):
        state = _require_state()
        return _run_sync_operation(state, function, *args, **kwargs)

    def _update_local_stat(self, sql, query_start_time):
        self._async_last_sql = sql

    @property
    def last_sql(self):
        return self._async_last_sql

    async def generate_mapping(
        self, filename=None, check_tables=True, create_tables=False
    ):
        core.Database.generate_mapping(
            self, filename=filename, check_tables=False, create_tables=False
        )
        if create_tables:
            await self.create_tables(check_tables=check_tables)
        elif check_tables:
            await self.check_tables()

    async def _schema_operation(self, function, ddl):
        state = _session_var.get()
        if state is None:
            options = db_session(ddl=ddl)
            async with options:
                return await self._run_sync(function)
        if ddl and not state.options.ddl:
            raise core.TransactionError(
                "Cannot start ddl transaction inside non-ddl transaction"
            )
        return await self._run_sync(function)

    async def create_tables(self, check_tables=False):
        if self.schema is None:
            raise core.MappingError("No mapping was generated for the database")

        def operation():
            cache = self._get_cache()
            connection = cache.prepare_connection_for_query_execution()
            self.schema.create_tables(self.provider, connection)
            if check_tables:
                self.schema.check_tables(self.provider, connection)

        return await self._schema_operation(operation, ddl=True)

    async def check_tables(self):
        if self.schema is None:
            raise core.MappingError("No mapping was generated for the database")

        def operation():
            cache = self._get_cache()
            connection = cache.prepare_connection_for_query_execution()
            self.schema.check_tables(self.provider, connection)

        return await self._schema_operation(operation, ddl=False)

    async def drop_all_tables(self, with_all_data=False):
        if self.schema is None:
            raise core.ERDiagramError("No mapping was generated for the database")
        return await self._schema_operation(
            lambda: self._drop_tables(self.schema.tables, True, with_all_data),
            ddl=True,
        )

    async def drop_table(self, table_name, if_exists=False, with_all_data=False):
        return await self._schema_operation(
            lambda: self._drop_tables(
                [table_name], if_exists, with_all_data, try_normalized=True
            ),
            ddl=True,
        )

    async def get_connection(self):
        connection = await self._run_sync(core.Database.get_connection, self)
        return connection._raw

    async def disconnect(self):
        if _session_var.get() is not None:
            raise core.TransactionError(
                "disconnect() cannot be called inside db_session"
            )
        provider = self.provider
        if provider is None:
            return
        await provider.async_disconnect()

    def execute(self, sql, globals=None, locals=None):
        if globals is None:
            frame = sys._getframe(1)
            globals, locals = frame.f_globals, frame.f_locals
        return self._run_sync(core.Database.execute, self, sql, globals, locals)

    def select(self, sql, globals=None, locals=None):
        if globals is None:
            frame = sys._getframe(1)
            globals, locals = frame.f_globals, frame.f_locals
        return self._run_sync(core.Database.select, self, sql, globals, locals)

    def get(self, sql, globals=None, locals=None):
        if globals is None:
            frame = sys._getframe(1)
            globals, locals = frame.f_globals, frame.f_locals
        return self._run_sync(core.Database.get, self, sql, globals, locals)

    def exists(self, sql, globals=None, locals=None):
        if globals is None:
            frame = sys._getframe(1)
            globals, locals = frame.f_globals, frame.f_locals
        return self._run_sync(core.Database.exists, self, sql, globals, locals)

    def insert(self, table_name, returning=None, **kwargs):
        return self._run_sync(
            core.Database.insert, self, table_name, returning, **kwargs
        )

    async def flush(self):
        return await self._run_sync(core.Database.flush, self)

    async def commit(self):
        return await self._run_sync(core.Database.commit, self)

    async def rollback(self):
        return await self._run_sync(core.Database.rollback, self)


def select(*args):
    return _AsyncQuery(core.select(*args))


def get(*args):
    return select(*args).get()


def exists(*args):
    return select(*args).exists()


def delete(*args, **kwargs):
    return select(*args).delete(**kwargs)


async def flush():
    state = _require_state()
    return await _run_sync_operation(state, core.flush)


async def commit():
    state = _require_state()
    return await _run_sync_operation(state, core.commit)


async def rollback():
    state = _require_state()
    return await _run_sync_operation(state, core.rollback)
