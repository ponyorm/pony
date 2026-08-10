"""Native asyncio API for Pony ORM.

Query construction and entity manipulation stay synchronous.  Every operation
which can touch a database is explicit and awaitable.
"""

import asyncio as _asyncio
import contextvars
import inspect
import logging
import sys
import warnings
import weakref
from collections import deque
from functools import wraps

from pony.orm import core
from pony.orm.asyncio.providers import (
    AsyncPoolError,
    AsyncPoolQueueFull,
    AsyncPoolTimeout,
    AsyncQueryTimeout,
    make_provider,
)
from pony.orm.asyncio.transport import run_sync

__all__ = [
    "Database",
    "AsyncImplicitIOError",
    "AsyncLazyLoadError",
    "AsyncPoolError",
    "AsyncPoolQueueFull",
    "AsyncPoolTimeout",
    "AsyncQueryTimeout",
    "AsyncSessionConcurrencyError",
    "AsyncTransactionTimeout",
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


class AsyncTransactionTimeout(core.TransactionError):
    """The complete async transaction exceeded its configured deadline."""


_session_var = contextvars.ContextVar("pony_async_session", default=None)
_io_allowed = contextvars.ContextVar("pony_async_io_allowed", default=False)


def _current_task():
    try:
        return _asyncio.current_task()
    except RuntimeError:
        return None


def _driver_requested_retry(exc):
    """Find a safe driver retry marker through Pony's exception wrappers."""

    pending = [exc]
    seen = set()
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if getattr(current, "should_retry", False):
            return True
        pending.append(getattr(current, "original_exc", None))
        pending.append(getattr(current, "__cause__", None))
        pending.append(getattr(current, "__context__", None))
        nested = getattr(current, "exceptions", ())
        for item in nested:
            if isinstance(item, tuple) and len(item) == 3:
                pending.append(item[1])
            else:
                pending.append(item)
    return False


class _SessionState:
    def __init__(self, options):
        self.options = options
        self.owner = _current_task()
        self.depth = 1
        self.db2cache = {}
        self.debug = (
            core.local.debug if options.sql_debug is None else options.sql_debug
        )
        self.show_values = (
            False if options.show_values is None else options.show_values
        )
        self.debug_stack = []
        self.operation_lock = _asyncio.Lock()
        self.cleanup_tasks = set()
        self.timeout_handle = None
        self.timed_out = False
        self.local_values = {
            "debug": self.debug,
            "show_values": self.show_values,
            "debug_stack": [],
            "db2cache": self.db2cache,
            "db_context_counter": self.depth,
            "db_session": self.options,
            "prefetch_context_stack": [],
            "current_user": core.local.current_user,
            "perms_context": None,
            "user_groups_cache": {},
            "user_roles_cache": core.defaultdict(dict),
        }
        timeout = options.transaction_timeout
        if timeout is not None:
            loop = _asyncio.get_running_loop()

            def cancel_owner():
                self.timed_out = True
                if self.owner is not None and not self.owner.done():
                    self.owner.cancel("Pony async transaction timed out")

            self.timeout_handle = loop.call_later(timeout, cancel_owner)

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


_local_names = (
    "debug",
    "show_values",
    "debug_stack",
    "db2cache",
    "db_context_counter",
    "db_session",
    "prefetch_context_stack",
    "current_user",
    "perms_context",
    "user_groups_cache",
    "user_roles_cache",
)


def _require_state():
    state = _session_var.get()
    if state is None:
        raise core.TransactionError(
            "pony.orm.asyncio.db_session is required when working with "
            "pony.orm.asyncio.Database"
        )
    state.check_owner()
    return state


class _OrmLocalContext:
    """Swap Pony's legacy thread-local state at every async suspension."""

    def __init__(self, state):
        self.state = state
        self.previous = None
        self.io_token = None

    def enter(self):
        if self.previous is not None:
            return
        local = core.local
        self.previous = {name: getattr(local, name) for name in _local_names}
        values = self.state.local_values
        values["db_context_counter"] = self.state.depth
        values["debug"] = self.state.debug
        values["show_values"] = self.state.show_values
        for name in _local_names:
            setattr(local, name, values[name])
        self.io_token = _io_allowed.set(True)

    def exit(self):
        if self.previous is None:
            return
        local = core.local
        values = self.state.local_values
        for name in _local_names:
            values[name] = getattr(local, name)
        for name, value in self.previous.items():
            setattr(local, name, value)
        _io_allowed.reset(self.io_token)
        self.io_token = None
        self.previous = None


async def _run_sync_operation(state, func, *args, **kwargs):
    state.check_owner()
    async with state.operation_lock:
        orm_context = _OrmLocalContext(state)

        def invoke():
            orm_context.enter()
            try:
                return func(*args, **kwargs)
            finally:
                orm_context.exit()

        return await run_sync(
            invoke,
            before_await=orm_context.exit,
            after_await=orm_context.enter,
        )


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
        if rollback_only:
            return await _asyncio.shield(task)
        # Commit remains cancellable.  Cancellation is thrown into the native
        # driver wait, after which ``cleanup`` rolls back (or drops an uncertain
        # connection) before propagating CancelledError.
        return await task
    except _asyncio.CancelledError:
        if not rollback_only and not task.done():
            task.cancel()
        try:
            await _asyncio.shield(task)
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
        "readonly",
        "deferrable",
        "isolation",
        "retry",
        "retry_exceptions",
        "serializable",
        "show_values",
        "sql_debug",
        "strict",
        "transaction_timeout",
    )

    def __init__(
        self,
        retry=0,
        immediate=False,
        ddl=False,
        serializable=False,
        strict=False,
        optimistic=True,
        readonly=False,
        deferrable=False,
        isolation=None,
        transaction_timeout=None,
        retry_exceptions=(core.OptimisticCheckError,),
        allowed_exceptions=(),
        sql_debug=None,
        show_values=None,
    ):
        if type(retry) is not int or retry < 0:
            raise TypeError("retry must be a non-negative integer")
        if retry and ddl:
            raise TypeError("ddl and retry cannot be used together")
        if isolation is not None:
            isolation = isolation.lower().replace(" ", "_").replace("-", "_")
            if isolation not in (
                "read_committed",
                "repeatable_read",
                "serializable",
            ):
                raise ValueError("Unsupported isolation level: %r" % isolation)
        if serializable:
            if isolation not in (None, "serializable"):
                raise TypeError("serializable=True conflicts with isolation=%r" % isolation)
            isolation = "serializable"
        serializable = isolation == "serializable"
        if deferrable and (not readonly or not serializable):
            raise TypeError(
                "deferrable transactions must be readonly and serializable"
            )
        if transaction_timeout is not None:
            transaction_timeout = float(transaction_timeout)
            if transaction_timeout <= 0:
                raise ValueError("transaction_timeout must be positive")
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
        self.isolation = isolation
        self.readonly = bool(readonly)
        self.deferrable = bool(deferrable)
        self.transaction_timeout = transaction_timeout
        self.immediate = (
            immediate or ddl or isolation is not None or readonly or not optimistic
        )
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
                attempt_databases = set()
                try:
                    async with self:
                        try:
                            return await function(*f_args, **f_kwargs)
                        finally:
                            attempt_state = _session_var.get()
                            if attempt_state is not None:
                                attempt_databases.update(attempt_state.db2cache)
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
                    should_retry = should_retry or _driver_requested_retry(exc)
                    if not should_retry or attempt == self.retry:
                        raise
                    for database in attempt_databases:
                        database._async_stats["retries"] += 1
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
        if self.isolation is not None and self.isolation != outer.isolation:
            raise core.TransactionError(
                "Cannot change transaction isolation in a nested db_session"
            )
        if self.readonly and not outer.readonly:
            raise core.TransactionError(
                "Cannot change readonly mode in a nested db_session"
            )
        if self.transaction_timeout is not None:
            warnings.warn(
                "transaction_timeout is ignored for a nested db_session",
                core.PonyRuntimeWarning,
                stacklevel=2,
            )
        state.debug_stack.append((state.debug, state.show_values))
        if self.sql_debug is not None:
            state.debug = self.sql_debug
            state.show_values = (
                False if self.show_values is None else self.show_values
            )
        state.depth += 1
        return self

    async def __aexit__(self, exc_type=None, exc=None, tb=None):
        state = _require_state()
        state.depth -= 1
        if state.depth:
            state.debug, state.show_values = state.debug_stack.pop()
            return False
        timed_out = False
        try:
            try:
                if exc_type is None:
                    can_commit = True
                elif issubclass(
                    exc_type,
                    (_asyncio.CancelledError, AsyncQueryTimeout, AsyncTransactionTimeout),
                ):
                    can_commit = False
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
            except _asyncio.CancelledError as cleanup_cancel:
                if state.timed_out:
                    raise AsyncTransactionTimeout(
                        "Async transaction exceeded %.3f seconds"
                        % state.options.transaction_timeout
                    ) from cleanup_cancel
                if exc_type is None or can_commit:
                    raise
            except BaseException:
                # Preserve the user's exception (especially CancelledError) after
                # rollback/drop cleanup, matching db_session semantics.
                if exc_type is None or can_commit:
                    raise
        finally:
            timed_out = state.timed_out
            if state.timeout_handle is not None:
                state.timeout_handle.cancel()
                state.timeout_handle = None
            _session_var.set(None)
        if timed_out and exc_type is _asyncio.CancelledError:
            raise AsyncTransactionTimeout(
                "Async transaction exceeded %.3f seconds"
                % state.options.transaction_timeout
            ) from exc
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
        database = self._database()

        def operation():
            items = self._query._actual_fetch(self._limit, self._offset)
            return core.Database.to_json(database, items, *args, **kwargs)

        return await self._run(operation)

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

    async def get_by_sql(cls, sql, globals=None, locals=None):  # noqa: N805
        if globals is None:
            frame = sys._getframe(1)
            globals, locals = frame.f_globals, frame.f_locals
        return await cls._database_._run_sync(
            core.EntityMeta.get_by_sql, cls, sql, globals, locals
        )

    async def select_by_sql(cls, sql, globals=None, locals=None):  # noqa: N805
        if globals is None:
            frame = sys._getframe(1)
            globals, locals = frame.f_globals, frame.f_locals
        return await cls._database_._run_sync(
            core.EntityMeta.select_by_sql, cls, sql, globals, locals
        )

    async def select_random(cls, limit):  # noqa: N805 - metaclass method
        return await cls._database_._run_sync(core.EntityMeta.select_random, cls, limit)

    async def drop_table(cls, with_all_data=False):  # noqa: N805
        return await cls._database_.drop_table(
            cls._table_, if_exists=True, with_all_data=with_all_data
        )

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

    async def to_dict(self, *args, **kwargs):
        return await self._database_._run_sync(
            core.Entity.to_dict, self, *args, **kwargs
        )

    async def to_json(self, *args, **kwargs):
        database = self._database_
        return await database._run_sync(
            core.Database.to_json, database, self, *args, **kwargs
        )


class AsyncSessionCache(core.SessionCache):
    """Task-owned Unit of Work backed by a native async provider connection.

    The inherited identity-map and change-tracking algorithms are CPU-only.  Its
    connection methods enter provider awaitables through the native runtime.
    Explicit async methods are useful to session cleanup and internal callers.
    """

    async def aflush(self):
        state = _require_state()
        return await _run_sync_operation(state, core.SessionCache.flush, self)

    async def acommit(self):
        state = _require_state()
        return await _run_sync_operation(state, core.SessionCache.commit, self)

    async def arollback(self):
        state = _require_state()
        return await _run_sync_operation(state, core.SessionCache.rollback, self)

    async def arelease(self):
        state = _require_state()
        return await _run_sync_operation(state, core.SessionCache.release, self)


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
        self._async_stats = {
            "queries": 0,
            "query_seconds": 0.0,
            "slow_queries": 0,
            "query_timeouts": 0,
            "cancellations": 0,
            "reconnects": 0,
            "retries": 0,
        }
        self._query_history = deque(maxlen=100)
        self._slow_query_threshold = None
        self._query_observers = []
        self._observer_tasks = set()
        self._closing = False
        self._caches = weakref.WeakSet()
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
        self._closing = False
        self._async_loop = _asyncio.get_running_loop()
        self._slow_query_threshold = kwargs.pop("slow_query_threshold", None)
        if self._slow_query_threshold is not None:
            self._slow_query_threshold = float(self._slow_query_threshold)
            if self._slow_query_threshold <= 0:
                raise ValueError("slow_query_threshold must be positive")
        query_observer = kwargs.pop("query_observer", None)
        if query_observer is not None:
            if not callable(query_observer):
                raise TypeError("query_observer must be callable")
            self._query_observers.append(query_observer)
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
        self._check_loop()
        if self.provider is None:
            raise core.MappingError("Database object is not bound with a provider yet")
        state = _require_state()
        cache = state.db2cache.get(self)
        if cache is not None:
            return cache
        old_session = core.local.db_session
        try:
            core.local.db_session = state.options
            cache = AsyncSessionCache(self)
        finally:
            core.local.db_session = old_session
        state.db2cache[self] = cache
        self._caches.add(cache)
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
        self._check_loop()
        state = _require_state()
        return _run_sync_operation(state, function, *args, **kwargs)

    def _check_loop(self):
        loop = self._async_loop
        if loop is None:
            return
        try:
            current = _asyncio.get_running_loop()
        except RuntimeError:
            raise core.TransactionError(
                "Async Database must be used from its event loop"
            ) from None
        if current is not loop:
            raise core.TransactionError(
                "Async Database cannot be shared between event loops"
            )

    def _update_local_stat(self, sql, query_start_time):
        self._async_last_sql = sql

    def _record_query(self, sql, duration, outcome="ok"):
        self._async_last_sql = sql
        stats = self._async_stats
        stats["queries"] += 1
        stats["query_seconds"] += duration
        slow = (
            self._slow_query_threshold is not None
            and duration >= self._slow_query_threshold
        )
        if slow:
            stats["slow_queries"] += 1
            logging.getLogger("pony.orm.asyncio").warning(
                "Slow async query (%.3fs): %s", duration, sql
            )
        event = {
            "type": "query",
            "sql": sql,
            "duration": duration,
            "slow": slow,
            "outcome": outcome,
        }
        self._query_history.append(event)
        for observer in tuple(self._query_observers):
            try:
                result = observer(event)
                if inspect.isawaitable(result):
                    async def run_observer(awaitable):
                        token = _session_var.set(None)
                        try:
                            await awaitable
                        finally:
                            _session_var.reset(token)

                    task = _asyncio.create_task(run_observer(result))
                    self._observer_tasks.add(task)

                    def observer_done(done):
                        self._observer_tasks.discard(done)
                        if done.cancelled():
                            return
                        error = done.exception()
                        if error is not None:
                            logging.getLogger("pony.orm.asyncio").error(
                                "Async query observer failed",
                                exc_info=(type(error), error, error.__traceback__),
                            )

                    task.add_done_callback(observer_done)
            except Exception:
                logging.getLogger("pony.orm.asyncio").exception(
                    "Async query observer failed"
                )

    def _record_cancellation(self, timed_out=False):
        self._async_stats["cancellations"] += 1
        if timed_out:
            self._async_stats["query_timeouts"] += 1

    def add_query_observer(self, observer):
        if not callable(observer):
            raise TypeError("observer must be callable")
        self._query_observers.append(observer)

    @property
    def async_stats(self):
        result = dict(self._async_stats)
        provider = self.provider
        pool = getattr(provider, "pool_transport", None) if provider else None
        result["pool"] = pool.metrics.snapshot() if pool is not None else None
        result["active_transactions"] = sum(
            bool(cache.is_alive and cache.in_transaction)
            for cache in tuple(self._caches)
        )
        return result

    @property
    def recent_queries(self):
        return tuple(dict(event) for event in self._query_history)

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
        self._check_loop()
        self._closing = True
        await provider.async_disconnect()
        tasks = tuple(self._observer_tasks)
        if tasks:
            timeout = getattr(provider.pool_transport, "shutdown_timeout", None)
            gather = _asyncio.gather(*tasks, return_exceptions=True)
            try:
                if timeout is None:
                    await gather
                else:
                    await _asyncio.wait_for(gather, timeout)
            except _asyncio.TimeoutError:
                for task in tasks:
                    task.cancel()
                await _asyncio.gather(*tasks, return_exceptions=True)

    async def health_check(self):
        if self.provider is None or self._closing:
            return False
        try:
            async with db_session:
                return bool(await self.select("1"))
        except BaseException:
            return False

    async def readiness_check(self):
        if not await self.health_check():
            return False
        pool = self.provider.pool_transport
        metrics = pool.metrics
        return not pool._closed and metrics.waiting < metrics.max_waiters

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

    async def to_json(self, data, *args, **kwargs):
        return await self._run_sync(
            core.Database.to_json, self, data, *args, **kwargs
        )

    async def from_json(self, changes, observer=None):
        operation = inspect.unwrap(core.Database.from_json)
        if _session_var.get() is None:
            async with db_session:
                return await self._run_sync(operation, self, changes, observer)
        return await self._run_sync(operation, self, changes, observer)

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
