from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2, izip, imap, iteritems, itervalues, items_list, values_list, xrange, cmp, \
                            basestring, unicode, buffer, int_types, builtins, pickle, with_metaclass

import io, json, re, sys, types, datetime, logging, itertools, warnings
from operator import attrgetter, itemgetter
from itertools import chain, starmap, repeat
from time import time
from decimal import Decimal
from random import shuffle, randint, random
from threading import Lock, RLock, currentThread as current_thread, _MainThread
from contextlib import contextmanager
from collections import defaultdict
from hashlib import md5
from inspect import isgeneratorfunction

from pony.thirdparty.compiler import ast, parse

import pony
from pony import options
from pony.orm.decompiling import decompile
from pony.orm.ormtypes import LongStr, LongUnicode, numeric_types, RawSQL, get_normalized_type_of, Json
from pony.orm.asttranslation import ast2src, create_extractors, TranslationError
from pony.orm.dbapiprovider import (
    DBAPIProvider, DBException, Warning, Error, InterfaceError, DatabaseError, DataError,
    OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError
    )
from pony import utils
from pony.utils import localbase, decorator, cut_traceback, throw, reraise, truncate_repr, get_lambda_args, \
     deprecated, import_module, parse_expr, is_ident, tostring, strjoin, concat

__all__ = '''
    pony

    DBException RowNotFound MultipleRowsFound TooManyRowsFound

    Warning Error InterfaceError DatabaseError DataError OperationalError
    IntegrityError InternalError ProgrammingError NotSupportedError

    OrmError ERDiagramError DBSchemaError MappingError
    TableDoesNotExist TableIsNotEmpty ConstraintError CacheIndexError PermissionError
    ObjectNotFound MultipleObjectsFoundError TooManyObjectsFoundError OperationWithDeletedObjectError
    TransactionError ConnectionClosedError TransactionIntegrityError IsolationError CommitException RollbackException
    UnrepeatableReadError OptimisticCheckError UnresolvableCyclicDependency UnexpectedError DatabaseSessionIsOver
    DatabaseContainsIncorrectValue DatabaseContainsIncorrectEmptyValue

    TranslationError ExprEvalError

    RowNotFound MultipleRowsFound TooManyRowsFound

    Database sql_debug show

    PrimaryKey Required Optional Set Discriminator
    composite_key composite_index
    flush commit rollback db_session with_transaction

    LongStr LongUnicode Json

    select left_join get exists delete

    count sum min max avg distinct

    JOIN desc concat raw_sql

    buffer unicode

    get_current_user set_current_user perm has_perm
    get_user_groups get_user_roles get_object_labels
    user_groups_getter user_roles_getter obj_labels_getter
    '''.split()

debug = False
suppress_debug_change = False

def sql_debug(value):
    global debug
    if not suppress_debug_change: debug = value

orm_logger = logging.getLogger('pony.orm')
sql_logger = logging.getLogger('pony.orm.sql')

orm_log_level = logging.INFO

def log_orm(msg):
    if logging.root.handlers:
        orm_logger.log(orm_log_level, msg)
    else:
        print(msg)

def log_sql(sql, arguments=None):
    if type(arguments) is list:
        sql = 'EXECUTEMANY (%d)\n%s' % (len(arguments), sql)
    if logging.root.handlers:
        sql_logger.log(orm_log_level, sql)  # arguments can hold sensitive information
    else:
        print(sql)
        if not arguments: pass
        elif type(arguments) is list:
            for args in arguments: print(args2str(args))
        else: print(args2str(arguments))
        print()

def args2str(args):
    if isinstance(args, (tuple, list)):
        return '[%s]' % ', '.join(imap(repr, args))
    elif isinstance(args, dict):
        return '{%s}' % ', '.join('%s:%s' % (repr(key), repr(val)) for key, val in sorted(iteritems(args)))

adapted_sql_cache = {}
string2ast_cache = {}

class OrmError(Exception): pass

class ERDiagramError(OrmError): pass
class DBSchemaError(OrmError): pass
class MappingError(OrmError): pass

class TableDoesNotExist(OrmError): pass
class TableIsNotEmpty(OrmError): pass

class ConstraintError(OrmError): pass
class CacheIndexError(OrmError): pass

class RowNotFound(OrmError): pass
class MultipleRowsFound(OrmError): pass
class TooManyRowsFound(OrmError): pass

class PermissionError(OrmError): pass

class ObjectNotFound(OrmError):
    def __init__(exc, entity, pkval=None):
        if pkval is not None:
            if type(pkval) is tuple:
                pkval = ','.join(imap(repr, pkval))
            else: pkval = repr(pkval)
            msg = '%s[%s]' % (entity.__name__, pkval)
        else: msg = entity.__name__
        OrmError.__init__(exc, msg)
        exc.entity = entity
        exc.pkval = pkval

class MultipleObjectsFoundError(OrmError): pass
class TooManyObjectsFoundError(OrmError): pass
class OperationWithDeletedObjectError(OrmError): pass
class TransactionError(OrmError): pass
class ConnectionClosedError(TransactionError): pass

class TransactionIntegrityError(TransactionError):
    def __init__(exc, msg, original_exc=None):
        Exception.__init__(exc, msg)
        exc.original_exc = original_exc

class CommitException(TransactionError):
    def __init__(exc, msg, exceptions):
        Exception.__init__(exc, msg)
        exc.exceptions = exceptions

class PartialCommitException(TransactionError):
    def __init__(exc, msg, exceptions):
        Exception.__init__(exc, msg)
        exc.exceptions = exceptions

class RollbackException(TransactionError):
    def __init__(exc, msg, exceptions):
        Exception.__init__(exc, msg)
        exc.exceptions = exceptions

class DatabaseSessionIsOver(TransactionError): pass
TransactionRolledBack = DatabaseSessionIsOver

class IsolationError(TransactionError): pass
class   UnrepeatableReadError(IsolationError): pass
class   OptimisticCheckError(IsolationError): pass
class UnresolvableCyclicDependency(TransactionError): pass

class UnexpectedError(TransactionError):
    def __init__(exc, msg, original_exc):
        Exception.__init__(exc, msg)
        exc.original_exc = original_exc

class ExprEvalError(TranslationError):
    def __init__(exc, src, cause):
        assert isinstance(cause, Exception)
        msg = '%s raises %s: %s' % (src, type(cause).__name__, str(cause))
        TranslationError.__init__(exc, msg)
        exc.cause = cause

class OptimizationFailed(Exception):
    pass  # Internal exception, cannot be encountered in user code

class DatabaseContainsIncorrectValue(RuntimeWarning):
    pass

class DatabaseContainsIncorrectEmptyValue(DatabaseContainsIncorrectValue):
    pass

def adapt_sql(sql, paramstyle):
    result = adapted_sql_cache.get((sql, paramstyle))
    if result is not None: return result
    pos = 0
    result = []
    args = []
    kwargs = {}
    if paramstyle in ('format', 'pyformat'): sql = sql.replace('%', '%%')
    while True:
        try: i = sql.index('$', pos)
        except ValueError:
            result.append(sql[pos:])
            break
        result.append(sql[pos:i])
        if sql[i+1] == '$':
            result.append('$')
            pos = i+2
        else:
            try: expr, _ = parse_expr(sql, i+1)
            except ValueError:
                raise # TODO
            pos = i+1 + len(expr)
            if expr.endswith(';'): expr = expr[:-1]
            compile(expr, '<?>', 'eval')  # expr correction check
            if paramstyle == 'qmark':
                args.append(expr)
                result.append('?')
            elif paramstyle == 'format':
                args.append(expr)
                result.append('%s')
            elif paramstyle == 'numeric':
                args.append(expr)
                result.append(':%d' % len(args))
            elif paramstyle == 'named':
                key = 'p%d' % (len(kwargs) + 1)
                kwargs[key] = expr
                result.append(':' + key)
            elif paramstyle == 'pyformat':
                key = 'p%d' % (len(kwargs) + 1)
                kwargs[key] = expr
                result.append('%%(%s)s' % key)
            else: throw(NotImplementedError)
    adapted_sql = ''.join(result)
    if args:
        source = '(%s,)' % ', '.join(args)
        code = compile(source, '<?>', 'eval')
    elif kwargs:
        source = '{%s}' % ','.join('%r:%s' % item for item in kwargs.items())
        code = compile(source, '<?>', 'eval')
    else:
        code = compile('None', '<?>', 'eval')
        if paramstyle in ('format', 'pyformat'): sql = sql.replace('%%', '%')
    result = adapted_sql, code
    adapted_sql_cache[(sql, paramstyle)] = result
    return result

num_counter = itertools.count()

class Local(localbase):
    def __init__(local):
        local.db2cache = {}
        local.db_context_counter = 0
        local.db_session = None
        local.current_user = None
        local.perms_context = None
        local.user_groups_cache = {}
        local.user_roles_cache = defaultdict(dict)

local = Local()

def _get_caches():
    return list(sorted((cache for cache in local.db2cache.values()),
                       reverse=True, key=lambda cache : (cache.database.priority, cache.num)))

@cut_traceback
def flush():
    for cache in _get_caches(): cache.flush()

def transact_reraise(exc_class, exceptions):
    cls, exc, tb = exceptions[0]
    new_exc = None
    try:
        msg = " ".join(tostring(arg) for arg in exc.args)
        if not issubclass(cls, TransactionError): msg = '%s: %s' % (cls.__name__, msg)
        new_exc = exc_class(msg, exceptions)
        new_exc.__cause__ = None
        reraise(exc_class, new_exc, tb)
    finally: del exceptions, exc, tb, new_exc

def rollback_and_reraise(exc_info):
    try:
        rollback()
    finally:
        reraise(*exc_info)

@cut_traceback
def commit():
    caches = _get_caches()
    if not caches: return

    try:
        for cache in caches:
            cache.flush()
    except:
        rollback_and_reraise(sys.exc_info())

    primary_cache = caches[0]
    other_caches = caches[1:]
    exceptions = []
    try:
        primary_cache.commit()
    except:
        exceptions.append(sys.exc_info())
        for cache in other_caches:
            try: cache.rollback()
            except: exceptions.append(sys.exc_info())
        transact_reraise(CommitException, exceptions)
    else:
        for cache in other_caches:
            try: cache.commit()
            except: exceptions.append(sys.exc_info())
        if exceptions:
            transact_reraise(PartialCommitException, exceptions)
    finally:
        del exceptions

@cut_traceback
def rollback():
    exceptions = []
    try:
        for cache in _get_caches():
            try: cache.rollback()
            except: exceptions.append(sys.exc_info())
        if exceptions:
            transact_reraise(RollbackException, exceptions)
        assert not local.db2cache
    finally:
        del exceptions

select_re = re.compile(r'\s*select\b', re.IGNORECASE)

class DBSessionContextManager(object):
    __slots__ = 'retry', 'retry_exceptions', 'allowed_exceptions', 'immediate', 'ddl', 'serializable', 'strict'
    def __init__(db_session, retry=0, immediate=False, ddl=False, serializable=False, strict=False,
                 retry_exceptions=(TransactionError,), allowed_exceptions=()):
        if retry is not 0:
            if type(retry) is not int: throw(TypeError,
                "'retry' parameter of db_session must be of integer type. Got: %s" % type(retry))
            if retry < 0: throw(TypeError,
                "'retry' parameter of db_session must not be negative. Got: %d" % retry)
            if ddl: throw(TypeError, "'ddl' and 'retry' parameters of db_session cannot be used together")
        if not callable(allowed_exceptions) and not callable(retry_exceptions):
            for e in allowed_exceptions:
                if e in retry_exceptions: throw(TypeError,
                    'The same exception %s cannot be specified in both '
                    'allowed and retry exception lists simultaneously' % e.__name__)
        db_session.retry = retry
        db_session.ddl = ddl
        db_session.serializable = serializable
        db_session.immediate = immediate or ddl or serializable
        db_session.strict = strict
        db_session.retry_exceptions = retry_exceptions
        db_session.allowed_exceptions = allowed_exceptions
    def __call__(db_session, *args, **kwargs):
        if not args and not kwargs: return db_session
        if len(args) > 1: throw(TypeError,
            'Pass only keyword arguments to db_session or use db_session as decorator')
        if not args: return db_session.__class__(**kwargs)
        if kwargs: throw(TypeError,
            'Pass only keyword arguments to db_session or use db_session as decorator')
        func = args[0]
        if not isgeneratorfunction(func):
            return db_session._wrap_function(func)
        return db_session._wrap_generator_function(func)
    def __enter__(db_session):
        if db_session.retry is not 0: throw(TypeError,
            "@db_session can accept 'retry' parameter only when used as decorator and not as context manager")
        if db_session.ddl: throw(TypeError,
            "@db_session can accept 'ddl' parameter only when used as decorator and not as context manager")
        db_session._enter()
    def _enter(db_session):
        if local.db_session is None:
            assert not local.db_context_counter
            local.db_session = db_session
        elif db_session.serializable and not local.db_session.serializable: throw(TransactionError,
            'Cannot start serializable transaction inside non-serializable transaction')
        local.db_context_counter += 1
    def __exit__(db_session, exc_type=None, exc=None, tb=None):
        local.db_context_counter -= 1
        if local.db_context_counter: return
        assert local.db_session is db_session
        try:
            if exc_type is None: can_commit = True
            elif not callable(db_session.allowed_exceptions):
                can_commit = issubclass(exc_type, tuple(db_session.allowed_exceptions))
            else:
                assert exc is not None # exc can be None in Python 2.6 even if exc_type is not None
                try: can_commit = db_session.allowed_exceptions(exc)
                except: rollback_and_reraise(sys.exc_info())
            if can_commit:
                commit()
                for cache in _get_caches(): cache.release()
                assert not local.db2cache
            else:
                try: rollback()
                except:
                    if exc_type is None: raise  # if exc_type is not None it will be reraised outside of __exit__
        finally:
            del exc, tb
            local.db_session = None
            local.user_groups_cache.clear()
            local.user_roles_cache.clear()
    def _wrap_function(db_session, func):
        def new_func(func, *args, **kwargs):
            if db_session.ddl and local.db_context_counter:
                if isinstance(func, types.FunctionType): func = func.__name__ + '()'
                throw(TransactionError, '%s cannot be called inside of db_session' % func)
            exc = tb = None
            try:
                for i in xrange(db_session.retry+1):
                    db_session._enter()
                    exc_type = exc = tb = None
                    try: return func(*args, **kwargs)
                    except:
                        exc_type, exc, tb = sys.exc_info()
                        retry_exceptions = db_session.retry_exceptions
                        if not callable(retry_exceptions):
                            do_retry = issubclass(exc_type, tuple(retry_exceptions))
                        else:
                            assert exc is not None  # exc can be None in Python 2.6
                            do_retry = retry_exceptions(exc)
                        if not do_retry: raise
                    finally: db_session.__exit__(exc_type, exc, tb)
                reraise(exc_type, exc, tb)
            finally: del exc, tb
        return decorator(new_func, func)
    def _wrap_generator_function(db_session, gen_func):
        for option in ('ddl', 'retry', 'serializable'):
            if getattr(db_session, option, None): throw(TypeError,
                "db_session with `%s` option cannot be applied to generator function" % option)

        def interact(iterator, input=None, exc_info=None):
            if exc_info is None:
                return next(iterator) if input is None else iterator.send(input)

            if exc_info[0] is GeneratorExit:
                close = getattr(iterator, 'close', None)
                if close is not None: close()
                reraise(*exc_info)

            throw_ = getattr(iterator, 'throw', None)
            if throw_ is None: reraise(*exc_info)
            return throw_(*exc_info)

        def new_gen_func(gen_func, *args, **kwargs):
            db2cache_copy = {}

            def wrapped_interact(iterator, input=None, exc_info=None):
                if local.db_session is not None: throw(TransactionError,
                    '@db_session-wrapped generator cannot be used inside another db_session')
                assert not local.db_context_counter and not local.db2cache
                local.db_context_counter = 1
                local.db_session = db_session
                local.db2cache.update(db2cache_copy)
                db2cache_copy.clear()
                try:
                    try:
                        output = interact(iterator, input, exc_info)
                    except StopIteration as e:
                        for cache in _get_caches():
                            if cache.modified or cache.in_transaction: throw(TransactionError,
                                'You need to manually commit() changes before exiting from the generator')
                        raise
                    for cache in _get_caches():
                        if cache.modified or cache.in_transaction: throw(TransactionError,
                            'You need to manually commit() changes before yielding from the generator')
                except:
                    rollback_and_reraise(sys.exc_info())
                else:
                    return output
                finally:
                    db2cache_copy.update(local.db2cache)
                    local.db2cache.clear()
                    local.db_context_counter = 0
                    local.db_session = None

            gen = gen_func(*args, **kwargs)
            iterator = iter(gen)
            output = wrapped_interact(iterator)
            try:
                while True:
                    try:
                        input = yield output
                    except:
                        output = wrapped_interact(iterator, exc_info=sys.exc_info())
                    else:
                        output = wrapped_interact(iterator, input)
            except StopIteration:
                return
        return decorator(new_gen_func, gen_func)

db_session = DBSessionContextManager()

def throw_db_session_is_over(obj, attr):
    throw(DatabaseSessionIsOver, 'Cannot read value of %s.%s: the database session is over'
                                 % (safe_repr(obj), attr.name))

def with_transaction(*args, **kwargs):
    deprecated(3, "@with_transaction decorator is deprecated, use @db_session decorator instead")
    return db_session(*args, **kwargs)

@decorator
def db_decorator(func, *args, **kwargs):
    web = sys.modules.get('pony.web')
    allowed_exceptions = [ web.HttpRedirect ] if web else []
    try:
        with db_session(allowed_exceptions=allowed_exceptions):
            return func(*args, **kwargs)
    except (ObjectNotFound, RowNotFound):
        if web: throw(web.Http404NotFound)
        raise

class Database(object):
    def __deepcopy__(self, memo):
        return self  # Database cannot be cloned by deepcopy()
    @cut_traceback
    def __init__(self, *args, **kwargs):
        # argument 'self' cannot be named 'database', because 'database' can be in kwargs
        self.priority = 0
        self._insert_cache = {}

        # ER-diagram related stuff:
        self._translator_cache = {}
        self._constructed_sql_cache = {}
        self.entities = {}
        self.schema = None
        self.Entity = type.__new__(EntityMeta, 'Entity', (Entity,), {})
        self.Entity._database_ = self

        # Statistics-related stuff:
        self._global_stats = {}
        self._global_stats_lock = RLock()
        self._dblocal = DbLocal()

        self.provider = None
        if args or kwargs: self._bind(*args, **kwargs)
    @cut_traceback
    def bind(self, *args, **kwargs):
        self._bind(*args, **kwargs)
    def _bind(self, *args, **kwargs):
        # argument 'self' cannot be named 'database', because 'database' can be in kwargs
        if self.provider is not None:
            throw(TypeError, 'Database object was already bound to %s provider' % self.provider.dialect)
        if not args:
            throw(TypeError, 'Database provider should be specified as a first positional argument')
        provider, args = args[0], args[1:]
        if isinstance(provider, type) and issubclass(provider, DBAPIProvider):
            provider_cls = provider
        else:
            if not isinstance(provider, basestring): throw(TypeError)
            if provider == 'pygresql': throw(TypeError,
                'Pony no longer supports PyGreSQL module. Please use psycopg2 instead.')
            provider_module = import_module('pony.orm.dbproviders.' + provider)
            provider_cls = provider_module.provider_cls
        self.provider = provider = provider_cls(*args, **kwargs)
    @property
    def last_sql(database):
        return database._dblocal.last_sql
    @property
    def local_stats(database):
        return database._dblocal.stats
    def _update_local_stat(database, sql, query_start_time):
        dblocal = database._dblocal
        dblocal.last_sql = sql
        stats = dblocal.stats
        stat = stats.get(sql)
        if stat is not None: stat.query_executed(query_start_time)
        else: stats[sql] = QueryStat(sql, query_start_time)
    def merge_local_stats(database):
        setdefault = database._global_stats.setdefault
        with database._global_stats_lock:
            for sql, stat in iteritems(database._dblocal.stats):
                global_stat = setdefault(sql, stat)
                if global_stat is not stat: global_stat.merge(stat)
        database._dblocal.stats.clear()
    @property
    def global_stats(database):
        with database._global_stats_lock:
            return {sql: stat.copy() for sql, stat in iteritems(database._global_stats)}
    @property
    def global_stats_lock(database):
        deprecated(3, "global_stats_lock is deprecated, just use global_stats property without any locking")
        return database._global_stats_lock
    @cut_traceback
    def get_connection(database):
        cache = database._get_cache()
        if not cache.in_transaction:
            cache.immediate = True
            cache.prepare_connection_for_query_execution()
            cache.in_transaction = True
        connection = cache.connection
        assert connection is not None
        return connection
    @cut_traceback
    def disconnect(database):
        provider = database.provider
        if provider is None: return
        if local.db_context_counter: throw(TransactionError, 'disconnect() cannot be called inside of db_sesison')
        cache = local.db2cache.get(database)
        if cache is not None: cache.rollback()
        provider.disconnect()
    def _get_cache(database):
        if database.provider is None: throw(MappingError, 'Database object is not bound with a provider yet')
        cache = local.db2cache.get(database)
        if cache is not None: return cache
        if not local.db_context_counter and not (
                pony.MODE == 'INTERACTIVE' and current_thread().__class__ is _MainThread
            ): throw(TransactionError, 'db_session is required when working with the database')
        cache = local.db2cache[database] = SessionCache(database)
        return cache
    @cut_traceback
    def flush(database):
        database._get_cache().flush()
    @cut_traceback
    def commit(database):
        cache = local.db2cache.get(database)
        if cache is not None:
            cache.flush_and_commit()
    @cut_traceback
    def rollback(database):
        cache = local.db2cache.get(database)
        if cache is not None:
            try: cache.rollback()
            except: transact_reraise(RollbackException, [sys.exc_info()])
    @cut_traceback
    def execute(database, sql, globals=None, locals=None):
        return database._exec_raw_sql(sql, globals, locals, frame_depth=3, start_transaction=True)
    def _exec_raw_sql(database, sql, globals, locals, frame_depth, start_transaction=False):
        provider = database.provider
        if provider is None: throw(MappingError, 'Database object is not bound with a provider yet')
        sql = sql[:]  # sql = templating.plainstr(sql)
        if globals is None:
            assert locals is None
            frame_depth += 1
            globals = sys._getframe(frame_depth).f_globals
            locals = sys._getframe(frame_depth).f_locals
        adapted_sql, code = adapt_sql(sql, provider.paramstyle)
        arguments = eval(code, globals, locals)
        return database._exec_sql(adapted_sql, arguments, False, start_transaction)
    @cut_traceback
    def select(database, sql, globals=None, locals=None, frame_depth=0):
        if not select_re.match(sql): sql = 'select ' + sql
        cursor = database._exec_raw_sql(sql, globals, locals, frame_depth + 3)
        max_fetch_count = options.MAX_FETCH_COUNT
        if max_fetch_count is not None:
            result = cursor.fetchmany(max_fetch_count)
            if cursor.fetchone() is not None: throw(TooManyRowsFound)
        else: result = cursor.fetchall()
        if len(cursor.description) == 1: return [ row[0] for row in result ]
        row_class = type("row", (tuple,), {})
        for i, column_info in enumerate(cursor.description):
            column_name = column_info[0]
            if not is_ident(column_name): continue
            if hasattr(tuple, column_name) and column_name.startswith('__'): continue
            setattr(row_class, column_name, property(itemgetter(i)))
        return [ row_class(row) for row in result ]
    @cut_traceback
    def get(database, sql, globals=None, locals=None):
        rows = database.select(sql, globals, locals, frame_depth=3)
        if not rows: throw(RowNotFound)
        if len(rows) > 1: throw(MultipleRowsFound)
        row = rows[0]
        return row
    @cut_traceback
    def exists(database, sql, globals=None, locals=None):
        if not select_re.match(sql): sql = 'select ' + sql
        cursor = database._exec_raw_sql(sql, globals, locals, frame_depth=3)
        result = cursor.fetchone()
        return bool(result)
    @cut_traceback
    def insert(database, table_name, returning=None, **kwargs):
        table_name = database._get_table_name(table_name)
        if database.provider is None: throw(MappingError, 'Database object is not bound with a provider yet')
        query_key = (table_name,) + tuple(kwargs)  # keys are not sorted deliberately!!
        if returning is not None: query_key = query_key + (returning,)
        cached_sql = database._insert_cache.get(query_key)
        if cached_sql is None:
            ast = [ 'INSERT', table_name, kwargs.keys(),
                    [ [ 'PARAM', (i, None, None) ] for i in xrange(len(kwargs)) ], returning ]
            sql, adapter = database._ast2sql(ast)
            cached_sql = sql, adapter
            database._insert_cache[query_key] = cached_sql
        else: sql, adapter = cached_sql
        arguments = adapter(values_list(kwargs))  # order of values same as order of keys
        if returning is not None:
            return database._exec_sql(sql, arguments, returning_id=True, start_transaction=True)
        cursor = database._exec_sql(sql, arguments, start_transaction=True)
        return getattr(cursor, 'lastrowid', None)
    def _ast2sql(database, sql_ast):
        sql, adapter = database.provider.ast2sql(sql_ast)
        return sql, adapter
    def _exec_sql(database, sql, arguments=None, returning_id=False, start_transaction=False):
        cache = database._get_cache()
        if start_transaction: cache.immediate = True
        connection = cache.prepare_connection_for_query_execution()
        cursor = connection.cursor()
        if debug: log_sql(sql, arguments)
        provider = database.provider
        t = time()
        try: new_id = provider.execute(cursor, sql, arguments, returning_id)
        except Exception as e:
            connection = cache.reconnect(e)
            cursor = connection.cursor()
            if debug: log_sql(sql, arguments)
            t = time()
            new_id = provider.execute(cursor, sql, arguments, returning_id)
        if cache.immediate: cache.in_transaction = True
        database._update_local_stat(sql, t)
        if not returning_id: return cursor
        if PY2 and type(new_id) is long: new_id = int(new_id)
        return new_id
    @cut_traceback
    def generate_mapping(database, filename=None, check_tables=True, create_tables=False):
        provider = database.provider
        if provider is None: throw(MappingError, 'Database object is not bound with a provider yet')
        if database.schema: throw(MappingError, 'Mapping was already generated')
        if filename is not None: throw(NotImplementedError)
        schema = database.schema = provider.dbschema_cls(provider)
        entities = list(sorted(database.entities.values(), key=attrgetter('_id_')))
        for entity in entities:
            entity._resolve_attr_types_()
        for entity in entities:
            entity._link_reverse_attrs_()

        def get_columns(table, column_names):
            column_dict = table.column_dict
            return tuple(column_dict[name] for name in column_names)

        for entity in entities:
            entity._get_pk_columns_()
            table_name = entity._table_

            is_subclass = entity._root_ is not entity
            if is_subclass:
                if table_name is not None: throw(NotImplementedError)
                table_name = entity._root_._table_
                entity._table_ = table_name
            elif table_name is None:
                table_name = provider.get_default_entity_table_name(entity)
                entity._table_ = table_name
            else: assert isinstance(table_name, (basestring, tuple))

            table = schema.tables.get(table_name)
            if table is None: table = schema.add_table(table_name)
            elif table.entities:
                for e in table.entities:
                    if e._root_ is not entity._root_:
                        throw(MappingError, "Entities %s and %s cannot be mapped to table %s "
                                           "because they don't belong to the same hierarchy"
                                           % (e, entity, table_name))
            table.entities.add(entity)

            for attr in entity._new_attrs_:
                if attr.is_collection:
                    if not isinstance(attr, Set): throw(NotImplementedError)
                    reverse = attr.reverse
                    if not reverse.is_collection: # many-to-one:
                        if attr.table is not None: throw(MappingError,
                            "Parameter 'table' is not allowed for many-to-one attribute %s" % attr)
                        elif attr.columns: throw(NotImplementedError,
                            "Parameter 'column' is not allowed for many-to-one attribute %s" % attr)
                        continue
                    # many-to-many:
                    if not isinstance(reverse, Set): throw(NotImplementedError)
                    if attr.entity.__name__ > reverse.entity.__name__: continue
                    if attr.entity is reverse.entity and attr.name > reverse.name: continue

                    if attr.table:
                        if not reverse.table: reverse.table = attr.table
                        elif reverse.table != attr.table:
                            throw(MappingError, "Parameter 'table' for %s and %s do not match" % (attr, reverse))
                        table_name = attr.table
                    elif reverse.table: table_name = attr.table = reverse.table
                    else:
                        table_name = provider.get_default_m2m_table_name(attr, reverse)

                    m2m_table = schema.tables.get(table_name)
                    if m2m_table is not None:
                        if not attr.table:
                            seq_counter = itertools.count(2)
                            while m2m_table is not None:
                                new_table_name = table_name + '_%d' % next(seq_counter)
                                m2m_table = schema.tables.get(new_table_name)
                            table_name = new_table_name
                        elif m2m_table.entities or m2m_table.m2m:
                            if isinstance(table_name, tuple): table_name = '.'.join(table_name)
                            throw(MappingError, "Table name '%s' is already in use" % table_name)
                        else: throw(NotImplementedError)
                    attr.table = reverse.table = table_name
                    m2m_table = schema.add_table(table_name)
                    m2m_columns_1 = attr.get_m2m_columns(is_reverse=False)
                    m2m_columns_2 = reverse.get_m2m_columns(is_reverse=True)
                    if m2m_columns_1 == m2m_columns_2: throw(MappingError,
                        'Different column names should be specified for attributes %s and %s' % (attr, reverse))
                    assert len(m2m_columns_1) == len(reverse.converters)
                    assert len(m2m_columns_2) == len(attr.converters)
                    for column_name, converter in izip(m2m_columns_1 + m2m_columns_2, reverse.converters + attr.converters):
                        m2m_table.add_column(column_name, converter.get_sql_type(), converter, True)
                    m2m_table.add_index(None, tuple(m2m_table.column_list), is_pk=True)
                    m2m_table.m2m.add(attr)
                    m2m_table.m2m.add(reverse)
                else:
                    if attr.is_required: pass
                    elif not attr.is_string:
                        if attr.nullable is False:
                            throw(TypeError, 'Optional attribute with non-string type %s must be nullable' % attr)
                        attr.nullable = True
                    elif entity._database_.provider.dialect == 'Oracle':
                        if attr.nullable is False: throw(ERDiagramError,
                            'In Oracle, optional string attribute %s must be nullable' % attr)
                        attr.nullable = True

                    columns = attr.get_columns()  # initializes attr.converters
                    if not attr.reverse and attr.default is not None:
                        assert len(attr.converters) == 1
                        if not callable(attr.default): attr.default = attr.validate(attr.default)
                    assert len(columns) == len(attr.converters)
                    if len(columns) == 1:
                        converter = attr.converters[0]
                        table.add_column(columns[0], converter.get_sql_type(attr),
                                         converter, not attr.nullable, attr.sql_default)
                    elif columns:
                        if attr.sql_type is not None: throw(NotImplementedError,
                            'sql_type cannot be specified for composite attribute %s' % attr)
                        for (column_name, converter) in izip(columns, attr.converters):
                            table.add_column(column_name, converter.get_sql_type(), converter, not attr.nullable)
                    else: pass  # virtual attribute of one-to-one pair
            entity._attrs_with_columns_ = [ attr for attr in entity._attrs_
                                                 if not attr.is_collection and attr.columns ]
            if not table.pk_index:
                if len(entity._pk_columns_) == 1 and entity._pk_attrs_[0].auto: is_pk = "auto"
                else: is_pk = True
                table.add_index(None, get_columns(table, entity._pk_columns_), is_pk)
            for index in entity._indexes_:
                if index.is_pk: continue
                column_names = []
                attrs = index.attrs
                for attr in attrs: column_names.extend(attr.columns)
                index_name = attrs[0].index if len(attrs) == 1 else None
                table.add_index(index_name, get_columns(table, column_names), is_unique=index.is_unique)
            columns = []
            columns_without_pk = []
            converters = []
            converters_without_pk = []
            for attr in entity._attrs_with_columns_:
                columns.extend(attr.columns)  # todo: inheritance
                converters.extend(attr.converters)
                if not attr.is_pk:
                    columns_without_pk.extend(attr.columns)
                    converters_without_pk.extend(attr.converters)
            entity._columns_ = columns
            entity._columns_without_pk_ = columns_without_pk
            entity._converters_ = converters
            entity._converters_without_pk_ = converters_without_pk
        for entity in entities:
            table = schema.tables[entity._table_]
            for attr in entity._new_attrs_:
                if attr.is_collection:
                    reverse = attr.reverse
                    if not reverse.is_collection: continue
                    if not isinstance(attr, Set): throw(NotImplementedError)
                    if not isinstance(reverse, Set): throw(NotImplementedError)
                    m2m_table = schema.tables[attr.table]
                    parent_columns = get_columns(table, entity._pk_columns_)
                    child_columns = get_columns(m2m_table, reverse.columns)
                    m2m_table.add_foreign_key(None, child_columns, table, parent_columns, attr.index)
                    if attr.symmetric:
                        child_columns = get_columns(m2m_table, attr.reverse_columns)
                        m2m_table.add_foreign_key(None, child_columns, table, parent_columns)
                elif attr.reverse and attr.columns:
                    rentity = attr.reverse.entity
                    parent_table = schema.tables[rentity._table_]
                    parent_columns = get_columns(parent_table, rentity._pk_columns_)
                    child_columns = get_columns(table, attr.columns)
                    table.add_foreign_key(None, child_columns, parent_table, parent_columns, attr.index)
                elif attr.index and attr.columns:
                    columns = tuple(imap(table.column_dict.__getitem__, attr.columns))
                    table.add_index(attr.index, columns, is_unique=attr.is_unique)
            entity._initialize_bits_()

        if create_tables: database.create_tables(check_tables)
        elif check_tables: database.check_tables()
    @cut_traceback
    @db_session(ddl=True)
    def drop_table(database, table_name, if_exists=False, with_all_data=False):
        table_name = database._get_table_name(table_name)
        database._drop_tables([ table_name ], if_exists, with_all_data, try_normalized=True)
    def _get_table_name(database, table_name):
        if isinstance(table_name, EntityMeta):
            entity = table_name
            table_name = entity._table_
        elif isinstance(table_name, Set):
            attr = table_name
            table_name = attr.table if attr.reverse.is_collection else attr.entity._table_
        elif isinstance(table_name, Attribute): throw(TypeError,
            "Attribute %s is not Set and doesn't have corresponding table" % table_name)
        elif table_name is None:
            if database.schema is None: throw(MappingError, 'No mapping was generated for the database')
            else: throw(TypeError, 'Table name cannot be None')
        elif not isinstance(table_name, basestring):
            throw(TypeError, 'Invalid table name: %r' % table_name)
        table_name = table_name[:]  # table_name = templating.plainstr(table_name)
        return table_name
    @cut_traceback
    @db_session(ddl=True)
    def drop_all_tables(database, with_all_data=False):
        if database.schema is None: throw(ERDiagramError, 'No mapping was generated for the database')
        database._drop_tables(database.schema.tables, True, with_all_data)
    def _drop_tables(database, table_names, if_exists, with_all_data, try_normalized=False):
        cache = database._get_cache()
        connection = cache.prepare_connection_for_query_execution()
        provider = database.provider
        existed_tables = []
        for table_name in table_names:
            table_name = database._get_table_name(table_name)
            if provider.table_exists(connection, table_name): existed_tables.append(table_name)
            elif not if_exists:
                if try_normalized:
                    normalized_table_name = provider.normalize_name(table_name)
                    if normalized_table_name != table_name \
                    and provider.table_exists(connection, normalized_table_name):
                        throw(TableDoesNotExist, 'Table %s does not exist (probably you meant table %s)'
                                                 % (table_name, normalized_table_name))
                throw(TableDoesNotExist, 'Table %s does not exist' % table_name)
        if not with_all_data:
            for table_name in existed_tables:
                if provider.table_has_data(connection, table_name): throw(TableIsNotEmpty,
                    'Cannot drop table %s because it is not empty. Specify option '
                    'with_all_data=True if you want to drop table with all data' % table_name)
        for table_name in existed_tables:
            if debug: log_orm('DROPPING TABLE %s' % table_name)
            provider.drop_table(connection, table_name)
    @cut_traceback
    @db_session(ddl=True)
    def create_tables(database, check_tables=False):
        cache = database._get_cache()
        if database.schema is None: throw(MappingError, 'No mapping was generated for the database')
        connection = cache.prepare_connection_for_query_execution()
        database.schema.create_tables(database.provider, connection)
        if check_tables: database.schema.check_tables(database.provider, connection)
    @cut_traceback
    @db_session()
    def check_tables(database):
        cache = database._get_cache()
        if database.schema is None: throw(MappingError, 'No mapping was generated for the database')
        connection = cache.prepare_connection_for_query_execution()
        database.schema.check_tables(database.provider, connection)
    @contextmanager
    def set_perms_for(database, *entities):
        if not entities: throw(TypeError, 'You should specify at least one positional argument')
        entity_set = set(entities)
        for entity in entities:
            if not isinstance(entity, EntityMeta):
                throw(TypeError, 'Entity class expected. Got: %s' % entity)
            entity_set.update(entity._subclasses_)
        if local.perms_context is not None:
            throw(OrmError, "'set_perms_for' context manager calls cannot be nested")
        local.perms_context = database, entity_set
        try: yield
        finally:
            assert local.perms_context and local.perms_context[0] is database
            local.perms_context = None
    def _get_schema_dict(database):
        result = []
        user = get_current_user()
        for entity in sorted(database.entities.values(), key=attrgetter('_id_')):
            if not can_view(user, entity): continue
            attrs = []
            for attr in entity._new_attrs_:
                if not can_view(user, attr): continue
                d = dict(name=attr.name, type=attr.py_type.__name__, kind=attr.__class__.__name__)
                if attr.auto: d['auto'] = True
                if attr.reverse:
                    if not can_view(user, attr.reverse.entity): continue
                    if not can_view(user, attr.reverse): continue
                    d['reverse'] = attr.reverse.name
                if attr.lazy: d['lazy'] = True
                if attr.nullable: d['nullable'] = True
                if attr.default and issubclass(type(attr.default), (int_types, basestring)):
                    d['defaultValue'] = attr.default
                attrs.append(d)
            d = dict(name=entity.__name__, newAttrs=attrs, pkAttrs=[ attr.name for attr in entity._pk_attrs_ ])
            if entity._all_bases_:
                d['bases'] = [ base.__name__ for base in entity._all_bases_ ]
            if entity._simple_keys_:
                d['simpleKeys'] = [ attr.name for attr in entity._simple_keys_ ]
            if entity._composite_keys_:
                d['compositeKeys'] = [ [ attr.name for attr in attrs ] for attrs in entity._composite_keys_ ]
            result.append(d)
        return result
    def _get_schema_json(database):
        schema_json = json.dumps(database._get_schema_dict(), default=basic_converter, sort_keys=True)
        schema_hash = md5(schema_json.encode('utf-8')).hexdigest()
        return schema_json, schema_hash
    @cut_traceback
    def to_json(database, data, include=(), exclude=(), converter=None, with_schema=True, schema_hash=None):
        for attrs, param_name in ((include, 'include'), (exclude, 'exclude')):
            for attr in attrs:
                if not isinstance(attr, Attribute): throw(TypeError,
                    "Each item of '%s' list should be attribute. Got: %s" % (param_name, attr))
        include, exclude = set(include), set(exclude)
        if converter is None: converter = basic_converter

        user = get_current_user()

        def user_has_no_rights_to_see(obj, attr=None):
            user_groups = get_user_groups(user)
            throw(PermissionError, 'The current user %s which belongs to groups %s '
                                   'has no rights to see the object %s on the frontend'
                                   % (user, sorted(user_groups), obj))

        object_set = set()
        caches = set()
        def obj_converter(obj):
            if not isinstance(obj, Entity): return converter(obj)
            caches.add(obj._session_cache_)
            if len(caches) > 1: throw(TransactionError,
                'An attempt to serialize objects belonging to different transactions')
            if not can_view(user, obj):
                user_has_no_rights_to_see(obj)
            object_set.add(obj)
            pkval = obj._get_raw_pkval_()
            if len(pkval) == 1: pkval = pkval[0]
            return { 'class': obj.__class__.__name__, 'pk': pkval }

        data_json = json.dumps(data, default=obj_converter)

        objects = {}
        if caches:
            cache = caches.pop()
            if cache.database is not database:
                throw(TransactionError, 'An object does not belong to specified database')
            object_list = list(object_set)
            objects = {}
            for obj in object_list:
                if obj in cache.seeds[obj._pk_attrs_]: obj._load_()
                entity = obj.__class__
                if not can_view(user, obj):
                    user_has_no_rights_to_see(obj)
                d = objects.setdefault(entity.__name__, {})
                for val in obj._get_raw_pkval_(): d = d.setdefault(val, {})
                assert not d, d
                for attr in obj._attrs_:
                    if attr in exclude: continue
                    if attr in include: pass
                        # if attr not in entity_perms.can_read: user_has_no_rights_to_see(obj, attr)
                    elif attr.is_collection: continue
                    elif attr.lazy: continue
                    # elif attr not in entity_perms.can_read: continue

                    if attr.is_collection:
                        if not isinstance(attr, Set): throw(NotImplementedError)
                        value = []
                        for item in attr.__get__(obj):
                            if item not in object_set:
                                object_set.add(item)
                                object_list.append(item)
                            pkval = item._get_raw_pkval_()
                            value.append(pkval[0] if len(pkval) == 1 else pkval)
                        value.sort()
                    else:
                        value = attr.__get__(obj)
                        if value is not None and attr.is_relation:
                            if attr in include and value not in object_set:
                                object_set.add(value)
                                object_list.append(value)
                            pkval = value._get_raw_pkval_()
                            value = pkval[0] if len(pkval) == 1 else pkval

                    d[attr.name] = value
        objects_json = json.dumps(objects, default=converter)
        if not with_schema:
            return '{"data": %s, "objects": %s}' % (data_json, objects_json)
        schema_json, new_schema_hash = database._get_schema_json()
        if schema_hash is not None and schema_hash == new_schema_hash:
            return '{"data": %s, "objects": %s, "schema_hash": "%s"}' \
                   % (data_json, objects_json, new_schema_hash)
        return '{"data": %s, "objects": %s, "schema": %s, "schema_hash": "%s"}' \
               % (data_json, objects_json, schema_json, new_schema_hash)
    @cut_traceback
    @db_session
    def from_json(database, changes, observer=None):
        changes = json.loads(changes)

        import pprint; pprint.pprint(changes)

        objmap = {}
        for diff in changes['objects']:
            if diff['_status_'] == 'c': continue
            pk = diff['_pk_']
            pk = (pk,) if type(pk) is not list else tuple(pk)
            entity_name = diff['class']
            entity = database.entities[entity_name]
            obj = entity._get_by_raw_pkval_(pk, from_db=False)
            oid = diff['_id_']
            objmap[oid] = obj

        def id2obj(attr, val):
            return objmap[val] if attr.reverse and val is not None else val

        user = get_current_user()

        def user_has_no_rights_to(operation, x):
            user_groups = get_user_groups(user)
            s = 'attribute %s' % x if isinstance(x, Attribute) else 'object %s' % x
            throw(PermissionError, 'The current user %s which belongs to groups %s '
                                   'has no rights to %s the %s on the frontend'
                                   % (user, sorted(user_groups), operation, s))

        for diff in changes['objects']:
            entity_name = diff['class']
            entity = database.entities[entity_name]
            oldvals = {}
            newvals = {}
            oldadict = {}
            newadict = {}
            for name, val in diff.items():
                if name not in ('class', '_pk_', '_id_', '_status_'):
                    attr = entity._adict_[name]
                    if not attr.is_collection:
                        if type(val) is dict:
                            if 'old' in val: oldvals[attr.name] = oldadict[attr] = attr.validate(id2obj(attr, val['old']))
                            if 'new' in val: newvals[attr.name] = newadict[attr] = attr.validate(id2obj(attr, val['new']))
                        else: newvals[attr.name] = newadict[attr] = attr.validate(id2obj(attr, val))
            oid = diff['_id_']
            status = diff['_status_']
            if status == 'c':
                assert not oldvals
                for attr in newadict:
                    if not can_create(user, attr): user_has_no_rights_to('initialize', attr)
                obj = entity(**newvals)
                if observer:
                    flush()  # in order to get obj.id
                    observer('create', obj, newvals)
                objmap[oid] = obj
                if not can_edit(user, obj): user_has_no_rights_to('create', obj)
            else:
                obj = objmap[oid]
                if status == 'd':
                    if not can_delete(user, obj): user_has_no_rights_to('delete', obj)
                    if observer: observer('delete', obj)
                    obj.delete()
                elif status == 'u':
                    if not can_edit(user, obj): user_has_no_rights_to('update', obj)
                    if newvals:
                        for attr in newadict:
                            if not can_edit(user, attr): user_has_no_rights_to('edit', attr)
                        assert oldvals
                        if observer:
                            observer('update', obj, newvals, oldvals)
                        obj._db_set_(oldadict)  # oldadict can be modified here
                        for attr in oldadict: attr.__get__(obj)
                        obj.set(**newvals)
                    else: assert not oldvals
                    objmap[oid] = obj
        flush()
        for diff in changes['objects']:
            if diff['_status_'] == 'd': continue
            obj = objmap[diff['_id_']]
            entity = obj.__class__
            for name, val in diff.items():
                if name not in ('class', '_pk_', '_id_', '_status_'):
                    attr = entity._adict_[name]
                    if attr.is_collection and attr.reverse.is_collection and attr < attr.reverse:
                        removed = [ objmap[oid] for oid in val.get('removed', ()) ]
                        added = [ objmap[oid] for oid in val.get('added', ()) ]
                        if (added or removed) and not can_edit(user, attr): user_has_no_rights_to('edit', attr)
                        collection = attr.__get__(obj)
                        if removed:
                            observer('remove', obj, {name: removed})
                            collection.remove(removed)
                        if added:
                            observer('add', obj, {name: added})
                            collection.add(added)
        flush()

        def deserialize(x):
            t = type(x)
            if t is list: return list(imap(deserialize, x))
            if t is dict:
                if '_id_' not in x:
                    return {key: deserialize(val) for key, val in iteritems(x)}
                obj = objmap.get(x['_id_'])
                if obj is None:
                    entity_name = x['class']
                    entity = database.entities[entity_name]
                    pk = x['_pk_']
                    obj = entity[pk]
                return obj
            return x

        return deserialize(changes['data'])

def basic_converter(x):
    if isinstance(x, (datetime.datetime, datetime.date, Decimal)):
        return str(x)
    if isinstance(x, dict):
        return dict(x)
    if isinstance(x, Entity):
        pkval = x._get_raw_pkval_()
        return pkval[0] if len(pkval) == 1 else pkval
    if hasattr(x, '__iter__'): return list(x)
    throw(TypeError, 'The following object cannot be converted to JSON: %r' % x)

@cut_traceback
def perm(*args, **kwargs):
    if local.perms_context is None:
        throw(OrmError, "'perm' function can be called within 'set_perm_for' context manager only")
    database, entities = local.perms_context
    permissions = _split_names('Permission', args)
    groups = pop_names_from_kwargs('Group', kwargs, 'group', 'groups')
    roles = pop_names_from_kwargs('Role', kwargs, 'role', 'roles')
    labels = pop_names_from_kwargs('Label', kwargs, 'label', 'labels')
    for kwname in kwargs: throw(TypeError, 'Unknown keyword argument name: %s' % kwname)
    return AccessRule(database, entities, permissions, groups, roles, labels)

def _split_names(typename, names):
    if names is None: return set()
    if isinstance(names, basestring):
        names = names.replace(',', ' ').split()
    else:
        try: namelist = list(names)
        except: throw(TypeError, '%s name should be string. Got: %s' % (typename, names))
        names = []
        for name in namelist:
            names.extend(_split_names(typename, name))
    for name in names:
        if not is_ident(name): throw(TypeError, '%s name should be identifier. Got: %s' % (typename, name))
    return set(names)

def pop_names_from_kwargs(typename, kwargs, *kwnames):
    result = set()
    for kwname in kwnames:
        kwarg = kwargs.pop(kwname, None)
        if kwarg is not None: result.update(_split_names(typename, kwarg))
    return result

class AccessRule(object):
    def __init__(rule, database, entities, permissions, groups, roles, labels):
        rule.database = database
        rule.entities = entities
        if not permissions: throw(TypeError, 'At least one permission should be specified')
        rule.permissions = permissions
        rule.groups = groups
        rule.groups.add('anybody')
        rule.roles = roles
        rule.labels = labels
        rule.entities_to_exclude = set()
        rule.attrs_to_exclude = set()
        for entity in entities:
            for perm in rule.permissions:
                entity._access_rules_[perm].add(rule)
    def exclude(rule, *args):
        for arg in args:
            if isinstance(arg, EntityMeta):
                entity = arg
                rule.entities_to_exclude.add(entity)
                rule.entities_to_exclude.update(entity._subclasses_)
            elif isinstance(arg, Attribute):
                attr = arg
                if attr.pk_offset is not None: throw(TypeError, 'Primary key attribute %s cannot be excluded' % attr)
                rule.attrs_to_exclude.add(attr)
            else: throw(TypeError, 'Entity or attribute expected. Got: %r' % arg)

@cut_traceback
def has_perm(user, perm, x):
    if isinstance(x, EntityMeta):
        entity = x
    elif isinstance(x, Entity):
        entity = x.__class__
    elif isinstance(x, Attribute):
        if x.hidden: return False
        entity = x.entity
    else: throw(TypeError, "The third parameter of 'has_perm' function should be entity class, entity instance "
                           "or attribute. Got: %r" % x)
    access_rules = entity._access_rules_.get(perm)
    if not access_rules: return False
    cache = entity._database_._get_cache()
    perm_cache = cache.perm_cache[user][perm]
    result = perm_cache.get(x)
    if result is not None: return result
    user_groups = get_user_groups(user)
    result = False
    if isinstance(x, EntityMeta):
        for rule in access_rules:
            if user_groups.issuperset(rule.groups) and entity not in rule.entities_to_exclude:
                result = True
                break
    elif isinstance(x, Attribute):
        attr = x
        for rule in access_rules:
            if user_groups.issuperset(rule.groups) and entity not in rule.entities_to_exclude \
                                                   and attr not in rule.attrs_to_exclude:
                result = True
                break
            reverse = attr.reverse
            if reverse:
                reverse_rules = reverse.entity._access_rules_.get(perm)
                if not reverse_rules: return False
                for reverse_rule in access_rules:
                    if user_groups.issuperset(reverse_rule.groups) \
                            and reverse.entity not in reverse_rule.entities_to_exclude \
                            and reverse not in reverse_rule.attrs_to_exclude:
                        result = True
                        break
                if result: break
    else:
        obj = x
        user_roles = get_user_roles(user, obj)
        obj_labels = get_object_labels(obj)
        for rule in access_rules:
            if x in rule.entities_to_exclude: continue
            elif not user_groups.issuperset(rule.groups): pass
            elif not user_roles.issuperset(rule.roles): pass
            elif not obj_labels.issuperset(rule.labels): pass
            else:
                result = True
                break
    perm_cache[perm] = result
    return result

def can_view(user, x):
    return has_perm(user, 'view', x) or has_perm(user, 'edit', x)

def can_edit(user, x):
    return has_perm(user, 'edit', x)

def can_create(user, x):
    return has_perm(user, 'create', x)

def can_delete(user, x):
    return has_perm(user, 'delete', x)

def get_current_user():
    return local.current_user

def set_current_user(user):
    local.current_user = user

anybody_frozenset = frozenset(['anybody'])

def get_user_groups(user):
    result = local.user_groups_cache.get(user)
    if result is not None: return result
    if user is None: return anybody_frozenset
    result = {'anybody'}
    for cls, func in usergroup_functions:
        if cls is None or isinstance(user, cls):
            groups = func(user)
            if isinstance(groups, basestring):  # single group name
                result.add(groups)
            elif groups is not None:
                result.update(groups)
    result = frozenset(result)
    local.user_groups_cache[user] = result
    return result

def get_user_roles(user, obj):
    if user is None: return frozenset()
    roles_cache = local.user_roles_cache[user]
    result = roles_cache.get(obj)
    if result is not None: return result
    result = set()
    if user is obj: result.add('self')
    for user_cls, obj_cls, func in userrole_functions:
        if user_cls is None or isinstance(user, user_cls):
            if obj_cls is None or isinstance(obj, obj_cls):
                roles = func(user, obj)
                if isinstance(roles, basestring):  # single role name
                    result.add(roles)
                elif roles is not None:
                    result.update(roles)
    result = frozenset(result)
    roles_cache[obj] = result
    return result

def get_object_labels(obj):
    cache = obj._database_._get_cache()
    obj_labels_cache = cache.obj_labels_cache
    result = obj_labels_cache.get(obj)
    if result is None:
        result = set()
        for obj_cls, func in objlabel_functions:
            if obj_cls is None or isinstance(obj, obj_cls):
                labels = func(obj)
                if isinstance(labels, basestring):  # single label name
                    result.add(labels)
                elif labels is not None:
                    result.update(labels)
        obj_labels_cache[obj] = result
    return result

usergroup_functions = []

def user_groups_getter(cls=None):
    def decorator(func):
        if func not in usergroup_functions:
            usergroup_functions.append((cls, func))
        return func
    return decorator

userrole_functions = []

def user_roles_getter(user_cls=None, obj_cls=None):
    def decorator(func):
        if func not in userrole_functions:
            userrole_functions.append((user_cls, obj_cls, func))
        return func
    return decorator

objlabel_functions = []

def obj_labels_getter(cls=None):
    def decorator(func):
        if func not in objlabel_functions:
            objlabel_functions.append((cls, func))
        return func
    return decorator

class DbLocal(localbase):
    def __init__(dblocal):
        dblocal.stats = {}
        dblocal.last_sql = None

class QueryStat(object):
    def __init__(stat, sql, query_start_time=None):
        if query_start_time is not None:
            query_end_time = time()
            duration = query_end_time - query_start_time
            stat.min_time = stat.max_time = stat.sum_time = duration
            stat.db_count = 1
            stat.cache_count = 0
        else:
            stat.min_time = stat.max_time = stat.sum_time = None
            stat.db_count = 0
            stat.cache_count = 1
        stat.sql = sql
    def copy(stat):
        result = object.__new__(QueryStat)
        result.__dict__.update(stat.__dict__)
        return result
    def query_executed(stat, query_start_time):
        query_end_time = time()
        duration = query_end_time - query_start_time
        if stat.db_count:
            stat.min_time = builtins.min(stat.min_time, duration)
            stat.max_time = builtins.max(stat.max_time, duration)
            stat.sum_time += duration
        else: stat.min_time = stat.max_time = stat.sum_time = duration
        stat.db_count += 1
    def merge(stat, stat2):
        assert stat.sql == stat2.sql
        if not stat2.db_count: pass
        elif stat.db_count:
            stat.min_time = builtins.min(stat.min_time, stat2.min_time)
            stat.max_time = builtins.max(stat.max_time, stat2.max_time)
            stat.sum_time += stat2.sum_time
        else:
            stat.min_time = stat2.min_time
            stat.max_time = stat2.max_time
            stat.sum_time = stat2.sum_time
        stat.db_count += stat2.db_count
        stat.cache_count += stat2.cache_count
    @property
    def avg_time(stat):
        if not stat.db_count: return None
        return stat.sum_time / stat.db_count

class SessionCache(object):
    def __init__(cache, database):
        cache.is_alive = True
        cache.num = next(num_counter)
        cache.database = database
        cache.objects = set()
        cache.indexes = defaultdict(dict)
        cache.seeds = defaultdict(set)
        cache.max_id_cache = {}
        cache.collection_statistics = {}
        cache.for_update = set()
        cache.noflush_counter = 0
        cache.modified_collections = defaultdict(set)
        cache.objects_to_save = []
        cache.saved_objects = []
        cache.query_results = {}
        cache.modified = False
        cache.db_session = db_session = local.db_session
        cache.immediate = db_session is not None and db_session.immediate
        cache.connection = None
        cache.in_transaction = False
        cache.saved_fk_state = None
        cache.perm_cache = defaultdict(lambda : defaultdict(dict))  # user -> perm -> cls_or_attr_or_obj -> bool
        cache.user_roles_cache = defaultdict(dict)  # user -> obj -> roles
        cache.obj_labels_cache = {}  # obj -> labels
    def connect(cache):
        assert cache.connection is None
        if cache.in_transaction: throw(ConnectionClosedError,
            'Transaction cannot be continued because database connection failed')
        provider = cache.database.provider
        connection = provider.connect()
        try: provider.set_transaction_mode(connection, cache)  # can set cache.in_transaction
        except:
            provider.drop(connection, cache)
            raise
        cache.connection = connection
        return connection
    def reconnect(cache, exc):
        provider = cache.database.provider
        if exc is not None:
            exc = getattr(exc, 'original_exc', exc)
            if not provider.should_reconnect(exc): reraise(*sys.exc_info())
            if debug: log_orm('CONNECTION FAILED: %s' % exc)
            connection = cache.connection
            assert connection is not None
            cache.connection = None
            provider.drop(connection, cache)
        else: assert cache.connection is None
        return cache.connect()
    def prepare_connection_for_query_execution(cache):
        db_session = local.db_session
        if db_session is not None and cache.db_session is None:
            # This situation can arise when a transaction was started
            # in the interactive mode, outside of the db_session
            if cache.in_transaction or cache.modified:
                local.db_session = None
                try: cache.flush_and_commit()
                finally: local.db_session = db_session
            cache.db_session = db_session
            cache.immediate = cache.immediate or db_session.immediate
        else: assert cache.db_session is db_session, (cache.db_session, db_session)
        connection = cache.connection
        if connection is None: connection = cache.connect()
        elif cache.immediate and not cache.in_transaction:
            provider = cache.database.provider
            try: provider.set_transaction_mode(connection, cache)  # can set cache.in_transaction
            except Exception as e: connection = cache.reconnect(e)
        if not cache.noflush_counter and cache.modified: cache.flush()
        return connection
    def flush_and_commit(cache):
        try: cache.flush()
        except:
            cache.rollback()
            raise
        try: cache.commit()
        except: transact_reraise(CommitException, [sys.exc_info()])
    def commit(cache):
        assert cache.is_alive
        try:
            if cache.modified: cache.flush()
            if cache.in_transaction:
                assert cache.connection is not None
                cache.database.provider.commit(cache.connection, cache)
            cache.for_update.clear()
            cache.immediate = True
        except:
            cache.rollback()
            raise
    def rollback(cache):
        cache.close(rollback=True)
    def release(cache):
        cache.close(rollback=False)
    def close(cache, rollback=True):
        assert cache.is_alive
        if not rollback: assert not cache.in_transaction
        database = cache.database
        x = local.db2cache.pop(database); assert x is cache
        cache.is_alive = False
        provider = database.provider
        connection = cache.connection
        if connection is None: return
        cache.connection = None
        if rollback:
            try: provider.rollback(connection, cache)
            except:
                provider.drop(connection, cache)
                raise
        provider.release(connection, cache)
        db_session = cache.db_session or local.db_session
        if db_session and db_session.strict:
            cache.clear()
    def clear(cache):
        for obj in cache.objects:
            obj._vals_ = obj._dbvals_ = obj._session_cache_ = None
        cache.objects = cache.indexes = cache.seeds = cache.for_update = cache.modified_collections \
            = cache.objects_to_save = cache.saved_objects = cache.query_results \
            = cache.perm_cache = cache.user_roles_cache = cache.obj_labels_cache = None
    @contextmanager
    def flush_disabled(cache):
        cache.noflush_counter += 1
        try: yield
        finally: cache.noflush_counter -= 1
    def flush(cache):
        if cache.noflush_counter: return
        assert cache.is_alive
        assert not cache.saved_objects
        if not cache.immediate: cache.immediate = True
        for i in xrange(50):
            if not cache.modified: return

            with cache.flush_disabled():
                for obj in cache.objects_to_save:  # can grow during iteration
                    if obj is not None: obj._before_save_()

                cache.query_results.clear()
                modified_m2m = cache._calc_modified_m2m()
                for attr, (added, removed) in iteritems(modified_m2m):
                    if not removed: continue
                    attr.remove_m2m(removed)
                for obj in cache.objects_to_save:
                    if obj is not None: obj._save_()
                for attr, (added, removed) in iteritems(modified_m2m):
                    if not added: continue
                    attr.add_m2m(added)

            cache.max_id_cache.clear()
            cache.modified_collections.clear()
            cache.objects_to_save[:] = ()
            cache.modified = False

            cache.call_after_save_hooks()
        else:
            if cache.modified: throw(TransactionError,
                'Recursion depth limit reached in obj._after_save_() call')
    def call_after_save_hooks(cache):
        saved_objects = cache.saved_objects
        cache.saved_objects = []
        for obj, status in saved_objects:
            obj._after_save_(status)
    def _calc_modified_m2m(cache):
        modified_m2m = {}
        for attr, objects in sorted(iteritems(cache.modified_collections),
                                    key=lambda pair: (pair[0].entity.__name__, pair[0].name)):
            if not isinstance(attr, Set): throw(NotImplementedError)
            reverse = attr.reverse
            if not reverse.is_collection:
                for obj in objects:
                    setdata = obj._vals_[attr]
                    setdata.added = setdata.removed = setdata.absent = None
                continue

            if not isinstance(reverse, Set): throw(NotImplementedError)
            if reverse in modified_m2m: continue
            added, removed = modified_m2m.setdefault(attr, (set(), set()))
            for obj in objects:
                setdata = obj._vals_[attr]
                if setdata.added:
                    for obj2 in setdata.added: added.add((obj, obj2))
                if setdata.removed:
                    for obj2 in setdata.removed: removed.add((obj, obj2))
                if obj._status_ == 'marked_to_delete': del obj._vals_[attr]
                else: setdata.added = setdata.removed = setdata.absent = None
        cache.modified_collections.clear()
        return modified_m2m
    def update_simple_index(cache, obj, attr, old_val, new_val, undo):
        assert old_val != new_val
        cache_index = cache.indexes[attr]
        if new_val is not None:
            obj2 = cache_index.setdefault(new_val, obj)
            if obj2 is not obj: throw(CacheIndexError, 'Cannot update %s.%s: %s with key %s already exists'
                                                 % (obj.__class__.__name__, attr.name, obj2, new_val))
        if old_val is not None: del cache_index[old_val]
        undo.append((cache_index, old_val, new_val))
    def db_update_simple_index(cache, obj, attr, old_dbval, new_dbval):
        assert old_dbval != new_dbval
        cache_index = cache.indexes[attr]
        if new_dbval is not None:
            obj2 = cache_index.setdefault(new_dbval, obj)
            if obj2 is not obj: throw(TransactionIntegrityError,
                '%s with unique index %s.%s already exists: %s'
                % (obj2.__class__.__name__, obj.__class__.__name__, attr.name, new_dbval))
                # attribute which was created or updated lately clashes with one stored in database
        cache_index.pop(old_dbval, None)
    def update_composite_index(cache, obj, attrs, prev_vals, new_vals, undo):
        if None in prev_vals: prev_vals = None
        if None in new_vals: new_vals = None
        if prev_vals is None and new_vals is None: return
        cache_index = cache.indexes[attrs]
        if new_vals is not None:
            obj2 = cache_index.setdefault(new_vals, obj)
            if obj2 is not obj:
                attr_names = ', '.join(attr.name for attr in attrs)
                throw(CacheIndexError, 'Cannot update %r: composite key (%s) with value %s already exists for %r'
                                 % (obj, attr_names, new_vals, obj2))
        if prev_vals is not None: del cache_index[prev_vals]
        undo.append((cache_index, prev_vals, new_vals))
    def db_update_composite_index(cache, obj, attrs, prev_vals, new_vals):
        cache_index = cache.indexes[attrs]
        if None not in new_vals:
            obj2 = cache_index.setdefault(new_vals, obj)
            if obj2 is not obj:
                key_str = ', '.join(repr(item) for item in new_vals)
                throw(TransactionIntegrityError, '%s with unique index (%s) already exists: %s'
                                 % (obj2.__class__.__name__, ', '.join(attr.name for attr in attrs), key_str))
        cache_index.pop(prev_vals, None)

class NotLoadedValueType(object):
    def __repr__(self): return 'NOT_LOADED'

NOT_LOADED = NotLoadedValueType()

class DefaultValueType(object):
    def __repr__(self): return 'DEFAULT'

DEFAULT = DefaultValueType()

class DescWrapper(object):
    def __init__(self, attr):
        self.attr = attr
    def __repr__(self):
        return '<DescWrapper(%s)>' % self.attr
    def __call__(self):
        return self
    def __eq__(self, other):
        return type(other) is DescWrapper and self.attr == other.attr
    def __ne__(self, other):
        return type(other) is not DescWrapper or self.attr != other.attr
    def __hash__(self):
        return hash(self.attr) + 1

attr_id_counter = itertools.count(1)

class Attribute(object):
    __slots__ = 'nullable', 'is_required', 'is_discriminator', 'is_unique', 'is_part_of_unique_index', \
                'is_pk', 'is_collection', 'is_relation', 'is_basic', 'is_string', 'is_volatile', 'is_implicit', \
                'id', 'pk_offset', 'pk_columns_offset', 'py_type', 'sql_type', 'entity', 'name', \
                'lazy', 'lazy_sql_cache', 'args', 'auto', 'default', 'reverse', 'composite_keys', \
                'column', 'columns', 'col_paths', '_columns_checked', 'converters', 'kwargs', \
                'cascade_delete', 'index', 'original_default', 'sql_default', 'py_check', 'hidden', \
                'optimistic'
    def __deepcopy__(attr, memo):
        return attr  # Attribute cannot be cloned by deepcopy()
    @cut_traceback
    def __init__(attr, py_type, *args, **kwargs):
        if attr.__class__ is Attribute: throw(TypeError, "'Attribute' is abstract type")
        attr.is_implicit = False
        attr.is_required = isinstance(attr, Required)
        attr.is_discriminator = isinstance(attr, Discriminator)
        attr.is_unique = kwargs.pop('unique', None)
        if isinstance(attr, PrimaryKey):
            if attr.is_unique is not None:
                throw(TypeError, "'unique' option cannot be set for PrimaryKey attribute ")
            attr.is_unique = True
        attr.nullable = kwargs.pop('nullable', None)
        attr.is_part_of_unique_index = attr.is_unique  # Also can be set to True later
        attr.is_pk = isinstance(attr, PrimaryKey)
        if attr.is_pk: attr.pk_offset = 0
        else: attr.pk_offset = None
        attr.id = next(attr_id_counter)
        if not isinstance(py_type, (type, basestring, types.FunctionType)):
            if py_type is datetime: throw(TypeError,
                'datetime is the module and cannot be used as attribute type. Use datetime.datetime instead')
            throw(TypeError, 'Incorrect type of attribute: %r' % py_type)
        attr.py_type = py_type
        attr.is_string = type(py_type) is type and issubclass(py_type, basestring)
        attr.is_collection = isinstance(attr, Collection)
        attr.is_relation = isinstance(attr.py_type, (EntityMeta, basestring, types.FunctionType))
        attr.is_basic = not attr.is_collection and not attr.is_relation
        attr.sql_type = kwargs.pop('sql_type', None)
        attr.entity = attr.name = None
        attr.args = args
        attr.auto = kwargs.pop('auto', False)
        attr.cascade_delete = kwargs.pop('cascade_delete', None)

        attr.reverse = kwargs.pop('reverse', None)
        if not attr.reverse: pass
        elif not isinstance(attr.reverse, (basestring, Attribute)):
            throw(TypeError, "Value of 'reverse' option must be name of reverse attribute). Got: %r" % attr.reverse)
        elif not attr.is_relation:
            throw(TypeError, 'Reverse option cannot be set for this type: %r' % attr.py_type)

        attr.column = kwargs.pop('column', None)
        attr.columns = kwargs.pop('columns', None)
        if attr.column is not None:
            if attr.columns is not None:
                throw(TypeError, "Parameters 'column' and 'columns' cannot be specified simultaneously")
            if not isinstance(attr.column, basestring):
                throw(TypeError, "Parameter 'column' must be a string. Got: %r" % attr.column)
            attr.columns = [ attr.column ]
        elif attr.columns is not None:
            if not isinstance(attr.columns, (tuple, list)):
                throw(TypeError, "Parameter 'columns' must be a list. Got: %r'" % attr.columns)
            for column in attr.columns:
                if not isinstance(column, basestring):
                    throw(TypeError, "Items of parameter 'columns' must be strings. Got: %r" % attr.columns)
            if len(attr.columns) == 1: attr.column = attr.columns[0]
        else: attr.columns = []
        attr.index = kwargs.pop('index', None)
        attr.col_paths = []
        attr._columns_checked = False
        attr.composite_keys = []
        attr.lazy = kwargs.pop('lazy', getattr(py_type, 'lazy', False))
        attr.lazy_sql_cache = None
        attr.is_volatile = kwargs.pop('volatile', False)
        attr.optimistic = kwargs.pop('optimistic', True)
        attr.sql_default = kwargs.pop('sql_default', None)
        attr.py_check = kwargs.pop('py_check', None)
        attr.hidden = kwargs.pop('hidden', False)
        attr.kwargs = kwargs
        attr.converters = []
    def _init_(attr, entity, name):
        attr.entity = entity
        attr.name = name
        if attr.pk_offset is not None and attr.lazy:
            throw(TypeError, 'Primary key attribute %s cannot be lazy' % attr)
        if attr.cascade_delete is not None and attr.is_basic:
            throw(TypeError, "'cascade_delete' option cannot be set for attribute %s, "
                             "because it is not relationship attribute" % attr)

        if not attr.is_required:
            if attr.is_unique and attr.nullable is False:
                throw(TypeError, 'Optional unique attribute %s must be nullable' % attr)
        if entity._root_ is not entity:
            if attr.nullable is False: throw(ERDiagramError,
                'Attribute %s must be nullable due to single-table inheritance' % attr)
            attr.nullable = True

        if 'default' in attr.kwargs:
            attr.default = attr.original_default = attr.kwargs.pop('default')
            if attr.is_required:
                if attr.default is None: throw(TypeError,
                    'Default value for required attribute %s cannot be None' % attr)
                if attr.default == '': throw(TypeError,
                    'Default value for required attribute %s cannot be empty string' % attr)
            elif attr.default is None and not attr.nullable: throw(TypeError,
                'Default value for non-nullable attribute %s cannot be set to None' % attr)
        elif attr.is_string and not attr.is_required and not attr.nullable:
            attr.default = ''
        else:
            attr.default = None

        sql_default = attr.sql_default
        if isinstance(sql_default, basestring):
            if sql_default == '': throw(TypeError,
                "'sql_default' option value cannot be empty string, "
                "because it should be valid SQL literal or expression. "
                "Try to use \"''\", or just specify default='' instead.")
        elif attr.sql_default not in (None, True, False):
            throw(TypeError, "'sql_default' option of %s attribute must be of string or bool type. Got: %s"
                             % (attr, attr.sql_default))

        if attr.py_check is not None and not callable(attr.py_check):
            throw(TypeError, "'py_check' parameter of %s attribute should be callable" % attr)

        # composite keys will be checked later inside EntityMeta.__init__
        if attr.py_type == float:
            if attr.is_pk: throw(TypeError, 'PrimaryKey attribute %s cannot be of type float' % attr)
            elif attr.is_unique: throw(TypeError, 'Unique attribute %s cannot be of type float' % attr)
        if attr.is_volatile and (attr.is_pk or attr.is_collection): throw(TypeError,
            '%s attribute %s cannot be volatile' % (attr.__class__.__name__, attr))
    def linked(attr):
        reverse = attr.reverse
        if attr.cascade_delete is None:
            attr.cascade_delete = attr.is_collection and reverse.is_required
        elif attr.cascade_delete:
            if reverse.cascade_delete: throw(TypeError,
                "'cascade_delete' option cannot be set for both sides of relationship "
                "(%s and %s) simultaneously" % (attr, reverse))
            if reverse.is_collection: throw(TypeError,
                "'cascade_delete' option cannot be set for attribute %s, "
                "because reverse attribute %s is collection" % (attr, reverse))
    @cut_traceback
    def __repr__(attr):
        owner_name = attr.entity.__name__ if attr.entity else '?'
        return '%s.%s' % (owner_name, attr.name or '?')
    def __lt__(attr, other):
        return attr.id < other.id
    def validate(attr, val, obj=None, entity=None, from_db=False):
        if val is None:
            if not attr.nullable and not from_db and not attr.is_required:
                # for required attribute the exception will be thrown later with another message
                throw(ValueError, 'Attribute %s cannot be set to None' % attr)
            return val
        assert val is not NOT_LOADED
        if val is DEFAULT:
            default = attr.default
            if default is None: return None
            if callable(default): val = default()
            else: val = default

        if entity is not None: pass
        elif obj is not None: entity = obj.__class__
        else: entity = attr.entity

        reverse = attr.reverse
        if not reverse:
            if isinstance(val, Entity): throw(TypeError, 'Attribute %s must be of %s type. Got: %s'
                % (attr, attr.py_type.__name__, val))
            if not attr.converters:
                return val if type(val) is attr.py_type else attr.py_type(val)
            if len(attr.converters) != 1: throw(NotImplementedError)
            converter = attr.converters[0]
            if converter is not None:
                try:
                    if from_db: return converter.sql2py(val)
                    val = converter.validate(val)
                except UnicodeDecodeError as e:
                    throw(ValueError, 'Value for attribute %s cannot be converted to %s: %s'
                                      % (attr, unicode.__name__, truncate_repr(val)))
        else:
            rentity = reverse.entity
            if not isinstance(val, rentity):
                vals = val if type(val) is tuple else (val,)
                if len(vals) != len(rentity._pk_columns_): throw(TypeError,
                    'Invalid number of columns were specified for attribute %s. Expected: %d, got: %d'
                    % (attr, len(rentity._pk_columns_), len(vals)))
                try: val = rentity._get_by_raw_pkval_(vals, from_db=from_db)
                except TypeError: throw(TypeError, 'Attribute %s must be of %s type. Got: %r'
                                                   % (attr, rentity.__name__, val))
            else:
                if obj is not None: cache = obj._session_cache_
                else: cache = entity._database_._get_cache()
                if cache is not val._session_cache_:
                    throw(TransactionError, 'An attempt to mix objects belonging to different transactions')
        if attr.py_check is not None and not attr.py_check(val):
            throw(ValueError, 'Check for attribute %s failed. Value: %s' % (attr, truncate_repr(val)))
        return val
    def parse_value(attr, row, offsets):
        assert len(attr.columns) == len(offsets)
        if not attr.reverse:
            if len(offsets) > 1: throw(NotImplementedError)
            offset = offsets[0]
            val = attr.validate(row[offset], None, attr.entity, from_db=True)
        else:
            vals = [ row[offset] for offset in offsets ]
            if None in vals:
                assert len(set(vals)) == 1
                val = None
            else: val = attr.py_type._get_by_raw_pkval_(vals)
        return val
    def load(attr, obj):
        if not obj._session_cache_.is_alive: throw(DatabaseSessionIsOver,
            'Cannot load attribute %s.%s: the database session is over' % (safe_repr(obj), attr.name))
        if not attr.columns:
            reverse = attr.reverse
            assert reverse is not None and reverse.columns
            dbval = reverse.entity._find_in_db_({reverse : obj})
            if dbval is None: obj._vals_[attr] = None
            else: assert obj._vals_[attr] == dbval
            return dbval

        if attr.lazy:
            entity = attr.entity
            database = entity._database_
            if not attr.lazy_sql_cache:
                select_list = [ 'ALL' ] + [ [ 'COLUMN', None, column ] for column in attr.columns ]
                from_list = [ 'FROM', [ None, 'TABLE', entity._table_ ] ]
                pk_columns = entity._pk_columns_
                pk_converters = entity._pk_converters_
                criteria_list = [ [ converter.EQ, [ 'COLUMN', None, column ], [ 'PARAM', (i, None, None), converter ] ]
                                  for i, (column, converter) in enumerate(izip(pk_columns, pk_converters)) ]
                sql_ast = [ 'SELECT', select_list, from_list, [ 'WHERE' ] + criteria_list ]
                sql, adapter = database._ast2sql(sql_ast)
                offsets = tuple(xrange(len(attr.columns)))
                attr.lazy_sql_cache = sql, adapter, offsets
            else: sql, adapter, offsets = attr.lazy_sql_cache
            arguments = adapter(obj._get_raw_pkval_())
            cursor = database._exec_sql(sql, arguments)
            row = cursor.fetchone()
            dbval = attr.parse_value(row, offsets)
            attr.db_set(obj, dbval)
        else: obj._load_()
        return obj._vals_[attr]
    @cut_traceback
    def __get__(attr, obj, cls=None):
        if obj is None: return attr
        if attr.pk_offset is not None: return attr.get(obj)
        value = attr.get(obj)
        bit = obj._bits_except_volatile_[attr]
        wbits = obj._wbits_
        if wbits is not None and not wbits & bit: obj._rbits_ |= bit
        return value
    def get(attr, obj):
        if attr.pk_offset is None and obj._status_ in ('deleted', 'cancelled'):
            throw_object_was_deleted(obj)
        vals = obj._vals_
        if vals is None: throw_db_session_is_over(obj, attr)
        val = vals[attr] if attr in vals else attr.load(obj)
        if val is not None and attr.reverse and val._subclasses_ and val._status_ not in ('deleted', 'cancelled'):
            seeds = obj._session_cache_.seeds[val._pk_attrs_]
            if val in seeds: val._load_()
        return val
    @cut_traceback
    def __set__(attr, obj, new_val, undo_funcs=None):
        cache = obj._session_cache_
        if not cache.is_alive: throw(DatabaseSessionIsOver,
            'Cannot assign new value to attribute %s.%s: the database session is over' % (safe_repr(obj), attr.name))
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        reverse = attr.reverse
        new_val = attr.validate(new_val, obj, from_db=False)
        if attr.pk_offset is not None:
            pkval = obj._pkval_
            if pkval is None: pass
            elif obj._pk_is_composite_:
                if new_val == pkval[attr.pk_offset]: return
            elif new_val == pkval: return
            throw(TypeError, 'Cannot change value of primary key')
        with cache.flush_disabled():
            old_val =  obj._vals_.get(attr, NOT_LOADED)
            if old_val is NOT_LOADED and reverse and not reverse.is_collection:
                old_val = attr.load(obj)
            status = obj._status_
            wbits = obj._wbits_
            bit = obj._bits_[attr]
            objects_to_save = cache.objects_to_save
            objects_to_save_needs_undo = False
            if wbits is not None and bit:
                obj._wbits_ = wbits | bit
                if status != 'modified':
                    assert status in ('loaded', 'inserted', 'updated')
                    assert obj._save_pos_ is None
                    obj._status_ = 'modified'
                    obj._save_pos_ = len(objects_to_save)
                    objects_to_save.append(obj)
                    objects_to_save_needs_undo = True
                    cache.modified = True
            if not attr.reverse and not attr.is_part_of_unique_index:
                obj._vals_[attr] = new_val
                return
            is_reverse_call = undo_funcs is not None
            if not is_reverse_call: undo_funcs = []
            undo = []
            def undo_func():
                obj._status_ = status
                obj._wbits_ = wbits
                if objects_to_save_needs_undo:
                    assert objects_to_save
                    obj2 = objects_to_save.pop()
                    assert obj2 is obj and obj._save_pos_ == len(objects_to_save)
                    obj._save_pos_ = None

                if old_val is NOT_LOADED: obj._vals_.pop(attr)
                else: obj._vals_[attr] = old_val
                for cache_index, old_key, new_key in undo:
                    if new_key is not None: del cache_index[new_key]
                    if old_key is not None: cache_index[old_key] = obj
            undo_funcs.append(undo_func)
            if old_val == new_val: return
            try:
                if attr.is_unique:
                    cache.update_simple_index(obj, attr, old_val, new_val, undo)
                get_val = obj._vals_.get
                for attrs, i in attr.composite_keys:
                    vals = [ get_val(a) for a in attrs ]  # In Python 2 var name leaks into the function scope!
                    prev_vals = tuple(vals)
                    vals[i] = new_val
                    new_vals = tuple(vals)
                    cache.update_composite_index(obj, attrs, prev_vals, new_vals, undo)

                obj._vals_[attr] = new_val

                if not reverse: pass
                elif not is_reverse_call: attr.update_reverse(obj, old_val, new_val, undo_funcs)
                elif old_val not in (None, NOT_LOADED):
                    if not reverse.is_collection:
                        if new_val is not None:
                            if reverse.is_required: throw(ConstraintError,
                                'Cannot unlink %r from previous %s object, because %r attribute is required'
                                % (old_val, obj, reverse))
                            reverse.__set__(old_val, None, undo_funcs)
                    elif isinstance(reverse, Set):
                        reverse.reverse_remove((old_val,), obj, undo_funcs)
                    else: throw(NotImplementedError)
            except:
                if not is_reverse_call:
                    for undo_func in reversed(undo_funcs): undo_func()
                raise
    def db_set(attr, obj, new_dbval, is_reverse_call=False):
        cache = obj._session_cache_
        assert cache.is_alive
        assert obj._status_ not in created_or_deleted_statuses
        assert attr.pk_offset is None
        if new_dbval is NOT_LOADED: assert is_reverse_call
        old_dbval = obj._dbvals_.get(attr, NOT_LOADED)
        if old_dbval is not NOT_LOADED:
            if old_dbval == new_dbval or (
                    not attr.reverse and attr.converters[0].dbvals_equal(old_dbval, new_dbval)):
                return

        bit = obj._bits_except_volatile_[attr]
        if obj._rbits_ & bit:
            assert old_dbval is not NOT_LOADED
            if new_dbval is NOT_LOADED: diff = ''
            else: diff = ' (was: %s, now: %s)' % (old_dbval, new_dbval)
            throw(UnrepeatableReadError,
                'Value of %s.%s for %s was updated outside of current transaction%s'
                % (obj.__class__.__name__, attr.name, obj, diff))

        if new_dbval is NOT_LOADED: obj._dbvals_.pop(attr, None)
        else: obj._dbvals_[attr] = new_dbval

        wbit = bool(obj._wbits_ & bit)
        if not wbit:
            old_val = obj._vals_.get(attr, NOT_LOADED)
            assert old_val == old_dbval
            if attr.is_part_of_unique_index:
                cache = obj._session_cache_
                if attr.is_unique: cache.db_update_simple_index(obj, attr, old_val, new_dbval)
                get_val = obj._vals_.get
                for attrs, i in attr.composite_keys:
                    vals = [ get_val(a) for a in attrs ]  # In Python 2 var name leaks into the function scope!
                    old_vals = tuple(vals)
                    vals[i] = new_dbval
                    new_vals = tuple(vals)
                    cache.db_update_composite_index(obj, attrs, old_vals, new_vals)
            if new_dbval is NOT_LOADED:
                obj._vals_.pop(attr, None)
            elif attr.reverse:
                obj._vals_[attr] = new_dbval
            else:
                assert len(attr.converters) == 1
                obj._vals_[attr] = attr.converters[0].dbval2val(new_dbval, obj)

        reverse = attr.reverse
        if not reverse: pass
        elif not is_reverse_call: attr.db_update_reverse(obj, old_dbval, new_dbval)
        elif old_dbval not in (None, NOT_LOADED):
            if not reverse.is_collection:
                if new_dbval is not NOT_LOADED: reverse.db_set(old_dbval, NOT_LOADED, is_reverse_call=True)
            elif isinstance(reverse, Set):
                reverse.db_reverse_remove((old_dbval,), obj)
            else: throw(NotImplementedError)
    def update_reverse(attr, obj, old_val, new_val, undo_funcs):
        reverse = attr.reverse
        if not reverse.is_collection:
            if old_val not in (None, NOT_LOADED):
                if attr.cascade_delete: old_val._delete_(undo_funcs)
                elif reverse.is_required: throw(ConstraintError,
                    'Cannot unlink %r from previous %s object, because %r attribute is required'
                    % (old_val, obj, reverse))
                else: reverse.__set__(old_val, None, undo_funcs)
            if new_val is not None: reverse.__set__(new_val, obj, undo_funcs)
        elif isinstance(reverse, Set):
            if old_val not in (None, NOT_LOADED): reverse.reverse_remove((old_val,), obj, undo_funcs)
            if new_val is not None: reverse.reverse_add((new_val,), obj, undo_funcs)
        else: throw(NotImplementedError)
    def db_update_reverse(attr, obj, old_dbval, new_dbval):
        reverse = attr.reverse
        if not reverse.is_collection:
            if old_dbval not in (None, NOT_LOADED): reverse.db_set(old_dbval, NOT_LOADED, True)
            if new_dbval is not None: reverse.db_set(new_dbval, obj, True)
        elif isinstance(reverse, Set):
            if old_dbval not in (None, NOT_LOADED): reverse.db_reverse_remove((old_dbval,), obj)
            if new_dbval is not None: reverse.db_reverse_add((new_dbval,), obj)
        else: throw(NotImplementedError)
    def __delete__(attr, obj):
        throw(NotImplementedError)
    def get_raw_values(attr, val):
        reverse = attr.reverse
        if not reverse: return (val,)
        rentity = reverse.entity
        if val is None: return rentity._pk_nones_
        return val._get_raw_pkval_()
    def get_columns(attr):
        assert not attr.is_collection
        assert not isinstance(attr.py_type, basestring)
        if attr._columns_checked: return attr.columns

        provider = attr.entity._database_.provider
        reverse = attr.reverse
        if not reverse: # attr is not part of relationship
            if not attr.columns: attr.columns = provider.get_default_column_names(attr)
            elif len(attr.columns) > 1: throw(MappingError, "Too many columns were specified for %s" % attr)
            attr.col_paths = [ attr.name ]
            attr.converters = [ provider.get_converter_by_attr(attr) ]
        else:
            def generate_columns():
                reverse_pk_columns = reverse.entity._get_pk_columns_()
                reverse_pk_col_paths = reverse.entity._pk_paths_
                if not attr.columns:
                    attr.columns = provider.get_default_column_names(attr, reverse_pk_columns)
                elif len(attr.columns) != len(reverse_pk_columns): throw(MappingError,
                    'Invalid number of columns specified for %s' % attr)
                attr.col_paths = [ '-'.join((attr.name, paths)) for paths in reverse_pk_col_paths ]
                attr.converters = []
                for a in reverse.entity._pk_attrs_:
                    attr.converters.extend(a.converters)

            if reverse.is_collection: # one-to-many:
                generate_columns()
            # one-to-one:
            elif attr.is_required:
                assert not reverse.is_required
                generate_columns()
            elif attr.columns: generate_columns()
            elif reverse.columns: pass
            elif reverse.is_required: pass
            elif attr.entity.__name__ > reverse.entity.__name__: pass
            else: generate_columns()
        attr._columns_checked = True
        if len(attr.columns) == 1: attr.column = attr.columns[0]
        else: attr.column = None
        return attr.columns
    @property
    def asc(attr):
        return attr
    @property
    def desc(attr):
        return DescWrapper(attr)
    def describe(attr):
        t = attr.py_type
        if isinstance(t, type): t = t.__name__
        options = []
        if attr.args: options.append(', '.join(imap(str, attr.args)))
        if attr.auto: options.append('auto=True')
        if not isinstance(attr, PrimaryKey) and attr.is_unique: options.append('unique=True')
        if attr.default is not None: options.append('default=%r' % attr.default)
        if not options: options = ''
        else: options = ', ' + ', '.join(options)
        result = "%s(%s%s)" % (attr.__class__.__name__, t, options)
        return "%s = %s" % (attr.name, result)

class Optional(Attribute):
    __slots__ = []

class Required(Attribute):
    __slots__ = []
    def validate(attr, val, obj=None, entity=None, from_db=False):
        val = Attribute.validate(attr, val, obj, entity, from_db)
        if val == '' or (val is None and not (attr.auto or attr.is_volatile or attr.sql_default)):
            if not from_db:
                throw(ValueError, 'Attribute %s is required' % (attr if obj is None else '%r.%s' % (obj, attr.name)))
            else:
                warnings.warn('Database contains %s for required attribute %s'
                              % ('NULL' if val is None else 'empty string', attr),
                              DatabaseContainsIncorrectEmptyValue)
        return val

class Discriminator(Required):
    __slots__ = [ 'code2cls' ]
    def __init__(attr, py_type, *args, **kwargs):
        Attribute.__init__(attr, py_type, *args, **kwargs)
        attr.code2cls = {}
    def _init_(attr, entity, name):
        if entity._root_ is not entity: throw(ERDiagramError,
            'Discriminator attribute %s cannot be declared in subclass' % attr)
        Required._init_(attr, entity, name)
        entity._discriminator_attr_ = attr
    @staticmethod
    def create_default_attr(entity):
        if hasattr(entity, 'classtype'): throw(ERDiagramError,
            "Cannot create discriminator column for %s automatically "
            "because name 'classtype' is already in use" % entity.__name__)
        attr = Discriminator(str, column='classtype')
        attr.is_implicit = True
        attr._init_(entity, 'classtype')
        entity._attrs_.append(attr)
        entity._new_attrs_.append(attr)
        entity._adict_['classtype'] = attr
        entity.classtype = attr
        attr.process_entity_inheritance(entity)
    def process_entity_inheritance(attr, entity):
        if '_discriminator_' not in entity.__dict__:
            entity._discriminator_ = entity.__name__
        discr_value = entity._discriminator_
        if discr_value is not None:
            try: entity._discriminator_ = discr_value = attr.validate(discr_value)
            except ValueError: throw(TypeError,
                "Incorrect discriminator value is set for %s attribute '%s' of '%s' type: %r"
                % (entity.__name__, attr.name, attr.py_type.__name__, discr_value))
        elif issubclass(attr.py_type, basestring):
            discr_value = entity._discriminator_ = entity.__name__
        else: throw(TypeError, "Discriminator value for entity %s "
                               "with custom discriminator column '%s' of '%s' type is not set"
                               % (entity.__name__, attr.name, attr.py_type.__name__))
        attr.code2cls[discr_value] = entity
    def validate(attr, val, obj=None, entity=None, from_db=False):
        if from_db: return val
        elif val is DEFAULT:
            assert entity is not None
            return entity._discriminator_
        return Attribute.validate(attr, val, obj, entity)
    def load(attr, obj):
        assert False  # pragma: no cover
    def __get__(attr, obj, cls=None):
        if obj is None: return attr
        return obj._discriminator_
    def __set__(attr, obj, new_val):
        throw(TypeError, 'Cannot assign value to discriminator attribute')
    def db_set(attr, obj, new_dbval):
        assert False  # pragma: no cover
    def update_reverse(attr, obj, old_val, new_val, undo_funcs):
        assert False  # pragma: no cover

class Index(object):
    __slots__ = 'entity', 'attrs', 'is_pk', 'is_unique'
    def __init__(index, *attrs, **options):
        index.entity = None
        index.attrs = list(attrs)
        index.is_pk = options.pop('is_pk', False)
        index.is_unique = options.pop('is_unique', True)
        assert not options
    def _init_(index, entity):
        index.entity = entity
        attrs = index.attrs
        for i, attr in enumerate(index.attrs):
            if isinstance(attr, basestring):
                try: attr = getattr(entity, attr)
                except AttributeError: throw(AttributeError,
                    'Entity %s does not have attribute %s' % (entity.__name__, attr))
                attrs[i] = attr
        index.attrs = attrs = tuple(attrs)
        for i, attr in enumerate(attrs):
            if not isinstance(attr, Attribute):
                func_name = 'PrimaryKey' if index.is_pk else 'composite_key' if index.is_unique else 'composite_index'
                throw(TypeError, '%s() arguments must be attributes. Got: %r' % (func_name, attr))
            if index.is_unique:
                attr.is_part_of_unique_index = True
                if len(attrs) > 1: attr.composite_keys.append((attrs, i))
            if not issubclass(entity, attr.entity): throw(ERDiagramError,
                'Invalid use of attribute %s in entity %s' % (attr, entity.__name__))
            key_type = 'primary key' if index.is_pk else 'unique index' if index.is_unique else 'index'
            if attr.is_collection or (index.is_pk and not attr.is_required and not attr.auto):
                throw(TypeError, '%s attribute %s cannot be part of %s' % (attr.__class__.__name__, attr, key_type))
            if isinstance(attr.py_type, type) and issubclass(attr.py_type, float):
                throw(TypeError, 'Attribute %s of type float cannot be part of %s' % (attr, key_type))
            if index.is_pk and attr.is_volatile:
                throw(TypeError, 'Volatile attribute %s cannot be part of primary key' % attr)
            if not attr.is_required:
                if attr.nullable is False:
                    throw(TypeError, 'Optional attribute %s must be nullable, because it is part of composite key' % attr)
                attr.nullable = True
                if attr.is_string and attr.default == '' and not hasattr(attr, 'original_default'):
                    attr.default = None

def _define_index(func_name, attrs, is_unique=False):
    if len(attrs) < 2: throw(TypeError,
        '%s() must receive at least two attributes as arguments' % func_name)
    cls_dict = sys._getframe(2).f_locals
    indexes = cls_dict.setdefault('_indexes_', [])
    indexes.append(Index(*attrs, is_pk=False, is_unique=is_unique))

def composite_index(*attrs):
    _define_index('composite_index', attrs)

def composite_key(*attrs):
    _define_index('composite_key', attrs, is_unique=True)

class PrimaryKey(Required):
    __slots__ = []
    def __new__(cls, *args, **kwargs):
        if not args: throw(TypeError, 'PrimaryKey must receive at least one positional argument')
        cls_dict = sys._getframe(1).f_locals
        attrs = tuple(a for a in args if isinstance(a, Attribute))
        non_attrs = [ a for a in args if not isinstance(a, Attribute) ]
        cls_dict = sys._getframe(1).f_locals

        if not attrs:
            return Required.__new__(cls)
        elif non_attrs or kwargs:
            throw(TypeError, 'PrimaryKey got invalid arguments: %r %r' % (args, kwargs))
        elif len(attrs) == 1:
            attr = attrs[0]
            attr_name = 'something'
            for key, val in iteritems(cls_dict):
                if val is attr: attr_name = key; break
            py_type = attr.py_type
            type_str = py_type.__name__ if type(py_type) is type else repr(py_type)
            throw(TypeError, 'Just use %s = PrimaryKey(%s, ...) directly instead of PrimaryKey(%s)'
                  % (attr_name, type_str, attr_name))

        for i, attr in enumerate(attrs):
            attr.is_part_of_unique_index = True
            attr.composite_keys.append((attrs, i))
        indexes = cls_dict.setdefault('_indexes_', [])
        indexes.append(Index(*attrs, is_pk=True))
        return None

class Collection(Attribute):
    __slots__ = 'table', 'wrapper_class', 'symmetric', 'reverse_column', 'reverse_columns', \
                'nplus1_threshold', 'cached_load_sql', 'cached_add_m2m_sql', 'cached_remove_m2m_sql', \
                'cached_count_sql', 'cached_empty_sql'
    def __init__(attr, py_type, *args, **kwargs):
        if attr.__class__ is Collection: throw(TypeError, "'Collection' is abstract type")
        table = kwargs.pop('table', None)  # TODO: rename table to link_table or m2m_table
        if table is not None and not isinstance(table, basestring):
            if not isinstance(table, (list, tuple)):
                throw(TypeError, "Parameter 'table' must be a string. Got: %r" % table)
            for name_part in table:
                if not isinstance(name_part, basestring):
                    throw(TypeError, 'Each part of table name must be a string. Got: %r' % name_part)
            table = tuple(table)
        attr.table = table
        Attribute.__init__(attr, py_type, *args, **kwargs)
        if attr.auto: throw(TypeError, "'auto' option could not be set for collection attribute")
        kwargs = attr.kwargs

        attr.reverse_column = kwargs.pop('reverse_column', None)
        attr.reverse_columns = kwargs.pop('reverse_columns', None)
        if attr.reverse_column is not None:
            if attr.reverse_columns is not None and attr.reverse_columns != [ attr.reverse_column ]:
                throw(TypeError, "Parameters 'reverse_column' and 'reverse_columns' cannot be specified simultaneously")
            if not isinstance(attr.reverse_column, basestring):
                throw(TypeError, "Parameter 'reverse_column' must be a string. Got: %r" % attr.reverse_column)
            attr.reverse_columns = [ attr.reverse_column ]
        elif attr.reverse_columns is not None:
            if not isinstance(attr.reverse_columns, (tuple, list)):
                throw(TypeError, "Parameter 'reverse_columns' must be a list. Got: %r" % attr.reverse_columns)
            for reverse_column in attr.reverse_columns:
                if not isinstance(reverse_column, basestring):
                    throw(TypeError, "Parameter 'reverse_columns' must be a list of strings. Got: %r" % attr.reverse_columns)
            if len(attr.reverse_columns) == 1: attr.reverse_column = attr.reverse_columns[0]
        else: attr.reverse_columns = []

        attr.nplus1_threshold = kwargs.pop('nplus1_threshold', 1)
        for option in attr.kwargs: throw(TypeError, 'Unknown option %r' % option)
        attr.cached_load_sql = {}
        attr.cached_add_m2m_sql = None
        attr.cached_remove_m2m_sql = None
        attr.cached_count_sql = None
        attr.cached_empty_sql = None
    def _init_(attr, entity, name):
        Attribute._init_(attr, entity, name)
        if attr.is_unique: throw(TypeError,
            "'unique' option cannot be set for attribute %s because it is collection" % attr)
        if attr.default is not None:
            throw(TypeError, 'Default value could not be set for collection attribute')
        attr.symmetric = (attr.py_type == entity.__name__ and attr.reverse == name)
        if not attr.symmetric and attr.reverse_columns: throw(TypeError,
            "'reverse_column' and 'reverse_columns' options can be set for symmetric relations only")
        if attr.py_check is not None:
            throw(NotImplementedError, "'py_check' parameter is not supported for collection attributes")
    def load(attr, obj):
        assert False, 'Abstract method'  # pragma: no cover
    def __get__(attr, obj, cls=None):
        assert False, 'Abstract method'  # pragma: no cover
    def __set__(attr, obj, val):
        assert False, 'Abstract method'  # pragma: no cover
    def __delete__(attr, obj):
        assert False, 'Abstract method'  # pragma: no cover
    def prepare(attr, obj, val, fromdb=False):
        assert False, 'Abstract method'  # pragma: no cover
    def set(attr, obj, val, fromdb=False):
        assert False, 'Abstract method'  # pragma: no cover

class SetData(set):
    __slots__ = 'is_fully_loaded', 'added', 'removed', 'absent', 'count'
    def __init__(setdata):
        setdata.is_fully_loaded = False
        setdata.added = setdata.removed = setdata.absent = None
        setdata.count = None

def construct_batchload_criteria_list(alias, columns, converters, batch_size, row_value_syntax, start=0, from_seeds=True):
    assert batch_size > 0
    def param(i, j, converter):
        if from_seeds:
            return [ 'PARAM', (i, None, j), converter ]
        else:
            return [ 'PARAM', (i, j, None), converter ]
    if batch_size == 1:
        return [ [ converter.EQ, [ 'COLUMN', alias, column ], param(start, j, converter) ]
                 for j, (column, converter) in enumerate(izip(columns, converters)) ]
    if len(columns) == 1:
        column = columns[0]
        converter = converters[0]
        param_list = [ param(i+start, 0, converter) for i in xrange(batch_size) ]
        condition = [ 'IN', [ 'COLUMN', alias, column ], param_list ]
        return [ condition ]
    elif row_value_syntax:
        row = [ 'ROW' ] + [ [ 'COLUMN', alias, column ] for column in columns ]
        param_list = [ [ 'ROW' ] + [ param(i+start, j, converter) for j, converter in enumerate(converters) ]
                       for i in xrange(batch_size) ]
        condition = [ 'IN', row, param_list ]
        return [ condition ]
    else:
        conditions = [ [ 'AND' ] + [ [ converter.EQ, [ 'COLUMN', alias, column ], param(i+start, j, converter) ]
                                     for j, (column, converter) in enumerate(izip(columns, converters)) ]
                       for i in xrange(batch_size) ]
        return [ [ 'OR' ] + conditions ]

class Set(Collection):
    __slots__ = []
    def validate(attr, val, obj=None, entity=None, from_db=False):
        assert val is not NOT_LOADED
        if val is DEFAULT: return set()
        reverse = attr.reverse
        if val is None: throw(ValueError, 'A single %(cls)s instance or %(cls)s iterable is expected. '
                                          'Got: None' % dict(cls=reverse.entity.__name__))
        if entity is not None: pass
        elif obj is not None: entity = obj.__class__
        else: entity = attr.entity
        if not reverse: throw(NotImplementedError)
        if isinstance(val, reverse.entity): items = set((val,))
        else:
            rentity = reverse.entity
            try: items = set(val)
            except TypeError: throw(TypeError, 'Item of collection %s.%s must be an instance of %s. Got: %r'
                                              % (entity.__name__, attr.name, rentity.__name__, val))
            for item in items:
                if not isinstance(item, rentity):
                    throw(TypeError, 'Item of collection %s.%s must be an instance of %s. Got: %r'
                                    % (entity.__name__, attr.name, rentity.__name__, item))
        if obj is not None: cache = obj._session_cache_
        else: cache = entity._database_._get_cache()
        for item in items:
            if item._session_cache_ is not cache:
                throw(TransactionError, 'An attempt to mix objects belonging to different transactions')
        return items
    def load(attr, obj, items=None):
        cache = obj._session_cache_
        if not cache.is_alive: throw(DatabaseSessionIsOver,
            'Cannot load collection %s.%s: the database session is over' % (safe_repr(obj), attr.name))
        assert obj._status_ not in del_statuses
        setdata = obj._vals_.get(attr)
        if setdata is None: setdata = obj._vals_[attr] = SetData()
        elif setdata.is_fully_loaded: return setdata
        entity = attr.entity
        reverse = attr.reverse
        rentity = reverse.entity
        if not reverse: throw(NotImplementedError)
        database = obj._database_
        if cache is not database._get_cache():
            throw(TransactionError, "Transaction of object %s belongs to different thread")

        if items:
            if not reverse.is_collection:
                items = {item for item in items if reverse not in item._vals_}
            else:
                items = set(items)
                items -= setdata
                if setdata.removed: items -= setdata.removed
            if not items: return setdata

        if items and (attr.lazy or not setdata):
            items = list(items)
            if not reverse.is_collection:
                sql, adapter, attr_offsets = rentity._construct_batchload_sql_(len(items))
                arguments = adapter(items)
                cursor = database._exec_sql(sql, arguments)
                items = rentity._fetch_objects(cursor, attr_offsets)
                return setdata

            sql, adapter = attr.construct_sql_m2m(1, len(items))
            items.append(obj)
            arguments = adapter(items)
            cursor = database._exec_sql(sql, arguments)
            loaded_items = {rentity._get_by_raw_pkval_(row) for row in cursor.fetchall()}
            setdata |= loaded_items
            reverse.db_reverse_add(loaded_items, obj)
            return setdata

        counter = cache.collection_statistics.setdefault(attr, 0)
        nplus1_threshold = attr.nplus1_threshold
        prefetching = options.PREFETCHING and not attr.lazy and nplus1_threshold is not None \
                      and (counter >= nplus1_threshold or cache.noflush_counter)

        objects = [ obj ]
        setdata_list = [ setdata ]
        if prefetching:
            pk_index = cache.indexes[entity._pk_attrs_]
            max_batch_size = database.provider.max_params_count // len(entity._pk_columns_)
            for obj2 in itervalues(pk_index):
                if obj2 is obj: continue
                if obj2._status_ in created_or_deleted_statuses: continue
                setdata2 = obj2._vals_.get(attr)
                if setdata2 is None: setdata2 = obj2._vals_[attr] = SetData()
                elif setdata2.is_fully_loaded: continue
                objects.append(obj2)
                setdata_list.append(setdata2)
                if len(objects) >= max_batch_size: break

        if not reverse.is_collection:
            sql, adapter, attr_offsets = rentity._construct_batchload_sql_(len(objects), reverse)
            arguments = adapter(objects)
            cursor = database._exec_sql(sql, arguments)
            items = rentity._fetch_objects(cursor, attr_offsets)
        else:
            sql, adapter = attr.construct_sql_m2m(len(objects))
            arguments = adapter(objects)
            cursor = database._exec_sql(sql, arguments)
            pk_len = len(entity._pk_columns_)
            d = {}
            if len(objects) > 1:
                for row in cursor.fetchall():
                    obj2 = entity._get_by_raw_pkval_(row[:pk_len])
                    item = rentity._get_by_raw_pkval_(row[pk_len:])
                    items = d.get(obj2)
                    if items is None: items = d[obj2] = set()
                    items.add(item)
            else: d[obj] = {rentity._get_by_raw_pkval_(row) for row in cursor.fetchall()}
            for obj2, items in iteritems(d):
                setdata2 = obj2._vals_.get(attr)
                if setdata2 is None: setdata2 = obj._vals_[attr] = SetData()
                else:
                    phantoms = setdata2 - items
                    if setdata2.added: phantoms -= setdata2.added
                    if phantoms: throw(UnrepeatableReadError,
                        'Phantom object %s disappeared from collection %s.%s'
                        % (safe_repr(phantoms.pop()), safe_repr(obj), attr.name))
                items -= setdata2
                if setdata2.removed: items -= setdata2.removed
                setdata2 |= items
                reverse.db_reverse_add(items, obj2)

        for setdata2 in setdata_list:
            setdata2.is_fully_loaded = True
            setdata2.absent = None
            setdata2.count = len(setdata2)
        cache.collection_statistics[attr] = counter + 1
        return setdata
    def construct_sql_m2m(attr, batch_size=1, items_count=0):
        if items_count:
            assert batch_size == 1
            cache_key = -items_count
        else: cache_key = batch_size
        cached_sql = attr.cached_load_sql.get(cache_key)
        if cached_sql is not None: return cached_sql
        reverse = attr.reverse
        assert reverse is not None and reverse.is_collection and issubclass(reverse.py_type, Entity)
        table_name = attr.table
        assert table_name is not None
        select_list = [ 'ALL' ]
        if not attr.symmetric:
            columns = attr.columns
            converters = attr.converters
            rcolumns = reverse.columns
            rconverters = reverse.converters
        else:
            columns = attr.reverse_columns
            rcolumns = attr.columns
            converters = rconverters = attr.converters
        if batch_size > 1:
            select_list.extend([ 'COLUMN', 'T1', column ] for column in rcolumns)
        select_list.extend([ 'COLUMN', 'T1', column ] for column in columns)
        from_list = [ 'FROM', [ 'T1', 'TABLE', table_name ]]
        database = attr.entity._database_
        row_value_syntax = database.provider.translator_cls.row_value_syntax
        where_list = [ 'WHERE' ]
        where_list += construct_batchload_criteria_list(
            'T1', rcolumns, rconverters, batch_size, row_value_syntax, items_count)
        if items_count:
            where_list += construct_batchload_criteria_list(
                'T1', columns, converters, items_count, row_value_syntax)
        sql_ast = [ 'SELECT', select_list, from_list, where_list ]
        sql, adapter = attr.cached_load_sql[cache_key] = database._ast2sql(sql_ast)
        return sql, adapter
    def copy(attr, obj):
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        if obj._vals_ is None: throw_db_session_is_over(obj, attr)
        setdata = obj._vals_.get(attr)
        if setdata is None or not setdata.is_fully_loaded: setdata = attr.load(obj)
        reverse = attr.reverse
        if not reverse.is_collection and reverse.pk_offset is None:
            added = setdata.added or ()
            for item in setdata:
                if item in added: continue
                bit = item._bits_except_volatile_[reverse]
                assert item._wbits_ is not None
                if not item._wbits_ & bit: item._rbits_ |= bit
        return set(setdata)
    @cut_traceback
    def __get__(attr, obj, cls=None):
        if obj is None: return attr
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        rentity = attr.py_type
        wrapper_class = rentity._get_set_wrapper_subclass_()
        return wrapper_class(obj, attr)
    @cut_traceback
    def __set__(attr, obj, new_items, undo_funcs=None):
        if isinstance(new_items, SetInstance) and new_items._obj_ is obj and new_items._attr_ is attr:
            return  # after += or -=
        cache = obj._session_cache_
        if not cache.is_alive: throw(DatabaseSessionIsOver,
            'Cannot change collection %s.%s: the database session is over' % (safe_repr(obj), attr))
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        with cache.flush_disabled():
            new_items = attr.validate(new_items, obj)
            reverse = attr.reverse
            if not reverse: throw(NotImplementedError)
            setdata = obj._vals_.get(attr)
            if setdata is None:
                if obj._status_ == 'created':
                    setdata = obj._vals_[attr] = SetData()
                    setdata.is_fully_loaded = True
                    setdata.count = 0
                else: setdata = attr.load(obj)
            elif not setdata.is_fully_loaded: setdata = attr.load(obj)
            if new_items == setdata: return
            to_add = new_items - setdata
            to_remove = setdata - new_items
            is_reverse_call = undo_funcs is not None
            if not is_reverse_call: undo_funcs = []
            try:
                if not reverse.is_collection:
                    if attr.cascade_delete:
                        for item in to_remove: item._delete_(undo_funcs)
                    else:
                        for item in to_remove: reverse.__set__(item, None, undo_funcs)
                    for item in to_add: reverse.__set__(item, obj, undo_funcs)
                else:
                    reverse.reverse_remove(to_remove, obj, undo_funcs)
                    reverse.reverse_add(to_add, obj, undo_funcs)
            except:
                if not is_reverse_call:
                    for undo_func in reversed(undo_funcs): undo_func()
                raise
        setdata.clear()
        setdata |= new_items
        if setdata.count is not None: setdata.count = len(new_items)
        added = setdata.added
        removed = setdata.removed
        if to_add:
            if removed: (to_add, setdata.removed) = (to_add - removed, removed - to_add)
            if added: added |= to_add
            else: setdata.added = to_add  # added may be None
        if to_remove:
            if added: (to_remove, setdata.added) = (to_remove - added, added - to_remove)
            if removed: removed |= to_remove
            else: setdata.removed = to_remove  # removed may be None
        cache.modified_collections[attr].add(obj)
        cache.modified = True
    def __delete__(attr, obj):
        throw(NotImplementedError)
    def reverse_add(attr, objects, item, undo_funcs):
        undo = []
        cache = item._session_cache_
        objects_with_modified_collections = cache.modified_collections[attr]
        for obj in objects:
            setdata = obj._vals_.get(attr)
            if setdata is None: setdata = obj._vals_[attr] = SetData()
            else: assert item not in setdata
            if setdata.added is None: setdata.added = set()
            else: assert item not in setdata.added
            in_removed = setdata.removed and item in setdata.removed
            was_modified_earlier = obj in objects_with_modified_collections
            undo.append((obj, in_removed, was_modified_earlier))
            setdata.add(item)
            if setdata.count is not None: setdata.count += 1
            if in_removed: setdata.removed.remove(item)
            else: setdata.added.add(item)
            objects_with_modified_collections.add(obj)
        def undo_func():
            for obj, in_removed, was_modified_earlier in undo:
                setdata = obj._vals_[attr]
                setdata.remove(item)
                if setdata.count is not None: setdata.count -= 1
                if in_removed: setdata.removed.add(item)
                else: setdata.added.remove(item)
                if not was_modified_earlier: objects_with_modified_collections.remove(obj)
        undo_funcs.append(undo_func)
    def db_reverse_add(attr, objects, item):
        for obj in objects:
            setdata = obj._vals_.get(attr)
            if setdata is None: setdata = obj._vals_[attr] = SetData()
            elif setdata.is_fully_loaded: throw(UnrepeatableReadError,
                'Phantom object %s appeared in collection %s.%s' % (safe_repr(item), safe_repr(obj), attr.name))
            setdata.add(item)
    def reverse_remove(attr, objects, item, undo_funcs):
        undo = []
        cache = item._session_cache_
        objects_with_modified_collections = cache.modified_collections[attr]
        for obj in objects:
            setdata = obj._vals_.get(attr)
            assert setdata is not None
            assert item in setdata
            if setdata.removed is None: setdata.removed = set()
            else: assert item not in setdata.removed
            in_added = setdata.added and item in setdata.added
            was_modified_earlier = obj in objects_with_modified_collections
            undo.append((obj, in_added, was_modified_earlier))
            objects_with_modified_collections.add(obj)
            setdata.remove(item)
            if setdata.count is not None: setdata.count -= 1
            if in_added: setdata.added.remove(item)
            else: setdata.removed.add(item)
        def undo_func():
            for obj, in_removed, was_modified_earlier in undo:
                setdata = obj._vals_[attr]
                setdata.add(item)
                if setdata.count is not None: setdata.count += 1
                if in_added: setdata.added.add(item)
                else: setdata.removed.remove(item)
                if not was_modified_earlier: objects_with_modified_collections.remove(obj)
        undo_funcs.append(undo_func)
    def db_reverse_remove(attr, objects, item):
        for obj in objects:
            setdata = obj._vals_[attr]
            setdata.remove(item)
    def get_m2m_columns(attr, is_reverse=False):
        reverse = attr.reverse
        entity = attr.entity
        pk_length = len(entity._get_pk_columns_())
        provider = entity._database_.provider
        if attr.symmetric or entity is reverse.entity:
            if attr._columns_checked:
                if not attr.symmetric: return attr.columns
                if not is_reverse: return attr.columns
                return attr.reverse_columns

            if not attr.symmetric: assert not reverse._columns_checked
            if attr.columns:
                if len(attr.columns) != pk_length: throw(MappingError,
                    'Invalid number of columns for %s' % reverse)
            else: attr.columns = provider.get_default_m2m_column_names(entity)
            attr._columns_checked = True
            attr.converters = entity._pk_converters_

            if attr.symmetric:
                if not attr.reverse_columns:
                    attr.reverse_columns = [ column + '_2' for column in attr.columns ]
                elif len(attr.reverse_columns) != pk_length:
                    throw(MappingError, "Invalid number of reverse columns for symmetric attribute %s" % attr)
                return attr.columns if not is_reverse else attr.reverse_columns
            else:
                if not reverse.columns:
                    reverse.columns = [ column + '_2' for column in attr.columns ]
                reverse._columns_checked = True
                reverse.converters = entity._pk_converters_
                return attr.columns if not is_reverse else reverse.columns

        if attr._columns_checked: return reverse.columns
        elif reverse.columns:
            if len(reverse.columns) != pk_length: throw(MappingError,
                'Invalid number of columns for %s' % reverse)
        else: reverse.columns = provider.get_default_m2m_column_names(entity)
        reverse.converters = entity._pk_converters_
        attr._columns_checked = True
        return reverse.columns
    def remove_m2m(attr, removed):
        assert removed
        entity = attr.entity
        database = entity._database_
        cached_sql = attr.cached_remove_m2m_sql
        if cached_sql is None:
            reverse = attr.reverse
            where_list = [ 'WHERE' ]
            if attr.symmetric:
                columns = attr.columns + attr.reverse_columns
                converters = attr.converters + attr.converters
            else:
                columns = reverse.columns + attr.columns
                converters = reverse.converters + attr.converters
            for i, (column, converter) in enumerate(izip(columns, converters)):
                where_list.append([ converter.EQ, ['COLUMN', None, column], [ 'PARAM', (i, None, None), converter ] ])
            from_ast = [ 'FROM', [ None, 'TABLE', attr.table ] ]
            sql_ast = [ 'DELETE', None, from_ast, where_list ]
            sql, adapter = database._ast2sql(sql_ast)
            attr.cached_remove_m2m_sql = sql, adapter
        else: sql, adapter = cached_sql
        arguments_list = [ adapter(obj._get_raw_pkval_() + robj._get_raw_pkval_())
                           for obj, robj in removed ]
        database._exec_sql(sql, arguments_list)
    def add_m2m(attr, added):
        assert added
        entity = attr.entity
        database = entity._database_
        cached_sql = attr.cached_add_m2m_sql
        if cached_sql is None:
            reverse = attr.reverse
            if attr.symmetric:
                columns = attr.columns + attr.reverse_columns
                converters = attr.converters + attr.converters
            else:
                columns = reverse.columns + attr.columns
                converters = reverse.converters + attr.converters
            params = [ [ 'PARAM', (i, None, None), converter ] for i, converter in enumerate(converters) ]
            sql_ast = [ 'INSERT', attr.table, columns, params ]
            sql, adapter = database._ast2sql(sql_ast)
            attr.cached_add_m2m_sql = sql, adapter
        else: sql, adapter = cached_sql
        arguments_list = [ adapter(obj._get_raw_pkval_() + robj._get_raw_pkval_())
                           for obj, robj in added ]
        database._exec_sql(sql, arguments_list)
    @cut_traceback
    @db_session(ddl=True)
    def drop_table(attr, with_all_data=False):
        if attr.reverse.is_collection: table_name = attr.table
        else: table_name = attr.entity._table_
        attr.entity._database_._drop_tables([ table_name ], True, with_all_data)

def unpickle_setwrapper(obj, attrname, items):
    attr = getattr(obj.__class__, attrname)
    wrapper_cls = attr.py_type._get_set_wrapper_subclass_()
    wrapper = wrapper_cls(obj, attr)
    setdata = obj._vals_.get(attr)
    if setdata is None: setdata = obj._vals_[attr] = SetData()
    setdata.is_fully_loaded = True
    setdata.absent = None
    setdata.count = len(setdata)
    return wrapper

class SetInstance(object):
    __slots__ = '_obj_', '_attr_', '_attrnames_'
    _parent_ = None
    def __init__(wrapper, obj, attr):
        wrapper._obj_ = obj
        wrapper._attr_ = attr
        wrapper._attrnames_ = (attr.name,)
    def __reduce__(wrapper):
        return unpickle_setwrapper, (wrapper._obj_, wrapper._attr_.name, wrapper.copy())
    @cut_traceback
    def copy(wrapper):
        return wrapper._attr_.copy(wrapper._obj_)
    @cut_traceback
    def __repr__(wrapper):
        return '<%s %r.%s>' % (wrapper.__class__.__name__, wrapper._obj_, wrapper._attr_.name)
    @cut_traceback
    def __str__(wrapper):
        if not wrapper._obj_._session_cache_.is_alive: content = '...'
        else: content = ', '.join(imap(str, wrapper))
        return '%s([%s])' % (wrapper.__class__.__name__, content)
    @cut_traceback
    def __nonzero__(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        if obj._vals_ is None: throw_db_session_is_over(obj, attr)
        setdata = obj._vals_.get(attr)
        if setdata is None: setdata = attr.load(obj)
        if setdata: return True
        if not setdata.is_fully_loaded: setdata = attr.load(obj)
        return bool(setdata)
    @cut_traceback
    def is_empty(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        if obj._vals_ is None: throw_db_session_is_over(obj, attr)
        setdata = obj._vals_.get(attr)
        if setdata is None: setdata = obj._vals_[attr] = SetData()
        elif setdata.is_fully_loaded: return not setdata
        elif setdata: return False
        elif setdata.count is not None: return not setdata.count
        entity = attr.entity
        reverse = attr.reverse
        rentity = reverse.entity
        database = entity._database_
        cached_sql = attr.cached_empty_sql
        if cached_sql is None:
            where_list = [ 'WHERE' ]
            for i, (column, converter) in enumerate(izip(reverse.columns, reverse.converters)):
                where_list.append([ converter.EQ, [ 'COLUMN', None, column ], [ 'PARAM', (i, None, None), converter ] ])
            if not reverse.is_collection:
                table_name = rentity._table_
                select_list, attr_offsets = rentity._construct_select_clause_()
            else:
                table_name = attr.table
                select_list = [ 'ALL' ] + [ [ 'COLUMN', None, column ] for column in attr.columns ]
                attr_offsets = None
            sql_ast = [ 'SELECT', select_list, [ 'FROM', [ None, 'TABLE', table_name ] ],
                        where_list, [ 'LIMIT', [ 'VALUE', 1 ] ] ]
            sql, adapter = database._ast2sql(sql_ast)
            attr.cached_empty_sql = sql, adapter, attr_offsets
        else: sql, adapter, attr_offsets = cached_sql
        arguments = adapter(obj._get_raw_pkval_())
        cursor = database._exec_sql(sql, arguments)
        if reverse.is_collection:
            row = cursor.fetchone()
            if row is not None:
                loaded_item = rentity._get_by_raw_pkval_(row)
                setdata.add(loaded_item)
                reverse.db_reverse_add((loaded_item,), obj)
        else: rentity._fetch_objects(cursor, attr_offsets)
        if setdata: return False
        setdata.is_fully_loaded = True
        setdata.absent = None
        setdata.count = 0
        return True
    @cut_traceback
    def __len__(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        if obj._vals_ is None: throw_db_session_is_over(obj, attr)
        setdata = obj._vals_.get(attr)
        if setdata is None or not setdata.is_fully_loaded: setdata = attr.load(obj)
        return len(setdata)
    @cut_traceback
    def count(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        cache = obj._session_cache_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        if obj._vals_ is None: throw_db_session_is_over(obj, attr)
        setdata = obj._vals_.get(attr)
        if setdata is None: setdata = obj._vals_[attr] = SetData()
        elif setdata.count is not None: return setdata.count
        entity = attr.entity
        reverse = attr.reverse
        database = entity._database_
        cached_sql = attr.cached_count_sql
        if cached_sql is None:
            where_list = [ 'WHERE' ]
            for i, (column, converter) in enumerate(izip(reverse.columns, reverse.converters)):
                where_list.append([ converter.EQ, [ 'COLUMN', None, column ], [ 'PARAM', (i, None, None), converter ] ])
            if not reverse.is_collection: table_name = reverse.entity._table_
            else: table_name = attr.table
            sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'COUNT', 'ALL' ] ],
                                  [ 'FROM', [ None, 'TABLE', table_name ] ], where_list ]
            sql, adapter = database._ast2sql(sql_ast)
            attr.cached_count_sql = sql, adapter
        else: sql, adapter = cached_sql
        arguments = adapter(obj._get_raw_pkval_())
        with cache.flush_disabled():
            cursor = database._exec_sql(sql, arguments)
        setdata.count = cursor.fetchone()[0]
        if setdata.added: setdata.count += len(setdata.added)
        if setdata.removed: setdata.count -= len(setdata.removed)
        return setdata.count
    @cut_traceback
    def __iter__(wrapper):
        return iter(wrapper.copy())
    @cut_traceback
    def __eq__(wrapper, other):
        if isinstance(other, SetInstance):
            if wrapper._obj_ is other._obj_ and wrapper._attr_ is other._attr_: return True
            else: other = other.copy()
        elif not isinstance(other, set): other = set(other)
        items = wrapper.copy()
        return items == other
    @cut_traceback
    def __ne__(wrapper, other):
        return not wrapper.__eq__(other)
    @cut_traceback
    def __add__(wrapper, new_items):
        return wrapper.copy().union(new_items)
    @cut_traceback
    def __sub__(wrapper, items):
        return wrapper.copy().difference(items)
    @cut_traceback
    def __contains__(wrapper, item):
        attr = wrapper._attr_
        obj = wrapper._obj_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        if obj._vals_ is None: throw_db_session_is_over(obj, attr)
        if not isinstance(item, attr.py_type): return False

        reverse = attr.reverse
        if not reverse.is_collection:
            obj2 = item._vals_[reverse] if reverse in item._vals_ else reverse.load(item)
            wbits = item._wbits_
            if wbits is not None:
                bit = item._bits_except_volatile_[reverse]
                if not wbits & bit: item._rbits_ |= bit
            return obj is obj2

        setdata = obj._vals_.get(attr)
        if setdata is not None:
            if item in setdata: return True
            if setdata.is_fully_loaded: return False
            if setdata.absent is not None and item in setdata.absent: return False
        else:
            reverse_setdata = item._vals_.get(reverse)
            if reverse_setdata is not None and reverse_setdata.is_fully_loaded:
                return obj in reverse_setdata
        setdata = attr.load(obj, (item,))
        if item in setdata: return True
        if setdata.absent is None: setdata.absent = set()
        setdata.absent.add(item)
        return False
    @cut_traceback
    def create(wrapper, **kwargs):
        attr = wrapper._attr_
        reverse = attr.reverse
        if reverse.name in kwargs: throw(TypeError,
            'When using %s.%s.create(), %r attribute should not be passed explicitly'
            % (attr.entity.__name__, attr.name, reverse.name))
        kwargs[reverse.name] = wrapper._obj_
        item_type = attr.py_type
        item = item_type(**kwargs)
        wrapper.add(item)
        return item
    @cut_traceback
    def add(wrapper, new_items):
        obj = wrapper._obj_
        attr = wrapper._attr_
        cache = obj._session_cache_
        if not cache.is_alive: throw(DatabaseSessionIsOver,
            'Cannot change collection %s.%s: the database session is over' % (safe_repr(obj), attr))
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        with cache.flush_disabled():
            reverse = attr.reverse
            if not reverse: throw(NotImplementedError)
            new_items = attr.validate(new_items, obj)
            if not new_items: return
            setdata = obj._vals_.get(attr)
            if setdata is not None: new_items -= setdata
            if setdata is None or not setdata.is_fully_loaded:
                setdata = attr.load(obj, new_items)
            new_items -= setdata
            undo_funcs = []
            try:
                if not reverse.is_collection:
                    for item in new_items: reverse.__set__(item, obj, undo_funcs)
                else: reverse.reverse_add(new_items, obj, undo_funcs)
            except:
                for undo_func in reversed(undo_funcs): undo_func()
                raise
        setdata |= new_items
        if setdata.count is not None: setdata.count += len(new_items)
        added = setdata.added
        removed = setdata.removed
        if removed: (new_items, setdata.removed) = (new_items-removed, removed-new_items)
        if added: added |= new_items
        else: setdata.added = new_items  # added may be None

        cache.modified_collections[attr].add(obj)
        cache.modified = True
    @cut_traceback
    def __iadd__(wrapper, items):
        wrapper.add(items)
        return wrapper
    @cut_traceback
    def remove(wrapper, items):
        obj = wrapper._obj_
        attr = wrapper._attr_
        cache = obj._session_cache_
        if not cache.is_alive: throw(DatabaseSessionIsOver,
            'Cannot change collection %s.%s: the database session is over' % (safe_repr(obj), attr))
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        with cache.flush_disabled():
            reverse = attr.reverse
            if not reverse: throw(NotImplementedError)
            items = attr.validate(items, obj)
            setdata = obj._vals_.get(attr)
            if setdata is not None and setdata.removed:
                items -= setdata.removed
            if not items: return
            if setdata is None or not setdata.is_fully_loaded:
                setdata = attr.load(obj, items)
            items &= setdata
            undo_funcs = []
            try:
                if not reverse.is_collection:
                    if attr.cascade_delete:
                        for item in items: item._delete_(undo_funcs)
                    else:
                        for item in items: reverse.__set__(item, None, undo_funcs)
                else: reverse.reverse_remove(items, obj, undo_funcs)
            except:
                for undo_func in reversed(undo_funcs): undo_func()
                raise
        setdata -= items
        if setdata.count is not None: setdata.count -= len(items)
        added = setdata.added
        removed = setdata.removed
        if added: (items, setdata.added) = (items - added, added - items)
        if removed: removed |= items
        else: setdata.removed = items  # removed may be None

        cache.modified_collections[attr].add(obj)
        cache.modified = True
    @cut_traceback
    def __isub__(wrapper, items):
        wrapper.remove(items)
        return wrapper
    @cut_traceback
    def clear(wrapper):
        obj = wrapper._obj_
        attr = wrapper._attr_
        if not obj._session_cache_.is_alive: throw(DatabaseSessionIsOver,
            'Cannot change collection %s.%s: the database session is over' % (safe_repr(obj), attr))
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        attr.__set__(obj, ())
    @cut_traceback
    def load(wrapper):
        wrapper._attr_.load(wrapper._obj_)
    @cut_traceback
    def select(wrapper, *args):
        obj = wrapper._obj_
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        attr = wrapper._attr_
        reverse = attr.reverse
        query = reverse.entity._select_all()
        s = 'lambda item: JOIN(obj in item.%s)' if reverse.is_collection else 'lambda item: item.%s == obj'
        query = query.filter(s % reverse.name, {'obj' : obj, 'JOIN': JOIN})
        if args:
            func, globals, locals = get_globals_and_locals(args, kwargs=None, frame_depth=3)
            query = query.filter(func, globals, locals)
        return query
    filter = select
    def limit(wrapper, limit, offset=None):
        return wrapper.select().limit(limit, offset)
    def page(wrapper, pagenum, pagesize=10):
        return wrapper.select().page(pagenum, pagesize)
    def order_by(wrapper, *args):
        return wrapper.select().order_by(*args)
    def random(wrapper, limit):
        return wrapper.select().random(limit)

def unpickle_multiset(obj, attrnames, items):
    entity = obj.__class__
    for name in attrnames:
        attr = entity._adict_[name]
        if attr.reverse: entity = attr.py_type
        else:
            entity = None
            break
    if entity is None: multiset_cls = Multiset
    else: multiset_cls = entity._get_multiset_subclass_()
    return multiset_cls(obj, attrnames, items)

class Multiset(object):
    __slots__ = [ '_obj_', '_attrnames_', '_items_' ]
    @cut_traceback
    def __init__(multiset, obj, attrnames, items):
        multiset._obj_ = obj
        multiset._attrnames_ = attrnames
        if type(items) is dict: multiset._items_ = items
        else: multiset._items_ = utils.distinct(items)
    def __reduce__(multiset):
        return unpickle_multiset, (multiset._obj_, multiset._attrnames_, multiset._items_)
    @cut_traceback
    def distinct(multiset):
        return multiset._items_.copy()
    @cut_traceback
    def __repr__(multiset):
        if multiset._obj_._session_cache_.is_alive:
            size = builtins.sum(itervalues(multiset._items_))
            if size == 1: size_str = ' (1 item)'
            else: size_str = ' (%d items)' % size
        else: size_str = ''
        return '<%s %r.%s%s>' % (multiset.__class__.__name__, multiset._obj_,
                                 '.'.join(multiset._attrnames_), size_str)
    @cut_traceback
    def __str__(multiset):
        items_str = '{%s}' % ', '.join('%r: %r' % pair for pair in sorted(iteritems(multiset._items_)))
        return '%s(%s)' % (multiset.__class__.__name__, items_str)
    @cut_traceback
    def __nonzero__(multiset):
        return bool(multiset._items_)
    @cut_traceback
    def __len__(multiset):
        return builtins.sum(multiset._items_.values())
    @cut_traceback
    def __iter__(multiset):
        for item, cnt in iteritems(multiset._items_):
            for i in xrange(cnt): yield item
    @cut_traceback
    def __eq__(multiset, other):
        if isinstance(other, Multiset):
            return multiset._items_ == other._items_
        if isinstance(other, dict):
            return multiset._items_ == other
        if hasattr(other, 'keys'):
            return multiset._items_ == dict(other)
        return multiset._items_ == utils.distinct(other)
    @cut_traceback
    def __ne__(multiset, other):
        return not multiset.__eq__(other)
    @cut_traceback
    def __contains__(multiset, item):
        return item in multiset._items_

##class List(Collection): pass
##class Dict(Collection): pass
##class Relation(Collection): pass

class EntityIter(object):
    def __init__(self, entity):
        self.entity = entity
    def next(self):
        throw(TypeError, 'Use select(...) function or %s.select(...) method for iteration'
                         % self.entity.__name__)
    if not PY2: __next__ = next

entity_id_counter = itertools.count(1)
new_instance_id_counter = itertools.count(1)

select_re = re.compile(r'select\b', re.IGNORECASE)
lambda_re = re.compile(r'lambda\b')

class EntityMeta(type):
    def __new__(meta, name, bases, cls_dict):
        if 'Entity' in globals():
            if '__slots__' in cls_dict: throw(TypeError, 'Entity classes cannot contain __slots__ variable')
            cls_dict['__slots__'] = ()
        return super(EntityMeta, meta).__new__(meta, name, bases, cls_dict)
    @cut_traceback
    def __init__(entity, name, bases, cls_dict):
        super(EntityMeta, entity).__init__(name, bases, cls_dict)
        entity._database_ = None
        if name == 'Entity': return

        if not entity.__name__[:1].isupper():
            throw(ERDiagramError, 'Entity class name should start with a capital letter. Got: %s' % entity.__name__)
        databases = set()
        for base_class in bases:
            if isinstance(base_class, EntityMeta):
                database = base_class._database_
                if database is None: throw(ERDiagramError, 'Base Entity does not belong to any database')
                databases.add(database)
        if not databases: assert False  # pragma: no cover
        elif len(databases) > 1: throw(ERDiagramError,
            'With multiple inheritance of entities, all entities must belong to the same database')
        database = databases.pop()

        if entity.__name__ in database.entities:
            throw(ERDiagramError, 'Entity %s already exists' % entity.__name__)
        assert entity.__name__ not in database.__dict__

        if database.schema is not None: throw(ERDiagramError,
            'Cannot define entity %r: database mapping has already been generated' % entity.__name__)

        entity._database_ = database

        entity._id_ = next(entity_id_counter)
        direct_bases = [ c for c in entity.__bases__ if issubclass(c, Entity) and c.__name__ != 'Entity' ]
        entity._direct_bases_ = direct_bases
        all_bases = entity._all_bases_ = set()
        entity._subclasses_ = set()
        for base in direct_bases:
            all_bases.update(base._all_bases_)
            all_bases.add(base)
        for base in all_bases:
            base._subclasses_.add(entity)
        if direct_bases:
            root = entity._root_ = direct_bases[0]._root_
            for base in direct_bases[1:]:
                if base._root_ is not root: throw(ERDiagramError, 'Multiple inheritance graph must be diamond-like. '
                    "Entity %s inherits from %s and %s entities which don't have common base class."
                    % (name, root.__name__, base._root_.__name__))
            if root._discriminator_attr_ is None:
                assert root._discriminator_ is None
                Discriminator.create_default_attr(root)
        else:
            entity._root_ = entity
            entity._discriminator_attr_ = None

        base_attrs = []
        base_attrs_dict = {}
        for base in direct_bases:
            for a in base._attrs_:
                prev = base_attrs_dict.get(a.name)
                if prev is None:
                    base_attrs_dict[a.name] = a
                    base_attrs.append(a)
                elif prev is not a: throw(ERDiagramError,
                    'Attribute "%s" clashes with attribute "%s" in derived entity "%s"'
                    % (prev, a, entity.__name__))
        entity._base_attrs_ = base_attrs

        new_attrs = []
        for name, attr in items_list(entity.__dict__):
            if name in base_attrs_dict: throw(ERDiagramError, "Name '%s' hides base attribute %s" % (name,base_attrs_dict[name]))
            if not isinstance(attr, Attribute): continue
            if name.startswith('_') and name.endswith('_'): throw(ERDiagramError,
                'Attribute name cannot both start and end with underscore. Got: %s' % name)
            if attr.entity is not None: throw(ERDiagramError,
                'Duplicate use of attribute %s in entity %s' % (attr, entity.__name__))
            attr._init_(entity, name)
            new_attrs.append(attr)
        new_attrs.sort(key=attrgetter('id'))

        indexes = entity._indexes_ = entity.__dict__.get('_indexes_', [])
        for attr in new_attrs:
            if attr.is_unique: indexes.append(Index(attr, is_pk=isinstance(attr, PrimaryKey)))
        for index in indexes: index._init_(entity)
        primary_keys = {index.attrs for index in indexes if index.is_pk}
        if direct_bases:
            if primary_keys: throw(ERDiagramError, 'Primary key cannot be redefined in derived classes')
            base_indexes = []
            for base in direct_bases:
                for index in base._indexes_:
                    if index not in base_indexes and index not in indexes: base_indexes.append(index)
            indexes[:0] = base_indexes
            primary_keys = {index.attrs for index in indexes if index.is_pk}

        if len(primary_keys) > 1: throw(ERDiagramError, 'Only one primary key can be defined in each entity class')
        elif not primary_keys:
            if hasattr(entity, 'id'): throw(ERDiagramError,
                "Cannot create default primary key attribute for %s because name 'id' is already in use."
                " Please create a PrimaryKey attribute for entity %s or rename the 'id' attribute"
                % (entity.__name__, entity.__name__))
            attr = PrimaryKey(int, auto=True)
            attr.is_implicit = True
            attr._init_(entity, 'id')
            entity.id = attr
            new_attrs.insert(0, attr)
            pk_attrs = (attr,)
            index = Index(attr, is_pk=True)
            indexes.insert(0, index)
            index._init_(entity)
        else: pk_attrs = primary_keys.pop()
        for i, attr in enumerate(pk_attrs): attr.pk_offset = i
        entity._pk_columns_ = None
        entity._pk_attrs_ = pk_attrs
        entity._pk_is_composite_ = len(pk_attrs) > 1
        entity._pk_ = pk_attrs if len(pk_attrs) > 1 else pk_attrs[0]
        entity._keys_ = [ index.attrs for index in indexes if index.is_unique and not index.is_pk ]
        entity._simple_keys_ = [ key[0] for key in entity._keys_ if len(key) == 1 ]
        entity._composite_keys_ = [ key for key in entity._keys_ if len(key) > 1 ]

        entity._new_attrs_ = new_attrs
        entity._attrs_ = base_attrs + new_attrs
        entity._adict_ = {attr.name: attr for attr in entity._attrs_}
        entity._subclass_attrs_ = []
        entity._subclass_adict_ = {}
        for base in entity._all_bases_:
            for attr in new_attrs:
                if attr.is_collection: continue
                prev = base._subclass_adict_.setdefault(attr.name, attr)
                if prev is not attr: throw(ERDiagramError,
                    'Attribute %s conflicts with attribute %s because both entities inherit from %s. '
                    'To fix this, move attribute definition to base class'
                    % (attr, prev, entity._root_.__name__))
                base._subclass_attrs_.append(attr)
        entity._attrnames_cache_ = {}

        try: table_name = entity.__dict__['_table_']
        except KeyError: entity._table_ = None
        else:
            if not isinstance(table_name, basestring):
                if not isinstance(table_name, (list, tuple)): throw(TypeError,
                    '%s._table_ property must be a string. Got: %r' % (entity.__name__, table_name))
                for name_part in table_name:
                    if not isinstance(name_part, basestring):throw(TypeError,
                        'Each part of table name must be a string. Got: %r' % name_part)
                entity._table_ = table_name = tuple(table_name)

        database.entities[entity.__name__] = entity
        setattr(database, entity.__name__, entity)

        entity._cached_max_id_sql_ = None
        entity._find_sql_cache_ = {}
        entity._load_sql_cache_ = {}
        entity._batchload_sql_cache_ = {}
        entity._insert_sql_cache_ = {}
        entity._update_sql_cache_ = {}
        entity._delete_sql_cache_ = {}

        entity._propagation_mixin_ = None
        entity._set_wrapper_subclass_ = None
        entity._multiset_subclass_ = None

        if '_discriminator_' not in entity.__dict__:
            entity._discriminator_ = None
        if entity._discriminator_ is not None and not entity._discriminator_attr_:
            Discriminator.create_default_attr(entity)
        if entity._discriminator_attr_:
            entity._discriminator_attr_.process_entity_inheritance(entity)

        iter_name = entity._default_iter_name_ = (
            ''.join(letter for letter in entity.__name__ if letter.isupper()).lower()
            or entity.__name__
            )
        for_expr = ast.GenExprFor(ast.AssName(iter_name, 'OP_ASSIGN'), ast.Name('.0'), [])
        inner_expr = ast.GenExprInner(ast.Name(iter_name), [ for_expr ])
        entity._default_genexpr_ = inner_expr

        entity._access_rules_ = defaultdict(set)
    def _initialize_bits_(entity):
        entity._bits_ = {}
        entity._bits_except_volatile_ = {}
        offset_counter = itertools.count()
        all_bits = all_bits_except_volatile = 0
        for attr in entity._attrs_:
            if attr.is_collection or attr.is_discriminator or attr.pk_offset is not None: bit = 0
            elif not attr.columns: bit = 0
            else: bit = 1 << next(offset_counter)
            all_bits |= bit
            entity._bits_[attr] = bit
            if attr.is_volatile: bit = 0
            all_bits_except_volatile |= bit
            entity._bits_except_volatile_[attr] = bit
        entity._all_bits_ = all_bits
        entity._all_bits_except_volatile_ = all_bits_except_volatile
    def _resolve_attr_types_(entity):
        database = entity._database_
        for attr in entity._new_attrs_:
            py_type = attr.py_type
            if isinstance(py_type, basestring):
                rentity = database.entities.get(py_type)
                if rentity is None:
                    throw(ERDiagramError, 'Entity definition %s was not found' % py_type)
                attr.py_type = py_type = rentity
            elif isinstance(py_type, types.FunctionType):
                rentity = py_type()
                if not isinstance(rentity, EntityMeta): throw(TypeError,
                    'Invalid type of attribute %s: expected entity class, got %r' % (attr, rentity))
                attr.py_type = py_type = rentity
            if isinstance(py_type, EntityMeta) and py_type.__name__ == 'Entity': throw(TypeError,
                'Cannot link attribute %s to abstract Entity class. Use specific Entity subclass instead' % attr)
    def _link_reverse_attrs_(entity):
        database = entity._database_
        for attr in entity._new_attrs_:
            py_type = attr.py_type
            if not issubclass(py_type, Entity): continue

            entity2 = py_type
            if entity2._database_ is not database:
                throw(ERDiagramError, 'Interrelated entities must belong to same database. '
                                   'Entities %s and %s belongs to different databases'
                                   % (entity.__name__, entity2.__name__))
            reverse = attr.reverse
            if isinstance(reverse, basestring):
                attr2 = getattr(entity2, reverse, None)
                if attr2 is None: throw(ERDiagramError, 'Reverse attribute %s.%s not found' % (entity2.__name__, reverse))
            elif isinstance(reverse, Attribute):
                attr2 = reverse
                if attr2.entity is not entity2: throw(ERDiagramError, 'Incorrect reverse attribute %s used in %s' % (attr2, attr)) ###
            elif reverse is not None: throw(ERDiagramError, "Value of 'reverse' option must be string. Got: %r" % type(reverse))
            else:
                candidates1 = []
                candidates2 = []
                for attr2 in entity2._new_attrs_:
                    if attr2.py_type not in (entity, entity.__name__): continue
                    reverse2 = attr2.reverse
                    if reverse2 in (attr, attr.name): candidates1.append(attr2)
                    elif not reverse2:
                        if attr2 is attr: continue
                        candidates2.append(attr2)
                msg = "Ambiguous reverse attribute for %s. Use the 'reverse' parameter for pointing to right attribute"
                if len(candidates1) > 1: throw(ERDiagramError, msg % attr)
                elif len(candidates1) == 1: attr2 = candidates1[0]
                elif len(candidates2) > 1: throw(ERDiagramError, msg % attr)
                elif len(candidates2) == 1: attr2 = candidates2[0]
                else: throw(ERDiagramError, 'Reverse attribute for %s not found' % attr)

            type2 = attr2.py_type
            if type2 != entity:
                throw(ERDiagramError, 'Inconsistent reverse attributes %s and %s' % (attr, attr2))
            reverse2 = attr2.reverse
            if reverse2 not in (None, attr, attr.name):
                throw(ERDiagramError, 'Inconsistent reverse attributes %s and %s' % (attr, attr2))

            if attr.is_required and attr2.is_required: throw(ERDiagramError,
                "At least one attribute of one-to-one relationship %s - %s must be optional" % (attr, attr2))

            attr.reverse = attr2
            attr2.reverse = attr
            attr.linked()
            attr2.linked()
    def _get_pk_columns_(entity):
        if entity._pk_columns_ is not None: return entity._pk_columns_
        pk_columns = []
        pk_converters = []
        pk_paths = []
        for attr in entity._pk_attrs_:
            attr_columns = attr.get_columns()
            attr_col_paths = attr.col_paths
            attr.pk_columns_offset = len(pk_columns)
            pk_columns.extend(attr_columns)
            pk_converters.extend(attr.converters)
            pk_paths.extend(attr_col_paths)
        entity._pk_columns_ = pk_columns
        entity._pk_converters_ = pk_converters
        entity._pk_nones_ = (None,) * len(pk_columns)
        entity._pk_paths_ = pk_paths
        return pk_columns
    def __iter__(entity):
        return EntityIter(entity)
    def _normalize_args_(entity, kwargs, setdefault=False):
        avdict = {}
        if setdefault:
            for name in kwargs:
                if name not in entity._adict_: throw(TypeError, 'Unknown attribute %r' % name)
            for attr in entity._attrs_:
                val = kwargs.get(attr.name, DEFAULT)
                avdict[attr] = attr.validate(val, None, entity, from_db=False)
        else:
            get_attr = entity._adict_.get
            for name, val in iteritems(kwargs):
                attr = get_attr(name)
                if attr is None: throw(TypeError, 'Unknown attribute %r' % name)
                avdict[attr] = attr.validate(val, None, entity, from_db=False)
        if entity._pk_is_composite_:
            get_val = avdict.get
            pkval = tuple(get_val(attr) for attr in entity._pk_attrs_)
            if None in pkval: pkval = None
        else: pkval = avdict.get(entity._pk_attrs_[0])
        return pkval, avdict
    @cut_traceback
    def __getitem__(entity, key):
        if type(key) is not tuple: key = (key,)
        if len(key) != len(entity._pk_attrs_):
            throw(TypeError, 'Invalid count of attrs in %s primary key (%s instead of %s)'
                             % (entity.__name__, len(key), len(entity._pk_attrs_)))
        kwargs = {attr.name: value for attr, value in izip(entity._pk_attrs_, key)}
        return entity._find_one_(kwargs)
    @cut_traceback
    def exists(entity, *args, **kwargs):
        if args: return entity._query_from_args_(args, kwargs, frame_depth=3).exists()
        try: obj = entity._find_one_(kwargs)
        except ObjectNotFound: return False
        except MultipleObjectsFoundError: return True
        return True
    @cut_traceback
    def get(entity, *args, **kwargs):
        if args: return entity._query_from_args_(args, kwargs, frame_depth=3).get()
        try: return entity._find_one_(kwargs)  # can throw MultipleObjectsFoundError
        except ObjectNotFound: return None
    @cut_traceback
    def get_for_update(entity, *args, **kwargs):
        nowait = kwargs.pop('nowait', False)
        if args: return entity._query_from_args_(args, kwargs, frame_depth=3).for_update(nowait).get()
        try: return entity._find_one_(kwargs, True, nowait)  # can throw MultipleObjectsFoundError
        except ObjectNotFound: return None
    @cut_traceback
    def get_by_sql(entity, sql, globals=None, locals=None):
        objects = entity._find_by_sql_(1, sql, globals, locals, frame_depth=3)  # can throw MultipleObjectsFoundError
        if not objects: return None
        assert len(objects) == 1
        return objects[0]
    @cut_traceback
    def select(entity, *args):
        return entity._query_from_args_(args, kwargs=None, frame_depth=3)
    @cut_traceback
    def select_by_sql(entity, sql, globals=None, locals=None):
        return entity._find_by_sql_(None, sql, globals, locals, frame_depth=3)
    @cut_traceback
    def select_random(entity, limit):
        if entity._pk_is_composite_: return entity.select().random(limit)
        pk = entity._pk_attrs_[0]
        if not issubclass(pk.py_type, int) or entity._discriminator_ is not None and entity._root_ is not entity:
            return entity.select().random(limit)
        database = entity._database_
        cache = database._get_cache()
        if cache.modified: cache.flush()
        max_id = cache.max_id_cache.get(pk)
        if max_id is None:
            max_id_sql = entity._cached_max_id_sql_
            if max_id_sql is None:
                sql_ast = [ 'SELECT', [ 'AGGREGATES', [ 'MAX', [ 'COLUMN', None, pk.column ] ] ],
                                      [ 'FROM', [ None, 'TABLE', entity._table_ ] ] ]
                max_id_sql, adapter = database._ast2sql(sql_ast)
                entity._cached_max_id_sql_ = max_id_sql
            cursor = database._exec_sql(max_id_sql)
            max_id = cursor.fetchone()[0]
            cache.max_id_cache[pk] = max_id
        if max_id is None: return []
        if max_id <= limit * 2: return entity.select().random(limit)
        cache_index = cache.indexes[entity._pk_attrs_]
        result = []
        tried_ids = set()
        found_in_cache = False
        for i in xrange(5):
            ids = []
            n = (limit - len(result)) * (i+1)
            for j in xrange(n * 2):
                id = randint(1, max_id)
                if id in tried_ids: continue
                if id in ids: continue
                obj = cache_index.get(id)
                if obj is not None:
                    found_in_cache = True
                    tried_ids.add(id)
                    result.append(obj)
                    n -= 1
                else: ids.append(id)
                if len(ids) >= n: break

            if len(result) >= limit: break
            if not ids: continue
            sql, adapter, attr_offsets = entity._construct_batchload_sql_(len(ids), from_seeds=False)
            arguments = adapter([ (id,) for id in ids ])
            cursor = database._exec_sql(sql, arguments)
            objects = entity._fetch_objects(cursor, attr_offsets)
            result.extend(objects)
            tried_ids.update(ids)
            if len(result) >= limit: break

        if len(result) < limit: return entity.select().random(limit)

        result = result[:limit]
        if entity._subclasses_:
            seeds = cache.seeds[entity._pk_attrs_]
            if seeds:
                for obj in result:
                    if obj in seeds: obj._load_()
        if found_in_cache: shuffle(result)
        return result
    def _find_one_(entity, kwargs, for_update=False, nowait=False):
        if entity._database_.schema is None:
            throw(ERDiagramError, 'Mapping is not generated for entity %r' % entity.__name__)
        pkval, avdict = entity._normalize_args_(kwargs, False)
        for attr in avdict:
            if attr.is_collection:
                throw(TypeError, 'Collection attribute %s cannot be specified as search criteria' % attr)
        obj, unique = entity._find_in_cache_(pkval, avdict, for_update)
        if obj is None: obj = entity._find_in_db_(avdict, unique, for_update, nowait)
        if obj is None: throw(ObjectNotFound, entity, pkval)
        return obj
    def _find_in_cache_(entity, pkval, avdict, for_update=False):
        cache = entity._database_._get_cache()
        cache_indexes = cache.indexes
        obj = None
        unique = False
        if pkval is not None:
            unique = True
            obj = cache_indexes[entity._pk_attrs_].get(pkval)
        if obj is None:
            for attr in entity._simple_keys_:
                val = avdict.get(attr)
                if val is not None:
                    unique = True
                    obj = cache_indexes[attr].get(val)
                    if obj is not None: break
        if obj is None:
            for attrs in entity._composite_keys_:
                get_val = avdict.get
                vals = tuple(get_val(attr) for attr in attrs)
                if None in vals: continue
                cache_index = cache_indexes.get(attrs)
                if cache_index is None: continue
                unique = True
                obj = cache_index.get(vals)
                if obj is not None: break
        if obj is None:
            for attr, val in iteritems(avdict):
                if val is None: continue
                reverse = attr.reverse
                if reverse and not reverse.is_collection:
                    obj = reverse.__get__(val)
                    break
        if obj is not None:
            if obj._discriminator_ is not None:
                if obj._subclasses_:
                    cls = obj.__class__
                    if not issubclass(entity, cls) and not issubclass(cls, entity):
                        throw(ObjectNotFound, entity, pkval)
                    seeds = cache.seeds[entity._pk_attrs_]
                    if obj in seeds: obj._load_()
                if not isinstance(obj, entity): throw(ObjectNotFound, entity, pkval)
            if obj._status_ == 'marked_to_delete': throw(ObjectNotFound, entity, pkval)
            for attr, val in iteritems(avdict):
                if val != attr.__get__(obj): throw(ObjectNotFound, entity, pkval)
            if for_update and obj not in cache.for_update:
                return None, unique  # object is found, but it is not locked
            entity._set_rbits((obj,), avdict)
            return obj, unique
        return None, unique
    def _find_in_db_(entity, avdict, unique=False, for_update=False, nowait=False):
        database = entity._database_
        query_attrs = {attr: value is None for attr, value in iteritems(avdict)}
        limit = 2 if not unique else None
        sql, adapter, attr_offsets = entity._construct_sql_(query_attrs, False, limit, for_update, nowait)
        arguments = adapter(avdict)
        if for_update: database._get_cache().immediate = True
        cursor = database._exec_sql(sql, arguments)
        objects = entity._fetch_objects(cursor, attr_offsets, 1, for_update, avdict)
        return objects[0] if objects else None
    def _find_by_sql_(entity, max_fetch_count, sql, globals, locals, frame_depth):
        if not isinstance(sql, basestring): throw(TypeError)
        database = entity._database_
        cursor = database._exec_raw_sql(sql, globals, locals, frame_depth+1)

        col_names = [ column_info[0].upper() for column_info in cursor.description ]
        attr_offsets = {}
        used_columns = set()
        for attr in chain(entity._attrs_with_columns_, entity._subclass_attrs_):
            offsets = []
            for column in attr.columns:
                try: offset = col_names.index(column.upper())
                except ValueError: break
                offsets.append(offset)
                used_columns.add(offset)
            else: attr_offsets[attr] = offsets
        if len(used_columns) < len(col_names):
            for i in xrange(len(col_names)):
                if i not in used_columns: throw(NameError,
                    'Column %s does not belong to entity %s' % (cursor.description[i][0], entity.__name__))
        for attr in entity._pk_attrs_:
            if attr not in attr_offsets: throw(ValueError,
                'Primary key attribue %s was not found in query result set' % attr)

        objects = entity._fetch_objects(cursor, attr_offsets, max_fetch_count)
        return objects
    def _construct_select_clause_(entity, alias=None, distinct=False,
                                  query_attrs=(), attrs_to_prefetch=(), all_attributes=False):
        attr_offsets = {}
        select_list = [ 'DISTINCT' ] if distinct else [ 'ALL' ]
        root = entity._root_
        for attr in chain(root._attrs_, root._subclass_attrs_):
            if not all_attributes and not issubclass(attr.entity, entity) \
                                  and not issubclass(entity, attr.entity): continue
            if attr.is_collection: continue
            if not attr.columns: continue
            if not attr.lazy or attr in query_attrs or attr in attrs_to_prefetch:
                attr_offsets[attr] = offsets = []
                for column in attr.columns:
                    offsets.append(len(select_list) - 1)
                    select_list.append([ 'COLUMN', alias, column ])
        return select_list, attr_offsets
    def _construct_discriminator_criteria_(entity, alias=None):
        discr_attr = entity._discriminator_attr_
        if discr_attr is None: return None
        code2cls = discr_attr.code2cls
        discr_values = [ [ 'VALUE', cls._discriminator_ ] for cls in entity._subclasses_ ]
        discr_values.append([ 'VALUE', entity._discriminator_])
        return [ 'IN', [ 'COLUMN', alias, discr_attr.column ], discr_values ]
    def _construct_batchload_sql_(entity, batch_size, attr=None, from_seeds=True):
        query_key = batch_size, attr, from_seeds
        cached_sql = entity._batchload_sql_cache_.get(query_key)
        if cached_sql is not None: return cached_sql
        select_list, attr_offsets = entity._construct_select_clause_(all_attributes=True)
        from_list = [ 'FROM', [ None, 'TABLE', entity._table_ ]]
        if attr is None:
            columns = entity._pk_columns_
            converters = entity._pk_converters_
        else:
            columns = attr.columns
            converters = attr.converters
        row_value_syntax = entity._database_.provider.translator_cls.row_value_syntax
        criteria_list = construct_batchload_criteria_list(
            None, columns, converters, batch_size, row_value_syntax, from_seeds=from_seeds)
        sql_ast = [ 'SELECT', select_list, from_list, [ 'WHERE' ] + criteria_list ]
        database = entity._database_
        sql, adapter = database._ast2sql(sql_ast)
        cached_sql = sql, adapter, attr_offsets
        entity._batchload_sql_cache_[query_key] = cached_sql
        return cached_sql
    def _construct_sql_(entity, query_attrs, order_by_pk=False, limit=None, for_update=False, nowait=False):
        if nowait: assert for_update
        sorted_query_attrs = tuple(sorted(query_attrs.items()))
        query_key = sorted_query_attrs, order_by_pk, limit, for_update, nowait
        cached_sql = entity._find_sql_cache_.get(query_key)
        if cached_sql is not None: return cached_sql
        select_list, attr_offsets = entity._construct_select_clause_(query_attrs=query_attrs)
        from_list = [ 'FROM', [ None, 'TABLE', entity._table_ ]]
        where_list = [ 'WHERE' ]

        discr_attr = entity._discriminator_attr_
        if discr_attr and query_attrs.get(discr_attr) != False:
            discr_criteria = entity._construct_discriminator_criteria_()
            if discr_criteria: where_list.append(discr_criteria)

        for attr, attr_is_none in sorted_query_attrs:
            if not attr.reverse:
                if attr_is_none: where_list.append([ 'IS_NULL', [ 'COLUMN', None, attr.column ] ])
                else:
                    if len(attr.converters) > 1: throw(NotImplementedError)
                    converter = attr.converters[0]
                    where_list.append([ converter.EQ, [ 'COLUMN', None, attr.column ], [ 'PARAM', (attr, None, None), converter ] ])
            elif not attr.columns: throw(NotImplementedError)
            else:
                attr_entity = attr.py_type; assert attr_entity == attr.reverse.entity
                if attr_is_none:
                    for column in attr.columns:
                        where_list.append([ 'IS_NULL', [ 'COLUMN', None, column ] ])
                else:
                    for j, (column, converter) in enumerate(izip(attr.columns, attr_entity._pk_converters_)):
                        where_list.append([ converter.EQ, [ 'COLUMN', None, column ], [ 'PARAM', (attr, None, j), converter ] ])

        if not for_update: sql_ast = [ 'SELECT', select_list, from_list, where_list ]
        else: sql_ast = [ 'SELECT_FOR_UPDATE', bool(nowait), select_list, from_list, where_list ]
        if order_by_pk: sql_ast.append([ 'ORDER_BY' ] + [ [ 'COLUMN', None, column ] for column in entity._pk_columns_ ])
        if limit is not None: sql_ast.append([ 'LIMIT', [ 'VALUE', limit ] ])
        database = entity._database_
        sql, adapter = database._ast2sql(sql_ast)
        cached_sql = sql, adapter, attr_offsets
        entity._find_sql_cache_[query_key] = cached_sql
        return cached_sql
    def _fetch_objects(entity, cursor, attr_offsets, max_fetch_count=None, for_update=False, used_attrs=()):
        if max_fetch_count is None: max_fetch_count = options.MAX_FETCH_COUNT
        if max_fetch_count is not None:
            rows = cursor.fetchmany(max_fetch_count + 1)
            if len(rows) == max_fetch_count + 1:
                if max_fetch_count == 1: throw(MultipleObjectsFoundError,
                    'Multiple objects were found. Use %s.select(...) to retrieve them' % entity.__name__)
                throw(TooManyObjectsFoundError,
                    'Found more then pony.options.MAX_FETCH_COUNT=%d objects' % options.MAX_FETCH_COUNT)
        else: rows = cursor.fetchall()
        objects = []
        if attr_offsets is None:
            objects = [ entity._get_by_raw_pkval_(row, for_update) for row in rows ]
            entity._load_many_(objects)
        else:
            for row in rows:
                real_entity_subclass, pkval, avdict = entity._parse_row_(row, attr_offsets)
                obj = real_entity_subclass._get_from_identity_map_(pkval, 'loaded', for_update)
                if obj._status_ in del_statuses: continue
                obj._db_set_(avdict)
                objects.append(obj)
        if used_attrs: entity._set_rbits(objects, used_attrs)
        return objects
    def _set_rbits(entity, objects, attrs):
        rbits_dict = {}
        get_rbits = rbits_dict.get
        for obj in objects:
            wbits = obj._wbits_
            if wbits is None: continue
            rbits = get_rbits(obj.__class__)
            if rbits is None:
                rbits = sum(obj._bits_except_volatile_.get(attr, 0) for attr in attrs)
                rbits_dict[obj.__class__] = rbits
            obj._rbits_ |= rbits & ~wbits
    def _parse_row_(entity, row, attr_offsets):
        discr_attr = entity._discriminator_attr_
        if not discr_attr: real_entity_subclass = entity
        else:
            discr_offset = attr_offsets[discr_attr][0]
            discr_value = discr_attr.validate(row[discr_offset], None, entity, from_db=True)
            real_entity_subclass = discr_attr.code2cls[discr_value]

        avdict = {}
        for attr in real_entity_subclass._attrs_:
            offsets = attr_offsets.get(attr)
            if offsets is None or attr.is_discriminator: continue
            avdict[attr] = attr.parse_value(row, offsets)
        if not entity._pk_is_composite_: pkval = avdict.pop(entity._pk_attrs_[0], None)
        else: pkval = tuple(avdict.pop(attr, None) for attr in entity._pk_attrs_)
        return real_entity_subclass, pkval, avdict
    def _load_many_(entity, objects):
        database = entity._database_
        cache = database._get_cache()
        seeds = cache.seeds[entity._pk_attrs_]
        if not seeds: return
        objects = {obj for obj in objects if obj in seeds}
        objects = sorted(objects, key=attrgetter('_pkval_'))
        max_batch_size = database.provider.max_params_count // len(entity._pk_columns_)
        while objects:
            batch = objects[:max_batch_size]
            objects = objects[max_batch_size:]
            sql, adapter, attr_offsets = entity._construct_batchload_sql_(len(batch))
            arguments = adapter(batch)
            cursor = database._exec_sql(sql, arguments)
            result = entity._fetch_objects(cursor, attr_offsets)
            if len(result) < len(batch):
                for obj in result:
                    if obj not in batch: throw(UnrepeatableReadError,
                                               'Phantom object %s disappeared' % safe_repr(obj))
    def _select_all(entity):
        return Query(entity._default_iter_name_, entity._default_genexpr_, {}, { '.0' : entity })
    def _query_from_args_(entity, args, kwargs, frame_depth):
        if not args and not kwargs: return entity._select_all()
        func, globals, locals = get_globals_and_locals(args, kwargs, frame_depth+1)

        if type(func) is types.FunctionType:
            names = get_lambda_args(func)
            code_key = id(func.func_code if PY2 else func.__code__)
            cond_expr, external_names, cells = decompile(func)
        elif isinstance(func, basestring):
            code_key = func
            lambda_ast = string2ast(func)
            if not isinstance(lambda_ast, ast.Lambda):
                throw(TypeError, 'Lambda function is expected. Got: %s' % func)
            names = get_lambda_args(lambda_ast)
            cond_expr = lambda_ast.code
            cells = None
        else: assert False  # pragma: no cover

        if len(names) != 1: throw(TypeError,
            'Lambda query requires exactly one parameter name, like %s.select(lambda %s: ...). '
            'Got: %d parameters' % (entity.__name__, entity.__name__[0].lower(), len(names)))
        name = names[0]

        if_expr = ast.GenExprIf(cond_expr)
        for_expr = ast.GenExprFor(ast.AssName(name, 'OP_ASSIGN'), ast.Name('.0'), [ if_expr ])
        inner_expr = ast.GenExprInner(ast.Name(name), [ for_expr ])
        locals = locals.copy() if locals is not None else {}
        assert '.0' not in locals
        locals['.0'] = entity
        return Query(code_key, inner_expr, globals, locals, cells)
    def _get_from_identity_map_(entity, pkval, status, for_update=False, undo_funcs=None, obj_to_init=None):
        cache = entity._database_._get_cache()
        pk_attrs = entity._pk_attrs_
        cache_index = cache.indexes[pk_attrs]
        if pkval is None: obj = None
        else: obj = cache_index.get(pkval)

        if obj is None: pass
        elif status == 'created':
            if entity._pk_is_composite_: pkval = ', '.join(str(item) for item in pkval)
            throw(CacheIndexError, 'Cannot create %s: instance with primary key %s already exists'
                             % (obj.__class__.__name__, pkval))
        elif obj.__class__ is entity: pass
        elif issubclass(obj.__class__, entity): pass
        elif not issubclass(entity, obj.__class__): throw(TransactionError,
            'Unexpected class change from %s to %s for object with primary key %r' %
            (obj.__class__, entity, obj._pkval_))
        elif obj._rbits_ or obj._wbits_: throw(NotImplementedError)
        else: obj.__class__ = entity

        if obj is None:
            with cache.flush_disabled():
                obj = obj_to_init or object.__new__(entity)
                cache.objects.add(obj)
                obj._pkval_ = pkval
                obj._status_ = status
                obj._vals_ = {}
                obj._dbvals_ = {}
                obj._save_pos_ = None
                obj._session_cache_ = cache
                if pkval is not None:
                    cache_index[pkval] = obj
                    obj._newid_ = None
                else: obj._newid_ = next(new_instance_id_counter)
                if obj._pk_is_composite_: pairs = izip(pk_attrs, pkval)
                else: pairs = ((pk_attrs[0], pkval),)
                if status == 'loaded':
                    assert undo_funcs is None
                    obj._rbits_ = obj._wbits_ = 0
                    for attr, val in pairs:
                        obj._vals_[attr] = val
                        if attr.reverse: attr.db_update_reverse(obj, NOT_LOADED, val)
                    cache.seeds[pk_attrs].add(obj)
                elif status == 'created':
                    assert undo_funcs is not None
                    obj._rbits_ = obj._wbits_ = None
                    for attr, val in pairs:
                        obj._vals_[attr] = val
                        if attr.reverse: attr.update_reverse(obj, NOT_LOADED, val, undo_funcs)
                    cache.for_update.add(obj)
                else: assert False  # pragma: no cover
        if for_update:
            assert cache.in_transaction
            cache.for_update.add(obj)
        return obj
    def _get_by_raw_pkval_(entity, raw_pkval, for_update=False, from_db=True):
        i = 0
        pkval = []
        for attr in entity._pk_attrs_:
            if attr.column is not None:
                val = raw_pkval[i]
                i += 1
                if not attr.reverse: val = attr.validate(val, None, entity, from_db=from_db)
                else: val = attr.py_type._get_by_raw_pkval_((val,), from_db=from_db)
            else:
                if not attr.reverse: throw(NotImplementedError)
                vals = raw_pkval[i:i+len(attr.columns)]
                val = attr.py_type._get_by_raw_pkval_(vals, from_db=from_db)
                i += len(attr.columns)
            pkval.append(val)
        if not entity._pk_is_composite_: pkval = pkval[0]
        else: pkval = tuple(pkval)
        obj = entity._get_from_identity_map_(pkval, 'loaded', for_update)
        assert obj._status_ != 'cancelled'
        return obj
    def _get_propagation_mixin_(entity):
        mixin = entity._propagation_mixin_
        if mixin is not None: return mixin
        cls_dict = { '_entity_' : entity }
        for attr in entity._attrs_:
            if not attr.reverse:
                def fget(wrapper, attr=attr):
                    attrnames = wrapper._attrnames_ + (attr.name,)
                    items = [ x for x in (attr.__get__(item) for item in wrapper) if x is not None ]
                    return Multiset(wrapper._obj_, attrnames, items)
            elif not attr.is_collection:
                def fget(wrapper, attr=attr):
                    attrnames = wrapper._attrnames_ + (attr.name,)
                    items = [ x for x in (attr.__get__(item) for item in wrapper) if x is not None ]
                    rentity = attr.py_type
                    cls = rentity._get_multiset_subclass_()
                    return cls(wrapper._obj_, attrnames, items)
            else:
                def fget(wrapper, attr=attr):
                    cache = attr.entity._database_._get_cache()
                    cache.collection_statistics.setdefault(attr, attr.nplus1_threshold)
                    attrnames = wrapper._attrnames_ + (attr.name,)
                    items = [ subitem for item in wrapper
                                      for subitem in attr.__get__(item) ]
                    rentity = attr.py_type
                    cls = rentity._get_multiset_subclass_()
                    return cls(wrapper._obj_, attrnames, items)
            cls_dict[attr.name] = property(fget)
        result_cls_name = entity.__name__ + 'SetMixin'
        result_cls = type(result_cls_name, (object,), cls_dict)
        entity._propagation_mixin_ = result_cls
        return result_cls
    def _get_multiset_subclass_(entity):
        result_cls = entity._multiset_subclass_
        if result_cls is None:
            mixin = entity._get_propagation_mixin_()
            cls_name = entity.__name__ + 'Multiset'
            result_cls = type(cls_name, (Multiset, mixin), {})
            entity._multiset_subclass_ = result_cls
        return result_cls
    def _get_set_wrapper_subclass_(entity):
        result_cls = entity._set_wrapper_subclass_
        if result_cls is None:
            mixin = entity._get_propagation_mixin_()
            cls_name = entity.__name__ + 'Set'
            result_cls = type(cls_name, (SetInstance, mixin), {})
            entity._set_wrapper_subclass_ = result_cls
        return result_cls
    @cut_traceback
    def describe(entity):
        result = []
        parents = ','.join(cls.__name__ for cls in entity.__bases__)
        result.append('class %s(%s):' % (entity.__name__, parents))
        if entity._base_attrs_:
            result.append('# inherited attrs')
            result.extend(attr.describe() for attr in entity._base_attrs_)
            result.append('# attrs introduced in %s' % entity.__name__)
        result.extend(attr.describe() for attr in entity._new_attrs_)
        if entity._pk_is_composite_:
            result.append('PrimaryKey(%s)' % ', '.join(attr.name for attr in entity._pk_attrs_))
        return '\n    '.join(result)
    @cut_traceback
    @db_session(ddl=True)
    def drop_table(entity, with_all_data=False):
        entity._database_._drop_tables([ entity._table_ ], True, with_all_data)
    def _get_attrs_(entity, only=None, exclude=None, with_collections=False, with_lazy=False):
        if only and not isinstance(only, basestring): only = tuple(only)
        if exclude and not isinstance(exclude, basestring): exclude = tuple(exclude)
        key = (only, exclude, with_collections, with_lazy)
        attrs = entity._attrnames_cache_.get(key)
        if not attrs:
            attrs = []
            append = attrs.append
            if only:
                if isinstance(only, basestring): only = only.replace(',', ' ').split()
                get_attr = entity._adict_.get
                for attrname in only:
                    attr = get_attr(attrname)
                    if attr is None: throw(AttributeError,
                        'Entity %s does not have attriute %s' % (entity.__name__, attrname))
                    else: append(attr)
            else:
                for attr in entity._attrs_:
                    if attr.is_collection:
                        if with_collections: append(attr)
                    elif attr.lazy:
                        if with_lazy: append(attr)
                    else: append(attr)
            if exclude:
                if isinstance(exclude, basestring): exclude = exclude.replace(',', ' ').split()
                for attrname in exclude:
                    if attrname not in entity._adict_: throw(AttributeError,
                        'Entity %s does not have attriute %s' % (entity.__name__, attrname))
                attrs = (attr for attr in attrs if attr.name not in exclude)
            attrs = tuple(attrs)
            entity._attrnames_cache_[key] = attrs
        return attrs

def populate_criteria_list(criteria_list, columns, converters, operations,
                           params_count=0, table_alias=None, optimistic=False):
    for column, op, converter in izip(columns, operations, converters):
        if op == 'IS_NULL':
            criteria_list.append([ op, [ 'COLUMN', None, column ] ])
        else:
            criteria_list.append([ op, [ 'COLUMN', table_alias, column ],
                                       [ 'PARAM', (params_count, None, None), converter, optimistic ] ])
        params_count += 1
    return params_count

statuses = {'created', 'cancelled', 'loaded', 'modified', 'inserted', 'updated', 'marked_to_delete', 'deleted'}
del_statuses = {'marked_to_delete', 'deleted', 'cancelled'}
created_or_deleted_statuses = {'created'} | del_statuses
saved_statuses = {'inserted', 'updated', 'deleted'}

def throw_object_was_deleted(obj):
    assert obj._status_ in del_statuses
    throw(OperationWithDeletedObjectError, '%s was %s'
          % (safe_repr(obj), obj._status_.replace('_', ' ')))

def unpickle_entity(d):
    entity = d.pop('__class__')
    cache = entity._database_._get_cache()
    if not entity._pk_is_composite_: pkval = d.get(entity._pk_attrs_[0].name)
    else: pkval = tuple(d[attr.name] for attr in entity._pk_attrs_)
    assert pkval is not None
    obj = entity._get_from_identity_map_(pkval, 'loaded')
    if obj._status_ in del_statuses: return obj
    avdict = {}
    for attrname, val in iteritems(d):
        attr = entity._adict_[attrname]
        if attr.pk_offset is not None: continue
        avdict[attr] = val
    obj._db_set_(avdict, unpickling=True)
    return obj

def safe_repr(obj):
    return Entity.__repr__(obj)

class Entity(with_metaclass(EntityMeta)):
    __slots__ = '_session_cache_', '_status_', '_pkval_', '_newid_', '_dbvals_', '_vals_', '_rbits_', '_wbits_', '_save_pos_', '__weakref__'
    def __reduce__(obj):
        if obj._status_ in del_statuses: throw(
            OperationWithDeletedObjectError, 'Deleted object %s cannot be pickled' % safe_repr(obj))
        if obj._status_ in ('created', 'modified'): throw(
            OrmError, '%s object %s has to be stored in DB before it can be pickled'
                      % (obj._status_.capitalize(), safe_repr(obj)))
        d = {'__class__' : obj.__class__}
        adict = obj._adict_
        for attr, val in iteritems(obj._vals_):
            if not attr.is_collection: d[attr.name] = val
        return unpickle_entity, (d,)
    @cut_traceback
    def __init__(obj, *args, **kwargs):
        entity = obj.__class__
        if args: raise TypeError('%s constructor accept only keyword arguments. Got: %d positional argument%s'
                                 % (entity.__name__, len(args), len(args) > 1 and 's' or ''))
        if entity._database_.schema is None:
            throw(ERDiagramError, 'Mapping is not generated for entity %r' % entity.__name__)

        pkval, avdict = entity._normalize_args_(kwargs, True)
        undo_funcs = []
        cache = entity._database_._get_cache()
        cache_indexes = cache.indexes
        indexes_update = {}
        with cache.flush_disabled():
            for attr in entity._simple_keys_:
                val = avdict[attr]
                if val is None: continue
                if val in cache_indexes[attr]: throw(CacheIndexError,
                    'Cannot create %s: value %r for key %s already exists' % (entity.__name__, val, attr.name))
                indexes_update[attr] = val
            for attrs in entity._composite_keys_:
                vals = tuple(avdict[attr] for attr in attrs)
                if None in vals: continue
                if vals in cache_indexes[attrs]:
                    attr_names = ', '.join(attr.name for attr in attrs)
                    throw(CacheIndexError, 'Cannot create %s: value %s for composite key (%s) already exists'
                                     % (entity.__name__, vals, attr_names))
                indexes_update[attrs] = vals
            try:
                entity._get_from_identity_map_(pkval, 'created', undo_funcs=undo_funcs, obj_to_init=obj)
                for attr, val in iteritems(avdict):
                    if attr.pk_offset is not None: continue
                    elif not attr.is_collection:
                        obj._vals_[attr] = val
                        if attr.reverse: attr.update_reverse(obj, None, val, undo_funcs)
                    else: attr.__set__(obj, val, undo_funcs)
            except:
                for undo_func in reversed(undo_funcs): undo_func()
                raise
        if pkval is not None: cache_indexes[entity._pk_attrs_][pkval] = obj
        for key, vals in iteritems(indexes_update): cache_indexes[key][vals] = obj
        objects_to_save = cache.objects_to_save
        obj._save_pos_ = len(objects_to_save)
        objects_to_save.append(obj)
        cache.modified = True
    def get_pk(obj):
        pkval = obj._get_raw_pkval_()
        if len(pkval) == 1: return pkval[0]
        return pkval
    def _get_raw_pkval_(obj):
        pkval = obj._pkval_
        if not obj._pk_is_composite_:
            if not obj._pk_attrs_[0].reverse: return (pkval,)
            else: return pkval._get_raw_pkval_()
        raw_pkval = []
        append, extend = raw_pkval.append, raw_pkval.extend
        for attr, val in izip(obj._pk_attrs_, pkval):
            if not attr.reverse: append(val)
            else: extend(val._get_raw_pkval_())
        return tuple(raw_pkval)
    @cut_traceback
    def __lt__(entity, other):
        return entity._cmp_(other) < 0
    @cut_traceback
    def __le__(entity, other):
        return entity._cmp_(other) <= 0
    @cut_traceback
    def __gt__(entity, other):
        return entity._cmp_(other) > 0
    @cut_traceback
    def __ge__(entity, other):
        return entity._cmp_(other) >= 0
    def _cmp_(entity, other):
        if entity is other: return 0
        if isinstance(other, Entity):
            pkval = entity._pkval_
            other_pkval = other._pkval_
            if pkval is not None:
                if other_pkval is None: return -1
                result = cmp(pkval, other_pkval)
            else:
                if other_pkval is not None: return 1
                result = cmp(entity._newid_, other._newid_)
            if result: return result
        return cmp(id(entity), id(other))
    @cut_traceback
    def __repr__(obj):
        pkval = obj._pkval_
        if pkval is None: return '%s[new:%d]' % (obj.__class__.__name__, obj._newid_)
        if obj._pk_is_composite_: pkval = ','.join(imap(repr, pkval))
        else: pkval = repr(pkval)
        return '%s[%s]' % (obj.__class__.__name__, pkval)
    def _load_(obj):
        cache = obj._session_cache_
        if not cache.is_alive: throw(DatabaseSessionIsOver,
            'Cannot load object %s: the database session is over' % safe_repr(obj))
        entity = obj.__class__
        database = entity._database_
        if cache is not database._get_cache():
            throw(TransactionError, "Object %s doesn't belong to current transaction" % safe_repr(obj))
        seeds = cache.seeds[entity._pk_attrs_]
        max_batch_size = database.provider.max_params_count // len(entity._pk_columns_)
        objects = [ obj ]
        if options.PREFETCHING:
            for seed in seeds:
                if len(objects) >= max_batch_size: break
                if seed is not obj: objects.append(seed)
        sql, adapter, attr_offsets = entity._construct_batchload_sql_(len(objects))
        arguments = adapter(objects)
        cursor = database._exec_sql(sql, arguments)
        objects = entity._fetch_objects(cursor, attr_offsets)
        if obj not in objects: throw(UnrepeatableReadError,
                                     'Phantom object %s disappeared' % safe_repr(obj))
    @cut_traceback
    def load(obj, *attrs):
        cache = obj._session_cache_
        if not cache.is_alive: throw(DatabaseSessionIsOver,
            'Cannot load object %s: the database session is over' % safe_repr(obj))
        entity = obj.__class__
        database = entity._database_
        if cache is not database._get_cache():
            throw(TransactionError, "Object %s doesn't belong to current transaction" % safe_repr(obj))
        if obj._status_ in created_or_deleted_statuses: return
        if not attrs:
            attrs = tuple(attr for attr, bit in iteritems(entity._bits_)
                          if bit and attr not in obj._vals_)
        else:
            args = attrs
            attrs = set()
            for arg in args:
                if isinstance(arg, basestring):
                    attr = entity._adict_.get(arg)
                    if attr is None:
                        if not is_ident(arg): throw(ValueError, 'Invalid attribute name: %r' % arg)
                        throw(AttributeError, 'Object %s does not have attribute %r' % (obj, arg))
                elif isinstance(arg, Attribute):
                    attr = arg
                    if not isinstance(obj, attr.entity): throw(AttributeError,
                        'Attribute %s does not belong to object %s' % (attr, obj))
                else: throw(TypeError, 'Invalid argument type: %r' % arg)
                if attr.is_collection: throw(NotImplementedError,
                    'The load() method does not support collection attributes yet. Got: %s' % attr.name)
                if entity._bits_[attr] and attr not in obj._vals_: attrs.add(attr)
            attrs = tuple(sorted(attrs, key=attrgetter('id')))

        sql_cache = entity._root_._load_sql_cache_
        cached_sql = sql_cache.get(attrs)
        if cached_sql is None:
            if entity._discriminator_attr_ is not None:
                attrs = (entity._discriminator_attr_,) + attrs
            attrs = entity._pk_attrs_ + attrs

            attr_offsets = {}
            select_list = [ 'ALL' ]
            for attr in attrs:
                attr_offsets[attr] = offsets = []
                for column in attr.columns:
                    offsets.append(len(select_list) - 1)
                    select_list.append([ 'COLUMN', None, column ])
            from_list = [ 'FROM', [ None, 'TABLE', entity._table_ ]]
            criteria_list = [ [ converter.EQ, [ 'COLUMN', None, column ], [ 'PARAM', (i, None, None), converter ] ]
                              for i, (column, converter) in enumerate(izip(obj._pk_columns_, obj._pk_converters_)) ]
            where_list = [ 'WHERE' ] + criteria_list

            sql_ast = [ 'SELECT', select_list, from_list, where_list ]
            sql, adapter = database._ast2sql(sql_ast)
            cached_sql = sql, adapter, attr_offsets
            sql_cache[attrs] = cached_sql
        else: sql, adapter, attr_offsets = cached_sql
        arguments = adapter(obj._get_raw_pkval_())

        cursor = database._exec_sql(sql, arguments)
        objects = entity._fetch_objects(cursor, attr_offsets)
        if obj not in objects: throw(UnrepeatableReadError,
                                     'Phantom object %s disappeared' % safe_repr(obj))
    def _attr_changed_(obj, attr):
        cache = obj._session_cache_
        if not cache.is_alive: throw(
            DatabaseSessionIsOver,
            'Cannot assign new value to attribute %s.%s: the database session'
            ' is over' % (safe_repr(obj), attr.name))
        if obj._status_ in del_statuses:
            throw_object_was_deleted(obj)
        status = obj._status_
        wbits = obj._wbits_
        bit = obj._bits_[attr]
        objects_to_save = cache.objects_to_save
        if wbits is not None and bit:
            obj._wbits_ |= bit
            if status != 'modified':
                assert status in ('loaded', 'inserted', 'updated')
                assert obj._save_pos_ is None
                obj._status_ = 'modified'
                obj._save_pos_ = len(objects_to_save)
                objects_to_save.append(obj)
                cache.modified = True
    def _db_set_(obj, avdict, unpickling=False):
        assert obj._status_ not in created_or_deleted_statuses
        cache = obj._session_cache_
        assert cache.is_alive
        cache.seeds[obj._pk_attrs_].discard(obj)
        if not avdict: return

        get_val = obj._vals_.get
        get_dbval = obj._dbvals_.get
        rbits = obj._rbits_
        wbits = obj._wbits_
        for attr, new_dbval in items_list(avdict):
            assert attr.pk_offset is None
            assert new_dbval is not NOT_LOADED
            old_dbval = get_dbval(attr, NOT_LOADED)
            if old_dbval is not NOT_LOADED:
                if unpickling or old_dbval == new_dbval or (
                        not attr.reverse and attr.converters[0].dbvals_equal(old_dbval, new_dbval)):
                    del avdict[attr]
                    continue

            bit = obj._bits_except_volatile_[attr]
            if rbits & bit: throw(UnrepeatableReadError,
                'Value of %s.%s for %s was updated outside of current transaction (was: %r, now: %r)'
                % (obj.__class__.__name__, attr.name, obj, old_dbval, new_dbval))

            if attr.reverse: attr.db_update_reverse(obj, old_dbval, new_dbval)
            obj._dbvals_[attr] = new_dbval
            if wbits & bit: del avdict[attr]
            if attr.is_unique:
                old_val = get_val(attr)
                if old_val != new_dbval:
                    cache.db_update_simple_index(obj, attr, old_val, new_dbval)

        for attrs in obj._composite_keys_:
            for attr in attrs:
                if attr in avdict: break
            else: continue
            vals = [ get_val(a) for a in attrs ]  # In Python 2 var name leaks into the function scope!
            prev_vals = tuple(vals)
            for i, attr in enumerate(attrs):
                if attr in avdict: vals[i] = avdict[attr]
            new_vals = tuple(vals)
            cache.db_update_composite_index(obj, attrs, prev_vals, new_vals)

        for attr, new_val in iteritems(avdict):
            converter = attr.converters[0]
            new_val = converter.dbval2val(new_val, obj)
            obj._vals_[attr] = new_val
    def _delete_(obj, undo_funcs=None):
        status = obj._status_
        if status in del_statuses: return
        is_recursive_call = undo_funcs is not None
        if not is_recursive_call: undo_funcs = []
        cache = obj._session_cache_
        with cache.flush_disabled():
            get_val = obj._vals_.get
            undo_list = []
            objects_to_save = cache.objects_to_save
            save_pos = obj._save_pos_

            def undo_func():
                if obj._status_ == 'marked_to_delete':
                    assert objects_to_save
                    obj2 = objects_to_save.pop()
                    assert obj2 is obj
                    if save_pos is not None:
                        assert objects_to_save[save_pos] is None
                        objects_to_save[save_pos] = obj
                    obj._save_pos_ = save_pos
                obj._status_ = status
                for cache_index, old_key in undo_list: cache_index[old_key] = obj

            undo_funcs.append(undo_func)
            try:
                for attr in obj._attrs_:
                    reverse = attr.reverse
                    if not reverse: continue
                    if not attr.is_collection:
                        if not reverse.is_collection:
                            val = get_val(attr) if attr in obj._vals_ else attr.load(obj)
                            if val is None: continue
                            if attr.cascade_delete: val._delete_(undo_funcs)
                            elif not reverse.is_required: reverse.__set__(val, None, undo_funcs)
                            else: throw(ConstraintError, "Cannot delete object %s, because it has associated %s, "
                                                         "and 'cascade_delete' option of %s is not set"
                                                         % (obj, attr.name, attr))
                        elif isinstance(reverse, Set):
                            if attr not in obj._vals_: continue
                            val = get_val(attr)
                            if val is None: continue
                            reverse.reverse_remove((val,), obj, undo_funcs)
                        else: throw(NotImplementedError)
                    elif isinstance(attr, Set):
                        set_wrapper = attr.__get__(obj)
                        if not set_wrapper.__nonzero__(): pass
                        elif attr.cascade_delete:
                            for robj in set_wrapper: robj._delete_(undo_funcs)
                        elif not reverse.is_required: attr.__set__(obj, (), undo_funcs)
                        else: throw(ConstraintError, "Cannot delete object %s, because it has non-empty set of %s, "
                                                     "and 'cascade_delete' option of %s is not set"
                                                     % (obj, attr.name, attr))
                    else: throw(NotImplementedError)

                cache_indexes = cache.indexes
                for attr in obj._simple_keys_:
                    val = get_val(attr)
                    if val is None: continue
                    cache_index = cache_indexes[attr]
                    obj2 = cache_index.pop(val)
                    assert obj2 is obj
                    undo_list.append((cache_index, val))

                for attrs in obj._composite_keys_:
                    vals = tuple(get_val(attr) for attr in attrs)
                    if None in vals: continue
                    cache_index = cache_indexes[attrs]
                    obj2 = cache_index.pop(vals)
                    assert obj2 is obj
                    undo_list.append((cache_index, vals))

                if status == 'created':
                    assert save_pos is not None
                    objects_to_save[save_pos] = None
                    obj._save_pos_ = None
                    obj._status_ = 'cancelled'
                    if obj._pkval_ is not None:
                        pk_index = cache_indexes[obj._pk_attrs_]
                        obj2 = pk_index.pop(obj._pkval_)
                        assert obj2 is obj
                        undo_list.append((pk_index, obj._pkval_))
                else:
                    if status == 'modified':
                        assert save_pos is not None
                        objects_to_save[save_pos] = None
                    else:
                        assert status in ('loaded', 'inserted', 'updated')
                        assert save_pos is None
                    obj._save_pos_ = len(objects_to_save)
                    objects_to_save.append(obj)
                    obj._status_ = 'marked_to_delete'
                    cache.modified = True
            except:
                if not is_recursive_call:
                    for undo_func in reversed(undo_funcs): undo_func()
                raise
    @cut_traceback
    def delete(obj):
        if not obj._session_cache_.is_alive: throw(DatabaseSessionIsOver,
            'Cannot delete object %s: the database session is over' % safe_repr(obj))
        obj._delete_()
    @cut_traceback
    def set(obj, **kwargs):
        cache = obj._session_cache_
        if not cache.is_alive: throw(DatabaseSessionIsOver,
            'Cannot change object %s: the database session is over' % safe_repr(obj))
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        with cache.flush_disabled():
            avdict, collection_avdict = obj._keyargs_to_avdicts_(kwargs)
            status = obj._status_
            wbits = obj._wbits_
            get_val = obj._vals_.get
            objects_to_save = cache.objects_to_save
            if avdict:
                for attr in avdict:
                    if attr not in obj._vals_ and attr.reverse and not attr.reverse.is_collection:
                        attr.load(obj)  # loading of one-to-one relations
                if wbits is not None:
                    new_wbits = wbits
                    for attr in avdict: new_wbits |= obj._bits_[attr]
                    obj._wbits_ = new_wbits
                    if status != 'modified':
                        assert status in ('loaded', 'inserted', 'updated')
                        assert obj._save_pos_ is None
                        obj._status_ = 'modified'
                        obj._save_pos_ = len(objects_to_save)
                        objects_to_save.append(obj)
                        cache.modified = True
                if not collection_avdict:
                    for attr in avdict:
                        if attr.reverse or attr.is_part_of_unique_index: break
                    else:
                        obj._vals_.update(avdict)
                        return
            undo_funcs = []
            undo = []
            def undo_func():
                obj._status_ = status
                obj._wbits_ = wbits
                if status in ('loaded', 'inserted', 'updated'):
                    assert objects_to_save
                    obj2 = objects_to_save.pop()
                    assert obj2 is obj and obj._save_pos_ == len(objects_to_save)
                    obj._save_pos_ = None
                for cache_index, old_key, new_key in undo:
                    if new_key is not None: del cache_index[new_key]
                    if old_key is not None: cache_index[old_key] = obj
            try:
                for attr in obj._simple_keys_:
                    if attr not in avdict: continue
                    new_val = avdict[attr]
                    old_val = get_val(attr)
                    if old_val != new_val: cache.update_simple_index(obj, attr, old_val, new_val, undo)
                for attrs in obj._composite_keys_:
                    for attr in attrs:
                        if attr in avdict: break
                    else: continue
                    vals = [ get_val(a) for a in attrs ]  # In Python 2 var name leaks into the function scope!
                    prev_vals = tuple(vals)
                    for i, attr in enumerate(attrs):
                        if attr in avdict: vals[i] = avdict[attr]
                    new_vals = tuple(vals)
                    cache.update_composite_index(obj, attrs, prev_vals, new_vals, undo)
                for attr, new_val in iteritems(avdict):
                    if not attr.reverse: continue
                    old_val = get_val(attr)
                    attr.update_reverse(obj, old_val, new_val, undo_funcs)
                for attr, new_val in iteritems(collection_avdict):
                    attr.__set__(obj, new_val, undo_funcs)
            except:
                for undo_func in undo_funcs: undo_func()
                raise
        obj._vals_.update(avdict)
    def _keyargs_to_avdicts_(obj, kwargs):
        avdict, collection_avdict = {}, {}
        get_attr = obj._adict_.get
        for name, new_val in kwargs.items():
            attr = get_attr(name)
            if attr is None: throw(TypeError, 'Unknown attribute %r' % name)
            new_val = attr.validate(new_val, obj, from_db=False)
            if attr.is_collection: collection_avdict[attr] = new_val
            elif attr.pk_offset is None: avdict[attr] = new_val
            elif obj._vals_.get(attr, new_val) != new_val:
                throw(TypeError, 'Cannot change value of primary key attribute %s' % attr.name)
        return avdict, collection_avdict
    @classmethod
    def _attrs_with_bit_(entity, attrs, mask=-1):
        get_bit = entity._bits_.get
        for attr in attrs:
            if get_bit(attr) & mask: yield attr
    def _construct_optimistic_criteria_(obj):
        optimistic_columns = []
        optimistic_converters = []
        optimistic_values = []
        optimistic_operations = []
        for attr in obj._attrs_with_bit_(obj._attrs_with_columns_, obj._rbits_):
            converters = attr.converters
            assert converters
            if not (attr.optimistic and converters[0].optimistic): continue
            dbval = obj._dbvals_[attr]
            optimistic_columns.extend(attr.columns)
            optimistic_converters.extend(attr.converters)
            values = attr.get_raw_values(dbval)
            optimistic_values.extend(values)
            if dbval is None:
                optimistic_operations.append('IS_NULL')
            else:
                optimistic_operations.extend(converter.EQ for converter in converters)
        return optimistic_operations, optimistic_columns, optimistic_converters, optimistic_values
    def _save_principal_objects_(obj, dependent_objects):
        if dependent_objects is None: dependent_objects = []
        elif obj in dependent_objects:
            chain = ' -> '.join(obj2.__class__.__name__ for obj2 in dependent_objects)
            throw(UnresolvableCyclicDependency, 'Cannot save cyclic chain: ' + chain)
        dependent_objects.append(obj)
        status = obj._status_
        if status == 'created': attrs = obj._attrs_with_columns_
        elif status == 'modified': attrs = obj._attrs_with_bit_(obj._attrs_with_columns_, obj._wbits_)
        else: assert False  # pragma: no cover
        for attr in attrs:
            if not attr.reverse: continue
            val = obj._vals_[attr]
            if val is not None and val._status_ == 'created':
                val._save_(dependent_objects)
    def _update_dbvals_(obj, after_create):
        bits = obj._bits_
        vals = obj._vals_
        dbvals = obj._dbvals_
        cache_indexes = obj._session_cache_.indexes
        for attr in obj._attrs_with_columns_:
            if not bits.get(attr): continue
            if attr not in vals: continue
            val = vals[attr]
            if attr.is_volatile:
                if val is not None:
                    if attr.is_unique: cache_indexes[attr].pop(val, None)
                    get_val = vals.get
                    for key, i in attr.composite_keys:
                        keyval = tuple(get_val(attr) for attr in key)
                        cache_indexes[key].pop(keyval, None)
            elif after_create and val is None:
                obj._rbits_ &= ~bits[attr]
            else:
                # For normal attribute, set `dbval` to the same value as `val` after update/create
                # dbvals[attr] = val
                converter = attr.converters[0]
                dbvals[attr] = converter.val2dbval(val, obj)  # TODO this conversion should be unnecessary
                continue
            # Clear value of volatile attribute or null values after create, because the value may be changed in the DB
            del vals[attr]
            dbvals.pop(attr, None)

    def _save_created_(obj):
        auto_pk = (obj._pkval_ is None)
        attrs = []
        values = []
        for attr in obj._attrs_with_columns_:
            if auto_pk and attr.is_pk: continue
            val = obj._vals_[attr]
            if val is not None:
                attrs.append(attr)
                values.extend(attr.get_raw_values(val))
        attrs = tuple(attrs)

        database = obj._database_
        cached_sql = obj._insert_sql_cache_.get(attrs)
        if cached_sql is None:
            columns = []
            converters = []
            for attr in attrs:
                columns.extend(attr.columns)
                converters.extend(attr.converters)
            assert len(columns) == len(converters)
            params = [ [ 'PARAM', (i, None, None),  converter ] for i, converter in enumerate(converters) ]
            entity = obj.__class__
            if not columns and database.provider.dialect == 'Oracle':
                sql_ast = [ 'INSERT', entity._table_, obj._pk_columns_,
                            [ [ 'DEFAULT' ] for column in obj._pk_columns_ ] ]
            else: sql_ast = [ 'INSERT', entity._table_, columns, params ]
            if auto_pk: sql_ast.append(entity._pk_columns_[0])
            sql, adapter = database._ast2sql(sql_ast)
            entity._insert_sql_cache_[attrs] = sql, adapter
        else: sql, adapter = cached_sql

        arguments = adapter(values)
        try:
            if auto_pk: new_id = database._exec_sql(sql, arguments, returning_id=True,
                                                    start_transaction=True)
            else: database._exec_sql(sql, arguments, start_transaction=True)
        except IntegrityError as e:
            msg = " ".join(tostring(arg) for arg in e.args)
            throw(TransactionIntegrityError,
                  'Object %r cannot be stored in the database. %s: %s'
                  % (obj, e.__class__.__name__, msg), e)
        except DatabaseError as e:
            msg = " ".join(tostring(arg) for arg in e.args)
            throw(UnexpectedError, 'Object %r cannot be stored in the database. %s: %s'
                                   % (obj, e.__class__.__name__, msg), e)

        if auto_pk:
            pk_attrs = obj._pk_attrs_
            cache_index = obj._session_cache_.indexes[pk_attrs]
            obj2 = cache_index.setdefault(new_id, obj)
            if obj2 is not obj: throw(TransactionIntegrityError,
                'Newly auto-generated id value %s was already used in transaction cache for another object' % new_id)
            obj._pkval_ = obj._vals_[pk_attrs[0]] = new_id
            obj._newid_ = None

        obj._status_ = 'inserted'
        obj._rbits_ = obj._all_bits_except_volatile_
        obj._wbits_ = 0
        obj._update_dbvals_(True)
    def _save_updated_(obj):
        update_columns = []
        values = []
        for attr in obj._attrs_with_bit_(obj._attrs_with_columns_, obj._wbits_):
            update_columns.extend(attr.columns)
            val = obj._vals_[attr]
            values.extend(attr.get_raw_values(val))
        if update_columns:
            for attr in obj._pk_attrs_:
                val = obj._vals_[attr]
                values.extend(attr.get_raw_values(val))
            cache = obj._session_cache_
            if obj not in cache.for_update:
                optimistic_ops, optimistic_columns, optimistic_converters, optimistic_values = \
                    obj._construct_optimistic_criteria_()
                values.extend(optimistic_values)
            else: optimistic_columns = optimistic_converters = optimistic_ops = ()
            query_key = tuple(update_columns), tuple(optimistic_columns), tuple(optimistic_ops)
            database = obj._database_
            cached_sql = obj._update_sql_cache_.get(query_key)
            if cached_sql is None:
                update_converters = []
                for attr in obj._attrs_with_bit_(obj._attrs_with_columns_, obj._wbits_):
                    update_converters.extend(attr.converters)
                assert len(update_columns) == len(update_converters)
                update_params = [ [ 'PARAM', (i, None, None), converter ] for i, converter in enumerate(update_converters) ]
                params_count = len(update_params)
                where_list = [ 'WHERE' ]
                pk_columns = obj._pk_columns_
                pk_converters = obj._pk_converters_
                params_count = populate_criteria_list(where_list, pk_columns, pk_converters, repeat('EQ'), params_count)
                if optimistic_columns: populate_criteria_list(
                    where_list, optimistic_columns, optimistic_converters, optimistic_ops, params_count, optimistic=True)
                sql_ast = [ 'UPDATE', obj._table_, list(izip(update_columns, update_params)), where_list ]
                sql, adapter = database._ast2sql(sql_ast)
                obj._update_sql_cache_[query_key] = sql, adapter
            else: sql, adapter = cached_sql
            arguments = adapter(values)
            cursor = database._exec_sql(sql, arguments, start_transaction=True)
            if cursor.rowcount != 1:
                throw(OptimisticCheckError, 'Object %s was updated outside of current transaction' % safe_repr(obj))
        obj._status_ = 'updated'
        obj._rbits_ |= obj._wbits_ & obj._all_bits_except_volatile_
        obj._wbits_ = 0
        obj._update_dbvals_(False)
    def _save_deleted_(obj):
        values = []
        values.extend(obj._get_raw_pkval_())
        cache = obj._session_cache_
        if obj not in cache.for_update:
            optimistic_ops, optimistic_columns, optimistic_converters, optimistic_values = \
                obj._construct_optimistic_criteria_()
            values.extend(optimistic_values)
        else: optimistic_columns = optimistic_converters = optimistic_ops = ()
        query_key = tuple(optimistic_columns), tuple(optimistic_ops)
        database = obj._database_
        cached_sql = obj._delete_sql_cache_.get(query_key)
        if cached_sql is None:
            where_list = [ 'WHERE' ]
            params_count = populate_criteria_list(where_list, obj._pk_columns_, obj._pk_converters_, repeat('EQ'))
            if optimistic_columns: populate_criteria_list(
                where_list, optimistic_columns, optimistic_converters, optimistic_ops, params_count, optimistic=True)
            from_ast = [ 'FROM', [ None, 'TABLE', obj._table_ ] ]
            sql_ast = [ 'DELETE', None, from_ast, where_list ]
            sql, adapter = database._ast2sql(sql_ast)
            obj.__class__._delete_sql_cache_[query_key] = sql, adapter
        else: sql, adapter = cached_sql
        arguments = adapter(values)
        database._exec_sql(sql, arguments, start_transaction=True)
        obj._status_ = 'deleted'
        cache.indexes[obj._pk_attrs_].pop(obj._pkval_)
    def _save_(obj, dependent_objects=None):
        cache = obj._session_cache_
        assert cache.is_alive
        status = obj._status_

        if status in ('created', 'modified'):
            obj._save_principal_objects_(dependent_objects)

        if status == 'created': obj._save_created_()
        elif status == 'modified': obj._save_updated_()
        elif status == 'marked_to_delete': obj._save_deleted_()
        else: assert False, "_save_() called for object %r with incorrect status %s" % (obj, status)  # pragma: no cover

        assert obj._status_ in saved_statuses
        cache = obj._session_cache_
        cache.saved_objects.append((obj, obj._status_))
        objects_to_save = cache.objects_to_save
        save_pos = obj._save_pos_
        if save_pos == len(objects_to_save) - 1:
            objects_to_save.pop()
        else:
            objects_to_save[save_pos] = None
        obj._save_pos_ = None
    def flush(obj):
        if obj._status_ not in ('created', 'modified', 'marked_to_delete'):
            return

        assert obj._save_pos_ is not None, 'save_pos is None for %s object' % obj._status_
        cache = obj._session_cache_
        assert not cache.saved_objects
        with cache.flush_disabled():
            obj._before_save_() # should be inside flush_disabled to prevent infinite recursion
                                # TODO: add to documentation that flush is disabled inside before_xxx hooks
            obj._save_()
        cache.call_after_save_hooks()
    def _before_save_(obj):
        status = obj._status_
        if status == 'created': obj.before_insert()
        elif status == 'modified': obj.before_update()
        elif status == 'marked_to_delete': obj.before_delete()
    def before_insert(obj):
        pass
    def before_update(obj):
        pass
    def before_delete(obj):
        pass
    def _after_save_(obj, status):
        if status == 'inserted': obj.after_insert()
        elif status == 'updated': obj.after_update()
        elif status == 'deleted': obj.after_delete()
    def after_insert(obj):
        pass
    def after_update(obj):
        pass
    def after_delete(obj):
        pass
    @cut_traceback
    def to_dict(obj, only=None, exclude=None, with_collections=False, with_lazy=False, related_objects=False):
        if obj._session_cache_.modified: obj._session_cache_.flush()
        attrs = obj.__class__._get_attrs_(only, exclude, with_collections, with_lazy)
        result = {}
        for attr in attrs:
            value = attr.__get__(obj)
            if attr.is_collection:
                if related_objects: value = sorted(value)
                elif len(attr.reverse.entity._pk_columns_) > 1:
                    value = sorted(item._get_raw_pkval_() for item in value)
                else: value = sorted(item._get_raw_pkval_()[0] for item in value)
            elif attr.is_relation and not related_objects and value is not None:
                value = value._get_raw_pkval_()
                if len(value) == 1: value = value[0]
            result[attr.name] = value
        return result
    def to_json(obj, include=(), exclude=(), converter=None, with_schema=True, schema_hash=None):
        return obj._database_.to_json(obj, include, exclude, converter, with_schema, schema_hash)

def string2ast(s):
    result = string2ast_cache.get(s)
    if result is not None: return result
    if PY2:
        if isinstance(s, str):
            try: s.encode('ascii')
            except UnicodeDecodeError: throw(TypeError,
                'The bytestring %r contains non-ascii symbols. Try to pass unicode string instead' % s)
        else: s = s.encode('ascii', 'backslashreplace')
    module_node = parse('(%s)' % s)
    if not isinstance(module_node, ast.Module): throw(TypeError)
    stmt_node = module_node.node
    if not isinstance(stmt_node, ast.Stmt) or len(stmt_node.nodes) != 1: throw(TypeError)
    discard_node = stmt_node.nodes[0]
    if not isinstance(discard_node, ast.Discard): throw(TypeError)
    result = string2ast_cache[s] = discard_node.expr
    # result = deepcopy(result)  # no need for now, but may be needed later
    return result

def get_globals_and_locals(args, kwargs, frame_depth, from_generator=False):
    args_len = len(args)
    assert args_len > 0
    func = args[0]
    if from_generator:
        if not isinstance(func, (basestring, types.GeneratorType)): throw(TypeError,
            'The first positional argument must be generator expression or its text source. Got: %r' % func)
    else:
        if not isinstance(func, (basestring, types.FunctionType)): throw(TypeError,
            'The first positional argument must be lambda function or its text source. Got: %r' % func)
    if args_len > 1:
        globals = args[1]
        if not hasattr(globals, 'keys'): throw(TypeError,
            'The second positional arguments should be globals dictionary. Got: %r' % globals)
        if args_len > 2:
            locals = args[2]
            if local is not None and not hasattr(locals, 'keys'): throw(TypeError,
                'The third positional arguments should be locals dictionary. Got: %r' % locals)
        else: locals = {}
        if type(func) is types.GeneratorType:
            locals = locals.copy()
            locals.update(func.gi_frame.f_locals)
        if len(args) > 3: throw(TypeError, 'Excess positional argument%s: %s'
                                % (len(args) > 4 and 's' or '', ', '.join(imap(repr, args[3:]))))
    else:
        locals = {}
        locals.update(sys._getframe(frame_depth+1).f_locals)
        if type(func) is types.GeneratorType:
            globals = func.gi_frame.f_globals
            locals.update(func.gi_frame.f_locals)
        else:
            globals = sys._getframe(frame_depth+1).f_globals
    if kwargs: throw(TypeError, 'Keyword arguments cannot be specified together with positional arguments')
    return func, globals, locals

def make_query(args, frame_depth, left_join=False):
    gen, globals, locals = get_globals_and_locals(
        args, kwargs=None, frame_depth=frame_depth+1, from_generator=True)
    if isinstance(gen, types.GeneratorType):
        tree, external_names, cells = decompile(gen)
        code_key = id(gen.gi_frame.f_code)
    elif isinstance(gen, basestring):
        tree = string2ast(gen)
        if not isinstance(tree, ast.GenExpr): throw(TypeError,
            'Source code should represent generator. Got: %s' % gen)
        code_key = gen
        cells = None
    else: assert False
    return Query(code_key, tree.code, globals, locals, cells, left_join)

@cut_traceback
def select(*args):
    return make_query(args, frame_depth=3)

@cut_traceback
def left_join(*args):
    return make_query(args, frame_depth=3, left_join=True)

@cut_traceback
def get(*args):
    return make_query(args, frame_depth=3).get()

@cut_traceback
def exists(*args):
    return make_query(args, frame_depth=3).exists()

@cut_traceback
def delete(*args):
    return make_query(args, frame_depth=3).delete()

def make_aggrfunc(std_func):
    def aggrfunc(*args, **kwargs):
        if kwargs: return std_func(*args, **kwargs)
        if len(args) != 1: return std_func(*args)
        arg = args[0]
        if type(arg) is types.GeneratorType:
            try: iterator = arg.gi_frame.f_locals['.0']
            except: return std_func(*args)
            if isinstance(iterator, EntityIter):
                return getattr(select(arg), std_func.__name__)()
        return std_func(*args)
    aggrfunc.__name__ = std_func.__name__
    return aggrfunc

count = make_aggrfunc(utils.count)
sum = make_aggrfunc(builtins.sum)
min = make_aggrfunc(builtins.min)
max = make_aggrfunc(builtins.max)
avg = make_aggrfunc(utils.avg)

distinct = make_aggrfunc(utils.distinct)

def JOIN(expr):
    return expr

def desc(expr):
    if isinstance(expr, Attribute):
        return expr.desc
    if isinstance(expr, int_types) and expr > 0:
        return -expr
    if isinstance(expr, basestring):
        return 'desc(%s)' % expr
    return expr

def raw_sql(sql, result_type=None):
    globals = sys._getframe(1).f_globals
    locals = sys._getframe(1).f_locals
    return RawSQL(sql, globals, locals, result_type)

def extract_vars(extractors, globals, locals, cells=None):
    if cells:
        locals = locals.copy()
        for name, cell in cells.items():
            locals[name] = cell.cell_contents
    vars = {}
    vartypes = {}
    for key, code in iteritems(extractors):
        filter_num, src = key
        if src == '.0': value = locals['.0']
        else:
            try: value = eval(code, globals, locals)
            except Exception as cause: raise ExprEvalError(src, cause)
            if src == 'None' and value is not None: throw(TranslationError)
            if src == 'True' and value is not True: throw(TranslationError)
            if src == 'False' and value is not False: throw(TranslationError)
        try: vartypes[key] = get_normalized_type_of(value)
        except TypeError:
            if not isinstance(value, dict):
                unsupported = False
                try: value = tuple(value)
                except: unsupported = True
            else: unsupported = True
            if unsupported:
                typename = type(value).__name__
                if src == '.0': throw(TypeError, 'Cannot iterate over non-entity object')
                throw(TypeError, 'Expression `%s` has unsupported type %r' % (src, typename))
            vartypes[key] = get_normalized_type_of(value)
        vars[key] = value
    return vars, vartypes

def unpickle_query(query_result):
    return query_result

def persistent_id(obj):
    if obj is Ellipsis:
        return "Ellipsis"

def persistent_load(persid):
    if persid == "Ellipsis":
        return Ellipsis
    raise pickle.UnpicklingError("unsupported persistent object")

def pickle_ast(val):
    pickled = io.BytesIO()
    pickler = pickle.Pickler(pickled)
    pickler.persistent_id = persistent_id
    pickler.dump(val)
    return pickled

def unpickle_ast(pickled):
    pickled.seek(0)
    unpickler = pickle.Unpickler(pickled)
    unpickler.persistent_load = persistent_load
    return unpickler.load()


class Query(object):
    def __init__(query, code_key, tree, globals, locals, cells=None, left_join=False):
        assert isinstance(tree, ast.GenExprInner)
        extractors, varnames, tree, pretranslator_key = create_extractors(
            code_key, tree, 0, globals, locals, special_functions, const_functions)
        vars, vartypes = extract_vars(extractors, globals, locals, cells)

        node = tree.quals[0].iter
        origin = vars[0, node.src]
        if isinstance(origin, EntityIter): origin = origin.entity
        elif not isinstance(origin, EntityMeta):
            if node.src == '.0': throw(TypeError, 'Cannot iterate over non-entity object')
            throw(TypeError, 'Cannot iterate over non-entity object %s' % node.src)
        query._origin = origin
        database = origin._database_
        if database is None: throw(TranslationError, 'Entity %s is not mapped to a database' % origin.__name__)
        if database.schema is None: throw(ERDiagramError, 'Mapping is not generated for entity %r' % origin.__name__)
        database.provider.normalize_vars(vars, vartypes)
        query._vars = vars
        query._key = pretranslator_key, tuple(vartypes[name] for name in varnames), left_join
        query._database = database

        translator = database._translator_cache.get(query._key)
        if translator is None:
            pickled_tree = pickle_ast(tree)
            tree = unpickle_ast(pickled_tree)  # tree = deepcopy(tree)
            translator_cls = database.provider.translator_cls
            translator = translator_cls(tree, extractors, vartypes, left_join=left_join)
            name_path = translator.can_be_optimized()
            if name_path:
                tree = unpickle_ast(pickled_tree)  # tree = deepcopy(tree)
                try: translator = translator_cls(tree, extractors, vartypes, left_join=True, optimize=name_path)
                except OptimizationFailed: translator.optimization_failed = True
            translator.pickled_tree = pickled_tree
            database._translator_cache[query._key] = translator
        query._translator = translator
        query._filters = ()
        query._next_kwarg_id = 0
        query._for_update = query._nowait = False
        query._distinct = None
        query._prefetch = False
        query._entities_to_prefetch = set()
        query._attrs_to_prefetch_dict = defaultdict(set)
    def _clone(query, **kwargs):
        new_query = object.__new__(Query)
        new_query.__dict__.update(query.__dict__)
        new_query.__dict__.update(kwargs)
        return new_query
    def __reduce__(query):
        return unpickle_query, (query._fetch(),)
    def _construct_sql_and_arguments(query, range=None, aggr_func_name=None):
        translator = query._translator
        expr_type = translator.expr_type
        if isinstance(expr_type, EntityMeta) and query._attrs_to_prefetch_dict:
            attrs_to_prefetch = tuple(sorted(query._attrs_to_prefetch_dict.get(expr_type, ())))
        else:
            attrs_to_prefetch = ()
        sql_key = query._key + (range, query._distinct, aggr_func_name, query._for_update, query._nowait,
                                options.INNER_JOIN_SYNTAX, attrs_to_prefetch)
        database = query._database
        cache_entry = database._constructed_sql_cache.get(sql_key)
        if cache_entry is None:
            sql_ast, attr_offsets = translator.construct_sql_ast(
                range, query._distinct, aggr_func_name, query._for_update, query._nowait, attrs_to_prefetch)
            cache = database._get_cache()
            sql, adapter = database.provider.ast2sql(sql_ast)
            cache_entry = sql, adapter, attr_offsets
            database._constructed_sql_cache[sql_key] = cache_entry
        else: sql, adapter, attr_offsets = cache_entry
        arguments = adapter(query._vars)
        if query._translator.query_result_is_cacheable:
            arguments_type = type(arguments)
            if arguments_type is tuple: arguments_key = arguments
            elif arguments_type is dict: arguments_key = tuple(sorted(iteritems(arguments)))
            try: hash(arguments_key)
            except: query_key = None  # arguments are unhashable
            else: query_key = sql_key + (arguments_key)
        else: query_key = None
        return sql, arguments, attr_offsets, query_key
    def get_sql(query):
        sql, arguments, attr_offsets, query_key = query._construct_sql_and_arguments()
        return sql
    def _fetch(query, range=None):
        translator = query._translator
        sql, arguments, attr_offsets, query_key = query._construct_sql_and_arguments(range)
        database = query._database
        cache = database._get_cache()
        if query._for_update: cache.immediate = True
        cache.prepare_connection_for_query_execution()  # may clear cache.query_results
        try: result = cache.query_results[query_key]
        except KeyError:
            cursor = database._exec_sql(sql, arguments)
            if isinstance(translator.expr_type, EntityMeta):
                entity = translator.expr_type
                result = entity._fetch_objects(cursor, attr_offsets, for_update=query._for_update,
                                               used_attrs=translator.get_used_attrs())
            elif len(translator.row_layout) == 1:
                func, slice_or_offset, src = translator.row_layout[0]
                result = list(starmap(func, cursor.fetchall()))
            else:
                result = [ tuple(func(sql_row[slice_or_offset])
                                 for func, slice_or_offset, src in translator.row_layout)
                           for sql_row in cursor.fetchall() ]
                for i, t in enumerate(translator.expr_type):
                    if isinstance(t, EntityMeta) and t._subclasses_: t._load_many_(row[i] for row in result)
            if query_key is not None: cache.query_results[query_key] = result
        else:
            stats = database._dblocal.stats
            stat = stats.get(sql)
            if stat is not None: stat.cache_count += 1
            else: stats[sql] = QueryStat(sql)

        if query._prefetch: query._do_prefetch(result)
        return QueryResult(result, query, translator.expr_type, translator.col_names)
    @cut_traceback
    def prefetch(query, *args):
        query = query._clone(_entities_to_prefetch=query._entities_to_prefetch.copy(),
                             _attrs_to_prefetch_dict=query._attrs_to_prefetch_dict.copy())
        query._prefetch = True
        for arg in args:
            if isinstance(arg, EntityMeta):
                entity = arg
                if query._database is not entity._database_: throw(TypeError,
                    'Entity %s belongs to different database and cannot be prefetched' % entity.__name__)
                query._entities_to_prefetch.add(entity)
            elif isinstance(arg, Attribute):
                attr = arg
                entity = attr.entity
                if query._database is not entity._database_: throw(TypeError,
                    'Entity of attribute %s belongs to different database and cannot be prefetched' % attr)
                if isinstance(attr.py_type, EntityMeta) or attr.lazy:
                    query._attrs_to_prefetch_dict[entity].add(attr)
            else: throw(TypeError, 'Argument of prefetch() query method must be entity class or attribute. '
                                   'Got: %r' % arg)
        return query
    def _do_prefetch(query, result):
        expr_type = query._translator.expr_type
        object_list = []
        object_set = set()
        append_to_object_list = object_list.append
        add_to_object_set = object_set.add

        if isinstance(expr_type, EntityMeta):
            for obj in result:
                if obj not in object_set:
                    add_to_object_set(obj)
                    append_to_object_list(obj)
        elif type(expr_type) is tuple:
            for i, t in enumerate(expr_type):
                if not isinstance(t, EntityMeta): continue
                for row in result:
                    obj = row[i]
                    if obj not in object_set:
                        add_to_object_set(obj)
                        append_to_object_list(obj)

        cache = query._database._get_cache()
        entities_to_prefetch = query._entities_to_prefetch
        attrs_to_prefetch_dict = query._attrs_to_prefetch_dict
        prefetching_attrs_cache = {}
        for obj in object_list:
            entity = obj.__class__
            if obj in cache.seeds[entity._pk_attrs_]: obj._load_()

            all_attrs_to_prefetch = prefetching_attrs_cache.get(entity)
            if all_attrs_to_prefetch is None:
                all_attrs_to_prefetch = []
                append = all_attrs_to_prefetch.append
                attrs_to_prefetch = attrs_to_prefetch_dict[entity]
                for attr in obj._attrs_:
                    if attr.is_collection:
                        if attr in attrs_to_prefetch: append(attr)
                    elif attr.is_relation:
                        if attr in attrs_to_prefetch or attr.py_type in entities_to_prefetch: append(attr)
                    elif attr.lazy:
                        if attr in attrs_to_prefetch: append(attr)
                prefetching_attrs_cache[entity] = all_attrs_to_prefetch

            for attr in all_attrs_to_prefetch:
                if attr.is_collection:
                    if not isinstance(attr, Set): throw(NotImplementedError)
                    setdata = obj._vals_.get(attr)
                    if setdata is None or not setdata.is_fully_loaded: setdata = attr.load(obj)
                    for obj2 in setdata:
                        if obj2 not in object_set:
                            add_to_object_set(obj2)
                            append_to_object_list(obj2)
                elif attr.is_relation:
                    obj2 = attr.get(obj)
                    if obj2 is not None and obj2 not in object_set:
                        add_to_object_set(obj2)
                        append_to_object_list(obj2)
                elif attr.lazy: attr.get(obj)
                else: assert False  # pragma: no cover
    @cut_traceback
    def show(query, width=None):
        query._fetch().show(width)
    @cut_traceback
    def get(query):
        objects = query[:2]
        if not objects: return None
        if len(objects) > 1: throw(MultipleObjectsFoundError,
            'Multiple objects were found. Use select(...) to retrieve them')
        return objects[0]
    @cut_traceback
    def first(query):
        translator = query._translator
        if translator.order: pass
        elif type(translator.expr_type) is tuple:
            query = query.order_by(*[i+1 for i in xrange(len(query._translator.expr_type))])
        else:
            query = query.order_by(1)
        objects = query.without_distinct()[:1]
        if not objects: return None
        return objects[0]
    @cut_traceback
    def without_distinct(query):
        return query._clone(_distinct=False)
    @cut_traceback
    def distinct(query):
        return query._clone(_distinct=True)
    @cut_traceback
    def exists(query):
        objects = query[:1]
        return bool(objects)
    @cut_traceback
    def delete(query, bulk=None):
        if not bulk:
            if not isinstance(query._translator.expr_type, EntityMeta): throw(TypeError,
                'Delete query should be applied to a single entity. Got: %s'
                % ast2src(query._translator.tree.expr))
            objects = query._fetch()
            for obj in objects: obj._delete_()
            return len(objects)
        translator = query._translator
        sql_key = query._key + ('DELETE',)
        database = query._database
        cache = database._get_cache()
        cache_entry = database._constructed_sql_cache.get(sql_key)
        if cache_entry is None:
            sql_ast = translator.construct_delete_sql_ast()
            cache_entry = database.provider.ast2sql(sql_ast)
            database._constructed_sql_cache[sql_key] = cache_entry
        sql, adapter = cache_entry
        arguments = adapter(query._vars)
        cache.immediate = True
        cache.prepare_connection_for_query_execution()  # may clear cache.query_results
        cursor = database._exec_sql(sql, arguments)
        return cursor.rowcount
    @cut_traceback
    def __len__(query):
        return len(query._fetch())
    @cut_traceback
    def __iter__(query):
        return iter(query._fetch())
    @cut_traceback
    def order_by(query, *args):
        if not args: throw(TypeError, 'order_by() method requires at least one argument')
        if args[0] is None:
            if len(args) > 1: throw(TypeError, 'When first argument of order_by() method is None, it must be the only argument')
            tup = ((),)
            new_key = query._key + tup
            new_filters = query._filters + tup
            new_translator = query._database._translator_cache.get(new_key)
            if new_translator is None:
                new_translator = query._translator.without_order()
                query._database._translator_cache[new_key] = new_translator
            return query._clone(_key=new_key, _filters=new_filters, _translator=new_translator)

        if isinstance(args[0], (basestring, types.FunctionType)):
            func, globals, locals = get_globals_and_locals(args, kwargs=None, frame_depth=3)
            return query._process_lambda(func, globals, locals, order_by=True)

        if isinstance(args[0], RawSQL):
            raw = args[0]
            return query.order_by(lambda: raw)

        attributes = numbers = False
        for arg in args:
            if isinstance(arg, int_types): numbers = True
            elif isinstance(arg, (Attribute, DescWrapper)): attributes = True
            else: throw(TypeError, "order_by() method receive an argument of invalid type: %r" % arg)
        if numbers and attributes:
            throw(TypeError, 'order_by() method receive invalid combination of arguments')
        new_key = query._key + ('order_by', args,)
        new_filters = query._filters + ((numbers, args),)
        new_translator = query._database._translator_cache.get(new_key)
        if new_translator is None:
            if numbers: new_translator = query._translator.order_by_numbers(args)
            else: new_translator = query._translator.order_by_attributes(args)
            query._database._translator_cache[new_key] = new_translator
        return query._clone(_key=new_key, _filters=new_filters, _translator=new_translator)
    def _process_lambda(query, func, globals, locals, order_by):
        prev_translator = query._translator
        argnames = ()
        if isinstance(func, basestring):
            func_id = func
            func_ast = string2ast(func)
            if isinstance(func_ast, ast.Lambda):
                argnames = get_lambda_args(func_ast)
                func_ast = func_ast.code
            cells = None
        elif type(func) is types.FunctionType:
            argnames = get_lambda_args(func)
            subquery = prev_translator.subquery
            func_id = id(func.func_code if PY2 else func.__code__)
            func_ast, external_names, cells = decompile(func)
        elif not order_by: throw(TypeError,
            'Argument of filter() method must be a lambda functon or its text. Got: %r' % func)
        else: assert False  # pragma: no cover

        if argnames:
            expr_type = prev_translator.expr_type
            expr_count = len(expr_type) if type(expr_type) is tuple else 1
            if len(argnames) != expr_count:
                throw(TypeError, 'Incorrect number of lambda arguments. '
                                 'Expected: %d, got: %d' % (expr_count, len(argnames)))

        filter_num = len(query._filters) + 1
        extractors, varnames, func_ast, pretranslator_key = create_extractors(
            func_id, func_ast, filter_num, globals, locals, special_functions, const_functions,
            argnames or prev_translator.subquery)
        if extractors:
            vars, vartypes = extract_vars(extractors, globals, locals, cells)
            query._database.provider.normalize_vars(vars, vartypes)
            new_query_vars = query._vars.copy()
            new_query_vars.update(vars)
            sorted_vartypes = tuple(vartypes[name] for name in varnames)
        else: new_query_vars, vartypes, sorted_vartypes = query._vars, {}, ()

        new_key = query._key + (('order_by' if order_by else 'filter', pretranslator_key, sorted_vartypes),)
        new_filters = query._filters + ((order_by, func_ast, argnames, extractors, vartypes),)
        new_translator = query._database._translator_cache.get(new_key)
        if new_translator is None:
            prev_optimized = prev_translator.optimize
            new_translator = prev_translator.apply_lambda(filter_num, order_by, func_ast, argnames, extractors, vartypes)
            if not prev_optimized:
                name_path = new_translator.can_be_optimized()
                if name_path:
                    tree = unpickle_ast(prev_translator.pickled_tree)  # tree = deepcopy(tree)
                    prev_extractors = prev_translator.extractors
                    prev_vartypes = prev_translator.vartypes
                    translator_cls = prev_translator.__class__
                    new_translator = translator_cls(tree, prev_extractors, prev_vartypes,
                                                    left_join=True, optimize=name_path)
                    new_translator = query._reapply_filters(new_translator)
                    new_translator = new_translator.apply_lambda(filter_num, order_by, func_ast, argnames, extractors, vartypes)
            query._database._translator_cache[new_key] = new_translator
        return query._clone(_vars=new_query_vars, _key=new_key, _filters=new_filters, _translator=new_translator)
    def _reapply_filters(query, translator):
        for i, tup in enumerate(query._filters):
            if not tup:
                translator = translator.without_order()
            elif len(tup) == 1:
                attrnames = tup[0]
                translator.apply_kwfilters(attrnames)
            elif len(tup) == 2:
                numbers, args = tup
                if numbers: translator = translator.order_by_numbers(args)
                else: translator = translator.order_by_attributes(args)
            else:
                order_by, func_ast, argnames, extractors, vartypes = tup
                translator = translator.apply_lambda(i+1, order_by, func_ast, argnames, extractors, vartypes)
        return translator
    @cut_traceback
    def filter(query, *args, **kwargs):
        if args:
            if isinstance(args[0], RawSQL):
                raw = args[0]
                return query.filter(lambda: raw)
            func, globals, locals = get_globals_and_locals(args, kwargs, frame_depth=3)
            return query._process_lambda(func, globals, locals, order_by=False)
        if not kwargs: return query

        entity = query._translator.expr_type
        if not isinstance(entity, EntityMeta): throw(TypeError,
            'Keyword arguments are not allowed: since query result type is not an entity, filter() method can accept only lambda')

        get_attr = entity._adict_.get
        filterattrs = []
        value_dict = {}
        next_id = query._next_kwarg_id
        for attrname, val in sorted(iteritems(kwargs)):
            attr = get_attr(attrname)
            if attr is None: throw(AttributeError,
                'Entity %s does not have attribute %s' % (entity.__name__, attrname))
            if attr.is_collection: throw(TypeError,
                '%s attribute %s cannot be used as a keyword argument for filtering'
                % (attr.__class__.__name__, attr))
            val = attr.validate(val, None, entity, from_db=False)
            id = next_id
            next_id += 1
            filterattrs.append((attr, id, val is None))
            value_dict[id] = val

        filterattrs = tuple(filterattrs)
        new_key = query._key + ('filter', filterattrs)
        new_filters = query._filters + ((filterattrs,),)
        new_translator = query._database._translator_cache.get(new_key)
        if new_translator is None:
            new_translator = query._translator.apply_kwfilters(filterattrs)
            query._database._translator_cache[new_key] = new_translator
        new_query = query._clone(_key=new_key, _filters=new_filters, _translator=new_translator,
                                 _next_kwarg_id=next_id, _vars=query._vars.copy())
        new_query._vars.update(value_dict)
        return new_query
    @cut_traceback
    def __getitem__(query, key):
        if isinstance(key, slice):
            step = key.step
            if step is not None and step != 1: throw(TypeError, "Parameter 'step' of slice object is not allowed here")
            start = key.start
            if start is None: start = 0
            elif start < 0: throw(TypeError, "Parameter 'start' of slice object cannot be negative")
            stop = key.stop
            if stop is None:
                if not start: return query._fetch()
                else: throw(TypeError, "Parameter 'stop' of slice object should be specified")
        else: throw(TypeError, 'If you want apply index to query, convert it to list first')
        if start >= stop: return []
        return query._fetch(range=(start, stop))
    @cut_traceback
    def limit(query, limit, offset=None):
        start = offset or 0
        stop = start + limit
        return query[start:stop]
    @cut_traceback
    def page(query, pagenum, pagesize=10):
        start = (pagenum - 1) * pagesize
        stop = pagenum * pagesize
        return query[start:stop]
    def _aggregate(query, aggr_func_name):
        translator = query._translator
        sql, arguments, attr_offsets, query_key = query._construct_sql_and_arguments(aggr_func_name=aggr_func_name)
        cache = query._database._get_cache()
        try: result = cache.query_results[query_key]
        except KeyError:
            cursor = query._database._exec_sql(sql, arguments)
            row = cursor.fetchone()
            if row is not None: result = row[0]
            else: result = None
            if result is None and aggr_func_name == 'SUM': result = 0
            if result is None: pass
            elif aggr_func_name == 'COUNT': pass
            else:
                expr_type = float if aggr_func_name == 'AVG' else translator.expr_type
                provider = query._database.provider
                converter = provider.get_converter_by_py_type(expr_type)
                result = converter.sql2py(result)
            if query_key is not None: cache.query_results[query_key] = result
        return result
    @cut_traceback
    def sum(query):
        return query._aggregate('SUM')
    @cut_traceback
    def avg(query):
        return query._aggregate('AVG')
    @cut_traceback
    def min(query):
        return query._aggregate('MIN')
    @cut_traceback
    def max(query):
        return query._aggregate('MAX')
    @cut_traceback
    def count(query):
        return query._aggregate('COUNT')
    @cut_traceback
    def for_update(query, nowait=False):
        provider = query._database.provider
        if nowait and not provider.select_for_update_nowait_syntax: throw(TranslationError,
            '%s provider does not support SELECT FOR UPDATE NOWAIT syntax' % provider.dialect)
        return query._clone(_for_update=True, _nowait=nowait)
    def random(query, limit):
        return query.order_by('random()')[:limit]
    def to_json(query, include=(), exclude=(), converter=None, with_schema=True, schema_hash=None):
        return query._database.to_json(query[:], include, exclude, converter, with_schema, schema_hash)

def strcut(s, width):
    if len(s) <= width:
        return s + ' ' * (width - len(s))
    else:
        return s[:width-3] + '...'

class QueryResult(list):
    __slots__ = '_query', '_expr_type', '_col_names'
    def __init__(result, list, query, expr_type, col_names):
        result[:] = list
        result._query = query
        result._expr_type = expr_type
        result._col_names = col_names
    def __getstate__(result):
        return list(result), result._expr_type, result._col_names
    def __setstate__(result, state):
        result[:] = state[0]
        result._expr_type = state[1]
        result._col_names = state[2]
    @cut_traceback
    def show(result, width=None):
        if not width: width = options.CONSOLE_WIDTH
        max_columns = width // 5
        expr_type = result._expr_type
        col_names = result._col_names

        def to_str(x):
            return tostring(x).replace('\n', ' ')

        if isinstance(expr_type, EntityMeta):
            entity = expr_type
            col_names = [ attr.name for attr in entity._attrs_
                                    if not attr.is_collection and not attr.lazy ][:max_columns]
            if len(col_names) == 1:
                col_name = col_names[0]
                row_maker = lambda obj: (getattr(obj, col_name),)
            else: row_maker = attrgetter(*col_names)
            rows = [ tuple(to_str(value) for value in row_maker(obj)) for obj in result  ]
        elif len(col_names) == 1:
            rows = [ (to_str(obj),) for obj in result ]
        else:
            rows = [ tuple(to_str(value) for value in row) for row in result ]

        remaining_columns = {}
        for col_num, colname in enumerate(col_names):
            if not rows: max_len = len(colname)
            else: max_len = max(len(colname), max(len(row[col_num]) for row in rows))
            remaining_columns[col_num] = max_len

        width_dict = {}
        available_width = width - len(col_names) + 1
        while remaining_columns:
            base_len = (available_width - len(remaining_columns) + 1) // len(remaining_columns)
            for col_num, max_len in remaining_columns.items():
                if max_len <= base_len:
                    width_dict[col_num] = max_len
                    del remaining_columns[col_num]
                    available_width -= max_len
                    break
            else: break
        if remaining_columns:
            base_len = available_width // len(remaining_columns)
            for col_num, max_len in remaining_columns.items():
                width_dict[col_num] = base_len

        print(strjoin('|', (strcut(colname, width_dict[i]) for i, colname in enumerate(col_names))))
        print(strjoin('+', ('-' * width_dict[i] for i in xrange(len(col_names)))))
        for row in rows:
            print(strjoin('|', (strcut(item, width_dict[i]) for i, item in enumerate(row))))
    def to_json(result, include=(), exclude=(), converter=None, with_schema=True, schema_hash=None):
        return result._query._database.to_json(result, include, exclude, converter, with_schema, schema_hash)


@cut_traceback
def show(entity):
    x = entity
    if isinstance(x, EntityMeta):
        print(x.describe())
    elif isinstance(x, Entity):
        print('instance of ' + x.__class__.__name__)
        # width = options.CONSOLE_WIDTH
        # for attr in x._attrs_:
        #     if attr.is_collection or attr.lazy: continue
        #     value = str(attr.__get__(x)).replace('\n', ' ')
        #     print('  %s: %s' % (attr.name, strcut(value, width-len(attr.name)-4)))
        # print()
        QueryResult([ x ], None, x.__class__, None).show()
    elif isinstance(x, (basestring, types.GeneratorType)):
        select(x).show()
    elif hasattr(x, 'show'):
        x.show()
    else:
        from pprint import pprint
        pprint(x)

special_functions = {itertools.count, utils.count, count, random, raw_sql, getattr}
const_functions = {buffer, Decimal, datetime.datetime, datetime.date, datetime.time, datetime.timedelta}
