from __future__ import with_statement

import re, sys, types, inspect, logging
from compiler import ast, parse
from cPickle import loads, dumps
from operator import attrgetter, itemgetter
from itertools import count as _count, ifilter, ifilterfalse, imap, izip, chain, starmap
from time import time
import datetime
from threading import Lock, currentThread as current_thread, _MainThread
from __builtin__ import min as _min, max as _max, sum as _sum
import warnings

import pony
from pony import options
from pony.orm.decompiling import decompile
from pony.orm.ormtypes import AsciiStr, LongStr, LongUnicode, numeric_types, get_normalized_type_of
from pony.orm.asttranslation import create_extractors, TranslationError
from pony.orm.dbapiprovider import (
    DBAPIProvider, DBException, RowNotFound, MultipleRowsFound, TooManyRowsFound,
    Warning, Error, InterfaceError, DatabaseError, DataError, OperationalError,
    IntegrityError, InternalError, ProgrammingError, NotSupportedError
    )
from pony.utils import (
    localbase, simple_decorator, cut_traceback, throw,
    import_module, parse_expr, is_ident, count, avg as _avg, tostring, strjoin,
    copy_func_attrs
    )

__all__ = '''
    pony

    DBException RowNotFound MultipleRowsFound TooManyRowsFound

    Warning Error InterfaceError DatabaseError DataError OperationalError
    IntegrityError InternalError ProgrammingError NotSupportedError

    OrmError ERDiagramError DBSchemaError MappingError ConstraintError CacheIndexError ObjectNotFound
    MultipleObjectsFoundError TooManyObjectsFoundError OperationWithDeletedObjectError
    TransactionError TransactionIntegrityError IsolationError CommitException RollbackException
    UnrepeatableReadError UnresolvableCyclicDependency UnexpectedError

    TranslationError ExprEvalError

    Database sql_debug show

    PrimaryKey Required Optional Set Discriminator
    composite_key
    flush commit rollback db_session with_transaction

    AsciiStr LongStr LongUnicode

    select left_join get exists

    count sum min max avg

    desc

    JOIN
    '''.split()

debug = False

def sql_debug(value):
    global debug
    debug = value

class PonyDeprecationWarning(DeprecationWarning):
    pass

def deprecated(message):
    warnings.warn(message, PonyDeprecationWarning, stacklevel=3)

warnings.simplefilter('once', PonyDeprecationWarning)

orm_logger = logging.getLogger('pony.orm')
sql_logger = logging.getLogger('pony.orm.sql')

orm_log_level = logging.INFO

def log_orm(msg):
    if logging.root.handlers:
        orm_logger.log(orm_log_level, msg)
    else:
        print msg
        print

def log_sql(sql, arguments=None):
    if type(arguments) is list:
        sql = 'EXECUTEMANY (%d)\n%s' % (len(arguments), sql)
    if logging.root.handlers:
        sql_logger.log(orm_log_level, sql)  # arguments can hold sensitive information
    else:
        print sql
        if not arguments: pass
        elif type(arguments) is list:
            for args in arguments: print args2str(args)
        else: print args2str(arguments)
        print

def args2str(args):
    if isinstance(args, (tuple, list)):
        return '[%s]' % ', '.join(map(repr, args))
    elif isinstance(args, dict):
        return '{%s}' % ', '.join('%s:%s' % (repr(key), repr(val)) for key, val in sorted(args.iteritems()))

adapted_sql_cache = {}
string2ast_cache = {}

class OrmError(Exception): pass

class ERDiagramError(OrmError): pass
class DBSchemaError(OrmError): pass
class MappingError(OrmError): pass
class ConstraintError(OrmError): pass
class CacheIndexError(OrmError): pass

class ObjectNotFound(OrmError):
    def __init__(exc, entity, pkval):
        if type(pkval) is tuple:
            pkval = ','.join(map(repr, pkval))
        else: pkval = repr(pkval)
        msg = '%s[%s]' % (entity.__name__, pkval)
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

class TransactionRolledBack(TransactionError): pass
class IsolationError(TransactionError): pass
class   UnrepeatableReadError(IsolationError): pass
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

###############################################################################

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

next_num = _count().next

class Local(localbase):
    def __init__(local):
        local.db2cache = {}
        local.db_context_counter = 0

local = Local()

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
    def query_executed(stat, query_start_time):
        query_end_time = time()
        duration = query_end_time - query_start_time
        if stat.db_count:
            stat.min_time = _min(stat.min_time, duration)
            stat.max_time = _max(stat.max_time, duration)
            stat.sum_time += duration
        else: stat.min_time = stat.max_time = stat.sum_time = duration
        stat.db_count += 1
    def merge(stat, stat2):
        assert stat.sql == stat2.sql
        if not stat2.db_count: pass
        elif stat.db_count:
            stat.min_time = _min(stat.min_time, stat2.min_time)
            stat.max_time = _max(stat.max_time, stat2.max_time)
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

select_re = re.compile(r'\s*select\b', re.IGNORECASE)

class Database(object):
    def __deepcopy__(self, memo):
        return self  # Database cannot be cloned by deepcopy()
    @cut_traceback
    def __init__(self, provider, *args, **kwargs):
        # First argument cannot be named 'database', because 'database' can be in kwargs
        if isinstance(provider, type) and issubclass(provider, DBAPIProvider):
            provider_cls = provider
        else:
            if not isinstance(provider, basestring): throw(TypeError)
            provider_module = import_module('pony.orm.dbproviders.' + provider)
            provider_cls = provider_module.provider_cls

        self.provider = provider = provider_cls(*args, **kwargs)

        self.priority = 0
        self.optimistic = True
        self._insert_cache = {}

        # ER-diagram related stuff:
        self._translator_cache = {}
        self._constructed_sql_cache = {}
        self.entities = {}
        self._unmapped_attrs = {}
        self.schema = None
        self.Entity = type.__new__(EntityMeta, 'Entity', (Entity,), {})
        self.Entity._database_ = self

        self.global_stats = {}
        self.global_stats_lock = Lock()
        self.dblocal = DbLocal()
    @property
    def last_sql(database):
        return database.dblocal.last_sql
    @property
    def local_stats(database):
        return database.dblocal.stats
    def _update_local_stat(database, sql, query_start_time):
        dblocal = database.dblocal
        dblocal.last_sql = sql
        stats = dblocal.stats
        stat = stats.get(sql)
        if stat is not None: stat.query_executed(query_start_time)
        else: stats[sql] = QueryStat(sql, query_start_time)
    def merge_local_stats(database):
        setdefault = database.global_stats.setdefault
        database.global_stats_lock.acquire()
        try:
            for sql, stat in database.dblocal.stats.iteritems():
                global_stat = setdefault(sql, stat)
                if global_stat is not stat: global_stat.merge(stat)
        finally: database.global_stats_lock.release()
        database.dblocal.stats.clear()
    @cut_traceback
    def get_connection(database):
        cache = database._get_cache()
        cache.flush()
        assert cache.connection is not None
        return cache.connection
    def _get_cache(database):
        cache = local.db2cache.get(database)
        if cache is not None: return cache
        if not local.db_context_counter and not (
                pony.MODE == 'INTERACTIVE' and current_thread().__class__ is _MainThread
            ): throw(TransactionError, 'db_session is required when working with the database')
        cache = local.db2cache[database] = Cache(database)
        return cache
    @cut_traceback
    def flush(database):
        database._get_cache().flush()
    @cut_traceback
    def commit(database):
        cache = local.db2cache.get(database)
        if cache is not None: cache.commit()
    @cut_traceback
    def rollback(database):
        cache = local.db2cache.get(database)
        if cache is not None: cache.rollback()
    @cut_traceback
    def execute(database, sql, globals=None, locals=None):
        database._get_cache().flush()
        return database._exec_raw_sql(sql, globals, locals, 2)
    def _exec_raw_sql(database, sql, globals, locals, frame_depth):
        sql = sql[:]  # sql = templating.plainstr(sql)
        if globals is None:
            assert locals is None
            frame_depth += 1
            globals = sys._getframe(frame_depth).f_globals
            locals = sys._getframe(frame_depth).f_locals
        provider = database.provider
        adapted_sql, code = adapt_sql(sql, provider.paramstyle)
        arguments = eval(code, globals, locals)
        return database._exec_sql(sql, arguments)
    @cut_traceback
    def select(database, sql, globals=None, locals=None, frame_depth=0):
        if not select_re.match(sql): sql = 'select ' + sql
        cursor = database._exec_raw_sql(sql, globals, locals, frame_depth + 2)
        max_fetch_count = options.MAX_FETCH_COUNT
        if max_fetch_count is not None:
            result = cursor.fetchmany(max_fetch_count)
            if cursor.fetchone() is not None: throw(TooManyRowsFound)
        else: result = cursor.fetchall()
        if len(cursor.description) == 1: result = map(itemgetter(0), result)
        else:
            row_class = type("row", (tuple,), {})
            for i, column_info in enumerate(cursor.description):
                column_name = column_info[0]
                if not is_ident(column_name): continue
                if hasattr(tuple, column_name) and column_name.startswith('__'): continue
                setattr(row_class, column_name, property(itemgetter(i)))
            result = [ row_class(row) for row in result ]
        return result
    @cut_traceback
    def get(database, sql, globals=None, locals=None):
        rows = database.select(sql, globals, locals, 2)
        if not rows: throw(RowNotFound)
        if len(rows) > 1: throw(MultipleRowsFound)
        row = rows[0]
        return row
    @cut_traceback
    def exists(database, sql, globals=None, locals=None):
        if not select_re.match(sql): sql = 'select ' + sql
        cursor = database._exec_raw_sql(sql, globals, locals, 2)
        result = cursor.fetchone()
        return bool(result)
    @cut_traceback
    def insert(database, table_name, returning=None, **kwargs):
        table_name = table_name[:]  # table_name = templating.plainstr(table_name)
        query_key = (table_name,) + tuple(kwargs)  # keys are not sorted deliberately!!
        if returning is not None: query_key = query_key + (returning,)
        cached_sql = database._insert_cache.get(query_key)
        if cached_sql is None:
            ast = [ 'INSERT', table_name, kwargs.keys(), [ [ 'PARAM', i ] for i in range(len(kwargs)) ], returning ]
            sql, adapter = database._ast2sql(ast)
            cached_sql = sql, adapter
            database._insert_cache[query_key] = cached_sql
        else: sql, adapter = cached_sql
        arguments = adapter(kwargs.values())  # order of values same as order of keys
        cache = database._get_cache()
        if cache.optimistic: cache.flush()
        if returning is None:
            cursor = database._exec_sql(sql, arguments)
            return getattr(cursor, 'lastrowid', None)
        new_id = database._exec_sql(sql, arguments, returning_id=True)
        return new_id
    def _ast2sql(database, sql_ast):
        sql, adapter = database.provider.ast2sql(sql_ast)
        return sql, adapter
    def _exec_sql(database, sql, arguments=None, returning_id=False):
        cache = database._get_cache()
        if not cache.saving and not cache.optimistic and cache.modified: cache.flush()
        connection = cache.connection or cache.establish_connection()
        cursor = connection.cursor()
        if debug: log_sql(sql, arguments)
        provider = database.provider
        t = time()
        try: new_id = provider.execute(cursor, sql, arguments, returning_id)
        except Exception, e:
            if not provider.should_reconnect(e.original_exc): raise
            log_orm('CONNECTION FAILED: %s' % e.original_exc)
            cache.connection = None
            provider.drop(connection)
            connection = cache.establish_connection()
            cursor = connection.cursor()
            t = time()
            new_id = provider.execute(cursor, sql, arguments, returning_id)
        database._update_local_stat(sql, t)
        if not returning_id: return cursor
        if type(new_id) is long: new_id = int(new_id)
        return new_id
    @cut_traceback
    def generate_mapping(database, filename=None, check_tables=False, create_tables=False):
        if create_tables and check_tables: throw(TypeError,
            "Parameters 'check_tables' and 'create_tables' cannot be set to True at the same time")
        if local.db_context_counter: throw(MappingError,
            "generate_mapping() couldn't be used inside @db_session")
        database.rollback()

        def get_columns(table, column_names):
            return tuple(map(table.column_dict.__getitem__, column_names))

        if database.schema: throw(MappingError, 'Mapping was already generated')
        if filename is not None: throw(NotImplementedError)
        for entity_name in database._unmapped_attrs:
            throw(ERDiagramError, 'Entity definition %s was not found' % entity_name)

        provider = database.provider
        schema = database.schema = provider.dbschema_cls(provider)
        entities = list(sorted(database.entities.values(), key=attrgetter('_id_')))
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
                        attr.table = reverse.table = table_name

                    m2m_table = schema.tables.get(table_name)
                    if m2m_table is not None:
                        if m2m_table.entities or m2m_table.m2m:
                            if isinstance(table_name, tuple): table_name = '.'.join(table_name)
                            throw(MappingError, "Table name '%s' is already in use" % table_name)
                        throw(NotImplementedError)
                    m2m_table = schema.add_table(table_name)
                    m2m_columns_1 = attr.get_m2m_columns(is_reverse=False)
                    m2m_columns_2 = reverse.get_m2m_columns(is_reverse=True)
                    if m2m_columns_1 == m2m_columns_2: throw(MappingError,
                        'Different column names should be specified for attributes %s and %s' % (attr, reverse))
                    if attr.symmetric and len(attr.reverse_columns) != len(attr.entity._pk_attrs_):
                        throw(MappingError, "Invalid number of reverse columns for symmetric attribute %s" % attr)
                    assert len(m2m_columns_1) == len(reverse.converters)
                    assert len(m2m_columns_2) == len(attr.converters)
                    for column_name, converter in zip(m2m_columns_1 + m2m_columns_2, reverse.converters + attr.converters):
                        m2m_table.add_column(column_name, converter.sql_type(), True)
                    m2m_table.add_index(None, tuple(m2m_table.column_list), is_pk=True)
                    m2m_table.m2m.add(attr)
                    m2m_table.m2m.add(reverse)
                else:
                    if schema.dialect == 'Oracle' and attr.is_string and not attr.is_required:
                        if attr.nullable is False: throw(ERDiagramError,
                            'In Oracle, optional string attribute %s must be nullable' % attr)
                        attr.nullable = True
                    if entity._root_ is not entity:
                        if attr.nullable is False: throw(ERDiagramError,
                            'Attribute %s must be nullable due to single-table inheritance' % attr)
                        attr.nullable = True
                    columns = attr.get_columns()
                    if not attr.reverse and attr.default is not None:
                        assert len(attr.converters) == 1
                        if not callable(attr.default): attr.default = attr.check(attr.default)
                    assert len(columns) == len(attr.converters)
                    if len(columns) == 1:
                        sql_type = attr.sql_type or attr.converters[0].sql_type()
                        table.add_column(columns[0], sql_type, not attr.nullable)
                    else:
                        if attr.sql_type is not None: throw(NotImplementedError,
                            'sql_type cannot be specified for composite attribute %s' % attr)
                        for (column_name, converter) in zip(columns, attr.converters):
                            table.add_column(column_name, converter.sql_type(), not attr.nullable)
            if not table.pk_index:
                if len(entity._pk_columns_) == 1 and entity.__dict__['_pk_'].auto: is_pk = "auto"
                else: is_pk = True
                table.add_index(None, get_columns(table, entity._pk_columns_), is_pk)
            for key in entity._keys_:
                column_names = []
                for attr in key: column_names.extend(attr.columns)
                if len(key) == 1: index_name = key[0].index
                else: index_name = None
                table.add_index(index_name, get_columns(table, column_names), is_unique=True)
            columns = []
            columns_without_pk = []
            converters = []
            converters_without_pk = []
            for attr in entity._attrs_:
                if attr.is_collection: continue
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
                    columns = tuple(map(table.column_dict.__getitem__, attr.columns))
                    table.add_index(attr.index, columns, is_unique=attr.is_unique)

        if create_tables: schema.create_tables()

        if not check_tables and not create_tables: return

        local.db_context_counter = True
        try:
            for table in schema.tables.values():
                if isinstance(table.name, tuple): alias = table.name[-1]
                elif isinstance(table.name, basestring): alias = table.name
                else: assert False
                sql_ast = [ 'SELECT',
                            [ 'ALL', ] + [ [ 'COLUMN', alias, column.name ] for column in table.column_list ],
                            [ 'FROM', [ alias, 'TABLE', table.name ] ],
                            [ 'WHERE', [ 'EQ', [ 'VALUE', 0 ], [ 'VALUE', 1 ] ] ]
                          ]
                sql, adapter = database._ast2sql(sql_ast)
                database._exec_sql(sql)
        finally: local.db_context_counter = False
        database.rollback()

###############################################################################

class NotLoadedValueType(object):
    def __repr__(self): return 'NOT_LOADED'

NOT_LOADED = NotLoadedValueType()

class DefaultValueType(object):
    def __repr__(self): return 'DEFAULT'

DEFAULT = DefaultValueType()

class NoUndoNeededValueType(object):
    def __repr__(self): return 'NO_UNDO_NEEDED'

NO_UNDO_NEEDED = NoUndoNeededValueType()

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

next_attr_id = _count(1).next

class Attribute(object):
    __slots__ = 'nullable', 'is_required', 'is_discriminator', 'is_unique', 'is_part_of_unique_index', \
                'is_pk', 'is_collection', 'is_ref', 'is_basic', 'is_string', \
                'id', 'pk_offset', 'pk_columns_offset', 'py_type', 'sql_type', 'entity', 'name', \
                'lazy', 'lazy_sql_cache', 'args', 'auto', 'default', 'reverse', 'composite_keys', \
                'column', 'columns', 'col_paths', '_columns_checked', 'converters', 'kwargs', \
                'cascade_delete', 'index'
    def __deepcopy__(attr, memo):
        return attr  # Attribute cannot be cloned by deepcopy()
    @cut_traceback
    def __init__(attr, py_type, *args, **kwargs):
        if attr.__class__ is Attribute: throw(TypeError, "'Attribute' is abstract type")
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
        attr.id = next_attr_id()
        if not isinstance(py_type, basestring) and not isinstance(py_type, type):
            if py_type is datetime: throw(TypeError,
                'datetime is the module and cannot be used as attribute type. Use datetime.datetime instead')
            throw(TypeError, 'Incorrect type of attribute: %r' % py_type)
        if py_type == 'Entity' or (isinstance(py_type, EntityMeta) and py_type.__name__ == 'Entity'):
            throw(TypeError, 'Cannot link attribute to Entity class. Must use Entity subclass instead')
        attr.py_type = py_type
        attr.is_string = type(py_type) is type and issubclass(py_type, basestring)
        attr.is_collection = isinstance(attr, Collection)
        attr.is_ref = not attr.is_collection and isinstance(attr.py_type, (EntityMeta, basestring))
        attr.is_basic = not attr.is_collection and not attr.is_ref
        attr.sql_type = kwargs.pop('sql_type', None)
        attr.entity = attr.name = None
        attr.args = args
        attr.auto = kwargs.pop('auto', False)
        attr.cascade_delete = kwargs.pop('cascade_delete', None)

        attr.reverse = kwargs.pop('reverse', None)
        if not attr.reverse: pass
        elif not isinstance(attr.reverse, (basestring, Attribute)):
            throw(TypeError, "Value of 'reverse' option must be name of reverse attribute). Got: %r" % attr.reverse)
        elif not isinstance(attr.py_type, (basestring, EntityMeta)):
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
            if not attr.is_string:
                if attr.nullable is False:
                    throw(TypeError, 'Optional attribute with non-string type %s must be nullable' % attr)
                attr.nullable = True

        try: attr.default = attr.kwargs.pop('default')
        except KeyError: attr.default = '' if attr.is_string and not attr.is_required else None
        else:
            if attr.is_required:
                if attr.default is None:
                    throw(TypeError, 'Default value for required attribute %s cannot be None' % attr)
                if attr.default == '':
                    throw(TypeError, 'Default value for required attribute %s cannot be empty string' % attr)

        if attr.py_type == float:
            if attr.pk_offset is not None:
                throw(TypeError, 'Primary key attribute %s cannot be of type float' % attr)
            elif attr.is_unique:
                throw(TypeError, 'Unique attribute %s cannot be of type float' % attr)
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
        owner_name = not attr.entity and '?' or attr.entity.__name__
        return '%s.%s' % (owner_name, attr.name or '?')
    def check(attr, val, obj=None, entity=None, from_db=False):
        if val is None:
            if not attr.nullable and not from_db:
                throw(ConstraintError, 'Attribute %s cannot be set to None' % attr)
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
            if attr.converters:
                if len(attr.converters) != 1: throw(NotImplementedError)
                converter = attr.converters[0]
                if converter is not None:
                    try:
                        if from_db: return converter.sql2py(val)
                        else: return converter.validate(val)
                    except UnicodeDecodeError, e:
                        vrepr = repr(val)
                        if len(vrepr) > 100: vrepr = vrepr[:97] + '...'
                        raise ValueError('Value for attribute %s cannot be converted to unicode: %s' % (attr, vrepr))
            return attr.py_type(val)

        if not isinstance(val, reverse.entity):
            throw(ConstraintError, 'Value of attribute %s must be an instance of %s. Got: %s'
                                  % (attr, reverse.entity.__name__, val))
        if obj is not None: cache = obj._cache_
        else: cache = entity._get_cache_()
        if cache is not val._cache_:
            throw(TransactionError, 'An attempt to mix objects belongs to different caches')
        return val
    def parse_value(attr, row, offsets):
        assert len(attr.columns) == len(offsets)
        if not attr.reverse:
            if len(offsets) > 1: throw(NotImplementedError)
            offset = offsets[0]
            val = attr.check(row[offset], None, attr.entity, from_db=True)
        else:
            vals = map(row.__getitem__, offsets)
            if None in vals:
                assert len(set(vals)) == 1
                val = None
            else: val = attr.py_type._get_by_raw_pkval_(vals)
        return val
    def load(attr, obj):
        if not attr.columns:
            reverse = attr.reverse
            assert reverse is not None and reverse.columns
            objects = reverse.entity._find_in_db_({reverse : obj}, 1)
            if not objects:
                obj._vals_[attr.name] = None
                return None
            elif len(objects) == 1:
                dbval = objects[0]
                assert obj._vals_[attr.name] == dbval
                return dbval
            else: assert False
        if attr.lazy:
            entity = attr.entity
            database = entity._database_
            if not attr.lazy_sql_cache:
                select_list = [ 'ALL' ] + [ [ 'COLUMN', None, column ] for column in attr.columns ]
                from_list = [ 'FROM', [ None, 'TABLE', entity._table_ ] ]
                pk_columns = entity._pk_columns_
                pk_converters = entity._pk_converters_
                criteria_list = [ [ 'EQ', [ 'COLUMN', None, column ], [ 'PARAM', i, converter ] ]
                                  for i, (column, converter) in enumerate(izip(pk_columns, pk_converters)) ]
                sql_ast = [ 'SELECT', select_list, from_list, [ 'WHERE' ] + criteria_list ]
                sql, adapter = database._ast2sql(sql_ast)
                offsets = tuple(range(len(attr.columns)))
                attr.lazy_sql_cache = sql, adapter, offsets
            else: sql, adapter, offsets = attr.lazy_sql_cache
            arguments = adapter(obj._get_raw_pkval_())
            cursor = database._exec_sql(sql, arguments)
            row = cursor.fetchone()
            dbval = attr.parse_value(row, offsets)
            attr.db_set(obj, dbval)
        else: obj._load_()
        return obj._vals_[attr.name]
    @cut_traceback
    def __get__(attr, obj, cls=None):
        if obj is None: return attr
        if not obj._cache_.is_alive: throw_db_session_is_over(obj)
        result = attr.get(obj)
        if attr.pk_offset is not None: return result
        bit = obj._bits_[attr]
        wbits = obj._wbits_
        if wbits is not None and not wbits & bit: obj._rbits_ |= bit
        return result
    def get(attr, obj):
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        val = obj._vals_.get(attr.name, NOT_LOADED)
        if val is NOT_LOADED: val = attr.load(obj)
        if val is None: return val
        if attr.reverse and val._discriminator_ and val._subclasses_:
            seeds = obj._cache_.seeds.get(val.__class__.__dict__['_pk_'])
            if seeds and val in seeds: val._load_()
        return val
    @cut_traceback
    def __set__(attr, obj, new_val, undo_funcs=None):
        cache = obj._cache_
        if not cache.is_alive: throw_db_session_is_over(obj)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        is_reverse_call = undo_funcs is not None
        reverse = attr.reverse
        new_val = attr.check(new_val, obj, from_db=False)
        if attr.pk_offset is not None:
            pkval = obj._pkval_
            if pkval is None: pass
            elif obj._pk_is_composite_:
                if new_val == pkval[attr.pk_offset]: return
            elif new_val == pkval: return
            throw(TypeError, 'Cannot change value of primary key')
        old_val =  obj._vals_.get(attr.name, NOT_LOADED)
        if old_val is NOT_LOADED and reverse and not reverse.is_collection:
            old_val = attr.load(obj)
        status = obj._status_
        wbits = obj._wbits_
        if wbits is not None:
            obj._wbits_ = wbits | obj._bits_[attr]
            if status != 'updated':
                if status in ('loaded', 'saved'): cache.to_be_checked.append(obj)
                else: assert status == 'locked'
                obj._status_ = 'updated'
                cache.modified = True
                cache.updated.add(obj)
        if not attr.reverse and not attr.is_part_of_unique_index:
            obj._vals_[attr.name] = new_val
            return
        if not is_reverse_call: undo_funcs = []
        undo = []
        def undo_func():
            obj._status_ = status
            obj._wbits_ = wbits
            if wbits == 0: cache.updated.remove(obj)
            if status in ('loaded', 'saved'):
                to_be_checked = cache.to_be_checked
                if to_be_checked and to_be_checked[-1] is obj: to_be_checked.pop()
                assert obj not in to_be_checked
            obj._vals_[attr.name] = old_val
            for index, old_key, new_key in undo:
                if new_key is NO_UNDO_NEEDED: pass
                else: del index[new_key]
                if old_key is NO_UNDO_NEEDED: pass
                else: index[old_key] = obj
        undo_funcs.append(undo_func)
        if old_val == new_val: return
        try:
            if attr.is_unique:
                cache.update_simple_index(obj, attr, old_val, new_val, undo)
            for attrs, i in attr.composite_keys:
                get = obj._vals_.get
                vals = [ get(a.name, NOT_LOADED) for a in attrs ]
                currents = tuple(vals)
                vals[i] = new_val
                vals = tuple(vals)
                cache.update_composite_index(obj, attrs, currents, vals, undo)

            obj._vals_[attr.name] = new_val

            if not reverse: pass
            elif not is_reverse_call: attr.update_reverse(obj, old_val, new_val, undo_funcs)
            elif old_val not in (None, NOT_LOADED):
                if not reverse.is_collection:
                    if new_val is not None: reverse.__set__(old_val, None, undo_funcs)
                elif isinstance(reverse, Set):
                    reverse.reverse_remove((old_val,), obj, undo_funcs)
                else: throw(NotImplementedError)
        except:
            if not is_reverse_call:
                for undo_func in reversed(undo_funcs): undo_func()
            raise
    def db_set(attr, obj, new_dbval, is_reverse_call=False):
        cache = obj._cache_
        assert cache.is_alive
        assert obj._status_ not in created_or_deleted_statuses
        assert attr.pk_offset is None
        if new_dbval is NOT_LOADED: assert is_reverse_call
        old_dbval = obj._dbvals_.get(attr.name, NOT_LOADED)

        if attr.py_type is float:
            if old_dbval is NOT_LOADED: pass
            elif attr.converters[0].equals(old_dbval, new_dbval): return
        elif old_dbval == new_dbval: return

        bit = obj._bits_[attr]
        if obj._rbits_ & bit:
            assert old_dbval is not NOT_LOADED
            if new_dbval is NOT_LOADED: diff = ''
            else: diff = ' (was: %s, now: %s)' % (old_dbval, new_dbval)
            throw(UnrepeatableReadError,
                'Value of %s.%s for %s was updated outside of current transaction%s'
                % (obj.__class__.__name__, attr.name, obj, diff))

        if new_dbval is NOT_LOADED: obj._dbvals_.pop(attr.name, None)
        else: obj._dbvals_[attr.name] = new_dbval

        wbit = bool(obj._wbits_ & bit)
        if not wbit:
            old_val = obj._vals_.get(attr.name, NOT_LOADED)
            assert old_val == old_dbval
            if attr.is_part_of_unique_index:
                cache = obj._cache_
                if attr.is_unique: cache.db_update_simple_index(obj, attr, old_val, new_dbval)
                for attrs, i in attr.composite_keys:
                    vals = [ obj._vals_.get(a.name, NOT_LOADED) for a in attrs ]
                    old_vals = tuple(vals)
                    vals[i] = new_dbval
                    new_vals = tuple(vals)
                    cache.db_update_composite_index(obj, attrs, old_vals, new_vals)
            if new_dbval is NOT_LOADED: obj._vals_.pop(attr.name, None)
            else: obj._vals_[attr.name] = new_dbval

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
            if old_val not in (None, NOT_LOADED): reverse.__set__(old_val, None, undo_funcs)
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
        if attr.args: options.append(', '.join(map(str, attr.args)))
        if attr.auto: options.append('auto=True')
        if not isinstance(attr, PrimaryKey) and attr.is_unique: options.append('unique=True')
        if attr.default is not None: options.append('default=%s' % attr.default)
        if not options: options = ''
        else: options = ', ' + ', '.join(options)
        result = "%s(%s%s)" % (attr.__class__.__name__, t, options)
        return "%s = %s" % (attr.name,result)

class Optional(Attribute):
    __slots__ = []

class Required(Attribute):
    __slots__ = []
    def check(attr, val, obj=None, entity=None, from_db=False):
        val = Attribute.check(attr, val, obj, entity, from_db)  # val may be changed to None here
        if val == '' or val is None and not attr.auto:
            if entity is not None: pass
            elif obj is not None: entity = obj.__class__
            else: entity = attr.entity
            if obj is None: throw(ConstraintError, 'Attribute %s is required' % attr)
            else: throw(ConstraintError, 'Attribute %s of %r is required' % (attr, obj))
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
        attr._init_(entity, 'classtype')
        entity._attrs_.append(attr)
        entity._new_attrs_.append(attr)
        entity._adict_['classtype'] = attr
        type.__setattr__(entity, 'classtype', attr)
        attr.process_entity_inheritance(entity)
    def process_entity_inheritance(attr, entity):
        if '_discriminator_' not in entity.__dict__:
            entity._discriminator_ = entity.__name__
        discr_value = entity._discriminator_
        if discr_value is None:
            discr_value = entity._discriminator_ = entity.__name__
        discr_type = type(discr_value)
        for code, cls in attr.code2cls.items():
            if type(code) != discr_type: throw(ERDiagramError,
                'Discriminator values %r and %r of entities %s and %s have different types'
                % (code, discr_value, cls, entity))
        attr.code2cls[discr_value] = entity
    def check(attr, val, obj=None, entity=None, from_db=False):
        if from_db: return val
        elif val is DEFAULT:
            assert entity is not None
            return entity._discriminator_
        return Attribute.check(attr, val, obj, entity)
    def load(attr, obj):
        raise AssertionError
    def __get__(attr, obj, cls=None):
        if obj is None: return attr
        return obj._discriminator_
    def __set__(attr, obj, new_val):
        throw(TypeError, 'Cannot assign value to discriminator attribute')
    def db_set(attr, obj, new_dbval):
        assert False
    def update_reverse(attr, obj, old_val, new_val, undo_funcs):
        assert False

def composite_key(*attrs):
    if len(attrs) < 2: throw(TypeError,
        'composite_key() must receive at least two attributes as arguments')
    for i, attr in enumerate(attrs):
        if not isinstance(attr, Attribute): throw(TypeError,
            'composite_key() arguments must be attributes. Got: %r' % attr)
        attr.is_part_of_unique_index = True
        attr.composite_keys.append((attrs, i))
    cls_dict = sys._getframe(1).f_locals
    composite_keys = cls_dict.setdefault('_keys_', {})
    composite_keys[attrs] = False

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
            for key, val in cls_dict.iteritems():
                if val is attr: attr_name = key; break
            py_type = attr.py_type
            type_str = py_type.__name__ if type(py_type) is type else repr(py_type)
            throw(TypeError, 'Just use %s = PrimaryKey(%s, ...) directly instead of PrimaryKey(%s)'
                  % (attr_name, type_str, attr_name))

        for i, attr in enumerate(attrs):
            attr.is_part_of_unique_index = True
            attr.composite_keys.append((attrs, i))
        keys = cls_dict.setdefault('_keys_', {})
        keys[attrs] = True
        return None

class Collection(Attribute):
    __slots__ = 'table', 'cached_load_sql', 'cached_add_m2m_sql', 'cached_remove_m2m_sql', 'wrapper_class', \
                'symmetric', 'reverse_column', 'reverse_columns', 'nplus1_threshold'
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
    def _init_(attr, entity, name):
        Attribute._init_(attr, entity, name)
        if attr.is_unique: throw(TypeError,
            "'unique' option cannot be set for attribute %s because it is collection" % attr)
        if attr.default is not None:
            throw(TypeError, 'Default value could not be set for collection attribute')
        attr.symmetric = (attr.py_type == entity.__name__ and attr.reverse == name)
        if not attr.symmetric and attr.reverse_columns: throw(TypeError,
            "'reverse_column' and 'reverse_columns' options can be set for symmetric relations only")
    def load(attr, obj):
        assert False, 'Abstract method'
    def __get__(attr, obj, cls=None):
        assert False, 'Abstract method'
    def __set__(attr, obj, val):
        assert False, 'Abstract method'
    def __delete__(attr, obj):
        assert False, 'Abstract method'
    def prepare(attr, obj, val, fromdb=False):
        assert False, 'Abstract method'
    def set(attr, obj, val, fromdb=False):
        assert False, 'Abstract method'

EMPTY = ()

class SetData(set):
    __slots__ = 'is_fully_loaded', 'added', 'removed'
    def __init__(setdata):
        setdata.is_fully_loaded = False
        setdata.added = setdata.removed = EMPTY

def construct_criteria_list(alias, columns, converters, row_value_syntax, count=1, start=0):
    assert count > 0
    if count == 1:
        return [ [ 'EQ', [ 'COLUMN', alias, column ], [ 'PARAM', (start, i), converter ] ]
                 for i, (column, converter) in enumerate(izip(columns, converters)) ]
    if len(columns) == 1:
        column = columns[0]
        converter = converters[0]
        param_list = [ [ 'PARAM', (i+start, 0), converter ] for i in xrange(count) ]
        condition = [ 'IN', [ 'COLUMN', alias, column ], param_list ]
        return [ condition ]
    elif row_value_syntax:
        row = [ 'ROW' ] + [ [ 'COLUMN', alias, column ] for column in columns ]
        param_list = [ [ 'ROW' ] + [ [ 'PARAM', (i+start, j), converter ]
                                     for j, converter in enumerate(converters) ]
                       for i in xrange(count) ]
        condition = [ 'IN', row, param_list ]
        return [ condition ]
    else:
        conditions = [ [ 'AND' ] + [ [ 'EQ', [ 'COLUMN', alias, column ], [ 'PARAM', (i+start, j), converter ] ]
                                     for j, (column, converter) in enumerate(izip(columns, converters)) ]
                       for i in xrange(count) ]
        return [ [ 'OR' ] + conditions ]

class Set(Collection):
    __slots__ = []
    def check(attr, val, obj=None, entity=None, from_db=False):
        assert val is not NOT_LOADED
        if val is None or val is DEFAULT: return set()
        if entity is not None: pass
        elif obj is not None: entity = obj.__class__
        else: entity = attr.entity
        reverse = attr.reverse
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
        if obj is not None: cache = obj._cache_
        else: cache = entity._get_cache_()
        for item in items:
            if item._cache_ is not cache:
                throw(TransactionError, 'An attempt to mix objects belongs to different caches')
        return items
    def load(attr, obj, items=None):
        assert obj._status_ not in del_statuses
        setdata = obj._vals_.get(attr.name, NOT_LOADED)
        if setdata is NOT_LOADED: setdata = obj._vals_[attr.name] = SetData()
        elif setdata.is_fully_loaded: return setdata
        entity = attr.entity
        reverse = attr.reverse
        rentity = reverse.entity
        if not reverse: throw(NotImplementedError)
        cache = obj._cache_
        assert cache.is_alive
        database = obj._database_
        if cache is not database._get_cache():
            throw(TransactionError, "Transaction of object %s belongs to different thread")

        counter = cache.collection_statistics.setdefault(attr, 0)
        nplus1_threshold = attr.nplus1_threshold
        prefetching = not attr.lazy and nplus1_threshold is not None and counter >= nplus1_threshold

        if items and (attr.lazy or not setdata):
            items_to_load = [ item for item in items
                              if item not in setdata and item not in setdata.removed ]
            if not items_to_load: return setdata

            value_dict = dict(enumerate(items))
            if not reverse.is_collection:
                sql, adapter, attr_offsets = rentity._construct_batchload_sql_(len(items))
                arguments = adapter(value_dict)
                cursor = database._exec_sql(sql, arguments)
                items = rentity._fetch_objects(cursor, attr_offsets)
                return setdata
            
            items_count = len(items_to_load)            
            sql, adapter = attr.construct_sql_m2m(1, items_count)
            value_dict[items_count] = obj
            arguments = adapter(value_dict)
            cursor = database._exec_sql(sql, arguments)
            loaded_items = set(imap(rentity._get_by_raw_pkval_, cursor.fetchall()))
            setdata |= loaded_items
            reverse.db_reverse_add(loaded_items, obj)
            return setdata

        objects = [ obj ]
        setdata_list = [ setdata ]
        if prefetching:
            pk_index = cache.indexes.get(entity.__dict__['_pk_'])
            max_batch_size = database.provider.max_params_count // len(entity._pk_columns_)
            for obj2 in pk_index.itervalues():
                if obj2 is obj: continue
                if obj2._status_ in created_or_deleted_statuses: continue
                setdata2 = obj2._vals_.get(attr.name, NOT_LOADED)
                if setdata2 is NOT_LOADED: setdata2 = obj2._vals_[attr.name] = SetData()
                elif setdata2.is_fully_loaded: continue
                objects.append(obj2)
                setdata_list.append(setdata2)
                if len(objects) >= max_batch_size: break

        value_dict = dict(enumerate(objects))

        if not reverse.is_collection:
            sql, adapter, attr_offsets = rentity._construct_batchload_sql_(len(objects), reverse)
            arguments = adapter(value_dict)
            cursor = database._exec_sql(sql, arguments)
            items = rentity._fetch_objects(cursor, attr_offsets)
        else:
            sql, adapter = attr.construct_sql_m2m(len(objects))
            arguments = adapter(value_dict)
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
            else: d[obj] = set(imap(rentity._get_by_raw_pkval_, cursor.fetchall()))
            for obj2, items in d.iteritems():
                setdata2 = obj2._vals_.get(attr.name, NOT_LOADED)
                if setdata2 is NOT_LOADED: setdata2 = obj._vals_[attr.name] = SetData()
                else:
                    phantoms = setdata2 - items
                    phantoms.difference_update(setdata2.added)
                    if phantoms: throw(UnrepeatableReadError,
                        'Phantom object %r disappeared from collection %r.%s' % (phantoms.pop(), obj, attr.name))
                items -= setdata2
                items.difference_update(setdata2.removed)
                setdata2 |= items
                reverse.db_reverse_add(items, obj2)

        for setdata2 in setdata_list: setdata2.is_fully_loaded = True
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
        where_list += construct_criteria_list('T1', rcolumns, rconverters, row_value_syntax, batch_size, items_count)
        if items_count:
            where_list += construct_criteria_list('T1', columns, converters, row_value_syntax, items_count)
        sql_ast = [ 'SELECT', select_list, from_list, where_list ]
        sql, adapter = attr.cached_load_sql[cache_key] = database._ast2sql(sql_ast)
        return sql, adapter
    def copy(attr, obj):
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        setdata = obj._vals_.get(attr.name, NOT_LOADED)
        if setdata is NOT_LOADED or not setdata.is_fully_loaded: setdata = attr.load(obj)
        reverse = attr.reverse
        if not reverse.is_collection and reverse.pk_offset is None:
            added = setdata.added
            for item in setdata:
                if item not in added: item._rbits_ |= item._bits_[reverse]
        return set(setdata)
    @cut_traceback
    def __get__(attr, obj, cls=None):
        if obj is None: return attr
        if not obj._cache_.is_alive: throw_db_session_is_over(obj)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        rentity = attr.py_type
        wrapper_class = rentity._get_set_wrapper_subclass_()
        return wrapper_class(obj, attr)
    @cut_traceback
    def __set__(attr, obj, new_items, undo_funcs=None):
        if isinstance(new_items, SetWrapper) and new_items._obj_ is obj and new_items._attr_ is attr:
            return  # after += or -=
        cache = obj._cache_
        if not cache.is_alive: throw_db_session_is_over(obj)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        new_items = attr.check(new_items, obj)
        reverse = attr.reverse
        if not reverse: throw(NotImplementedError)
        setdata = obj._vals_.get(attr.name, NOT_LOADED)
        if setdata is NOT_LOADED:
            if obj._status_ == 'created':
                setdata = obj._vals_[attr.name] = SetData()
                setdata.is_fully_loaded = True
                if not new_items: return
            else: setdata = attr.load(obj)
        elif not setdata.is_fully_loaded: setdata = attr.load(obj)
        to_add = set(ifilterfalse(setdata.__contains__, new_items))
        to_remove = setdata - new_items
        if undo_funcs is None: undo_funcs = []
        try:
            if not reverse.is_collection:
                for item in to_remove: reverse.__set__(item, None, undo_funcs)
                for item in to_add: reverse.__set__(item, obj, undo_funcs)
            else:
                reverse.reverse_remove(to_remove, obj, undo_funcs)
                reverse.reverse_add(to_add, obj, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
            raise
        setdata.clear()
        setdata.update(new_items)
        if to_add:
            if setdata.added is EMPTY: setdata.added = to_add
            else: setdata.added.update(to_add)
            if setdata.removed is not EMPTY: setdata.removed -= to_add
        if to_remove:
            if setdata.removed is EMPTY: setdata.removed = to_remove
            else: setdata.removed.update(to_remove)
            if setdata.added is not EMPTY: setdata.added -= to_remove
        cache.modified = True
        cache.modified_collections.setdefault(attr, set()).add(obj)
    def __delete__(attr, obj):
        throw(NotImplementedError)
    def reverse_add(attr, objects, item, undo_funcs):
        undo = []
        cache = item._cache_
        objects_with_modified_collections = cache.modified_collections.setdefault(attr, set())
        for obj in objects:
            setdata = obj._vals_.get(attr.name, NOT_LOADED)
            if setdata is NOT_LOADED:
                setdata = obj._vals_[attr.name] = SetData()
            if setdata.added is EMPTY: setdata.added = set()
            elif item in setdata.added: raise AssertionError
            in_setdata = item in setdata
            in_removed = item in setdata.removed
            was_modified_earlier = obj in objects_with_modified_collections
            undo.append((obj, in_setdata, in_removed, was_modified_earlier))
            if not in_setdata: setdata.add(item)
            setdata.added.add(item)
            if in_removed: setdata.removed.remove(item)
            objects_with_modified_collections.add(obj)
        def undo_func():
            for obj, in_setdata, in_removed, was_modified_earlier in undo:
                setdata = obj._vals_[attr.name]
                setdata.added.remove(item)
                if not in_setdata: setdata.remove(item)
                if in_removed: setdata.removed.add(item)
                if not was_modified_earlier: objects_with_modified_collections.remove(obj)
        undo_funcs.append(undo_func)
    def db_reverse_add(attr, objects, item):
        for obj in objects:
            setdata = obj._vals_.get(attr.name, NOT_LOADED)
            if setdata is NOT_LOADED:
                setdata = obj._vals_[attr.name] = SetData()
            elif setdata.is_fully_loaded:
                throw(UnrepeatableReadError, 'Phantom object %r appeared in collection %r.%s' % (item, obj, attr.name))
            setdata.add(item)
    def reverse_remove(attr, objects, item, undo_funcs):
        undo = []
        cache = item._cache_
        objects_with_modified_collections = cache.modified_collections.setdefault(attr, set())
        for obj in objects:
            setdata = obj._vals_.get(attr.name, NOT_LOADED)
            if setdata is NOT_LOADED:
                setdata = obj._vals_[attr.name] = SetData()
            if setdata.removed is EMPTY: setdata.removed = set()
            elif item in setdata.removed: raise AssertionError
            in_setdata = item in setdata
            in_added = item in setdata.added
            was_modified_earlier = obj in objects_with_modified_collections
            undo.append((obj, in_setdata, in_added, was_modified_earlier))
            objects_with_modified_collections.add(obj)
            if in_setdata: setdata.remove(item)
            if in_added: setdata.added.remove(item)
            if item._status_ not in ('created', 'cancelled'):
                setdata.removed.add(item)
        def undo_func():
            for obj, in_setdata, in_removed, was_modified_earlier in undo:
                setdata = obj._vals_[attr.name]
                if in_added: setdata.added.add(item)
                if in_setdata: setdata.add(item)
                setdata.removed.discard(item)
                if not was_modified_earlier: objects_with_modified_collections.remove(obj)
        undo_funcs.append(undo_func)
    def db_reverse_remove(attr, objects, item):
        for obj in objects:
            setdata = obj._vals_[attr.name]
            setdata.remove(item)
    def get_m2m_columns(attr, is_reverse=False):
        entity = attr.entity
        if attr.symmetric:
            if attr._columns_checked:
                if not is_reverse: return attr.columns
                else: return attr.reverse_columns
            if attr.columns:
                if len(attr.columns) != len(entity._get_pk_columns_()): throw(MappingError,
                    'Invalid number of columns for %s' % attr.reverse)
            else:
                provider = attr.entity._database_.provider
                attr.columns = provider.get_default_m2m_column_names(entity)
            attr.converters = entity._pk_converters_
            if not attr.reverse_columns:
                attr.reverse_columns = [ column + '_2' for column in attr.columns ]
            attr._columns_checked = True
            if not is_reverse: return attr.columns
            else: return attr.reverse_columns

        reverse = attr.reverse
        if attr._columns_checked: return attr.reverse.columns
        elif reverse.columns:
            if len(reverse.columns) != len(entity._get_pk_columns_()): throw(MappingError,
                'Invalid number of columns for %s' % reverse)
        else:
            provider = attr.entity._database_.provider
            reverse.columns = provider.get_default_m2m_column_names(entity)
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
            table_name = attr.table
            assert table_name is not None
            where_list = [ 'WHERE' ]
            if attr.symmetric:
                columns = attr.columns + attr.reverse_columns
                converters = attr.converters + attr.converters
            else:
                columns = reverse.columns + attr.columns
                converters = reverse.converters + attr.converters
            for i, (column, converter) in enumerate(zip(columns, converters)):
                where_list.append([ 'EQ', ['COLUMN', None, column], [ 'PARAM', i, converter ] ])
            sql_ast = [ 'DELETE', table_name, where_list ]
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
            table_name = attr.table
            assert table_name is not None
            if attr.symmetric:
                columns = attr.columns + attr.reverse_columns
                converters = attr.converters + attr.converters
            else:
                columns = reverse.columns + attr.columns
                converters = reverse.converters + attr.converters
            params = [ [ 'PARAM', i, converter ] for i, converter in enumerate(converters) ]
            sql_ast = [ 'INSERT', table_name, columns, params ]
            sql, adapter = database._ast2sql(sql_ast)
            attr.cached_add_m2m_sql = sql, adapter
        else: sql, adapter = cached_sql
        arguments_list = [ adapter(obj._get_raw_pkval_() + robj._get_raw_pkval_())
                           for obj, robj in added ]
        database._exec_sql(sql, arguments_list)

def unpickle_setwrapper(obj, attrname, items):
    attr = getattr(obj.__class__, attrname)
    wrapper_cls = attr.py_type._get_set_wrapper_subclass_()
    wrapper = wrapper_cls(obj, attr)
    setdata = obj._vals_.get(attr.name, NOT_LOADED)
    if setdata is NOT_LOADED: setdata = obj._vals_[attr.name] = SetData()
    setdata.is_fully_loaded = True
    return wrapper

class SetWrapper(object):
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
        if not wrapper._obj_._cache_.is_alive: throw_db_session_is_over(wrapper._obj_)
        return wrapper._attr_.copy(wrapper._obj_)
    @cut_traceback
    def __repr__(wrapper):
        return '<%s %r.%s>' % (wrapper.__class__.__name__, wrapper._obj_, wrapper._attr_.name)
    @cut_traceback
    def __str__(wrapper):
        if not wrapper._obj_._cache_.is_alive: content = '-'
        else: content = ', '.join(imap(str, wrapper))
        return '%s([%s])' % (wrapper.__class__.__name__, content)
    @cut_traceback
    def __nonzero__(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        if not obj._cache_.is_alive: throw_db_session_is_over(obj)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        setdata = obj._vals_.get(attr.name, NOT_LOADED)
        if setdata is NOT_LOADED: setdata = attr.load(obj)
        if setdata: return True
        if not setdata.is_fully_loaded: setdata = attr.load(obj)
        return bool(setdata)
    @cut_traceback
    def __len__(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        if not obj._cache_.is_alive: throw_db_session_is_over(obj)
        setdata = obj._vals_.get(attr.name, NOT_LOADED)
        if setdata is NOT_LOADED or not setdata.is_fully_loaded: setdata = attr.load(obj)
        return len(setdata)
    @cut_traceback
    def __iter__(wrapper):
        return iter(wrapper.copy())
    @cut_traceback
    def __eq__(wrapper, other):
        if isinstance(other, SetWrapper):
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
        obj = wrapper._obj_
        if not obj._cache_.is_alive: throw_db_session_is_over(obj)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        attr = wrapper._attr_
        if not isinstance(item, attr.py_type): return False
        reverse = attr.reverse
        if not reverse.is_collection:
            obj2 = item._vals_.get(reverse.name, NOT_LOADED)
            if obj2 is NOT_LOADED: obj2 = reverse.load(item)
            bit = item._bits_[reverse]

            wbits = item._wbits_
            if wbits is not None and not wbits & bit: item._rbits_ |= bit

            return obj is obj2
        setdata = obj._vals_.get(attr.name, NOT_LOADED)
        if setdata is not NOT_LOADED:
            if item in setdata: return True
            if setdata.is_fully_loaded: return False
        setdata = attr.load(obj, (item,))
        return item in setdata
    @cut_traceback
    def create(wrapper, **kwargs):
        attr = wrapper._attr_
        item_type = attr.py_type
        item = item_type(**kwargs)
        wrapper.add(item)
        return item
    @cut_traceback
    def add(wrapper, new_items):
        obj = wrapper._obj_
        if not obj._cache_.is_alive: throw_db_session_is_over(obj)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        attr = wrapper._attr_
        reverse = attr.reverse
        if not reverse: throw(NotImplementedError)
        new_items = attr.check(new_items, obj)
        setdata = obj._vals_.get(attr.name, NOT_LOADED)
        if setdata is not NOT_LOADED:
            new_items.difference_update(setdata)
        if setdata is NOT_LOADED or not setdata.is_fully_loaded:
            setdata = attr.load(obj, new_items)
        new_items.difference_update(setdata)
        undo_funcs = []
        try:
            if not reverse.is_collection:
                  for item in new_items - setdata: reverse.__set__(item, obj, undo_funcs)
            else: reverse.reverse_add(new_items - setdata, obj, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
            raise
        setdata.update(new_items)
        if setdata.added is EMPTY: setdata.added = new_items
        else: setdata.added.update(new_items)
        if setdata.removed is not EMPTY: setdata.removed -= new_items
        cache = obj._cache_
        cache.modified = True
        cache.modified_collections.setdefault(attr, set()).add(obj)
    @cut_traceback
    def __iadd__(wrapper, items):
        wrapper.add(items)
        return wrapper
    @cut_traceback
    def remove(wrapper, items):
        obj = wrapper._obj_
        if not obj._cache_.is_alive: throw_db_session_is_over(obj)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        attr = wrapper._attr_
        reverse = attr.reverse
        if not reverse: throw(NotImplementedError)
        items = attr.check(items, obj)
        setdata = obj._vals_.get(attr.name, NOT_LOADED)
        if setdata is not NOT_LOADED:
            items.difference_update(setdata.removed)
        if not items: return
        if setdata is NOT_LOADED or not setdata.is_fully_loaded:
            setdata = attr.load(obj, items)
        undo_funcs = []
        try:
            if not reverse.is_collection:
                for item in (items & setdata): reverse.__set__(item, None, undo_funcs)
            else: reverse.reverse_remove(items & setdata, obj, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
            raise
        setdata -= items
        if setdata.added is not EMPTY: setdata.added -= items
        if setdata.removed is EMPTY: setdata.removed = items
        else: setdata.removed.update(items)
        cache = obj._cache_
        cache.modified = True
        cache.modified_collections.setdefault(attr, set()).add(obj)
    @cut_traceback
    def __isub__(wrapper, items):
        wrapper.remove(items)
        return wrapper
    @cut_traceback
    def clear(wrapper):
        obj = wrapper._obj_
        if not obj._cache_.is_alive: throw_db_session_is_over(obj)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        wrapper._attr_.__set__(obj, None)

def iter2dict(iter):
    d = {}
    for item in iter:
        d[item] = d.get(item, 0) + 1
    return d

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
        else: multiset._items_ = iter2dict(items)
    def __reduce__(multiset):
        return unpickle_multiset, (multiset._obj_, multiset._attrnames_, multiset._items_)
    @cut_traceback
    def distinct(multiset):
        if not multiset._obj_._cache_.is_alive: throw_db_session_is_over(multiset._obj_)
        return multiset._items_.copy()
    @cut_traceback
    def __repr__(multiset):
        if multiset._obj_._cache_.is_alive:
            size = _sum(multiset._items_.itervalues())
            if size == 1: size_str = ' (1 item)'
            else: size_str = ' (%d items)' % size
        else: size_str = ''
        return '<%s %s.%s%s>' % (multiset.__class__.__name__, multiset._obj_,
                                 '.'.join(multiset._attrnames_), size_str)
    @cut_traceback
    def __str__(multiset):
        if not multiset._obj_._cache_.is_alive: throw_db_session_is_over(multiset._obj_)
        return '%s(%s)' % (multiset.__class__.__name__, str(multiset._items_))
    @cut_traceback
    def __nonzero__(multiset):
        if not multiset._obj_._cache_.is_alive: throw_db_session_is_over(multiset._obj_)
        return bool(multiset._items_)
    @cut_traceback
    def __len__(multiset):
        if not multiset._obj_._cache_.is_alive: throw_db_session_is_over(multiset._obj_)
        return _sum(multiset._items_.values())
    @cut_traceback
    def __iter__(multiset):
        if not multiset._obj_._cache_.is_alive: throw_db_session_is_over(multiset._obj_)
        for item, cnt in multiset._items_.iteritems():
            for i in range(cnt): yield item
    @cut_traceback
    def __eq__(multiset, other):
        if not multiset._obj_._cache_.is_alive: throw_db_session_is_over(multiset._obj_)
        if isinstance(other, Multiset):
            return multiset._items_ == other._items_
        if isinstance(other, dict):
            return multiset._items_ == other
        if hasattr(other, 'keys'):
            return multiset._items_ == dict(other)
        return multiset._items_ == iter2dict(other)
    @cut_traceback
    def __ne__(multiset, other):
        return not multiset.__eq__(other)
    @cut_traceback
    def __contains__(multiset, item):
        if not multiset._obj_._cache_.is_alive: throw_db_session_is_over(multiset._obj_)
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

next_entity_id = _count(1).next
next_new_instance_id = _count(1).next

select_re = re.compile(r'select\b', re.IGNORECASE)
lambda_re = re.compile(r'lambda\b')

class EntityMeta(type):
    def __setattr__(entity, name, val):
        if name.startswith('_') and name.endswith('_'):
            type.__setattr__(entity, name, val)
        else: throw(NotImplementedError)
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

        databases = set()
        for base_class in bases:
            if isinstance(base_class, EntityMeta):
                database = base_class._database_
                if database is None: throw(ERDiagramError, 'Base Entity does not belong to any database')
                databases.add(database)
        if not databases: assert False
        elif len(databases) > 1: throw(ERDiagramError,
            'With multiple inheritance of entities, all entities must belong to the same database')
        database = databases.pop()

        if entity.__name__ in database.entities:
            throw(ERDiagramError, 'Entity %s already exists' % entity.__name__)
        assert entity.__name__ not in database.__dict__

        entity._id_ = next_entity_id()
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
            roots = set(base._root_ for base in direct_bases)
            if len(roots) > 1: throw(ERDiagramError,
                'With multiple inheritance of entities, inheritance graph must be diamond-like')
            root = entity._root_ = roots.pop()
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
        for name, attr in entity.__dict__.items():
            if name in base_attrs_dict: throw(ERDiagramError, "Name '%s' hides base attribute %s" % (name,base_attrs_dict[name]))
            if not isinstance(attr, Attribute): continue
            if name.startswith('_') and name.endswith('_'): throw(ERDiagramError,
                'Attribute name cannot both start and end with underscore. Got: %s' % name)
            if attr.entity is not None: throw(ERDiagramError,
                'Duplicate use of attribute %s in entity %s' % (attr, entity.__name__))
            attr._init_(entity, name)
            new_attrs.append(attr)
        new_attrs.sort(key=attrgetter('id'))

        keys = entity.__dict__.get('_keys_', {})
        for attr in new_attrs:
            if attr.is_unique: keys[(attr,)] = isinstance(attr, PrimaryKey)
        for key, is_pk in keys.items():
            for attr in key:
                if attr.entity is not entity: throw(ERDiagramError,
                    'Invalid use of attribute %s in entity %s' % (attr, entity.__name__))
                if attr.is_collection or attr.is_discriminator or (is_pk and not attr.is_required and not attr.auto):
                    throw(TypeError, '%s attribute %s cannot be part of %s'
                                    % (attr.__class__.__name__, attr, is_pk and 'primary key' or 'unique index'))
                if isinstance(attr.py_type, type) and issubclass(attr.py_type, float):
                    throw(TypeError, 'Attribute %s of type float cannot be part of %s'
                                    % (attr, is_pk and 'primary key' or 'unique index'))
                if not attr.is_required:
                    if attr.nullable is False:
                        throw(TypeError, 'Optional attribute %s must be nullable, because it is part of composite key' % attr)
                    attr.nullable = True

        primary_keys = set(key for key, is_pk in keys.items() if is_pk)
        if direct_bases:
            if primary_keys: throw(ERDiagramError, 'Primary key cannot be redefined in derived classes')
            for base in direct_bases:
                keys[base._pk_attrs_] = True
                for key in base._keys_: keys[key] = False
            primary_keys = set(key for key, is_pk in keys.items() if is_pk)

        if len(primary_keys) > 1: throw(ERDiagramError, 'Only one primary key can be defined in each entity class')
        elif not primary_keys:
            if hasattr(entity, 'id'): throw(ERDiagramError,
                "Cannot create primary key for %s automatically because name 'id' is alredy in use" % entity.__name__)
            attr = PrimaryKey(int, auto=True)
            attr._init_(entity, 'id')
            type.__setattr__(entity, 'id', attr)  # entity.id = attr
            new_attrs.insert(0, attr)
            pk_attrs = (attr,)
            keys[pk_attrs] = True
        else: pk_attrs = primary_keys.pop()
        for i, attr in enumerate(pk_attrs): attr.pk_offset = i
        entity._pk_columns_ = None
        entity._pk_attrs_ = pk_attrs
        entity._pk_is_composite_ = len(pk_attrs) > 1
        entity._pk_ = len(pk_attrs) > 1 and pk_attrs or pk_attrs[0]
        entity._keys_ = [ key for key, is_pk in keys.items() if not is_pk ]
        entity._simple_keys_ = [ key[0] for key in entity._keys_ if len(key) == 1 ]
        entity._composite_keys_ = [ key for key in entity._keys_ if len(key) > 1 ]

        entity._new_attrs_ = new_attrs
        entity._attrs_ = base_attrs + new_attrs
        entity._adict_ = dict((attr.name, attr) for attr in entity._attrs_)
        entity._subclass_attrs_ = set()
        for base in entity._all_bases_:
            base._subclass_attrs_.update(new_attrs)

        entity._bits_ = {}
        next_offset = _count().next
        all_bits = 0
        for attr in entity._attrs_:
            if attr.is_collection or attr.is_discriminator or attr.pk_offset is not None: continue
            next_bit = 1 << next_offset()
            entity._bits_[attr] = next_bit
            all_bits |= next_bit
        entity._all_bits_ = all_bits

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

        entity._database_ = database
        database.entities[entity.__name__] = entity
        setattr(database, entity.__name__, entity)
        entity._link_reverse_attrs_()

        entity._cached_create_sql_ = None
        entity._cached_create_sql_auto_pk_ = None
        entity._cached_delete_sql_ = None
        entity._find_sql_cache_ = {}
        entity._batchload_sql_cache_ = {}
        entity._update_sql_cache_ = {}
        entity._lock_sql_cache_ = {}

        entity._propagation_mixin_ = None
        entity._set_wrapper_subclass_ = None
        entity._multiset_subclass_ = None

        if '_discriminator_' not in entity.__dict__:
            entity._discriminator_ = None
        if entity._discriminator_ and not entity._discriminator_attr_:
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
    def _link_reverse_attrs_(entity):
        database = entity._database_
        unmapped_attrs = database._unmapped_attrs.pop(entity.__name__, set())
        for attr in entity._new_attrs_:
            py_type = attr.py_type
            if isinstance(py_type, basestring):
                entity2 = database.entities.get(py_type)
                if entity2 is None:
                    database._unmapped_attrs.setdefault(py_type, set()).add(attr)
                    continue
                attr.py_type = py_type = entity2
            elif not issubclass(py_type, Entity): continue

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
                msg = 'Ambiguous reverse attribute for %s'
                if len(candidates1) > 1: throw(ERDiagramError, msg % attr)
                elif len(candidates1) == 1: attr2 = candidates1[0]
                elif len(candidates2) > 1: throw(ERDiagramError, msg % attr)
                elif len(candidates2) == 1: attr2 = candidates2[0]
                else: throw(ERDiagramError, 'Reverse attribute for %s not found' % attr)

            type2 = attr2.py_type
            msg = 'Inconsistent reverse attributes %s and %s'
            if isinstance(type2, basestring):
                if type2 != entity.__name__: throw(ERDiagramError, msg % (attr, attr2))
                attr2.py_type = entity
            elif type2 != entity: throw(ERDiagramError, msg % (attr, attr2))
            reverse2 = attr2.reverse
            if reverse2 not in (None, attr, attr.name): throw(ERDiagramError, msg % (attr,attr2))

            if attr.is_required and attr2.is_required: throw(ERDiagramError,
                "At least one attribute of one-to-one relationship %s - %s must be optional" % (attr, attr2))

            attr.reverse = attr2
            attr2.reverse = attr
            attr.linked()
            attr2.linked()
            unmapped_attrs.discard(attr2)
        for attr in unmapped_attrs:
            throw(ERDiagramError, 'Reverse attribute for %s.%s was not found' % (attr.entity.__name__, attr.name))
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
            for name in ifilterfalse(entity._adict_.__contains__, kwargs):
                throw(TypeError, 'Unknown attribute %r' % name)
            for attr in entity._attrs_:
                val = kwargs.get(attr.name, DEFAULT)
                avdict[attr] = attr.check(val, None, entity, from_db=False)
        else:
            get = entity._adict_.get
            for name, val in kwargs.items():
                attr = get(name)
                if attr is None: throw(TypeError, 'Unknown attribute %r' % name)
                avdict[attr] = attr.check(val, None, entity, from_db=False)
        pk = entity.__dict__['_pk_']
        if entity._pk_is_composite_:
            pkval = map(avdict.get, pk)
            if None in pkval: pkval = None
            else: pkval = tuple(pkval)
        else: pkval = avdict.get(pk)
        return pkval, avdict
    @cut_traceback
    def __getitem__(entity, key):
        if type(key) is not tuple: key = (key,)
        if len(key) != len(entity._pk_attrs_):
            throw(TypeError, 'Invalid count of attrs in %s primary key (%s instead of %s)'
                             % (entity.__name__, len(key), len(entity._pk_attrs_)))
        kwargs = dict(izip(imap(attrgetter('name'), entity._pk_attrs_), key))
        objects = entity._find_(1, kwargs)
        if not objects: throw(ObjectNotFound, entity, key)
        assert len(objects) == 1
        return objects[0]
    @cut_traceback
    def get(entity, *args, **kwargs):
        if args:
            if len(args) > 1: throw(TypeError, 'Only one positional argument expected')
            if kwargs: throw(TypeError, 'If positional argument presented, no keyword arguments expected')
            first_arg = args[0]
            if not (isinstance(first_arg, types.FunctionType)
                    or isinstance(first_arg, basestring) and lambda_re.match(first_arg)):
                throw(TypeError, 'Positional argument must be lambda function or its text source. '
                                 'Got: %s.get(%r)' % (entity.__name__, first_arg))

            globals = sys._getframe(2).f_globals
            locals = sys._getframe(2).f_locals
            return entity._query_from_lambda_(first_arg, globals, locals).get()

        objects = entity._find_(1, kwargs)  # can throw MultipleObjectsFoundError
        if not objects: return None
        assert len(objects) == 1
        return objects[0]
    @cut_traceback
    def get_by_sql(entity, sql, globals=None, locals=None):
        objects = entity._find_by_sql_(1, sql, globals, locals, 2)  # can throw MultipleObjectsFoundError
        if not objects: return None
        assert len(objects) == 1
        return objects[0]
    @cut_traceback
    def select(entity, func=None):
        if func is None:
            return Query(entity._default_iter_name_, entity._default_genexpr_, {}, { '.0' : entity })
        if not (isinstance(func, types.FunctionType)
                or isinstance(func, basestring) and lambda_re.match(func)):
            throw(TypeError, 'Lambda function or its text representation expected. Got: %r' % func)
        elif not isinstance(func, types.FunctionType): throw(TypeError)
        globals = sys._getframe(2).f_globals
        locals = sys._getframe(2).f_locals
        return entity._query_from_lambda_(func, globals, locals)
    @cut_traceback
    def select_by_sql(entity, sql, globals=None, locals=None):
        return entity._find_by_sql_(None, sql, globals, locals, 2)
    @cut_traceback
    def order_by(entity, *args):
        query = Query(entity._default_iter_name_, entity._default_genexpr_, {}, { '.0' : entity })
        return query.order_by(*args)
    def _find_(entity, max_fetch_count, kwargs):
        if entity._database_.schema is None:
            throw(ERDiagramError, 'Mapping is not generated for entity %r' % entity.__name__)

        pkval, avdict = entity._normalize_args_(kwargs, False)
        rbits = 0
        for attr in avdict:
            if attr.is_collection: throw(TypeError,
                'Collection attribute %s.%s cannot be specified as search criteria' % (attr.entity.__name__, attr.name))
            bit = entity._bits_.get(attr)
            if bit is not None: rbits |= bit
        try:
            objects = entity._find_in_cache_(pkval, avdict)
        except KeyError:  # not found in cache, can exist in db
            objects = entity._find_in_db_(avdict, max_fetch_count)
        if rbits:
            for obj in objects:
                if obj._rbits_ is not None: obj._rbits_ |= rbits
        return objects
    def _find_in_cache_(entity, pkval, avdict):
        cache = entity._get_cache_()
        obj = None
        if pkval is not None:
            index = cache.indexes.get(entity.__dict__['_pk_'])
            if index is not None: obj = index.get(pkval)
        if obj is None:
            for attr in ifilter(avdict.__contains__, entity._simple_keys_):
                index = cache.indexes.get(attr)
                if index is None: continue
                val = avdict[attr]
                obj = index.get(val)
                if obj is not None: break
        if obj is None:
            NOT_FOUND = object()
            for attrs in entity._composite_keys_:
                vals = tuple(avdict.get(attr, NOT_FOUND) for attr in attrs)
                if NOT_FOUND in vals: continue
                index = cache.indexes.get(attrs)
                if index is None: continue
                obj = index.get(vals)
                if obj is not None: break
        if obj is None:
            for attr, val in avdict.iteritems():
                if val is None: continue
                reverse = attr.reverse
                if reverse and not reverse.is_collection:
                    obj = reverse.__get__(val)
                    break
        if obj is None:
            for attr, val in avdict.iteritems():
                if isinstance(val, Entity) and val._pkval_ is None:
                    reverse = attr.reverse
                    if not reverse.is_collection:
                        obj = reverse.__get__(val)
                        if obj is None: return []
                    elif isinstance(reverse, Set):
                        filtered_objects = []
                        for obj in reverse.__get__(val):
                            for attr, val in avdict.iteritems():
                                if val != attr.get(obj): break
                            else: filtered_objects.append(obj)
                        filtered_objects.sort(key=entity._get_raw_pkval_)
                        return filtered_objects
                    else: throw(NotImplementedError)
        if obj is not None:
            if obj._status_ == 'deleted': return []
            for attr, val in avdict.iteritems():
                if val != attr.__get__(obj):
                    return []
            return [ obj ]
        throw(KeyError)  # not found in cache, can exist in db
    def _find_in_db_(entity, avdict, max_fetch_count=None):
        if max_fetch_count is None: max_fetch_count = options.MAX_FETCH_COUNT
        database = entity._database_
        query_attrs = tuple((attr, value is None) for attr, value in sorted(avdict.iteritems()))
        single_row = (max_fetch_count == 1)
        sql, adapter, attr_offsets = entity._construct_sql_(query_attrs, order_by_pk=not single_row)
        arguments = adapter(avdict)
        cursor = database._exec_sql(sql, arguments)
        objects = entity._fetch_objects(cursor, attr_offsets, max_fetch_count)
        return objects
    def _find_by_sql_(entity, max_fetch_count, sql, globals, locals, frame_depth):
        if not isinstance(sql, basestring): throw(TypeError)
        database = entity._database_
        cursor = database._exec_raw_sql(sql, globals, locals, frame_depth+1)

        col_names = [ column_info[0].upper() for column_info in cursor.description ]
        attr_offsets = {}
        used_columns = set()
        for attr in entity._attrs_:
            if attr.is_collection: continue
            if not attr.columns: continue
            offsets = []
            for column in attr.columns:
                try: offset = col_names.index(column.upper())
                except ValueError: break
                offsets.append(offset)
                used_columns.add(offset)
            else: attr_offsets[attr] = offsets
        if len(used_columns) < len(col_names):
            for i in range(len(col_names)):
                if i not in used_columns: throw(NameError,
                    'Column %s does not belong to entity %s' % (cursor.description[i][0], entity.__name__))
        for attr in entity._pk_attrs_:
            if attr not in attr_offsets: throw(ValueError,
                'Primary key attribue %s was not found in query result set' % attr)

        objects = entity._fetch_objects(cursor, attr_offsets, max_fetch_count)
        return objects
    def _construct_select_clause_(entity, alias=None, distinct=False):
        attr_offsets = {}
        select_list = distinct and [ 'DISTINCT' ] or [ 'ALL' ]
        root = entity._root_
        for attr in chain(root._attrs_, root._subclass_attrs_):
            if attr.is_collection: continue
            if not attr.columns: continue
            if attr.lazy: continue
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
    def _construct_batchload_sql_(entity, batch_size, attr=None):
        query_key = batch_size, attr
        cached_sql = entity._batchload_sql_cache_.get(query_key)
        if cached_sql is not None: return cached_sql
        table_name = entity._table_
        select_list, attr_offsets = entity._construct_select_clause_()
        from_list = [ 'FROM', [ None, 'TABLE', table_name ]]
        if attr is None:
            columns = entity._pk_columns_
            converters = entity._pk_converters_
        else:
            columns = attr.columns
            converters = attr.converters
        row_value_syntax = entity._database_.provider.translator_cls.row_value_syntax
        criteria_list = construct_criteria_list(None, columns, converters, row_value_syntax, batch_size)
        discr_criteria = entity._construct_discriminator_criteria_()
        if discr_criteria: criteria_list.insert(0, discr_criteria)
        sql_ast = [ 'SELECT', select_list, from_list, [ 'WHERE' ] + criteria_list ]
        database = entity._database_
        sql, adapter = database._ast2sql(sql_ast)
        cached_sql = sql, adapter, attr_offsets
        entity._batchload_sql_cache_[query_key] = cached_sql
        return cached_sql
    def _construct_sql_(entity, query_attrs, order_by_pk=False):
        query_key = query_attrs, order_by_pk
        cached_sql = entity._find_sql_cache_.get(query_key)
        if cached_sql is not None: return cached_sql
        table_name = entity._table_
        select_list, attr_offsets = entity._construct_select_clause_()
        from_list = [ 'FROM', [ None, 'TABLE', table_name ]]
        where_list = [ 'WHERE' ]
        values = []

        discr_attr = entity._discriminator_attr_
        if discr_attr and (discr_attr, False) not in query_attrs:
            discr_criteria = entity._construct_discriminator_criteria_()
            if discr_criteria: where_list.append(discr_criteria)

        for attr, attr_is_none in query_attrs:
            if not attr.reverse:
                if attr_is_none: where_list.append([ 'IS_NULL', [ 'COLUMN', None, attr.column ] ])
                else:
                    if len(attr.converters) > 1: throw(NotImplementedError)
                    where_list.append([ 'EQ', [ 'COLUMN', None, attr.column ], [ 'PARAM', attr, attr.converters[0] ] ])
            elif not attr.columns: throw(NotImplementedError)
            else:
                attr_entity = attr.py_type; assert attr_entity == attr.reverse.entity
                if attr_is_none:
                    for column in attr.columns:
                        where_list.append([ 'IS_NULL', [ 'COLUMN', None, column ] ])
                else:
                    for i, (column, converter) in enumerate(zip(attr.columns, attr_entity._pk_converters_)):
                        where_list.append([ 'EQ', [ 'COLUMN', None, column ], [ 'PARAM', (attr, i), converter ] ])

        sql_ast = [ 'SELECT', select_list, from_list, where_list ]
        if order_by_pk: sql_ast.append([ 'ORDER_BY' ] + [ [ 'COLUMN', None, column ] for column in entity._pk_columns_ ])
        database = entity._database_
        sql, adapter = database._ast2sql(sql_ast)
        cached_sql = sql, adapter, attr_offsets
        entity._find_sql_cache_[query_key] = cached_sql
        return cached_sql
    def _fetch_objects(entity, cursor, attr_offsets, max_fetch_count=None, rbits=None):
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
            objects = [ entity._get_by_raw_pkval_(row) for row in rows ]
            entity._load_many_(objects)
        else:
            for row in rows:
                real_entity_subclass, pkval, avdict = entity._parse_row_(row, attr_offsets)
                obj = real_entity_subclass._new_(pkval, 'loaded')
                if obj._status_ in del_statuses: continue
                obj._db_set_(avdict)
                objects.append(obj)
        if rbits is not None:
            for obj in objects: obj._rbits_ |= rbits
        return objects
    def _parse_row_(entity, row, attr_offsets):
        discr_attr = entity._discriminator_attr_
        if not discr_attr: real_entity_subclass = entity
        else:
            discr_offset = attr_offsets[discr_attr][0]
            discr_value = discr_attr.check(row[discr_offset], None, entity, from_db=True)
            real_entity_subclass = discr_attr.code2cls[discr_value]

        avdict = {}
        for attr in real_entity_subclass._attrs_:
            offsets = attr_offsets.get(attr)
            if offsets is None or attr.is_discriminator: continue
            avdict[attr] = attr.parse_value(row, offsets)
        pk = entity.__dict__['_pk_']
        if not entity._pk_is_composite_: pkval = avdict.pop(pk, None)
        else: pkval = tuple(avdict.pop(attr, None) for attr in pk)
        return real_entity_subclass, pkval, avdict
    def _load_many_(entity, objects):
        database = entity._database_
        cache = database._get_cache()
        pk = entity.__dict__['_pk_']
        seeds = cache.seeds.get(pk)
        if not seeds: return
        objects = set(obj for obj in objects if obj in seeds)
        objects = sorted(objects, key=attrgetter('_pkval_'))
        max_batch_size = database.provider.max_params_count // len(entity._pk_columns_)
        while objects:
            batch = objects[:max_batch_size]
            objects = objects[max_batch_size:]
            sql, adapter, attr_offsets = entity._construct_batchload_sql_(len(batch))
            value_dict = dict(enumerate(batch))
            arguments = adapter(value_dict)
            cursor = database._exec_sql(sql, arguments)
            result = entity._fetch_objects(cursor, attr_offsets)
            if len(result) < len(batch):
                for obj in result:
                    if obj not in batch: throw(UnrepeatableReadError, '%s disappeared' % obj)
    def _query_from_lambda_(entity, lambda_func, globals, locals):
        if type(lambda_func) is types.FunctionType:
            names, argsname, keyargsname, defaults = inspect.getargspec(lambda_func)
            if len(names) != 1: throw(TypeError,
                'Lambda query requires exactly one parameter name, like %s.select(lambda %s: ...). '
                'Got: %d parameters' % (entity.__name__, entity.__name__[0].lower(), len(names)))
            if argsname or keyargsname: throw(TypeError)
            if defaults: throw(TypeError)
            code_key = id(lambda_func.func_code)
            name = names[0]
            cond_expr, external_names = decompile(lambda_func)
        elif isinstance(lambda_func, basestring):
            lambda_text = lambda_func
            lambda_expr = string2ast(lambda_text)
            if not isinstance(lambda_expr, ast.Lambda): throw(TypeError)
            if len(lambda_expr.argnames) != 1: throw(TypeError)
            if lambda_expr.varargs: throw(TypeError)
            if lambda_expr.kwargs: throw(TypeError)
            if lambda_expr.defaults: throw(TypeError)
            code_key = lambda_text
            name = lambda_expr.argnames[0]
            cond_expr = lambda_expr.code
        else: assert False

        if_expr = ast.GenExprIf(cond_expr)
        for_expr = ast.GenExprFor(ast.AssName(name, 'OP_ASSIGN'), ast.Name('.0'), [ if_expr ])
        inner_expr = ast.GenExprInner(ast.Name(name), [ for_expr ])
        locals = locals.copy()
        assert '.0' not in locals
        locals['.0'] = entity
        return Query(code_key, inner_expr, globals, locals)
    def _get_cache_(entity):
        database = entity._database_
        if database is None: throw(TransactionError)
        return database._get_cache()
    def _new_(entity, pkval, status, undo_funcs=None):
        cache = entity._get_cache_()
        pk = entity.__dict__['_pk_']
        index = cache.indexes.setdefault(pk, {})
        if pkval is None: obj = None
        else: obj = index.get(pkval)
        if obj is None: pass
        elif status == 'created':
            if entity._pk_is_composite_: pkval = ', '.join(str(item) for item in pkval)
            throw(CacheIndexError, 'Cannot create %s: instance with primary key %s already exists'
                             % (obj.__class__.__name__, pkval))
        elif obj.__class__ is entity: return obj
        elif issubclass(obj.__class__, entity): return obj
        elif not issubclass(entity, obj.__class__): throw(TransactionError,
            'Unexpected class change from %s to %s for object with primary key %r' %
            (obj.__class__, entity, obj._pkval_))
        elif obj._rbits_ or obj._wbits_: throw(NotImplementedError)
        else:
            obj.__class__ = entity
            return obj
        obj = object.__new__(entity)
        obj._dbvals_ = {}
        obj._vals_ = {}
        obj._cache_ = cache
        obj._status_ = status
        obj._pkval_ = pkval
        if pkval is not None:
            index[pkval] = obj
            obj._newid_ = None
        else: obj._newid_ = next_new_instance_id()
        if obj._pk_is_composite_: pairs = zip(pk, pkval)
        else: pairs = ((pk, pkval),)
        if status == 'loaded':
            assert undo_funcs is None
            obj._rbits_ = obj._wbits_ = 0
            for attr, val in pairs:
                obj._vals_[attr.name] = val
                if attr.reverse: attr.db_update_reverse(obj, NOT_LOADED, val)
            seeds = cache.seeds.setdefault(pk, set())
            seeds.add(obj)
        elif status == 'created':
            assert undo_funcs is not None
            obj._rbits_ = obj._wbits_ = None
            for attr, val in pairs:
                obj._vals_[attr.name] = val
                if attr.reverse: attr.update_reverse(obj, NOT_LOADED, val, undo_funcs)
        else: assert False
        return obj
    def _get_by_raw_pkval_(entity, raw_pkval):
        i = 0
        pkval = []
        for attr in entity._pk_attrs_:
            if attr.column is not None:
                val = raw_pkval[i]
                i += 1
                if not attr.reverse: val = attr.check(val, None, entity, from_db=True)
                else: val = attr.py_type._get_by_raw_pkval_((val,))
            else:
                if not attr.reverse: throw(NotImplementedError)
                vals = raw_pkval[i:i+len(attr.columns)]
                val = attr.py_type._get_by_raw_pkval_(vals)
                i += len(attr.columns)
            pkval.append(val)
        if not entity._pk_is_composite_: pkval = pkval[0]
        else: pkval = tuple(pkval)
        obj = entity._new_(pkval, 'loaded')
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
                    items = [ attr.__get__(item) for item in wrapper ]
                    return Multiset(wrapper._obj_, attrnames, items)
            elif not attr.is_collection:
                def fget(wrapper, attr=attr):
                    attrnames = wrapper._attrnames_ + (attr.name,)
                    items = [ attr.__get__(item) for item in wrapper ]
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
            result_cls = type(cls_name, (SetWrapper, mixin), {})
            entity._set_wrapper_subclass_ = result_cls
        return result_cls
    def describe(entity):
        result = []
        parents = ','.join(cls.__name__ for cls in entity.__bases__)
        result.append('class %s(%s):' % (entity.__name__, parents))
        if entity._base_attrs_:
            result.append('# inherited attrs')
            result.extend(attr.describe() for attr in entity._base_attrs_)
            result.append('# attrs introduced in %s' % entity.__name__)
        result.extend(attr.describe() for attr in entity._new_attrs_)
        return '\n    '.join(result)

def populate_criteria_list(criteria_list, columns, converters, params_count=0, table_alias=None):
    assert len(columns) == len(converters)
    for column, converter in zip(columns, converters):
        if converter is not None:
            criteria_list.append([ 'EQ', [ 'COLUMN', table_alias, column ], [ 'PARAM', params_count, converter ] ])
        else:
            criteria_list.append([ 'IS_NULL', [ 'COLUMN', None, column ] ])
        params_count += 1
    return params_count

statuses = set(['created', 'loaded', 'updated', 'deleted', 'cancelled', 'saved', 'locked'])
del_statuses = set(['deleted', 'cancelled'])
created_or_deleted_statuses = set(['created']) | del_statuses

def throw_object_was_deleted(obj):
    throw(OperationWithDeletedObjectError, '%s was deleted' % obj)

def throw_db_session_is_over(obj):
    throw(TransactionRolledBack, 'Object %s cannot be used after the database session is over' % obj)

def unpickle_entity(d):
    entity = d.pop('__class__')
    cache = entity._get_cache_()
    pk = entity.__dict__['_pk_']
    if not entity._pk_is_composite_: pkval = d.get(pk.name)
    else: pkval = tuple(d[attr.name] for attr in entity._pk_attrs_)
    assert pkval is not None
    obj = entity._new_(pkval, 'loaded')
    if obj._status_ in del_statuses: return obj
    avdict = {}
    for attrname, val in d.iteritems():
        attr = entity._adict_[attrname]
        if attr.pk_offset is not None: continue
        avdict[attr] = val
    obj._db_set_(avdict, unpickling=True)
    return obj

class Entity(object):
    __metaclass__ = EntityMeta
    __slots__ = '_cache_', '_status_', '_pkval_', '_newid_', '_dbvals_', '_vals_', '_rbits_', '_wbits_', '__weakref__'
    def __reduce__(obj):
        if obj._status_ in del_statuses: throw(
            OperationWithDeletedObjectError, 'Deleted object %s cannot be pickled' % obj)
        if obj._status_ in ('created', 'updated'): throw(
            OrmError, '%s object %s has to be stored in DB before it can be pickled'
                      % (obj._status_.capitalize(), obj))
        d = {'__class__' : obj.__class__}
        adict = obj._adict_
        for attrname, val in obj._vals_.iteritems():
            attr = adict[attrname]
            if not attr.is_collection: d[attrname] = val
        return unpickle_entity, (d,)
    @cut_traceback
    def __new__(entity, *args, **kwargs):
        if args: raise TypeError('%s constructor accept only keyword arguments. Got: %d positional argument%s'
                                 % (entity.__name__, len(args), len(args) > 1 and 's' or ''))
        if entity._database_.schema is None:
            throw(ERDiagramError, 'Mapping is not generated for entity %r' % entity.__name__)

        pkval, avdict = entity._normalize_args_(kwargs, True)
        undo_funcs = []
        cache = entity._get_cache_()
        indexes = {}
        for attr in entity._simple_keys_:
            val = avdict[attr]
            if val in cache.indexes.setdefault(attr, {}): throw(CacheIndexError,
                'Cannot create %s: value %s for key %s already exists' % (entity.__name__, val, attr.name))
            indexes[attr] = val
        for attrs in entity._composite_keys_:
            vals = tuple(map(avdict.__getitem__, attrs))
            if vals in cache.indexes.setdefault(attrs, {}):
                attr_names = ', '.join(attr.name for attr in attrs)
                throw(CacheIndexError, 'Cannot create %s: value %s for composite key (%s) already exists'
                                 % (entity.__name__, vals, attr_names))
            indexes[attrs] = vals
        try:
            obj = entity._new_(pkval, 'created', undo_funcs)
            for attr, val in avdict.iteritems():
                if attr.pk_offset is not None: continue
                elif not attr.is_collection:
                    obj._vals_[attr.name] = val
                    if attr.reverse: attr.update_reverse(obj, None, val, undo_funcs)
                else: attr.__set__(obj, val, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
            raise
        if pkval is not None:
            pk = entity.__dict__['_pk_']
            cache.indexes[pk][pkval] = obj
        for key, vals in indexes.iteritems():
            cache.indexes[key][vals] = obj
        cache.modified = True
        cache.created.add(obj)
        cache.to_be_checked.append(obj)
        return obj
    def _get_raw_pkval_(obj):
        pkval = obj._pkval_
        if not obj._pk_is_composite_:
            pk = obj.__class__.__dict__['_pk_']
            if not pk.reverse: return (pkval,)
            else: return pkval._get_raw_pkval_()
        raw_pkval = []
        append = raw_pkval.append
        for attr, val in zip(obj._pk_attrs_, pkval):
            if not attr.reverse: append(val)
            else: raw_pkval += val._get_raw_pkval_()
        return tuple(raw_pkval)
    @cut_traceback
    def __repr__(obj):
        pkval = obj._pkval_
        if pkval is None: return '%s[new:%d]' % (obj.__class__.__name__, obj._newid_)
        if obj._pk_is_composite_: pkval = ','.join(map(repr, pkval))
        else: pkval = repr(pkval)
        return '%s[%s]' % (obj.__class__.__name__, pkval)
    def _load_(obj):
        cache = obj._cache_
        if not cache.is_alive: throw_db_session_is_over(obj)
        entity = obj.__class__
        database = entity._database_
        if cache is not database._get_cache():
            throw(TransactionError, "Object %s doesn't belong to current transaction" % obj)
        pk = entity.__dict__['_pk_']
        seeds = cache.seeds[pk]
        max_batch_size = database.provider.max_params_count // len(entity._pk_columns_)
        objects = [ obj ]
        for seed in seeds:
            if len(objects) >= max_batch_size: break
            if seed is not obj: objects.append(seed)
        sql, adapter, attr_offsets = entity._construct_batchload_sql_(len(objects))
        value_dict = dict(enumerate(objects))
        arguments = adapter(value_dict)
        cursor = database._exec_sql(sql, arguments)
        objects = entity._fetch_objects(cursor, attr_offsets)
        if obj not in objects: throw(UnrepeatableReadError, '%s disappeared' % obj)
    def _db_set_(obj, avdict, unpickling=False):
        assert obj._status_ not in created_or_deleted_statuses
        if not avdict: return

        cache = obj._cache_
        assert cache.is_alive
        pk = obj.__class__.__dict__['_pk_']
        seeds = cache.seeds.setdefault(pk, set())
        seeds.discard(obj)

        get_val = obj._vals_.get
        get_dbval = obj._dbvals_.get
        rbits = obj._rbits_
        wbits = obj._wbits_
        for attr, new_dbval in avdict.items():
            assert attr.pk_offset is None
            assert new_dbval is not NOT_LOADED
            old_dbval = get_dbval(attr.name, NOT_LOADED)
            if unpickling and old_dbval is not NOT_LOADED:
                del avdict[attr]
                continue
            elif attr.py_type is float:
                if old_dbval is NOT_LOADED: pass
                elif attr.converters[0].equals(old_dbval, new_dbval):
                    del avdict[attr]
                    continue
            elif old_dbval == new_dbval:
                del avdict[attr]
                continue

            bit = obj._bits_[attr]
            if rbits & bit: throw(UnrepeatableReadError,
                'Value of %s.%s for %s was updated outside of current transaction (was: %r, now: %r)'
                % (obj.__class__.__name__, attr.name, obj, old_dbval, new_dbval))

            if attr.reverse: attr.db_update_reverse(obj, old_dbval, new_dbval)
            obj._dbvals_[attr.name] = new_dbval
            if wbits & bit: del avdict[attr]
            if attr.is_unique:
                old_val = get_val(attr.name, NOT_LOADED)
                if old_val != new_dbval:
                    cache.db_update_simple_index(obj, attr, old_val, new_dbval)

        NOT_FOUND = object()
        for attrs in obj._composite_keys_:
            for attr in attrs:
                if attr in avdict: break
            else: continue
            vals = [ get_val(a.name, NOT_LOADED) for a in attrs ]
            currents = tuple(vals)
            for i, attr in enumerate(attrs):
                new_dbval = avdict.get(attr, NOT_FOUND)
                if new_dbval is NOT_FOUND: continue
                vals[i] = new_dbval
            vals = tuple(vals)
            cache.db_update_composite_index(obj, attrs, currents, vals)

        for attr, new_dbval in avdict.iteritems():
            obj._vals_[attr.name] = new_dbval
    def _delete_(obj, undo_funcs=None):
        status = obj._status_
        if status in del_statuses: return
        is_recursive_call = undo_funcs is not None
        if not is_recursive_call: undo_funcs = []
        cache = obj._cache_
        get_val = obj._vals_.get
        undo_list = []
        undo_dict = {}
        def undo_func():
            obj._status_ = status
            if status in ('loaded', 'saved'):
                to_be_checked = cache.to_be_checked
                if to_be_checked and to_be_checked[-1] is obj: to_be_checked.pop()
                assert obj not in to_be_checked
            obj._vals_.update((attr.name, val) for attr, val in undo_dict.iteritems())
            for index, old_key in undo_list: index[old_key] = obj
        undo_funcs.append(undo_func)
        try:
            for attr in obj._attrs_:
                reverse = attr.reverse
                if not reverse: continue
                if not attr.is_collection:
                    val = get_val(attr.name, NOT_LOADED)
                    if val is None: continue
                    if not reverse.is_collection:
                        if val is NOT_LOADED: val = attr.load(obj)
                        if val is None: continue
                        if attr.cascade_delete: val._delete_()
                        elif not reverse.is_required: reverse.__set__(val, None, undo_funcs)
                        else: throw(ConstraintError, "Cannot delete object %s, because it has associated %s, "
                                                     "and 'cascade_delete' option of %s is not set"
                                                     % (obj, attr.name, attr))
                    elif isinstance(reverse, Set):
                        if val is NOT_LOADED: pass
                        else: reverse.reverse_remove((val,), obj, undo_funcs)
                    else: throw(NotImplementedError)
                elif isinstance(attr, Set):
                    set_wrapper = attr.__get__(obj)
                    if not set_wrapper.__nonzero__(): pass
                    elif attr.cascade_delete:
                        for robj in set_wrapper: robj._delete_()
                    elif not reverse.is_required: attr.__set__(obj, (), undo_funcs)
                    else: throw(ConstraintError, "Cannot delete object %s, because it has non-empty set of %s, "
                                                 "and 'cascade_delete' option of %s is not set"
                                                 % (obj, attr.name, attr))
                else: throw(NotImplementedError)

            for attr in obj._simple_keys_:
                val = get_val(attr.name, NOT_LOADED)
                if val is NOT_LOADED: continue
                if val is None and cache.ignore_none: continue
                index = cache.indexes.get(attr)
                if index is None: continue
                obj2 = index.pop(val)
                assert obj2 is obj
                undo_list.append((index, val))

            for attrs in obj._composite_keys_:
                vals = tuple(get_val(a.name, NOT_LOADED) for a in attrs)
                if NOT_LOADED in vals: continue
                if cache.ignore_none and None in vals: continue
                index = cache.indexes.get(attrs)
                if index is None: continue
                obj2 = index.pop(vals)
                assert obj2 is obj
                undo_list.append((index, vals))

            if status == 'created':
                obj._status_ = 'cancelled'
                assert obj in cache.created
                cache.created.remove(obj)
                for attr in obj._attrs_:
                    if attr.pk_offset is not None: continue
                    obj._vals_.pop(attr.name, None)
                    if attr.is_collection:
                        mc = cache.modified_collections.get(attr)
                        if mc is not None: mc.discard(obj)
                if obj._pkval_ is not None:
                    pk = obj.__class__.__dict__['_pk_']
                    del cache.indexes[pk][obj._pkval_]
            else:
                if status == 'updated': cache.updated.remove(obj)
                elif status in ('loaded', 'saved'): cache.to_be_checked.append(obj)
                else: assert status == 'locked'
                obj._status_ = 'deleted'
                cache.modified = True
                cache.deleted.append(obj)
        except:
            if not is_recursive_call:
                for undo_func in reversed(undo_funcs): undo_func()
            raise
    @cut_traceback
    def delete(obj):
        if not obj._cache_.is_alive: throw_db_session_is_over(obj)
        obj._delete_()
    @cut_traceback
    def set(obj, **kwargs):
        cache = obj._cache_
        if not cache.is_alive: throw_db_session_is_over(obj)
        if obj._status_ in del_statuses: throw_object_was_deleted(obj)
        avdict, collection_avdict = obj._keyargs_to_avdicts_(kwargs)
        status = obj._status_
        wbits = obj._wbits_
        get_val = obj._vals_.get
        if avdict:
            for attr in avdict:
                old_val = get_val(attr.name, NOT_LOADED)
                if old_val is NOT_LOADED and attr.reverse and not attr.reverse.is_collection:
                    attr.load(obj)
            if wbits is not None:
                new_wbits = wbits
                for attr in avdict: new_wbits |= obj._bits_[attr]
                obj._wbits_ = new_wbits
                if status != 'updated':
                    obj._status_ = 'updated'
                    cache.modified = True
                    cache.updated.add(obj)
                    if status in ('loaded', 'saved'): cache.to_be_checked.append(obj)
                    else: assert status == 'locked'
            if not collection_avdict:
                for attr in avdict:
                    if attr.reverse or attr.is_part_of_unique_index: break
                else:
                    obj._vals_.update((attr.name, new_val) for attr, new_val in avdict.iteritems())
                    return
        undo_funcs = []
        undo = []
        def undo_func():
            obj._status_ = status
            obj._wbits_ = wbits
            if wbits == 0: cache.updated.remove(obj)
            if status in ('loaded', 'saved'):
                to_be_checked = cache.to_be_checked
                if to_be_checked and to_be_checked[-1] is obj: to_be_checked.pop()
                assert obj not in to_be_checked
            for index, old_key, new_key in undo:
                if new_key is NO_UNDO_NEEDED: pass
                else: del index[new_key]
                if old_key is NO_UNDO_NEEDED: pass
                else: index[old_key] = obj
        NOT_FOUND = object()
        try:
            for attr in obj._simple_keys_:
                new_val = avdict.get(attr, NOT_FOUND)
                if new_val is NOT_FOUND: continue
                old_val = get_val(attr.name, NOT_LOADED)
                if old_val == new_val: continue
                cache.update_simple_index(obj, attr, old_val, new_val, undo)
            for attrs in obj._composite_keys_:
                for attr in attrs:
                    if attr in avdict: break
                else: continue
                vals = [ get_val(a.name, NOT_LOADED) for a in attrs ]
                currents = tuple(vals)
                for i, attr in enumerate(attrs):
                    new_val = avdict.get(attr, NOT_FOUND)
                    if new_val is NOT_FOUND: continue
                    vals[i] = new_val
                vals = tuple(vals)
                cache.update_composite_index(obj, attrs, currents, vals, undo)
            for attr, new_val in avdict.iteritems():
                if not attr.reverse: continue
                old_val = get_val(attr.name, NOT_LOADED)
                attr.update_reverse(obj, old_val, new_val, undo_funcs)
            for attr, new_val in collection_avdict.iteritems():
                attr.__set__(obj, new_val, undo_funcs)
        except:
            for undo_func in undo_funcs: undo_func()
            raise
        obj._vals_.update((attr.name, new_val) for attr, new_val in avdict.iteritems())
    def _keyargs_to_avdicts_(obj, kwargs):
        avdict, collection_avdict = {}, {}
        get = obj._adict_.get
        for name, new_val in kwargs.items():
            attr = get(name)
            if attr is None: throw(TypeError, 'Unknown attribute %r' % name)
            new_val = attr.check(new_val, obj, from_db=False)
            if not attr.is_collection:
                if attr.pk_offset is not None:
                    old_val = obj._vals_.get(attr.name, NOT_LOADED)
                    if old_val != new_val: throw(TypeError, 'Cannot change value of primary key attribute %s' % attr.name)
                else: avdict[attr] = new_val
            else: collection_avdict[attr] = new_val
        return avdict, collection_avdict
    @cut_traceback
    def check_on_commit(obj):
        cache = obj._cache_
        if not cache.is_alive: throw_db_session_is_over(obj)
        if obj._status_ not in ('loaded', 'saved'): return
        obj._status_ = 'locked'
        cache.to_be_checked.append(obj)
    @classmethod
    def _attrs_with_bit_(entity, mask=-1):
        get_bit = entity._bits_.get
        for attr in entity._attrs_:
            bit = get_bit(attr)
            if bit is None: continue
            if not bit & mask: continue
            yield attr
    def _save_principal_objects_(obj, dependent_objects):
        if dependent_objects is None: dependent_objects = []
        elif obj in dependent_objects:
            chain = ' -> '.join(obj2.__class__.__name__ for obj2 in dependent_objects)
            throw(UnresolvableCyclicDependency, 'Cannot save cyclic chain: ' + chain)
        dependent_objects.append(obj)
        status = obj._status_
        if status == 'created': attr_iter = obj._attrs_with_bit_()
        elif status == 'updated': attr_iter = obj._attrs_with_bit_(obj._wbits_)
        else: assert False
        for attr in attr_iter:
            val = obj._vals_[attr.name]
            if not attr.reverse: continue
            if not attr.columns: continue
            if val is None: continue
            if val._status_ == 'created':
                val._save_(dependent_objects)
                assert val._status_ == 'saved'
    def _save_created_(obj):
        values = []
        auto_pk = (obj._pkval_ is None)
        if auto_pk: pk = obj.__class__.__dict__['_pk_']
        for attr in obj._attrs_:
            if not attr.columns: continue
            if attr.is_collection: continue
            val = obj._vals_[attr.name]
            if auto_pk and attr.is_pk: continue
            values.extend(attr.get_raw_values(val))
        database = obj._database_
        if auto_pk: cached_sql = obj._cached_create_sql_auto_pk_
        else: cached_sql = obj._cached_create_sql_
        if cached_sql is None:
            entity = obj.__class__
            if auto_pk:
                columns = entity._columns_without_pk_
                converters = entity._converters_without_pk_
            else:
                columns = entity._columns_
                converters = entity._converters_
            assert len(columns) == len(converters)
            params = [ [ 'PARAM', i,  converter ] for i, converter in enumerate(converters) ]
            sql_ast = [ 'INSERT', entity._table_, columns, params ]
            if auto_pk:
                assert len(entity._pk_columns_) == 1
                assert pk.auto
                sql_ast.append(obj._pk_columns_[0])
            sql, adapter = database._ast2sql(sql_ast)
            if auto_pk: entity._cached_create_sql_auto_pk_ = sql, adapter
            else: entity._cached_create_sql_ = sql, adapter
        else: sql, adapter = cached_sql
        arguments = adapter(values)
        try:
            if auto_pk: new_id = database._exec_sql(sql, arguments, returning_id=True)
            else: database._exec_sql(sql, arguments)
        except IntegrityError, e:
            msg = " ".join(tostring(arg) for arg in e.args)
            throw(TransactionIntegrityError,
                  'Object %r cannot be stored in the database (probably it already exists). %s: %s'
                  % (obj, e.__class__.__name__, msg), e)
        except DatabaseError, e:
            msg = " ".join(tostring(arg) for arg in e.args)
            throw(UnexpectedError, 'Object %r cannot be stored in the database. %s: %s'
                                   % (obj, e.__class__.__name__, msg), e)

        if auto_pk:
            index = obj._cache_.indexes.setdefault(pk, {})
            obj2 = index.setdefault(new_id, obj)
            if obj2 is not obj: throw(TransactionIntegrityError,
                'Newly auto-generated id value %s was already used in transaction cache for another object' % new_id)
            obj._pkval_ = obj._vals_[pk.name] = new_id
            obj._newid_ = None

        obj._status_ = 'saved'
        obj._rbits_ = obj._all_bits_
        obj._wbits_ = 0
        bits = obj._bits_
        for attr in obj._attrs_:
            if attr not in bits: continue
            obj._dbvals_[attr.name] = obj._vals_[attr.name]
    def _save_updated_(obj):
        update_columns = []
        values = []
        for attr in obj._attrs_with_bit_(obj._wbits_):
            if not attr.columns: continue
            update_columns.extend(attr.columns)
            val = obj._vals_[attr.name]
            values.extend(attr.get_raw_values(val))
        if update_columns:
            for attr in obj._pk_attrs_:
                val = obj._vals_[attr.name]
                values.extend(attr.get_raw_values(val))
            optimistic_check_columns = []
            optimistic_check_converters = []
            if obj._cache_.optimistic:
                for attr in obj._attrs_with_bit_(obj._rbits_):
                    if not attr.columns: continue
                    dbval = obj._dbvals_.get(attr.name, NOT_LOADED)
                    assert dbval is not NOT_LOADED
                    optimistic_check_columns.extend(attr.columns)
                    if dbval is not None:
                        optimistic_check_converters.extend(attr.converters)
                    else:
                        optimistic_check_converters.extend(None for converter in attr.converters)
                    values.extend(attr.get_raw_values(dbval))
            query_key = (tuple(update_columns), tuple(optimistic_check_columns), tuple(converter is not None for converter in optimistic_check_converters))
            database = obj._database_
            cached_sql = obj._update_sql_cache_.get(query_key)
            if cached_sql is None:
                update_converters = []
                for attr in obj._attrs_with_bit_(obj._wbits_):
                    if not attr.columns: continue
                    update_converters.extend(attr.converters)
                assert len(update_columns) == len(update_converters)
                update_params = [ [ 'PARAM', i, converter ] for i, converter in enumerate(update_converters) ]
                params_count = len(update_params)
                where_list = [ 'WHERE' ]
                pk_columns = obj._pk_columns_
                pk_converters = obj._pk_converters_
                params_count = populate_criteria_list(where_list, pk_columns, pk_converters, params_count)
                populate_criteria_list(where_list, optimistic_check_columns, optimistic_check_converters, params_count)
                sql_ast = [ 'UPDATE', obj._table_, zip(update_columns, update_params), where_list ]
                sql, adapter = database._ast2sql(sql_ast)
                obj._update_sql_cache_[query_key] = sql, adapter
            else: sql, adapter = cached_sql
            arguments = adapter(values)
            cursor = database._exec_sql(sql, arguments)
            if cursor.rowcount != 1:
                throw(UnrepeatableReadError, 'Object %r was updated outside of current transaction' % obj)
        obj._status_ = 'saved'
        obj._rbits_ |= obj._wbits_
        obj._wbits_ = 0
        for attr in obj._attrs_with_bit_():
            val = obj._vals_.get(attr.name, NOT_LOADED)
            if val is NOT_LOADED: assert attr.name not in obj._dbvals_
            else: obj._dbvals_[attr.name] = val
    def _save_locked_(obj):
        assert obj._wbits_ == 0
        if not obj._cache_.optimistic:
            obj._status_ = 'loaded'
            return
        values = []
        for attr in obj._pk_attrs_:
            val = obj._vals_[attr.name]
            values.extend(attr.get_raw_values(val))
        optimistic_check_columns = []
        optimistic_check_converters = []
        for attr in obj._attrs_with_bit_(obj._rbits_):
            if not attr.columns: continue
            dbval = obj._dbvals_.get(attr.name, NOT_LOADED)
            assert dbval is not NOT_LOADED
            optimistic_check_columns.extend(attr.columns)
            optimistic_check_converters.extend(attr.converters)
            values.extend(attr.get_raw_values(dbval))
        query_key = tuple(optimistic_check_columns)
        database = obj._database_
        cached_sql = obj._lock_sql_cache_.get(query_key)
        if cached_sql is None:
            where_list = [ 'WHERE' ]
            params_count = populate_criteria_list(where_list, obj._pk_columns_, obj._pk_converters_)
            populate_criteria_list(where_list, optimistic_check_columns, optimistic_check_converters, params_count)
            sql_ast = [ 'SELECT', [ 'ALL', [ 'VALUE', 1 ]], [ 'FROM', [ None, 'TABLE', obj._table_ ] ], where_list ]
            sql, adapter = database._ast2sql(sql_ast)
            obj._lock_sql_cache_[query_key] = sql, adapter
        else: sql, adapter = cached_sql
        arguments = adapter(values)
        cursor = database._exec_sql(sql, arguments)
        row = cursor.fetchone()
        if row is None: throw(UnrepeatableReadError, 'Object %r was updated outside of current transaction' % obj)
        obj._status_ = 'loaded'
    def _save_deleted_(obj):
        database = obj._database_
        cached_sql = obj._cached_delete_sql_
        if cached_sql is None:
            where_list = [ 'WHERE' ]
            populate_criteria_list(where_list, obj._pk_columns_, obj._pk_converters_)
            sql_ast = [ 'DELETE', obj._table_, where_list ]
            sql, adapter = database._ast2sql(sql_ast)
            obj.__class__._cached_delete_sql_ = sql, adapter
        else: sql, adapter = cached_sql
        values = obj._get_raw_pkval_()
        arguments = adapter(values)
        database._exec_sql(sql, arguments)
    def _save_(obj, dependent_objects=None):
        cache = obj._cache_
        assert cache.is_alive and cache.saving
        status = obj._status_
        if status in ('loaded', 'saved', 'cancelled'): return
        if status in ('created', 'updated'):
            obj._save_principal_objects_(dependent_objects)

        if status == 'created': obj._save_created_()
        elif status == 'updated': obj._save_updated_()
        elif status == 'deleted': obj._save_deleted_()
        elif status == 'locked': obj._save_locked_()
        else: assert False

class Cache(object):
    def __init__(cache, database):
        cache.is_alive = True
        cache.num = next_num()
        cache.database = database
        cache.optimistic = database.optimistic
        cache.ignore_none = True  # todo : get from provider
        cache.indexes = {}
        cache.seeds = {}
        cache.collection_statistics = {}
        cache.created = set()
        cache.deleted = []
        cache.updated = set()
        cache.modified_collections = {}
        cache.to_be_checked = []
        cache.query_results = {}
        cache.modified = False
        cache.saving = False
        cache.connection = cache.establish_connection(False)
    def establish_connection(cache, reestablish=True):
        if reestablish:
            assert not cache.connection
            if not cache.optimistic: throw(ConnectionClosedError,
                'Pessimistic transaction cannot be continued because database connection failed')
            elif cache.saving: throw(ConnectionClosedError,
                'Optimistic transaction cannot be completed because database connection failed during saving changes')
            log_orm('RECONNECT')
        provider = cache.database.provider
        connection = provider.connect()
        cache.connection = connection
        if cache.optimistic: provider.set_optimistic_mode(connection)
        else: provider.set_pessimistic_mode(connection)
        return connection
    def switch_to_pessimistic_mode(cache):
        assert cache.optimistic
        connection = cache.connection or cache.establish_connection()
        cache.optimistic = False
        provider = cache.database.provider
        provider.set_pessimistic_mode(cache.connection)
    def commit(cache):
        assert cache.is_alive
        database = cache.database
        provider = database.provider
        connection = cache.connection or cache.establish_connection()
        try:
            if cache.optimistic:
                if debug: log_orm('OPTIMISTIC ROLLBACK')
                provider.rollback(connection)
        except:
            cache.is_alive = False
            cache.connection = None
            x = local.db2cache.pop(database); assert x is cache
            provider.drop(connection)
            raise
        try:
            modified = cache.modified
            if modified:
                if cache.optimistic:
                    if debug: log_orm('START OPTIMISTIC SAVE')
                    provider.start_optimistic_save(connection)
                cache.save()
            if modified or not cache.optimistic:
                if debug: log_orm('COMMIT')
                provider.commit(connection)
            if database.optimistic:
                cache.optimistic = True
                provider.set_optimistic_mode(connection)
            elif cache.optimistic:
                cache.switch_to_pessimistic_mode()
        except:
            cache.rollback()
            raise
    def rollback(cache, close_connection=False):
        assert cache.is_alive
        database = cache.database
        x = local.db2cache.pop(database); assert x is cache
        cache.is_alive = False
        provider = database.provider
        connection = cache.connection
        if connection is None: return
        cache.connection = None
        try:
            if debug: log_orm('ROLLBACK')
            provider.rollback(connection)
            if not close_connection:
                if debug: log_orm('RELEASE_CONNECTION')
                provider.release(connection)
        except:
            if debug: log_orm('CLOSE_CONNECTION')
            provider.drop(connection)
            raise
        if close_connection:
            if debug: log_orm('CLOSE_CONNECTION')
            provider.drop(connection)
    def release(cache):
        assert cache.is_alive
        database = cache.database
        x = local.db2cache.pop(database); assert x is cache
        cache.is_alive = False
        provider = database.provider
        connection = cache.connection
        if connection is None: return
        cache.connection = None
        if debug: log_orm('RELEASE_CONNECTION')
        provider.release(connection)
    def flush(cache):
        if cache.optimistic: cache.switch_to_pessimistic_mode()
        if cache.modified: cache.save()
    def save(cache):
        assert cache.is_alive
        if not cache.modified: return
        if not (cache.created or cache.updated or cache.deleted or cache.modified_collections): return
        assert cache.saving == False
        cache.saving = True
        try:
            cache.query_results.clear()
            modified_m2m = cache.calc_modified_m2m()
            for attr, (added, removed) in modified_m2m.iteritems():
                if not removed: continue
                attr.remove_m2m(removed)
            for obj in cache.to_be_checked:
                obj._save_()
            for attr, (added, removed) in modified_m2m.iteritems():
                if not added: continue
                attr.add_m2m(added)

            cache.created.clear()
            cache.updated.clear()

            indexes = cache.indexes
            for obj in cache.deleted:
                pkval = obj._pkval_
                pk = obj.__class__.__dict__['_pk_']
                index = indexes[pk]
                index.pop(pkval)

            cache.deleted[:] = []
            cache.modified_collections.clear()
            cache.to_be_checked[:] = []
            cache.modified = False
        finally:
            cache.saving = False
    def calc_modified_m2m(cache):
        modified_m2m = {}
        for attr, objects in sorted(cache.modified_collections.iteritems(),
                                    key=lambda (attr, objects): (attr.entity.__name__, attr.name)):
            if not isinstance(attr, Set): throw(NotImplementedError)
            reverse = attr.reverse
            if not reverse.is_collection: continue
            if not isinstance(reverse, Set): throw(NotImplementedError)
            if reverse in modified_m2m: continue
            added, removed = modified_m2m.setdefault(attr, (set(), set()))
            for obj in objects:
                setdata = obj._vals_.pop(attr.name)
                for obj2 in setdata.added: added.add((obj, obj2))
                for obj2 in setdata.removed: removed.add((obj, obj2))
        cache.modified_collections.clear()
        return modified_m2m
    def update_simple_index(cache, obj, attr, old_val, new_val, undo):
        assert old_val != new_val
        index = cache.indexes.get(attr)
        if index is None: index = cache.indexes[attr] = {}
        if new_val is None and cache.ignore_none: new_val = NO_UNDO_NEEDED
        else:
            obj2 = index.setdefault(new_val, obj)
            if obj2 is not obj: throw(CacheIndexError, 'Cannot update %s.%s: %s with key %s already exists'
                                                 % (obj.__class__.__name__, attr.name, obj2, new_val))
        if old_val is NOT_LOADED: old_val = NO_UNDO_NEEDED
        elif old_val is None and cache.ignore_none: old_val = NO_UNDO_NEEDED
        else: del index[old_val]
        undo.append((index, old_val, new_val))
    def db_update_simple_index(cache, obj, attr, old_dbval, new_dbval):
        assert old_dbval != new_dbval
        index = cache.indexes.get(attr)
        if index is None: index = cache.indexes[attr] = {}
        if new_dbval is NOT_LOADED: pass
        elif new_dbval is None and cache.ignore_none: pass
        else:
            obj2 = index.setdefault(new_dbval, obj)
            if obj2 is not obj: throw(TransactionIntegrityError,
                '%s with unique index %s.%s already exists: %s'
                % (obj2.__class__.__name__, obj.__class__.__name__, attr.name, new_dbval))
                # attribute which was created or updated lately clashes with one stored in database
        index.pop(old_dbval, None)
    def update_composite_index(cache, obj, attrs, currents, vals, undo):
        if cache.ignore_none:
            if None in currents: currents = NO_UNDO_NEEDED
            if None in vals: vals = NO_UNDO_NEEDED
        if currents is NO_UNDO_NEEDED: pass
        elif NOT_LOADED in currents: currents = NO_UNDO_NEEDED
        if vals is NO_UNDO_NEEDED: pass
        elif NOT_LOADED in vals: vals = NO_UNDO_NEEDED
        if currents is NO_UNDO_NEEDED and vals is NO_UNDO_NEEDED: return
        index = cache.indexes.get(attrs)
        if index is None: index = cache.indexes[attrs] = {}
        if vals is NO_UNDO_NEEDED: pass
        else:
            obj2 = index.setdefault(vals, obj)
            if obj2 is not obj:
                attr_names = ', '.join(attr.name for attr in attrs)
                throw(CacheIndexError, 'Cannot update %r: composite key (%s) with value %s already exists for %r'
                                 % (obj, attr_names, vals, obj2))
        if currents is NO_UNDO_NEEDED: pass
        else: del index[currents]
        undo.append((index, currents, vals))
    def db_update_composite_index(cache, obj, attrs, currents, vals):
        index = cache.indexes.get(attrs)
        if index is None: index = cache.indexes[attrs] = {}
        if NOT_LOADED in vals: pass
        elif None in vals and cache.ignore_none: pass
        else:
            obj2 = index.setdefault(vals, obj)
            if obj2 is not obj:
                key_str = ', '.join(repr(item) for item in vals)
                throw(TransactionIntegrityError, '%s with unique index (%s) already exists: %s'
                                 % (obj2.__class__.__name__, ', '.join(attr.name for attr in attrs), key_str))
        index.pop(currents, None)

def _get_caches():
    return list(sorted((cache for cache in local.db2cache.values()),
                       reverse=True, key=lambda cache : (cache.database.priority, cache.num)))

@cut_traceback
def flush():
    for cache in _get_caches(): cache.flush()

def reraise(exc_class, exceptions):
    try:
        cls, exc, tb = exceptions[0]
        msg = " ".join(tostring(arg) for arg in exc.args)
        if not issubclass(cls, TransactionError):
            msg = '%s: %s' % (cls.__name__, msg)
        raise exc_class, exc_class(msg, exceptions), tb
    finally: del tb

@cut_traceback
def commit():
    caches = _get_caches()
    if not caches: return
    primary_cache = caches[0]
    other_caches = caches[1:]
    exceptions = []
    try:
        try: primary_cache.commit()
        except:
            exceptions.append(sys.exc_info())
            for cache in other_caches:
                try: cache.rollback()
                except: exceptions.append(sys.exc_info())
            reraise(CommitException, exceptions)
        else:
            for cache in other_caches:
                try: cache.commit()
                except: exceptions.append(sys.exc_info())
            if exceptions:
                reraise(PartialCommitException, exceptions)
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
            reraise(RollbackException, exceptions)
        assert not local.db2cache
    finally:
        del exceptions

def _release():
    for cache in _get_caches(): cache.release()
    assert not local.db2cache

class DBSessionContextManager(object):
    def __init__(self, retry=1, retry_exceptions=(TransactionError,), allowed_exceptions=()):
        if retry is not 1 and (type(retry) is not int or retry < 0): throw(TypeError)
        self.retry = retry
        self.retry_exceptions = retry_exceptions
        self.allowed_exceptions = allowed_exceptions
        self.is_decorator = False
    def __call__(self, *args, **kwargs):
        if not args and not kwargs: return self
        if len(args) > 1: throw(TypeError)
        if len(args) == 1:
            if kwargs: throw(TypeError)
            self.is_decorator = True
            old_func = args[0]
            def new_func(*args, **kwargs):
                for i in xrange(self.retry):
                    try:
                        with self: return old_func(*args, **kwargs)
                    except Exception, e:
                        if not isinstance(e, self.retry_exceptions): raise
                raise
            return copy_func_attrs(new_func, old_func, 'db_session')
        return self.__class__(**kwargs)
    def __enter__(self):
        if not self.is_decorator and self.retry != 1: throw(TypeError,
            "@db_session can accept 'retry' parameter only when used as decorator and not as context manager")
        local.db_context_counter += 1
    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        local.db_context_counter -= 1
        if local.db_context_counter: return
        try:
            if exc_type is None:
                commit()
                return
            assert isinstance(exc_value, exc_type)
            allowed_exceptions = self.allowed_exceptions
            if callable(allowed_exceptions): allowed = allowed_exceptions(exc_value)
            else: allowed = isinstance(exc_value, tuple(allowed_exceptions))
            if allowed: commit()
            else: rollback()
        finally: _release()

db_session = DBSessionContextManager()

def with_transaction(*args, **kwargs):
    deprecated("@with_transaction decorator is deprecated, use @db_session decorator instead")
    return db_session(*args, **kwargs)

@simple_decorator
def db_decorator(func, *args, **kwargs):
    web = sys.modules.get('pony.web')
    allowed_exceptions = web and [ web.HttpRedirect ] or []
    try:
        with db_session(allowed_exceptions=allowed_exceptions):
            return func(*args, **kwargs)
    except (ObjectNotFound, RowNotFound):
        if web: throw(web.Http404NotFound)
        raise

###############################################################################

def string2ast(s):
    result = string2ast_cache.get(s)
    if result is not None: return result
    module_node = parse('(%s)' % s)
    if not isinstance(module_node, ast.Module): throw(TypeError)
    stmt_node = module_node.node
    if not isinstance(stmt_node, ast.Stmt) or len(stmt_node.nodes) != 1: throw(TypeError)
    discard_node = stmt_node.nodes[0]
    if not isinstance(discard_node, ast.Discard): throw(TypeError)
    result = string2ast_cache[s] = discard_node.expr
    # result = deepcopy(result)  # no need for now, but may be needed later
    return result

@cut_traceback
def select(gen, frame_depth=0, left_join=False):
    if isinstance(gen, types.GeneratorType):
        tree, external_names = decompile(gen)
        code_key = id(gen.gi_frame.f_code)
        globals = gen.gi_frame.f_globals
        locals = gen.gi_frame.f_locals
    elif isinstance(gen, basestring):
        query_string = gen
        tree = string2ast(query_string)
        if not isinstance(tree, ast.GenExpr): throw(TypeError)
        code_key = query_string
        globals = sys._getframe(frame_depth+2).f_globals
        locals = sys._getframe(frame_depth+2).f_locals
    else: throw(TypeError)
    return Query(code_key, tree.code, globals, locals, left_join)

@cut_traceback
def left_join(gen, frame_depth=0):
    return select(gen, frame_depth=frame_depth+2, left_join=True)

@cut_traceback
def get(gen):
    return select(gen, frame_depth=2).get()

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

count = make_aggrfunc(count)
sum = make_aggrfunc(_sum)
min = make_aggrfunc(_min)
max = make_aggrfunc(_max)
avg = make_aggrfunc(_avg)

@cut_traceback
def exists(gen, frame_depth=0):
    return select(gen, frame_depth=frame_depth+2).exists()

def JOIN(expr):
    return expr

def desc(expr):
    if isinstance(expr, Attribute):
        return expr.desc
    if isinstance(expr, (int, long)) and expr > 0:
        return -expr
    if isinstance(expr, basestring):
        return 'desc(%s)' % expr
    return expr

def extract_vars(extractors, globals, locals):
    vars = {}
    vartypes = {}
    for src, code in extractors.iteritems():
        if src == '.0': value = locals['.0']
        else:
            try: value = eval(code, globals, locals)
            except Exception, cause: raise ExprEvalError(src, cause)
            if src == 'None' and value is not None: throw(TranslationError)
            if src == 'True' and value is not True: throw(TranslationError)
            if src == 'False' and value is not False: throw(TranslationError)
        try: vartypes[src] = get_normalized_type_of(value)
        except TypeError:
            if not isinstance(value, dict):
                unsupported = False
                try: value = tuple(value)
                except: unsupported = True
            else: unsupported = True
            if unsupported:
                typename = type(value).__name__
                if src == '.0': throw(TypeError, 'Cannot iterate over non-entity object')
                throw(TypeError, 'Expression %s has unsupported type %r' % (src, typename))
            vartypes[src] = get_normalized_type_of(value)
        vars[src] = value
    return vars, vartypes

def unpickle_query(query_result):
    return query_result

class Query(object):
    def __init__(query, code_key, tree, globals, locals, left_join=False):
        assert isinstance(tree, ast.GenExprInner)
        extractors, varnames, tree = create_extractors(code_key, tree)
        vars, vartypes = extract_vars(extractors, globals, locals)

        node = tree.quals[0].iter
        origin = vars[node.src]
        if isinstance(origin, EntityIter): origin = origin.entity
        elif not isinstance(origin, EntityMeta):
            if node.src == '.0': throw(TypeError, 'Cannot iterate over non-entity object')
            else: throw(TypeError, 'Cannot iterate over non-entity object %s' % node.src)
        database = origin._database_
        if database is None: throw(TranslationError, 'Entity %s is not mapped to a database' % origin.__name__)
        if database.schema is None: throw(ERDiagramError, 'Mapping is not generated for entity %r' % origin.__name__)

        if database.provider.dialect == 'Oracle':
            for name, value in vars.iteritems():
                if value == '':
                    vars[name] = None
                    vartypes[name] = type(None)

        query._vars = vars
        query._key = code_key, tuple(map(vartypes.__getitem__, varnames)), left_join
        query._database = database
        query._cache = database._get_cache()

        translator = database._translator_cache.get(query._key)
        if translator is None:
            pickled_tree = query._pickled_tree = dumps(tree, 2)
            tree = loads(pickled_tree)  # tree = deepcopy(tree)
            translator_cls = database.provider.translator_cls
            translator = translator_cls(tree, extractors, vartypes, left_join=left_join)
            name_path = translator.can_be_optimized()
            if name_path:
                tree = loads(pickled_tree)  # tree = deepcopy(tree)
                try: translator = translator_cls(tree, extractors, vartypes, left_join=True, optimize=name_path)
                except OptimizationFailed: translator.optimization_failed = True
            database._translator_cache[query._key] = translator
        query._translator = translator
        query._filters = []
    def __reduce__(query):
        return unpickle_query, (query._fetch(),)
    def _construct_sql_and_arguments(query, range=None, distinct=None, aggr_func_name=None):
        translator = query._translator
        sql_key = query._key + (range, distinct, aggr_func_name, options.INNER_JOIN_SYNTAX)
        database = query._database
        cache_entry = database._constructed_sql_cache.get(sql_key)
        if cache_entry is None:
            sql_ast, attr_offsets = translator.construct_sql_ast(range, distinct, aggr_func_name)
            cache = database._get_cache()
            sql, adapter = database.provider.ast2sql(sql_ast)
            cache_entry = sql, adapter, attr_offsets
            database._constructed_sql_cache[sql_key] = cache_entry
        else: sql, adapter, attr_offsets = cache_entry
        arguments = adapter(query._vars)
        arguments_type = type(arguments)
        if arguments_type is tuple: arguments_key = arguments
        elif arguments_type is dict: arguments_key = tuple(sorted(arguments.iteritems()))
        try: hash(arguments_key)
        except: query_key = None  # arguments are unhashable
        else: query_key = sql_key + (arguments_key)
        return sql, arguments, attr_offsets, query_key
    def _fetch(query, range=None, distinct=None):
        translator = query._translator
        sql, arguments, attr_offsets, query_key = query._construct_sql_and_arguments(range, distinct)
        cache = query._cache
        try: result = cache.query_results[query_key]
        except KeyError:
            cursor = query._database._exec_sql(sql, arguments)
            if isinstance(translator.expr_type, EntityMeta):
                entity = translator.expr_type
                result = entity._fetch_objects(cursor, attr_offsets, rbits=translator.tableref.rbits)
            elif len(translator.row_layout) == 1:
                func, slice_or_offset, src = translator.row_layout[0]
                result = list(starmap(func, cursor.fetchall()))
            else:
                result = [ tuple(func(sql_row[slice_or_offset])
                                 for func, slice_or_offset, src in translator.row_layout)
                           for sql_row in cursor.fetchall() ]
                for i, t in enumerate(translator.expr_type):
                    if isinstance(t, EntityMeta) and t._discriminator_ and t._subclasses_:
                        t._load_many_(row[i] for row in result)
            if query_key is not None:
                query._cache.query_results[query_key] = result
        else:
            stats = query._database.dblocal.stats
            stat = stats.get(sql)
            if stat is not None: stat.cache_count += 1
            else: stats[sql] = QueryStat(sql)
        return QueryResult(result, translator.expr_type, translator.col_names)
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
            query = query.order_by(*[i+1 for i in range(len(query._translator.expr_type))])
        else:
            query = query.order_by(1)
        objects = query[:1]
        if not objects: return None
        return objects[0]
    @cut_traceback
    def without_distinct(query):
        return query._fetch(distinct=False)
    @cut_traceback
    def distinct(query):
        return query._fetch(distinct=True)
    @cut_traceback
    def exists(query):
        # new_query = query._clone()
        new_query = object.__new__(Query)
        new_query.__dict__.update(query.__dict__)

        new_query._aggr_func_name = 'EXISTS'
        new_query._aggr_select = [ 'ALL', [ 'VALUE', 1 ] ]
        sql, arguments, attr_offsets, query_key = new_query._construct_sql_and_arguments(range=(0, 1))
        cache = new_query._cache
        try: result = cache.query_results[query_key]
        except KeyError:
            cursor = new_query._database._exec_sql(sql, arguments)
            row = cursor.fetchone()
            result = row is not None
            if query_key is not None: cache.query_results[query_key] = result
        return result
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
            return query._without_order_by()

        attributes = functions = strings = numbers = False
        for arg in args:
            if isinstance(arg, basestring): strings = True
            elif type(arg) is types.FunctionType: functions = True
            elif isinstance(arg, (int, long)): numbers = True
            elif isinstance(arg, (Attribute, DescWrapper)): attributes = True
            else: throw(TypeError, "Arguments of order_by() method must be attributes, numbers, strings or lambdas. Got: %r" % arg)
        if strings + functions + numbers + attributes > 1:
            throw(TypeError, 'All arguments of order_by() method must be of the same type')
        if len(args) > 1 and strings + functions:
            throw(TypeError, 'When argument of order_by() method is string or lambda, it must be the only argument')

        if numbers or attributes:
            query._filters.append((numbers, args))
            new_key = query._key + (args,)
            translator = query._database._translator_cache.get(new_key)
            if translator is None:
                if numbers: translator = query._translator.order_by_numbers(args)
                else: translator = query._translator.order_by_attributes(args)
                query._database._translator_cache[new_key] = translator
            query._key = new_key
            query._translator = translator
            return query

        globals = sys._getframe(2).f_globals
        locals = sys._getframe(2).f_locals
        if strings:
            expr_text = func_id = args[0]
            func_ast = string2ast(expr_text)
            if isinstance(func_ast, ast.Lambda): func_ast = func_ast.code
        elif functions:
            func = args[0]
            for name in func.func_code.co_varnames:
                if name not in query._translator.subquery:
                    throw(TranslationError, 'Unknown name %s' % name)
            func_id = id(func.func_code)
            func_ast = decompile(func)[0]
        else: assert False
        return query._process_lambda(func_id, func_ast, globals, locals, order_by=True)
    def _without_order_by(query):
        query._key = query._key[:3]
        database = query._database
        translator = database._translator_cache.get(query._key)
        assert translator is not None  # Translator for query without order_by must be in cache already
        query._translator = translator
        return query
    def _process_lambda(query, func_id, func_ast, globals, locals, order_by):
        extractors, varnames, func_ast = create_extractors(func_id, func_ast, query._translator.subquery)
        if extractors:
            vars, vartypes = extract_vars(extractors, globals, locals)
            query_vars = query._vars
            for name, value in vars.iteritems():
                if query_vars.setdefault(name, value) != value: throw(TranslationError,
                    'Meaning of expression %s has changed during query translation' % name)
            sorted_vartypes = tuple(map(vartypes.__getitem__, varnames))
        else: vars, vartypes, sorted_vartypes = {}, {}, ()
        query._filters.append((order_by, func_ast, extractors, vartypes))
        new_key = query._key + ((order_by and 'order_by' or 'filter', func_id, sorted_vartypes),)
        translator = query._database._translator_cache.get(new_key)
        if translator is None:
            prev_optimized = query._translator.optimize
            translator = query._translator.apply_lambda(order_by, func_ast, extractors, vartypes)
            if not prev_optimized:
                name_path = translator.can_be_optimized()
                if name_path:
                    tree = loads(query._pickled_tree)  # tree = deepcopy(tree)
                    prev_extractors = query._translator.extractors
                    prev_vartypes = query._translator.vartypes
                    translator_cls = query._translator.__class__
                    translator = translator_cls(tree, prev_extractors, prev_vartypes, left_join=True, optimize=name_path)
                    translator = query.reapply_filters(translator)
            query._database._translator_cache[new_key] = translator
        query._key = new_key
        query._translator = translator
        return query
    def reapply_filters(query, translator):
        for tup in query._filters:
            if len(tup) == 2:
                numbers, args = tup
                if numbers: translator = translator.order_by_numbers(args)
                else: translator = translator.order_by_attributes(args)
                continue
            order_by, func_ast, extractors, vartypes = tup
            translator = translator.apply_lambda(order_by, func_ast, extractors, vartypes)
        return translator
    @cut_traceback
    def filter(query, func):
        globals = sys._getframe(2).f_globals
        locals = sys._getframe(2).f_locals
        if isinstance(func, basestring):
            func_id = func
            func_ast = string2ast(func)
            if isinstance(func_ast, ast.Lambda): func_ast = func_ast.code
        elif type(func) is types.FunctionType:
            for name in func.func_code.co_varnames:
                if name not in query._translator.subquery:
                    throw(TranslationError, 'Unknown name %s' % name)
            func_id = id(func.func_code)
            func_ast = decompile(func)[0]
        else: throw(TypeError, 'Argument of filter() method must be a lambda functon or its text. Got: %r' % func)
        return query._process_lambda(func_id, func_ast, globals, locals, order_by=False)
    @cut_traceback
    def __getitem__(query, key):
        if isinstance(key, slice):
            step = key.step
            if step is not None and step <> 1: throw(TypeError, "Parameter 'step' of slice object is not allowed here")
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
    def _aggregate(query, aggr_func_name):
        translator = query._translator
        sql, arguments, attr_offsets, query_key = query._construct_sql_and_arguments(aggr_func_name=aggr_func_name)
        cache = query._cache
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
                expr_type = translator.expr_type
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

def strcut(s, width):
    if len(s) <= width:
        return s + ' ' * (width - len(s))
    else:
        return s[:width-3] + '...'

class QueryResult(list):
    __slots__ = '_expr_type', '_col_names'
    def __init__(result, list, expr_type, col_names):
        result[:] = list
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
            row_maker = attrgetter(*col_names)
            rows = [ map(to_str, row_maker(obj)) for obj in result ]
        elif len(col_names) == 1:
            rows = [ (to_str(obj),) for obj in result ]
        else:
            rows = [ map(to_str, row) for row in result ]

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

        print strjoin('|', (strcut(colname, width_dict[i]) for i, colname in enumerate(col_names)))
        print strjoin('+', ('-' * width_dict[i] for i in xrange(len(col_names))))
        for row in rows:
            print strjoin('|', (strcut(item, width_dict[i]) for i, item in enumerate(row)))

@cut_traceback
def show(entity):
    x = entity
    if isinstance(x, EntityMeta):
        print x.describe()
    elif isinstance(x, Entity):
        print 'instance of ' + x.__class__.__name__
        # width = options.CONSOLE_WIDTH
        # for attr in x._attrs_:
        #     if attr.is_collection or attr.lazy: continue
        #     value = str(attr.__get__(x)).replace('\n', ' ')
        #     print '  %s: %s' % (attr.name, strcut(value, width-len(attr.name)-4))
        # print
        QueryResult([ x ], x.__class__, None).show()
    elif isinstance(x, (basestring, types.GeneratorType)):
        select(x).show()
    elif hasattr(x, 'show'):
        x.show()
    else:
        from pprint import pprint
        pprint(x)
