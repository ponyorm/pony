import re, sys, threading
from itertools import count
from operator import itemgetter

from pony import options
from pony.utils import import_module, parse_expr, is_ident, localbase
from pony.sqlsymbols import *

debug = True

class DBException(Exception):
    def __init__(self, *args, **keyargs):
        exceptions = keyargs.pop('exceptions', [])
        assert not keyargs
        if not args and exceptions:
            if len(exceptions) == 1: args = getattr(exceptions[0], 'args', ())
            else: args = ('Multiple exceptions have occured',)
        Exception.__init__(self, *args)
        self.exceptions = exceptions

class RowNotFound(DBException): pass
class MultipleRowsFound(DBException): pass
class TooManyRowsFound(DBException): pass

##StandardError
##        |__Warning
##        |__Error
##           |__InterfaceError
##           |__DatabaseError
##              |__DataError
##              |__OperationalError
##              |__IntegrityError
##              |__InternalError
##              |__ProgrammingError
##              |__NotSupportedError

class Warning(DBException): pass
class Error(DBException): pass
class   InterfaceError(Error): pass
class   DatabaseError(Error): pass
class     DataError(DatabaseError): pass
class     OperationalError(DatabaseError): pass
class     IntegrityError(DatabaseError): pass
class     InternalError(DatabaseError): pass
class     ProgrammingError(DatabaseError): pass
class     NotSupportedError(DatabaseError): pass

def wrap_dbapi_exceptions(provider, func, *args, **keyargs):
    try: return func(*args, **keyargs)
    except provider.NotSupportedError, e: raise NotSupportedError(exceptions=[e])
    except provider.ProgrammingError, e: raise ProgrammingError(exceptions=[e])
    except provider.InternalError, e: raise InternalError(exceptions=[e])
    except provider.IntegrityError, e: raise IntegrityError(exceptions=[e])
    except provider.OperationalError, e: raise OperationalError(exceptions=[e])
    except provider.DataError, e: raise DataError(exceptions=[e])
    except provider.DatabaseError, e: raise DatabaseError(exceptions=[e])
    except provider.InterfaceError, e: raise InterfaceError(exceptions=[e])
    except provider.Error, e: raise Error(exceptions=[e])
    except provider.Warning, e: raise Warning(exceptions=[e])

class LongStr(str): pass
class LongUnicode(unicode): pass

sql_cache = {}
insert_cache = {}

def adapt_sql(sql, paramstyle):
    result = sql_cache.get((sql, paramstyle))
    if result is not None: return result
    pos = 0
    result = []
    args = []
    keyargs = {}
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
                key = 'p%d' % (len(keyargs) + 1)
                keyargs[key] = expr
                result.append(':' + key)
            elif paramstyle == 'pyformat':
                key = 'p%d' % (len(keyargs) + 1)
                keyargs[key] = expr
                result.append('%%(%s)s' % key)
            else: raise NotImplementedError
    adapted_sql = ''.join(result)
    if args:
        source = '(%s,)' % ', '.join(args)
        code = compile(source, '<?>', 'eval')
    elif keyargs:
        source = '{%s}' % ','.join('%r:%s' % item for item in keyargs.items())
        code = compile(source, '<?>', 'eval')
    else:
        code = compile('None', '<?>', 'eval')
        if paramstyle in ('format', 'pyformat'): sql = sql.replace('%%', '%')
    result = adapted_sql, code
    sql_cache[(sql, paramstyle)] = result
    return result

def get_session(create_session_if_not_exists=True):
    assert not create
    return None

next_num = count().next

class ConnectionInfo(object):
    __slots__ = 'con', 'num', 'optimistic'
    def __init__(info, con, optimistic):
        info.con = con
        info.num = next_num()
        info.optimistic = optimistic

class Local(localbase):
    def __init__(local):
        local.db2coninfo = {}

local = Local()        

select_re = re.compile(r'\s*select\b', re.IGNORECASE)

class Database(object):
    def __init__(database, provider, *args, **keyargs):
        if isinstance(provider, basestring): provider = import_module('pony.dbproviders.' + provider)
        database.provider = provider
        database.args = args
        database.keyargs = keyargs
        database._pool = provider.get_pool(*args, **keyargs)
        database.priority = 0
        database.optimistic = True
        info = database._get_connection()
        provider.release(info.con)
    def _get_connection(database):
        info = local.db2coninfo.get(database)
        if info is not None: return info
        provider = database.provider
        con = wrap_dbapi_exceptions(provider, provider.connect, database._pool, *database.args, **database.keyargs)
        info = local.db2coninfo[database] = ConnectionInfo(con, database.optimistic)
        return info
    def get_connection(database):
        info = database._get_connection()
        info.optimistic = False
        return info.con
    def commit(database):
        session = get_session(create_session_if_not_exists=False)
        if session is not None: session._commit(database)
        else: database._commit()
    def rollback(database):
        session = get_session(create_session_if_not_exists=False)
        if session is not None: session._rollback(database)
        else: database._rollback()
    def _commit(database):
        info = local.db2coninfo.pop(database, None)
        if info is None: return
        if debug: print 'COMMIT'
        provider = database.provider
        wrap_dbapi_exceptions(provider, info.con.commit)
        provider.release(info.con)
    def _rollback(database):
        info = local.db2coninfo.pop(database, None)
        if info is None: return
        if debug: print 'ROLLBACK'
        provider = database.provider
        wrap_dbapi_exceptions(provider, info.con.rollback)
        provider.release(info.con)
    def execute(database, sql, globals=None, locals=None):
        info = database._get_connection()
        info.optimistic = False
        sql = sql[:]  # sql = templating.plainstr(sql)
        if globals is None:
            assert locals is None
            globals = sys._getframe(1).f_globals
            locals = sys._getframe(1).f_locals
        provider = database.provider
        adapted_sql, code = adapt_sql(sql, provider.paramstyle)
        values = eval(code, globals, locals)
        cursor = info.con.cursor()
        if values is None: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql)
        else: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql, values)
        return cursor
    def select(database, sql, globals=None, locals=None):
        info = database._get_connection()
        sql = sql[:]  # sql = templating.plainstr(sql)
        if not select_re.match(sql): sql = 'select ' + sql
        if globals is None:
            assert locals is None
            globals = sys._getframe(1).f_globals
            locals = sys._getframe(1).f_locals
        provider = database.provider
        adapted_sql, code = adapt_sql(sql, provider.paramstyle)
        values = eval(code, globals, locals)
        cursor = info.con.cursor()
        if values is None: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql)
        else: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql, values)
        result = cursor.fetchmany(options.MAX_ROWS_COUNT)
        if cursor.fetchone() is not None: raise TooManyRowsFound
        if len(cursor.description) == 1: result = [ row[0] for row in result ]
        else:
            row_class = type("row", (tuple,), {})
            for i, column_info in enumerate(cursor.description):
                column_name = column_info[0]
                if not is_ident(column_name): continue
                if hasattr(tuple, column_name) and column_name.startswith('__'): continue
                setattr(row_class, column_name, property(itemgetter(i)))
            result = [ row_class(row) for row in result ]
        return result
    def get(database, sql, globals=None, locals=None):
        if globals is None:
            assert locals is None
            globals = sys._getframe(1).f_globals
            locals = sys._getframe(1).f_locals
        rows = database.select(sql, globals, locals)
        if not rows: raise RowNotFound
        if len(rows) > 1: raise MultipleRowsFound
        row = rows[0]
        return row
    def exists(database, sql, globals=None, locals=None):
        info = database._get_connection()
        sql = sql[:]  # sql = templating.plainstr(sql)
        if not select_re.match(sql): sql = 'select ' + sql
        if globals is None:
            assert locals is None
            globals = sys._getframe(1).f_globals
            locals = sys._getframe(1).f_locals
        provider = database.provider
        adapted_sql, code = adapt_sql(sql, provider.paramstyle)
        values = eval(code, globals, locals)
        cursor = info.con.cursor()
        if values is None: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql)
        else: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql, values)
        result = cursor.fetchone()
        return bool(result)
    def insert(database, table_name, **keyargs):
        info = database._get_connection()
        info.optimistic = False
        table_name = table_name[:]  # table_name = templating.plainstr(table_name)
        query_key = (table_name,) + tuple(keyargs)  # keys are not sorted deliberately!!
        cached_sql = insert_cache.get(query_key)
        if cached_sql is None:
            ast = [ INSERT, table_name, keyargs.keys(), [ [PARAM, i] for i in range(len(keyargs)) ] ]
            sql, adapter = database._ast2sql(ast)
            cached_sql = sql, adapter
            insert_cache[query_key] = cached_sql
        else: sql, adapter = cached_sql
        arguments = adapter(keyargs.values())  # order of values same as order of keys
        cursor = database._exec_sql(sql, arguments)
        return getattr(cursor, 'lastrowid', None)
    def _ast2sql(database, sql_ast):
        info = database._get_connection()
        sql, adapter = database.provider.ast2sql(info.con, sql_ast)
        return sql, adapter
    def _exec_sql(database, sql, arguments=None):
        info = database._get_connection()
        cursor = info.con.cursor()
        if debug:
            print sql
            print arguments
            print
        provider = database.provider
        if arguments is None: wrap_dbapi_exceptions(provider, cursor.execute, sql)
        else: wrap_dbapi_exceptions(provider, cursor.execute, sql, arguments)
        return cursor
    def exec_sql_many(database, sql, arguments_list=None):
        info = database._get_connection()
        info.optimistic = False
        cursor = info.con.cursor()
        if debug:
            print 'EXECUTEMANY', sql
            print arguments_list
            print
        provider = database.provider
        if arguments_list is None: wrap_dbapi_exceptions(provider, cursor.executemany, sql)
        else: wrap_dbapi_exceptions(provider, cursor.executemany, sql, arguments_list)
        return cursor
    def _commit_commands(database, commands):
        info = database._get_connection()
        cursor = info.con.cursor()
        provider = database.provider
        for command in commands:
            if debug: print 'DDLCOMMAND', command
            wrap_dbapi_exceptions(provider, cursor.execute, command)
        if debug: print 'COMMIT'
        wrap_dbapi_exceptions(provider, info.con.commit)

Database.Warning = Warning
Database.Error = Error
Database.InterfaceError = InterfaceError
Database.DatabaseError = DatabaseError
Database.DataError = DataError
Database.OperationalError = OperationalError
Database.IntegrityError = IntegrityError
Database.InternalError = InternalError
Database.ProgrammingError = ProgrammingError
Database.NotSupportedError = NotSupportedError
