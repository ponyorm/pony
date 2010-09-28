import re, sys, threading

from operator import itemgetter

from pony import options
from pony.utils import import_module, parse_expr, is_ident, localbase, simple_decorator
from pony.sqlsymbols import *

class DBException(Exception):
    def __init__(self, *args, **keyargs):
        exceptions = keyargs.pop('exceptions', [])
        assert not keyargs
        if not args and exceptions:
            if len(exceptions) == 1: args = getattr(exceptions[0], 'args', ())
            else: args = ('Multiple exceptions have occured',)
        Exception.__init__(self, *args)
        self.exceptions = exceptions

class NoDefaultDbException(DBException): pass
class CommitException(DBException): pass
class RollbackException(DBException): pass

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

class Local(localbase):
    def __init__(self):
        self.default_db = None
        self.connections = {}

local = Local()        

sql_cache = {}

def adapt_sql(sql, paramstyle):
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
                key = 'param%d' % (len(keyargs) + 1)
                keyargs[key] = expr
                result.append(':' + key)
            elif paramstyle == 'pyformat':
                key = 'param%d' % (len(keyargs) + 1)
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

select_re = re.compile(r'\s*select\b', re.IGNORECASE)

class Database(object):
    def __init__(self, provider, *args, **keyargs):
        if isinstance(provider, basestring): provider = import_module('pony.dbproviders.' + provider)
        self.provider = provider
        self.args = args
        self.keyargs = keyargs
        self.sql_insert_cache = {}
        self.sql_update_cache = {}
        con = self.get_connection()
        self.release()
    def _get_connection(self):
        x = local.connections.get(self)
        if x is not None: return x[:2]
        provider = self.provider
        con = wrap_dbapi_exceptions(provider, provider.connect, *self.args, **self.keyargs)
        local.connections[self] = con, provider, len(local.connections)
        return con, provider
    def get_connection(self):
        con, provider = self._get_connection()
        return con
    def release(self):
        x = local.connections.pop(self, None)
        if x is None: return
        connection, provider, _ = x
        provider.release(connection)
    def commit(self):
        con, provider = self._get_connection()
        wrap_dbapi_exceptions(provider, con.commit)
    def rollback(self):
        con, provider = self._get_connection()
        wrap_dbapi_exceptions(provider, con.rollback)
    def execute(self, sql, globals=None, locals=None):
        sql = sql[:]  # sql = templating.plainstr(sql)
        if globals is None:
            assert locals is None
            globals = sys._getframe(1).f_globals
            locals = sys._getframe(1).f_locals
        con, provider = self._get_connection()
        adapted_sql, code = adapt_sql(sql, provider.paramstyle)
        values = eval(code, globals, locals)
        cursor = con.cursor()
        if values is None: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql)
        else: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql, values)
        return cursor
    def select(self, sql, globals=None, locals=None):
        sql = sql[:]  # sql = templating.plainstr(sql)
        if not select_re.match(sql): sql = 'select ' + sql
        if globals is None:
            assert locals is None
            globals = sys._getframe(1).f_globals
            locals = sys._getframe(1).f_locals
        con, provider = self._get_connection()
        adapted_sql, code = adapt_sql(sql, provider.paramstyle)
        values = eval(code, globals, locals)
        cursor = con.cursor()
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
    def get(self, sql, globals=None, locals=None):
        if globals is None:
            assert locals is None
            globals = sys._getframe(1).f_globals
            locals = sys._getframe(1).f_locals
        rows = self.select(sql, globals, locals)
        if not rows: raise RowNotFound
        if len(rows) > 1: raise MultipleRowsFound
        row = rows[0]
        return row
    def exists(self, sql, globals=None, locals=None):
        sql = sql[:]  # sql = templating.plainstr(sql)
        if not select_re.match(sql): sql = 'select ' + sql
        if globals is None:
            assert locals is None
            globals = sys._getframe(1).f_globals
            locals = sys._getframe(1).f_locals
        con, provider = self._get_connection()
        adapted_sql, code = adapt_sql(sql, provider.paramstyle)
        values = eval(code, globals, locals)
        cursor = con.cursor()
        if values is None: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql)
        else: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql, values)
        result = cursor.fetchone()
        return bool(result)
    def insert(self, table_name, **keyargs):
        table_name = table_name[:]  # table_name = templating.plainstr(table_name)
        items = keyargs.items()
        items.sort()
        con, provider = self._get_connection()
        key = table_name, tuple(name for name, value in items)
        x = self.sql_insert_cache.get(key)
        if x is None:
            ast = [ INSERT, table_name, [ name for name, value in items ], [ [PARAM, 'p%d' % i] for i in range(len(items)) ] ]
            adapted_sql, params = provider.ast2sql(con, ast)
        else: adapted_sql, params = x
        if not isinstance(params, dict):
            for i, param in enumerate(params): assert param == 'p%d' % i
            values = tuple(value for name, value in items)
        else: values = dict((key, items[i]) for key, i in params.items())
        cursor = con.cursor()
        wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql, values)
        return getattr(cursor, 'lastrowid', None)
    def _exec_ast(self, ast, params={}):
        con, provider = self._get_connection()
        sql, params_mapping = provider.ast2sql(con, ast)
        if not isinstance(params_mapping, dict):
            values = tuple(params[key] for key in params_mapping)
        else: values = dict((name, params[key]) for name, key in params_mapping.items())
        cursor = con.cursor()
        wrap_dbapi_exceptions(provider, cursor.execute, sql, values)
        return cursor

def use_db(db):
    local.default_db = db

def _get_database():
    db = local.default_db
    if db is None: raise NoDefaultDbException('There is no default database defined')
    return db

def get_connection():
    db = _get_database()
    return db.get_connection()

def release():
    for con, provider, _ in local.connections.values():
        provider.release(con)
    local.connections.clear()

def execute(sql):
    db = _get_database()
    return db.execute(sql, sys._getframe(1).f_globals, sys._getframe(1).f_locals)

def select(sql):
    db = _get_database()
    return db.select(sql, sys._getframe(1).f_globals, sys._getframe(1).f_locals)
    
def get(sql):
    db = _get_database()
    return db.get(sql, sys._getframe(1).f_globals, sys._getframe(1).f_locals)

def exists(sql):
    db = _get_database()
    return db.exists(sql, sys._getframe(1).f_globals, sys._getframe(1).f_locals)

def insert(table_name, **keyargs):
    db = _get_database()
    return db.insert(table_name, **keyargs)

def commit():
    db = _get_database()
    return db.commit()

def rollback():
    db = _get_database()
    return db.rollback()

def auto_commit():
    default_db = local.default_db
    databases = [ (num, db) for db, (con, provider, num) in local.connections.items() if db != default_db ]
    databases.sort()
    databases = [ db for num, db in databases ]
    if default_db is not None: databases.insert(0, default_db)
    if not databases: return
    # ...
    exceptions = []
    try:
        try: databases[0].commit()
        except:
            exceptions.append(sys.exc_info())
            for db in databases[1:]:
                try: db.rollback()
                except: exceptions.append(sys.sys.exc_info())
            raise CommitException(exceptions)
        for db in databases[1:]:
            try: db.commit()
            except: exceptions.append(sys.sys.exc_info())
        # write exceptions to log
    finally:
        del exceptions
        local.connections.clear()

def auto_rollback():
    exceptions = []
    try:
        for db in local.connections:
            try: db.rollback()
            except: exceptions.append(sys.sys.exc_info())
        if exceptions: raise RollbackException(exceptions)
    finally:
        del exceptions
        local.connections.clear()

def with_transaction(func, args, keyargs, allowed_exceptions=[]):
    try: result = func(*args, **keyargs)
    except Exception, e:
        exc_info = sys.exc_info()
        try:
            # write to log
            for exc_class in allowed_exceptions:
                if isinstance(e, exc_class):
                    auto_commit()
                    break
            else: auto_rollback()
        finally:
            try: raise exc_info[0], exc_info[1], exc_info[2]
            finally: del exc_info
    auto_commit()
    return result

@simple_decorator
def db_decorator(func, *args, **keyargs):
    web = sys.modules.get('pony.web')
    allowed_exceptions = web and [ web.HttpRedirect ] or []
    try: return with_transaction(func, args, keyargs, allowed_exceptions)
    except RowNotFound:
        if web: raise web.Http404NotFound
        raise
    