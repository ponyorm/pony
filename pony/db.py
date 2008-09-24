import re, sys, threading

from pony.utils import import_module
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
class MultipleRowException(DBException): pass

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
class   InrefaceError(Error): pass
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
    except provider.InrefaceError, e: raise InrefaceError(exceptions=[e])
    except provider.Error, e: raise Error(exceptions=[e])
    except provider.Warning, e: raise Warning(exceptions=[e])

class Local(threading.local):
    def __init__(self):
        self.default_db = None
        self.connections = {}

local = Local()        

param_re = re.compile(r'[$]([$]|[A-Za-z_]\w*)')

sql_cache = {}

def adapt_sql(sql, paramstyle):
    result = sql_cache.get((sql, paramstyle))
    if result is not None: return result
    args, keyargs = [], {}
    if paramstyle == 'qmark':
        def replace(expr): args.append(expr); return '?'
    elif paramstyle == 'format':
        def replace(expr): args.append(expr); return '%s'
    elif paramstyle == 'numeric':
        def replace(expr): args.append(expr); return ':%d' % len(args)
    elif paramstyle == 'named':
        def replace(expr):
            key = 'param%d' % (len(keyargs) + 1)
            keyargs[key] = expr
            return ':' + key
    elif paramstyle == 'pyformat':
        def replace(expr):
            key = 'param%d' % (len(keyargs) + 1)
            keyargs[key] = expr
            return '%%(%s)s' % key
    else: assert False
    def replace_func(match):
        expr = match.group(1)
        if expr == '$': return '$'
        compile(expr, '<?>', 'eval')
        return replace(expr)
    adapted_sql = param_re.sub(replace_func, sql)
    params = args or keyargs or None
    if args:
        source = '(%s,)' % ', '.join(args)
        code = compile(source, '<?>', 'eval')
    elif keyargs:
        source = '{%s}' % ','.join('%r:%s' % item for item in keyargs.items())
        code = compile(source, '<?>', 'eval')
    else: code = compile('None', '<?>', 'eval')
    result = adapted_sql, code
    sql_cache[(sql, paramstyle)] = result
    return result

select_re = re.compile('\s*select\b', re.IGNORECASE)

class Database(object):
    def __init__(self, provider, *args, **keyargs):
        if isinstance(provider, basestring): provider = import_module('pony.dbproviders.' + provider)
        self.provider = provider
        self.args = args
        self.keyargs = keyargs
        self.sql_insert_cache = {}
        self.sql_update_cache = {}
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
    def select(self, sql, globals=None, locals=None):
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
        result = cursor.fetchall()
        if result and len(result[0]) == 1: result = [ row[0] for row in result ]
        return result
    def get(self, sql, globals=None, locals=None):
        rows = self.select(sql, globals, locals)
        if not rows: return None
        if len(rows) > 1: raise MultipleRowsException
        row = rows[0]
        return row
    def exists(self, sql, globals=None, locals=None):
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
    def execute(self, sql, globals=None, locals=None):
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
    def insert(self, table_name, **keyargs):
        items = keyargs.items()
        items.sort()
        con, provider = self._get_connection()
        key = table_name, tuple(name for name, value in items)
        x = self.sql_insert_cache.get(key)
        if x is None:
            ast = [ INSERT, table_name, [ name for name, value in items ], [ [PARAM, i] for i in range(len(items)) ] ]
            adapted_sql, params = provider.ast2sql(ast)
        else: adapted_sql, params = x
        if not isinstance(params, dict):
            for i, param in enumerate(params): assert param == i
            values = tuple(value for name, value in items)
        else: values = dict((key, items[i]) for key, i in params.items())
        cursor = con.cursor()
        wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql, values)
        return getattr(cursor, 'lastrowid', None)
    def update(self, table_name, where, globals=None, locals=None, **keyargs):
        items = keyargs.items()
        items.sort()
        con, provider = self._get_connection()
        key = table_name, tuple(name for name, value in items)
        try: sql1, params1 = self.sql_update_cache[key]
        except KeyError:
            ast = [ UPDATE, table_name, [ (name, [PARAM, i]) for i, (name, value) in enumerate(items) ] ]
            sql1, params1 = provider.ast2sql(ast)

        sql2, code = adapt_sql(where, provider.paramstyle)
        if globals is None:
            assert locals is None
            globals = sys._getframe(1).f_globals
            locals = sys._getframe(1).f_locals
        values2 = eval(code, globals, locals)
        adapted_sql = sql1 + ' WHERE ' + sql2
        if params1.__class__ is tuple:
            for i, param in enumerate(params1): assert param == i
            values = tuple(value for name, value in items) + (values2 or ())
        elif params1.__class__ is dict:
            values = dict((key, items[i]) for key, i in params1.items())
            values.update(values2)
        else: assert False
        cursor = con.cursor()
        wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql, values)
    def delete(self, table_name, where, globals=None, locals=None):
        if globals is None:
            assert locals is None
            globals = sys._getframe(1).f_globals
            locals = sys._getframe(1).f_locals
        con, provider = self._get_connection()
        sql = ('delete from %s where ' % provider.quote_name(table_name)) + where
        adapted_sql, code = adapt_sql(sql, provider.paramstyle)
        values = eval(code, globals, locals)
        cursor = con.cursor()
        if values is None: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql)
        else: wrap_dbapi_exceptions(provider, cursor.execute, adapted_sql, values)
    def commit(self):
        con, provider = self._get_connection()
        wrap_dbapi_exceptions(provider, con.commit)
    def rollback(self):
        con, provider = self._get_connection()
        wrap_dbapi_exceptions(provider, con.rollback)
    def release(self):
        x = local.connections.pop(self, None)
        if x is None: return
        x[0].release()

def release():
    for con, provider in local.connections.values():
        provider.release(con)
    local.connections.clear()

def _get_database():
    db = local.default_db
    if db is not None: return db
    db = sys._getframe(2).f_globals.get('__database__')
    if db is None: raise NoDefaultDbException('There is no default database defined')
    local.default_db = db
    return db

def get_connection():
    db = _get_database()
    return db.get_connection()

def select(sql):
    db = _get_database()
    return db.select(sql, sys._getframe(1).f_globals, sys._getframe(1).f_locals)
    
def get(sql):
    db = _get_database()
    return db.get(sql, sys._getframe(1).f_globals, sys._getframe(1).f_locals)

def exists(sql):
    db = _get_database()
    return db.exists(sql, sys._getframe(1).f_globals, sys._getframe(1).f_locals)

def execute(sql):
    db = _get_database()
    return db.execute(sql, sys._getframe(1).f_globals, sys._getframe(1).f_locals)

def insert(table_name, **keyargs):
    db = _get_database()
    return db.insert(table_name, **keyargs)

def update(table_name, table_name, where, **keyargs):
    db = _get_database()
    return db.update(table_name, where, sys._getframe(1).f_globals, sys._getframe(1).f_locals, **keyargs)

def delete(table_name, where):
    db = _get_database()
    return db.delete(table_name, where, sys._getframe(1).f_globals, sys._getframe(1).f_locals)

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

def with_transaction(func, *args, **keyargs):
    try: result = func(*args, **keyargs)
    except:
        exc_info = sys.exc_info()
        try:
            # write to log
            auto_rollback()
        finally:
            try: raise exc_info[0], exc_info[1], exc_info[2]
            finally: del exc_info
    else: auto_commit()
    return result
