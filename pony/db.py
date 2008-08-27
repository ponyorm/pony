import re, sys

from pony.utils import import_module

param_re = re.compile(r'[$]([$]|[A-Za-z_]\w*)')

sql_cache = {}

def adapt_sql(sql, paramstyle):
    result = sql_cache.get((sql, paramstyle))
    if result is not None: return result
    args, keyargs = [], {}
    if paramstyle == 'qmark':
        def replace(name): args.append(name); return '?'
    elif paramstyle == 'format':
        def replace(name): args.append(name); return '%s'
    elif paramstyle == 'numeric':
        def replace(name): args.append(name); return ':%d' % len(args)
    elif paramstyle == 'named':
        def replace(name):
            key = 'param%d' % (len(keyargs) + 1)
            keyargs[key] = name
            return ':' + key
    elif paramstyle == 'pyformat':
        def replace(name):
            key = 'param%d' % (len(keyargs) + 1)
            keyargs[key] = name
            return '%%(%s)s' % key
    else: assert False
    def replace_func(match):
        name = match.group(1)
        if name == '$': return '$'
        return replace(name)
    adapted_sql = param_re.sub(replace_func, sql)
    params = args or keyargs
    result = adapted_sql, params
    sql_cache[(sql, paramstyle)] = result
    return result

class Database(object):
    def __init__(self, provider, *args, **keyargs):
        if isinstance(provider, basestring):
            provider = utils.import_module('pony.dbproviders.' + provider)
        self.provider = provider
        self.args = args
        self.keyargs = keyargs
    def _get_connection(self):
        return self.provider.connect(*self.args, **self.keyargs)
    @property
    def connection():
        return self._get_connection().dbapi_connection
    def select(sql):
        pass
    def get(sql):
        pass
    def exists(sql):
        pass
    def execute(sql):
        pass
    def insert(table_name, *args, **keyargs):
        pass
    def update(table_name, where, *args, **keyargs):
        pass
    def delete(table_name, where):
        pass
    def commit():
        pass
    def rollback():
        pass
    