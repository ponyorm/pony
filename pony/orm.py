# -*- coding: cp1251 -*-

import threading

import utils

__all__ = ('database', 'PrimaryKey', 'Unique', 'Required', 'Optional',
           'Set', 'List', 'Persistent')

################################################################################

_local = threading.local()

class DatabaseError(Exception): pass

def database(*args, **kwargs):
    return DatabaseInfo(*args, **kwargs)

class DatabaseInfo(object):
    _lock = threading.Lock()
    _cache = {}

    def __new__(cls, provider, *args, **kwargs):
        self = object.__new__(cls, provider, *args, **kwargs)
        self._lock.acquire()
        try: return self._cache.setdefault(self, self)
        finally: self._lock.release()

    def __init__(self, provider, *args, **kwargs):
        kwargs = utils.FrozenDict(kwargs)
        self._hash = hash(provider) ^ hash(args) ^ hash(kwargs)
        self._provider_name = provider
        self._args = args
        self._kwargs = kwargs

    provider = property(lambda self: self._provider)
    args     = property(lambda self: self._args)
    kwargs   = property(lambda self: self._kwargs)

    def __eq__(self, other):
        if other.__class__ is not DatabaseInfo: return NotImplemented
        return (self._hash, self._provider_name, self._args, self._kwargs) \
               == (other._hash, other._provider, other._args, other._kwargs)

    def __hash__(self):
        return hash(self._provider) ^ hash(self._args) ^ hash(self._kwargs)
        
    def open(self):
        if isinstance(self._provider, basestring):
              provider = utils.import_module('pony.dbproviders.'
                                             + self._provider)
        else: provider = self._provider
        if getattr(_local, 'connection', None) is None:
            raise DatabaseError('Connection already opened')
        _local.database = self
        _local.connection = provider.connect(*self._args, **self._kwargs)

    def close(self):
        self._get_connection().close()
        _local.connection = (None, None)
        
    def _get_connection(self):
        if getattr(_local, 'database', None) is not self:
            raise DatabaseError, 'The connection is not opened'
        return _local.connection
    connection = property(_get_connection)

##    def __getattr__(self, name):
##        return getattr(self._get_connection(), name)

################################################################################

class Attribute(object):
    pass

class Optional(Attribute): pass
class Required(Attribute): pass
class Unique(Required): pass
class PrimaryKey(Unique): pass

class Relation(object): pass
class Set(Relation): pass
class List(Relation): pass

################################################################################

class PersistentMeta(type):
    pass

################################################################################

class Persistent(object):
    __metaclass__ = PersistentMeta










