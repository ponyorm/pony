# -*- coding: cp1251 -*-

from operator import attrgetter
from threading import local

import pony.utils as utils

__all__ = ('connection',
           'Optional', 'Required', 'Unique', 'PrimaryKey',
           'Set', 'List',
           'Persistent',)

###############################################################################

class Local(threading.local):
    connection = (None, None)

_local = Local()

###############################################################################

def connection(provider, *args, **kwargs):
    return ConnectionInfo(provider, *args, **kwargs)

_connection_info_cache = {}

class ConnectionInfo(object):
    def __new__(cls, provider, *args, **kwargs):
        self = object.__new__(cls, provider, *args, **kwargs)
        cache = _connection_info_cache
        return cache.setdefault(self, self)
    def __init__(self, provider, *args, **kwargs):
        self._provider = provider_name
        self._args = args
        self._kwargs = utils.FrozenDict(kwargs)
        self._hash = hash(provider_name) ^ hash(args) ^ hash(kwargs)
    provider = property(lambda self: self._provider)
    args     = property(lambda self: self._args)
    kwargs   = property(lambda self: dict(self._kwargs))
    def __eq__(self, other):
        if other.__class__ is not Connection: return NotImplemented
        return (self._hash, self._provider, self._args, self._kwargs) \
               == (other._hash, other._provider, other._args, other._kwargs)
    def __hash__(self):
        return self._hash
    def open(self):
        if isinstance(provider, basestring):
              provider = utils.import_module('pony.db.providers.'
                                             + self._provider)
        else: provider = self._provider
        con_info, con = _local.connection # must be (None, None)
        if con_info is self:
            raise "This connection is already opened in the current thread"
        elif con is not None:
            raise "Another connection is already opened in the current thread"
        _local.connection = self, provider.connect(*self._args, **self._kwargs)
    def _get_connection(self):
        con_info, con = _local.connection
        if con_info is not self:
            raise "This connection is not opened in the current thread"
        return con
    def commit(self):
        self._get_connection().commit()
    def abort(self):
        self._get_connection().abort()
    def close(self):
        self._get_connection().close()
       _local.connection = (None, None)
    def callproc(self, procname, parameters=None):
        self._get_connection().callproc(procname, parameters)
    def get(self, operation, parameters=None):
        self._get_connection().get(operation, parameters)
    def fetchone(self, operation, parameters=None):
        self._get_connection().fetchone(operation, parameters)
    def execute(self, operation, parameters=None):
        self._get_connection().execute(operation, parameters)
    def executemany(self, operation, seq_of_parameters):
        self._get_connection().executemany(operation, seq_of_parameters)

###############################################################################



class Attribute(object):
    def __init__(self, type,
                 column=None, reverse=None, default=None, **options):
        self.index = utils.get_next_index(1)
        self.type = type
        self.column = column
        self.reverse = reverse
        self.default = default
        self.options = options
    def __get__(self, obj, type=None):
        if obj is None:
            return self.name
        else:
            return self.index
    def __set__(self, obj, value):
        pass
    def __delete__(self, obj):
        pass

class Optional(Attribute): pass
class Required(Attribute): pass
class Unique(Required): pass
class PrimaryKey(Unique): pass

class Relation(object): pass
class Set(Relation): pass
class List(Relation): pass

###############################################################################

class PersistentMeta(type):
    def __new__(meta, name, bases, dict):
        utils.clear_system_stuff(dict)
        attrs = []
        for name, value in dict.items():
            if isinstance(value, Attribute):
                if getattr(value, 'owner', None) is not None:
                    raise AttributeError('Single attribute cannot be used'
                                         ' in several persistent classes')
                value.name = name
                attrs.append(value)
        attrs.sort(key=attrgetter('index'))
        for index, attr in enumerate(attrs): attr.index = index
        cls = super(PersistentMeta, meta).__new__(meta, name, bases, dict)
        cls._attrs_ = attrs
        for attr in attrs: attr.owner = cls
        return cls
    def __init__(cls, name, bases, dict):
        if not hasattr(cls, '_table_name_'):
            cls._table_name_ = cls.__name__
    def __setattr__(cls, name, value):
        if isinstance(getattr(cls, name, None), Attribute):
            raise TypeError('Attributes cannot be reassigned'
                             ' after persistent class creation')
        if isinstance(value, Attribute):
            raise TypeError('New attributes cannot be assigned'
                             ' after persistent class creation')
        super(PersistentMeta, cls).__setattr__(name, value)

###############################################################################

class Persistent(object):
    __metaclass__ = PersistentMeta
    @staticmethod
    def table_name_from_class_name(name):
        return name
    @staticmethod
    def column_name_from_attr_name(name):
        return utils.camelcase_name(name)
    @staticmethod
    def attr_name_from_column_name(name):
        return utils.lowercase_name(name)
    def __init__(self):
        pass

###############################################################################
    
if __name__ == '__main__':
    import doctest
    failed, total = doctest.testfile('persistence.test',
                                     optionflags=doctest.ELLIPSIS)
    print 'passed %d of %d' % (total - failed, total)







    
