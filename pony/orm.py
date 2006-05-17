# -*- coding: cp1251 -*-

import itertools, operator, sys, threading

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

next_id = itertools.count().next

class Attribute(object):
    __slots__ = '_id', '_initialized', 'name', 'full_name', 'py_type', 'options'
    def __init__(self, py_type, **options):
        self._id = next_id()
        self._initialized = False
        self.py_type = py_type
        self.options = options
        self.name = None
        self.owner = None
    def _init_(self):
        assert not self._initialized
        if isinstance(self.py_type, type) \
           and issubclass(self.py_type, Persistent):
            self._init_reverse()
        self._initialized = True
    def _init_reverse(self):
        t = self.py_type
        reverse = self.options.get('reverse')
        if isinstance(reverse, str):
            reverse = getattr(t, reverse, None)
            if reverse is None: raise AttributeError(
                'Reverse attribute for %s not found' % self)
        if reverse is None:
            candidates = [ attr for attr in t._attrs_
                                if attr.py_type is self.owner
                                and attr.options.get('reverse') \
                                    in (self, self.name, None)  ]
            for attr in candidates:
                if attr.options.get('reverse') in (self, self.name):
                    if reverse is not None: raise AttributeError(
                        'Ambiguous reverse attribute for %s' % self)
                    reverse = attr
            if reverse is None:
                if len(candidates) == 0: raise AttributeError(
                    'Reverse attribute for %s not found' % self)
                if len(candidates) > 1: raise AttributeError(
                    'Ambiguous reverse attribute for %s' % self)
                reverse = candidates[0]
        if not isinstance(reverse, Attribute): raise AttributeError(
            'Incorrect reverse attribute type for %s' % self)
        if reverse.py_type is not self.owner \
           or reverse.options.get('reverse') not in (self, self.name, None):
            raise AttributeError('Inconsistent attributes %s and %s'
                                 % (self, reverse))
        self.options['reverse'] = reverse
        assert reverse.options.get('reverse') in (self, self.name, None)
        reverse.options['reverse'] = self
    def __str__(self):
        if self.owner is None: return '?.%s' % (self.__name__ or '?')
        return '%s.%s' % (self.owner.__name__, self.name or '?')
    def __repr__(self):
        return '<%s : %s>' % (self, self.__class__.__name__)
    def __get__(self, obj, type=None):
        return self
    def __set__(self, obj, value):
        pass
    def __delete__(self, obj):
        pass

class Optional(Attribute): pass
class Required(Attribute): pass
class Unique(Required):
    __slots__ = 'attrs',
    def __init__(self, *type_or_attrs):
        if len(type_or_attrs) == 0:
            assert TypeError('Invalid count of positional arguments')
        elif len(type_or_attrs) == 1:
            py_type = type_or_attrs
            Attribute.__init__(self, py_type)
            self.attrs = None
        else:
            Attribute.__init__(self, None)
            self.attrs = type_or_attrs
        cls_dict = sys._getframe(1).f_locals
        cls_dict.setdefault('_keys_', []).append(self)
       
class PrimaryKey(Unique): pass
        
class Collection(Attribute): pass
class Set(Collection): pass
class List(Collection): pass

################################################################################

class _PonyInfo(object):
    __slots__ = 'persistent_classes', 'uninitialized_attrs'
    def __init__(self):
        self.persistent_classes = {}  # map(class_name -> class)
        self.uninitialized_attrs = {} # map(referenced_class_name -> attr_list)

class PersistentMeta(type):
    def __init__(cls, cls_name, bases, cls_dict):
        super(PersistentMeta, cls).__init__(cls_name, bases, dict)
        outer_dict = sys._getframe(1).f_locals
        pony_info = outer_dict.get('_pony_info_')
        if pony_info is None:
            pony_info = outer_dict['_pony_info_'] = _PonyInfo()
        cls._cls_init_(cls_name, pony_info)

################################################################################

class Persistent(object):
    __metaclass__ = PersistentMeta
    @classmethod
    def _cls_init_(cls, cls_name, info):
        info.persistent_classes[cls_name] = cls
        my_attrs = cls._attrs_ = []
        for attr_name, x in cls.__dict__.items():
            if isinstance(x, Attribute):
                my_attrs.append(x)
                x.name = attr_name
                x.owner = cls
        my_attrs.sort(key = operator.attrgetter('_id'))

        other_attrs = info.uninitialized_attrs.get(cls_name, [])
        for attr in other_attrs:
            attr.py_type = cls
            if 'reversed' in attr.options:
                other_attrs.remove(attr)
                attr._init_()
        for attr in my_attrs:
            if isinstance(attr.py_type, str):
                other_name = attr.py_type
                other_cls = info.persistent_classes.get(other_name)
                if other_cls is None:
                    u = info.uninitialized_attrs
                    u.setdefault(other_name, []).append(attr)
                    continue
                else: attr.py_type = other_cls
            if 'reverse' in attr.options: attr._init_()

        for attr in other_attrs: attr._init_()
        for attr in my_attrs:
            if not isinstance(attr.py_type, str):
                attr._init_()

        if not hasattr(cls, '_keys_'): cls._keys_ = []
        for key in cls._keys_:
            if not key._initialized:
                key.name = None
                key.owner = cls
                key.py_type = None
                key._init_()








