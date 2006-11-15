import sys, operator, thread, threading
from operator import attrgetter
from itertools import count, izip

try: import lxml.etree as ET
except ImportError:
    try: from xml.etree import ElementTree as ET
    except ImportError:
        try: import cElementTree as ET
        except ImportError:
            try: from elementtree import ElementTree as ET
            except: pass

def _error_method(self, *args, **keyargs):
    raise TypeError

class _Local(threading.local):
    def __init__(self):
        self.transaction = None

_local = threading.local()

class DataSource(object):
    _lock = threading.Lock() # threadsafe access to cache of datasources
    _cache = {}
    def __new__(cls, provider, *args, **kwargs):
        self = object.__new__(cls, provider, *args, **kwargs)
        self._init_(provider, *args, **kwargs)
        cls._lock.acquire()  # because __hash__ and __eq__ are not atomic
        try: return cls._cache.setdefault(self, self)
        finally: cls._lock.release()
    def _init_(self, provider, *args, **kwargs):
        self.provider = provider
        self.args = args
        self.kwargs = kwargs
        kwargs_hash = hash(tuple(sorted(self.iteritems())))
        self.hash = hash(provider) ^ hash(args) ^ kwargs_hash
        self.mapping = Mapping(kwargs.get('mapping', None))
    def __eq__(self, other):
        if other.__class__ is not DataSource: return NotImplemented
        return (self.hash == other.hash and self.provider == other.provider
                and self.args == other.args and self.kwargs == other.kwargs
                and self.mapping == other.mapping)
    def __ne__(self, other):
        return not self.__eq__(other)
    def __hash__(self):
        return self.hash
    def begin(self):
        if _local.transaction is not None: raise TransactionError(
            'Transaction already started in thread %d' % thread.get_ident())
        transaction = Transaction(self.get_connection(), self.mapping)
        _local.transaction = transaction
        return transaction
    def get_connection(self):
        provider = self.provider
        if isinstance(provider, basestring):
            provider = utils.import_module('pony.dbproviders.' + provider)
        return provider.connect(*self.args, **self.kwargs)

next_id = count().next

class Attribute(object):
    def __init__(self, py_type, **options):
        self._id_ = next_id()
        self.py_type = py_type
        self.name = None
        self.owner = None
        self.options = options
        self.reverse = options.pop('reverse', None)
        self.column = options.pop('column', None)
        self.table = options.pop('table', None)
    def __str__(self):
        owner_name = self.owner is None and '?' or self.owner.cls.__name__
        return '%s.%s' % (owner_name, self.name or '?')
    def __repr__(self):
        return '<%s: %s>' % (self, self.__class__.__name__)

class Optional(Attribute):
    pass

class Required(Attribute):
    pass

class Key(Attribute):
    def __new__(cls, *args, **options):
        if not args: raise TypeError('Invalid count of positional arguments')
        attrs = [ a for a in args if isinstance(a, Attribute) ]
        non_attrs = [ a for a in args if not isinstance(a, Attribute) ]
        if attrs and non_attrs: raise TypeError('Invalid arguments')
        if attrs: result = key = tuple(attrs)
        else:
            result = Attribute.__new__(cls, *args, **options)
            key = (result,)
##        cls_dict = sys._getframe(1).f_locals
##        if (issubclass(cls, PrimaryKey)
##            and cls_dict.setdefault('_primary_key_', key) != key):
##            raise TypeError('Only one primary key can be defined in each class')
##        cls_dict.setdefault('_keys_', set()).add(key)
        return result

class Unique(Key):
    pass

class OptionalUnique(Key):
    pass

class PrimaryKey(Key):
    pass

class Collection(Attribute):
    pass

class Set(Collection):
    pass

##class List(Collection):
##    pass
##
##class Dict(Collection):
##    pass
##
##class Relation(Collection):
##    pass

class Entity:
    pass
