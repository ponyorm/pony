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
        key = (provider, args, tuple(sorted(kwargs.items())))
        return cls._cache.setdefault(key, self)
        # is it thread safe? I think - yes, if args & kwargs only contains
        #   types with C-written __eq__ and __hash__
    def _init_(self, provider, *args, **kwargs):
        self.provider = provider
        self.args = args
        self.kwargs = kwargs
        kwargs_hash = hash(tuple(sorted(self.iteritems())))
        self.hash = hash(provider) ^ hash(args) ^ kwargs_hash
        self.mapping = Mapping(kwargs.get('mapping', None))
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
        if self.reverse is not None and not isinstance(self.reverse,basestring):
            raise TypeError("Type of 'reverse' argument must be string "
                            "(name of reverse attribute)")
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

class Unique(Required):
    def __new__(cls, *args, **options):
        if not args: raise TypeError('Invalid count of positional arguments')
        attrs = [ a for a in args if isinstance(a, Attribute) ]
        non_attrs = [ a for a in args if not isinstance(a, Attribute) ]
        if attrs and non_attrs: raise TypeError('Invalid arguments')
        is_primary_key = issubclass(cls, PrimaryKey)
        cls_dict = sys._getframe(1).f_locals
        keys = cls_dict.setdefault('_pony_keys_', set())
        if attrs:
            key = EntityKey(attrs, is_primary_key)
            keys.add(key)
            return key
        else:
            result = Required.__new__(cls, *args, **options)
            keys.add(EntityKey([result], is_primary_key))
            return result

class PrimaryKey(Unique):
    pass

class EntityKey(object):
    def __init__(self, attrs, is_primary_key):
        self.attrs = tuple(attrs)
        self.is_primary_key = is_primary_key

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

class EntityMeta(type):
    def __init__(cls, name, bases, dict):
        super(EntityMeta, cls).__init__(name, bases, dict)
        if 'Entity' not in globals(): return
        outer_dict = sys._getframe(1).f_locals
        diagramm = outer_dict.setdefault('_pony_diagramm_', Diagramm())
        cls._class_init_(diagramm)
    def __setattr__(cls, name, value):
        cls._cls_setattr_(name, value)

class Entity(object):
    __metaclass__ = EntityMeta
    @classmethod
    def _class_init_(cls, diagramm):
        bases = [ c for c in cls.__bases__
                    if issubclass(c, Entity) and c is not Entity ]
        # cls._bases_ = bases
        type.__setattr__(cls, '_bases_', bases)
        if bases:
            roots = set(c._root_ for c in bases)
            if len(roots) > 1: raise TypeError(
                'With multiple inheritance of entities, '
                'inheritance graph must be diamond-like')
            # cls._root_ = roots.pop()
            type.__setattr__(cls, '_root_', roots.pop())
        else:
            # cls._root_ = cls
            type.__setattr__(cls, '_root_', cls)

        base_attrs = {}
        for c in cls._bases_:
            for a in c._attrs_:
                if base_attrs.setdefault(a.name, a) is not a:
                    raise TypeError('Ambiguous attribute name %s' % a.name)

        # cls._attrs_ = []
        type.__setattr__(cls, '_attrs_', [])
        for name, a in cls.__dict__.items():
            if name in base_attrs: raise TypeError(
                'Name %s hide base attribute %s' % (a, base_attrs[name]))
            if not isinstance(a, Attribute): continue
            if a.owner is not None:
                raise TypeError('Duplicate use of attribute %s' % value)
            a.name = name
            a.owner = cls
            cls._attrs_.append(a)
        cls._attrs_.sort(key=attrgetter('_id_'))

        # cls._keys_ = cls.__dict__.pop('_pony_keys_', set())
        try: keys = cls._pony_keys_
        except AttributeError: keys = set()
        else: del cls._pony_keys_
        type.__setattr__(cls, '_keys_', keys)
        primary_keys = set(key for key in cls._keys_ if key.is_primary_key)
        for base in cls._bases_: cls._keys_.update(base._keys_)
        if cls._bases_:
            if primary_keys: raise TypeError(
                'Primary key cannot be redefined in derived classes')
            assert hasattr(cls, '_primary_key_')
        elif len(primary_keys) > 1: raise TypeError(
            'Only one primary key can be defined in each entity class')
        elif not primary_keys:
            if hasattr(cls, 'id'): raise TypeError("Name 'id' alredy in use")
            _pony_keys_ = set()
            attr = PrimaryKey(int) # Side effect: modifies _pony_keys_ variable
            attr.name = 'id'
            attr.owner = cls
            # cls.id = attr
            type.__setattr__(cls, 'id', attr)
            # cls._primary_key_ = _pony_keys_.pop()
            type.__setattr__(cls, '_primary_key_', _pony_keys_.pop())
            cls._keys_.add(cls._primary_key_)
        else:
            assert len(primary_keys) == 1
            # cls._primary_key_ = primary_keys.pop()
            type.__setattr__(cls, '_primary_key_', primary_keys.pop())
        
        diagramm.add_entity(cls)
    @classmethod
    def _cls_setattr_(cls, name, value):
        pass

class Diagramm(object):
    def __init__(self):
        self.entities = {} # entity_name -> entity
        self.lock = threading.RLock()
    def add_entity(self, entity):
        self.lock.acquire()
        try:
            pass
        finally:
            self.lock.release()

class Schema(object):
    pass

class Transaction(object):
    pass
