import sys, os.path, operator, thread, threading
from operator import attrgetter
from itertools import count, izip

from pony import utils

try: import lxml.etree as ET
except ImportError:
    try: from xml.etree import ElementTree as ET
    except ImportError:
        try: import cElementTree as ET
        except ImportError:
            try: from elementtree import ElementTree as ET
            except: pass

class DiagramError(Exception): pass
class MappingError(Exception): pass
class TransactionError(Exception): pass

def _error_method(self, *args, **keyargs):
    raise TypeError

class Local(threading.local):
    def __init__(self):
        self.transaction = None

local = Local()

class DataSource(object):
    _lock = threading.Lock() # threadsafe access to cache of datasources
    _cache = {}
    def __new__(cls, provider, *args, **kwargs):
        self = object.__new__(cls)
        self._init_(provider, *args, **kwargs)
        key = (self.provider, self.mapping, self.args,
               tuple(sorted(self.kwargs.items())))
        return cls._cache.setdefault(key, self) # is it thread safe?
               # I think - yes, if args & kwargs only contains
               # types with C-written __eq__ and __hash__
    def _init_(self, provider, *args, **kwargs):
        self.provider = provider
        self.args = args
        self.kwargs = kwargs
        self.mapping = kwargs.pop('mapping', None)
    def begin(self):
        return Transaction(self)
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
        if self.reverse is None: pass
        elif not isinstance(self.reverse,basestring): raise TypeError(
            "Value of 'reverse' option must be name of reverse attribute)")
        elif not (self.py_type, basestring):
            raise DiagramError('Reverse option cannot be set for this type %r'
                            % self.py_type)
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

##class List(Collection): pass
##class Dict(Collection): pass
##class Relation(Collection): pass

class EntityMeta(type):
    def __init__(cls, name, bases, dict):
        super(EntityMeta, cls).__init__(name, bases, dict)
        if 'Entity' not in globals(): return
        outer_dict = sys._getframe(1).f_locals
        diagram = (dict.pop('_diagram_', None)
                   or outer_dict.get('_diagram_', None)
                   or outer_dict.setdefault('_diagram_', Diagram()))
        cls._cls_init_(diagram)
    def __setattr__(cls, name, value):
        cls._cls_setattr_(name, value)

class Entity(object):
    __metaclass__ = EntityMeta
    @classmethod
    def _cls_init_(cls, diagram):
        bases = [ c for c in cls.__bases__
                    if issubclass(c, Entity) and c is not Entity ]
        # cls._bases_ = bases
        type.__setattr__(cls, '_bases_', bases)
        if bases:
            roots = set(c._root_ for c in bases)
            if len(roots) > 1: raise DiagramError(
                'With multiple inheritance of entities, '
                'inheritance graph must be diamond-like')
            # cls._root_ = roots.pop()
            type.__setattr__(cls, '_root_', roots.pop())
            for c in bases:
                if c._diagram_ is not diagram: raise DiagramError(
                    'When use inheritance, base and derived entities '
                    'must belong to same diagram')
        else:
            # cls._root_ = cls
            # cls._diagram_ = diagram
            type.__setattr__(cls, '_root_', cls)
            type.__setattr__(cls, '_diagram_', diagram)

        base_attrs = {}
        for c in cls._bases_:
            for a in c._attrs_:
                if base_attrs.setdefault(a.name, a) is not a:
                    raise DiagramError('Ambiguous attribute name %s' % a.name)

        # cls._attrs_ = []
        type.__setattr__(cls, '_attrs_', [])
        for name, a in cls.__dict__.items():
            if name in base_attrs: raise DiagramError(
                'Name %s hide base attribute %s' % (a, base_attrs[name]))
            if not isinstance(a, Attribute): continue
            if a.owner is not None:
                raise DiagramError('Duplicate use of attribute %s' % value)
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
            if primary_keys: raise DiagramError(
                'Primary key cannot be redefined in derived classes')
            assert hasattr(cls, '_primary_key_')
        elif len(primary_keys) > 1: raise DiagramError(
            'Only one primary key can be defined in each entity class')
        elif not primary_keys:
            if hasattr(cls, 'id'): raise DiagramError("Name 'id' alredy in use")
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
        
        diagram.add_entity(cls)
    @classmethod
    def _cls_setattr_(cls, name, value):
        raise NotImplementedError
    @classmethod
    def _cls_get_info(cls):
        return diagram.get_entity_info(self)
        
class Diagram(object):
    def __init__(self):
        self.lock = threading.RLock()
        self.entities = {} # entity_name -> entity
        self.schemata = {} # mapping -> schema
        self.default_mapping = Mapping()
        self.transactions = set()
    def clear(self):
        self.lock.acquire()
        try:
            if self.transactions:
                raise DiagramError('Cannot change entity diagram '
                                   'because it is used by active transaction')
            self.schemata.clear()
        finally:
            self.lock.release()
    def add_entity(self, entity):
        self.lock.acquire()
        try:
            assert entity._diagram_ == self
            self._clear()
            # entity._diagram_ = self
            type.__setattr__(entity, '_diagram_', self)
            self.entities[entity.__name__] = entity
        finally:
            self.lock.release()
    def get_schema(self):
        transaction = local.transaction
        if transaction is None: raise TransactionError(
            'There are no active transaction in this thread: %d'
            % thread.get_ident())
        mapping = transaction.data_source.mapping
        self.lock.acquire()
        try:
            return (self.schemata.get(mapping)
                    or self.schemata.setdefault(mapping, Schema(self, mapping)))
        finally:
            self.lock.release()

class Schema(object):
    def __init__(self, diagram, mapping):
        self.mapping = mapping
        self.entities = {}  # entity -> entity_info
        self.tables = {}    # table_name -> table_info

class EntityInfo(object):
    pass

class FieldInfo(object):
    pass

class TableInfo(object):
    pass

def get_transaction():
    return local.transaction

class Transaction(object):
    def __init__(self, data_source):
        if local.transaction is not None: raise TransactionError(
            'Transaction already started in thread %d' % thread.get_ident())
        self.data_source = data_source
        self.diagrams = set()
        self.cache = {} # TableInfo -> TableCache
        local.transaction = self
    def _close(self):
        assert local.transaction is self
        while self.diagrams:
            diagram = self.diagrams.pop()
            # diagram.lock.acquire()
            # try:
            diagram.transactions.remove(self)
            # finally: diagram.lock.release()
        local.transaction = None

class Mapping(object):
    _cache = {}
    def __new__(cls, filename):
        mapping = cls._cache.get(filename)
        if mapping is not None: return mapping
        mapping = object.__new__(cls)
        mapping._init_(filename)
        return cls._cache.setdefault(filename, mapping)
    def _init_(self, filename):
        self.filename = filename
        self.tables = {}   # table_name -> TableMapping
        self.entities = {} # entity_name -> EntityMapping
        if not os.path.exists(filename):
            raise MappingError('File not found: %s' % filename)
        document = ET.parse(filename)
        for t in document.findall('table'):
            table = TableMapping(self, t.get('name'))
            ename = t.get('entity')
            relations = t.get('relations')
            if ename and relations: raise MappingError(
                'For table %r specified both entity name and relations. '
                'It is not allowed' % table.name)
            elif ename:
                entity = self.entities.get(ename) or EntityMapping(self, ename)
                table.entity = entity
                entity.tables.append(table)
            else:
                table.relations = relations.split()
                # for r in table.relations...
            for c in t.findall('column'):
                table.add_column(c.get('name'), c.get('kind'), c.get('attr'))

class TableMapping(object):
    def __init__(self, mapping, name):
        if not name:
            raise MappingError("Table element without 'name' attribute")
        if name in mapping.tables:
            raise MappingError('Duplicate table definition: %s' %name)
        mapping.tables[name] = self
        self.mapping = mapping
        self.name = name
        self.columns = []
        self.cdict = {}
        self.entity = None
        self.relations = []
    def add_column(self, name, kind, attr):
        column = ColMapping(self, name, kind, attr)
        self.columns.append(column)
        self.cdict[name] = column
        return column
    def add_entity(self, entity):
        self.entities.append(entity)
        self.edict[entity.name] = entity
        entity.tables.append(self)

class ColMapping(object):
    def __init__(self, table, name, kind, attr):
        if not name: raise MappingError('Error in table definition %r: '
            "Column element without 'name' attribute" % tname)
        if name in table.cdict:
            raise MappingError('Error in table definition %r: '
                'Duplicate column definition: %s' % (tname, cname))
        if kind and kind not in ('discriminator'):
            raise MappingError('Error in table definition %r: '
                            'invalid column kind: %s' % (table.name, kind))
        self.mapping = mapping = table.mapping
        self.table = table
        self.name = name
        self.kind = kind
        if attr:
            ename, fname = attr.split('.', 1)
            entity = mapping.entities.get(ename) or EntityMapping(mapping,ename)
            entity.add_field(fname, self)

class EntityMapping(object):
    def __init__(self, mapping, name):
        if not utils.is_ident(name): raise MappingError(
            'Entity name must be correct Python identifier. Got: %s' % ename)
        mapping.entities[name] = self
        self.mapping = mapping
        self.name = name
        self.tables = []
        self.fields = []
        self.fdict = {}
    def add_field(self, name, column):
        field = self.fdict.get(name)
        if not field:
            field = self.fdict[name] = FieldMapping(self, name)
            self.fields.append(field)
        assert column not in field.columns
        field.columns.append(column)
        column.field = field

class FieldMapping(object):
    def __init__(self, entity, name):
        self.mapping = entity.mapping
        self.entity = entity
        self.name = name
        self.columns = []
