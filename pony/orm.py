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
        transaction = Transaction(self)
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
        transaction = _local.transaction
        if transaction is None: raise TransactionError(
            'There are no active transaction in this thread: %d'
            % thread.get_ident())
        data_source = transaction.data_source
        diagram = cls._diagram_
        
class Diagram(object):
    def __init__(self):
        self.lock = threading.RLock()
        self.initialized = False
        self.entities = {} # entity_name -> entity
        self.schemata = {} # data_source -> schema
        self.default_mapping = Mapping()
        self.transactions = set()
    def _clear(self): # Must be protected by lock!
        if self.transactions: raise DiagramError(
            'Cannot change entity diagram '
            'because it is used by active transaction')
        self.initialized = False
        self.schemata.clear()
    def add_entity(self, entity):
        self.lock.acquire()
        try:
            assert entity._diagram_ == self
            self._clear()
            # entity._schema_ = self
            type.__setattr__(entity, '_diagram_', self)
            self.entities[entity.__name__] = entity
        finally:
            self.lock.release()
    def init(self):
        pass

class Schema(object):
    def __init__(self):
        pass

class EntityInfo(object):
    pass

def get_transaction():
    return _local.transaction

class Transaction(object):
    def __init__(self, data_source):
        self.data_source = data_source
        self.connection = data_source.get_connection()

class Mapping(object):
    def __init__(self, filename=None, xml=None):
        if filename and xml: raise MappingError(
            "You must not supply both 'filename' and 'xml' attributes")
        self.filename = filename
        if self.filename:
            if not os.path.exists(filename):
                raise MappingError('File not found: %s' % filename)
            self.xml = ET.parse(filename)
        elif isinstance(xml, basestring): self.xml = ET.fromstring(xml)
        else:
            assert hasattr(xml, 'findall')
            self.xml = xml
        self.tables = {}   # table_name -> TableMapping
        self.entities = {} # entity_name -> EntityMapping
        self._load()
    def _load(self):
        for table_element in self.xml.findall('table'):
            table = TableMapping(self, table_element.get('name'))
            enames = table_element.get('entity', '').split()
            assert len(enames) == len(set(enames))
            for ename in enames:
                entity = self.entities.get(ename) or EntityMapping(self, ename)
                table.add_entity(entity)
            for col_element in table_element.findall('column'):
                column = table.add_column(col_element.get('name'),
                                          col_element.get('kind'))
                attr = col_element.get('attr')
                if not attr: continue
                ename, fname = attr.split('.', 1)
                entity = table.edict.get(ename)
                if not entity: raise MappingError(
                    'Error in table definition %r: '
                    'Entity name %r uses in attribute %s.%s, '
                    "but it is absent in table's 'entity' attribute"
                    % (tname, ename, fname))
                entity.add_field(fname, column)

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
        self.entities = []
        self.edict = {}
    def add_column(self, name, kind):
        column = ColMapping(self, name, kind)
        self.columns.append(column)
        self.cdict[name] = column
        return column
    def add_entity(self, entity):
        self.entities.append(entity)
        self.edict[entity.name] = entity
        entity.tables.append(self)

class ColMapping(object):
    def __init__(self, table, name, kind):
        if not name: raise MappingError('Error in table definition %r: '
            "Column element without 'name' attribute" % tname)
        if name in table.cdict:
            raise MappingError('Error in table definition %r: '
                'Duplicate column definition: %s' % (tname, cname))
        if kind and kind not in ('discriminator'):
            raise MappingError('Error in table definition %r: '
                            'invalid column kind: %s' % (table.name, kind))
        self.mapping = table.mapping
        self.table = table
        self.name = name
        self.kind = kind

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
