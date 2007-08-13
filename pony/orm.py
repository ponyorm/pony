import sys, os.path, operator, thread, threading
from operator import attrgetter
from itertools import count, izip

from pony import utils
from pony.thirdparty import etree

class DiagramError(Exception): pass
class MappingError(Exception): pass
class TransactionError(Exception): pass

def _error_method(self, *args, **keyargs):
    raise TypeError

class Local(threading.local):
    def __init__(self):
        self.transaction = None

local = Local()

class Transaction(object):
    def __init__(self, data_source):
        if local.transaction is not None: raise TransactionError(
            'Transaction already started in thread %d' % thread.get_ident())
        self.data_source = data_source
        self.diagrams = set()
        self.cache = {} # Table -> TableCache
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
    def commit(self):
        raise NotImplementedError
    def rollback(self):
        raise NotImplementedError

def get_transaction():
    return local.transaction

def begin():
    if local.transaction is not None: raise TransactionError(
        'Transaction already started in thread %d' % thread.get_ident())
    outer_dict = sys._getframe(1).f_locals
    data_source = outer_dict.get('_data_source_')
    if data_source is None: raise TransactionError(
        'Can not start transaction, because default data source is not set')
    return Transaction(data_source)

def commit():
    tr = local.transaction
    if tr is None: raise TransactionError(
        'Transaction not started in thread %d' % thread.get_ident())
    tr.commit()

def rollback():
    tr = local.transaction
    if tr is None: raise TransactionError(
        'Transaction not started in thread %d' % thread.get_ident())
    tr.rollback()

class DataSource(object):
    _lock = threading.Lock() # threadsafe access to cache of datasources
    _cache = {}
    def __new__(cls, provider, *args, **keyargs):
        self = object.__new__(cls)
        self._init_(provider, *args, **keyargs)
        key = (self.provider, self.mapping, self.args,
               tuple(sorted(self.keyargs.items())))
        return cls._cache.setdefault(key, self) # is it thread safe?
               # I think - yes, if args & keyargs only contains
               # types with C-written __eq__ and __hash__
    def _init_(self, provider, *args, **keyargs):
        self.provider = provider
        self.args = args
        self.keyargs = keyargs
        self.mapping = keyargs.pop('mapping', None)
    def get_connection(self):
        provider = self.provider
        if isinstance(provider, basestring):
            provider = utils.import_module('pony.dbproviders.' + provider)
        return provider.connect(*self.args, **self.keyargs)

next_id = count().next

class Attribute(object):
    def __init__(self, py_type, *args, **keyargs):
        self._id_ = next_id()
        self.py_type = py_type
        self.name = None
        self.entity = None
        self.args = args
        self.options = keyargs
        self.reverse = keyargs.pop('reverse', None)
        if self.reverse is None: pass
        elif not isinstance(self.reverse, basestring): raise TypeError(
            "Value of 'reverse' option must be name of reverse attribute)")
        elif not (self.py_type, basestring):
            raise DiagramError('Reverse option cannot be set for this type %r'
                            % self.py_type)
    def __str__(self):
        owner_name = self.entity is None and '?' or self.entity.__name__
        return '%s.%s' % (owner_name, self.name or '?')
    def __repr__(self):
        return '<%s: %s>' % (self, self.__class__.__name__)

class Optional(Attribute):
    pass

class Required(Attribute):
    pass

class Unique(Required):
    def __new__(cls, *args, **keyargs):
        if not args: raise TypeError('Invalid count of positional arguments')
        attrs = tuple(a for a in args if isinstance(a, Attribute))
        non_attrs = [ a for a in args if not isinstance(a, Attribute) ]
        if attrs and non_attrs: raise TypeError('Invalid arguments')
        cls_dict = sys._getframe(1).f_locals
        keys = cls_dict.setdefault('_keys_', set())
        if issubclass(cls, PrimaryKey): tuple_class = _PrimaryKeyTuple
        else: tuple_class = tuple
        if not attrs:
            result = Required.__new__(cls, *args, **keyargs)
            keys.add(tuple_class((result,)))
            return result
        else: keys.add(tuple_class(attrs))

class PrimaryKey(Unique):
    pass

class _PrimaryKeyTuple(tuple):
    pass

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
    def __iter__(cls):
        return iter(())

class Entity(object):
    __metaclass__ = EntityMeta
    @classmethod
    def _cls_init_(cls, diagram):
        bases = [ c for c in cls.__bases__
                    if issubclass(c, Entity) and c is not Entity ]
        cls._bases_ = bases
        if bases:
            roots = set(c._root_ for c in bases)
            if len(roots) > 1: raise DiagramError(
                'With multiple inheritance of entities, '
                'inheritance graph must be diamond-like')
            cls._root_ = roots.pop()
            for c in bases:
                if c._diagram_ is not diagram: raise DiagramError(
                    'When use inheritance, base and derived entities '
                    'must belong to same diagram')
        else:
            cls._root_ = cls
            cls._diagram_ = diagram

        base_attrs = {}
        for c in cls._bases_:
            for a in c._attrs_:
                if base_attrs.setdefault(a.name, a) is not a:
                    raise DiagramError('Ambiguous attribute name %s' % a.name)

        cls._attrs_ = []
        for name, attr in cls.__dict__.items():
            if name in base_attrs: raise DiagramError(
                'Name %s hide base attribute %s' % (attr, base_attrs[name]))
            if not isinstance(attr, Attribute): continue
            if attr.entity is not None:
                raise DiagramError('Duplicate use of attribute %s' % value)
            attr.name = name
            attr.entity = cls
            cls._attrs_.append(attr)
        cls._attrs_.sort(key=attrgetter('_id_'))

        if not hasattr(cls, '_keys_'): cls._keys_ = set()
        primary_keys = set(key for key in cls._keys_
                               if isinstance(key, _PrimaryKeyTuple))
        if cls._bases_:
            if primary_keys: raise DiagramError(
                'Primary key cannot be redefined in derived classes')
            assert hasattr(cls, '_primary_key_')
            for base in cls._bases_: cls._keys_.update(base._keys_)
        elif len(primary_keys) > 1: raise DiagramError(
            'Only one primary key can be defined in each entity class')
        elif not primary_keys:
            if hasattr(cls, 'id'): raise DiagramError("Name 'id' alredy in use")
            _keys_ = set()
            attr = PrimaryKey(int) # Side effect: modifies _keys_ local variable
            attr.name = 'id'
            attr.entity = cls
            type.__setattr__(cls, 'id', attr)  # cls.id = attr
            cls._primary_key_ = _keys_.pop()
            cls._attrs_.insert(0, cls._primary_key_[0])
            cls._keys_.add(cls._primary_key_)
        else: cls._primary_key_ = primary_keys.pop()
        
        diagram.add_entity(cls)
    @classmethod
    def _cls_setattr_(cls, name, value):
        if name.startswith('_') and name.endswith('_'):
            type.__setattr__(cls, name, value)
        else: raise NotImplementedError
    @classmethod
    def _cls_get_info(cls):
        return diagram.get_entity_info(self)
        
class Diagram(object):
    def __init__(self):
        self.lock = threading.RLock()
        self.entities = {} # entity_name -> entity
        self.schemata = {} # mapping -> schema
        self.transactions = set()
    def clear(self):
        self.lock.acquire()
        try: self._clear()
        finally: self.lock.release()
    def _clear(self):
        if self.transactions: raise DiagramError(
            'Cannot change entity diagram '
            'because it is used by active transaction')
        self.schemata.clear()
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
        self.entity_to_tables = {}
        self.attr_to_columns = {}

class Table(object):
    def __init__(self, name):
        self.name = name
        self.entities = []
        self.columns = []

class Column(object):
    def __init__(self, table, name):
        self.name = name
        self.attrs = []
        self.table = table
        table.columns.append(self)

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
        if not os.path.exists(filename):
            raise MappingError('File not found: %s' % filename)
        document = etree.parse(filename)
        for telement in document.findall('table'):
            table = TableMapping(telement)
            if self.tables.setdefault(table.name, table) is not table:
                raise MappingError('Duplicate table definition: %s'%table.name)

class TableMapping(object):
    def __init__(self, element):
        self.name = element.get('name')
        if not self.name:
            raise MappingError("Table element without 'name' attribute")
        self.entities = element.get('entity', '').split()
        self.relations = element.get('relation', '').split()
        if self.entities and self.relations: raise MappingError(
            'For table %r both entity name and relations are specified. '
            'It is not allowed' % table.name)
        self.columns = []
        self.cdict = {}
        for celement in element.findall('column'):
            col = ColumnMapping(self.name, celement)
            if self.cdict.setdefault(col.name, col) is not col:
                raise MappingError('Duplicate column definition: %s.%s'
                                   % (self.name, col.name))
            self.columns.append(col)

class ColumnMapping(object):
    def __init__(self, table_name, element):
        self.name = element.get('name')
        if not self.name: raise MappingError(
            'Error in table definition %r: '
            'Column element without "name" attribute' % table_name)
        self.domain = element.get('domain')
        self.attrs = [ attr.split('.')
                       for attr in element.get('attr', '').split() ]
        self.kind = element.get('kind')
        if self.kind not in (None, 'discriminator'): raise MappingError(
            'Error in column definition %s.%s: invalid column kind: %s'
            % (table_name, self.name, self.kind))
        cases = element.findall('case')
        if cases and self.kind != 'discriminator': raise MappingError(
            'Non-discriminator column %s.%s contains cases.It is not allowed'
            % (table_name, self.name))
        self.cases = [ (case.get('value'), case.get('entity'))
                       for case in cases ]
        for value, entity in self.cases:
            if not value or not entity: raise MappingError(
                'Invalid discriminator case in column %s.%s'
                % (table_name, self.name))
