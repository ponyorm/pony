import sys, os.path, operator, thread, threading
from operator import attrgetter
from itertools import count, izip

from pony import utils
from pony.thirdparty import etree

class OrmError(Exception): pass

class DiagramError(OrmError): pass
class SchemaError(OrmError): pass
class MappingError(OrmError): pass
class TransactionError(OrmError): pass

ROW_HEADER_SIZE = 5
ROW_OBJECT = 0
ROW_STATUS = 1
ROW_LOAD_MASK = 2
ROW_READ_MASK = 3
ROW_WRITE_MASK = 4

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
        self.entities = set(element.get('entity', '').split())
        self.relations = set(tuple(rel.split('.'))
                             for rel in element.get('relation', '').split())
        if self.entities and self.relations: raise MappingError(
            'For table %r both entity name and relations are specified. '
            'It is not allowed' % self.name)
        elif not self.entities and not self.relations: raise MappingError(
            'For table %r neither entity name nor relations are specified. '
            'It is not allowed' % self.name)
        for entity_name in self.entities:
            if not utils.is_ident(entity_name): raise MappingError(
                'Entity name must be valid identifier. Got: %r' % entity_name)
        for relation in self.relations:
            if len(relation) != 2: raise MappingError(
                'Each relation must be in form of EntityName.AttributeName. '
                'Got: %s' % '.'.join(relation))
            for component in relation:
                if not utils.is_ident(component): raise MappingError(
                    'Each part of relation name must be valid identifier. '
                    'Got: %r' % component)
        self.columns = []
        self.cdict = {}
        for celement in element.findall('column'):
            col = ColumnMapping(self, celement)
            if self.cdict.setdefault(col.name, col) is not col:
                raise MappingError('Duplicate column definition: %s.%s'
                                   % (self.name, col.name))
            self.columns.append(col)
    def __repr__(self):
        return '<TableMapping: %r>' % self.name

class ColumnMapping(object):
    def __init__(self, table_mapping, element):
        self.table_mapping = table_mapping
        self.name = element.get('name')
        if not self.name: raise MappingError(
            'Error in table definition %r: '
            'Column element without "name" attribute' % table_mapping.name)
        self.domain = element.get('domain')
        self.attrs = set(tuple(attr.split('.'))
                         for attr in element.get('attr', '').split())
        for attr in self.attrs:
            if len(attr) < 2: raise MappingError(
                'Invalid attribute value in column %s.%s: '
                'must be in form of EntityName.AttributeName'
                % (table_mapping.name, self.name))
##        if table_mapping.entities:
##            for attr in self.attrs:
##                if attr[0] not in table_mapping.entities: raise MappingError(
##                    "Invalid attribute value in column %s.%s: "
##                    "entity %s does not contains inside 'entity' attribute "
##                    "of table definition"
##                    % (table_mapping.name, self.name, attr[0]))
        if table_mapping.relations:
            for attr in self.attrs:
                if attr[:2] not in table_mapping.relations: raise MappingError(
                    'Attribute %s does not correspond any relation'
                    % '.'.join(attr))
        self.kind = element.get('kind')
        if self.kind not in (None, 'discriminator'): raise MappingError(
            'Error in column %s.%s: invalid column kind: %s'
            % (table_mapping.name, self.name, self.kind))
        cases = element.findall('case')
        if cases and self.kind != 'discriminator': raise MappingError(
            'Non-discriminator column %s.%s contains cases.It is not allowed'
            % (table_mapping.name, self.name))
        self.cases = [ (case.get('value'), case.get('entity'))
                       for case in cases ]
        for value, entity in self.cases:
            if not value or not entity: raise MappingError(
                'Invalid discriminator case in column %s.%s'
                % (table_mapping.name, self.name))
    def __repr__(self):
        return '<ColumnMapping: %r.%r>' % (self.table_mapping.name, self.name)

class DataSource(object):
    _cache = {}
    _cache_lock = threading.Lock() # threadsafe access to cache of datasources
    def __new__(cls, provider, *args, **keyargs):
        self = object.__new__(cls)
        self._init_(provider, *args, **keyargs)
        key = (self.provider, self.mapping, self.args,
               tuple(sorted(self.keyargs.items())))
        cls._cache_lock.acquire()
        try: return cls._cache.setdefault(key, self)
        finally: cls._cache_lock.release()
    def _init_(self, provider, *args, **keyargs):
        self.lock = threading.RLock() # threadsafe access to datasource schema
        self.provider = provider
        self.args = args
        self.keyargs = keyargs
        self.transactions = set()        
        self.tables = {}    # table_name -> TableInfo
        self.entities = {}  # Entity -> EntityInfo
        mapping = keyargs.pop('mapping', None)
        if mapping is None: self.mapping = None
        else:
            if not isinstance(mapping, Mapping): self.mapping = Mapping(mapping)
            self.load_schema_from_mapping()
    def load_schema_from_mapping(self):
        for table_name, table_mapping in self.mapping.tables.items():
            table_name = table_mapping.name
            table = TableInfo(self, table_name)
            for col_mapping in table_mapping.columns:
                table.columns.append(ColumnInfo(table, col_mapping.name))
            col_count = len(table.columns)
            for i, col in enumerate(table.columns):
                col.old_offset = ROW_HEADER_SIZE + i
                col.new_offset = col.old_offset + col_count
            self.tables[table_name] = table
    def clear_schema(self):
        self.lock.acquire()
        try:
            if self.transaction: raise SchemaError(
                'Cannot clear datasource schema information '
                'because it is used by active transaction')
            self.entities.clear()
            self.tables.clear()
        finally: self.lock.release()
    def get_connection(self):
        provider = self.provider
        if isinstance(provider, basestring):
            provider = utils.import_module('pony.dbproviders.' + provider)
        return provider.connect(*self.args, **self.keyargs)
    def begin(self):
        return begin(self)

class TableInfo(object):
    def __init__(self, data_source, name):
        self.data_source = data_source
        self.name = name
        self.entities = []
        self.columns = []
    def __repr__(self):
        return '<TableInfo: %r>' % self.name

class ColumnInfo(object):
    def __init__(self, table, name):
        self.table = table
        self.name = name
        self.attrs = []
    def __repr__(self):
        return '<ColumnInfo: %r.%r>' % (self.table, self.name)

class Diagram(object):
    def __init__(self):
        self.lock = threading.RLock()
        self.entities = {} # entity_name -> Entity
        self.transactions = set()
    def clear(self):
        self.lock.acquire()
        try: self._clear()
        finally: self.lock.release()
    def _clear(self):
        self.lock.acquire()
        try:
            for tr in self.transactions: tr.data_source.clear_schema()
        finally: self.lock.release()

class EntityMeta(type):
    def __init__(entity, name, bases, dict):
        super(EntityMeta, entity).__init__(name, bases, dict)
        if 'Entity' not in globals(): return
        outer_dict = sys._getframe(1).f_locals
        diagram = (dict.pop('_diagram_', None)
                   or outer_dict.get('_diagram_', None)
                   or outer_dict.setdefault('_diagram_', Diagram()))
        entity._cls_init_(diagram)
    def __setattr__(entity, name, value):
        entity._cls_setattr_(name, value)
    def __iter__(entity):
        return iter(())

class Entity(object):
    __metaclass__ = EntityMeta
    @classmethod
    def _cls_setattr_(entity, name, value):
        if name.startswith('_') and name.endswith('_'):
            type.__setattr__(entity, name, value)
        else: raise NotImplementedError
    @classmethod
    def _cls_init_(entity, diagram):
        if entity.__name__ in diagram.entities:
            raise DiagramError('Entity %s already exists' % entity.__name__)
        bases = [ c for c in entity.__bases__
                    if issubclass(c, Entity) and c is not Entity ]
        entity._bases_ = bases
        entity._self_with_all_bases_ = set((entity,))
        for base in bases:
            entity._self_with_all_bases_.update(base._self_with_all_bases_)
        if bases:
            roots = set(c._root_ for c in bases)
            if len(roots) > 1: raise DiagramError(
                'With multiple inheritance of entities, '
                'inheritance graph must be diamond-like')
            entity._root_ = roots.pop()
            for c in bases:
                if c._diagram_ is not diagram: raise DiagramError(
                    'When use inheritance, base and derived entities '
                    'must belong to same diagram')
        else: entity._root_ = entity

        base_attrs = []
        base_attrs_dict = {}
        for c in bases:
            for a in c._attrs_:
                if base_attrs_dict.setdefault(a.name, a) is not a:
                    raise DiagramError('Ambiguous attribute name %s' % a.name)
                base_attrs.append(a)
        entity._base_attrs_ = base_attrs

        new_attrs = []
        for name, attr in entity.__dict__.items():
            if name in base_attrs_dict: raise DiagramError(
                'Name %s hide base attribute %s' % (name,base_attrs_dict[name]))
            if not isinstance(attr, Attribute): continue
            if attr.entity is not None:
                raise DiagramError('Duplicate use of attribute %s' % value)
            attr.name = name
            attr.entity = entity
            new_attrs.append(attr)
        new_attrs.sort(key=attrgetter('_id_'))
        entity._new_attrs_ = new_attrs

        entity._keys_ = keys = entity.__dict__.get('_keys_', set())
        primary_keys = set(key for key in keys
                               if isinstance(key, _PrimaryKeyTuple))
        if bases:
            if primary_keys: raise DiagramError(
                'Primary key cannot be redefined in derived classes')
            for c in bases: keys.update(c._keys_)
            primary_keys = set(key for key in keys
                                   if isinstance(key, _PrimaryKeyTuple))
                                   
        if len(primary_keys) > 1: raise DiagramError(
            'Only one primary key can be defined in each entity class')
        elif not primary_keys:
            if hasattr(entity, 'id'):
                raise DiagramError("Name 'id' alredy in use")
            _keys_ = set()
            attr = PrimaryKey(int) # Side effect: modifies _keys_ local variable
            attr.name = 'id'
            attr.entity = entity
            type.__setattr__(entity, 'id', attr)  # entity.id = attr
            entity._new_attrs_.insert(0, attr)
            key = _keys_.pop()
            entity._keys_.add(key)
            entity._primary_key_ = key
        else: entity._primary_key_ = primary_keys.pop()
        entity._attrs_ = base_attrs + new_attrs
        diagram.lock.acquire()
        try:
            diagram._clear()
            entity._diagram_ = diagram
            diagram.entities[entity.__name__] = entity
            entity._link_reverse_attrs_()
        finally: diagram.lock.release()

    @classmethod
    def _link_reverse_attrs_(entity):
        diagram = entity._diagram_
        for attr in entity._new_attrs_:
            py_type = attr.py_type
            if isinstance(py_type, basestring):
                entity2 = diagram.entities.get(py_type)
                if entity2 is None: continue
                attr.py_type = entity2
            elif issubclass(py_type, Entity):
                entity2 = py_type
                if entity2._diagram_ is not diagram: raise DiagramError(
                    'Interrelated entities must belong to same diagram. '
                    'Entities %s and %s belongs to different diagrams'
                    % (entity.__name__, entity2.__name__))
            else: continue
            
            reverse = attr.reverse
            if isinstance(reverse, basestring):
                attr2 = getattr(entity2, reverse, None)
                if attr2 is None: raise DiagramError(
                    'Reverse attribute %s.%s not found'
                    % (entity2.__name__, reverse))
            elif isinstance(reverse, Attribute):
                attr2 = reverse
                if attr2.entity is not entity2: raise DiagramError(
                    'Incorrect reverse attribute %s used in %s' % (attr2, attr))
            elif reverse is not None: raise DiagramError(
                "Value of 'reverse' option must be string. Got: %r"
                % type(reverse))
            else:
                candidates1 = []
                candidates2 = []
                for attr2 in entity2._new_attrs_:
                    if attr2.py_type not in (entity, entity.__name__): continue
                    reverse2 = attr2.reverse
                    if reverse2 in (attr, attr.name): candidates1.append(attr2)
                    elif reverse2 is None: candidates2.append(attr2)
                msg = 'Ambiguous reverse attribute for %s'
                if len(candidates1) > 1: raise DiagramError(msg % attr)
                elif len(candidates1) == 1: attr2 = candidates1[0]
                elif len(candidates2) > 1: raise DiagramError(msg % attr)
                elif len(candidates2) == 1: attr2 = candidates2[0]
                else: raise DiagramError(
                    'Reverse attribute for %s not found' % attr)

            type2 = attr2.py_type
            msg = 'Inconsistent reverse attributes %s and %s'
            if isinstance(type2, basestring):
                if type2 != entity.__name__:
                    raise DiagramError(msg % (attr, attr2))
                attr2.py_type = entity
            elif type2 != entity: raise DiagramError(msg % (attr, attr2))
            reverse2 = attr2.reverse
            if reverse2 not in (None, attr, attr.name):
                raise DiagramError(msg % (attr,attr2))

            attr.reverse = attr2
            attr2.reverse = attr
    @classmethod
    def _get_entity_info(entity):
        tr = local.transaction
        if tr is None: raise TransactionError(
            'There are no active transaction in thread %s' % thread.get_ident())
        data_source = tr.data_source
        entity_info = data_source.entities.get(entity)
        return entity_info or EntityInfo(entity, data_source)
    def __init__(self):
        pass
    @classmethod
    def create(entity, *args, **keyargs):
        pass

class EntityInfo(object):
    def __new__(cls, entity, data_source):
        data_source.lock.acquire()
        try:
            entity_info = data_source.entities.get(entity)
            if entity_info: return entity_info
            self = object.__new__(cls)
            data_source.entities[entity] = self
            self._init_(entity, data_source)
            return self
        finally: data_source.lock.release()
    def _init_(self, entity, data_source):
        self.entity = entity
        self.data_source = data_source
        self.tables = []
        self.attrs = {} # Attribute -> AttrInfo
        if data_source.mapping is None: raise NotImplementedError
        self_or_bases = set()
        swab_names = set(e.__name__ for e in entity._self_with_all_bases_)
        for table_name, table_mapping in data_source.mapping.tables.items():
            for entity_name in table_mapping.entities:
                if entity_name in swab_names:
                    self.tables.append(data_source.tables[table_name])
                    break
        for attr in entity._attrs_:
            self.attrs[attr] = AttrInfo(self, attr)

class AttrInfo(object):
    def __init__(self, entity_info, attr):
        self.enity_info = entity_info
        self.attr = attr
        self.columns = []
        # ...
    
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
        elif not isinstance(self.reverse, (basestring, Attribute)):
            raise TypeError("Value of 'reverse' option must be name of "
                            "reverse attribute). Got: %r" % self.reverse)
        elif not (self.py_type, basestring):
            raise DiagramError('Reverse option cannot be set for this type %r'
                            % self.py_type)
    def __str__(self):
        owner_name = self.entity is None and '?' or self.entity.__name__
        return '%s.%s' % (owner_name, self.name or '?')
    def __repr__(self):
        return '<%s: %s>' % (self, self.__class__.__name__)
    def __get__(self, obj, type):
        if obj is None: return self
        return Property(obj, self)
    def __set__(self, obj, value):
        raise NotImplementedError
    def __delete__(self, obj):
        raise NotImplementedError

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

class Property(object):
    def __init__(self, obj, attr):
        self.obj = obj
        self.attr = attr

class Transaction(object):
    def __init__(self, data_source, connection=None):
        if local.transaction is not None: raise TransactionError(
            'Transaction already started in thread %d' % thread.get_ident())
        self.data_source = data_source
        self.connection = connection
        self.diagrams = set()
        self.cache = {} # TableInfo -> TableCache

        data_source.lock.acquire()
        try: data_source.transactions.add(self)
        finally: data_source.lock.release()
        local.transaction = self
    def _close(self):
        assert local.transaction is self
        data_source.lock.acquire()
        try:
            while self.diagrams:
                diagram = self.diagrams.pop()
                diagram.transactions.remove(self)
            data_source.transactions.remove(self)
        finally: data_source.lock.release()
        local.transaction = None
    def commit(self):
        self._close()
        raise NotImplementedError
    def rollback(self):
        self._close()
        raise NotImplementedError

class Local(threading.local):
    def __init__(self):
        self.transaction = None

local = Local()

def get_transaction():
    return local.transaction

def begin(data_source=None):
    if local.transaction is not None: raise TransactionError(
        'Transaction already started in thread %d' % thread.get_ident())
    if data_source is not None: return Transaction(data_source)
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
