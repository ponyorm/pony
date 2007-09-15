import sys, os.path, operator, thread, threading
from operator import attrgetter
from itertools import count, izip, ifilterfalse

from pony import utils
from pony.thirdparty import etree

class OrmError(Exception): pass

class DiagramError(OrmError): pass
class SchemaError(OrmError): pass
class MappingError(OrmError): pass
class TransactionError(OrmError): pass
class ConstraintError(OrmError): pass
class CreateError(OrmError): pass
class UpdateError(OrmError): pass

DATA_HEADER = [ None, None ]

ROW_HEADER = [ None, None, 0, 0 ]
ROW_READ_MASK = 2
ROW_UPDATE_MASK = 3

UNKNOWN = utils.Symbol('UNKNOWN')

next_id = count().next

class Attribute(object):
    def __init__(self, py_type, *args, **keyargs):
        if self.__class__ is Attribute:
            raise TypeError("'Atrribute' is abstract type")
        self.pk_offset = None
        self._id_ = next_id()
        self.py_type = py_type
        self.name = None
        self.entity = None
        self.args = args
        self.options = keyargs
        try: self.default = keyargs.pop('default')
        except KeyError: self.default = None
        else:
            if self.default is None and isinstance(self, Required):
                raise TypeError(
                    'Default value for required attribute %s cannot be None'
                    % self)
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
        return '<Attribute %s: %s>' % (self, self.__class__.__name__)
    def get_old(self, obj):
        raise NotImplementedError
    def __get__(self, obj, type=None):
        if obj is None: return self
        try: return obj._pk_[self.pk_offset]
        except TypeError: pass
        attr_info = obj._get_info().attrs[self]
        trans = local.transaction
        data = trans.objects.get(obj)
        if data is None: raise NotImplementedError
        value = data[self.new_offset]
        if value is UNKNOWN: raise NotImplementedError
        return value
    def __set__(self, obj, value):
        pk = obj._pk_
        if self.pk_offset is not None:
            if value == pk[self.pk_offset]: return
            raise TypeError('Cannot change value of primary key')
        if value is None and isinstance(self, Required):
            raise TypeError('Required attribute %s.%s cannot be set to None'
                            % (obj.__class__.__name__, self.name))
        attr_info = obj._get_info().attrs[self]
        trans = local.transaction
        data = trans.objects.get(obj)
        if data is None: raise NotImplementedError
        prev = data[self.new_offset]
        if prev == value: return
        undo = []
        try:
            for key in obj._keys_:
                if key is obj._primary_key_: continue
                if self not in key: continue
                position = list(key).index(self)
                new_key = [ data[attr.new_offset] for attr in key ]
                old_key = tuple(new_key)
                new_key[position] = value
                if UNKNOWN in new_key: continue
                new_key = tuple(new_key)
                try: old_index, new_index = trans.indexes[key]
                except KeyError:
                    old_index, new_index = trans.indexes[key] = ({}, {})
                obj2 = new_index.setdefault(new_key, obj)
                if obj2 is not obj:
                    key_str = ', '.join(repr(item) for item in new_key)
                    raise UpdateError(
                        'Cannot update %s.%s: '
                        '%s with such unique index already exists: %s'
                        % (obj.__class__.__name__, self.name,
                           obj2.__class__.__name__, key_str))
                if prev is not UNKNOWN:
                      del new_index[old_key]
                      undo.append((new_index, obj, old_key, new_key))
                else: undo.append((new_index, obj, None, new_key))
        except UpdateError:
            for new_index, obj, old_key, new_key in undo:
                del new_index[new_key]
                if old_key is not None: new_index[old_key] = obj
            raise
        else:
            if data[1] != 'C': data[1] = 'U'
            data[self.new_offset] = value
        for table, column in attr_info.columns.items():
            cache = trans.caches.get(table)
            if cache is None: cache = trans.caches[table] = TableCache(table)
            row = cache.rows.get(pk)
            if row is None: raise NotImplementedError # the cache remains in corrupted state
            assert row[0] is obj
            if row[1] != 'C':
                row[1] = 'U'
                row[ROW_UPDATE_MASK] |= column.mask
            row[column.new_offset] = value
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
            for trans in self.transactions: trans.data_source.clear_schema() # ????
        finally: self.lock.release()

class EntityMeta(type):
    def __init__(entity, name, bases, dict):
        super(EntityMeta, entity).__init__(name, bases, dict)
        if 'Entity' not in globals(): return
        outer_dict = sys._getframe(1).f_locals
        diagram = (dict.pop('_diagram_', None)
                   or outer_dict.get('_diagram_')
                   or outer_dict.setdefault('_diagram_', Diagram()))
        if not hasattr(diagram, 'data_source'):
            diagram.data_source = outer_dict.get('_data_source_')
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
        entity._objects_ = {}
        entity._lock_ = threading.Lock()
        direct_bases = [ c for c in entity.__bases__
                         if issubclass(c, Entity) and c is not Entity ]
        entity._direct_bases_ = direct_bases
        entity._all_bases_ = set((entity,))
        for base in direct_bases: entity._all_bases_.update(base._all_bases_)
        if direct_bases:
            roots = set(base._root_ for base in direct_bases)
            if len(roots) > 1: raise DiagramError(
                'With multiple inheritance of entities, '
                'inheritance graph must be diamond-like')
            entity._root_ = roots.pop()
            for base in direct_bases:
                if base._diagram_ is not diagram: raise DiagramError(
                    'When use inheritance, base and derived entities '
                    'must belong to same diagram')
        else: entity._root_ = entity

        base_attrs = []
        base_attrs_dict = {}
        for base in direct_bases:
            for a in base._attrs_:
                if base_attrs_dict.setdefault(a.name, a) is not a:
                    raise DiagramError('Ambiguous attribute name %s' % a.name)
                base_attrs.append(a)
        entity._base_attrs_ = base_attrs

        new_attrs = []
        for name, attr in entity.__dict__.items():
            if name in base_attrs_dict: raise DiagramError(
                'Name %s hide base attribute %s' % (name,base_attrs_dict[name]))
            if not isinstance(attr, Attribute): continue
            if name.startswith('_') and name.endswith('_'): raise DiagramError(
                'Attribute name cannot both starts and ends with underscore. '
                'Got: %s' % name)
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
        if direct_bases:
            if primary_keys: raise DiagramError(
                'Primary key cannot be redefined in derived classes')
            for base in direct_bases: keys.update(base._keys_)
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
        for i, attr in enumerate(entity._primary_key_): attr.pk_offset = i
        entity._pk_names_ = tuple(attr.name for attr in entity._primary_key_) # ???

        entity._attrs_ = base_attrs + new_attrs
        entity._attr_dict_ = dict((attr.name, attr) for attr in entity._attrs_)

        next_offset = count(len(DATA_HEADER)).next
        for attr in entity._attrs_:
            if attr.pk_offset is None:
                attr.old_offset = next_offset()
                attr.new_offset = next_offset()
            else: attr.old_offset = attr.new_offset = next_offset()
        data_size = entity._attrs_[-1].new_offset + 1
        entity._data_template_ = \
            DATA_HEADER + [ UNKNOWN ]*(data_size - len(DATA_HEADER))

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
    def _get_info(entity):
        trans = local.transaction
        if trans is None:
            data_source = entity._diagram_.data_source
            if data_source is None:
                outer_dict = sys._getframe(1).f_locals
                data_source = outer_dict.get('_data_source_')
            if data_source is not None: trans = Transaction(data_source)
            else: raise TransactionError(
                'There are no active transaction in thread %s. '
                'Cannot start transaction automatically, '
                'because default data source does not set'
                % thread.get_ident())
        else: data_source = trans.data_source
        info = data_source.entities.get(entity)
        if info is not None: return info
        data_source.generate_schema(entity._diagram_)
        return data_source.entities[entity]
    def __init__(self, *args, **keyargs):
        raise TypeError('You cannot create entity instances directly. '
                        'Use Entity.create(...) or Entity.find(...) instead')
    @property
    def old(self):
        return OldProxy(self)
    @classmethod
    def create(entity, *args, **keyargs):
        if args:
            if len(args) != len(entity._primary_key_):
                raise CreateError('Invalid count of attrs in primary key')
            for name, value in zip(entity._pk_names_, args):
                if keyargs.setdefault(name, value) != value:
                    raise CreateError('Ambiguous attribute value for %r' % name)
        for name in ifilterfalse(entity._attr_dict_.__contains__, keyargs):
            raise CreateError('Unknown attribute %r' % name)
        pk = args or tuple(map(keyargs.get, entity._pk_names_))
        if None in pk: raise CreateError('Primary key is not specified')

        entity._lock_.acquire()
        try:
            obj = entity._objects_.get(pk)
            if obj is None:
                obj = object.__new__(entity)
                obj._pk_ = pk
                entity._objects_[pk] = obj
        finally: entity._lock_.release()

        data = entity._data_template_[:]
        data[0] = obj
        data[1] = 'C'
        for attr in entity._attrs_:
            try: value = keyargs[attr.name]
            except KeyError:
                value = attr.default
                msg = 'Required attribute %s.%s does not specified'
            else: msg = 'Value of required attribute %s.%s cannot be None'
            if value is None and isinstance(attr, Required):
                raise CreateError(msg % (entity.__name__, attr.name))
            data[attr.new_offset] = value

        info = entity._get_info()
        trans = local.transaction
        try:
            for key in entity._keys_:
                key_value = tuple(data[attr.new_offset] for attr in key)
                try: old_index, new_index = trans.indexes[key]
                except KeyError:
                    old_index, new_index = trans.indexes[key] = ({}, {})
                obj2 = new_index.setdefault(key_value, obj)
                if obj2 is not obj:
                    key_str = ', '.join(repr(item) for item in key_value)
                    if key is entity._primary_key_: key_type = 'primary key'
                    else: key_type = 'unique index'
                    raise CreateError(
                        '%s with such %s already exists: %s'
                        % (obj2.__class__.__name__, key_type, key_str))
        except CreateError, e:
            for key in entity._keys_:
                key_value = tuple(data[attr.new_offset] for attr in key)
                index_pair = trans.indexes.get(key)
                if index_pair is None: continue
                old_index, new_index = index_pair
                if new_index.get(key_value) is obj: del new_index[key_value]
            raise
        if trans.objects.setdefault(obj, data) is not data: raise AssertionError
        for table in info.tables:
            cache = trans.caches.get(table)
            if cache is None: cache = trans.caches[table] = TableCache(table)
            new_row = cache.row_template[:]
            new_row[0] = obj
            new_row[1] = 'C'
            for column in table.columns:
                for attr in column.attrs:
                    if issubclass(entity, attr.entity):
                        new_row[column.new_offset] = data[attr.new_offset]
                        break
                else: new_row[column.new_offset] = None
            if cache.rows.setdefault(pk, new_row) is not new_row:
                raise AssertionError
        return obj
    @classmethod
    def find(entity, *args, **keyargs):
        raise NotImplementedError
    def set(self, **keyargs):
        for name in ifilterfalse(self._attr_dict_.__contains__, keyargs):
            raise CreateError("Unknown attribute %r" % name)
        pk = self._pk_
        info = self._get_info()
        trans = local.transaction
        try:
            for table in info.tables:
                pass
        except UpdateError:
            raise
        else:
            pass
        
def old(obj):
    return OldProxy(obj)

class OldProxy(object):
    def __init__(self, obj):
        cls = obj.__class__
        if not issubclass(cls, Entity): raise TypeError(
            'Expected subclass of Entity. Got: %s' % cls.__name__)
        self._obj_ = obj
        self._cls_ = cls
    def __getattr__(self, name):
        attr = getattr(self._cls_, name, None)
        if attr is None or not isinstance(attr, Attribute):
            return getattr(self._obj_, name)
        return attr.get_old(self._obj_)

class EntityInfo(object):
    def __init__(self, entity, data_source):
        self.entity = entity
        self.data_source = data_source
        self.tables = {}  # TableInfo -> dict(attr_name -> ColumnInfo)
        if data_source.mapping is None: raise NotImplementedError
        swab_names = set(e.__name__ for e in entity._all_bases_)
        for table in data_source.tables.values():
            for entity_name in table.entities:
                if entity_name in swab_names:
                    self.tables[table] = {}
                    break
        self.attrs = {} # Attribute -> AttrInfo
        for attr in entity._attrs_: self.attrs[attr] = AttrInfo(self, attr)
        self.keys = set()
        for attr_info in self.attrs.values():
            for table, column in attr_info.columns.items():
                self.tables[table][attr_info.attr.name] = column
        for key in entity._keys_:
            key2 = tuple(map(self.attrs.__getitem__, key))
            self.keys.add(key2)
            if key is entity._primary_key_: self.primary_key = key2
        assert hasattr(self, 'primary_key')

class AttrInfo(object):
    def __init__(self, info, attr):
        self.enity_info = info
        self.attr = attr
        name_pair = attr.entity.__name__, attr.name
        self.columns = info.data_source.attr_map.get(name_pair, {}).copy()
        for table, column in self.columns.items(): column.attrs.add(attr)
    def __repr__(self):
        return '<AttrInfo: %s.%s>' % (self.enity_info.entity.__name__,
                                      self.attr.name)
    
class DataSource(object):
    _cache = {}
    _cache_lock = threading.Lock() # threadsafe access to cache of datasources
    def __new__(cls, provider, *args, **keyargs):
        mapping = keyargs.pop('mapping', None)
        if isinstance(mapping, basestring):
            filename = utils.absolutize_path(mapping)
            try: mtime = utils.get_mtime(filename)
            except OSError:
                mapping_key = mapping
                try: document = etree.XML(mapping)
                except: raise MappingError('Invalid mapping or file not found')
            else:
                mapping_key = (filename, mtime)
                document = etree.parse(filename)
        else:
            mapping_key = mapping
            document = mapping
        key = (provider, mapping_key, args, tuple(sorted(keyargs.items())))
        data_source = cls._cache.get(key)
        if data_source is not None: return data_source
        cls._cache_lock.acquire()
        try:
            data_source = cls._cache.get(key)
            if data_source is not None: return data_source
            data_source = object.__new__(cls)
            data_source._init_(document, provider, *args, **keyargs)
            return data_source
        finally: cls._cache_lock.release()
    def _init_(self, mapping, provider, *args, **keyargs):
        self.lock = threading.RLock() # threadsafe access to datasource schema
        self.mapping = mapping
        self.provider = provider
        self.args = args
        self.keyargs = keyargs
        self.transactions = set()        
        self.tables = {}   # table_name -> TableInfo
        self.diagrams = set()
        self.entities = {} # Entity -> EntityInfo
        self.attr_map = {} # (entity_name, attr_name)->(TableInfo->ColumnInfo)
        if mapping is not None: self.load_mapping()
    def load_mapping(self):
        for table_element in self.mapping.findall('table'):
            table = TableInfo(self, table_element)
            if self.tables.setdefault(table.name, table) is not table:
                raise MappingError('Duplicate table definition: %s'
                                   % table.name)
            if table.entities:
                for column in table.columns:
                    for attr_name in column.attr_names:
                        tables = self.attr_map.setdefault(attr_name[:2], {})
                        if tables.setdefault(table, column) is not column:
                            raise NotImplementedError
    def generate_schema(self, diagram):
        self.lock.acquire()
        try:
            if diagram in self.diagrams: return
            for entity in diagram.entities.values():
                info = EntityInfo(entity, self)
                self.entities[entity] = info
                for key_attrs in entity._keys_:
                    name_pairs = []
                    for attr in key_attrs:
                        name_pairs.append((attr.entity.__name__, attr.name))
                    for table in info.tables:
                        key_columns = []
                        for name_pair in name_pairs:
                            tables = self.attr_map.get(name_pair)
                            if tables is None: raise SchemaError(
                                'Key column %r.%r does not have '
                                'correspond column' % name_pair)
                            column = tables.get(table)
                            if column is None: break
                            key_columns.append(column)
                        else:
                            key_columns = tuple(key_columns)
                            if key_attrs is not entity._primary_key_:
                                table.secondary_keys.add(key_columns)
                            elif not hasattr(table, 'primary_key'):
                                table.primary_key = key_columns
                            elif table.primary_key != key_columns:
                                raise SchemaError(
                                    'Multiple primary keys for table %r'
                                    % table.name)
            for table in self.tables.values():
                if not table.entities: continue
                next_offset = count(len(ROW_HEADER)).next
                mask_offset = count().next
                for i, column in enumerate(table.primary_key):
                    column.pk_offset = i
                for column in table.columns:
                    if column.pk_offset is None:
                        column.old_offset = next_offset()
                        column.new_offset = next_offset()
                        column.mask = 1 << mask_offset()
                    else:
                        column.old_offset = column.new_offset = next_offset()
                        column.mask = 0
        finally: self.lock.release()
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
    def __init__(self, data_source, x):
        self.data_source = data_source
        self.columns = []
        self.secondary_keys = set()
        if isinstance(x, basestring): self.name = x
        else: self._init_from_xml_element(x)
    def __repr__(self):
        return '<TableInfo: %r>' % self.name
    def _init_from_xml_element(self, element):
        self.name = element.get('name')
        if not self.name:
            raise MappingError('<table> element without "name" attribute')
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
                'Got: %r' % '.'.join(relation))
            for component in relation:
                if not utils.is_ident(component): raise MappingError(
                    'Each part of relation name must be valid identifier. '
                    'Got: %r' % component)
        self.columns = []
        self.cdict = {}
        for col_element in element.findall('column'):
            column = ColumnInfo(self, col_element)
            if self.cdict.setdefault(column.name, column) is not column:
                raise MappingError('Duplicate column definition: %r.%r'
                                   % (self.name, column.name))
            self.columns.append(column)

class ColumnInfo(object):
    def __init__(self, table, x):
        self.table = table
        self.pk_offset = None
        self.attrs = set()
        if isinstance(x, basestring): self.name = x
        else: self._init_from_xml_element(x)
    def __repr__(self):
        return '<ColumnInfo: %r.%r>' % (self.table.name, self.name)
    def _init_from_xml_element(self, element):
        table = self.table
        self.name = element.get('name')
        if not self.name: raise MappingError(
            'Error in table definition %r: '
            'Column element without "name" attribute' % table.name)
        self.domain = element.get('domain')
        self.attr_names = set(tuple(attr.split('.'))
                              for attr in element.get('attr', '').split())
        for attr_name in self.attr_names:
            if len(attr_name) < 2: raise MappingError(
                'Invalid attribute value in column %r.%r: '
                'must be in form of EntityName.AttributeName'
                % (table.name, self.name))
        if table.relations:
            for attr_name in self.attr_names:
                if attr_name[:2] not in table.relations: raise MappingError(
                    'Attribute %s does not correspond any relation'
                    % '.'.join(attr_name))
        self.kind = element.get('kind')
        if self.kind not in (None, 'discriminator'): raise MappingError(
            'Error in column %r.%r: invalid column kind: %r'
            % (table.name, self.name, self.kind))
        cases = element.findall('case')
        if cases and self.kind != 'discriminator': raise MappingError(
            'Non-discriminator column %r.%r contains cases. It is not allowed'
            % (table.name, self.name))
        self.cases = [ (case.get('value'), case.get('entity'))
                       for case in cases ]
        for value, entity in self.cases:
            if not value or not entity: raise MappingError(
                'Invalid discriminator case in column %r.%r'
                % (table.name, self.name))

class Transaction(object):
    def __init__(self, data_source, connection=None):
        if local.transaction is not None: raise TransactionError(
            'Transaction already started in thread %d' % thread.get_ident())
        self.data_source = data_source
        self.connection = connection
        self.diagrams = set()
        self.caches = {}  # TableInfo -> TableCache
        self.objects = {} # object -> row
        self.indexes = {} # key_attrs -> ({old_key -> obj}, {new_key -> obj})
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

class TableCache(object):
    def __init__(self, table):
        self.table = table
        row_size = table.columns[-1].new_offset + 1
        self.row_template = ROW_HEADER + [ UNKNOWN ]*(row_size-len(ROW_HEADER))
        self.rows = {}

class Local(threading.local):
    def __init__(self):
        self.transaction = None

local = Local()

def get_transaction():
    return local.transaction

def no_trans_error():
    raise TransactionError('There are no active transaction in thread %s'
                           % thread.get_ident())

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
    trans = local.transaction
    if trans is None: no_trans_error()
    trans.commit()

def rollback():
    trans = local.transaction
    if trans is None: no_trans_error()
    trans.rollback()
