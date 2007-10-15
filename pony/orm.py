import sys, os.path, operator, thread, threading
from operator import itemgetter, attrgetter
from itertools import count, imap, izip, ifilter, ifilterfalse

from pony import utils
from pony.thirdparty import etree

class OrmError(Exception): pass

class DiagramError(OrmError): pass
class SchemaError(OrmError): pass
class MappingError(OrmError): pass
class TransactionError(OrmError): pass
class ConstraintError(TransactionError): pass
class CreateError(TransactionError): pass
class UpdateError(TransactionError): pass
class TransferringObjectWithoutPkError(TransactionError):
    def __init__(self, obj):
        msg = 'Transferring %s with undefined primary key from one transaction to another is not allowed'
        TransactionError.__init__(self, msg % obj.__class__.__name__)

DATA_HEADER = [ None, None ]

ROW_HEADER = [ None, None, 0, 0 ]
ROW_READ_MASK = 2
ROW_UPDATE_MASK = 3

class UnknownType(object):
    def __repr__(self): return 'UNKNOWN'

UNKNOWN = UnknownType()

next_id = count().next

class Attribute(object):
    def __init__(attr, py_type, *args, **keyargs):
        if attr.__class__ is Attribute: raise TypeError("'Atrribute' is abstract type")
        attr.pk_offset = None
        attr._id_ = next_id()
        attr.py_type = py_type
        attr.name = None
        attr.entity = None
        attr.args = args
        attr.options = keyargs
        attr.autoincrement = keyargs.pop('autoincrement', False)

        try: attr.default = keyargs.pop('default')
        except KeyError: attr.default = None
        else:
            if attr.default is None and isinstance(attr, Required):
                raise TypeError('Default value for required attribute %s cannot be None' % attr)

        attr.reverse = keyargs.pop('reverse', None)
        if attr.reverse is None: pass
        elif not isinstance(attr.reverse, (basestring, Attribute)):
            raise TypeError("Value of 'reverse' option must be name of "
                            "reverse attribute). Got: %r" % attr.reverse)
        elif not isinstance(attr.py_type, (basestring, EntityMeta)):
            raise DiagramError('Reverse option cannot be set for this type %r' % attr.py_type)
    def __str__(attr):
        owner_name = attr.entity is None and '?' or attr.entity.__name__
        return '%s.%s' % (owner_name, attr.name or '?')
    def __repr__(attr):
        return '<Attribute %s: %s>' % (attr, attr.__class__.__name__)
    def check(attr, value, entity=None):
        if value is UNKNOWN: value = attr.default
        if value is None: return value
        reverse = attr.reverse
        if reverse and not isinstance(value, reverse.entity):
            entity_name = (entity or attr.entity).__name__
            raise ConstraintError('Value of attribute %s.%s must be an instance of %s. Got: %s'
                                  % (entity_name, attr.name, reverse.entity.__name__, value))
        return value
    def get_old(attr, obj):
        raise NotImplementedError
    def __get__(attr, obj, type=None):
        if obj is None: return attr
        pk = obj._pk_
        try: return pk[attr.pk_offset]
        except TypeError: pass  # pk is None or attr.pk_offset is None
        attr_info = obj._get_info().attrs[attr]
        trans = local.transaction
        data = trans.objects.get(obj)
        if data is None:
            if pk is None: raise TransferringObjectWithoutPkError(obj)
            raise NotImplementedError
        value = data[obj._new_offsets_[attr]]
        if value is UNKNOWN: raise NotImplementedError
        return value
    def __set__(attr, obj, value):
        value = attr.check(value, obj.__class__)
        pk = obj._pk_
        if attr.pk_offset is not None:
            if pk is not None and value == pk[attr.pk_offset]: return
            raise UpdateError('Cannot change value of primary key')

        attr_info = obj._get_info().attrs[attr]
        trans = local.transaction
        get_offset = obj._new_offsets_.__getitem__
        data = trans.objects.get(obj) or obj._get_data('U')
        prev = data[get_offset(attr)]
        if prev == value: return

        undo = []
        try:
            for key in obj._keys_:
                if key is obj._pk_attrs_: continue
                if attr not in key: continue
                position = list(key).index(attr)
                new_key = map(data.__getitem__, map(get_offset, key))
                old_key = tuple(new_key)
                new_key[position] = value
                new_key = tuple(new_key)
                if None in new_key or UNKNOWN in new_key: new_key = None
                if None in old_key or UNKNOWN in old_key: old_key = None
                if old_key is None and new_key is None: continue
                try: old_index, new_index = trans.indexes[key]
                except KeyError: old_index, new_index = trans.indexes[key] = ({}, {})
                if new_key is not None:
                    obj2 = new_index.setdefault(new_key, obj)
                    if obj2 is not obj:
                        key_str = ', '.join(repr(item) for item in new_key)
                        raise UpdateError('Cannot update %s.%s: %s with such unique index already exists: %s'
                                          % (obj.__class__.__name__, attr.name, obj2.__class__.__name__, key_str))
                if old_key is not None: del new_index[old_key]
                undo.append((new_index, obj, old_key, new_key))
            if attr.reverse is not None: attr.reverse.set_reverse(obj, prev, value)
        except UpdateError:
            for new_index, obj, old_key, new_key in undo:
                del new_index[new_key]
                if old_key is not None: new_index[old_key] = obj
            raise
        if data[1] != 'C': data[1] = 'U'
        data[get_offset(attr)] = value

        if pk is None: return
        
        for table, column in attr_info.tables.items():
            cache = trans.caches.get(table)
            if cache is None: cache = trans.caches[table] = Cache(table)
            row = cache.rows.get(pk)
            if row is None:
                row = cache.rows[pk] = cache.row_template[:]
                row[0] = obj
                row[1] = 'U'
                for c, v in zip(table.pk_columns, pk): row[c.new_offset] = v
            else: assert row[0] is obj
            if row[1] != 'C':
                row[1] = 'U'
                row[ROW_UPDATE_MASK] |= column.mask
            row[column.new_offset] = value
    def __delete__(attr, obj):
        raise NotImplementedError
    def set_reverse(attr, reverse_obj, prev_obj, obj):
        assert rvalue is not UNKNOWN
        raise NotImplementedError

class Optional(Attribute): pass

class Required(Attribute):
    def check(attr, value, entity=None):
        msg = None
        if value is UNKNOWN:
            value = attr.default
            if value is None and not attr.autoincrement: msg = 'Required attribute %s.%s does not specified'
        elif value is None: msg = 'Required attribute %s.%s cannot be set to None'
        if msg is None: return Attribute.check(attr, value, entity)
        entity_name = (entity or attr.entity).__name__
        raise ConstraintError(msg % (entity_name, attr.name))

class Unique(Required):
    def __new__(cls, *args, **keyargs):
        if not args: raise TypeError('Invalid count of positional arguments')
        attrs = tuple(a for a in args if isinstance(a, Attribute))
        non_attrs = [ a for a in args if not isinstance(a, Attribute) ]
        if attrs and non_attrs: raise TypeError('Invalid arguments')
        cls_dict = sys._getframe(1).f_locals
        keys = cls_dict.setdefault('_keys_', {})
        if not attrs:
            result = Required.__new__(cls, *args, **keyargs)
            keys[(result,)] = issubclass(cls, PrimaryKey)
            return result
        else:
            for attr in attrs:
                if isinstance(attr, Collection):
                    key_type = issubclass(cls, PrimaryKey) and 'primary key' or 'unique index'
                    raise TypeError('Collection attribute %s cannot be part of %s' % (attr, key_type))
            keys[attrs] = issubclass(cls, PrimaryKey)

class PrimaryKey(Unique): pass

class Collection(Attribute):
    def __init__(attr, py_type, *args, **keyargs):
        if attr.__class__ is Collection: raise TypeError("'Collection' is abstract type")
        Attribute.__init__(attr, py_type, *args, **keyargs)
        if attr.default is not None: raise TypeError(
            'default value could not be set for collection attribute %s' % attr)
        if attr.autoincrement: raise TypeError(
            "'autoincrement' option could not be set for collection attribute %s" % attr)
    def __get__(attr, obj, type=None):
        assert False, 'Abstract method'
    def __set__(attr, obj, value):
        assert False, 'Abstract method'
    def __delete__(attr, obj):
        assert False, 'Abstract method'
    def set_reverse(attr, reverse_obj, prev_obj, obj):
        assert False, 'Abstract method'
    def _get_coldata(attr, obj):
        trans = local.transaction
        data = trans.objects.get(obj) or obj._get_data('U')
        offset = obj._new_offsets_[attr]
        coldata = data[offset]
        if coldata is None:
            coldata = data[offset] = SetData()
            coldata.fully_loaded = (data[1] == 'C')
        return coldata

class Set(Collection):
    def __get__(attr, obj, type=None):
        if obj is None: return attr
        return SetProperty(obj, attr)
    def check(attr, value, entity=None):
        if value is None or value is UNKNOWN: return None
        coldata = SetData()
        coldata.fully_loaded = True
        reverse = attr.reverse
        if not isinstance(value, reverse.entity):
            coldata.new = set(value)
            for item in coldata.new:
                if not isinstance(item, reverse.entity):
                    entity_name = (entity or attr.entity).__name__
                    raise ConstraintError('Item of collection %s.%s must be instance of %s. Got: %s'
                                          % (entity_name, attr.name, reverse.entity.__name__, item))
        else: coldata.new = set((value,))
        return coldata
    def set_reverse(attr, reverse_obj, prev_obj, obj):
        assert prev_obj is not UNKNOWN
        assert obj is not UNKNOWN
        if prev_obj is not None:
            prev_coldata = attr._get_coldata(prev_obj)
            prev_coldata.delete_many((reverse_obj,))
        if obj is not None:
            coldata = attr._get_coldata(obj)
            coldata.add_many((reverse_obj,))

##class List(Collection): pass
##class Dict(Collection): pass
##class Relation(Collection): pass

class SetProperty(object):
    __slots__ = '_obj_', '_attr_'
    def __init__(col, obj, attr):
        self._obj_ = obj
        self._attr_ = attr
    def _get_stuff_(self):
        obj = col.obj
        info = obj._get_info()
        trans = local.transaction
        data = trans.objects.get(obj)
        if data is None:
            if pk is None: raise TransferringObjectWithoutPkError(obj)
            raise NotImplementedError
        offset = obj._new_offsets_[col.attr]
        coldata = data[offset]
        if coldata is None:
            coldata = data[offset] = SetData()
            coldata.fully_loaded = True
        elif coldata is UNKNOWN:
            coldata = data[offset] = SetData()
        return coldata
    def __len__(col):
        coldata = self._get_stuff_()
        if coldata.fully_loaded: return len(coldata.new)
        raise NotImplementedError
    def __contains__(self, x):
        coldata = self._get_stuff_()
        if coldata.fully_loaded: return x in coldata.new
        raise NotImplementedError
    def __add__(self, x):
        raise NotImplementedError
    def __iadd__(self, x):
        coldata = self._get_stuff_()
        coldata.new.add(x)
        raise NotImplementedError
    def __sub__(self, x):
        raise NotImplementedError
    def __isub__(self, x):
        raise NotImplementedError
    def __eq__(self, x):
        raise NotImplementedError
    def __ne__(self, x):
        raise NotImplementedError

SETDATA_LOADED = 1
SETDATA_EXISTS = 2
SETDATA_KNOWN = 4
SETDATA_ADDED = 8
SETDATA_DELETED = 16

class SetData(object):
    __slots__ = 'dict', 'fully_loaded'
    def __init__(coldata, objects=None):
        if objects is None:
            coldata.fully_loaded = False
            coldata.dict = {}  # object -> status
        else:
            coldata.fully_loaded = True
            coldata.dict = dict.fromkeys(izip(objects, repeat(SETDATA_ADDED)))
        
    def __len__(coldata):
        if not coldata.fully_loaded: raise NotImplementedError
        result = 0
        setitem = coldata.dict.__setitem__
        DELETED = SETDATA_DELETED; ADDED = SETDATA_ADDED; KNOWN = SETDATA_KNOWN; EXISTS = SETDATA_EXISTS
        for obj, status in coldata.dict.iteritems():
            if status & DELETED: continue
            if status & ADDED: result += 1
            else:
                if not status & KNOWN: setitem(obj, status | KNOWN)
                if status & EXISTS: result += 1
        return result
    def __iter__(coldata):
        if not coldata.fully_loaded: raise NotImplementedError
        get = coldata.dict.get; setitem = coldata.dict.__setitem__
        DELETED = SETDATA_DELETED; ADDED = SETDATA_ADDED; KNOWN = SETDATA_KNOWN; EXISTS = SETDATA_EXISTS
        for obj in coldata.dict.keys():
            status = get(obj, DELETED)
            if status & DELETED: continue
            if status & ADDED: yield obj
            else:
                if not status & KNOWN: setitem(obj, status | KNOWN)
                if status & EXISTS: yield obj
    def contains(coldata, obj):
        status = coldata.dict.get(obj)
        if status is None:
            if not coldata.fully_loaded: raise NotImplementedError
            return False
        if status & SETDATA_DELETED: return False
        if status & SETDATA_ADDED: return True
        if not status & SETDATA_EXISTS: return False
        if not status & SETDATA_KNOWN: coldata.dict[obj] = status | SETDATA_KNOWN
        return True
    __contains__ = contains
    def debug(coldata):
        for obj, status in coldata.dict.iteritems():
            list = []
            if status & SETDATA_LOADED: list.append('S')
            if status & SETDATA_EXISTS: list.append('E')
            if status & SETDATA_KNOWN: list.append('U')
            if status & SETDATA_ADDED: list.append('A')
            if status & SETDATA_DELETED: list.append('D')
            print obj, ''.join(list) or None
    def load_many(coldata, objects):
        assert not coldata.fully_loaded
        mask = SETDATA_LOADED | SETDATA_EXISTS
        setdefault = coldata.dict.setdefault
        setitem = coldata.dict.__setitem__
        for obj in objects:
            status = setdefault(obj, mask)
            if status & mask != mask: setitem(obj, status | mask)
    def delete_many(coldata, objects):
        get = coldata.dict.get; delitem = coldata.dict.__delitem__; setitem = coldata.dict.__setitem__
        ADDED = SETDATA_ADDED; DELETED = SETDATA_DELETED
        for obj in objects:
            status = get(obj, 0)
            if status & DELETED: continue
            setitem(obj, (status & ~ADDED) | DELETED)
    def add_many(coldata, objects):
        get = coldata.dict.get; setitem = coldata.dict.__setitem__
        ADDED = SETDATA_ADDED; DELETED = SETDATA_DELETED
        for obj in objects:
            status = get(obj, 0)
            if status & ADDED: continue
            if status & DELETED: setitem(obj, (status & ~DELETED) | ADDED)
            else: setitem(obj, status | ADDED)
    def clear(coldata):
        # can it be optimised if no reads was before? Something like coldata.cleared...
        if not coldata.fully_loaded: raise NotImplementedError
        delitem, setitem = coldata.dict.__delitem__, coldata.dict.__setitem__
        ADDED = SETDATA_ADDED; DELETED = SETDATA_DELETED
        for obj, status in coldata.dict.items():
            if status & DELETED: continue
            setitem(obj, (status & ~ADDED) | DELETED)
        
class Diagram(object):
    def __init__(diagram):
        diagram.lock = threading.RLock()
        diagram.entities = {} # entity_name -> Entity
        diagram.transactions = set()
    def clear(diagram):
        diagram.lock.acquire()
        try: diagram._clear()
        finally: diagram.lock.release()
    def _clear(diagram):
        diagram.lock.acquire()
        try:
            for trans in diagram.transactions: trans.data_source.clear_schema() # ????
        finally: diagram.lock.release()

class EntityMeta(type):
    def __new__(meta, name, bases, dict):
        if 'Entity' in globals():
            if '__slots__' in dict: raise TypeError('Entity classes cannot contain __slots__ variable')
            dict['__slots__'] = ()
        return super(EntityMeta, meta).__new__(meta, name, bases, dict)
    def __init__(entity, name, bases, dict):
        super(EntityMeta, entity).__init__(name, bases, dict)
        if 'Entity' not in globals(): return
        outer_dict = sys._getframe(1).f_locals
        diagram = (dict.pop('_diagram_', None) or outer_dict.get('_diagram_')
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
    __slots__ = '__weakref__', '_pk_'
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
            if len(roots) > 1:
                raise DiagramError('With multiple inheritance of entities, inheritance graph must be diamond-like')
            entity._root_ = roots.pop()
            for base in direct_bases:
                if base._diagram_ is not diagram:
                    raise DiagramError('When use inheritance, base and derived entities must belong to same diagram')
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
                'Attribute name cannot both starts and ends with underscore. Got: %s' % name)
            if attr.entity is not None:
                raise DiagramError('Duplicate use of attribute %s' % value)
            attr.name = name
            attr.entity = entity
            new_attrs.append(attr)
        new_attrs.sort(key=attrgetter('_id_'))
        entity._new_attrs_ = new_attrs

        entity._keys_ = keys = entity.__dict__.get('_keys_', {})
        primary_keys = set(key for key, is_pk in keys.items() if is_pk)
        if direct_bases:
            if primary_keys: raise DiagramError(
                'Primary key cannot be redefined in derived classes')
            for base in direct_bases: keys.update(base._keys_)
            primary_keys = set(key for key, is_pk in keys.items() if is_pk)
                                   
        if len(primary_keys) > 1: raise DiagramError(
            'Only one primary key can be defined in each entity class')
        elif not primary_keys:
            if hasattr(entity, 'id'): raise DiagramError("Name 'id' is alredy in use")
            _keys_ = {}
            attr = PrimaryKey(int, autoincrement=True) # Side effect: modifies _keys_ local variable
            attr.name = 'id'
            attr.entity = entity
            type.__setattr__(entity, 'id', attr)  # entity.id = attr
            entity._new_attrs_.insert(0, attr)
            key, is_pk = _keys_.popitem()
            entity._keys_[key] = True
            entity._pk_attrs_ = key
        else: entity._pk_attrs_ = primary_keys.pop()
        for i, attr in enumerate(entity._pk_attrs_): attr.pk_offset = i

        entity._attrs_ = base_attrs + new_attrs
        entity._attr_dict_ = dict((attr.name, attr) for attr in entity._attrs_)

        next_offset = count(len(DATA_HEADER)).next
        entity._old_offsets_ = old_offsets = {}
        entity._new_offsets_ = new_offsets = {}
        for attr in entity._attrs_:
            if attr.pk_offset is None and not isinstance(attr, Collection):
                old_offsets[attr] = next_offset()
                new_offsets[attr] = next_offset()
            else: old_offsets[attr] = new_offsets[attr] = next_offset()
        data_size = next_offset()
        entity._data_template_ = DATA_HEADER + [ UNKNOWN ]*(data_size - len(DATA_HEADER))

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
                if attr2 is None:
                    raise DiagramError('Reverse attribute %s.%s not found' % (entity2.__name__, reverse))
            elif isinstance(reverse, Attribute):
                attr2 = reverse
                if attr2.entity is not entity2:
                    raise DiagramError('Incorrect reverse attribute %s used in %s' % (attr2, attr))
            elif reverse is not None:
                raise DiagramError("Value of 'reverse' option must be string. Got: %r" % type(reverse))
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
                else: raise DiagramError('Reverse attribute for %s not found' % attr)

            type2 = attr2.py_type
            msg = 'Inconsistent reverse attributes %s and %s'
            if isinstance(type2, basestring):
                if type2 != entity.__name__: raise DiagramError(msg % (attr, attr2))
                attr2.py_type = entity
            elif type2 != entity: raise DiagramError(msg % (attr, attr2))
            reverse2 = attr2.reverse
            if reverse2 not in (None, attr, attr.name): raise DiagramError(msg % (attr,attr2))

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
            if data_source is not None: data_source.begin()
            else: raise TransactionError('There are no active transaction in thread %s. '
                                         'Cannot start transaction automatically, '
                                         'because default data source does not set'
                                         % thread.get_ident())
        else: data_source = trans.data_source
        info = data_source.entities.get(entity)
        if info is not None: return info
        data_source.generate_schema(entity._diagram_)
        return data_source.entities[entity]
    def __init__(obj, *args, **keyargs):
        raise TypeError('You cannot create entity instances directly. '
                        'Use Entity.create(...) or Entity.find(...) instead')
    def _get_data(obj, status):
        trans = local.transaction
        data = trans.objects.get(obj)
        if data is None:
            if pk is None: raise TransferringObjectWithoutPkError(obj)
            data = trans.objects[obj] = obj._data_template_[:]
            data[0] = obj
            data[1] = status
            get_offset = obj._new_offsets_.__getitem__
            for a, v in zip(obj._pk_attrs_, pk): data[get_offset(a)] = v
        return data
    @property
    def old(obj):
        return OldProxy(obj)
    @classmethod
    def find(entity, *args, **keyargs):
        raise NotImplementedError
    @classmethod
    def create(entity, *args, **keyargs):
        if args:
            if len(args) != len(entity._pk_attrs_):
                raise CreateError('Invalid count of attrs in primary key')
            for attr, value in zip(entity._pk_attrs_, args):
                if keyargs.setdefault(attr.name, value) != value:
                    raise CreateError('Ambiguous attribute value for %r' % attr.name)
        for name in ifilterfalse(entity._attr_dict_.__contains__, keyargs):
            raise CreateError('Unknown attribute %r' % name)

        get_offset = entity._new_offsets_.__getitem__
        data = entity._data_template_[:]
        for attr in entity._attrs_:
            value = keyargs.get(attr.name, UNKNOWN)
            if value is not UNKNOWN and isinstance(attr, Collection):
                raise NotImplementedError
            data[get_offset(attr)] = attr.check(value, entity)
        pk = args or tuple(map(data.__getitem__, map(get_offset, entity._pk_attrs_)))
        if None in pk:
            obj = object.__new__(entity)
            obj._pk_ = None
        else:
            entity._lock_.acquire()
            try:
                obj = entity._objects_.get(pk)
                if obj is None:
                    obj = object.__new__(entity)
                    obj._pk_ = pk
                    entity._objects_[pk] = obj
            finally: entity._lock_.release()
        data[0] = obj
        data[1] = 'C'

        info = entity._get_info()
        trans = local.transaction
        try:
            for key in entity._keys_:
                key_value = tuple(map(data.__getitem__, map(get_offset, key)))
                if None in key_value: continue
                try: old_index, new_index = trans.indexes[key]
                except KeyError: old_index, new_index = trans.indexes[key] = ({}, {})
                obj2 = new_index.setdefault(key_value, obj)
                if obj2 is not obj:
                    key_str = ', '.join(repr(item) for item in key_value)
                    key_type = key is entity._pk_attrs_ and 'primary key' or 'unique index'
                    raise CreateError('%s with such %s already exists: %s' % (obj2.__class__.__name__, key_type, key_str))
            for attr in entity._attrs_:
                if attr.reverse is None: continue
                value = data[get_offset(attr)]
                if value is None: continue
                attr.reverse.set_reverse(obj, None, value)
        except CreateError, e:
            for key in entity._keys_:
                key_value = tuple(map(data.__getitem__, map(get_offset, key)))
                try: old_index, new_index = trans.indexes.get(key)
                except TypeError: continue  # trans.indexes.get(key) is None
                if new_index.get(key_value) is obj: del new_index[key_value]
            raise e
        if trans.objects.setdefault(obj, data) is not data: raise AssertionError

        if obj._pk_ is None: return obj

        for table in info.tables:
            cache = trans.caches.get(table)
            if cache is None: cache = trans.caches[table] = Cache(table)
            new_row = cache.row_template[:]
            new_row[0] = obj
            new_row[1] = 'C'
            for column in table.columns:
                for attr in column.attrs:
                    if entity is attr.entity or issubclass(entity, attr.entity):
                        value = data[get_offset(attr)]
                        new_row[column.new_offset] = value
                        break
                else: new_row[column.new_offset] = None
            if cache.rows.setdefault(pk, new_row) is not new_row: raise AssertionError
        return obj
    def set(obj, **keyargs):
        pk = obj._pk_
        info = obj._get_info()
        trans = local.transaction
        get_offset = obj._new_offsets_.__getitem__

        data = trans.objects.get(obj) or obj._get_data('U')
        old_data = data[:]

        attrs = set()
        for name, value in keyargs.items():
            attr = obj._attr_dict_.get(name)
            if attr is None: raise UpdateError("Unknown attribute: %r" % name)
            if isinstance(attr, Collection): raise NotImplementedError
            value = attr.check(value, obj.__class__)
            if data[get_offset(attr)] == value: continue
            if attr.pk_offset is not None: raise UpdateError('Cannot change value of primary key')
            attrs.add(attr)
            data[get_offset(attr)] = value
        if not attrs: return

        undo = []
        try:
            for key in obj._keys_:
                if key is obj._pk_attrs_: continue
                new_key = tuple(map(data.__getitem__, map(get_offset, key)))
                old_key = tuple(map(old_data.__getitem__, map(get_offset, key)))
                if None in new_key or UNKNOWN in new_key: new_key = None
                if None in old_key or UNKNOWN in old_key: old_key = None
                if old_key == new_key: continue
                try: old_index, new_index = trans.indexes[key]
                except KeyError: old_index, new_index = trans.indexes[key] = ({}, {})
                if new_key is not None:
                    obj2 = new_index.setdefault(new_key, obj)
                    if obj2 is not obj:
                        key_str = ', '.join(repr(item) for item in new_key)
                        raise UpdateError('Cannot update %s.%s: %s with such unique index already exists: %s'
                                          % (obj.__class__.__name__, attr.name, obj2.__class__.__name__, key_str))
                if old_key is not None: del new_index[old_key]
                undo.append((new_index, obj, old_key, new_key))
            for attr in attrs:
                if attr.reverse is None: continue
                offset = get_offset(attr)
                attr.reverse.set_reverse(obj, old_data[offset], data[offset])
        except UpdateError:
            for new_index, obj, old_key, new_key in undo:
                if new_key is not None: del new_index[new_key]
                if old_key is not None: new_index[old_key] = obj
            data[:] = old_data
            raise
        if data[1] != 'C': data[1] = 'U'

        if pk is None: return

        for table in info.tables:
            cache = trans.caches.get(table)
            if cache is None: cache = trans.caches[table] = Cache(table)
            row = cache.rows.get(pk)
            if row is None:
                row = cache.row_template[:]
                row[0] = obj
                row[1] = 'U'
                for c, v in zip(table.pk_columns, pk): row[c.new_offset] = v
            else: assert row[0] is obj
            for attr in attrs:
                attr_info = info.attrs[attr]
                column = attr_info.tables.get(table)
                if column is None: continue
                if row[1] != 'C':
                    row[1] = 'U'
                    row[ROW_UPDATE_MASK] |= column.mask
                row[column.new_offset] = data[get_offset(attr)]
        
def old(obj):
    return OldProxy(obj)

class OldProxy(object):
    __slots__ = '_obj_', '_cls_'
    def __init__(old_proxy, obj):
        cls = obj.__class__
        if not issubclass(cls, Entity):
            raise TypeError('Expected subclass of Entity. Got: %s' % cls.__name__)
        object.__setattr__(old_proxy, '_obj_', obj)  # old_proxy._obj_ = obj
        object.__setattr__(old_proxy, '_cls_', cls)  # old_proxy._cls_ = cls
    def __getattr__(old_proxy, name):
        attr = getattr(old_proxy._cls_, name, None)
        if attr is None or not isinstance(attr, Attribute):
            return getattr(old_proxy._obj_, name)
        return attr.get_old(old_proxy._obj_)
    def __setattr__(old_proxy, name):
        raise TypeError('Old property values are read-only')

class EntityInfo(object):
    def __init__(info, entity, data_source):
        info.entity = entity
        info.data_source = data_source
        info.tables = {}  # Table -> dict(attr_name -> Column)
        if data_source.mapping is None: raise NotImplementedError
        entity_names = set(e.__name__ for e in entity._all_bases_)
        for table in data_source.tables.values():
            for entity_name in table.entities:
                if entity_name in entity_names:
                    info.tables[table] = {}
                    break
        info.attrs = {} # Attribute -> AttrInfo
        for attr in entity._attrs_: info.attrs[attr] = AttrInfo(info, attr)
        for attr_info in info.attrs.values():
            for table, column in attr_info.tables.items():
                info.tables[table][attr_info.attr.name] = column

class AttrInfo(object):
    def __init__(attr_info, info, attr):
        attr_info.enity_info = info
        attr_info.attr = attr
        name_pair = attr.entity.__name__, attr.name
        attr_info.tables = info.data_source.attr_map.get(name_pair, {}).copy()
        for table, column in attr_info.tables.items(): column.attrs.add(attr)
    def __repr__(attr_info):
        return '<AttrInfo: %s.%s>' % (attr_info.enity_info.entity.__name__,
                                      attr_info.attr.name)
    
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
    def _init_(data_source, mapping, provider, *args, **keyargs):
        data_source.lock = threading.RLock() # threadsafe access to datasource schema
        data_source.mapping = mapping
        data_source.provider = provider
        data_source.args = args
        data_source.keyargs = keyargs
        data_source.transactions = set()        
        data_source.tables = {}   # table_name -> Table
        data_source.diagrams = set()
        data_source.entities = {} # Entity -> EntityInfo
        data_source.attr_map = {} # (entity_name, attr_name)->(Table->Column)
        if mapping is not None: data_source.load_mapping()
    def load_mapping(data_source):
        for table_element in data_source.mapping.findall('table'):
            table = Table(data_source, table_element)
            if data_source.tables.setdefault(table.name, table) is not table:
                raise MappingError('Duplicate table definition: %s' % table.name)
            if table.entities:
                for column in table.columns:
                    for attr_name in column.attr_names:
                        tables = data_source.attr_map.setdefault(attr_name[:2], {})
                        if tables.setdefault(table, column) is not column:
                            raise NotImplementedError
    def generate_schema(data_source, diagram):
        data_source.lock.acquire()
        try:
            if diagram in data_source.diagrams: return
            for entity in diagram.entities.values():
                info = EntityInfo(entity, data_source)
                data_source.entities[entity] = info
                for key_attrs in entity._keys_:
                    name_pairs = []
                    for attr in key_attrs: name_pairs.append((attr.entity.__name__, attr.name))
                    for table in info.tables:
                        key_columns = []
                        for name_pair in name_pairs:
                            tables = data_source.attr_map.get(name_pair)
                            if tables is None:
                                raise SchemaError('Key column %r.%r does not have correspond column' % name_pair)
                            column = tables.get(table)
                            if column is None: break
                            key_columns.append(column)
                        else:
                            key_columns = tuple(key_columns)
                            if key_attrs is not entity._pk_attrs_: table.secondary_keys.add(key_columns)
                            elif not hasattr(table, 'primary_key'): table.pk_columns = key_columns
                            elif table.pk_columns != key_columns:
                                raise SchemaError('Multiple primary keys for table %r'% table.name)
            for table in data_source.tables.values():
                if not table.entities: continue
                next_offset = count(len(ROW_HEADER)).next
                mask_offset = count().next
                for i, column in enumerate(table.pk_columns): column.pk_offset = i
                for column in table.columns:
                    if column.pk_offset is None:
                        column.old_offset = next_offset()
                        column.new_offset = next_offset()
                        column.mask = 1 << mask_offset()
                    else:
                        column.old_offset = column.new_offset = next_offset()
                        column.mask = 0
        finally: data_source.lock.release()
    def clear_schema(data_source):
        data_source.lock.acquire()
        try:
            if data_source.transaction:
                raise SchemaError('Cannot clear datasource schema information '
                                  'because it is used by active transaction')
            data_source.entities.clear()
            data_source.tables.clear()
        finally: data_source.lock.release()
    def get_connection(data_source):
        provider = data_source.provider
        if isinstance(provider, basestring):
            provider = utils.import_module('pony.dbproviders.' + provider)
        return provider.connect(*data_source.args, **data_source.keyargs)
    def begin(data_source):
        return begin(data_source)

class Table(object):
    def __init__(table, data_source, x):
        table.data_source = data_source
        table.columns = []
        table.secondary_keys = set()
        if isinstance(x, basestring): table.name = x
        else: table._init_from_xml_element(x)
    def __repr__(table):
        return '<Table: %r>' % table.name
    def _init_from_xml_element(table, element):
        table.name = element.get('name')
        if not table.name:
            raise MappingError('<table> element without "name" attribute')
        table.entities = set(element.get('entity', '').split())
        table.relations = set(tuple(rel.split('.'))
                             for rel in element.get('relation', '').split())
        if table.entities and table.relations:
            raise MappingError('For table %r both entity name and relations are specified. '
                               'It is not allowed' % table.name)
        elif not table.entities and not table.relations:
            raise MappingError('For table %r neither entity name nor relations are specified. '
                               'It is not allowed' % table.name)
        for entity_name in table.entities:
            if not utils.is_ident(entity_name):
                raise MappingError('Entity name must be valid identifier. Got: %r' % entity_name)
        for relation in table.relations:
            if len(relation) != 2:
                raise MappingError('Each relation must be in form of EntityName.AttributeName. '
                                   'Got: %r' % '.'.join(relation))
            for component in relation:
                if not utils.is_ident(component):
                    raise MappingError('Each part of relation name must be valid identifier. '
                                       'Got: %r' % component)
        table.columns = []
        table.cdict = {}
        for col_element in element.findall('column'):
            column = Column(table, col_element)
            if table.cdict.setdefault(column.name, column) is not column:
                raise MappingError('Duplicate column definition: %r.%r' % (table.name, column.name))
            table.columns.append(column)

class Column(object):
    def __init__(column, table, x):
        column.table = table
        column.pk_offset = None
        column.attrs = set()
        if isinstance(x, basestring): column.name = x
        else: column._init_from_xml_element(x)
    def __repr__(column):
        return '<Column: %r.%r>' % (column.table.name, column.name)
    def _init_from_xml_element(column, element):
        table = column.table
        column.name = element.get('name')
        if not column.name:
            raise MappingError('Error in table definition %r: '
                               'Column element without "name" attribute' % table.name)
        column.domain = element.get('domain')
        column.attr_names = set(tuple(attr.split('.'))
                                for attr in element.get('attr', '').split())
        for attr_name in column.attr_names:
            if len(attr_name) < 2:
                raise MappingError('Invalid attribute value in column %r.%r: '
                                   'must be in form of EntityName.AttributeName'
                                   % (table.name, column.name))
        if table.relations:
            for attr_name in column.attr_names:
                if attr_name[:2] not in table.relations:
                    raise MappingError('Attribute %s does not correspond any relation' % '.'.join(attr_name))
        column.kind = element.get('kind')
        if column.kind not in (None, 'discriminator'):
            raise MappingError('Error in column %r.%r: invalid column kind: %r'
                               % (table.name, column.name, column.kind))
        cases = element.findall('case')
        if cases and column.kind != 'discriminator':
            raise MappingError('Non-discriminator column %r.%r contains cases. It is not allowed'
                               % (table.name, column.name))
        column.cases = [ (case.get('value'), case.get('entity')) for case in cases ]
        for value, entity in column.cases:
            if not value or not entity:
                raise MappingError('Invalid discriminator case in column %r.%r'
                                   % (table.name, column.name))

class Transaction(object):
    def __init__(trans, data_source, connection=None):
        if local.transaction is not None: raise TransactionError(
            'Transaction already started in thread %d' % thread.get_ident())
        trans.data_source = data_source
        trans.connection = connection
        trans.diagrams = set()
        trans.caches = {}  # Table -> Cache
        trans.objects = {} # object -> row
        trans.indexes = {} # key_attrs -> ({old_key -> obj}, {new_key -> obj})
        data_source.lock.acquire()
        try: data_source.transactions.add(trans)
        finally: data_source.lock.release()
        local.transaction = trans
    def _close(trans):
        assert local.transaction is trans
        data_source.lock.acquire()
        try:
            while trans.diagrams:
                diagram = trans.diagrams.pop()
                diagram.transactions.remove(trans)
            data_source.transactions.remove(trans)
        finally: data_source.lock.release()
        local.transaction = None
    def commit(trans):
        trans._close()
        raise NotImplementedError
    def rollback(trans):
        trans._close()
        raise NotImplementedError

class Cache(object):
    def __init__(trans, table):
        trans.table = table
        row_size = table.columns[-1].new_offset + 1
        trans.row_template = ROW_HEADER + [ UNKNOWN ]*(row_size-len(ROW_HEADER))
        trans.rows = {}

class Local(threading.local):
    def __init__(trans):
        trans.transaction = None

local = Local()

def get_transaction():
    return local.transaction

def no_trans_error():
    raise TransactionError('There are no active transaction in thread %s' % thread.get_ident())

def begin(data_source=None):
    if local.transaction is not None:
        raise TransactionError('Transaction already started in thread %d' % thread.get_ident())
    if data_source is not None: return Transaction(data_source)
    outer_dict = sys._getframe(1).f_locals
    data_source = outer_dict.get('_data_source_')
    if data_source is None:
        raise TransactionError('Can not start transaction, because default data source is not set')
    return Transaction(data_source)

def commit():
    trans = local.transaction
    if trans is None: no_trans_error()
    trans.commit()

def rollback():
    trans = local.transaction
    if trans is None: no_trans_error()
    trans.rollback()
