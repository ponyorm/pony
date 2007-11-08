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
        attr.auto = keyargs.pop('auto', False)

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
        for option in keyargs: raise TypeError('Unknown option %r' % option)
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
            if entity is None: entity = attr.entity
            raise ConstraintError('Value of attribute %s.%s must be an instance of %s. Got: %s'
                                  % (entity.__name__, attr.name, reverse.entity.__name__, value))
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
        data = trans.objects.get(obj) or obj._get_data('R')
        value = data[obj._new_offsets_[attr]]
        if value is UNKNOWN: raise NotImplementedError
        return value
    def __set__(attr, obj, value, undo_funcs=None):
        value = attr.check(value, obj.__class__)
        pk = obj._pk_
        if attr.pk_offset is not None:
            if pk is not None and value == pk[attr.pk_offset]: return
            raise UpdateError('Cannot change value of primary key')

        attr_info = obj._get_info().attrs[attr]
        trans = local.transaction
        data = trans.objects.get(obj) or obj._get_data('U')
        get_new_offset = obj._new_offsets_.__getitem__
        prev = data[get_new_offset(attr)]
        if attr.reverse and prev is UNKNOWN:
            raise NotImplementedError
        if prev == value: return

        is_reverse_call = undo_funcs is not None
        if not is_reverse_call: undo_funcs = []
        undo = []
        def undo_func():
            for new_index, obj, old_key, new_key in undo:
                if new_key is not None: del new_index[new_key]
                if old_key is not None: new_index[old_key] = obj
        undo_funcs.append(undo_func)
        try:
            for key in obj._keys_:
                if key is obj._pk_attrs_: continue
                if attr not in key: continue
                position = list(key).index(attr)
                new_key = map(data.__getitem__, map(get_new_offset, key))
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
            if attr.reverse is not None:
                old = data[obj._old_offsets_[attr]]
                if old is UNKNOWN: raise NotImplementedError
                if not is_reverse_call: attr.update_reverse(obj, prev, value, undo_funcs)
                elif prev is not None:
                    reverse = attr.reverse
                    if not isinstance(reverse, Collection): reverse.__set__(prev, None, undo_funcs)
                    elif isinstance(reverse, Set): reverse.reverse_remove((prev,), obj, undo_funcs)
                    else: raise NotImplementedError
        except:
            if not is_reverse_call:
                for undo_func in reversed(undo_funcs): undo_func()
            raise

        if data[1] != 'C': data[1] = 'U'
        data[get_new_offset(attr)] = value

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
    def update_reverse(attr, obj, prev, value, undo_funcs):
        reverse = attr.reverse
        if not isinstance(reverse, Collection):
            if prev is not None: reverse.__set__(prev, None, undo_funcs)
            if value is not None: reverse.__set__(value, obj, undo_funcs)
        elif isinstance(reverse, Set):
            if prev is not None: reverse.reverse_remove((prev,), obj, undo_funcs)
            if value is not None: reverse.reverse_add((value,), obj, undo_funcs)
        else: raise NotImplementedError

class Optional(Attribute): pass

class Required(Attribute):
    def check(attr, value, entity=None):
        msg = None
        if value is UNKNOWN:
            value = attr.default
            if value is None and not attr.auto: msg = 'Required attribute %s.%s does not specified'
        elif value is None: msg = 'Required attribute %s.%s cannot be set to None'
        if msg is None: return Attribute.check(attr, value, entity)
        if entity is None: entity = attr.entity
        raise ConstraintError(msg % (entity.__name__, attr.name))

class Unique(Required):
    def __new__(cls, *args, **keyargs):
        is_primary_key = issubclass(cls, PrimaryKey)
        if not args: raise TypeError('Invalid count of positional arguments')
        attrs = tuple(a for a in args if isinstance(a, Attribute))
        non_attrs = [ a for a in args if not isinstance(a, Attribute) ]
        if attrs and non_attrs: raise TypeError('Invalid arguments')
        cls_dict = sys._getframe(1).f_locals
        keys = cls_dict.setdefault('_keys_', {})
        if not attrs:
            result = Required.__new__(cls, *args, **keyargs)
            keys[(result,)] = is_primary_key
            return result
        else:
            msg = None
            for attr in attrs:
                if isinstance(attr, Collection):
                    key_type = is_primary_key and 'primary key' or 'unique index'
                    msg = "Collection attribute '%s' cannot be part of " + key_type
                elif is_primary_key and isinstance(attr, Optional):
                    msg = "Optional attribute '%s' cannot be part of primary key"
                if msg is not None:
                    attr_name = ''
                    for name, value in cls_dict.items():
                        if value is attr: attr_name = name
                    raise TypeError(msg % attr_name)
            keys[attrs] = issubclass(cls, PrimaryKey)

class PrimaryKey(Unique): pass

class Collection(Attribute):
    def __init__(attr, py_type, *args, **keyargs):
        if attr.__class__ is Collection: raise TypeError("'Collection' is abstract type")
        Attribute.__init__(attr, py_type, *args, **keyargs)
        if attr.default is not None: raise TypeError(
            'default value could not be set for collection attribute %s' % attr)
        if attr.auto: raise TypeError(
            "'auto' option could not be set for collection attribute %s" % attr)
    def __get__(attr, obj, type=None):
        assert False, 'Abstract method'
    def __set__(attr, obj, value):
        assert False, 'Abstract method'
    def __delete__(attr, obj):
        assert False, 'Abstract method'
    def reverse_add(attr, objects, reverse_obj, undo_funcs):
        assert False, 'Abstract method'
    def reverse_remove(attr, objects, reverse_obj, undo_funcs):
        assert False, 'Abstract method'

class Set(Collection):
    def check(attr, value, entity=None):
        if value is None or value is UNKNOWN: return None
        reverse = attr.reverse
        if not isinstance(value, reverse.entity):
            try:
                result = set(value)  # may raise TypeError if value is not iterable
                for value in result:
                    if not isinstance(value, reverse.entity): raise TypeError
            except TypeError:
                if entity is None: entity = attr.entity
                raise TypeError('Item of collection %s.%s must be instance of %s. Got: %s'
                                % (entity.__name__, attr.name, reverse.entity.__name__, value))
        else: result = set((value,))
        return result
    def reverse_add(attr, objects, reverse_obj, undo_funcs):
        trans = local.transaction
        undo = []
        for obj in objects:
            data = trans.objects.get(obj) or obj._get_data('U')
            new_offset = obj._new_offsets_[attr]
            value = data[new_offset]
            if value is None: value = data[new_offset] = set()
            undo.append(value)
            value.add(reverse_obj)
        def undo_func():
            for value in undo:
                value.remove(reverse_obj)
        undo_funcs.append(undo_func)
    def reverse_remove(attr, objects, reverse_obj, undo_funcs):
        trans = local.transaction
        undo = []
        for obj in objects:
            data = trans.objects.get(obj) or obj._get_data('U')
            new_offset = obj._new_offsets_[attr]
            value = data[new_offset]
            undo.append(value)
            value.remove(reverse_obj) # ???
        def undo_func():
            for value in undo:
                value.add(reverse_obj)
        undo_funcs.append(undo_func)
    def __get__(attr, obj, type=None):
        if obj is None: return attr
        return SetProperty(obj, attr)
    def __set__(attr, obj, value):
        value = attr.check(value, obj.__class__)
        info = obj._get_info()
        trans = local.transaction
        data = trans.objects.get(obj) or obj._get_data('R')
        old_offset = obj._old_offsets_[attr]
        new_offset = obj._new_offsets_[attr]
        prev = data[new_offset]
        if prev == value: return

        old = data[old_offset]
        if old is not None:
            if old is UNKNOWN or not old.loaded: raise NotImplementedError

        undo_funcs = []
        data[new_offset] = value
        try: attr.update_reverse(obj, prev, value, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
            data[new_offset] = prev
            raise
    def __delete__(attr, obj):
        raise NotImplementedError
    def update_reverse(attr, obj, prev, value, undo_funcs):
        reverse = attr.reverse
        if not isinstance(reverse, Collection):
            if prev is not None:
                if value is None: remove_set = prev
                else: remove_set = prev.difference(value)
                for reverse_obj in remove_set: reverse.__set__(reverse_obj, None, undo_funcs)
            if value is not None:
                if prev is None: add_set = value
                else: add_set = value.difference(prev)
                for reverse_obj in add_set: reverse.__set__(reverse_obj, obj, undo_funcs)
        elif isinstance(reverse, Set):
            if prev is not None:
                if value is None: reverse.reverse_remove(prev, obj, undo_funcs)
                else: reverse.reverse_remove(prev.difference(value), obj, undo_funcs)
            if value is not None:
                if prev is None: reverse.reverse_add(value, obj, undo_funcs)
                else: reverse.reverse_add(value.difference(prev), obj, undo_funcs)
        else: raise NotImplementedError

##class List(Collection): pass
##class Dict(Collection): pass
##class Relation(Collection): pass

class SetProperty(object):
    __slots__ = '_obj_', '_attr_'
    def __init__(setprop, obj, attr):
        setprop._obj_ = obj
        setprop._attr_ = attr
    def _get_value(setprop):
        attr = setprop._attr_
        obj = setprop._obj_
        info = obj._get_info()
        trans = local.transaction
        data = trans.objects.get(obj) or obj._get_data('R')

        old_offset = obj._old_offsets_[attr]
        prev = data[old_offset]
        if prev is not None:
            if prev is UNKNOWN or not prev.loaded: raise NotImplementedError

        new_offset = obj._new_offsets_[attr]
        value = data[new_offset]
        if value is None: return set()
        return value
    def __repr__(setprop):
        return '%r.%s->%r' % (setprop._obj_, setprop._attr_.name, setprop._get_value())
    def __len__(setprop):
        return len(setprop._get_value())
    def __iter__(setprop):
        return iter(list(setprop._get_value()))
    def __eq__(setprop, x):
        attr = setprop._attr_
        if isinstance(x, SetProperty) and setprop._obj_ is x._obj_ and _attr_ is x._attr_: return True
        if isinstance(x, attr.py_type): x = set((x,))
        elif not isinstance(x, set): x = set(x)
        return setprop._get_value() == x
    def __ne__(setprop, x):
        return not setprop.__eq__(x)
    def __add__(setprop, x):
        attr = setprop._attr_
        if isinstance(x, attr.py_type): x = set((x,))
        return setprop._get_value().union(x)
    def __sub__(setprop, x):
        attr = setprop._attr_
        if isinstance(x, attr.py_type): x = set((x,))
        elif not isinstance(x, set): x = set(x)
        return setprop._get_value().union(x)
    def __contains__(setprop, x):
        attr = setprop._attr_
        obj = setprop._obj_
        info = obj._get_info()
        trans = local.transaction
        data = trans.objects.get(obj) or obj._get_data('R')

        new_offset = obj._new_offsets_[attr]
        value = data[new_offset]
        if value is None: return False
        if x in value: return True
        
        old_offset = obj._old_offsets_[attr]
        prev = data[old_offset]
        if prev is None: return False
        if prev is UNKNOWN or not prev.loaded: raise NotImplementedError
        return False
    def __iadd__(setprop, x):
        attr = setprop._attr_
        obj = setprop._obj_
        info = obj._get_info()
        trans = local.transaction
        data = trans.objects.get(obj) or obj._get_data('R')

        new_offset = obj._new_offsets_[attr]
        value = data[new_offset]
        if value is None: value = data[new_offset] = set()

        add_set = attr.check(x, obj.__class__)
        add_set.difference_update(value)
        if not add_set: return setprop

        undo_funcs = []
        reverse = attr.reverse
        try:
            if not isinstance(reverse, Collection):
                for obj2 in add_set: reverse.__set__(obj2, obj, undo_funcs)
            elif isinstance(reverse, Set): reverse.reverse_add(add_set, obj, undo_funcs)
            else: raise NotImplementedError
        except:
            for undo_func in undo_funcs: undo_func()
            raise
        value.update(add_set)
        return setprop
    def __isub__(setprop, x):
        attr = setprop._attr_
        obj = setprop._obj_
        info = obj._get_info()
        trans = local.transaction
        data = trans.objects.get(obj) or obj._get_data('R')

        new_offset = obj._new_offsets_[attr]
        value = data[new_offset]
        if value is None: value = set()

        remove_set = attr.check(x, obj.__class__)
        remove_set.intersection_update(value)
        if not remove_set: return setprop
        
        undo_funcs = []
        reverse = attr.reverse
        try:
            if not isinstance(reverse, Collection):
                for obj2 in remove_set: reverse.__set__(obj2, None, undo_funcs)
            elif isinstance(reverse, Set): reverse.reverse_remove(remove_set, obj, undo_funcs)
            else: raise NotImplementedError
        except:
            for undo_func in undo_funcs: undo_func()
            raise
        value.difference_update(remove_set)
        return setprop

class _OldSet(set):
    __slots__ = 'loaded'

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

new_instance_counter = count(1).next

class Entity(object):
    __metaclass__ = EntityMeta
    __slots__ = '__weakref__', '_pk_', '_new_'
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
            attr = PrimaryKey(int, auto=True) # Side effect: modifies _keys_ local variable
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
            if attr.pk_offset is None:
                old_offsets[attr] = next_offset()
                new_offsets[attr] = next_offset()
            else: old_offsets[attr] = new_offsets[attr] = next_offset()
        data_size = next_offset()
        entity._data_template_ = DATA_HEADER + [ UNKNOWN ]*(data_size - len(DATA_HEADER))

        diagram.lock.acquire()
        try:
            diagram.clear()
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
    def __repr__(obj):
        pk = obj._pk_
        if pk is None: key_str = 'new:%d' % obj._new_
        else: key_str = ', '.join(repr(item) for item in pk)
        return '%s(%s)' % (obj.__class__.__name__, key_str)
    def _get_data(obj, status):
        trans = local.transaction
        data = trans.objects.get(obj)
        if data is None:
            pk = obj._pk_
            if pk is None: raise TransferringObjectWithoutPkError(obj)
            data = trans.objects[obj] = obj._data_template_[:]
            data[0] = obj
            data[1] = status
            get_new_offset = obj._new_offsets_.__getitem__
            for a, v in zip(obj._pk_attrs_, pk): data[get_new_offset(a)] = v
            if status != 'U': raise NotImplementedError
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

        info = entity._get_info()
        trans = local.transaction

        get_new_offset = entity._new_offsets_.__getitem__
        get_old_offset = entity._old_offsets_.__getitem__
        data = entity._data_template_[:]
        for attr in entity._attrs_:
            value = keyargs.get(attr.name, UNKNOWN)
            data[get_old_offset(attr)] = None
            data[get_new_offset(attr)] = attr.check(value, entity)
        pk = tuple(map(data.__getitem__, map(get_new_offset, entity._pk_attrs_)))
        if None in pk:
            obj = object.__new__(entity)
            obj._pk_ = None
            obj._new_ = new_instance_counter()
        else:
            obj = object.__new__(entity)
            obj._pk_ = pk
            obj._new_ = None
            entity._lock_.acquire()
            try: obj = entity._objects_.setdefault(pk, obj)
            finally: entity._lock_.release()
            if obj in trans.objects:
                key_str = ', '.join(repr(item) for item in pk)
                raise CreateError('%s with such primary key already exists: %s' % (obj.__class__.__name__, key_str))
        data[0] = obj
        data[1] = 'C'

        undo_funcs = []
        try:
            for key in entity._keys_:
                key_value = tuple(map(data.__getitem__, map(get_new_offset, key)))
                if None in key_value: continue
                try: old_index, new_index = trans.indexes[key]
                except KeyError: old_index, new_index = trans.indexes[key] = ({}, {})
                obj2 = new_index.setdefault(key_value, obj)
                if obj2 is not obj:
                    key_str = ', '.join(repr(item) for item in key_value)
                    raise CreateError('%s with such unique already exists: %s' % (obj2.__class__.__name__, key_str))
            for attr in entity._attrs_:
                if attr.reverse is None: continue
                value = data[get_new_offset(attr)]
                if value is None: continue
                attr.update_reverse(obj, None, value, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
            for key in entity._keys_:
                key_value = tuple(map(data.__getitem__, map(get_new_offset, key)))
                index_pair = trans.indexes.get(key)
                if index_pair is None: continue
                old_index, new_index = index_pair
                if new_index.get(key_value) is obj: del new_index[key_value]
            raise
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
                        value = data[get_new_offset(attr)]
                        new_row[column.new_offset] = value
                        break
                else: new_row[column.new_offset] = None
            if cache.rows.setdefault(pk, new_row) is not new_row: raise AssertionError
        return obj
    def set(obj, **keyargs):
        pk = obj._pk_
        info = obj._get_info()
        trans = local.transaction
        get_new_offset = obj._new_offsets_.__getitem__
        get_old_offset = obj._old_offsets_.__getitem__

        data = trans.objects.get(obj) or obj._get_data('U')
        old_data = data[:]

        attrs = set()
        for name, value in keyargs.items():
            attr = obj._attr_dict_.get(name)
            if attr is None: raise UpdateError("Unknown attribute: %r" % name)
            value = attr.check(value, obj.__class__)
            if data[get_new_offset(attr)] == value: continue
            if attr.pk_offset is not None: raise UpdateError('Cannot change value of primary key')
            attrs.add(attr)
            data[get_new_offset(attr)] = value
        if not attrs: return

        undo = []
        undo_funcs = []
        try:
            for key in obj._keys_:
                if key is obj._pk_attrs_: continue
                new_key = tuple(map(data.__getitem__, map(get_new_offset, key)))
                old_key = tuple(map(old_data.__getitem__, map(get_new_offset, key)))
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
                old = old_data[obj._old_offsets_[attr]]
                if old is UNKNOWN: raise NotImplementedError
                offset = get_new_offset(attr)
                prev = old_data[offset]
                value = data[offset]
                attr.update_reverse(obj, prev, value, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
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
                row[column.new_offset] = data[get_new_offset(attr)]
        
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
    
class Diagram(object):
    def __init__(diagram):
        diagram.lock = threading.RLock()
        diagram.entities = {} # entity_name -> Entity
        diagram.transactions = set()
    def clear(diagram):
        diagram.lock.acquire()
        try:
            for trans in diagram.transactions: trans.data_source.clear_schema() # ????
        finally: diagram.lock.release()

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
