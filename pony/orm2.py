from itertools import count
from operator import attrgetter

try: from pony.thirdparty import etree
except ImportError: etree = None

class OrmError(Exception): pass

class DiagramError(OrmError): pass
class SchemaError(OrmError): pass
class MappingError(OrmError): pass
class TransactionError(OrmError): pass

class UnknownValueType(object):
    def __repr__(self): return 'UNKNOWN'

UNKNOWN = UnknownValueType()

class DefaultValueType(object):
    def __repr__(self): return 'DEFAULT'

DEFAULT = DefaultValueType()

next_id = count().next

class Attribute(object):
    __slots__ = 'is_required', 'is_unique', 'is_indexed', 'is_collection', 'is_pk', \
                'id', 'pk_offset', 'type', 'entity', 'name', 'oldname', \
                'args', 'auto', 'default', 'reverse', 'composite_keys'
    def __init__(attr, type, *args, **keyargs):
        if attr.__class__ is Attribute: raise TypeError("'Atrribute' is abstract type")
        attr.is_required = isinstance(attr, Required)
        attr.is_unique = isinstance(attr, Unique)  # Also can be set to True later
        attr.is_indexed = attr.is_unique  # Also can be set to True later
        attr.is_collection = isinstance(attr, Collection)
        attr.is_pk = isinstance(attr, PrimaryKey)
        if attr.is_pk: attr.pk_offset = 0
        else: attr.pk_offset = None
        attr.id = next_id()
        attr.type = type
        attr.entity = attr.name = attr.oldname = None
        attr.args = args
        attr.auto = keyargs.pop('auto', False)
        try: attr.default = keyargs.pop('default')
        except KeyError: attr.default = None
        else:
            if attr.default is None and attr.is_required:
                raise TypeError('Default value for required attribute %s cannot be None' % attr)

        attr.reverse = keyargs.pop('reverse', None)
        if not attr.reverse: pass
        elif not isinstance(attr.reverse, (basestring, Attribute)):
            raise TypeError("Value of 'reverse' option must be name of reverse attribute). Got: %r" % attr.reverse)
        elif not isinstance(attr.type, (basestring, EntityMeta)):
            raise DiagramError('Reverse option cannot be set for this type %r' % attr.type)
        for option in keyargs: raise TypeError('Unknown option %r' % option)
        attr.composite_keys = []
    def _init_(attr, entity, name):
        attr.entity = entity
        attr.name = name
        attr.oldname = intern('__old_' + name)
    def __str__(attr):
        owner_name = not attr.entity and '?' or attr.entity.__name__
        return '%s.%s' % (owner_name, attr.name or '?')
    def __repr__(attr):
        return '<Attribute %s: %s>' % (attr, attr.__class__.__name__)
    def check(attr, val, obj=None, entity=None):
        assert val is not UNKNOWN
        if entity is not None: pass
        elif obj is not None: entity = obj.__class__
        else: entity = attr.entity
        if val is DEFAULT:
            val = attr.default
            if val is None and attr.is_required and not attr.auto: raise ConstraintError(
                'Required attribute %s.%s does not specified' % (entity.__name__, attr.name))
        if val is not None: pass
        elif attr.is_required: raise ConstraintError(
            'Required attribute %s.%s cannot be set to None' % (entity.__name__, attr.name))
        else: return val
        reverse = attr.reverse
        if not reverse or not val: return val
        if not isinstance(val, reverse.entity): raise ConstraintError(
            'Value of attribute %s.%s must be an instance of %s. Got: %s' % (entity.__name__, attr.name, reverse.entity.__name__, val))
        trans = obj and obj._trans_ or get_trans()
        if trans is not val._trans_: raise TransactionError('An attempt to mix objects belongs to different transactions')
        return val
    def __get__(attr, obj, cls=None):
        if obj is None: return attr
        return attr.get(obj, True)
    def __set__(attr, obj, val, is_reverse=False):
        val = attr.check(val, obj)
        prev = attr.get(obj)
        if prev == val: obj._rbits_ |= obj._bits_[attr]; return
        is_indexed = attr.is_indexed
        if is_indexed: attr.check_indexes(obj, val)
        attr.set(obj, val)
        if attr.reverse: attr.update_reverse(obj, val, is_reverse)
        if is_indexed: attr.update_indexes(obj, val)
        if obj._status_ != 'created': obj._status_ = 'updated'
    def __delete__(attr, obj):
        raise NotImplementedError
    def get(attr, obj, setbit=False):
        rbits = obj._rbits_
        if rbits is not None and setbit: obj._rbits_ |= obj._bits_[attr]
        val = obj.__dict__.get(attr.name, UNKNOWN)
        if val is UNKNOWN: raise NotImplementedError
    def set(attr, obj, val, setbit=True):
        wbits = obj._wbits_
        if wbits is not None and setbit: obj._wbits_ |= obj._bits_[attr]
        obj.__dict__[attr.name] = val
    def update_reverse(attr, obj, val, is_reverse):
        reverse = attr.reverse
        prev = obj.__dict__[attr.name]
        if not reverse.is_collection:
            if prev: reverse.__set__(prev, None, True)
            if not is_reverse and val: reverse.__set__(val, obj, True)
        elif isinstance(reverse, Set):
            if prev: reverse.get(prev).remove(obj)
            if not is_reverse and val: reverse.get(val).add(obj)
        else: raise NotImplementedError
    def new_keyvals(attr, obj, val):
        for key, i in attr.composite_keys:
            prev_keyval = obj.__dict__.get(key)
            new_keyval = list(prev_keyval)
            new_keyval[i] = val
            yield key, tuple(new_keyval)
    def check_indexes(attr, obj, val):
        trans = obj._trans_
        if val is None and trans.ignore_none: return
        if attr.is_unique:
            index = trans.indexes.get(attr)
            if index:
                obj2 = index.get(val)
                if obj2 is not None: raise UpdateError(
                    'Cannot update %s.%s: %s with such unique index value already exists: %r'
                    % (obj.__class__.__name__, attr.name, obj2.__class__.__name__, val))
        for key, new_keyval in attr.new_keyvals(obj, val):
            if trans.ignore_none and None in new_keyval: continue
            index = trans.indexes.get(key)
            if not index: continue
            obj2 = index.get(new_keyval)
            if not obj2: continue
            key_str = ', '.join(str(v) for v in new_keyval)
            raise UpdateError('Cannot update %s.%s: %s with such unique index value already exists: %r'
                              % (obj.__class__.__name__, attr.name, obj2.__class__.__name__, val))
    def update_indexes(attr, obj, val):
        trans = obj._trans_
        if val is None and trans.ignore_none: return
        if attr.is_unique:
            index = trans.indexes.get(attr)
            if index is None: index = trans.indexes[attr] = {}
            obj2 = index.setdefault(val, obj)
            assert obj2 is obj
        for key, new_keyval in attr.new_keyvals(obj, val):
            obj.__dict__[key] = new_keyval
            if trans.ignore_none and None in new_keyval: continue
            index = trans.indexes.get(key)
            if index is None: index = trans.indexes[key] = {}
            obj2 = index.setdefault(new_keyval, obj)
            assert obj2 is obj
            
class Optional(Attribute): pass
class Required(Attribute): pass

class Unique(Required):
    def __new__(cls, *args, **keyargs):
        is_pk = issubclass(cls, PrimaryKey)
        if not args: raise TypeError('Invalid count of positional arguments')
        attrs = tuple(a for a in args if isinstance(a, Attribute))
        non_attrs = [ a for a in args if not isinstance(a, Attribute) ]
        if attrs and (non_attrs or keyargs): raise TypeError('Invalid arguments')
        cls_dict = sys._getframe(1).f_locals
        keys = cls_dict.setdefault('_keys_', {})

        if not attrs:
            result = Required.__new__(cls, *args, **keyargs)
            keys[(result,)] = is_pk
            return result

        for attr in attrs:
            if attr.is_collection or (is_pk and not attr.is_required and not attr.auto): raise TypeError(
                '%s attribute cannot be part of %s' % (attr.__class__.__name__, is_pk and 'primary key' or 'unique index'))
            attr.is_indexed = True
        if len(attrs) == 1:
            attr = attrs[0]
            if not isinstance(attr, Optional): raise TypeError('Invalid declaration')
            attr.is_unique = True
        else:
            for i, attr in enumerate(attrs): attr.composite_keys.append((attrs, i))
        keys[attrs] = is_pk
        return None

class PrimaryKey(Unique): pass

class Collection(Attribute):
    def __init__(attr, py_type, *args, **keyargs):
        if attr.__class__ is Collection: raise TypeError("'Collection' is abstract type")
        Attribute.__init__(attr, py_type, *args, **keyargs)
        if attr.default is not None: raise TypeError('default value could not be set for collection attribute')
        if attr.auto: raise TypeError("'auto' option could not be set for collection attribute")
    def __get__(attr, obj, type=None):
        assert False, 'Abstract method'
    def __set__(attr, obj, val):
        assert False, 'Abstract method'
    def __delete__(attr, obj):
        assert False, 'Abstract method'
    def check_indexes(attr, obj, val):
        pass
    def update_indexes(attr, obj, val):
        pass

class Set(Collection):
    def check(attr, val, obj=None, entity=None):
        assert val is not UNKNOWN
        if val is None or val is DEFAULT: return set()
        reverse = attr.reverse
        if not reverse: raise NotImplementedError
        if isinstance(val, reverse.entity): result = set((val,))
        else:
            rentity = reverse.entity
            try:
                robjects = set(val)  # may raise TypeError if val is not iterable
                for robj in robjects:
                    if not isinstance(robj, rentity): raise TypeError
            except TypeError: raise TypeError('Item of collection %s.%s must be instance of %s. Got: %r'
                                              % (obj.__class__.__name__, attr.name, rentity.__name__, robj))
        trans = obj and obj._trans_ or get_trans()
        for robj in result:
            if robj._trans_ is not trans: raise TransactionError('An attempt to mix objects belongs to different transactions')
        return result
    def __get__(attr, obj, type=None):
        if obj is None: return attr
        return SetProperty(obj, attr)
    def __set__(attr, obj, val):
        val = attr.check(val, obj)
        prev = attr.get(obj)
        if val == prev: return
        reverse = attr.reverse
        if not reverse: raise NotImplementedError
        if reverse.is_indexed:
            for robj in val: reverse.check_indexes(robj, obj)
        attr.update_reverse(obj, val)
        attr.set(obj, val, False)
    def __delete__(attr, obj):
        raise NotImplementedError
    def update_reverse(attr, obj, val):
        prev = attr.get(obj)
        remove_set = prev and (not val and prev or prev-val) or ()
        add_set = val and (not prev and val or val-prev) or ()
        reverse = attr.reverse
        if not isinstance(reverse, Collection):
            for robj in remove_set: reverse.__set__(robj, None, True)
            for robj in add_set: reverse.__set__(robj, obj, True)
        elif isinstance(reverse, Set):
            for robj in remove_set: reverse.get(robj).remove(obj)
            for robj in add_set: reverse.get(robj).add(obj)
            if attr < reverse:
                d = trans.m2m.setdefault(attr, {})
                for robj in remove_set: d[obj, robj] = False
                for robj in add_set: d[obj, robj] = True
            else:
                d = trans.m2m.setdefault(reverse, {})
                for robj in remove_set: d[robj, obj] = False
                for robj in add_set: d[robj, obj] = True
        else: raise NotImplementedError

##class List(Collection): pass
##class Dict(Collection): pass
##class Relation(Collection): pass

class SetProperty(object):
    __slots__ = '_obj_', '_attr_'
    def __init__(setprop, obj, attr):
        setprop._obj_ = obj
        setprop._attr_ = attr
    def copy(setprop):
        return setprop._attr_.get(setprop._obj_).copy()
    def __repr__(setprop):
        val = setprop._attr_.get(setprop._obj_)
        return '%r.%s->%r' % (setprop._obj_, setprop._attr_.name, val)
    def __len__(setprop):
        return len(setprop._attr_.get(setprop._obj_))
    def __iter__(setprop):
        return iter(list(setprop._attr_.get(setprop._obj_)))
    def __eq__(setprop, x):
        val = setprop._attr_.get(setprop._obj_)
        if isinstance(x, SetProperty):
            if setprop._obj_ is x._obj_ and _attr_ is x._attr_: return True
            return val == x._attr_.get(x._obj_)
        if not isinstance(x, set): x = set(x)
        return val == x
    def __ne__(setprop, x):
        return not setprop.__eq__(x)
    def __add__(setprop, x):
        return setprop._attr_.get(setprop._obj_).union(x)
    def __sub__(setprop, x):
        return setprop._attr_.get(setprop._obj_).difference(x)
    def __contains__(setprop, x):
        return x in setprop._attr_.get(setprop._obj_)
    def __iadd__(setprop, x):
        attr, obj = setprop._attr_, setprop._obj_
        val = attr.get(obj)
        add_set = attr.check(x, obj)
        add_set.difference_update(val)
        if not add_set: return setprop
        reverse = attr.reverse
        if not reverse: raise NotImplementedError
        if reverse.is_indexed:
            for robj in add_set: reverse.check_indexes(robj, obj)
            for robj in add_set: reverse.__set__(robj, obj, True)
        elif isinstance(reverse, Set):
            for robj in add_set: reverse.get(robj).add(obj)
        else: raise NotImplementedError
        val.update(add_set)
        return setprop
    def __isub__(setprop, x):
        attr, obj = setprop._attr_, setprop._obj_
        val = attr.get(obj)
        remove_set = attr.check(x, obj)
        remove_set.intersection_update(val)
        if not remove_set: return setprop
        reverse = attr.reverse
        if not reverse: raise NotImplementedError
        if not reverse.is_collection:
            for robj in remove_set: reverse.__set__(robj, None, True)
        elif isinstance(reverse, Set):
            for robj in remove_set: reverse.get(robj).remove(obj)
        else: raise NotImplementedError
        val.difference_update(remove_set)
        return setprop

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
        diagram = (dict.pop('_diagram_', None)
                   or outer_dict.get('_diagram_')
                   or outer_dict.setdefault('_diagram_', Diagram()))
        if not hasattr(diagram, 'data_source'):
            diagram.data_source = outer_dict.get('_data_source_')
        entity._cls_init_(diagram)
    def __setattr__(entity, name, val):
        entity._cls_setattr_(name, val)
    def __iter__(entity):
        return iter(())

new_instance_next_id = count(1).next

class Entity(object):
    __metaclass__ = EntityMeta
    __slots__ = '__dict__', '__weakref__', '_pkval_', '_newid_', '_trans_', '_status_', '_rbits_', '_wbits_'
    @classmethod
    def _cls_setattr_(entity, name, val):
        if name.startswith('_') and name.endswith('_'):
            type.__setattr__(entity, name, val)
        else: raise NotImplementedError
    @classmethod
    def _cls_init_(entity, diagram):
        if entity.__name__ in diagram.entities:
            raise DiagramError('Entity %s already exists' % entity.__name__)
        entity._objects_ = {}
        entity._lock_ = threading.Lock()
        direct_bases = [ c for c in entity.__bases__ if issubclass(c, Entity) and c is not Entity ]
        entity._direct_bases_ = direct_bases
        entity._all_bases_ = set((entity,))
        for base in direct_bases: entity._all_bases_.update(base._all_bases_)
        if direct_bases:
            roots = set(base._root_ for base in direct_bases)
            if len(roots) > 1: raise DiagramError(
                'With multiple inheritance of entities, inheritance graph must be diamond-like')
            entity._root_ = roots.pop()
            for base in direct_bases:
                if base._diagram_ is not diagram: raise DiagramError(
                    'When use inheritance, base and derived entities must belong to same diagram')
        else: entity._root_ = entity

        base_attrs = []
        base_attrs_dict = {}
        for base in direct_bases:
            for a in base._attrs_:
                if base_attrs_dict.setdefault(a.name, a) is not a: raise DiagramError('Ambiguous attribute name %s' % a.name)
                base_attrs.append(a)
        entity._base_attrs_ = base_attrs

        new_attrs = []
        for name, attr in entity.__dict__.items():
            if name in base_attrs_dict: raise DiagramError('Name %s hide base attribute %s' % (name,base_attrs_dict[name]))
            if not isinstance(attr, Attribute): continue
            if name.startswith('_') and name.endswith('_'): raise DiagramError(
                'Attribute name cannot both starts and ends with underscore. Got: %s' % name)
            if attr.entity is not None: raise DiagramError('Duplicate use of attribute %s' % name)
            attr._init_(entity, name)
            new_attrs.append(attr)
        new_attrs.sort(key=attrgetter('_id_'))
        entity._new_attrs_ = new_attrs

        keys = entity.__dict__.get('_keys_', {})
        primary_keys = set(key for key, is_pk in keys.items() if is_pk)
        if direct_bases:
            if primary_keys: raise DiagramError('Primary key cannot be redefined in derived classes')
            for base in direct_bases:
                keys[base._keys_[0]] = True
                for key in base._keys_[1:]: keys[key] = False
            primary_keys = set(key for key, is_pk in keys.items() if is_pk)
                                   
        if len(primary_keys) > 1: raise DiagramError('Only one primary key can be defined in each entity class')
        elif not primary_keys:
            if hasattr(entity, 'id'): raise DiagramError(
                "Cannot create primary key for %s automatically because name 'id' is alredy in use" % entity.__name__)
            _keys_ = {}
            attr = PrimaryKey(int, auto=True) # Side effect: modifies _keys_ local variable
            attr._init_(entity, 'id')
            type.__setattr__(entity, 'id', attr)  # entity.id = attr
            entity._new_attrs_.insert(0, attr)
            key, is_pk = _keys_.popitem()
            keys[key] = True
            pk_attrs = key
        else: pk_attrs = primary_keys.pop()
        entity._keys_ = [ pk_attrs ] + [ key for key, is_pk in keys.items() if not is_pk ]

        for i, attr in enumerate(pk_attrs): attr.pk_offset = i

        entity._attrs_ = base_attrs + new_attrs
        entity._attr_dict_ = dict((attr.name, attr) for attr in entity._attrs_)
        entity._bits_ = {}
        next_offset = count().next
        for i, attr in enumerate(entity._attrs_):
            
            entity._bits_[attr] = 1 << i

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
                    'Interrelated entities must belong to same diagram. Entities %s and %s belongs to different diagrams'
                    % (entity.__name__, entity2.__name__))
            else: continue
            
            reverse = attr.reverse
            if isinstance(reverse, basestring):
                attr2 = getattr(entity2, reverse, None)
                if attr2 is None: raise DiagramError('Reverse attribute %s.%s not found' % (entity2.__name__, reverse))
            elif isinstance(reverse, Attribute):
                attr2 = reverse
                if attr2.entity is not entity2: raise DiagramError('Incorrect reverse attribute %s used in %s' % (attr2, attr))
            elif reverse is not None: raise DiagramError("Value of 'reverse' option must be string. Got: %r" % type(reverse))
            else:
                candidates1 = []
                candidates2 = []
                for attr2 in entity2._new_attrs_:
                    if attr2.py_type not in (entity, entity.__name__): continue
                    reverse2 = attr2.reverse
                    if reverse2 in (attr, attr.name): candidates1.append(attr2)
                    elif not reverse2: candidates2.append(attr2)
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
    def __init__(obj, *args, **keyargs):
        raise TypeError('You cannot create entity instances directly. Use Entity.create(...) or Entity.find(...) instead')
    def __repr__(obj):
        pkval = obj._pkval_
        if pkval is None: key_str = 'new:%d' % obj._newid_
        else: key_str = ', '.join(repr(val) for val in pkval)
        return '%s(%s)' % (obj.__class__.__name__, key_str)
    @classmethod
    def find(entity, *args, **keyargs):
        pk_attrs = entity._keys_[0]
        if args:
            if len(args) != len(pk_attrs):
                raise CreateError('Invalid count of attrs in primary key')
            for attr, val in zip(pk_attrs, args):
                if keyargs.setdefault(attr.name, val) != val:
                    raise CreateError('Ambiguous attribute value for %r' % attr.name)
        for name in ifilterfalse(entity._attr_dict_.__contains__, keyargs):
            raise CreateError('Unknown attribute %r' % name)

        info = entity._get_info()
        trans = local.transaction

        get_new_offset = entity._new_offsets_.__getitem__
        get_old_offset = entity._old_offsets_.__getitem__
        data = entity._data_template_[:]
        used_attrs = []
        for attr in entity._attrs_:
            val = keyargs.get(attr.name, UNKNOWN)
            data[get_old_offset(attr)] = None
            if val is not UNKNOWN:
                val = attr.check(val, None, entity)
                used_attrs.append((attr, val))
            data[get_new_offset(attr)] = val

        for key in entity._keys_:
            key_value = tuple(map(data.__getitem__, map(get_new_offset, key)))
            if None in key_value: continue
            try: old_index, new_index = trans.indexes[key]
            except KeyError: continue
            obj2 = new_index.get(key_value)
            if obj2 is None: continue
            obj2_data = trans.objects[obj2]
            obj2_get_new_offset = obj2._new_offsets_.__getitem__
            try:
                for attr in used_attrs:
                    val = data[get_new_offset(attr)]
                    val2 = obj2_data[obj2_get_new_offset(attr)]
                    if val2 is UNKNOWN: raise NotImplementedError
                    if val != val2: return None
            except KeyError: return None
            return obj2
        
        tables = {}
        select_list = []
        from_list = []
        where_list = []
        table_counter = count(1)
        column_counter = count(1)
        for attr, val in used_attrs:
            pass

        raise NotImplementedError
    @classmethod
    def create(entity, *args, **keyargs):
        pk_attrs = entity._keys_[0]
        if args:
            if len(args) != len(pk_attrs):
                raise CreateError('Invalid count of attrs in primary key')
            for attr, val in zip(pk_attrs, args):
                if keyargs.setdefault(attr.name, val) != val:
                    raise CreateError('Ambiguous attribute value for %r' % attr.name)
        for name in ifilterfalse(entity._attr_dict_.__contains__, keyargs):
            raise CreateError('Unknown attribute %r' % name)

        info = entity._get_info()
        trans = local.transaction

        get_new_offset = entity._new_offsets_.__getitem__
        get_old_offset = entity._old_offsets_.__getitem__
        data = entity._data_template_[:]
        for attr in entity._attrs_:
            val = keyargs.get(attr.name, UNKNOWN)
            data[get_old_offset(attr)] = None
            data[get_new_offset(attr)] = attr.check(val, None, entity)
        pkval = tuple(map(data.__getitem__, map(get_new_offset, pk_attrs)))
        if None in pkval:
            obj = object.__new__(entity)
            obj._pkval_ = None
            obj._newid_ = new_instance_next_id()
        else:
            obj = object.__new__(entity)
            obj._pkval_ = pkval
            obj._newid_ = None
            entity._lock_.acquire()
            try: obj = entity._objects_.setdefault(pkval, obj)
            finally: entity._lock_.release()
            if obj in trans.objects:
                key_str = ', '.join(repr(item) for item in pkval)
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
                    raise CreateError('%s with such unique index already exists: %s' % (obj2.__class__.__name__, key_str))
            for attr in entity._attrs_:
                if attr.reverse is None: continue
                val = data[get_new_offset(attr)]
                if val is None: continue
                attr.update_reverse(obj, None, val, undo_funcs)
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
        return obj
    def set(obj, **keyargs):
        pkval = obj._pkval_
        info = obj._get_info()
        trans = local.transaction
        get_new_offset = obj._new_offsets_.__getitem__
        get_old_offset = obj._old_offsets_.__getitem__

        data = trans.objects.get(obj) or obj._get_data('U')
        old_data = data[:]

        attrs = set()
        for name, val in keyargs.items():
            attr = obj._attr_dict_.get(name)
            if attr is None: raise UpdateError("Unknown attribute: %r" % name)
            val = attr.check(val, obj)
            if data[get_new_offset(attr)] == val: continue
            if attr.pk_offset is not None: raise UpdateError('Cannot change value of primary key')
            attrs.add(attr)
            data[get_new_offset(attr)] = val
        if not attrs: return

        undo = []
        undo_funcs = []
        try:
            for key in obj._keys_[1:]:
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
                val = data[offset]
                attr.update_reverse(obj, prev, val, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
            for new_index, obj, old_key, new_key in undo:
                if new_key is not None: del new_index[new_key]
                if old_key is not None: new_index[old_key] = obj
            data[:] = old_data
            raise
        if data[1] != 'C': data[1] = 'U'
