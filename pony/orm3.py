from itertools import count, ifilter, ifilterfalse, izip
from operator import attrgetter

try: from pony.thirdparty import etree
except ImportError: etree = None

class OrmError(Exception): pass

class DiagramError(OrmError): pass
class SchemaError(OrmError): pass
class MappingError(OrmError): pass
class TransactionError(OrmError): pass
class IndexError(TransactionError): pass

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
    def __init__(attr, py_type, *args, **keyargs):
        if attr.__class__ is Attribute: raise TypeError("'Atrribute' is abstract type")
        attr.is_required = isinstance(attr, Required)
        attr.is_unique = isinstance(attr, Unique)  # Also can be set to True later
        attr.is_indexed = attr.is_unique  # Also can be set to True later
        attr.is_collection = isinstance(attr, Collection)
        attr.is_pk = isinstance(attr, PrimaryKey)
        if attr.is_pk: attr.pk_offset = 0
        else: attr.pk_offset = None
        attr.id = next_id()
        attr.py_type = py_type
        attr.entity = attr.name = None
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
        elif not isinstance(attr.py_type, (basestring, EntityMeta)):
            raise DiagramError('Reverse option cannot be set for this type %r' % attr.py_type)
        for option in keyargs: raise TypeError('Unknown option %r' % option)
        attr.composite_keys = []
    def _init_(attr, entity, name):
        attr.entity = entity
        attr.name = name
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
        rbits = obj._rbits_
        if rbits is not None: obj._rbits_ |= obj._bits_[attr]
        return attr.get(obj)
    def get(attr, obj):
        val = obj.__dict__.get(attr, UNKNOWN)
        if val is UNKNOWN: val = obj._load_(attr)
        return val
    def __set__(attr, obj, val, fromdb=False):
        val = attr.check(val, obj)
        try: attr.prepare(obj, val, fromdb)
        except IndexError:
            obj._trans_.revert()
            raise
        try: attr.set(obj, val, fromdb)
        except: assert False
    def __delete__(attr, obj):
        raise NotImplementedError
    def prepare(attr, obj, val, fromdb=False):
        raise NotImplementedError
    def set(attr, obj, val, fromdb=False):
        if obj._status_ != 'created': obj._status_ = 'updated'
        raise NotImplementedError
            
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
    def prepare(attr, obj, val, fromdb=False):
        assert False, 'Abstract method'
    def set(attr, obj, val, fromdb=False):
        assert False, 'Abstract method'

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
    def __delete__(attr, obj):
        raise NotImplementedError
    def prepare(attr, obj, val, fromdb=False):
        raise NotImplementedError
    def set(attr, obj, val, fromdb=False):
        if obj._status_ != 'created': obj._status_ = 'updated'
        raise NotImplementedError

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
        raise NotImplementedError
    def __isub__(setprop, x):
        raise NotImplementedError

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
        new_attrs.sort(key=attrgetter('id'))
        entity._new_attrs_ = new_attrs

        entity._attrs_ = base_attrs + new_attrs
        entity._adict_ = dict((attr.name, attr) for attr in entity._attrs_)
        entity._required_attrs_ = [ attr for attr in entity._attrs_ if attr.is_required ]
        entity._bits_ = {}
        next_offset = count().next
        for attr in entity._attrs_:
            if attr.is_collection or attr.pk_offset is not None: continue
            entity._bits_[attr] = 1 << next_offset()

        keys = entity.__dict__.get('_keys_', {})
        primary_keys = set(key for key, is_pk in keys.items() if is_pk)
        if direct_bases:
            if primary_keys: raise DiagramError('Primary key cannot be redefined in derived classes')
            for base in direct_bases:
                keys[base._pk_attrs_] = True
                for key in base._keys_: keys[key] = False
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
        for i, attr in enumerate(pk_attrs): attr.pk_offset = i
        entity._pk_attrs_ = pk_attrs
        entity._pk_names_ = tuple(attr.name for attr in pk_attrs)
        entity._pk_is_composite_ = len(pk_attrs) > 1
        entity._pk_ = len(pk_attrs) > 1 and pk_attrs or pk_attrs[0]
        entity._keys_ = [ key for key, is_pk in keys.items() if not is_pk ]
        entity._simple_keys_ = [ key[0] for key in entity._keys_ if len(key) == 1 ]
        entity._composite_keys_ = [ key for key in entity._keys_ if len(key) > 1 ]

        entity._diagram_ = diagram
        diagram.entities[entity.__name__] = entity
        entity._link_reverse_attrs_()

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
        raise NotImplementedError
    @classmethod
    def create(entity, *args, **keyargs):
        raise NotImplementedError
    def set(obj, **keyargs):
        avdict = obj._keyargs_to_avdict_(keyargs)
        for attr in ifilter(avdict.__contains__, obj._pk_attrs_):
            raise TypeError('Cannot change value of primary key attribute %s' % attr.name)
        raise NotImplementedError
    def _prepare_(obj, avdict, fromdb=False):
        raise NotImplementedError
    def _set_(obj, avdict, fromdb=False):
        raise NotImplementedError
    def _load_(obj, attr):
        raise NotImplementedError
    @classmethod
    def _normalize_args_(entity, args, keyargs, setdefault=False):
        pk_names = entity._pk_names_        
        if not args: pass
        elif len(args) != len(pk_names): raise TypeError('Invalid count of attrs in primary key')
        else:
            for name, val in izip(pk_names, args):
                if keyargs.setdefault(name, val) is not val:
                    raise TypeError('Ambiguos value of attribute %s' % name)
        avdict = {}
        if setdefault:
            for name in ifilterfalse(entity._adict_.__contains__, keyargs):
                raise TypeError('Unknown attribute %r' % name)
            for attr in entity._attrs_:
                val = keyargs.get(attr.name, DEFAULT)
                avdict[attr] = attr.check(val, None, entity)
        else:
            get = entity._adict_.get 
            for name, val in keyargs.items():
                attr = get(name)
                if attr is None: raise TypeError('Unknown attribute %r' % name)
                avdict[attr] = attr.check(val, None, entity)
        if entity._pk_is_composite_:
            pkval = map(avdict.get, entity._pk_attrs_)
            pkval = None not in pkval and tuple(pkval) or None
        else: pkval = avdict.get(entity._pk_)
        return pkval, avdict        
    def _keyargs_to_avdict_(obj, keyargs):
        avdict = {}
        get = entity._adict_.get
        for name, val in keyargs.items():
            attr = get(name)
            if attr is None: raise TypeError('Unknown attribute %r' % name)
            avdict[attr] = attr.check(val, obj)
        return avdict
