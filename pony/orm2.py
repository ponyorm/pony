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
    __slots__ = 'is_required', 'is_unique', 'is_indexed', 'is_collection', \
                'id', 'bit', 'pk_offset', 'type', 'entity', 'name', 'oldname', \
                'args', 'auto', 'default', 'reverse', 'composite_keydefs'
    def __init__(attr, type, *args, **keyargs):
        if attr.__class__ is Attribute: raise TypeError("'Atrribute' is abstract type")
        attr.is_required = isinstance(attr, Required)
        attr.is_unique = isinstance(attr, Unique)  # Also can be set to True later
        attr.is_indexed = attr.is_unique  # Also can be set to True later
        attr.is_collection = isinstance(attr, Collection)
        attr.id = next_id()
        attr.bit = 0
        attr.pk_offset = None
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
        if attr.reverse is None: pass
        elif not isinstance(attr.reverse, (basestring, Attribute)):
            raise TypeError("Value of 'reverse' option must be name of "
                            "reverse attribute). Got: %r" % attr.reverse)
        elif not isinstance(attr.type, (basestring, EntityMeta)):
            raise DiagramError('Reverse option cannot be set for this type %r' % attr.type)
        for option in keyargs: raise TypeError('Unknown option %r' % option)
        attr.composite_keydefs = []
    def _init_(attr, entity, name):
        attr.entity = entity
        attr.name = name
        attr.oldname = intern('__old_' + name)
    def __str__(attr):
        owner_name = attr.entity is None and '?' or attr.entity.__name__
        return '%s.%s' % (owner_name, attr.name or '?')
    def __repr__(attr):
        return '<Attribute %s: %s>' % (attr, attr.__class__.__name__)
    def check(attr, obj, val):
        assert val is not UNKNOWN
        if val is DEFAULT:
            val = attr.default
            if val is None and attr.is_required and not attr.auto: raise ConstraintError(
                'Required attribute %s.%s does not specified' % (obj.__class__.__name__, attr.name))
        if val is not None: pass
        elif attr.is_required: raise ConstraintError(
            'Required attribute %s.%s cannot be set to None' % (obj.__class__.__name__, attr.name))
        else: return val
        reverse = attr.reverse
        if reverse is None: return val
        if obj._trans_ is not val._trans_: raise TransactionError('An attempt to mix objects belongs to different transactions')
        if isinstance(val, reverse.entity): return val
        if obj is not None: entity = obj.__class__
        else: entity = attr.entity
        raise ConstraintError('Value of attribute %s.%s must be an instance of %s. Got: %s'
                              % (obj.__class__.__name__, attr.name, reverse.entity.__name__, val))
    def __get__(attr, obj, cls=None):
        if obj is None: return attr
        return attr.get(obj, True)
    def __set__(attr, obj, val, is_reverse=False):
        val = attr.check(obj, val)
        prev = attr.get(obj)
        if prev == val: obj._rbits_ |= attr.bit; return
        is_indexed = attr.is_indexed
        reverse = attr.reverse
        if reverse is None:
            if is_indexed: attr.check_indexes(obj, val)
            attr.set(obj, val)
            if is_indexed: attr.update_indexes(obj, val)
        else:
            if is_indexed: attr.check_indexes(obj, val)
            if val is not None and reverse.is_indexed: reverse.check_indexes(val, obj)
            attr.update_reverse(obj, val, is_reverse)
            attr.set(obj, val)
            if is_indexed: attr.update_indexes(obj, val)
        if obj._status_ != 'created': obj._status_ = 'updated'
    def __delete__(attr, obj):
        raise NotImplementedError
    def get(attr, obj, setbit=False):
        if setbit: obj._rbits_ |= attr.bit
        val = obj.__dict__.get(attr.name, UNKNOWN)
        if val is UNKNOWN: raise NotImplementedError
    def set(attr, obj, val, setbit=True):
        if setbit: obj._wbits_ |= attr.bit
        obj.__dict__[attr.name] = val
    def update_reverse(attr, obj, val, is_reverse):
        reverse = attr.reverse
        prev = obj.__dict__[attr.name]
        if not reverse.is_collection:
            if prev is not None: reverse.__set__(prev, None, True)
            if not is_reverse and val is not None: reverse.__set__(val, obj, True)
        elif isinstance(reverse, Set):
            if prev is not None: reverse.get(prev).remove(obj)
            if not is_reverse and val is not None: reverse.get(val).add(obj)
        else: raise NotImplementedError
    def check_indexes(attr, obj, val):
        trans = obj._trans_
        if attr.is_unique:
            index = trans.simple_indexes.get(attr)
            if index is not None:
                obj2 = index.get(val)
                if obj2 is not None: raise UpdateError(
                    'Cannot update %s.%s: %s with such unique index value already exists: %r'
                    % (obj.__class__.__name__, attr.name, obj2.__class__.__name__, val))
        for keydef, i in attr.composite_keydefs:
            old_key_ = obj.__dict__.get(keydef)
            new_key = list(old_key)
            new_key[i] = val
            new_key = tuple(new_key)
            if keydef.ignore_none and None in new_key: continue
            index = trans.composite_indexes.get(keydef)
            if index is None: continue
            obj2 = index.get(new_key)
            if obj2 is None: continue
            key_str = ', '.join(str(v) for v in new_key)
            raise UpdateError('Cannot update %s.%s: %s with such unique index value already exists: %r'
                              % (obj.__class__.__name__, attr.name, obj2.__class__.__name__, val))
    def update_indexes(attr, obj, val):
        trans = obj._trans_
        if attr.is_unique:
            index = trans.simple_indexes.get(attr)
            if index is None: index = trans.simple_indexes[attr] = {}
            obj2 = index.setdefault(val, obj)
            assert obj2 is obj
        for keydef, i in attr.composite_keydefs:
            old_key = obj.__dict__.get(keydef)
            new_key = list(old_key)
            new_key[i] = val
            new_key = tuple(new_key)
            obj.__dict__[keydef] = new_key
            if keydef.ignore_none and None in new_key: continue
            index = trans.composite_indexes.get(keydef)
            if index is None: index = trans.composite_indexes[keydef] = {}
            obj2 = index.setdefault(new_key, obj)
            assert obj2 is obj
            
class Optional(Attribute): pass
class Required(Attribute): pass

class Unique(Required):
    def __new__(cls, *args, **keyargs):
        if not args: raise TypeError('Invalid count of positional arguments')
        attrs = tuple(a for a in args if isinstance(a, Attribute))
        non_attrs = [ a for a in args if not isinstance(a, Attribute) ]
        if attrs and (non_attrs or keyargs): raise TypeError('Invalid arguments')
        cls_dict = sys._getframe(1).f_locals
        if not attrs:
            result = Required.__new__(cls, *args, **keyargs)
            return result
        else:
            keys = cls_dict.setdefault('_keys_', {})
            keys[attrs] = issubclass(cls, PrimaryKey)

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
    def check(attr, val, obj):
        assert val is not UNKNOWN
        if val is None or val is DEFAULT: return set()
        reverse = attr.reverse
        if reverse is None: raise NotImplementedError
        trans = obj._trans_
        if isinstance(val, reverse.entity): result = set((val,))
        else:
            rentity = reverse.entity
            try:
                robjects = set(val)  # may raise TypeError if val is not iterable
                for robj in robjects:
                    if not isinstance(robj, rentity): raise TypeError
            except TypeError: raise TypeError('Item of collection %s.%s must be instance of %s. Got: %r'
                                              % (obj.__class__.__name__, attr.name, rentity.__name__, robj))
        trans = obj._trans_
        for robj in result:
            if robj._trans_ is not trans: raise TransactionError('An attempt to mix objects belongs to different transactions')
        return result
    def __get__(attr, obj, type=None):
        if obj is None: return attr
        return SetProperty(obj, attr)
    def __set__(attr, obj, val):
        val = attr.check(val, obj.__class__)
        prev = attr.get(obj)
        if val == prev: return
        reverse = attr.reverse
        if reverse is None: raise NotImplementedError
        if not reverse.is_collection:
            for robj in val: reverse.check_indexes(robj, obj)
        attr.update_reverse(obj, val)
        attr.set(obj, val, False)
    def __delete__(attr, obj):
        raise NotImplementedError
    def update_reverse(attr, obj, val):
        prev = attr.get(obj)
        remove_set = prev and (val and prev-val or prev) or ()
        add_set = val and (prev and val-prev or val) or ()
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
        if reverse is None: raise NotImplementedError
        if not reverse.is_collection:
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
        if reverse is None: raise NotImplementedError
        if not reverse.is_collection:
            for robj in remove_set: reverse.__set__(robj, None, True)
        elif isinstance(reverse, Set):
            for robj in remove_set: reverse.get(robj).remove(obj)
        else: raise NotImplementedError
        val.difference_update(remove_set)
        return setprop
