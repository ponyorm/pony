import sys, threading
from operator import attrgetter
from itertools import count, ifilter, ifilterfalse, izip

try: from pony.thirdparty import etree
except ImportError: etree = None

import dbapiprovider
from pony.sqlsymbols import *

class OrmError(Exception): pass

class DiagramError(OrmError): pass
class SchemaError(OrmError): pass
class MappingError(OrmError): pass
class ConstraintError(OrmError): pass
class TransactionError(OrmError): pass
class IndexError(TransactionError): pass
class MultipleObjectsFoundError(OrmError): pass

class NotLoadedValueType(object):
    def __repr__(self): return 'NOT_LOADED'

NOT_LOADED = NotLoadedValueType()

class DefaultValueType(object):
    def __repr__(self): return 'DEFAULT'

DEFAULT = DefaultValueType()

class NoUndoNeededType(object):
    def __repr__(self): return 'NO_UNDO_NEEDED'

NO_UNDO_NEEDED = NoUndoNeededType()

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
        if py_type == 'Entity' or py_type is Entity:
            raise TypeError('Cannot link attribute to Entity class. Must use Entity subclass instead')
        attr.py_type = py_type
        attr.sql_type = keyargs.pop('sql_type', None)
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
        attr.column = keyargs.pop('column', None)
        attr.columns = keyargs.pop('columns', None)
        for option in keyargs: raise TypeError('Unknown option %r' % option)
        attr.composite_keys = []
    def _init_(attr, entity, name):
        attr.entity = entity
        attr.name = name
        if attr.column is not None:
            if attr.columns is not None:
                raise TypeError("Parameters 'column' and 'columns' cannot be specified simultaneously")
            if not isinstance(attr.column, basestring):
                raise TypeError("Parameter 'column' must be a string. Got: %r" % attr.column)
            attr.columns = [ attr.column ]
        elif attr.columns is not None:
            if not isinstance(attr.columns, (tuple, list)):
                raise TypeError("Parameter 'columns' must be a list. Got: %r'" % attr.columns)
            if not attr.columns: raise TypeError("Parameter 'columns' must not be empty list")
            for column in attr.columns:
                if not isinstance(column, basestring):
                    raise TypeError("Items of parameter 'columns' must be strings. Got: %r" % column)
    def __repr__(attr):
        owner_name = not attr.entity and '?' or attr.entity.__name__
        return '%s.%s' % (owner_name, attr.name or '?')
    def check(attr, val, obj=None, entity=None):
        assert val is not NOT_LOADED
        if entity is not None: pass
        elif obj is not None: entity = obj.__class__
        else: entity = attr.entity
        if val is DEFAULT:
            val = attr.default
            if val is None and attr.is_required and not attr.auto: raise ConstraintError(
                'Required attribute %s.%s does not specified' % (entity.__name__, attr.name))
        elif val is None:
            if attr.is_required:
                if obj is None: raise ConstraintError(
                    'Required attribute %s.%s cannot be set to None' % (entity.__name__, attr.name))
                else: raise ConstraintError(
                    'Required attribute %s.%s for %r cannot be set to None' % (entity.__name__, attr.name, obj))
            return val
        if val is None: return val
        reverse = attr.reverse
        if not reverse:
            if isinstance(val, attr.py_type): return val
            elif isinstance(val, Entity): raise TypeError(
                'Attribute %s.%s must be of %s type. Got: %s'
                % (attr.entity.__name__, attr.name, attr.py_type.__name__, val))
            return attr.py_type(val)
        if not isinstance(val, reverse.entity): raise ConstraintError(
            'Value of attribute %s.%s must be an instance of %s. Got: %s' % (entity.__name__, attr.name, reverse.entity.__name__, val))
        if obj is not None: trans = obj._trans_
        else: trans = get_trans()
        if trans is not val._trans_: raise TransactionError('An attempt to mix objects belongs to different transactions')
        return val
    def load(attr, obj):
        raise NotImplementedError
    def __get__(attr, obj, cls=None):
        if obj is None: return attr
        rbits = obj._rbits_
        if rbits is not None: obj._rbits_ |= obj._bits_[attr]
        return attr.get(obj)
    def get(attr, obj):
        val = obj.__dict__.get(attr, NOT_LOADED)
        if val is NOT_LOADED: val = attr.load(obj)
        return val
    def __set__(attr, obj, val, undo_funcs=None):
        is_reverse_call = undo_funcs is not None
        reverse = attr.reverse
        val = attr.check(val, obj)
        pkval = obj._pkval_
        if attr.pk_offset is not None:
            if pkval is not None and val == pkval[attr.pk_offset]: return
            raise TypeError('Cannot change value of primary key')
        prev =  obj.__dict__.get(attr, NOT_LOADED)
        if prev is NOT_LOADED and reverse and not reverse.is_collection:
            assert not is_reverse_call
            prev = attr.load(obj)
        trans = obj._trans_
        status = obj._status_
        wbits = obj._wbits_
        if wbits is not None:
            obj._status_ = 'updated'
            obj._wbits_ = wbits | obj._bits_[attr]
            trans.updated.add(obj)
        if not attr.reverse and not attr.is_indexed:
            obj.__dict__[attr] = val
            return
        if prev == val:
            assert not is_reverse_call
            return
        if not is_reverse_call: undo_funcs = []
        undo = []
        def undo_func():
            obj.status = status
            obj.wbits = wbits
            if wbits == 0: trans.updated.remove(obj)
            obj.__dict__[attr] = prev
            for index, old_key, new_key in undo:
                if new_key is NO_UNDO_NEEDED: pass
                else: del index[new_key]
                if old_key is NO_UNDO_NEEDED: pass
                else: index[old_key] = obj
        undo_funcs.append(undo_func)
        try:
            if attr.is_unique:
                trans.update_simple_index(obj, attr, prev, val, undo)
            for attrs, i in attr.composite_keys:
                get = obj.__dict__.get
                vals = [ get(a, NOT_LOADED) for a in attrs ]
                prevs = tuple(vals)
                vals[i] = val
                vals = tuple(vals)
                trans.update_composite_index(obj, attrs, prevs, vals, undo)

            obj.__dict__[attr] = val
                
            if not reverse: pass
            elif not is_reverse_call: attr.update_reverse(obj, prev, val, undo_funcs)
            elif prev is not None:
                if not reverse.is_collection:
                    assert prev is not NOT_LOADED
                    reverse.__set__(prev, None, undo_funcs)
                elif isinstance(reverse, Set):
                    if prev is NOT_LOADED: pass
                    else: reverse.reverse_remove((prev,), obj, undo_funcs)
                else: raise NotImplementedError
        except:
            if not is_reverse_call:
                for undo_func in reversed(undo_funcs): undo_func()
            raise
    def update_reverse(attr, obj, prev, val, undo_funcs):
        reverse = attr.reverse
        if not reverse.is_collection:
            assert prev is not NOT_LOADED
            if prev is not None: reverse.__set__(prev, None, undo_funcs)
            if val is not None: reverse.__set__(val, obj, undo_funcs)
        elif isinstance(reverse, Set):
            if prev is NOT_LOADED: pass
            elif prev is not None: reverse.reverse_remove((prev,), obj, undo_funcs)
            if val is not None: reverse.reverse_add((val,), obj, undo_funcs)
        else: raise NotImplementedError
    def __delete__(attr, obj):
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
            if attr.is_required: raise TypeError('Invalid declaration')
            attr.is_unique = True
        else:
            for i, attr in enumerate(attrs): attr.composite_keys.append((attrs, i))
        keys[attrs] = is_pk
        return None

class PrimaryKey(Unique): pass

class Collection(Attribute):
    __slots__ = 'table'
    def __init__(attr, py_type, *args, **keyargs):
        if attr.__class__ is Collection: raise TypeError("'Collection' is abstract type")
        table = keyargs.pop('table', None)
        if table is not None and not isinstance(table, basestring):
            raise TypeError("Parameter 'table' must be a string. Got: %r" % table)
        attr.table = table
        Attribute.__init__(attr, py_type, *args, **keyargs)
        if attr.default is not None: raise TypeError('default value could not be set for collection attribute')
        if attr.auto: raise TypeError("'auto' option could not be set for collection attribute")
    def load(attr, obj):
        assert False, 'Abstract method'
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

class _set(dict):
    __slots__ = 'fully_loaded'

class Set(Collection):
    def check(attr, val, obj=None, entity=None):
        assert val is not NOT_LOADED
        if val is None or val is DEFAULT: return set()
        if entity is not None: pass
        elif obj is not None: entity = obj.__class__
        else: entity = attr.entity
        reverse = attr.reverse
        if not reverse: raise NotImplementedError
        if isinstance(val, reverse.entity): result = (val,)
        else:
            rentity = reverse.entity
            if isinstance(val, set): result = val
            else:
                try: result = set(val)
                except TypeError: raise TypeError(
                    'Item of collection %s.%s must be an instance of %s. Got: %r'
                    % (entity.__name__, attr.name, rentity.__name__, val))
            for robj in result:
                if not isinstance(robj, rentity): raise TypeError(
                    'Item of collection %s.%s must be an instance of %s. Got: %r'
                    % (entity.__name__, attr.name, rentity.__name__, robj))
        if obj is not None: trans = obj._trans_
        else: trans = get_trans()
        for robj in result:
            if robj._trans_ is not trans: raise TransactionError('An attempt to mix objects belongs to different transactions')
        return result
    def load(attr, obj):
        if obj._status_ == 'created':
            val = obj.__dict__[attr] = _set()
            val.fully_loaded = True
            return val
        elif not val.fully_loaded: raise NotImplementedError
    def copy(attr, obj):
        val = obj.__dict__.get(attr, NOT_LOADED)
        if val is NOT_LOADED or not val.fully_loaded: val = attr.load(obj)
        return set(x for x, status in val.iteritems() if status != 'deleted')
    def __get__(attr, obj, type=None):
        if obj is None: return attr
        return SetWrapper(obj, attr)
    def __set__(attr, obj, val, undo_funcs=None):
        val = attr.check(val, obj)
        reverse = attr.reverse
        if not reverse: raise NotImplementedError
        prev =  obj.__dict__.get(attr, NOT_LOADED)
        if prev is NOT_LOADED or not prev.fully_loaded: prev = attr.load(obj)
        to_add = set(ifilterfalse(prev.__contains__, val))
        to_delete = set()
        for robj, status in prev.iteritems():
            if robj in val:
                if status != 'added': to_add.add(robj)
            else:
                if status != 'deleted': to_delete.add(robj)
        if undo_funcs is None: undo_funcs = []
        try:
            if not reverse.is_collection:
                for robj in to_delete: reverse.__set__(robj, None, undo_funcs)
                for robj in to_add: reverse.__set__(robj, obj, undo_funcs)
            else:
                reverse.reverse_remove(to_delete, obj, undo_funcs)
                reverse.reverse_add(to_add, obj, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
            raise
        for robj in to_delete: prev[robj] = 'deleted'
        for robj in to_add: prev[robj] = 'added'
        trans = obj._trans_
        trans.removed.setdefault(attr, {}).setdefault(obj, set()).update(to_delete)
        trans.added.setdefault(attr, {}).setdefault(obj, set()).update(to_add)
    def __delete__(attr, obj):
        raise NotImplementedError
    def reverse_add(attr, objects, robj, undo_funcs):
        trans = robj._trans_
        for obj in objects:
            val = obj.__dict__.get(attr, NOT_LOADED)
            if val is NOT_LOADED:
                val = obj.__dict__[attr] = _set()
                val.fully_loaded = False
            val[robj] = 'added'
            trans.added.setdefault(attr, {}).setdefault(obj, set()).add(robj)
    def reverse_remove(attr, objects, robj, undo_funcs):
        trans = robj._trans_
        for obj in objects:
            val = obj.__dict__.get(attr, NOT_LOADED)
            if val is NOT_LOADED:
                val = obj.__dict__[attr] = _set()
                val.fully_loaded = False
            val[robj] = 'deleted'
            trans.removed.setdefault(attr, {}).setdefault(obj, set()).add(robj)

##class List(Collection): pass
##class Dict(Collection): pass
##class Relation(Collection): pass

class SetWrapper(object):
    __slots__ = '_obj_', '_attr_'
    def __init__(wrapper, obj, attr):
        wrapper._obj_ = obj
        wrapper._attr_ = attr
    def copy(wrapper):
        return wrapper._attr_.copy(wrapper._obj_)
    def __repr__(wrapper):
        val = wrapper.copy()
        return '%r.%s->%r' % (wrapper._obj_, wrapper._attr_.name, val)
    def __str__(wrapper):
        return str(wrapper.copy())
    def __len__(wrapper):
        return len(wrapper.copy())
    def __iter__(wrapper):
        return iter(wrapper.copy())
    def __eq__(wrapper, x):
        if isinstance(x, SetWrapper):
            if wrapper._obj_ is x._obj_ and _attr_ is x._attr_: return True
            else: x = x.copy()
        elif not isinstance(x, set): x = set(x)
        val = wrapper.copy()
        return val == x
    def __ne__(wrapper, x):
        return not wrapper.__eq__(x)
    def __add__(wrapper, x):
        return wrapper.copy().union(x)
    def __sub__(wrapper, x):
        return wrapper.copy().difference(x)
    def __contains__(wrapper, x):
        obj = wrapper._obj_
        attr = wrapper._attr_
        val = obj.__dict__.get(attr, NOT_LOADED)
        if val is not NOT_LOADED:
            status = val.get(x)
            if status is None: pass
            elif status == 'deleted': return False
            else: return True
        val = attr.load(obj)
        return val.get(x, 'deleted') != 'deleted'
    def add(wrapper, x):
        obj = wrapper._obj_
        attr = wrapper._attr_
        val = obj.__dict__.get(attr, NOT_LOADED)
        if val is NOT_LOADED:
            val = obj.__dict__[attr] = _set()
            val.fully_loaded = False
        x = attr.check(x)
        for y in x: val[y] = 'added'
    def __iadd__(wrapper, x):
        wrapper.add(x)
        return wrapper
    def remove(wrapper, x):
        obj = wrapper._obj_
        attr = wrapper._attr_
        val = obj.__dict__.get(attr, NOT_LOADED)
        if val is NOT_LOADED:
            val = obj.__dict__[attr] = _set()
            val.fully_loaded = False
        x = attr.check(x)
        for y in x: val[y] = 'deleted'
    def __isub__(wrapper, x):
        wrapper.remove(x)
        return wrapper

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

        diagram = dict.pop('_diagram_', None) or outer_dict.get('_diagram_')
        if diagram is None:
            diagram = Diagram()
            outer_dict['_diagram_'] = diagram

        entity._cls_init_(diagram)
    def __setattr__(entity, name, val):
        entity._cls_setattr_(name, val)
    def __iter__(entity):
        return iter(())

new_instance_next_id = count(1).next

class Entity(object):
    __metaclass__ = EntityMeta
    __slots__ = '__dict__', '__weakref__', '_pkval_', '_reduced_pkval_', '_newid_', '_trans_', '_status_', '_rbits_', '_wbits_'
    @classmethod
    def _cls_setattr_(entity, name, val):
        if name.startswith('_') and name.endswith('_'):
            type.__setattr__(entity, name, val)
        else: raise NotImplementedError
    @classmethod
    def _cls_init_(entity, diagram):
        if entity.__name__ in diagram.entities:
            raise DiagramError('Entity %s already exists' % entity.__name__)
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
            new_attrs.insert(0, attr)
            key, is_pk = _keys_.popitem()
            keys[key] = True
            pk_attrs = key
        else: pk_attrs = primary_keys.pop()
        for i, attr in enumerate(pk_attrs): attr.pk_offset = i
        entity._pk_info_ = None
        entity._pk_attrs_ = pk_attrs
        entity._pk_is_composite_ = len(pk_attrs) > 1
        entity._pk_ = len(pk_attrs) > 1 and pk_attrs or pk_attrs[0]
        entity._keys_ = [ key for key, is_pk in keys.items() if not is_pk ]
        entity._simple_keys_ = [ key[0] for key in entity._keys_ if len(key) == 1 ]
        entity._composite_keys_ = [ key for key in entity._keys_ if len(key) > 1 ]

        entity._new_attrs_ = new_attrs
        entity._attrs_ = base_attrs + new_attrs
        entity._adict_ = dict((attr.name, attr) for attr in entity._attrs_)
        entity._required_attrs_ = [ attr for attr in entity._attrs_ if attr.is_required ]
        entity._bits_ = {}
        next_offset = count().next
        for attr in entity._attrs_:
            if attr.is_collection or attr.pk_offset is not None: continue
            entity._bits_[attr] = 1 << next_offset()

        entity._diagram_ = diagram
        diagram.entities[entity.__name__] = entity
        entity._link_reverse_attrs_()
    @classmethod
    def _link_reverse_attrs_(entity):
        diagram = entity._diagram_
        unmapped_attrs = diagram.unmapped_attrs.pop(entity.__name__, set())
        for attr in entity._new_attrs_:
            py_type = attr.py_type
            if isinstance(py_type, basestring):
                entity2 = diagram.entities.get(py_type)
                if entity2 is None:
                    diagram.unmapped_attrs.setdefault(py_type, set()).add(attr)
                    continue
                attr.py_type = py_type = entity2
            elif not issubclass(py_type, Entity): continue

            entity2 = py_type
            if entity2._diagram_ is not diagram: raise DiagramError(
                'Interrelated entities must belong to same diagram. Entities %s and %s belongs to different diagrams'
                % (entity.__name__, entity2.__name__))
            
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
            unmapped_attrs.discard(attr2)          
        for attr in unmapped_attrs:
            raise DiagramError('Reverse attribute for %s.%s was not found' % (attr.entity.__name__, attr.name))
    def __init__(obj, *args, **keyargs):
        raise TypeError('Cannot create entity instances directly. Use Entity.create(...) or Entity.find(...) instead')
    @classmethod
    def _calculate_pk_info_(entity):
        if entity._pk_info_ is not None: return entity._pk_info_
        pk_info = []
        for attr in entity._pk_attrs_:
            py_type = attr.py_type
            assert not isinstance(py_type, basestring)
            if not issubclass(py_type, Entity): pk_info.append((attr, attr.name))
            else:
                for attr2, name in attr.py_type._calculate_pk_info_():
                    pk_info.append((attr2, attr.name + '_' + name))
        entity._pk_info_ = pk_info
        return pk_info
    def _calculate_reduced_pkval_(obj):
        if hasattr(obj, '_reduced_pkval_'): return obj._reduced_pkval_
        if len(obj._pk_attrs_) == 1:
              pk_pairs = [ (obj.__class__._pk_, obj._pkval_) ]
        else: pk_pairs = zip(obj._pk_attrs_, obj._pkval_)
        reduced_pkval = []
        for attr, val in pk_pairs:
            if not issubclass(attr.py_type, Entity): reduced_pkval.append(val)
            elif len(attr.py_type._pk_attrs_) == 1: reduced_pkval.append(val._reduced_pkval_)
            else: reduced_pkval.extend(val._reduced_pkval_)
        if len(reduced_pkval) > 1: obj._reduced_pkval_ = tuple(reduced_pkval)
        else: obj._reduced_pkval_ = reduced_pkval[0]
    def __repr__(obj):
        pkval = obj._pkval_
        if pkval is None: return '%s(new:%d)' % (obj.__class__.__name__, obj._newid_)
        elif obj._pk_is_composite_: return '%s%r' % (obj.__class__.__name__, pkval)
        else: return '%s(%r)' % (obj.__class__.__name__, pkval)
    @classmethod
    def find(entity, *args, **keyargs):
        pkval, avdict = entity._normalize_args_(args, keyargs, False)
        for attr in avdict:
            if attr.is_collection: raise TypeError(
                'Collection attribute %s.%s cannot be specified as search criteria' % (attr.entity.__name__, attr.name))
        trans = get_trans()
        obj = None
        if pkval is not None:
            index = trans.indexes.get(entity._pk_)
            if index is not None: obj = index.get(pkval)
        if obj is None:
            for attr in ifilter(avdict.__contains__, entity._simple_keys_):
                index = trans.indexes.get(attr)
                if index is None: continue
                val = avdict[attr]
                obj = index.get(val)
                if obj is not None: break
        if obj is None:
            NOT_FOUND = object()
            for attrs in entity._composite_keys_:
                vals = tuple(avdict.get(attr, NOT_FOUND) for attr in attrs)
                if NOT_FOUND in vals: continue
                index = trans.indexes.get(attrs)
                if index is None: continue
                obj = index.get(vals)
                if obj is not None: break
        if obj is None:
            for attr, val in avdict.iteritems():
                reverse = attr.reverse
                if reverse and not reverse.is_collection:
                    obj = reverse.__get__(val)
                    break
        if obj is None:
            for attr, val in avdict.iteritems():
                if isinstance(val, Entity) and val._reduced_pkval_ is None:
                    reverse = attr.reverse
                    if not reverse.is_collection:
                        obj = reverse.__get__(val)
                        if obj is None: return None
                    elif isinstance(reverse, Set):
                        objects = reverse.__get__(val).copy()
                        if not objects: return None
                        if len(objects) == 1: obj = objects[0]
                        else:
                            filtered_objects = []
                            for obj in objects:
                                for attr, val in avdict.iteritems():
                                    if val != attr.get(obj): break
                                else: filtered_objects.append(obj)
                            if not filtered_objects: return None
                            elif len(filtered_objects) == 1: return filtered_objects[0]
                            else: raise MultipleObjectsFoundError(
                                'Multiple objects was found. Use %s.find_all(...) instead of %s.find(...) to retrieve them'
                                % (entity.__name__, entity.__name__))
                    else: raise NotImplementedError
        if obj is not None:
            for attr, val in avdict.iteritems():
                if val != attr.__get__(obj): return None
            return obj

        table_name = entity._table_

        select_list = [ ALL ]
        for attr in entity._attrs_:
            if attr.is_collection: continue
            for column in attr.columns:
                select_list.append([ COLUMN, 'T1', column ])
        from_list = [ FROM, [ 'T1', TABLE, table_name ]]

        where_list = [ WHERE ]
        criteria_list = [ AND ]
        params = {}
        for attr, val in avdict.iteritems():
            if issubclass(attr.py_type, Entity): val = val._reduced_pkval_
            if attr.column is not None: pairs = [ (attr.column, val) ]
            else:
                assert len(attr.columns) == len(val)
                pairs = zip(attr.columns, val)
            for column, val in pairs:
                param_id = len(params) + 1
                params[param_id] = val
                criteria_list.append([EQ, [COLUMN, 'T1', column], [ PARAM, param_id ] ])
        if len(criteria_list) == 1: where_list.append(criteria_list[0])
        else: where_list.append([ AND, criteria_list ])

        sql_ast = [ SELECT, select_list, from_list, where_list ]
        builder = dbapiprovider.SQLBuilder(sql_ast)
        print builder.sql
        print [ params[i] for i in builder.params ]

    @classmethod
    def create(entity, *args, **keyargs):
        pkval, avdict = entity._normalize_args_(args, keyargs, True)
        trans = get_trans()
        obj = object.__new__(entity)
        obj._trans_ = trans
        obj._status_ = 'created'
        obj._pkval_ = pkval
        if pkval is None:
            obj._newid_ = new_instance_next_id()
            obj._reduced_pkval_ = None
        else:
            obj._newid_ = None
            if pkval in trans.indexes.setdefault(entity._pk_, {}):
                if entity._pk_is_composite_: pkval = ', '.join(str(item) for item in pkval)
                raise IndexError('Cannot create %s: instance with primary key %s already exists'
                                 % (obj.__class__.__name__, pkval))
            obj._calculate_reduced_pkval_()
        obj._rbits_ = obj._wbits_ = None
        indexes = {}
        for attr in entity._simple_keys_:
            val = avdict[attr]
            if val in trans.indexes.setdefault(attr, {}): raise IndexError(
                'Cannot create %s: value %s for key %s already exists'
                % (obj.__class__.__name__, val, attr.name))
            indexes[attr] = val
        for attrs in entity._composite_keys_:
            vals = tuple(map(avdict.__getitem__, attrs))
            if vals in trans.indexes.setdefault(attrs, {}):
                attr_names = ', '.join(attr.name for attr in attrs)
                raise IndexError('Cannot create %s: value %s for composite key (%s) already exists'
                                 % (obj.__class__.__name__, vals, attr_names))
            indexes[attrs] = vals
        undo_funcs = []
        try:
            for attr, val in avdict.iteritems():
                if not attr.is_collection:
                    obj.__dict__[attr] = val
                    if attr.reverse: attr.update_reverse(obj, None, val, undo_funcs)
                else: attr.__set__(obj, val, undo_funcs)
        except:
            for undo_func in undo_funcs: undo_func()
            raise
        if pkval is not None:
            trans.indexes[entity._pk_][pkval] = obj
        for key, vals in indexes.iteritems():
            trans.indexes[key][vals] = obj
        trans.created.add(obj)
        return obj
    def set(obj, **keyargs):
        avdict, collection_avdict = obj._keyargs_to_avdicts_(keyargs)
        trans = obj._trans_
        status = obj._status_
        wbits = obj._wbits_
        if avdict:
            for attr in avdict:
                prev = obj.__dict__.get(attr, NOT_LOADED)
                if prev is NOT_LOADED and attr.reverse and not attr.reverse.is_collection:
                    attr.load(obj)
            if wbits is not None:
                obj._status_ = 'updated'
                new_wbits = wbits
                for attr in avdict: new_wbits |= obj._bits_[attr]
                obj._wbits_ = new_wbits
                trans.updated.add(obj)
            if not collection_avdict:
                for attr in avdict:
                    if attr.reverse or attr.is_indexed: break
                else:
                    obj.__dict__.update(avdict)
                    return
        undo_funcs = []
        undo = []
        def undo_func():
            obj.status = status
            obj.wbits = wbits
            if wbits == 0: trans.updated.discard(obj)
            for index, old_key, new_key in undo:
                if new_key is NO_UNDO_NEEDED: pass
                else: del index[new_key]
                if old_key is NO_UNDO_NEEDED: pass
                else: index[old_key] = obj
        NOT_FOUND = object()
        try:
            for attr in obj._simple_keys_:
                val = avdict.get(attr, NOT_FOUND)
                if val is NOT_FOUND: continue
                prev = obj.__dict__.get(attr, NOT_LOADED)
                if prev == val: continue
                trans.update_simple_index(obj, attr, prev, val, undo)
            for attrs in obj._composite_keys_:
                for attr in attrs:
                    if attr in avdict: break
                else: continue
                get = obj.__dict__.get
                vals = [ get(a, NOT_LOADED) for a in attrs ]
                prevs = tuple(vals)
                for i, attr in enumerate(attrs):
                    val = avdict.get(attr, NOT_FOUND)
                    if val is NOT_FOUND: continue
                    vals[i] = val
                vals = tuple(vals)
                trans.update_composite_index(obj, attrs, prevs, vals, undo)
            for attr, val in avdict.iteritems():
                if not attr.reverse: continue
                prev = obj.__dict__.get(attr, NOT_LOADED)
                attr.update_reverse(obj, prev, val, undo_funcs)
            for attr, val in collection_avdict.iteritems():
                attr.__set__(obj, val, undo_funcs)
        except:
            for undo_func in undo_funcs: undo_func()
            raise
        obj.__dict__.update(avdict)
    def _set_(obj, avdict, fromdb=False):
        raise NotImplementedError
    @classmethod
    def _normalize_args_(entity, args, keyargs, setdefault=False):
        if not args: pass
        elif len(args) != len(entity._pk_attrs_): raise TypeError('Invalid count of attrs in primary key')
        else:
            for attr, val in izip(entity._pk_attrs_, args):
                if keyargs.setdefault(attr.name, val) is not val:
                    raise TypeError('Ambiguos value of attribute %s' % attr.name)
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
            if None in pkval: pkval = None
            else: pkval = tuple(pkval)
        else: pkval = avdict.get(entity._pk_)
        return pkval, avdict        
    def _keyargs_to_avdicts_(obj, keyargs):
        avdict, collection_avdict = {}, {}
        get = obj._adict_.get
        for name, val in keyargs.items():
            attr = get(name)
            if attr is None: raise TypeError('Unknown attribute %r' % name)
            val = attr.check(val, obj)
            if not attr.is_collection:
                if attr.pk_offset is not None:
                    prev = obj.__dict__.get(attr, NOT_LOADED)
                    if prev != val: raise TypeError('Cannot change value of primary key attribute %s' % attr.name)
                else: avdict[attr] = val
            else: collection_avdict[attr] = val
        return avdict, collection_avdict

class Diagram(object):
    def __init__(diagram):
        diagram.entities = {}
        diagram.unmapped_attrs = {}
        diagram.mapping = None
    def generate_mapping(diagram, filename=None):
        if diagram.mapping: raise MappingError('Mapping was already generated')
        if filename is not None: raise NotImplementedError
        for entity_name in diagram.unmapped_attrs:
            raise DiagramError('Entity definition %s was not found' % entity_name)

        mapping = diagram.mapping = Mapping()
        for entity in diagram.entities.values(): entity._calculate_pk_info_()
        for entity in diagram.entities.values():
            table_name = entity.__dict__.get('_table_')
            if table_name is None:
                table_name = entity._table_ = entity.__name__
            elif not isinstance(table_name, basestring):
                raise TypeError('%s._table_ property must be a string. Got: %r' % (entity.__name__, table_name))
            table = mapping.tables.get(table_name)
            if table is None:
                table = mapping.tables[table_name] = Table(mapping, table_name)
            elif table.entities: raise NotImplementedError
            table.entities.add(entity)

            if entity._base_attrs_: raise NotImplementedError
            for attr in entity._new_attrs_:
                py_type = attr.py_type
                if attr.is_collection:
                    reverse = attr.reverse
                    if not reverse.is_collection:
                        if attr.table is not None: raise MappingError(
                            "Parameter 'table' is allowed for many to many relations only")
                        continue
                    name1 = attr.entity.__name__
                    name2 = reverse.entity.__name__
                    if name1 >= name2: continue
                    if attr.table:
                        if reverse.table != attr.table: raise MappingError(
                            "Parameter 'table' for %s.%s and %s.%s do not match"
                            % (attr.entity.__name__, attr.name, reverse.entity.__name__, reverse.name))
                        table_name = attr.table
                    else: table_name = name1 + '_' + name2
                    m2m_table = mapping.tables.get(table_name)
                    if m2m_table is None:
                        m2m_table = mapping.tables[table_name] = Table(mapping, table_name)
                        pk_info = entity._pk_info_
                        if reverse.columns:
                            if len(reverse.columns) != len(pk_info): raise MappingError(
                                'Invalid number of columns for %s.%s' % (reverse.entity.__name__, reverse.name))
                            for col_name, (attr2, name2) in zip(reverse.columns, pk_info):
                                m2m_table.add_column(col_name, True, attr2)
                        else:
                            reverse.columns = []
                            prefix = attr.entity.__name__.lower() + '_'
                            for attr2, name in pk_info:
                                col_name = prefix + name
                                reverse.columns.append(col_name)
                                m2m_table.add_column(col_name, True, attr2)
                            if len(reverse.columns) == 1: reverse.column = reverse.columns[0]
                        pk_info = reverse.entity._pk_info_
                        if attr.columns:
                            if len(attr.columns) != len(pk_info): raise MappingError(
                                'Invalid number of columns for %s.%s' % (attr.entity.__name__, attr.name))
                            for col_name, (attr2, name2) in zip(attr.columns, pk_info):
                                m2m_table.add_column(col_name, True, attr2)
                        else:
                            attr.columns = []
                            prefix = reverse.entity.__name__.lower() + '_'
                            for attr2, name in pk_info:
                                col_name = prefix + name
                                attr.columns.append(col_name)
                                m2m_table.add_column(col_name, True, attr2)
                            if len(attr.columns) == 1: attr.column = attr.columns[0]
                    else:
                        if m2m_table.entities or m2m_table.m2m: raise MappingError(
                            "Table name '%s' is already in use" % table_name)
                        raise NotImplementedError
                    m2m_table.m2m.add(attr)
                    m2m_table.m2m.add(reverse)
                elif attr.columns is None:
                    if not issubclass(py_type, Entity):
                        col_name = attr.column = attr.name
                        attr.columns = [ attr.name ]
                        table.add_column(col_name, attr.pk_offset is not None, attr)
                    else:
                        attr.columns = []
                        for attr2, name2 in py_type._pk_info_:
                            col_name = attr.name + '_' + name2
                            attr.columns.append(col_name)
                            table.add_column(col_name, attr.pk_offset is not None, attr2)
                        if len(attr.columns) == 1: attr.column = attr.columns[0]
                else:
                    if not issubclass(py_type, Entity):
                        if len(attr.columns) > 1: raise MappingError(
                            'Invalid number of columns for %s.%s' % (attr.entity.__name__, attr.name))
                        table.add_column(attr.name, attr.pk_offset is not None, attr)
                    else:
                        pk_info = attr.reverse.entity._pk_info_
                        if len(attr.columns) != len(pk_info): raise MappingError(
                            'Invalid number of columns for %s.%s' % (attr.entity.__name__, attr.name))
                        for col_name, (attr2, name2) in zip(attr.columns, pk_info):
                            table.add_column(col_name, attr.pk_offset is not None, attr2)

def generate_mapping(*args, **keyargs):
    outer_dict = sys._getframe(1).f_locals
    diagram = outer_dict.get('_diagram_')
    if diagram is None: raise MappingError('No default diagram found')
    diagram.generate_mapping(*args, **keyargs)

class Transaction(object):
    def __init__(trans):
        trans.ignore_none = True
        trans.indexes = {}
        trans.created = set()
        trans.deleted = set()
        trans.updated = set()
        trans.added = {}
        trans.removed = {}
    def update_simple_index(trans, obj, attr, prev, val, undo):
        index = trans.indexes.get(attr)
        if index is None: index = trans.indexes[attr] = {}
        if val is None and trans.ignore_none: val = NO_UNDO_NEEDED
        else:
            obj2 = index.setdefault(val, obj)
            if obj2 is not obj: raise IndexError(
                'Cannot update %s.%s: %s with key %s already exists'
                % (obj.__class__.__name__, attr.name, obj2, val))
        if prev is NOT_LOADED: prev = NO_UNDO_NEEDED
        elif prev is None and trans.ignore_none: prev = NO_UNDO_NEEDED
        else: del index[prev]
        undo.append((index, prev, val))
    def update_composite_index(trans, obj, attrs, prevs, vals, undo):
        if trans.ignore_none:
            if None in prevs: prevs = NO_UNDO_NEEDED
            if None in vals: vals = NO_UNDO_NEEDED
        if prevs is NO_UNDO_NEEDED: pass
        elif NOT_LOADED in prevs: prevs = NO_UNDO_NEEDED
        if vals is NO_UNDO_NEEDED: pass
        elif NOT_LOADED in vals: vals = NO_UNDO_NEEDED
        if prevs is NO_UNDO_NEEDED and vals is NO_UNDO_NEEDED: return
        index = trans.indexes.get(attrs)
        if index is None: index = trans.indexes[attrs] = {}
        if vals is NO_UNDO_NEEDED: pass
        else:
            obj2 = index.setdefault(vals, obj)
            if obj2 is not obj:
                attr_names = ', '.join(attr.name for attr in attrs)
                raise IndexError('Cannot update %r: composite key (%s) with value %s already exists for %r'
                % (obj, attr_names, vals, obj2))
        if prevs is NO_UNDO_NEEDED: pass
        else: del index[prevs]
        undo.append((index, prevs, vals))

class Local(threading.local):
    def __init__(self):
        self.trans = None

local = Local()

def get_trans():
    trans = local.trans
    if trans is None: trans = local.trans = Transaction()
    return trans

class Mapping(object):
    def __init__(mapping):
        mapping.tables = {}

class Table(object):
    def __init__(table, mapping, name):
        table.mapping = mapping
        table.name = name
        table.column_list = []
        table.column_dict = {}
        table.entities = set()
        table.m2m = set()
    def __repr__(table):
        return '<Table(%s)>' % table.name
    def add_column(table, col_name, pk, attr):
        if col_name in table.column_dict:
            raise MappingError('Column %s in table %s was already mapped' % (col_name, table.name))
        column = Column(table, col_name, pk, attr)
        table.column_list.append(column)
        table.column_dict[col_name] = column
        return column

class Column(object):
    def __init__(column, table, name, pk=False, attr=None):
        column.table = table
        column.name = name
        column.pk = pk
        column.attr = attr
        column.sql_type = attr.sql_type
    def __repr__(column):
        if column.pk: return '<Column(%s.%s) PK>' % (column.table.name, column.name)
        return '<Column(%s.%s)>' % (column.table.name, column.name)
