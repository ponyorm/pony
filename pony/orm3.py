import sys, threading
from operator import attrgetter
from itertools import count, ifilter, ifilterfalse, izip

try: from pony.thirdparty import etree
except ImportError: etree = None

import dbapiprovider
from pony import options
from pony.sqlsymbols import *

class OrmError(Exception): pass

class DiagramError(OrmError): pass
class SchemaError(OrmError): pass
class MappingError(OrmError): pass
class ConstraintError(OrmError): pass
class IndexError(OrmError): pass
class MultipleObjectsFoundError(OrmError): pass
class TooManyObjectsFoundError(OrmError): pass
class OperationWithDeletedObjectError(OrmError): pass
class TransactionError(OrmError): pass
class IntegrityError(TransactionError): pass
class IsolationError(TransactionError): pass
class UnrepeatableReadError(IsolationError): pass
class UnresolvableCyclicDependency(TransactionError): pass
class UnexpectedError(TransactionError): pass

class NotLoadedValueType(object):
    def __repr__(self): return 'NOT_LOADED'

NOT_LOADED = NotLoadedValueType()

class DefaultValueType(object):
    def __repr__(self): return 'DEFAULT'

DEFAULT = DefaultValueType()

class NoUndoNeededValueType(object):
    def __repr__(self): return 'NO_UNDO_NEEDED'

NO_UNDO_NEEDED = NoUndoNeededValueType()

next_attr_id = count(1).next

class Attribute(object):
    __slots__ = 'is_required', 'is_unique', 'is_indexed', 'is_pk', 'is_collection', \
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
        attr.id = next_attr_id()
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
                raise TypeError('Default value for required attribute cannot be None' % attr)

        attr.reverse = keyargs.pop('reverse', None)
        if not attr.reverse: pass
        elif not isinstance(attr.reverse, (basestring, Attribute)):
            raise TypeError("Value of 'reverse' option must be name of reverse attribute). Got: %r" % attr.reverse)
        elif not isinstance(attr.py_type, (basestring, EntityMeta)):
            raise DiagramError('Reverse option cannot be set for this type %r' % attr.py_type)

        attr.column = keyargs.pop('column', None)
        attr.columns = keyargs.pop('columns', None)
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
            if len(attr.columns) == 1: attr.column = attr.columns[0]
        else: attr.columns = []
        attr._columns_checked = False

        for option in keyargs: raise TypeError('Unknown option %r' % option)
        attr.composite_keys = []
    def _init_(attr, entity, name):
        attr.entity = entity
        attr.name = name
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
        if not attr.columns:
            reverse = attr.reverse
            assert reverse is not None and reverse.columns
            objects = reverse.entity._find_in_db_(None, { reverse : obj }, 1)
            assert len(objects) == 1
            return objects[0]
        obj._load_()
        return obj.__dict__[attr]
    def __get__(attr, obj, cls=None):
        if obj is None: return attr
        result = attr.get(obj)
        if attr.pk_offset is not None: return result
        bit = obj._bits_[attr]
        wbits = obj._wbits_
        if wbits is not None and not wbits & bit: obj._rbits_ |= bit
        return result
    def get(attr, obj):
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
        val = obj.__dict__.get(attr, NOT_LOADED)
        if val is NOT_LOADED: val = attr.load(obj)
        return val
    def __set__(attr, obj, val, undo_funcs=None):
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
        is_reverse_call = undo_funcs is not None
        reverse = attr.reverse
        val = attr.check(val, obj)
        pkval = obj._pkval_
        if attr.pk_offset is not None:
            if pkval is None: pass
            elif obj._pk_is_composite_:
                if val == pkval[attr.pk_offset]: return
            elif val == pkval: return
            raise TypeError('Cannot change value of primary key')
        prev =  obj.__dict__.get(attr, NOT_LOADED)
        if prev is NOT_LOADED and reverse and not reverse.is_collection:
            assert not is_reverse_call
            prev = attr.load(obj)
        trans = obj._trans_
        status = obj._status_
        wbits = obj._wbits_
        if wbits is not None:
            obj._wbits_ = wbits | obj._bits_[attr]
            if status != 'updated':
                if status in ('loaded', 'saved'): trans.to_be_checked.append(obj)
                else: assert status == 'locked'
                obj._status_ = 'updated'
                trans.updated.add(obj)
        if not attr.reverse and not attr.is_indexed:
            obj.__dict__[attr] = val
            return
        if not is_reverse_call: undo_funcs = []
        undo = []
        def undo_func():
            obj._status_ = status
            obj._wbits_ = wbits
            if wbits == 0: trans.updated.remove(obj)
            if status in ('loaded', 'saved'):
                to_be_checked = trans.to_be_checked
                if to_be_checked and to_be_checked[-1] is obj: to_be_checked.pop()
                assert obj not in to_be_checked
            obj.__dict__[attr] = prev
            for index, old_key, new_key in undo:
                if new_key is NO_UNDO_NEEDED: pass
                else: del index[new_key]
                if old_key is NO_UNDO_NEEDED: pass
                else: index[old_key] = obj
        undo_funcs.append(undo_func)
        if prev == val: return
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
    def db_set(attr, obj, old, is_reverse_call=False):
        assert obj._status_ not in ('created', 'deleted', 'cancelled')
        assert attr.pk_offset is None
        reverse = attr.reverse
        old = attr.check(old, obj)
        prev_old = obj.__dict__.get(attr.name, NOT_LOADED)
        if prev_old == old: return
        bit = obj._bits_[attr]
        if obj._rbits_ & bit:
            assert prev_old is not NOT_LOADED
            raise UnrepeatableReadError('Value of %s.%s for %s was updated outside of current transaction (was: %s, now: %s)'
                                        % (obj.__class__.__name__, attr.name, obj, prev_old, old))
        obj.__dict__[attr.name] = old
        if obj._wbits_ & bit: return
        val = old
        prev = obj.__dict__.get(attr, NOT_LOADED)
        assert prev == prev_old

        if not attr.reverse and not attr.is_indexed: return
        trans = obj._trans_
        if attr.is_unique: trans.db_update_simple_index(obj, attr, prev, val)
        for attrs, i in attr.composite_keys:
            get = obj.__dict__.get
            vals = [ get(a, NOT_LOADED) for a in attrs ]
            prevs = tuple(vals)
            vals[i] = val
            vals = tuple(vals)
            trans.db_update_composite_index(obj, attrs, prevs, vals)
        if not reverse: pass
        elif not is_reverse_call: attr.db_update_reverse(obj, prev, val)
        elif prev is not None:
            if not reverse.is_collection:
                assert prev is not NOT_LOADED
                reverse.db_set(prev, None, is_reverse_call=True)
            elif isinstance(reverse, Set):
                if prev is NOT_LOADED: pass
                else: reverse.db_reverse_remove((prev,), obj)
            else: raise NotImplementedError
        obj.__dict__[attr] = val
    def update_reverse(attr, obj, prev, val, undo_funcs):
        reverse = attr.reverse
        if not reverse.is_collection:
            if prev is NOT_LOADED: pass
            elif prev is not None: reverse.__set__(prev, None, undo_funcs)
            if val is not None: reverse.__set__(val, obj, undo_funcs)
        elif isinstance(reverse, Set):
            if prev is NOT_LOADED: pass
            elif prev is not None: reverse.reverse_remove((prev,), obj, undo_funcs)
            if val is not None: reverse.reverse_add((val,), obj, undo_funcs)
        else: raise NotImplementedError
    def db_update_reverse(attr, obj, prev, val):
        reverse = attr.reverse
        if not reverse.is_collection:
            if prev is NOT_LOADED: pass
            elif prev is not None: reverse.db_set(prev, None)
            if val is not None: reverse.db_set(val, obj)
        elif isinstance(reverse, Set):
            if prev is NOT_LOADED: pass
            elif prev is not None: reverse.db_reverse_remove((prev,), obj)
            if val is not None: reverse.db_reverse_add((val,), obj)
        else: raise NotImplementedError
    def __delete__(attr, obj):
        raise NotImplementedError
    def append_criteria(attr, val, criteria_list, params, table_alias=None):
        if attr.reverse: val = val._raw_pkval_
        if attr.column is not None: pairs = [ (attr.column, val) ]
        else:
            assert len(attr.columns) == len(val)
            pairs = zip(attr.columns, val)
        for column, val in pairs:
            criteria_list.append([EQ, [COLUMN, table_alias, column], [ PARAM, len(params) ] ])
            params.append(val)
    def get_columns(attr):
        assert not attr.is_collection
        assert not isinstance(attr.py_type, basestring)
        if attr._columns_checked: return attr.columns

        reverse = attr.reverse
        if not reverse: # attr is not part of relationship
            if not attr.columns: attr.columns = [ attr.name ]
            elif len(attr.columns) > 1: raise MappingError("Too many columns were specified for %s" % attr)
        else:
            def generate_columns():
                reverse_pk_columns = reverse.entity._get_pk_columns_()
                if not attr.columns:
                    if len(reverse_pk_columns) == 1: attr.columns = [ attr.name ]
                    else:
                        prefix = attr.name + '_'
                        attr.columns = [ prefix + column for column in reverse_pk_columns ]
                elif len(attr.columns) != len(reverse_pk_columns): raise MappingError(
                    'Invalid number of columns specified for %s' % attr)
               
            if reverse.is_collection: # one-to-many:
                generate_columns()
            # one-to-one:
            elif attr.is_required:
                assert not reverse.is_required
                generate_columns()
            elif reverse.is_required:
                if attr.columns: raise MappingError(
                    "Parameter 'column' cannot be specified for attribute %s. "
                    "Specify this parameter for reverse attribute %s or make %s optional"
                    % (attr, reverse, reverse))
            elif reverse.columns:
                if attr.columns: raise MappingError(
                    "Both attributes %s and %s have parameter 'column'. "
                    "Parameter 'column' cannot be specified at both sides of one-to-one relation"
                    % (attr, reverse))
            elif attr.entity.__name__ > reverse.entity.__name__: pass
            else: generate_columns()
        attr._columns_checked = True
        if len(attr.columns) == 1: attr.column = attr.columns[0]
        else: attr.column = None
        return attr.columns
            
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
        table = keyargs.pop('table', None)  # TODO: rename table to link_table or m2m_table
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

EMPTY = ()

class SetData(set):
    __slots__ = 'is_fully_loaded', 'added', 'removed'
    def __init__(setdata):
        setdata.is_fully_loaded = False
        setdata.added = setdata.removed = EMPTY

class Set(Collection):
    def check(attr, val, obj=None, entity=None):
        assert val is not NOT_LOADED
        if val is None or val is DEFAULT: return set()
        if entity is not None: pass
        elif obj is not None: entity = obj.__class__
        else: entity = attr.entity
        reverse = attr.reverse
        if not reverse: raise NotImplementedError
        if isinstance(val, reverse.entity): items = set((val,))
        else:
            rentity = reverse.entity
            try: items = set(val)
            except TypeError: raise TypeError(
                'Item of collection %s.%s must be an instance of %s. Got: %r'
                % (entity.__name__, attr.name, rentity.__name__, val))
            for item  in items:
                if not isinstance(item, rentity): raise TypeError(
                    'Item of collection %s.%s must be an instance of %s. Got: %r'
                    % (entity.__name__, attr.name, rentity.__name__, item))
        if obj is not None: trans = obj._trans_
        else: trans = get_trans()
        for item in items:
            if item._trans_ is not trans:
                raise TransactionError('An attempt to mix objects belongs to different transactions')
        return items
    def load(attr, obj):
        assert obj._status_ not in ('deleted', 'cancelled')
        setdata = obj.__dict__.get(attr, NOT_LOADED)
        if setdata is not NOT_LOADED and setdata.is_fully_loaded: return setdata
        reverse = attr.reverse
        if reverse is None: raise NotImplementedError
        assert issubclass(reverse.py_type, Entity)
        if setdata is NOT_LOADED: setdata = obj.__dict__[attr] = SetData()
        if not reverse.is_collection:
            reverse.entity._find_(None, (), {reverse.name:obj})
        else:
            sql_ast, params = attr.construct_sql_m2m(obj)
            database = obj._diagram_.database
            cursor = database._exec_ast(sql_ast, params)
            items = []
            for row in cursor.fetchall():
                item = attr.py_type._get_by_raw_pkval_(row)
                if item in setdata: continue
                if item in setdata.removed: continue
                items.append(item)
                setdata.add(item)
            reverse.db_reverse_add(items, obj)
        setdata.is_fully_loaded = True
        return setdata
    def construct_sql_m2m(attr, obj):
        reverse = attr.reverse
        assert reverse is not None and reverse.is_collection and issubclass(reverse.py_type, Entity)
        table_name = attr.table
        assert table_name is not None
        select_list = [ ALL ]
        for column in attr.columns:
            select_list.append([COLUMN, 'T1', column ])
        from_list = [ FROM, [ 'T1', TABLE, table_name ]]
        criteria_list = []
        params = []
        raw_pkval = obj._calculate_raw_pkval_()
        if not obj._pk_is_composite_: raw_pkval = [ raw_pkval ]
        for column, val in zip(reverse.columns, raw_pkval):
            criteria_list.append([EQ, [COLUMN, 'T1', column], [ PARAM, len(params) ]])
            params.append(val)
        sql_ast = [ SELECT, select_list, from_list ]
        if criteria_list:
            where_list = [ WHERE ]
            if len(criteria_list) == 1: where_list.append(criteria_list[0])
            else: where_list.append([ AND ] + criteria_list)
            sql_ast.append(where_list)
        return sql_ast, params
    def copy(attr, obj):
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
        setdata = obj.__dict__.get(attr, NOT_LOADED)
        if setdata is NOT_LOADED or not setdata.is_fully_loaded: setdata = attr.load(obj)
        reverse = attr.reverse
        if reverse.is_collection or reverse.pk_offset is not None: return setdata.copy()
        for item in setdata:
            bit = item._bits_[reverse]
            wbits = item._wbits_
            if wbits is not None and not wbits & bit: item._rbits_ |= bit
        return setdata.copy()
    def __get__(attr, obj, type=None):
        if obj is None: return attr
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
        return SetWrapper(obj, attr)
    def __set__(attr, obj, val, undo_funcs=None):
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
        items = attr.check(val, obj)
        reverse = attr.reverse
        if not reverse: raise NotImplementedError
        setdata = obj.__dict__.get(attr, NOT_LOADED)
        if setdata is NOT_LOADED:
            if obj._status_ == 'created':
                setdata = obj.__dict__[attr] = SetData()
                setdata.is_fully_loaded = True
                if not items: return
            else: setdata = attr.load()
        elif not setdata.is_fully_loaded: setdata = attr.load()
        to_add = set(ifilterfalse(setdata.__contains__, items))
        to_remove = setdata - items
        if undo_funcs is None: undo_funcs = []
        try:
            if not reverse.is_collection:
                for item in to_remove: reverse.__set__(item, None, undo_funcs)
                for item in to_add: reverse.__set__(item, obj, undo_funcs)
            else:
                reverse.reverse_remove(to_remove, obj, undo_funcs)
                reverse.reverse_add(to_add, obj, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
            raise
        setdata.clear()
        setdata.update(items)
        if to_add:
            if setdata.added is EMPTY: setdata.added = to_add
            else: setdata.added.update(to_add)
            if setdata.removed is not EMPTY: setdata.removed -= to_add
        if to_remove:
            if setdata.removed is EMPTY: setdata.removed = to_remove
            else: setdata.removed.update(to_remove)
            if setdata.added is not EMPTY: setdata.added -= to_remove
        trans = obj._trans_
        trans.modified_collections.setdefault(attr, set()).add(obj)
    def __delete__(attr, obj):
        raise NotImplementedError
    def reverse_add(attr, objects, item, undo_funcs):
        undo = []
        trans = item._trans_
        objects_with_modified_collections = trans.modified_collections.setdefault(attr, set())
        for obj in objects:
            setdata = obj.__dict__.get(attr, NOT_LOADED)
            if setdata is NOT_LOADED:
                setdata = obj.__dict__[attr] = SetData()
            if setdata.added is EMPTY: setdata.added = set()  
            elif item in setdata.added: raise AssertionError
            in_setdata = item in setdata
            in_removed = item in setdata.removed
            was_modified_earlier = obj in objects_with_modified_collections
            undo.append((obj, in_setdata, in_removed, was_modified_earlier))
            if not in_setdata: setdata.add(item)
            setdata.added.add(item)
            if in_removed: setdata.removed.remove(item)
            objects_with_modified_collections.add(obj)
        def undo_func():
            for obj, in_setdata, in_removed, was_modified_earlier in undo:
                setdata = obj.__dict__[attr]
                setdata.added.remove(item)
                if not in_setdata: setdata.remove(item)
                if in_removed: setdata.removed.add(item)
                if not was_modified_earlier: objects_with_modified_collections.remove(obj)
        undo_funcs.append(undo_func)
    def db_reverse_add(attr, objects, item):
        for obj in objects:
            setdata = obj.__dict__.get(attr, NOT_LOADED)
            if setdata is NOT_LOADED:
                setdata = obj.__dict__[attr] = SetData()
            elif setdata.is_fully_loaded:
                raise UnrepeatableReadError('Phantom object %r appeared in collection %r.%s' % (item, obj, attr.name))
            setdata.add(item)
    def reverse_remove(attr, objects, item, undo_funcs):
        undo = []
        trans = item._trans_
        objects_with_modified_collections = trans.modified_collections.setdefault(attr, set())
        for obj in objects:
            setdata = obj.__dict__.get(attr, NOT_LOADED)
            if setdata is NOT_LOADED:
                setdata = obj.__dict__[attr] = SetData()
            if setdata.removed is EMPTY: setdata.removed = set()
            elif item in setdata.removed: raise AssertionError
            in_setdata = item in setdata
            in_added = item in setdata.added
            was_modified_earlier = obj in objects_with_modified_collections
            undo.append((obj, in_setdata, in_added, was_modified_earlier))
            if in_setdata: setdata.remove(item)
            if in_added: setdata.added.remove(item)
            setdata.removed.add(item)
            objects_with_modified_collections.add(obj)
        def undo_func():
            for obj, in_setdata, in_removed, was_modified_earlier in undo:
                setdata = obj.__dict__[attr]
                if in_added: setdata.added.add(item)
                if in_setdata: setdata.add(item)
                setdata.removed.remove(item)
                if not was_modified_earlier: objects_with_modified_collections.remove(obj)
        undo_funcs.append(undo_func)
    def db_reverse_remove(attr, objects, item):
        raise AssertionError
    def get_m2m_columns(attr):
        if attr._columns_checked: return reverse.columns
        entity = attr.entity
        reverse = attr.reverse
        if reverse.columns:
            if len(reverse.columns) != len(entity._get_pk_columns_()): raise MappingError(
                'Invalid number of columns for %s' % reverse)
        else:
            columns = entity._get_pk_columns_()
            if len(columns) == 1: reverse.columns = [ entity.__name__.lower() ]
            else:
                prefix = entity.__name__.lower() + '_'
                reverse.columns = [ prefix + column for column in columns ]
        attr._columns_checked = True
        return reverse.columns

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
        return '%r.%s => %r' % (wrapper._obj_, wrapper._attr_.name, wrapper.copy())
    def __str__(wrapper):
        return str(wrapper.copy())
    def __nonzero__(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
        setdata = obj.__dict__.get(attr, NOT_LOADED)
        if setdata is NOT_LOADED: setdata = attr.load(obj)
        if setdata: return True
        if not setdata.is_fully_loaded: setdata = attr.load(obj)
        return bool(setdata)
    def __len__(wrapper):
        attr = wrapper._attr_
        obj = wrapper._obj_
        setdata = obj.__dict__.get(attr, NOT_LOADED)
        if setdata is NOT_LOADED or not setdata.is_fully_loaded: setdata = attr.load(obj)
        return len(setdata)
    def __iter__(wrapper):
        return iter(wrapper.copy())
    def __eq__(wrapper, x):
        if isinstance(x, SetWrapper):
            if wrapper._obj_ is x._obj_ and _attr_ is x._attr_: return True
            else: x = x.copy()
        elif not isinstance(x, set): x = set(x)
        items = wrapper.copy()
        return items == x
    def __ne__(wrapper, x):
        return not wrapper.__eq__(x)
    def __add__(wrapper, x):
        return wrapper.copy().union(x)
    def __sub__(wrapper, x):
        return wrapper.copy().difference(x)
    def __contains__(wrapper, item):
        obj = wrapper._obj_
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
        attr = wrapper._attr_
        setdata = obj.__dict__.get(attr, NOT_LOADED)
        if setdata is not NOT_LOADED:
            if item in setdata: return True
            if setdata.is_fully_loaded: return False
        setdata = attr.load(obj)
        return item in setdata
    def add(wrapper, x):
        obj = wrapper._obj_
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
        attr = wrapper._attr_
        reverse = attr.reverse
        if not reverse: raise NotImplementedError
        items = attr.check(x, obj)
        setdata = obj.__dict__.get(attr, NOT_LOADED)
        if setdata is NOT_LOADED: setdata = obj.__dict__[attr] = SetData()
        items.difference_update(setdata.added)
        undo_funcs = []
        try:
            if not reverse.is_collection:
                  for item in items - setdata: reverse.__set__(item, obj, undo_funcs)
            else: reverse.reverse_add(items - setdata, obj, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
            raise
        setdata.update(items)
        if setdata.added is EMPTY: setdata.added = items
        else: setdata.added.update(items)
        if setdata.removed is not EMPTY: setdata.removed -= items
        obj._trans_.modified_collections.setdefault(attr, set()).add(obj)
    def __iadd__(wrapper, x):
        wrapper.add(x)
        return wrapper
    def remove(wrapper, x):
        obj = wrapper._obj_
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
        attr = wrapper._attr_
        reverse = attr.reverse
        if not reverse: raise NotImplementedError
        items = attr.check(x, obj)
        setdata = obj.__dict__.get(attr, NOT_LOADED)
        if setdata is NOT_LOADED or not setdata.is_fully_loaded:
            setdata = attr.load(obj) # TODO: Load only the necessary objects
        items.difference_update(setdata.removed)
        undo_funcs = []
        try:
            if not reverse.is_collection:
                for item in (items & setdata): reverse.__set__(item, None, undo_funcs)
            else: reverse.reverse_remove(items & setdata, obj, undo_funcs)
        except:
            for undo_func in reversed(undo_funcs): undo_func()
            raise
        setdata -= items
        if setdata.added is not EMPTY: setdata.added -= items
        if setdata.removed is EMPTY: setdata.removed = items
        else: setdata.removed.update(items)
        obj._trans_.modified_collections.setdefault(attr, set()).add(obj)
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

next_entity_id = count(1).next
next_new_instance_id = count(1).next

class Entity(object):
    __metaclass__ = EntityMeta
    __slots__ = '__dict__', '__weakref__', '_pkval_', '_raw_pkval_', '_newid_', '_trans_', '_status_', '_rbits_', '_wbits_'
    @classmethod
    def _cls_setattr_(entity, name, val):
        if name.startswith('_') and name.endswith('_'):
            type.__setattr__(entity, name, val)
        else: raise NotImplementedError
    @classmethod
    def _cls_init_(entity, diagram):
        if entity.__name__ in diagram.entities:
            raise DiagramError('Entity %s already exists' % entity.__name__)
        entity._id_ = next_entity_id()
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
        entity._pk_columns_ = None
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

            if attr.is_required and attr2.is_required: raise DiagramError(
                "At least one attribute of one-to-one relationship %s - %s must be optional" % (attr, attr2))

            attr.reverse = attr2
            attr2.reverse = attr
            unmapped_attrs.discard(attr2)          
        for attr in unmapped_attrs:
            raise DiagramError('Reverse attribute for %s.%s was not found' % (attr.entity.__name__, attr.name))
    def __init__(obj, *args, **keyargs):
        raise TypeError('Cannot create entity instances directly. Use Entity.create(...) or Entity.find(...) instead')
    @classmethod
    def _get_pk_columns_(entity):
        if entity._pk_columns_ is not None: return entity._pk_columns_
        pk_columns = []
        for attr in entity._pk_attrs_: pk_columns.extend(attr.get_columns())
        entity._pk_columns_ = pk_columns
        return pk_columns
    def _calculate_raw_pkval_(obj):
        if hasattr(obj, '_raw_pkval_'): return obj._raw_pkval_
        if len(obj._pk_attrs_) == 1:
              pk_pairs = [ (obj.__class__._pk_, obj._pkval_) ]
        else: pk_pairs = zip(obj._pk_attrs_, obj._pkval_)
        raw_pkval = []
        for attr, val in pk_pairs:
            if not attr.reverse: raw_pkval.append(val)
            elif len(attr.py_type._pk_attrs_) == 1: raw_pkval.append(val._raw_pkval_)
            else: raw_pkval.extend(val._raw_pkval_)
        if len(raw_pkval) > 1: obj._raw_pkval_ = tuple(raw_pkval)
        else: obj._raw_pkval_ = raw_pkval[0]
    def __repr__(obj):
        pkval = obj._pkval_
        if pkval is None: return '%s(new:%d)' % (obj.__class__.__name__, obj._newid_)
        elif obj._pk_is_composite_: return '%s%r' % (obj.__class__.__name__, pkval)
        else: return '%s(%r)' % (obj.__class__.__name__, pkval)
    @classmethod
    def _new_(entity, pkval, status, raw_pkval=None):
        assert status in ('loaded', 'created')
        trans = get_trans()
        index = trans.indexes.setdefault(entity._pk_, {})
        if pkval is None: obj = None
        else: obj = index.get(pkval)
        if obj is None: pass
        elif status == 'created':
            if entity._pk_is_composite_: pkval = ', '.join(str(item) for item in pkval)
            raise IndexError('Cannot create %s: instance with primary key %s already exists'
                             % (obj.__class__.__name__, pkval))                
        else: return obj
        obj = object.__new__(entity)
        obj._trans_ = trans
        obj._status_ = status
        obj._pkval_ = pkval
        if pkval is None:
            obj._newid_ = next_new_instance_id()
            obj._raw_pkval_ = None
        else:
            index[pkval] = obj
            obj._newid_ = None
            if raw_pkval is None: obj._calculate_raw_pkval_()
            elif len(raw_pkval) > 1: obj._raw_pkval_ = raw_pkval
            else: obj._raw_pkval_ = raw_pkval[0]
        if obj._pk_is_composite_: pairs = zip(entity._pk_attrs_, pkval)
        else: pairs = ((entity._pk_, pkval),)
        if status == 'loaded':
            obj._rbits_ = obj._wbits_ = 0
            for attr, val in pairs:
                obj.__dict__[attr] = val
                if attr.reverse: attr.db_update_reverse(obj, NOT_LOADED, val)
        elif status == 'created':
            obj._rbits_ = obj._wbits_ = None
            undo_funcs = []
            try:
                for attr, val in pairs:
                    obj.__dict__[attr] = val
                    if attr.reverse: attr.update_reverse(obj, NOT_LOADED, val, undo_funcs)
            except:  # will never be here in sane situation
                for undo_func in reversed(undo_funcs): undo_func()
                raise
        else: assert False
        return obj
    @classmethod
    def _get_by_raw_pkval_(entity, raw_pkval):
        i = 0
        pkval = []
        for attr in entity._pk_attrs_:
            if attr.column is not None:
                val = raw_pkval[i]
                i += 1
                if not attr.reverse: val = attr.check(val, None, entity)
                else: val = attr.py_type._get_by_raw_pkval_((val,))
            else:
                if not attr.reverse: raise NotImplementedError
                vals = raw_pkval[i:i+len(attr.columns)]
                val = attr.py_type._get_by_raw_pkval_(vals)
            pkval.append(val)
        if not entity._pk_is_composite_: pkval = pkval[0]
        else: pkval = tuple(pkval)
        obj = entity._new_(pkval, 'loaded', raw_pkval)
        assert obj._status_ not in ('deleted', 'cancelled')
        return obj
    @classmethod
    def _find_in_cache_(entity, pkval, avdict):
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
                if isinstance(val, Entity) and val._raw_pkval_ is None:  #############
                    reverse = attr.reverse
                    if not reverse.is_collection:
                        obj = reverse.__get__(val)
                        if obj is None: return None
                    elif isinstance(reverse, Set):
                        objects = reverse.__get__(val).copy()  ##################
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
        raise KeyError
    def _load_(obj):
        objects = obj._find_in_db_(obj._pkval_)
        if not objects: raise UnrepeatableReadError('%s disappeared' % obj)
        assert len(objects) == 1 and obj == objects[0]
    @classmethod
    def _construct_sql_(entity, pkval, avdict=None):
        table_name = entity._table_
        attr_offsets = {} 
        select_list = [ ALL ]
        for attr in entity._attrs_:
            if attr.is_collection: continue
            if not attr.columns: continue
            attr_offsets[attr] = len(select_list) - 1
            for column in attr.columns:
                select_list.append([ COLUMN, 'T1', column ])
        from_list = [ FROM, [ 'T1', TABLE, table_name ]]

        criteria_list = []
        params = []
        if avdict is not None: items = avdict.items()
        elif not entity._pk_is_composite_: items = [(entity._pk_, pkval)]
        else: items = zip(entity._pk_attrs_, pkval)
        for attr, val in items:
            if attr.reverse: val = val._raw_pkval_
            if attr.column is not None: pairs = [ (attr.column, val) ]
            else:
                assert len(attr.columns) == len(val)
                pairs = zip(attr.columns, val)
            for column, val in pairs:
                criteria_list.append([EQ, [COLUMN, 'T1', column], [ PARAM, len(params) ] ])
                params.append(val)
        sql_ast = [ SELECT, select_list, from_list ]
        if criteria_list:
            where_list = [ WHERE ]
            if len(criteria_list) == 1: where_list.append(criteria_list[0])
            else: where_list.append([ AND ] + criteria_list)
            sql_ast.append(where_list)
        return sql_ast, params, attr_offsets
    @classmethod
    def _parse_row_(entity, row, attr_offsets):
        avdict = {}
        for attr, i in attr_offsets.iteritems():
            if attr.column is not None:
                val = row[i]
                if not attr.reverse: val = attr.check(val, None, entity)
                else: val = attr.py_type._get_by_raw_pkval_((val,))
            else:
                if not attr.reverse: raise NotImplementedError
                vals = row[i:i+len(attr.columns)]
                val = attr.py_type._get_by_raw_pkval_(vals)
            avdict[attr] = val
        if not entity._pk_is_composite_: pkval = avdict.pop(entity._pk_, None)            
        else: pkval = tuple(avdict.pop(attr, None) for attr in entity._pk_attrs_)
        return pkval, avdict
    @classmethod
    def _find_in_db_(entity, pkval, avdict=None, max_rows_count=None):
        sql_ast, params, attr_offsets = entity._construct_sql_(pkval, avdict)
        database = entity._diagram_.database
        cursor = database._exec_ast(sql_ast, params)
        if max_rows_count is None: max_rows_count = options.MAX_ROWS_COUNT
        rows = cursor.fetchmany(max_rows_count + 1)
        if len(rows) == max_rows_count + 1:
            if max_rows_count == 1: raise MultipleObjectsFoundError(
                'Multiple objects was found. Use %s.find_all(...) instead of %s.find_one(...) to retrieve them'
                % (entity.__name__, entity.__name__))
            raise TooManyObjectsFoundError(
                'Found more then pony.options.MAX_ROWS_COUNT=%d objects' % options.MAX_ROWS_COUNT)
        objects = []
        for row in rows:
            pkval, avdict = entity._parse_row_(row, attr_offsets)
            obj = entity._new_(pkval, 'loaded')
            if obj._status_ in ('deleted', 'cancelled'): continue
            obj._db_set_(avdict)
            objects.append(obj)
        return objects
    @classmethod
    def _find_(entity, max_objects_count, args, keyargs):
        pkval, avdict = entity._normalize_args_(args, keyargs, False)
        for attr in avdict:
            if attr.is_collection: raise TypeError(
                'Collection attribute %s.%s cannot be specified as search criteria' % (attr.entity.__name__, attr.name))
        try:
            objects = [ entity._find_in_cache_(pkval, avdict) ]
        except KeyError:
            objects = entity._find_in_db_(pkval, avdict, max_objects_count)
        return objects        
    @classmethod
    def find_one(entity, *args, **keyargs):
        objects = entity._find_(1, args, keyargs)
        if not objects: return None
        assert len(objects) == 1
        return objects[0]
    @classmethod
    def find_all(entity, *args, **keyargs):
        return entity._find_(None, args, keyargs)
    @classmethod
    def create(entity, *args, **keyargs):
        pkval, avdict = entity._normalize_args_(args, keyargs, True)
        obj = entity._new_(pkval, 'created')
        trans = get_trans()
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
                if attr.pk_offset is not None: continue
                elif not attr.is_collection:
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
        trans.to_be_checked.append(obj)
        return obj
    def _db_set_(obj, avdict):
        assert obj._status_ not in ('created', 'deleted', 'cancelled')
        rbits = obj._rbits_
        wbits = obj._wbits_
        for attr, old in avdict.items():
            assert attr.pk_offset is None
            prev_old = obj.__dict__.get(attr.name, NOT_LOADED)
            if prev_old == old:
                del avdict[attr]
                continue
            bit = obj._bits_[attr]
            if rbits & bit: raise UnrepeatableReadError(
                'Value of %s.%s for %s was updated outside of current transaction (was: %s, now: %s)'
                % (obj.__class__.__name__, attr.name, obj, prev_old, old))
            obj.__dict__[attr.name] = old
            if wbits & bit:
                del avdict[attr]
                continue
            prev = obj.__dict__.get(attr, NOT_LOADED)
            assert prev == prev_old
        if not avdict: return
        NOT_FOUND = object()
        trans = obj._trans_
        for attr in obj._simple_keys_:
            val = avdict.get(attr, NOT_FOUND)
            if val is NOT_FOUND: continue
            prev = obj.__dict__.get(attr, NOT_LOADED)
            if prev == val: continue
            trans.db_update_simple_index(obj, attr, prev, val)
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
            trans.db_update_composite_index(obj, attrs, prevs, vals)
        for attr, val in avdict.iteritems():
            if not attr.reverse: continue
            prev = obj.__dict__.get(attr, NOT_LOADED)
            attr.db_update_reverse(obj, prev, val)
        obj.__dict__.update(avdict)
    def _delete_(obj, undo_funcs=None):
        is_recursive_call = undo_funcs is not None
        if not is_recursive_call: undo_funcs = []
        trans = obj._trans_
        status = obj._status_
        assert status not in ('deleted', 'cancelled')
        undo_list = []
        undo_dict = {}
        def undo_func():
            obj._status_ = status
            if status in ('loaded', 'saved'):
                to_be_checked = trans.to_be_checked
                if to_be_checked and to_be_checked[-1] is obj: to_be_checked.pop()
                assert obj not in to_be_checked
            obj.__dict__.update(undo_dict)
            for index, old_key in undo_list: index[old_key] = obj
        undo_funcs.append(undo_func)
        try:
            for attr in obj._attrs_:
                reverse = attr.reverse
                if not reverse: continue
                if not attr.is_collection:
                    val = obj.__dict__.get(attr, NOT_LOADED)
                    if val is None: continue
                    if not reverse.is_collection:
                        if val is NOT_LOADED: val = attr.load(obj)
                        if val is None: continue
                        if reverse.is_required:
                            raise ConstraintError('Cannot delete %s: Attribute %s.%s for %s cannot be set to None'
                                                  % (obj, reverse.entity.__name__, reverse.name, val))
                        reverse.__set__(val, None, undo_funcs)
                    elif isinstance(reverse, Set):
                        if val is NOT_LOADED: pass
                        else: reverse.reverse_remove((val,), obj, undo_funcs)
                    else: raise NotImplementedError
                elif isinstance(attr, Set):
                    if reverse.is_required and not attr.is_empty(obj): raise ConstraintError(
                        'Cannot delete %s: Attribute %s.%s for associated objects cannot be set to None'
                        % (obj, reverse.entity.__name__, reverse.name))
                    attr.__set__(obj, (), undo_funcs)
                else: raise NotImplementedError

            for attr in obj._simple_keys_:
                val = obj.__dict__.get(attr, NOT_LOADED)
                if val is NOT_LOADED: continue
                if val is None and trans.ignore_none: continue
                index = trans.indexes.get(attr)
                if index is None: continue
                obj2 = index.pop(val)
                assert obj2 is obj
                undo_list.append((index, val))
                
            for attrs in obj._composite_keys_:
                get = obj.__dict__.get
                vals = tuple(get(a, NOT_LOADED) for a in attrs)
                if NOT_LOADED in vals: continue
                if trans.ignore_none and None in vals: continue
                index = trans.indexes.get(attrs)
                if index is None: continue
                obj2 = index.pop(vals)
                assert obj2 is obj
                undo_list.append((index, vals))

            if status == 'created':
                obj._status_ = 'cancelled'
                assert obj in trans.created
                trans.created.remove(obj)
            else:
                if status == 'updated': trans.updated.remove(obj)
                elif status in ('loaded', 'saved'): trans.to_be_checked.append(obj)
                else: assert status == 'locked'
                obj._status_ = 'deleted'
                trans.deleted.add(obj)
            for attr in obj._attrs_:
                if attr.pk_offset is None:
                    val = obj.__dict__.pop(attr, NOT_LOADED)
                    if val is NOT_LOADED: continue
                    undo_dict[attr] = val
        except:
            if not is_recursive_call:
                for undo_func in reversed(undo_funcs): undo_func()
            raise
    def delete(obj):
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
        obj._delete_()
    def set(obj, **keyargs):
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
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
                new_wbits = wbits
                for attr in avdict: new_wbits |= obj._bits_[attr]
                obj._wbits_ = new_wbits
                if status != 'updated':
                    obj._status_ = 'updated'
                    trans.updated.add(obj)
                    if status in ('loaded', 'saved'): trans.to_be_checked.append(obj)
                    else: assert status == 'locked'
            if not collection_avdict:
                for attr in avdict:
                    if attr.reverse or attr.is_indexed: break
                else:
                    obj.__dict__.update(avdict)
                    return
        undo_funcs = []
        undo = []
        def undo_func():
            obj._status_ = status
            obj._wbits_ = wbits
            if wbits == 0: trans.updated.remove(obj)
            if status in ('loaded', 'saved'):
                to_be_checked = trans.to_be_checked
                if to_be_checked and to_be_checked[-1] is obj: to_be_checked.pop()
                assert obj not in to_be_checked
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
    def check_on_commit(obj):
        if obj._status_ not in ('loaded', 'saved'): return
        obj._status_ = 'locked'
        obj._trans_.to_be_checked.append(obj)
    def _save_(obj, delayed_objects=None):
        if delayed_objects is None: delayed_objects = []
        elif obj in delayed_objects:
            chain = ' -> '.join(obj2.__class__.__name__ for obj2 in delayed_objects)
            raise UnresolvableCyclicDependency('Cannot save cyclic chain: ' + chain)
        status = obj._status_
        if status in ('loaded', 'saved', 'cancelled'): return
        delayed_objects.append(obj)
        database = obj._diagram_.database
        rbits = obj._rbits_
        wbits = obj._wbits_
        get_bit = obj._bits_.get
        params = []

        if status in ('created', 'updated'):
            columns = []
            values = []
            for attr in obj._attrs_:
                if not attr.columns: continue
                if status == 'updated':
                    bit = get_bit(attr)
                    if bit is None: continue
                    if not bit & wbits: continue
                elif attr.is_collection: continue
                val = obj.__dict__[attr]
                if not attr.reverse: pairs = [ (val, attr.column) ]
                else:
                    if val._status_ == 'created': val._save_(delayed_objects)
                    assert val._status_ == 'saved'
                    if attr.column is not None: pairs = [ (val._raw_pkval_, attr.column) ]
                    else: pairs = zip(val._raw_pkval_, attr.columns)
                for val, column in pairs:
                    columns.append(column)
                    values.append([ PARAM, len(params) ])
                    params.append(val)
        
        if status == 'created':
            ast = [ INSERT, obj._table_, columns, values ]
            try:
                cursor = database._exec_ast(ast, params)
            except database.IntegrityError:
                raise IntegrityError('Object %r already exists in the database' % obj)
            except database.DatabaseError:
                raise UnexpectedError('Object %r cannot be stored in the database' % obj)
            if obj._raw_pkval_ is None:
                rowid = cursor.lastrowid # TODO
                obj._raw_pkval_ = rowid
            obj._status_ = 'saved'
            obj._rbits_ = 0
            obj._wbits_ = 0
            for attr in obj._attrs_:
                if attr not in obj._bits_: continue
                obj.__dict__[attr.name] = obj.__dict__[attr]
            return

        criteria_list = [ AND ]
        for attr in obj._pk_attrs_:
            val = obj.__dict__[attr]
            attr.append_criteria(val, criteria_list, params)

        if status == 'deleted':
            ast = [ DELETE, obj._table_, [ WHERE, criteria_list ] ]
            database._exec_ast(ast, params)
            return

        for attr in obj._attrs_:
            if not attr.columns: continue
            bit = get_bit(attr)
            if bit is None: continue
            if not bit & rbits: continue
            old = obj.__dict__.get(attr.name, NOT_LOADED)
            assert old is not NOT_LOADED
            attr.append_criteria(old, criteria_list, params)

        if status == 'locked':
            assert obj._wbits_ == 0
            ast = [ SELECT, [ ALL, [ VALUE, 1 ]], [ FROM, [ 'T1', TABLE, obj._table_ ] ], [ WHERE, criteria_list ] ]
            cursor = database._exec_ast(ast, params)
            row = cursor.fetchone()
            if row is None: raise UnrepeatableReadError('Object %r was updated outside of current transaction' % obj)
            obj._status_ = 'loaded'
        elif status == 'updated':
            ast = [ UPDATE, obj._table_, zip(columns, values), [ WHERE, criteria_list ] ]
            cursor = database._exec_ast(ast, params)
            if cursor.rowcount != 1:
                raise UnrepeatableReadError('Object %r was updated outside of current transaction' % obj)
            obj._status_ = 'saved'
            obj._rbits_ = 0
            obj._wbits_ = 0
            for attr in obj._attrs_:
                if attr not in obj._bits_: continue
                obj.__dict__[attr.name] = obj.__dict__[attr]
        else: assert False
        
class Diagram(object):
    def __init__(diagram):
        diagram.entities = {}
        diagram.unmapped_attrs = {}
        diagram.mapping = None
        diagram.database = None
    def generate_mapping(diagram, database, filename=None, check_tables=False):
        diagram.database = database
        if diagram.mapping: raise MappingError('Mapping was already generated')
        if filename is not None: raise NotImplementedError
        for entity_name in diagram.unmapped_attrs:
            raise DiagramError('Entity definition %s was not found' % entity_name)

        mapping = diagram.mapping = Mapping()
        entities = list(sorted(diagram.entities.values(), key=attrgetter('_id_')))
        for entity in entities:
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
                if attr.is_collection:
                    reverse = attr.reverse
                    if not reverse.is_collection: # many-to-one:
                        if attr.table is not None: raise MappingError(
                            "Parameter 'table' is not allowed for many-to-one attribute %s" % attr)
                        elif attr.columns: raise NotImplementedError(
                            "Parameter 'column' is not allowed for many-to-one attribute %s" % attr)
                        continue
                    # many-to-many:
                    if attr.entity.__name__ >= reverse.entity.__name__: continue
                    if attr.table:
                        if reverse.table != attr.table: raise MappingError(
                            "Parameter 'table' for %s and %s do not match" % (attr, reverse))
                        table_name = attr.table
                    else:
                        table_name = attr.entity.__name__ + '_' + reverse.entity.__name__
                        attr.table = reverse.table = table_name
                    m2m_table = mapping.tables.get(table_name)
                    if m2m_table is not None:
                        if m2m_table.entities or m2m_table.m2m: raise MappingError(
                            "Table name '%s' is already in use" % table_name)
                        raise NotImplementedError
                    m2m_table = mapping.tables[table_name] = Table(mapping, table_name)
                    for column in attr.get_m2m_columns():
                        m2m_table.add_column(column, True)
                    for column in reverse.get_m2m_columns():
                        m2m_table.add_column(column, True)
                    m2m_table.m2m.add(attr)
                    m2m_table.m2m.add(reverse)
                else:
                    for column in attr.get_columns():
                        table.add_column(column, attr.pk_offset is not None)
        if not check_tables: return
        for table in mapping.tables.values():
            sql_ast = [ SELECT,
                        [ ALL, ] + [ [ COLUMN, table.name, column.name ] for column in table.column_list ],
                        [ FROM, [ table.name, TABLE, table.name ] ],
                        [ WHERE, [ EQ, [ VALUE, 0 ], [ VALUE, 1 ] ] ]
                      ]
            database._exec_ast(sql_ast)

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
        trans.modified_collections = {}
        trans.to_be_checked = []
    def flush(trans):
        for obj in trans.to_be_checked: obj._save_()
    def commit(trans):
        databases = set()
        for obj in trans.to_be_checked:
            databases.add(obj._diagram_.database)
        modified_m2m = {}
        for attr, objects in trans.modified_collections.iteritems():
            if not isinstance(attr, Set): raise NotImplementedError
            reverse = attr.reverse
            if not reverse.is_collection: continue
            if not isinstance(reverse, Set): raise NotImplementedError
            if reverse in modified_m2m: continue
            added, removed = modified_m2m.setdefault(attr, (set(), set()))
            for obj in objects:
                databases.add(obj._diagram_.database)
                setdata = obj.__dict__[attr]
                for obj2 in setdata.added: added.add((obj, obj2))
                for obj2 in setdata.removed: removed.add((obj, obj2))
        if len(databases) > 1: raise NotImplementedError
        database = databases.pop()
        trans.remove_m2m(database, modified_m2m)
        for obj in trans.to_be_checked: obj._save_()
        trans.add_m2m(database, modified_m2m)
        database.commit()
    def remove_m2m(trans, database, modified_m2m):
        for attr, (added, removed) in modified_m2m.iteritems():
            reverse = attr.reverse
            table = attr.table
            assert table is not None
            criteria_list = [ AND ]
            for i, column in enumerate(reverse.columns + attr.columns):
                criteria_list.append([ EQ, [COLUMN, None, column], [PARAM, i] ])
            sql_ast = [ DELETE, table, [ WHERE, criteria_list ] ]
            for obj, robj in removed:
                params = []
                if not obj._pk_is_composite_: params.append(obj._pkval_)
                else: params.extend(obj._pkval_)
                if not robj._pk_is_composite_: params.append(robj._pkval_)
                else: params.extend(robj._pkval_)
                database._exec_ast(sql_ast, params)
    def add_m2m(trans, database, modified_m2m):
        for attr, (added, removed) in modified_m2m.iteritems():
            reverse = attr.reverse
            table = attr.table
            assert table is not None
            columns = []
            values = []
            for i, column in enumerate(reverse.columns + attr.columns):
                columns.append(column)
                values.append([PARAM, i])
            sql_ast = [ INSERT, table, columns, values ]
            for obj, robj in added:
                params = []
                if not obj._pk_is_composite_: params.append(obj._pkval_)
                else: params.extend(obj._pkval_)
                if not robj._pk_is_composite_: params.append(robj._pkval_)
                else: params.extend(robj._pkval_)
                database._exec_ast(sql_ast, params)
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
    def db_update_simple_index(trans, obj, attr, prev, val):
        index = trans.indexes.get(attr)
        if index is None: index = trans.indexes[attr] = {}
        if val is None or trans.ignore_none: pass
        else:
            obj2 = index.setdefault(val, obj)
            if obj2 is not obj: raise IntegrityError(
                '%s with unique index %s.%s already exists: %s'
                % (obj2.__class__.__name__, obj.__class__.__name__, attr.name, new_keyval))
                # attribute which was created or updated lately clashes with one stored in database
        index.pop(prev, None)
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
    def db_update_composite_index(trans, obj, attrs, prevs, vals):
        index = trans.indexes.get(attrs)
        if index is None: index = trans.indexes[attrs] = {}
        if NOT_LOADED in vals: pass
        elif None in vals and trans.ignore_none: pass
        else:
            obj2 = index.setdefault(vals, obj)
            if obj2 is not obj:
                key_str = ', '.join(repr(item) for item in new_keyval)
                raise IntegrityError('%s with unique index %s.%s already exists: %s'
                                     % (obj2.__class__.__name__, obj.__class__.__name__, attr.name, key_str))
        index.pop(prevs, None)

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
    def add_column(table, col_name, pk):
        if col_name in table.column_dict:
            raise MappingError('Column %s in table %s was already mapped' % (col_name, table.name))
        column = Column(table, col_name, pk)
        table.column_list.append(column)
        table.column_dict[col_name] = column
        return column

class Column(object):
    def __init__(column, table, name, pk=False):
        column.table = table
        column.name = name
        column.pk = pk
        # column.sql_type = attr.sql_type
    def __repr__(column):
        if column.pk: return '<Column(%s.%s) PK>' % (column.table.name, column.name)
        return '<Column(%s.%s)>' % (column.table.name, column.name)
