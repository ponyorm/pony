import sys, threading
from operator import attrgetter
from itertools import count, ifilter, ifilterfalse, izip

try: from pony.thirdparty import etree
except ImportError: etree = None

from pony import options, dbschema, dbapiprovider
from pony.sqlsymbols import *

class OrmError(Exception): pass

class DiagramError(OrmError): pass
class SchemaError(OrmError): pass
class MappingError(OrmError): pass
class ConstraintError(OrmError): pass
class IndexError(OrmError): pass
class ObjectNotFound(OrmError):
    def __init__(exc, entity, pkval):
        if len(pkval) == 1:
            pkval = pkval[0]
            msg = '%s(%r)' % (entity.__name__, pkval)
        else: msg = '%s%r' % (entity.__name__, pkval)
        OrmError.__init__(exc, msg)
        exc.entity = entity
        exc.pkval = pkval

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

class DescWrapper(object):
    def __init__(self, attr):
        self.attr = attr
    def __repr__(self):
        return '<DescWrapper(%s)>' % self.attr

next_attr_id = count(1).next

class Attribute(object):
    __slots__ = 'is_required', 'is_unique', 'is_indexed', 'is_pk', 'is_collection', \
                'id', 'pk_offset', 'py_type', 'sql_type', 'entity', 'name', 'oldname', \
                'args', 'auto', 'default', 'reverse', 'composite_keys', \
                'column', 'columns', 'col_paths', '_columns_checked', 'converters', 'keyargs'
    def __init__(attr, py_type, *args, **keyargs):
        if attr.__class__ is Attribute: raise TypeError("'Attribute' is abstract type")
        attr.is_required = isinstance(attr, Required)
        attr.is_unique = isinstance(attr, Unique)  # Also can be set to True later
        attr.is_indexed = attr.is_unique  # Also can be set to True later
        attr.is_collection = isinstance(attr, Collection)
        attr.is_pk = isinstance(attr, PrimaryKey)
        if attr.is_pk: attr.pk_offset = 0
        else: attr.pk_offset = None
        attr.id = next_attr_id()
        if not isinstance(py_type, basestring) and not isinstance(py_type, type):
            raise TypeError('Incorrect type of attribute: %r' % py_type)
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
            raise TypeError('Reverse option cannot be set for this type: %r' % attr.py_type)

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
                    raise TypeError("Items of parameter 'columns' must be strings. Got: %r" % attr.columns)
            if len(attr.columns) == 1: attr.column = attr.columns[0]
        else: attr.columns = []
        attr.col_paths = []
        attr._columns_checked = False
        attr.composite_keys = []
        attr.keyargs = keyargs
        attr.converters = []
    def _init_(attr, entity, name):
        attr.entity = entity
        attr.name = name
    def __repr__(attr):
        owner_name = not attr.entity and '?' or attr.entity.__name__
        return '%s.%s' % (owner_name, attr.name or '?')
    def check(attr, val, obj=None, entity=None, from_db=False):
        assert val is not NOT_LOADED
        if entity is not None: pass
        elif obj is not None: entity = obj.__class__
        else: entity = attr.entity
        if val is DEFAULT:
            val = attr.default
            if val is None:
                if attr.is_required and not attr.auto: raise ConstraintError(
                    'Required attribute %s.%s does not specified' % (entity.__name__, attr.name))
                return val
        elif val is None:
            if attr.is_required:
                if obj is None: raise ConstraintError(
                    'Required attribute %s.%s cannot be set to None' % (entity.__name__, attr.name))
                else: raise ConstraintError(
                    'Required attribute %s.%s for %r cannot be set to None' % (entity.__name__, attr.name, obj))
            return val
        reverse = attr.reverse
        if not reverse:
            if isinstance(val, attr.py_type): return val
            elif isinstance(val, Entity): raise TypeError(
                'Attribute %s.%s must be of %s type. Got: %s'
                % (attr.entity.__name__, attr.name, attr.py_type.__name__, val))
            if attr.converters:
                assert len(attr.converters) == 1
                converter = attr.converters[0]
                if converter is not None:
                    if from_db: return converter.sql2py(val)
                    else: return converter.validate(val)
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
            objects = reverse.entity._find_in_db_({reverse : obj}, 1)
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
        val = attr.check(val, obj, from_db=False)
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
        old = attr.check(old, obj, from_db=True)
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
    def get_raw_values(attr, val):
        reverse = attr.reverse
        if not reverse: return (val,)
        rentity = reverse.entity
        if val is None: return rentity._pk_nones_
        return val._get_raw_pkval_()
    def get_columns(attr):
        assert not attr.is_collection
        assert not isinstance(attr.py_type, basestring)
        if attr._columns_checked: return attr.columns

        provider = attr.entity._diagram_.database.provider
        reverse = attr.reverse
        if not reverse: # attr is not part of relationship
            if not attr.columns: attr.columns = [ attr.name ]
            elif len(attr.columns) > 1: raise MappingError("Too many columns were specified for %s" % attr)
            attr.col_paths = [ attr.name ]
            attr.converters = [ provider.get_converter_by_attr(attr) ]
        else:
            def generate_columns():
                reverse_pk_columns = reverse.entity._get_pk_columns_()
                reverse_pk_col_paths = reverse.entity._pk_paths_
                if not attr.columns:
                    if len(reverse_pk_columns) == 1: attr.columns = [ attr.name ]
                    else:
                        prefix = attr.name + '_'
                        attr.columns = [ prefix + column for column in reverse_pk_columns ]
                elif len(attr.columns) != len(reverse_pk_columns): raise MappingError(
                    'Invalid number of columns specified for %s' % attr)
                attr.col_paths = [ '-'.join((attr.name, paths)) for paths in reverse_pk_col_paths ]
                attr.converters = []
                for a in reverse.entity._pk_attrs_:
                    attr.converters.extend(a.converters)

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
    @property
    def asc(attr):
        return attr
    @property
    def desc(attr):
        return DescWrapper(attr)

class Optional(Attribute):
    __slots__ = []
    
class Required(Attribute):
    __slots__ = []

class Unique(Required):
    __slots__ = []
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

def populate_criteria_list(criteria_list, columns, converters, params_count=0, table_alias=None):
    assert len(columns) == len(converters)
    for column, converter in zip(columns, converters):
        criteria_list.append([EQ, [ COLUMN, table_alias, column ], [ PARAM, params_count, converter ] ])
        params_count += 1
    return params_count

class PrimaryKey(Unique):
    __slots__ = []


class Collection(Attribute):
    __slots__ = 'table', 'cached_load_sql', 'cached_add_m2m_sql', 'cached_remove_m2m_sql', 'wrapper_class'
    def __init__(attr, py_type, *args, **keyargs):
        if attr.__class__ is Collection: raise TypeError("'Collection' is abstract type")
        table = keyargs.pop('table', None)  # TODO: rename table to link_table or m2m_table
        if table is not None and not isinstance(table, basestring):
            raise TypeError("Parameter 'table' must be a string. Got: %r" % table)
        attr.table = table
        Attribute.__init__(attr, py_type, *args, **keyargs)
        if attr.default is not None: raise TypeError('default value could not be set for collection attribute')
        if attr.auto: raise TypeError("'auto' option could not be set for collection attribute")

        attr.cached_load_sql = None
        attr.cached_add_m2m_sql = None
        attr.cached_remove_m2m_sql = None
    def load(attr, obj):
        assert False, 'Abstract method'
    def __get__(attr, obj, cls=None):
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
    __slots__ = []
    def check(attr, val, obj=None, entity=None, from_db=False):
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
        if setdata is NOT_LOADED: setdata = obj.__dict__[attr] = SetData()
        if not reverse.is_collection:
            reverse.entity._find_(None, (), {reverse.name:obj})
        else:
            database = obj._diagram_.database
            if attr.cached_load_sql is None:
                sql_ast = attr.construct_sql_m2m()
                sql, adapter = database._ast2sql(sql_ast)
                attr.cached_load_sql = sql, adapter
            else: sql, adapter = attr.cached_load_sql
            values = obj._get_raw_pkval_()
            arguments = adapter(values)
            cursor = database._exec_sql(sql, arguments)
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
    def construct_sql_m2m(attr):
        reverse = attr.reverse
        assert reverse is not None and reverse.is_collection and issubclass(reverse.py_type, Entity)
        table_name = attr.table
        assert table_name is not None
        select_list = [ ALL ]
        for column in attr.columns:
            select_list.append([COLUMN, 'T1', column ])
        from_list = [ FROM, [ 'T1', TABLE, table_name ]]
        criteria_list = [ AND ]
        assert len(reverse.columns) == len(reverse.converters)
        for i, (column, converter) in enumerate(zip(reverse.columns, reverse.converters)):
            criteria_list.append([EQ, [COLUMN, 'T1', column], [ PARAM, i, converter ]])
        sql_ast = [ SELECT, select_list, from_list, [ WHERE, criteria_list ] ]
        return sql_ast
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
    def __get__(attr, obj, cls=None):
        if obj is None: return attr
        if obj._status_ in ('deleted', 'cancelled'): raise OperationWithDeletedObjectError('%s was deleted' % obj)
        rentity = attr.py_type
        wrapper_class = rentity._get_set_wrapper_subclass_()
        return wrapper_class(obj, attr)
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
            else: setdata = attr.load(obj)
        elif not setdata.is_fully_loaded: setdata = attr.load(obj)
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
        if attr._columns_checked: return attr.reverse.columns
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
        reverse.converters = entity._pk_converters_
        attr._columns_checked = True
        return reverse.columns
    def remove_m2m(attr, removed):
        entity = attr.entity
        database = entity._diagram_.database
        cached_sql = attr.cached_remove_m2m_sql
        if cached_sql is None:
            reverse = attr.reverse
            table = attr.table
            assert table is not None
            criteria_list = [ AND ]
            for i, (column, converter) in enumerate(zip(reverse.columns + attr.columns, reverse.converters + attr.converters)):
                criteria_list.append([ EQ, [COLUMN, None, column], [ PARAM, i, converter ] ])
            sql_ast = [ DELETE, table, [ WHERE, criteria_list ] ]
            sql, adapter = database._ast2sql(sql_ast)
            attr.cached_remove_m2m_sql = sql, adapter
        else: sql, adapter = cached_sql
        arguments_list = [ adapter(obj._get_raw_pkval_() + robj._get_raw_pkval_())
                           for obj, robj in removed ]
        database.exec_sql_many(sql, arguments_list)
    def add_m2m(attr, added):
        entity = attr.entity
        database = entity._diagram_.database
        cached_sql = attr.cached_add_m2m_sql
        if cached_sql is None:
            reverse = attr.reverse
            table = attr.table
            assert table is not None
            columns = []
            params = []
            for i, (column, converter) in enumerate(zip(reverse.columns + attr.columns, reverse.converters + attr.converters)):
                columns.append(column)
                params.append([PARAM, i, converter])
            sql_ast = [ INSERT, table, columns, params ]
            sql, adapter = database._ast2sql(sql_ast)
            attr.cached_add_m2m_sql = sql, adapter
        else: sql, adapter = cached_sql
        arguments_list = [ adapter(obj._get_raw_pkval_() + robj._get_raw_pkval_())
                           for obj, robj in added ]
        database.exec_sql_many(sql, arguments_list)

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
            if wrapper._obj_ is x._obj_ and wrapper._attr_ is x._attr_: return True
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

class PropagatedSet(object):
    __slots__ = [ '_items_' ]
    def __init__(pset, items):
        pset._items_ = frozenset(items)
    def __repr__(pset):
        s = ', '.join(map(repr, sorted(pset._items_)))
        return '%s([%s])' % (pset.__class__.__name__, s)
    def __nonzero__(pset):
        return bool(pset._items_)
    def __len__(pset):
        return len(pset._items_)
    def __iter__(pset):
        return iter(pset._items_)
    def __eq__(pset, x):
        if isinstance(x, PropagatedSet):
            return pset._items_ == x._items_
        if isinstance(x, (set, frozenset)):
            return pset._items_ == x
        return pset._items_ == frozenset(x)
    def __ne__(pset, x):
        return not pset.__eq__(x)
    def __contains__(pset, item):
        return item in pset._items_

##class List(Collection): pass
##class Dict(Collection): pass
##class Relation(Collection): pass

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
        return EntityIter(entity)

class EntityIter(object):
    def __init__(self, entity):
        self.entity = entity
    def next(self):
        raise StopIteration
    
next_entity_id = count(1).next
next_new_instance_id = count(1).next

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
            if name in base_attrs_dict: raise DiagramError("Name '%s' hides base attribute %s" % (name,base_attrs_dict[name]))
            if not isinstance(attr, Attribute): continue
            if name.startswith('_') and name.endswith('_'): raise DiagramError(
                'Attribute name cannot both starts and ends with underscore. Got: %s' % name)
            if attr.entity is not None: raise DiagramError(
                'Duplicate use of attribute %s in entity %s' % (attr, entity.__name__))
            attr._init_(entity, name)
            new_attrs.append(attr)
        new_attrs.sort(key=attrgetter('id'))

        keys = entity.__dict__.get('_keys_', {})
        for key in keys:
            for attr in key:
                assert isinstance(attr, Attribute) and not attr.is_collection
                if attr.entity is not entity: raise DiagramError(
                    'Invalid use of attribute %s in entity %s' % (attr, entity.__name__))
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

        entity._bits_ = {}
        next_offset = count().next
        for attr in entity._attrs_:
            if attr.is_collection or attr.pk_offset is not None: continue
            entity._bits_[attr] = 1 << next_offset()

        try: table_name = entity.__dict__['_table_']
        except KeyError: entity._table_ = None
        else:
            if not isinstance(table_name, basestring): raise TypeError(
                '%s._table_ property must be a string. Got: %r' % (entity.__name__, table_name))

        entity._diagram_ = diagram
        diagram.entities[entity.__name__] = entity
        entity._link_reverse_attrs_()

        entity._cached_create_sql_ = None
        entity._cached_delete_sql_ = None
        entity._find_sql_cache_ = {}
        entity._update_sql_cache_ = {}
        entity._lock_sql_cache_ = {}

        entity._propagation_mixin_ = None
        entity._set_wrapper_subclass_ = None
        entity._propagated_set_subclass_ = None
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
                if attr2.entity is not entity2: raise DiagramError('Incorrect reverse attribute %s used in %s' % (attr2, attr)) ###
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
    def __new__(entity, *args):
        obj = entity.find_one(*args)
        if obj is None: raise ObjectNotFound(entity, args)
        return obj
    @classmethod
    def _get_pk_columns_(entity):
        if entity._pk_columns_ is not None: return entity._pk_columns_
        pk_columns = []
        pk_converters = []
        pk_paths = []
        for attr in entity._pk_attrs_:
            attr_columns = attr.get_columns()
            attr_col_paths = attr.col_paths
            pk_columns.extend(attr_columns)
            pk_converters.extend(attr.converters)
            pk_paths.extend(attr_col_paths)
        entity._pk_columns_ = pk_columns
        entity._pk_converters_ = pk_converters
        entity._pk_nones_ = (None,) * len(pk_columns)
        entity._pk_paths_ = pk_paths
        return pk_columns
    def _get_raw_pkval_(obj):
        pkval = obj._pkval_
        if not obj._pk_is_composite_:
            if not obj.__class__._pk_.reverse: return (pkval,)
            else: return pkval._get_raw_pkval_()
        raw_pkval = []
        append = raw_pkval.append
        for attr, val in zip(obj._pk_attrs_, pkval):
            if not attr.reverse: append(val)
            else: raw_pkval += val._get_raw_pkval_()
        return tuple(raw_pkval)
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
        if pkval is not None:
            index[pkval] = obj
            obj._newid_ = None
        else: obj._newid_ = next_new_instance_id()
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
                if not attr.reverse: val = attr.check(val, None, entity, from_db=True)
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
                if val is None: continue
                reverse = attr.reverse
                if reverse and not reverse.is_collection:
                    obj = reverse.__get__(val)
                    break
        if obj is None:
            for attr, val in avdict.iteritems():
                if isinstance(val, Entity) and val._pkval_ is None:
                    reverse = attr.reverse
                    if not reverse.is_collection:
                        obj = reverse.__get__(val)
                        if obj is None: return []
                    elif isinstance(reverse, Set):
                        filtered_objects = []
                        for obj in reverse.__get__(val):
                            for attr, val in avdict.iteritems():
                                if val != attr.get(obj): break
                            else: filtered_objects.append(obj)
                        return filtered_objects
                    else: raise NotImplementedError
        if obj is not None:
            for attr, val in avdict.iteritems():
                if val != attr.__get__(obj): return []
            return [ obj ]
        raise KeyError
    def _load_(obj):
        objects = obj._find_in_db_({obj.__class__._pk_ :obj._pkval_})
        if not objects: raise UnrepeatableReadError('%s disappeared' % obj)
        assert len(objects) == 1 and obj == objects[0]
    @classmethod
    def _construct_select_clause_(entity, alias=None, distinct=False):
        table_name = entity._table_
        attr_offsets = {}
        if distinct: select_list = [ DISTINCT ]
        else: select_list = [ ALL ]
        for attr in entity._attrs_:
            if attr.is_collection: continue
            if not attr.columns: continue
            attr_offsets[attr] = len(select_list) - 1
            for column in attr.columns:
                select_list.append([ COLUMN, alias, column ])
        return select_list, attr_offsets
    @classmethod
    def _construct_sql_(entity, query_key):
        table_name = entity._table_
        select_list, attr_offsets = entity._construct_select_clause_()
        from_list = [ FROM, [ None, TABLE, table_name ]]

        criteria_list = [ AND ]
        values = []
        extractors = {}
        for attr, attr_is_none in query_key:
            if not attr.reverse:
                if not attr_is_none:
                    assert len(attr.converters) == 1
                    criteria_list.append([EQ, [COLUMN, None, attr.column], [ PARAM, attr.name, attr.converters[0] ]])
                    extractors[attr.name] = lambda avdict, attr=attr: avdict[attr]
                else: criteria_list.append([IS_NULL, [COLUMN, None, attr.column]])
            elif not attr.columns: raise NotImplementedError
            else:
                attr_entity = attr.py_type
                assert attr_entity == attr.reverse.entity
                if len(attr_entity._pk_columns_) == 1:
                    if not attr_is_none:
                        assert len(attr.converters) == 1
                        criteria_list.append([EQ, [COLUMN, None, attr.column], [ PARAM, attr.name, attr.converters[0] ]])
                        extractors[attr.name] = lambda avdict, attr=attr: avdict[attr]._get_raw_pkval_()[0]
                    else: criteria_list.append([IS_NULL, [COLUMN, None, attr.column]])
                elif not attr_is_none:
                    for i, (column, converter) in enumerate(zip(attr_entity._pk_columns_, attr_entity._pk_converters_)):
                        param_name = '%s-%d' % (attr.name, i+1)
                        criteria_list.append([EQ, [COLUMN, None, column], [ PARAM, param_name, converter ]])
                        extractors[param_name] = lambda avdict, attr=attr, i=i: avdict[attr]._get_raw_pkval_()[i]
                else:
                    for column in attr_entity._pk_columns_:
                        criteria_list.append([IS_NULL, [COLUMN, None, column]])

        sql_ast = [ SELECT, select_list, from_list ]
        if len(criteria_list) > 1:
            sql_ast.append([ WHERE, criteria_list  ])
        def extractor(avdict):
            param_dict = {}
            for param, extractor in extractors.iteritems():
                param_dict[param] = extractor(avdict)
            return param_dict
        return sql_ast, extractor, attr_offsets
    @classmethod
    def _find_in_db_(entity, avdict, max_rows_count=None):
        if avdict is None: query_key = None
        else: query_key = tuple((attr, value is None) for attr, value in sorted(avdict.iteritems()))
        database = entity._diagram_.database
        cached_sql = entity._find_sql_cache_.get(query_key)
        if cached_sql is None:
            sql_ast, extractor, attr_offsets = entity._construct_sql_(query_key)
            sql, adapter = database._ast2sql(sql_ast)
            cached_sql = sql, extractor, adapter, attr_offsets
            entity._find_sql_cache_[query_key] = cached_sql
        else: sql, extractor, adapter, attr_offsets = cached_sql
        value_dict = extractor(avdict)
        arguments = adapter(value_dict)
        cursor = database._exec_sql(sql, arguments)
        objects = entity._fetch_objects(cursor, attr_offsets)
        return objects
    @classmethod
    def _fetch_objects(entity, cursor, attr_offsets, max_rows_count=None):
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
    def _parse_row_(entity, row, attr_offsets):
        avdict = {}
        for attr, i in attr_offsets.iteritems():
            if attr.column is not None:
                val = row[i]
                if not attr.reverse:  val = attr.check(val, None, entity, from_db=True)
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
    def _find_(entity, max_objects_count, args, keyargs):
        pkval, avdict = entity._normalize_args_(args, keyargs, False)
        for attr in avdict:
            if attr.is_collection: raise TypeError(
                'Collection attribute %s.%s cannot be specified as search criteria' % (attr.entity.__name__, attr.name))
        try:
            objects = entity._find_in_cache_(pkval, avdict)
        except KeyError:
            objects = entity._find_in_db_(avdict, max_objects_count)
        return objects        
    @classmethod
    def find_one(entity, *args, **keyargs):
        objects = entity._find_(1, args, keyargs)
        if not objects: return None
        if len(objects) > 1: raise MultipleObjectsFoundError(
            'Multiple objects was found. Use %s.find_all(...) instead of %s.find(...) to retrieve them'
            % (entity.__name__, entity.__name__))
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
                    if reverse.is_required and attr.__get__(obj).__nonzero__(): raise ConstraintError(
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
                avdict[attr] = attr.check(val, None, entity, from_db=False)
        else:
            get = entity._adict_.get 
            for name, val in keyargs.items():
                attr = get(name)
                if attr is None: raise TypeError('Unknown attribute %r' % name)
                avdict[attr] = attr.check(val, None, entity, from_db=False)
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
            val = attr.check(val, obj, from_db=False)
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
    @classmethod
    def _attrs_with_bit_(entity, mask=-1):
        get_bit = entity._bits_.get
        for attr in entity._attrs_:
            bit = get_bit(attr)
            if bit is None: continue
            if not bit & mask: continue
            yield attr
    def _save_principal_objects_(obj, dependent_objects):
        if dependent_objects is None: dependent_objects = []
        elif obj in dependent_objects:
            chain = ' -> '.join(obj2.__class__.__name__ for obj2 in dependent_objects)
            raise UnresolvableCyclicDependency('Cannot save cyclic chain: ' + chain)
        dependent_objects.append(obj)
        status = obj._status_
        if status == 'created': attr_iter = obj._attrs_with_bit_()
        elif status == 'updated': attr_iter = obj._attrs_with_bit_(obj._wbits_)
        else: assert False
        for attr in attr_iter:
            val = obj.__dict__[attr]
            if not attr.reverse: continue
            if val is None: continue
            if val._status_ == 'created':
                val._save_(dependent_objects)
                assert val._status_ == 'saved'
    def _save_created_(obj):
        values = []
        for attr in obj._attrs_:
            if not attr.columns: continue
            if attr.is_collection: continue
            val = obj.__dict__[attr]
            values.extend(attr.get_raw_values(val))
        database = obj._diagram_.database
        if obj._cached_create_sql_ is None:
            columns = obj._columns_
            converters = obj._converters_
            assert len(columns) == len(converters)
            params = [ [ PARAM, i,  converter ] for i, converter in enumerate(converters) ]
            sql_ast = [ INSERT, obj._table_, columns, params ]
            sql, adapter = database._ast2sql(sql_ast)
            obj.__class__._cached_create_sql_ = sql, adapter
        else: sql, adapter = obj._cached_create_sql_
        arguments = adapter(values)
        try:
            cursor = database._exec_sql(sql, arguments)
        except database.IntegrityError, e:
            raise IntegrityError('Object %r cannot be stored in the database (probably it already exists). DB message: %s' % (obj, e.args[0]))
        except database.DatabaseError, e:
            raise UnexpectedError('Object %r cannot be stored in the database. DB message: %s' % (obj, e.args[0]))

        if obj._pkval_ is None:
            rowid = cursor.lastrowid # TODO
            pk_attr = obj.__class__._pk_
            index = obj._trans_.indexes.setdefault(pk_attr, {})
            obj2 = index.setdefault(rowid, obj)
            if obj2 is not obj: raise IntegrityError(
                'Newly auto-generated rowid value %s was already used in transaction cache for another object' % rowid)
            obj._pkval_ = obj.__dict__[pk_attr] = rowid
            obj._newid_ = None
            
        obj._status_ = 'saved'
        obj._rbits_ = 0
        obj._wbits_ = 0
        bits = obj._bits_
        for attr in obj._attrs_:
            if attr not in bits: continue
            obj.__dict__[attr.name] = obj.__dict__[attr]
    def _save_updated_(obj):
        update_columns = []
        values = []
        for attr in obj._attrs_with_bit_(obj._wbits_):
            if not attr.columns: continue
            update_columns.extend(attr.columns)
            val = obj.__dict__[attr]
            values.extend(attr.get_raw_values(val))
        for attr in obj._pk_attrs_:
            val = obj.__dict__[attr]
            values.extend(attr.get_raw_values(val))
        optimistic_check_columns = []
        optimistic_check_converters = []
        for attr in obj._attrs_with_bit_(obj._rbits_):
            if not attr.columns: continue
            old = obj.__dict__.get(attr.name, NOT_LOADED)
            assert old is not NOT_LOADED
            optimistic_check_columns.extend(attr.columns)
            optimistic_check_converters.extend(attr.converters)
            values.extend(attr.get_raw_values(old))
        query_key = (tuple(update_columns), tuple(optimistic_check_columns))
        database = obj._diagram_.database
        cached_sql = obj._update_sql_cache_.get(query_key)
        if cached_sql is None:
            update_converters = []
            for attr in obj._attrs_with_bit_(obj._wbits_):
                if not attr.columns: continue
                update_converters.extend(attr.converters)
            assert len(update_columns) == len(update_converters)
            update_params = [ [ PARAM, i, converter ] for i, converter in enumerate(update_converters) ]
            params_count = len(update_params)
            criteria_list = [ AND ]
            pk_columns = obj._pk_columns_
            pk_converters = obj._pk_converters_
            params_count = populate_criteria_list(criteria_list, pk_columns, pk_converters, params_count)
            populate_criteria_list(criteria_list, optimistic_check_columns, optimistic_check_converters, params_count)
            sql_ast = [ UPDATE, obj._table_, zip(update_columns, update_params), [ WHERE, criteria_list ] ]
            sql, adapter = database._ast2sql(sql_ast)
            obj._update_sql_cache_[query_key] = sql, adapter
        else: sql, adapter = cached_sql
        arguments = adapter(values)
        cursor = database._exec_sql(sql, arguments)
        if cursor.rowcount != 1:
            raise UnrepeatableReadError('Object %r was updated outside of current transaction' % obj)
        obj._status_ = 'saved'
        obj._rbits_ = 0
        obj._wbits_ = 0
        for attr in obj._attrs_with_bit_():
            val = obj.__dict__.get(attr, NOT_LOADED)
            if val is NOT_LOADED: assert attr.name not in obj.__dict__
            else: obj.__dict__[attr.name] = val
    def _save_locked_(obj):
        assert obj._wbits_ == 0
        values = []
        for attr in obj._pk_attrs_:
            val = obj.__dict__[attr]
            values.extend(attr.get_raw_values(val))
        optimistic_check_columns = []
        optimistic_check_converters = []
        for attr in obj._attrs_with_bit_(obj._rbits_):
            if not attr.columns: continue
            old = obj.__dict__.get(attr.name, NOT_LOADED)
            assert old is not NOT_LOADED
            optimistic_check_columns.extend(attr.columns)
            optimistic_check_converters.extend(attr.converters)
            values.extend(attr.get_raw_values(old))
        query_key = tuple(optimistic_check_columns)
        database = obj._diagram_.database
        cached_sql = obj._lock_sql_cache_.get(query_key)        
        if cached_sql is None:
            criteria_list = [ AND ]
            params_count = populate_criteria_list(criteria_list, obj._pk_columns_, obj._pk_converters_)
            populate_criteria_list(criteria_list, optimistic_check_columns, optimistic_check_converters, params_count)
            sql_ast = [ SELECT, [ ALL, [ VALUE, 1 ]], [ FROM, [ None, TABLE, obj._table_ ] ], [ WHERE, criteria_list ] ]
            sql, adapter = database._ast2sql(sql_ast)
            obj._lock_sql_cache_[query_key] = sql, adapter
        else: sql, adapter = cached_sql
        arguments = adapter(values)
        cursor = database._exec_sql(sql, arguments)
        row = cursor.fetchone()
        if row is None: raise UnrepeatableReadError('Object %r was updated outside of current transaction' % obj)
        obj._status_ = 'loaded'
    def _save_deleted_(obj):
        database = obj._diagram_.database
        cached_sql = obj._cached_delete_sql_
        if cached_sql is None:
            criteria_list = [ AND ]
            populate_criteria_list(criteria_list, obj._pk_columns_, obj._pk_converters_)
            sql_ast = [ DELETE, obj._table_, [ WHERE, criteria_list ] ]
            sql, adapter = database._ast2sql(sql_ast)
            obj.__class__._cached_delete_sql_ = sql, adapter
        else: sql, adapter = cached_sql
        values = obj._get_raw_pkval_()
        arguments = adapter(values)
        database._exec_sql(sql, arguments)
    def _save_(obj, dependent_objects=None):
        status = obj._status_
        if status in ('loaded', 'saved', 'cancelled'): return
        if status in ('created', 'updated'):
            obj._save_principal_objects_(dependent_objects)

        if status == 'created': obj._save_created_()
        elif status == 'updated': obj._save_updated_()
        elif status == 'deleted': obj._save_deleted_()
        elif status == 'locked': obj._save_locked_()
        else: assert False
    @classmethod
    def _get_propagation_mixin_(entity):
        mixin = entity._propagation_mixin_
        if mixin is not None: return mixin
        cls_dict = { '_entity_' : entity }
        for attr in entity._attrs_:
            if not attr.reverse:
                def fget(wrapper, attr=attr):
                    return set(attr.__get__(item) for item in wrapper)
            elif not attr.is_collection:
                def fget(wrapper, attr=attr):
                    rentity = attr.py_type
                    cls = rentity._get_propagated_set_subclass_()
                    print cls
                    return cls(attr.__get__(item) for item in wrapper)
            else:
                def fget(wrapper, attr=attr):
                    rentity = attr.py_type
                    cls = rentity._get_propagated_set_subclass_()
                    result_items = set()
                    for item in wrapper:
                        result_items.update(attr.__get__(item))
                    return cls(result_items)
            cls_dict[attr.name] = property(fget)
        result_cls_name = entity.__name__ + 'SetMixin'
        result_cls = type(result_cls_name, (object,), cls_dict)
        entity._propagation_mixin_ = result_cls
        return result_cls
    @classmethod
    def _get_propagated_set_subclass_(entity):
        result_cls = entity._propagated_set_subclass_
        if result_cls is None:
            mixin = entity._get_propagation_mixin_()
            cls_name = entity.__name__ + 'PropagatedSet'
            result_cls = type(cls_name, (PropagatedSet, mixin), {})
            entity._propagated_set_subclass_ = result_cls
        return result_cls
    @classmethod
    def _get_set_wrapper_subclass_(entity):
        result_cls = entity._set_wrapper_subclass_
        if result_cls is None:
            mixin = entity._get_propagation_mixin_()
            cls_name = entity.__name__ + 'SetWrapper'
            result_cls = type(cls_name, (SetWrapper, mixin), {})
            entity._set_wrapper_subclass_ = result_cls
        return result_cls

class Diagram(object):
    def __init__(diagram):
        diagram.entities = {}
        diagram.unmapped_attrs = {}
        diagram.schema = None
        diagram.database = None
    def generate_mapping(diagram, database, filename=None, check_tables=False, create_tables=False):
        if create_tables and check_tables: raise TypeError(
            "Parameters 'check_tables' and 'create_tables' cannot be set to True at the same time")

        def get_columns(table, column_names):
            return tuple(map(table.column_dict.__getitem__, column_names))

        diagram.database = database
        if diagram.schema: raise MappingError('Mapping was already generated')
        if filename is not None: raise NotImplementedError
        for entity_name in diagram.unmapped_attrs:
            raise DiagramError('Entity definition %s was not found' % entity_name)

        schema = diagram.schema = dbschema.DBSchema(database)
        foreign_keys = []
        entities = list(sorted(diagram.entities.values(), key=attrgetter('_id_')))
        for entity in entities:
            entity._get_pk_columns_()
            table_name = entity._table_
            if table_name is None: table_name = entity._table_ = entity.__name__
            else: assert isinstance(table_name, basestring)
            table = schema.tables.get(table_name)
            if table is None: table = dbschema.Table(table_name, schema)
            elif table.entities: raise NotImplementedError
            table.entities.add(entity)

            if entity._base_attrs_: raise NotImplementedError
            for attr in entity._new_attrs_:
                if attr.is_collection:
                    if not isinstance(attr, Set): raise NotImplementedError
                    reverse = attr.reverse
                    if not reverse.is_collection: # many-to-one:
                        if attr.table is not None: raise MappingError(
                            "Parameter 'table' is not allowed for many-to-one attribute %s" % attr)
                        elif attr.columns: raise NotImplementedError(
                            "Parameter 'column' is not allowed for many-to-one attribute %s" % attr)
                        continue
                    # many-to-many:
                    if not isinstance(reverse, Set): raise NotImplementedError
                    if attr.entity.__name__ >= reverse.entity.__name__: continue
                    if attr.table:
                        if reverse.table != attr.table: raise MappingError(
                            "Parameter 'table' for %s and %s do not match" % (attr, reverse))
                        table_name = attr.table
                    else:
                        table_name = attr.entity.__name__ + '_' + reverse.entity.__name__
                        attr.table = reverse.table = table_name
                    m2m_table = schema.tables.get(table_name)
                    if m2m_table is not None:
                        if m2m_table.entities or m2m_table.m2m: raise MappingError(
                            "Table name '%s' is already in use" % table_name)
                        raise NotImplementedError
                    m2m_table = dbschema.Table(table_name, schema)
                    m2m_columns_1 = attr.get_m2m_columns()
                    m2m_columns_2 = reverse.get_m2m_columns()
                    assert len(m2m_columns_1) == len(reverse.converters)
                    assert len(m2m_columns_2) == len(attr.converters)
                    for column_name, converter in zip(m2m_columns_1 + m2m_columns_2, reverse.converters + attr.converters):
                        dbschema.Column(column_name, m2m_table, converter.sql_type(), True)
                    dbschema.Index(None, m2m_table, tuple(m2m_table.column_list), is_pk=True)
                    m2m_table.m2m.add(attr)
                    m2m_table.m2m.add(reverse)
                else:
                    columns = attr.get_columns()
                    if not attr.reverse and attr.default is not None:
                        assert len(attr.converters) == 1
                        attr.converters[0].validate(attr.default)
                    assert len(columns) == len(attr.converters)
                    for (column_name, converter) in zip(columns, attr.converters):
                        dbschema.Column(column_name, table, converter.sql_type(), attr.is_required)
            dbschema.Index(None, table, get_columns(table, entity._pk_columns_), is_pk=True)
            for key in entity._keys_:
                column_names = []
                for attr in key: column_names.extend(attr.columns)
                dbschema.Index(None, table, get_columns(table, column_names), is_unique=True)
            columns = []
            converters = []
            for attr in entity._attrs_:
                if attr.is_collection: continue
                columns.extend(attr.columns)  # todo: inheritance
                converters.extend(attr.converters)
            entity._columns_ = columns
            entity._converters_ = converters
        for entity in entities:
            table = schema.tables[entity._table_]
            for attr in entity._new_attrs_:
                if attr.is_collection:
                    reverse = attr.reverse
                    if not reverse.is_collection: continue
                    if not isinstance(attr, Set): raise NotImplementedError
                    if not isinstance(reverse, Set): raise NotImplementedError
                    m2m_table = schema.tables[attr.table]
                    parent_columns = get_columns(table, entity._pk_columns_)
                    child_columns = get_columns(m2m_table, reverse.columns)
                    dbschema.ForeignKey(None, table, parent_columns, m2m_table, child_columns)
                elif attr.reverse and attr.columns:
                    rentity = attr.reverse.entity
                    parent_table = schema.tables[rentity._table_]
                    parent_columns = get_columns(parent_table, rentity._pk_columns_)
                    child_columns = get_columns(table, attr.columns)
                    dbschema.ForeignKey(None, parent_table, parent_columns, table, child_columns)        

        if create_tables: schema.create_tables()
            
        if not check_tables: return
        for table in schema.tables.values():
            sql_ast = [ SELECT,
                        [ ALL, ] + [ [ COLUMN, table.name, column.name ] for column in table.column_list ],
                        [ FROM, [ table.name, TABLE, table.name ] ],
                        [ WHERE, [ EQ, [ VALUE, 0 ], [ VALUE, 1 ] ] ]
                      ]
            sql, adapter = database._ast2sql(sql_ast)
            database._exec_sql(sql)

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
        trans.remove_m2m(modified_m2m)
        for obj in trans.to_be_checked: obj._save_()
        trans.add_m2m(modified_m2m)
        database = databases.pop()
        database.commit()
    def remove_m2m(trans, modified_m2m):
        for attr, (added, removed) in modified_m2m.iteritems():
            if not removed: continue
            attr.remove_m2m(removed)
    def add_m2m(trans, modified_m2m):
        for attr, (added, removed) in modified_m2m.iteritems():
            if not added: continue
            attr.add_m2m(added)
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
