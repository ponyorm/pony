from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2, items_list, izip, basestring, unicode, buffer, int_types, iteritems

import types, weakref
from decimal import Decimal
from datetime import date, time, datetime, timedelta
from functools import wraps, WRAPPER_ASSIGNMENTS
from uuid import UUID

from pony.utils import throw, parse_expr

NoneType = type(None)

class LongStr(str):
    lazy = True

if PY2:
    class LongUnicode(unicode):
        lazy = True
else:
    LongUnicode = LongStr

class SetType(object):
    __slots__ = 'item_type'
    def __deepcopy__(self, memo):
        return self  # SetType instances are "immutable"
    def __init__(self, item_type):
        self.item_type = item_type
    def __eq__(self, other):
        return type(other) is SetType and self.item_type == other.item_type
    def __ne__(self, other):
        return type(other) is not SetType or self.item_type != other.item_type
    def __hash__(self):
        return hash(self.item_type) + 1

class FuncType(object):
    __slots__ = 'func'
    def __deepcopy__(self, memo):
        return self  # FuncType instances are "immutable"
    def __init__(self, func):
        self.func = func
    def __eq__(self, other):
        return type(other) is FuncType and self.func == other.func
    def __ne__(self, other):
        return type(other) is not FuncType or self.func != other.func
    def __hash__(self):
        return hash(self.func) + 1
    def __repr__(self):
        return 'FuncType(%s at %d)' % (self.func.__name__, id(self.func))

class MethodType(object):
    __slots__ = 'obj', 'func'
    def __deepcopy__(self, memo):
        return self  # MethodType instances are "immutable"
    def __init__(self, method):
        if PY2:
            self.obj = method.im_self
            self.func = method.im_func
        else:
            self.obj = method.__self__
            self.func = method.__func__
    def __eq__(self, other):
        return type(other) is MethodType and self.obj == other.obj and self.func == other.func
    def __ne__(self, other):
        return type(other) is not MethodType or self.obj != other.obj or self.func != other.func
    def __hash__(self):
        return hash(self.obj) ^ hash(self.func)

raw_sql_cache = {}

def parse_raw_sql(sql):
    result = raw_sql_cache.get(sql)
    if result is not None: return result
    assert isinstance(sql, basestring) and len(sql) > 0
    items = []
    codes = []
    pos = 0
    while True:
        try: i = sql.index('$', pos)
        except ValueError:
            items.append(sql[pos:])
            break
        items.append(sql[pos:i])
        if sql[i+1] == '$':
            items.append('$')
            pos = i+2
        else:
            try: expr, _ = parse_expr(sql, i+1)
            except ValueError:
                raise ValueError(sql[i:])
            pos = i+1 + len(expr)
            if expr.endswith(';'): expr = expr[:-1]
            code = compile(expr, '<?>', 'eval')  # expr correction check
            codes.append(code)
            items.append((expr, code))
    result = tuple(items), tuple(codes)
    raw_sql_cache[sql] = result
    return result

class RawSQL(object):
    def __deepcopy__(self, memo):
        assert False  # should not attempt to deepcopy RawSQL instances, because of locals/globals
    def __init__(self, sql, globals=None, locals=None, result_type=None):
        self.sql = sql
        self.items, self.codes = parse_raw_sql(sql)
        self.types, self.values = normalize(tuple(eval(code, globals, locals) for code in self.codes))
        self.result_type = result_type
    def _get_type_(self):
        return RawSQLType(self.sql, self.items, self.types, self.result_type)

class RawSQLType(object):
    def __deepcopy__(self, memo):
        return self  # RawSQLType instances are "immutable"
    def __init__(self, sql, items, types, result_type):
        self.sql = sql
        self.items = items
        self.types = types
        self.result_type = result_type
    def __hash__(self):
        return hash(self.sql) ^ hash(self.types)
    def __eq__(self, other):
        return type(other) is RawSQLType and self.sql == other.sql and self.types == other.types
    def __ne__(self, other):
        return not self.__eq__(other)

class QueryType(object):
    def __init__(self, query, limit=None, offset=None):
        self.query_key = query._key
        self.translator = query._translator
        self.limit = limit
        self.offset = offset
    def __hash__(self):
        result = hash(self.query_key)
        if self.limit is not None:
            result ^= hash(self.limit + 3)
        if self.offset is not None:
            result ^= hash(self.offset)
        return result
    def __eq__(self, other):
        return type(other) is QueryType and self.query_key == other.query_key \
               and self.limit == other.limit and self.offset == other.offset
    def __ne__(self, other):
        return not self.__eq__(other)

numeric_types = {bool, int, float, Decimal}
comparable_types = {int, float, Decimal, unicode, date, time, datetime, timedelta, bool, UUID}
primitive_types = comparable_types | {buffer}
function_types = {type, types.FunctionType, types.BuiltinFunctionType}
type_normalization_dict = { long : int } if PY2 else {}

def normalize(value):
    t = type(value)
    if t.__name__ == 'LocalProxy' and '_get_current_object' in t.__dict__:
        value = value._get_current_object()
        t = type(value)

    if t is tuple:
        item_types, item_values = [], []
        for item in value:
            item_type, item_value = normalize(item)
            item_values.append(item_value)
            item_types.append(item_type)
        return tuple(item_types), tuple(item_values)

    if t.__name__ == 'EntityMeta':
        return SetType(value), value

    if t.__name__ == 'EntityIter':
        entity = value.entity
        return SetType(entity), entity

    if PY2 and isinstance(value, str):
        try:
            value.decode('ascii')
        except UnicodeDecodeError:
            throw(TypeError, 'The bytestring %r contains non-ascii symbols. Try to pass unicode string instead' % value)
        else:
            return unicode, value
    elif isinstance(value, unicode):
        return unicode, value

    if t in function_types:
        return FuncType(value), value

    if t is types.MethodType:
        return MethodType(value), value

    if hasattr(value, '_get_type_'):
        return value._get_type_(), value

    return normalize_type(t), value

def normalize_type(t):
    tt = type(t)
    if tt is tuple: return tuple(normalize_type(item) for item in t)
    if not isinstance(t, type):
        return t
    assert t.__name__ != 'EntityMeta'
    if tt.__name__ == 'EntityMeta': return t
    if t is NoneType: return t
    t = type_normalization_dict.get(t, t)
    if t in primitive_types: return t
    if t in (slice, type(Ellipsis)): return t
    if issubclass(t, basestring): return unicode
    if issubclass(t, (dict, Json)): return Json
    if tt.__name__ == 'EnumMeta': return t
    throw(TypeError, 'Unsupported type %r' % t.__name__)

coercions = {
    (int, float) : float,
    (int, Decimal) : Decimal,
    (date, datetime) : datetime,
    (bool, int) : int,
    (bool, float) : float,
    (bool, Decimal) : Decimal
    }
coercions.update(((t2, t1), t3) for ((t1, t2), t3) in items_list(coercions))

def coerce_types(t1, t2):
    if t1 == t2: return t1
    is_set_type = False
    if type(t1) is SetType:
        is_set_type = True
        t1 = t1.item_type
    if type(t2) is SetType:
        is_set_type = True
        t2 = t2.item_type
    result = coercions.get((t1, t2))
    if result is not None and is_set_type: result = SetType(result)
    return result

def are_comparable_types(t1, t2, op='=='):
    # types must be normalized already!
    tt1 = type(t1)
    tt2 = type(t2)

    t12 = {t1, t2}
    if Json in t12 and t12 < {Json, str, unicode, int, bool, float}:
        return True
    if op in ('in', 'not in'):
        if tt2 is RawSQLType: return True
        if tt2 is not SetType: return False
        op = '=='
        t2 = t2.item_type
        tt2 = type(t2)
    if op in ('is', 'is not'):
        return t1 is not None and t2 is NoneType
    if tt1 is tuple:
        if not tt2 is tuple: return False
        if len(t1) != len(t2): return False
        for item1, item2 in izip(t1, t2):
            if not are_comparable_types(item1, item2): return False
        return True
    if tt1 is RawSQLType or tt2 is RawSQLType: return True
    if op in ('==', '<>', '!='):
        if t1 is NoneType and t2 is NoneType: return False
        if t1 is NoneType or t2 is NoneType: return True
        if t1 in primitive_types:
            if t1 is t2: return True
            if (t1, t2) in coercions: return True
            if tt1 is not type or tt2 is not type: return False
            if issubclass(t1, int_types) and issubclass(t2, basestring): return True
            if issubclass(t2, int_types) and issubclass(t1, basestring): return True
            return False
        if tt1.__name__ == tt2.__name__ == 'EntityMeta':
            return t1._root_ is t2._root_
        if tt1.__name__ == tt2.__name__ == 'EnumMeta':
            return t1.__name__ == t2.__name__
        return False
    if t1 is t2 and t1 in comparable_types: return True
    return (t1, t2) in coercions

class TrackedValue(object):
    def __init__(self, obj, attr):
        self.obj_ref = weakref.ref(obj)
        self.attr = attr
    @classmethod
    def make(cls, obj, attr, value):
        if isinstance(value, dict):
            return TrackedDict(obj, attr, value)
        if isinstance(value, list):
            return TrackedList(obj, attr, value)
        return value
    def _changed_(self):
        obj = self.obj_ref()
        if obj is not None:
            obj._attr_changed_(self.attr)
    def get_untracked(self):
        assert False, 'Abstract method'  # pragma: no cover

def tracked_method(func):
    @wraps(func, assigned=('__name__', '__doc__') if PY2 else WRAPPER_ASSIGNMENTS)
    def new_func(self, *args, **kwargs):
        obj = self.obj_ref()
        attr = self.attr
        if obj is not None:
            args = tuple(TrackedValue.make(obj, attr, arg) for arg in args)
            if kwargs: kwargs = {key: TrackedValue.make(obj, attr, value) for key, value in iteritems(kwargs)}
        result = func(self, *args, **kwargs)
        self._changed_()
        return result
    return new_func

class TrackedDict(TrackedValue, dict):
    def __init__(self, obj, attr, value):
        TrackedValue.__init__(self, obj, attr)
        dict.__init__(self, {key: self.make(obj, attr, val) for key, val in iteritems(value)})
    def __reduce__(self):
        return dict, (dict(self),)
    __setitem__ = tracked_method(dict.__setitem__)
    __delitem__ = tracked_method(dict.__delitem__)
    _update = tracked_method(dict.update)
    def update(self, *args, **kwargs):
        args = [ arg if isinstance(arg, dict) else dict(arg) for arg in args ]
        return self._update(*args, **kwargs)
    setdefault = tracked_method(dict.setdefault)
    pop = tracked_method(dict.pop)
    popitem = tracked_method(dict.popitem)
    clear = tracked_method(dict.clear)
    def get_untracked(self):
        return {key: val.get_untracked() if isinstance(val, TrackedValue) else val
                for key, val in self.items()}

class TrackedList(TrackedValue, list):
    def __init__(self, obj, attr, value):
        TrackedValue.__init__(self, obj, attr)
        list.__init__(self, (self.make(obj, attr, val) for val in value))
    def __reduce__(self):
        return list, (list(self),)
    __setitem__ = tracked_method(list.__setitem__)
    __delitem__ = tracked_method(list.__delitem__)
    extend = tracked_method(list.extend)
    append = tracked_method(list.append)
    pop = tracked_method(list.pop)
    remove = tracked_method(list.remove)
    insert = tracked_method(list.insert)
    reverse = tracked_method(list.reverse)
    sort = tracked_method(list.sort)
    if PY2:
        __setslice__ = tracked_method(list.__setslice__)
    else:
        clear = tracked_method(list.clear)
    def get_untracked(self):
        return [val.get_untracked() if isinstance(val, TrackedValue) else val for val in self]

class Json(object):
    """A wrapper over a dict or list
    """
    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __repr__(self):
        return '<Json %r>' % self.wrapped
