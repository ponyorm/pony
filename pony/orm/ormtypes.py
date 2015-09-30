from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2, items_list, izip, basestring, unicode, buffer, int_types

import types
from decimal import Decimal
from datetime import date, time, datetime, timedelta
from uuid import UUID

from pony.utils import throw

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

class MethodType(object):
    __slots__ = 'obj', 'func'
    def __deepcopy__(self, memo):
        return self  # MethodType instances are "immutable"
    if PY2:
        def __init__(self, method):
            self.obj = method.im_self
            self.func = method.im_func
    else:
        def __init__(self, method):
            self.obj = method.__self__
            self.func = method.__func__
    def __eq__(self, other):
        return type(other) is MethodType and self.obj == other.obj and self.func == other.func
    def __ne__(self, other):
        return type(other) is not SetType or self.obj != other.obj or self.func != other.func
    def __hash__(self):
        return hash(self.obj) ^ hash(self.func)

numeric_types = set([ bool, int, float, Decimal ])
comparable_types = set([ int, float, Decimal, unicode, date, time, datetime, timedelta, bool, UUID ])
primitive_types = comparable_types | set([ buffer ])
function_types = set([type, types.FunctionType, types.BuiltinFunctionType])
type_normalization_dict = { long : int } if PY2 else {}

def get_normalized_type_of(value):
    t = type(value)
    if t is tuple: return tuple(get_normalized_type_of(item) for item in value)
    try: hash(value)  # without this, cannot do tests like 'if value in special_fucntions...'
    except TypeError: throw(TypeError, 'Unsupported type %r' % t.__name__)
    if t.__name__ == 'EntityMeta': return SetType(value)
    if t.__name__ == 'EntityIter': return SetType(value.entity)
    if PY2 and isinstance(value, str):
        try: value.decode('ascii')
        except UnicodeDecodeError: raise
        else: return unicode
    elif isinstance(value, unicode): return unicode
    if t in function_types: return FuncType(value)
    if t is types.MethodType: return MethodType(value)
    return normalize_type(t)

def normalize_type(t):
    tt = type(t)
    if tt is tuple: return tuple(normalize_type(item) for item in t)
    assert t.__name__ != 'EntityMeta'
    if tt.__name__ == 'EntityMeta': return t
    if t is NoneType: return t
    t = type_normalization_dict.get(t, t)
    if t in primitive_types: return t
    if issubclass(t, basestring): return unicode
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
    if op in ('in', 'not in'):
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
        return False
    if t1 is t2 and t1 in comparable_types: return True
    return (t1, t2) in coercions
