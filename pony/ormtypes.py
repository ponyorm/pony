from types import NoneType
from decimal import Decimal
from datetime import date, datetime
from itertools import izip

from pony.utils import throw

class AsciiStr(str): pass

class LongStr(str):
    lazy = True

class LongUnicode(unicode):
    lazy = True

class SetType(object):
    __slots__ = 'item_type'
    def __init__(self, item_type):
        self.item_type = item_type
    def __eq__(self, other):
        return type(other) is SetType and self.item_type == other.item_type
    def __ne__(self, other):
        return type(other) is not SetType and self.item_type != other.item_type
    def __hash__(self):
        return hash(self.item_type) + 1

numeric_types = set([ int, float, Decimal ])
string_types = set([ str, AsciiStr, unicode ])
comparable_types = set([ int, float, Decimal, str, AsciiStr, unicode, date, datetime, bool ])
primitive_types = set([ int, float, Decimal, str, AsciiStr, unicode, date, datetime, bool, buffer ])
type_normalization_dict = { long : int, bool : int, LongStr : str, LongUnicode : unicode }

def get_normalized_type_of(value):
    if isinstance(value, str):
        try: value.decode('ascii')
        except UnicodeDecodeError: pass
        else: return AsciiStr
    elif isinstance(value, unicode):
        try: value.encode('ascii')
        except UnicodeEncodeError: pass
        else: return AsciiStr
    return normalize_type(type(value))

def normalize_type(t):
    if t is NoneType: return t
    if issubclass(t, basestring):  # Mainly for Html -> unicode & StrHtml -> str conversion
        if t in (str, AsciiStr, unicode): return t
        if issubclass(t, str): return str
        if issubclass(t, unicode): return unicode
        assert False
    t = type_normalization_dict.get(t, t)
    if t in primitive_types: return t
    if type(t).__name__ == 'EntityMeta': return t
    throw(TypeError, 'Unsupported type %r' % t.__name__)

coercions = {
    (int, float) : float,
    (int, Decimal) : Decimal,
    (date, datetime) : datetime,
    (AsciiStr, str) : str,
    (AsciiStr, unicode) : unicode
    }
coercions.update(((t2, t1), t3) for ((t1, t2), t3) in coercions.items())

def coerce_types(t1, t2):
    if t1 is t2: return t1
    return coercions.get((t1, t2))

def are_comparable_types(t1, t2, op='=='):
    # types must be normalized already!
    if op in ('in', 'not in'):
        if not isinstance(t2, SetType): return False
        op = '=='
        t2 = t2.item_type
    if op in ('is', 'is not'):
        return t1 is not None and t2 is NoneType
    if isinstance(t1, tuple):
        if not isinstance(t2, tuple): return False
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
            if not isinstance(t1, type) or not isinstance(t2, type): return False
            if issubclass(t1, (int, long)) and issubclass(t2, basestring): return True
            if issubclass(t2, (int, long)) and issubclass(t1, basestring): return True
            return False
        if type(t1).__name__ == type(t2).__name__ == 'EntityMeta':
            return t1._root_ is t2._root_
        return False
    if t1 is t2 and t1 in comparable_types: return True
    return (t1, t2) in coercions
