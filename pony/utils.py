# -*- coding: cp1251 -*-

from itertools import chain, imap, ifilter
from new import instancemethod
from operator import is_not, itemgetter
from os import urandom
from re import compile

NOTHING = object()

class OrderedDict(object):
    __slots__ = '__dict', '__list', '__contains__'
    @classmethod
    def fromkeys(cls, seq, value):
        result = cls()
        result.update((key, value) for key in seq)
        return result
    def copy(self):
        return self.__class__(self.iteritems())
    def __init__(self, *args, **keyargs):
        self.__list = []
        self.__dict = {}
        self.__contains__ = self.__dict.__contains__  # optimisation
        self.update(*args, **keyargs)
##  def __contains__(self, key):
##      return key in self.__dict
    def __hash__(self):
        raise TypeError('OrderedDict objects are unhashable')
    def iterkeys(self):
        is_not_nothing = instancemethod(is_not, NOTHING, object)
        return imap(itemgetter(0), ifilter(is_not_nothing, self.__list))
    __iter__ = iterkeys
    def itervalues(self):
        is_not_nothing = instancemethod(is_not, NOTHING, object)
        return imap(itemgetter(1), ifilter(is_not_nothing, self.__list))
    def iteritems(self):
        is_not_nothing = instancemethod(is_not, NOTHING, object)
        return ifilter(is_not_nothing, self.__list)
    def keys(self):
        return list(self.iterkeys())
    def values(self):
        return list(self.itervalues())
    def items(self):
        return list(self.iteritems())
    def __repr__(self):
        content = ', '.join('%r: %r' % x for x in self.iteritems())
        return '<%s {%s}>' % (self.__class__.__name__, content)
    def __getitem__(self, key):
        return self.__list[self.__dict[key]][1]
    def __setitem__(self, key, value):
        new = len(self.__list)
        index = self.__dict.setdefault(key, new)
        if index is new: self.__list.append((key, value))
        else: self.__list[index] = (key, value)
    def __delitem__(self, key):
        self.__list[self.__dict.pop(key)] = NOTHING
        if len(self.__list) > len(self.__dict) * 2: self._pack()
    def _pack(self):
        self.__list = self.items()
        self.__dict = dict((key,i) for i,(key,value) in enumerate(self.__list))
    def clear(self):
        self.__list = []
        self.__dict.clear()  # must keep previous dict because of __contains__
    def get(self, key, default=None):
        index = self.__dict.get(key, NOTHING)
        if index is not NOTHING: return  self.__list[index][1]
        return default
    def setdefault(self, key, default=None):
        new = len(self.__list)
        index = self.__dict.setdefault(key, new)
        if index == new:
            self.__list.append((key, default))
            return default
        else: return self.__list[index][1]
    def pop(self, key, default=NOTHING):
        if default is not NOTHING:
            index = self.__dict.pop(key, NOTHING)
            if index is NOTHING: return default
        else: index = self.__dict.pop(key)
        result = self.__list[index][1]
        self.__list[index] = NOTHING
        if len(self.__list) > len(self.__dict) * 2: self._pack()
        return result
    def popitem(self):
        index = self.__dict.popitem()[1]
        result = self.__list[index]
        self.__list[index] = NOTHING
        if len(self.__list) > len(self.__dict) * 2: self._pack()
        return result
    def update(self, *args, **keyargs):
        if args:
            if len(args) > 1: raise TypeError(
                'OrderedDict expected at most 1 positional arguments, got %s'
                % len(args))
            x = args[0]
            if hasattr(x, 'keys'):
                if hasattr(x, 'iteritems'): seq = x.iteritems()
                elif hasattr(x, 'items'): seq = x.items()
            else: seq = x
            if keyargs: seq = chain(seq, keyargs.iteritems())
        else: seq = keyargs.iteritems()
        if ((  not hasattr(self, '__dict__')
               or '__setitem__' not in self.__dict__  )
            and self.__class__.__setitem__ == OrderedDict.__setitem__):
            print '!!!'
            for key, value in seq:  # optimisation
                new = len(self.__list)
                index = self.__dict.setdefault(key, new)
                if index is new: self.__list.append((key, value))
                else: self.__list[index] = (key, value)
        else:
            for key, value in seq: self[key] = value

def error_method(*args, **kwargs):
    raise TypeError

class FrozenDict(dict):
    "Dictionary subclass with __hash__ method. Cannot be modified."

    def __init__(self, x):
        dict.__init__(self, x)
        self._hash = hash(tuple(sorted(self.iteritems())))

    __setitem__ = __delitem__ = clear = update = setdefault = pop = popitem \
        = error_method

    def __hash__(self):
        return self._hash

ident_re = compile(r'[A-Za-z_]\w*')

# is_ident = ident_re.match
def is_ident(string):
    'is_ident(string) -> bool'
    return bool(ident_re.match(string))

def import_module(name):
    "import_module('a.b.c') -> <module a.b.c>"
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]: mod = getattr(mod, comp)
    return mod

def new_guid():
    'new_guid() -> new_binary_guid'
    return buffer(urandom(16))

def guid2str(guid):
    """guid_binary2str(binary_guid) -> string_guid

    >>> guid2str(unxehlify('ff19966f868b11d0b42d00c04fc964ff'))
    '6F9619FF-8B86-D011-B42D-00C04FC964FF'
    """
    assert isinstance(guid, buffer) and len(guid) == 16
    guid = str(guid)
    return '%s-%s-%s-%s-%s' % tuple(map(hexlify, (
        guid[3::-1], guid[5:3:-1], guid[7:5:-1], guid[8:10], guid[10:])))

def str2guid(s):
    """guid_str2binary(str_guid) -> binary_guid

    >>> unhexlify(str2guid('6F9619FF-8B86-D011-B42D-00C04FC964FF'))
    'ff19966f868b11d0b42d00c04fc964ff'
    """
    assert isinstance(s, basestring) and len(s) == 36
    a, b, c, d, e = map(unhexlify, (s[:8],s[9:13],s[14:18],s[19:23],s[24:]))
    reverse = slice(-1, None, -1)
    return buffer(''.join((a[reverse], b[reverse], c[reverse], d, e)))

