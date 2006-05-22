# -*- coding: cp1251 -*-

from itertools import imap, ifilter
from operator import itemgetter
from os import urandom
from re import compile

class NameMapMixin(object):
    def __init__(self):
        self.__list = [] # list of (name, value) pairs or None values
        self.__dict = {} # mapping: name -> index in list
    def __len__(self):
        return len(self.__dict)
    def __iter__(self):
        return imap(itemgetter(1), ifilter(None, self.__list))
    def __contains__(self, key):
        return key in self.__dict
    def __getitem__(self, x):
        if isinstance(x, basestring): return self.__list[self.__dict[x]][1]
        if len(self.__list) != len (self.__dict): self._pack()
        if isinstance(x, slice): return map(itemgetter(1), self.__list[x])
        return self.__list[x][1]
    def __setitem__(self, name, value):
        if not isinstance(name, basestring):
            raise TypeError('Unexpected name type')
        new = len(self.__list)
        index = self.__dict.setdefault(name, new)
        if index is new: self.__list.append((name, value))
        else: self.__list[index] = (name, value)
    def __delitem__(self, x):
        if isinstance(x, basestring):
            self.__list[self.__dict.pop(x)] = None
            if len(self.__list) > len(self.__dict) * 2: self._pack()
            return
        if len(self.__list) != len(self.__dict):
            self.__list = filter(None, self.__list)
        del self.__list[x]
        self.__dict = dict((name, i) for i, (name, value)
                                     in enumerate(self.__list))
    def _pack(self):
        self.__list = filter(None, self.__list)
        self.__dict = dict((name, i) for i, (name, value)
                                     in enumerate(self.__list))
    def _clear(self):
        self.__list = []
        self.__dict = {}
    def _get(self, key, default=None):
        try: index = self.__dict[key]
        except KeyError: return default
        return self.__list[index][1]
    

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


