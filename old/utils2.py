# -*- coding: cp1251 -*-

from os import urandom
from itertools import ifilter
from binascii import hexlify, unhexlify

##def guid(x=None):
##    if x is None: return new_guid()
##    if isinstance(x, basestring): return str2guid(x)
##    if not isinstance(x, buffer) or len(x) != 16: assert ArgumentError, x
##    return x

def new_guid():
    'new_guid() -> new guid'
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

def join(delimiter, iterables):
    """join(delimiter, iterables) -> combined list of elements

    >>> join(None, [[1, 2], [3, 4], [5, 6]])
    [1, 2, None, 3, 4, None, 5, 6]
    """
    lists = ifilter(None, lists)
    try:
        result = list(lists.next())
    except StopIteration:
        return []
    for list_ in lists:
        result.append(delimiter)
        result.extend(list_)
    return result









