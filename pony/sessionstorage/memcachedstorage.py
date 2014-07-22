from __future__ import absolute_import, print_function, division

import os, binascii

from pony.caching import session_memcache as memcache

DEFAULT_TIME = 0
COOKIE_PREFIX = 'mc:'   # stands for "M"emcached
MEMCACHE_PREFIX = 's:'  # stands for "S"ession

def id2key(session_id):
    assert type(session_id) is str
    if not session_id.startswith(COOKIE_PREFIX): return None
    return MEMCACHE_PREFIX + session_id[len(COOKIE_PREFIX):]

def key2id(key):
    assert type(key) is str
    if not key.startswith(MEMCACHE_PREFIX): return None
    return COOKIE_PREFIX + key[len(MEMCACHE_PREFIX):]

def get(session_id, ctime, mtime):
    key = id2key(session_id)
    if key is None: return None
    return memcache.get(key)

def put(data, ctime, mtime, session_id=None, expire=DEFAULT_TIME):
    assert type(data) is str
    if not session_id:
        key = MEMCACHE_PREFIX + binascii.hexlify(os.urandom(15))  # 32 bytes
        if not memcache.add(key, data, expire): return '<memcache error>'
    else:
        key = id2key(session_id)
        assert key is not None
        # Race conditions between simultaneous requests are possible
        if not memcache.set(key, data, expire): return '<memcache error>'
    return key2id(key)

def delete(session_id):
    key = id2key(session_id)
    assert key is not None
    memcache.delete(key)  # May be resurrected by simultaneous requests (race condition)
