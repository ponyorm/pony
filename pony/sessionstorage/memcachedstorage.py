from pony.thirdparty import memcache
import os, binascii

_servers = ["127.0.0.1:11211"] ## Servers list

_memcached = memcache.Client(_servers, debug = 0)
DEFAULT_TIME = 0
EMPTY_KEY = ''

def _key_generate(data):
    key = binascii.hexlify(os.urandom(16))
    return key

class SessionDataNotFound(Exception): pass

def getdata(key, ctime, mtime):
    try: k = str(key)
    except: raise TypeError("Key must be string")
    data = _memcached.get(k)
    if data is not None: return data
    else: raise SessionDataNotFound()

def putdata(data, ctime, mtime, oldkey=None, time = DEFAULT_TIME):
    if not oldkey: key = _key_generate(data)
    else: key = oldkey
    if _memcached.set(key,data,time): return key
    else: return EMPTY_KEY