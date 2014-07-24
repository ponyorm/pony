"""
    Example usage of Memcache class:

    >>> from pony import caching
    >>> cache = caching.Memcache()
    >>> cache.set('k1', 'v1')
    True
    >>> cache.set('k2', 'v2', 10)
    True
    >>> cache.set('k3', 'v3')
    True
    >>> cache.set('k4', 'v4')
    True
    >>> cache.items()
    [('k4', 'v4'), ('k3', 'v3'), ('k2', 'v2'), ('k1', 'v1')]
    >>> cache.get('k2')
    'v2'
    >>> from time import sleep; sleep(10)
    >>> print(cache.get('k2'))
    None
    >>> cache.items()
    [('k4', 'v4'), ('k3', 'v3'), ('k1', 'v1')]
    >>> cache.add('k5', 'v5')
    True
    >>> cache.add('k5', 'v6')
    False
    >>> cache.items()
    [('k5', 'v5'), ('k4', 'v4'), ('k3', 'v3'), ('k1', 'v1')]
    >>> print(cache.incr('x'))
    None
    >>> cache.set('x', '100')
    True
    >>> cache.incr('x')
    101
    >>> cache.decr('x', 10)
    91
    >>> cache.get('k3')
    'v3'
    >>> cache.items()
    [('k3', 'v3'), ('x', '91'), ('k5', 'v5'), ('k4', 'v4'), ('k1', 'v1')]
    >>> len(cache)
    5
    >>> cache.replace('k4', 'v4_new')
    True
    >>> cache.replace('notexists', 'value')
    False
    >>> cache.items()
    [('k4', 'v4_new'), ('k3', 'v3'), ('x', '91'), ('k5', 'v5'), ('k1', 'v1')]
"""

from __future__ import absolute_import, print_function
from pony.py23compat import itervalues, iteritems

from heapq import heappush, heappop, heapify
from threading import Lock
from time import time as gettime
from weakref import ref

import pony
from pony import options
from pony.utils import decorator

class Node(object):
    __slots__ = 'prev', 'next', 'key', 'value', 'expire', 'access', '__weakref__'

MONTH = 30*24*60*60
MAX_KEY_LENGTH = 250
MAX_VALUE_LENGTH = 1024*1024  # 1 Megabyte

char_table = "?" * 256
noncontrol_chars = "".join(chr(i) for i in range(33, 256) if i != 127)

def normalize(key, value="", expire=None):
    if isinstance(key, tuple): hash_value, key = key
    elif not isinstance(key, str): raise ValueError('Key must be tuple or string. Got: %s' % key.__class__.__name__)
    if len(key) > MAX_KEY_LENGTH: raise ValueError('Key size too big: %d' % len(key))
    if key.translate(char_table, noncontrol_chars): raise ValueError('Key cannot contains spaces or control characters')

    if not isinstance(value, str): raise ValueError('Value must be string. Got: %s' % value.__class__.__name__)
    if len(value) > MAX_VALUE_LENGTH: raise ValueError('Value size too big: %d' % len(value))
    if expire is not None:
        expire = int(expire)
        if expire == 0: expire = None
        elif expire < 0: raise ValueError('Expiration must not be negative')
        elif expire <= MONTH: expire = int(gettime()) + expire
        elif expire <= MONTH * 100: raise ValueError('Invalid expire value: %d' % expire)
    return key, value, expire

@decorator
def with_lock(func, cache, *args, **kwargs):
    with cache._lock:
        return func(cache, *args, **kwargs)

DEFAULT_MAX_DATA_SIZE = 64*1024*1024  # 64 Megabytes

class Memcache(object):
    def __init__(cache, max_data_size=DEFAULT_MAX_DATA_SIZE):
        cache._lock = Lock()
        cache._heap = []
        cache._dict = {}
        cache._list = list = Node()
        list.prev = list.next = list.expire = list.key = list.value = None
        list.prev = list.next = list
        list.access = int(gettime())
        cache._data_size = 0
        if not isinstance(max_data_size, int):
            raise TypeError('Max data size must be int. Got: %s' % type(max_data_size).__name__)
        cache.max_data_size = max_data_size
        cache._stat_get_hits = cache._stat_get_misses = cache._stat_evictions = 0
        cache._stat_cmd_get = cache._stat_cmd_set = 0
        cache._stat_total_items = 0
        cache._start_time = int(gettime())
    def __len__(cache):
        return len(cache._dict)
    def __iter__(cache):
        return iter(cache.items())
    @with_lock
    def items(cache):
        now = int(gettime())
        result = []
        append = result.append
        list = cache._list
        node = list.next
        while node is not list:
            expire = node.expire
            if expire is not None and expire <= now: cache._delete_node(node)
            elif node.value is not None: append((node.key, node.value))
            node = node.next
        return result
    def _delete_node(cache, node, unlink=True):
        if unlink:
            prev, next = node.prev, node.next
            prev.next = next
            next.prev = prev
        del cache._dict[node.key]
        cache._data_size -= len(node.key)
        if node.value is not None: cache._data_size -= len(node.value)
    def _find_node(cache, key):
        node = cache._dict.get(key)
        if node is None: return None
        prev, next = node.prev, node.next
        prev.next = next
        next.prev = prev
        expire = node.expire
        if expire is None or expire > int(gettime()): return node
        cache._delete_node(node, unlink=False)
        return None
    def _create_node(cache, key):
        cache._dict[key] = node = Node()
        node.key = key
        node.value = None
        cache._data_size += len(node.key)
        cache._stat_total_items += 1
        return node
    def _place_on_top(cache, node):
        list = cache._list
        old_top = list.next
        node.prev, node.next = list, old_top
        list.next = old_top.prev = node
        node.access = int(gettime())
    def _set_node_value(cache, node, value, expire):
        if node.value is not None: cache._data_size -= len(node.value)
        if value is not None: cache._data_size += len(value)
        node.value, node.expire = value, expire
        if expire is not None: heappush(cache._heap, (expire, ref(node)))
        cache._delete_expired_nodes()
        cache._conform_to_limits()
        if len(cache._heap) > len(cache._dict) * 2: cache._pack_heap()
    def _delete_expired_nodes(cache):
        now = int(gettime())
        heap = cache._heap
        while heap:
            expire, node_ref = heap[0]
            if expire > now: break
            heappop(heap)
            node = node_ref()
            if node is not None:
                expire = node.expire
                if expire is not None and expire <= now: cache._delete_node(node)
    def _conform_to_limits(cache):
        list = cache._list
        while cache._data_size > cache.max_data_size:
            bottom = list.prev
            cache._delete_node(bottom)
            cache._stat_evictions += 1
    def _pack_heap(cache):
        new_heap = []
        for item in cache._heap:
            expire, node_ref = item
            node = node_ref()
            if node is not None and expire == node.expire: new_heap.append(item)
        heapify(new_heap)
        cache._heap = new_heap
    def _get(cache, key):
        cache._stat_cmd_get += 1
        key, _, _ = normalize(key)
        node = cache._find_node(key)
        if node is None:
            cache._stat_get_misses += 1
            return None
        cache._place_on_top(node)
        cache._stat_get_hits += 1
        return node.value
    get = with_lock(_get)
    @with_lock
    def get_multi(cache, keys, key_prefix=''):
        result = {}
        for key in keys:
            val = cache._get(key_prefix + key)
            if val is not None: result[key] = val
        return result
    def _set(cache, key, value, time=0):
        cache._stat_cmd_set += 1
        key, value, expire = normalize(key, value, time)
        node = cache._find_node(key)
        if node is None: node = cache._create_node(key)
        cache._place_on_top(node)
        cache._set_node_value(node, value, expire)
        return True
    set = with_lock(_set)
    @with_lock
    def set_multi(cache, mapping, time=0, key_prefix=''):
        for key, value in iteritems(mapping):
            cache._set(key_prefix + key, value, time)
        return []
    def _add(cache, key, value, time=0):
        cache._stat_cmd_set += 1
        key, value, expire = normalize(key, value, time)
        node = cache._find_node(key)
        if node is not None:
            cache._place_on_top(node)
            return False
        node = cache._create_node(key)
        cache._place_on_top(node)
        cache._set_node_value(node, value, expire)
        return True
    add = with_lock(_add)
    @with_lock
    def add_multi(cache, mapping, time=0, key_prefix=''):
        result = []
        for key, value in iteritems(mapping):
            if not cache._add(key_prefix + key, value, time):
                result.append(key)
        return result
    def _replace(cache, key, value, time=0):
        cache._stat_cmd_set += 1
        key, value, expire = normalize(key, value, time)
        node = cache._find_node(key)
        if node is None: return False
        cache._place_on_top(node)
        if node.value is None: return False
        cache._set_node_value(node, value, expire)
        return True
    replace = with_lock(_replace)
    @with_lock
    def replace_multi(cache, mapping, time=0, key_prefix=''):
        result = []
        for key, value in iteritems(mapping):
            if not cache._replace(key_prefix + key, value, time):
                result.append(key)
        return result
    def _delete(cache, key, seconds=0):
        cache._stat_cmd_set += 1
        key, _, seconds = normalize(key, "", seconds)
        node = cache._find_node(key)
        if node is None or node.value is None: return 1
        if seconds is None: cache._delete_node(node)
        else:
            cache._place_on_top(node)
            cache._set_node_value(node, None, seconds)
        return 2
    delete = with_lock(_delete)
    @with_lock
    def delete_multi(cache, keys, seconds=0, key_prefix=''):
        for key in keys:
            cache._delete(key_prefix + key, seconds)
        return []
    @with_lock
    def incr(cache, key, delta=1):
        cache._stat_cmd_set += 1
        key, _, _ = normalize(key)
        node = cache._find_node(key)
        if node is None: return None
        cache._place_on_top(node)
        value = node.value
        if value is None: return None
        try: value = int(value) + delta
        except ValueError: return None
        if value < 0: value = 0
        node.value = str(value)
        return value
    def decr(cache, key, delta=1):
        return cache.incr(key, -delta)
    @with_lock
    def append(cache, key, value):
        key, value, _ = normalize(key, value)
        node = cache._find_node(key)
        if node is None: return False
        cache._place_on_top(node)
        if node.value is None: return False
        cache._data_size += len(value)
        node.value += value
        cache._delete_expired_nodes()
        cache._conform_to_limits()
        return True
    @with_lock
    def prepend(cache, key, value):
        key, value, _ = normalize(key, value)
        node = cache._find_node(key)
        if node is None: return False
        cache._place_on_top(node)
        if node.value is None: return False
        cache._data_size += len(value)
        node.value = value + node.value
        cache._delete_expired_nodes()
        cache._conform_to_limits()
        return True
    @with_lock
    def flush_all(cache):
        cache._dict.clear()
        cache._heap = []
        cache._list.prev = cache._list.next = cache._list
        for node in itervalues(cache._dict):
            node.prev = node.next = None
    @with_lock
    def get_stats(cache):
        now = int(gettime())
        return dict(curr_items=len(cache._dict), total_items=cache._stat_total_items,
                    bytes=cache._data_size, limit_maxbytes=cache.max_data_size,
                    get_hits=cache._stat_get_hits, get_misses=cache._stat_get_misses, evictions=cache._stat_evictions,
                    oldest_item_age=int(gettime())-cache._list.prev.access,
                    cmd_get=cache._stat_cmd_get, cmd_set=cache._stat_cmd_set,
                    time=now, uptime=now-cache._start_time)

if pony.MODE.startswith('GAE-'):
    import google.appengine.api.memcache
    memcache = google.appengine.api.memcache.Client()
    session_memcache = memcache
    orm_memcache = memcache
    templating_memcache = memcache
    responce_memcache = memcache
else:
    def choose_memcache(opt, default=None):
        if opt is None:
            if default is not None: return default
            return Memcache()
        if hasattr(opt, 'get_multi'): return opt
        from pony.thirdparty.memcache import Client
        return Client(opt)

    memcache = choose_memcache(options.MEMCACHE)
    session_memcache = choose_memcache(options.ALTERNATIVE_SESSION_MEMCACHE, memcache)
    orm_memcache = choose_memcache(options.ALTERNATIVE_ORM_MEMCACHE, memcache)
    templating_memcache = choose_memcache(options.ALTERNATIVE_TEMPLATING_MEMCACHE, memcache)
    responce_memcache = choose_memcache(options.ALTERNATIVE_RESPONCE_MEMCACHE, memcache)
