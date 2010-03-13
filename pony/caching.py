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
    >>> print cache.get('k2')
    None
    >>> cache.items()
    [('k4', 'v4'), ('k3', 'v3'), ('k1', 'v1')]
    >>> cache.add('k5', 'v5')
    True
    >>> cache.add('k5', 'v6')
    False
    >>> cache.items()
    [('k5', 'v5'), ('k4', 'v4'), ('k3', 'v3'), ('k1', 'v1')]
    >>> print cache.incr('x')
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

from heapq import heappush, heappop, heapify
from threading import Lock
from time import time as gettime
from weakref import ref

from pony.utils import simple_decorator

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

@simple_decorator
def with_lock(func, self, *args, **keyargs):
    self._lock.acquire()
    try: return func(self, *args, **keyargs)
    finally: self._lock.release()

DEFAULT_MAX_DATA_SIZE = 64*1024*1024  # 64 Megabytes

class Memcache(object):
    def __init__(self, max_data_size=DEFAULT_MAX_DATA_SIZE):
        self._lock = Lock()
        self._heap = []
        self._dict = {}
        self._list = list = Node()
        list.prev = list.next = list.expire = list.key = list.value = None
        list.prev = list.next = list
        list.access = int(gettime())
        self._data_size = 0
        if not isinstance(max_data_size, int): raise TypeError, 'Max data size must be int. Got: %s' % type(max_data_size).__name__
        self.max_data_size = max_data_size
        self._stat_get_hits = self._stat_get_misses = self._stat_evictions = 0
        self._stat_cmd_get = self._stat_cmd_set = 0
        self._stat_total_items = 0
        self._start_time = int(gettime())
    def __len__(self):
        return len(self._dict)
    def __iter__(self):
        return iter(self.items())
    @with_lock
    def items(self):
        now = int(gettime())
        result = []
        append = result.append
        list = self._list
        node = list.next
        while node is not list:
            expire = node.expire
            if expire is not None and expire <= now: self._delete_node(node)
            elif node.value is not None: append((node.key, node.value))
            node = node.next
        return result
    def _delete_node(self, node, unlink=True):
        if unlink:
            prev, next = node.prev, node.next
            prev.next = next
            next.prev = prev
        del self._dict[node.key]
        self._data_size -= len(node.key)
        if node.value is not None: self._data_size -= len(node.value)
    def _find_node(self, key):
        node = self._dict.get(key)
        if node is None: return None
        prev, next = node.prev, node.next
        prev.next = next
        next.prev = prev
        expire = node.expire
        if expire is None or expire > int(gettime()): return node
        self._delete_node(node, unlink=False)
        return None
    def _create_node(self, key):
        self._dict[key] = node = Node()
        node.key = key
        node.value = None
        self._data_size += len(node.key)
        self._stat_total_items += 1
        return node
    def _place_on_top(self, node):
        list = self._list
        old_top = list.next
        node.prev, node.next = list, old_top
        list.next = old_top.prev = node
        node.access = int(gettime())
    def _set_node_value(self, node, value, expire):
        if node.value is not None: self._data_size -= len(node.value)
        if value is not None: self._data_size += len(value)
        node.value, node.expire = value, expire
        if expire is not None: heappush(self._heap, (expire, ref(node)))
        self._delete_expired_nodes()
        self._conform_to_limits()
        if len(self._heap) > len(self._dict) * 2: self._pack_heap()
    def _delete_expired_nodes(self):
        now = int(gettime())
        heap = self._heap
        while heap:
            expire, node_ref = heap[0]
            if expire > now: break
            heappop(heap)
            node = node_ref()
            if node is not None:
                expire = node.expire
                if expire is not None and expire <= now: self._delete_node(node)
    def _conform_to_limits(self):
        list = self._list
        while self._data_size > self.max_data_size:
            bottom = list.prev
            self._delete_node(bottom)
            self._stat_evictions += 1
    def _pack_heap(self):
        new_heap = []
        for item in self._heap:
            expire, node_ref = item
            node = node_ref()
            if node is not None and expire == node.expire: new_heap.append(item)
        heapify(new_heap)
        self._heap = new_heap
    def _get(self, key):
        self._stat_cmd_get += 1
        key, _, _ = normalize(key)
        node = self._find_node(key)
        if node is None:
            self._stat_get_misses += 1
            return None
        self._place_on_top(node)
        self._stat_get_hits += 1
        return node.value
    get = with_lock(_get)
    @with_lock
    def get_multi(self, keys, key_prefix=''):
        result = {}
        for key in keys:
            val = self._get(key_prefix + key)
            if val is not None: result[key] = val
        return result
    def _set(self, key, value, time=0):
        self._stat_cmd_set += 1
        key, value, expire = normalize(key, value, time)
        node = self._find_node(key)
        if node is None: node = self._create_node(key)
        self._place_on_top(node)
        self._set_node_value(node, value, expire)
        return True
    set = with_lock(_set)
    @with_lock
    def set_multi(self, mapping, time=0, key_prefix=''):
        for key, value in mapping.iteritems():
            self._set(key_prefix + key, value, time)
        return []
    def _add(self, key, value, time=0):
        self._stat_cmd_set += 1
        key, value, expire = normalize(key, value, time)
        node = self._find_node(key)
        if node is not None:
            self._place_on_top(node)
            return False
        node = self._create_node(key)
        self._place_on_top(node)
        self._set_node_value(node, value, expire)
        return True
    add = with_lock(_add)
    @with_lock
    def add_multi(self, mapping, time=0, key_prefix=''):
        result = []
        for key, value in mapping.iteritems():
            if not self._add(key_prefix + key, value, time):
                result.append(key)
        return result
    def _replace(self, key, value, time=0):
        self._stat_cmd_set += 1
        key, value, expire = normalize(key, value, time)
        node = self._find_node(key)
        if node is None: return False
        self._place_on_top(node)
        if node.value is None: return False
        self._set_node_value(node, value, expire)
        return True
    replace = with_lock(_replace)
    @with_lock
    def replace_multi(self, mapping, time=0, key_prefix=''):
        result = []
        for key, value in mapping.iteritems():
            if not self._replace(key_prefix + key, value, time):
                result.append(key)
        return result
    def _delete(self, key, seconds=0):
        self._stat_cmd_set += 1
        key, _, seconds = normalize(key, "", seconds)
        node = self._find_node(key)
        if node is None or node.value is None: return 1
        if seconds is None: self._delete_node(node)
        else:
            self._place_on_top(node)
            self._set_node_value(node, None, seconds)
        return 2
    delete = with_lock(_delete)
    @with_lock
    def delete_multi(self, keys, seconds=0, key_prefix=''):
        for key in keys:
            self._delete(key_prefix + key, seconds)
        return []
    @with_lock
    def incr(self, key, delta=1):
        self._stat_cmd_set += 1
        key, _, _ = normalize(key)
        node = self._find_node(key)
        if node is None: return None
        self._place_on_top(node)
        value = node.value
        if value is None: return None
        try: value = int(value) + delta
        except ValueError: return None
        if value < 0: value = 0
        node.value = str(value)
        return value
    def decr(self, key, delta=1):
        return self.incr(key, -delta)
    @with_lock
    def append(self, key, value):
        key, value, _ = normalize(key, value)
        node = self._find_node(key)
        if node is None: return False
        self._place_on_top(node)
        if node.value is None: return False
        self._data_size += len(value)
        node.value += value
        self._delete_expired_nodes()
        self._conform_to_limits()
        return True
    @with_lock
    def prepend(self, key, value):
        key, value, _ = normalize(key, value)
        node = self._find_node(key)
        if node is None: return False
        self._place_on_top(node)
        if node.value is None: return False
        self._data_size += len(value)
        node.value = value + node.value
        self._delete_expired_nodes()
        self._conform_to_limits()
        return True
    @with_lock
    def flush_all(self):
        self._dict.clear()
        self._heap = []
        self._list.prev = self._list.next = self._list
        for node in self._dict.itervalues():
            node.prev = node.next = None
    @with_lock
    def get_stats(self):
        now = int(gettime())
        return dict(curr_items=len(self._dict), total_items=self._stat_total_items,
                    bytes=self._data_size, limit_maxbytes=self.max_data_size,
                    get_hits=self._stat_get_hits, get_misses=self._stat_get_misses, evictions=self._stat_evictions,
                    oldest_item_age=int(gettime())-self._list.prev.access,
                    cmd_get=self._stat_cmd_get, cmd_set=self._stat_cmd_set,
                    time=now, uptime=now-self._start_time)
