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
    >>> 'x' in cache
    True
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

class Node(object):
    __slots__ = 'prev', 'next', 'key', 'value', 'expire', 'access', '__weakref__'

MONTH = 31*24*60*60

char_table = "?" * 256
noncontrol_chars = "".join(chr(i) for i in range(32, 256) if i != 127)

def normalize(key, value="", expire=None):
    if isinstance(key, tuple): hash_value, key = key
    elif not isinstance(key, str): raise ValueError('Key must be tuple or string. Got: %s' % key.__class__.__name__)
    if len(key) > 1024 * 1024: raise ValueError('Key size too big: %d' % len(key))
    if key.translate(char_table, noncontrol_chars): raise ValueError('Key cannot contains control characters')

    if not isinstance(value, str): raise ValueError('Value must be string. Got: %s' % value.__class__.__name__)
    if len(value) > 1024 * 1024: raise ValueError('Value size too big: %d' % len(value))
    if expire is not None:
        expire = int(expire)
        if expire == 0: expire = None
        elif expire < 0: raise ValueError('Expiration must not be negative')
        elif expire <= MONTH: expire = int(gettime()) + expire
        elif expire <= MONTH * 100: raise ValueError('Invalid expire value: %d' % expire)
    return key, value, expire

class Memcache(object):
    def __init__(self, max_data_size=64*1024*1024):
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
        self._hits = self._misses = self._evictions = 0
        self._get_count = self._set_count = 0
        self._add_count = self._replace_count = self._delete_count = 0
        self._incr_count = self._decr_count = 0
    def __len__(self):
        return len(self._dict)
    def __contains__(self, key):
        return key in self._dict
    def __iter__(self):
        return iter(self.items())
    def items(self):
        now = int(gettime())
        result = []
        append = result.append
        list = self._list
        self._lock.acquire()
        try:
            node = list.next
            while node is not list:
                expire = node.expire
                if expire is not None and expire <= now: self._delete_node(node)
                elif node.value is not None: append((node.key, node.value))
                node = node.next
        finally: self._lock.release()
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
            self._evictions += 1
    def _pack_heap(self):
        new_heap = []
        for item in self._heap:
            expire, node_ref = item
            node = node_ref()
            if node is not None and expire == node.expire: new_heap.append(item)
        heapify(new_heap)
        self._heap = new_heap
    def get(self, key):
        self._get_count += 1
        key, _, _ = normalize(key)
        self._lock.acquire()
        try:
            node = self._find_node(key)
            if node is None:
                self._misses += 1
                return None
            self._place_on_top(node)
            self._hits += 1
            return node.value
        finally: self._lock.release()
    def get_multi(self, keys, key_prefix=''):
        result = {}
        for key in keys:
            val = self.get(key_prefix + key)
            if val is not None: result[key] = val
        return result
    def set(self, key, value, time=None):
        self._set_count += 1
        key, value, expire = normalize(key, value, time)
        self._lock.acquire()
        try:
            node = self._find_node(key)
            if node is None: node = self._create_node(key)
            self._place_on_top(node)
            self._set_node_value(node, value, expire)
        finally: self._lock.release()
        return True
    def set_multi(self, mapping, time=None, key_prefix=''):
        for key, value in mapping.iteritems():
            self.set(key_prefix + key, value, time)
        return []
    def add(self, key, value, time=None):
        self._add_count += 1
        key, value, expire = normalize(key, value, time)
        self._lock.acquire()
        try:
            node = self._find_node(key)
            if node is not None:
                self._place_on_top(node)
                return False
            node = self._create_node(key)
            self._place_on_top(node)
            self._set_node_value(node, value, expire)
        finally: self._lock.release()
        return True
    def add_multi(self, mapping, time=None, key_prefix=''):
        result = []
        for key, value in mapping.iteritems():
            if not self.add(key_prefix + key, value, time):
                result.append(key)
        return result
    def replace(self, key, value, time=None):
        self._replace_count += 1
        key, value, expire = normalize(key, value, time)
        self._lock.acquire()
        try:
            node = self._find_node(key)
            if node is None: return False
            self._place_on_top(node)
            if node.value is None: return False
            self._set_node_value(node, value, expire)
        finally: self._lock.release()
        return True
    def replace_multi(self, mapping, time=None, key_prefix=''):
        result = []
        for key, value in mapping.iteritems():
            if not self.replace(key_prefix + key, value, time):
                result.append(key)
        return result
    def delete(self, key, seconds=None):
        self._delete_count += 1
        key, _, seconds = normalize(key, "", seconds)
        node = self._find_node(key)
        if node is None or node.value is None: return 1
        if seconds is None: self._delete_node(node)
        else: self._set_node_value(node, None, seconds)
        return 2
    def delete_multi(self, keys, seconds=None, key_prefix=''):
        for key in keys:
            self.delete(key_prefix + key, seconds)
        return []
    def incr(self, key, delta=1):
        self._incr_count += 1
        key, _, _ = normalize(key)
        self._lock.acquire()
        try:
            node = self._find_node(key)
            if node is None: return None
            self._place_on_top(node)
            value = node.value
            if value is None: return None
            try: value = int(value) + delta
            except ValueError: return None
            if value < 0: value = 0
            node.value = str(value)
        finally: self._lock.release()
        return value
    def decr(self, key, delta=1):
        self._decr_count += 1
        self._incr_count -= 1
        return self.incr(key, -delta)
    def flush_all(self):
        self._lock.acquire()
        try:
            self._dict.clear()
            self._heap = []
            self._list.prev = self._list.next = self._list
            for node in self._dict.itervalues():
                node.prev = node.next = None
        finally: self._lock.release()
    def get_stats(self):
        return dict(items=len(self._dict), bytes=self._data_size,
                    hits=self._hits, misses=self._misses, evictions=self._evictions,
                    oldest_item_age=int(gettime())-self._list.prev.access,
                    cmd_get=self._get_count, cmd_set=self._set_count,
                    cmd_add=self._add_count, cmd_replace=self._replace_count, cmd_delete=self._delete_count,
                    cmd_incr = self._incr_count, cmd_decr=self._decr_count)
