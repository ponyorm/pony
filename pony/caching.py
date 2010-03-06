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
    __slots__ = 'prev', 'next', 'key', 'value', 'expire', '__weakref__'

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
        self.lock = Lock()
        self.heap = []
        self.dict = {}
        self.list = list = Node()
        list.prev = list.next = list.expire = list.key = list.value = None
        list.prev = list.next = list
        self.data_size = 0
        if not isinstance(max_data_size, int): raise TypeError, 'Max data size must be int. Got: %s' % type(max_data_size).__name__
        self.max_data_size = max_data_size
    def __len__(self):
        return len(self.dict)
    def __contains__(self, key):
        return key in self.dict
    def __iter__(self):
        return iter(self.items())
    def items(self):
        now = int(gettime())
        result = []
        append = result.append
        list = self.list
        self.lock.acquire()
        try:
            node = list.next
            while node is not list:
                expire = node.expire
                if expire is not None and expire <= now: self._delete_node(node)
                elif node.value is not None: append((node.key, node.value))
                node = node.next
        finally: self.lock.release()
        return result
    def _delete_node(self, node, unlink=True):
        if unlink:
            prev, next = node.prev, node.next
            prev.next = next
            next.prev = prev
        del self.dict[node.key]
        self.data_size -= len(node.key)
        if node.value is not None: self.data_size -= len(node.value)
    def _find_node(self, key):
        node = self.dict.get(key)
        if node is None: return None
        prev, next = node.prev, node.next
        prev.next = next
        next.prev = prev
        expire = node.expire
        if expire is None or expire > int(gettime()): return node
        self._delete_node(node, unlink=False)
        return None
    def _create_node(self, key):
        self.dict[key] = node = Node()
        node.key = key
        node.value = None
        self.data_size += len(node.key)
        return node
    def _place_on_top(self, node):
        list = self.list
        old_top = list.next
        node.prev, node.next = list, old_top
        list.next = old_top.prev = node
    def _set_node_value(self, node, value, expire):
        if node.value is not None: self.data_size -= len(node.value)
        if value is not None: self.data_size += len(value)
        node.value, node.expire = value, expire
        if expire is not None: heappush(self.heap, (expire, ref(node)))
        self._delete_expired_nodes()
        self._conform_to_limits()
        if len(self.heap) > len(self.dict) * 2: self._pack_heap()
    def _delete_expired_nodes(self):
        now = int(gettime())
        heap = self.heap
        while heap:
            expire, node_ref = heap[0]
            if expire > now: break
            heappop(heap)
            node = node_ref()
            if node is not None:
                expire = node.expire
                if expire is not None and expire <= now: self._delete_node(node)
    def _conform_to_limits(self):
        list = self.list
        while self.data_size > self.max_data_size:
            bottom = list.prev
            self._delete_node(bottom)
    def _pack_heap(self):
        new_heap = []
        for item in self.heap:
            expire, node_ref = item
            node = node_ref()
            if node is not None and expire == node.expire: new_heap.append(item)
        heapify(new_heap)
        self.heap = new_heap
    def get(self, key):
        key, _, _ = normalize(key)
        self.lock.acquire()
        try:
            node = self._find_node(key)
            if node is None: return None
            self._place_on_top(node)
            return node.value
        finally: self.lock.release()
    def get_multi(self, keys, key_prefix=''):
        result = {}
        for key in keys:
            val = self.get(key_prefix + key)
            if val is not None: result[key] = val
        return result
    def set(self, key, value, time=None):
        key, value, expire = normalize(key, value, time)
        self.lock.acquire()
        try:
            node = self._find_node(key)
            if node is None: node = self._create_node(key)
            self._place_on_top(node)
            self._set_node_value(node, value, expire)
        finally: self.lock.release()
        return True
    def set_multi(self, mapping, time=None, key_prefix=''):
        for key, value in mapping.iteritems():
            self.set(key_prefix + key, value, time)
        return []
    def add(self, key, value, time=None):
        key, value, expire = normalize(key, value, time)
        self.lock.acquire()
        try:
            node = self._find_node(key)
            if node is not None:
                self._place_on_top(node)
                return False
            node = self._create_node(key)
            self._place_on_top(node)
            self._set_node_value(node, value, expire)
        finally: self.lock.release()
        return True
    def add_multi(self, mapping, time=None, key_prefix=''):
        result = []
        for key, value in mapping.iteritems():
            if not self.add(key_prefix + key, value, time):
                result.append(key)
        return result
    def replace(self, key, value, time=None):
        key, value, expire = normalize(key, value, time)
        self.lock.acquire()
        try:
            node = self._find_node(key)
            if node is None: return False
            self._place_on_top(node)
            if node.value is None: return False
            self._set_node_value(node, value, expire)
        finally: self.lock.release()
        return True
    def replace_multi(self, mapping, time=None, key_prefix=''):
        result = []
        for key, value in mapping.iteritems():
            if not self.replace(key_prefix + key, value, time):
                result.append(key)
        return result
    def delete(self, key, seconds=None):
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
        key, _, _ = normalize(key)
        self.lock.acquire()
        try:
            node = self._find_node(key)
            if node is None: return None
            self._place_on_top(node)
            value = node.value
            if value is None: return None
            try: value = int(value) + delta
            except ValueError: return None
            node.value = str(value)
        finally: self.lock.release()
        return value
    def decr(self, key, delta=1):
        self.incr(key, -delta)
    def flush_all(self):
        self.lock.acquire()
        try:
            self.dict.clear()
            self.heap = []
            self.list.prev = self.list.next = self.list
            for node in self.dict.itervalues():
                node.prev = node.next = None
        finally: self.lock.release()
