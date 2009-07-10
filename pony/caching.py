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

from threading import Lock
from time import time

class Node(object):
    __slots__ = 'prev', 'next', 'expire', 'key', 'value'

def normalize(key, value="", expire=None):
    if isinstance(key, tuple): hash_value, key = key
    elif not isinstance(key, str): raise ValueError('Key must be tuple or string. Got: %s' % key.__class__.__name__)
    if not isinstance(value, str): raise ValueError('Value must be string. Got: %s' % value.__class__.__name__)
    if len(value) > 1024 * 1024: raise ValueError('Value size too big: %d' % len(value))
    if expire is not None: expire = time() + expire
    return key, value, expire

class Memcache(object):
    def __init__(self):
        self.dict = {}
        list = self.list = Node()
        list.prev = list.next = list.expire = list.key = list.value = None
        list.prev = list.next = list
        self.lock = Lock()
    def __len__(self):
        return len(self.dict)
    def __contains__(self, key):
        return key in self.dict
    def __iter__(self):
        return iter(self.items())
    def items(self):
        now = time()
        result = []
        append = result.append
        list = self.list
        self.lock.acquire()
        try:
            node = list.prev
            while node is not list:
                expire = node.expire
                if expire is not None and expire <= now:
                    prev, next = node.prev, node.next
                    prev.next = next
                    next.prev = prev
                    del self.dict[node.key]
                else: append((node.key, node.value))
                node = node.prev
        finally: self.lock.release()
        return result
    def _find(self, key, do_create_when_not_present):
        node = self.dict.get(key)
        if node is not None:
            expire = node.expire
            success = expire is None or expire > time()
            prev, next, expire = node.prev, node.next, node.expire
            prev.next = next
            next.prev = prev
        else:
            success = False
            if do_create_when_not_present:
                self.dict[key] = node = Node()
                node.key = key
        if node is not None:
            list = self.list
            node.next = list
            node.prev = prev_top = list.prev
            list.prev = prev_top.next = node
        return success, node
    def get(self, key):
        key, _, _ = normalize(key)
        self.lock.acquire()
        try:
            success, node = self._find(key, False)
            if success: return node.value
            return None
        finally: self.lock.release()
    def set(self, key, value, expire=None):
        key, value, expire = normalize(key, value, expire)
        self.lock.acquire()
        try:
            success, node = self._find(key, True)
            node.value, node.expire = value, expire
        finally: self.lock.release()
        return True
    def add(self, key, value, expire=None):
        key, value, expire = normalize(key, value, expire)
        self.lock.acquire()
        try:
            success, node = self._find(key, True)
            if success: return False
            node.value, node.expire = value, expire
        finally: self.lock.release()
        return True
    def replace(self, key, value, expire=None):
        key, value, expire = normalize(key, value, expire)
        self.lock.acquire()
        try:
            success, node = self._find(key, False)
            if not success: return False
            node.value, node.expire = value, expire
        finally: self.lock.release()
        return True
    def incr(self, key, delta=1):
        key, _, _ = normalize(key)
        self.lock.acquire()
        try:
            success, node = self._find(key, False)
            if not success: return None
            try: value = int(node.value) + delta
            except ValueError: return None
            node.value = str(value)
        finally: self.lock.release()
        return value
    def decr(self, key, delta=1):
        key, _, _ = normalize(key)
        self.lock.acquire()
        try:
            success, node = self._find(key, False)
            if not success: return None
            try: value = int(node.value) - delta
            except ValueError: return None
            node.value = str(value)
        finally: self.lock.release()
        return value
    def flush_all(self):
        self.lock.acquire()
        try:
            self.dict.clear()
            self.list.prev = self.list.next = self.list
            for node in self.dict.itervalues():
                node.prev = node.next = None
        finally: self.lock.release()
