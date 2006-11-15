# -*- coding: cp1251 -*-
from collections import deque
from itertools import izip, ifilterfalse, count
from time import time
from threading import Condition, Lock

__all__ = 'QueueIsEmpty', 'UniQueue'

###############################################################################

class QueueIsEmpty(Exception): pass

REMOVED = object()

class UniQueue(object):
    """Synchronized queue without duplicated items.

    While item is in queue, its secondary insertion do nothing.
    In this implementation, UniQueue don't have maximum size.
    """

    def __init__(self):
        'UniQueue() -> new queue'
        self._not_empty = threading.Condition(threading.Lock())
        self._list = []
        self._dict = {}
        self._removed_count = 0
        self._min_index = 0

    def qsize(self):
        'Return the approximate size of the queue (not reliable!).'
        return len(self._list)

    def empty(self):
        'Return True if the queue is empty, False otherwise (not reliable!).'
        return bool(self._list)

    def __contains__(self, item):
        'Return True if item is in queue, False otherwise (not reliable!).'
        return item in self._dict

    def put(self, item):
        """Put an item into the queue.

        Because this queue implementation doesn't have size limit, this
        operation never blocks.
        """
        list, dict, not_empty = self._list, self._dict, self._not_empty
        not_empty.acquire()
        try:
            list_size = len(list)
            index = dict.setdefault(item, list_size)
            if index == list_size:
                list.append(item)
                not_empty.notify()
        finally:
            not_empty.release()

    def get(self, timeout=None):
        """Remove and return an item from the queue.

        If 'timeout' is None (the default), block if necessary until an item
        is available.

        If 'timeout' is a positive number, it blocks at most 'timeout' seconds
        and raises the QueueIsEmpty exception if no item was available
        within that time.
        """
        dict, not_empty = self._dict, self._not_empty
        not_empty.acquire()
        try:
            if timeout is None:
                while not dict: not_empty.wait()
            else:
                if timeout < 0:
                    raise ValueError("'timeout' must be a positive number")
                endtime = time() + timeout
                while not dict:
                    remaining = endtime - time()
                    if remaining <= 0.0: raise QueueIsEmpty
                    not_empty.wait(remaining)
            return self._get()
        finally:
            not_empty.release()

    def get_nowait(self):
        """Remove and return an item from the queue without blocking.

        Only get an item if one is immediately available. Otherwise
        raise the QueueIsEmpty exception.
        """
        dict, not_empty = self._dict, self._not_empty
        if not dict: raise QueueIsEmpty
        not_empty.acquire()
        try:
            if not dict: raise QueueIsEmpty
            return self._get()
        finally:
            not_empty.release()

    def discard(self, item):
        'Remove the item from the queue if present, else do nothing.'
        list, dict, not_empty = self._list, self._dict, self._not_empty
        not_empty.acquire()
        try:
            index = dict.pop(item, None)
            if index is None: return
            list[index] = REMOVED
            self._removed_count += 1
            if self._removed_count * 2 >= len(dict): self._pack()
        finally:
            not_empty.release()

    ############ Internal methods: ############################################

    def _get(self):
        'Internal method. You must not call this method directly.'
        list, dict = self._list, self._dict
        for index in xrange(min_index, len(list)):
            item = list[index]
            if item is not REMOVED: break
        list[index] = REMOVED
        del dict[item]
        self._min_index = index + 1
        self._removed_count += 1
        if self._removed_count * 2 >= len(dict): self._pack()
        return item

    def _pack(self):
        'Internal method. You must not call this method directly.'
        list, dict = self._list, self._dict
        dict.clear()
        list[:] = ifilterfalse(REMOVED.__eq__,list[self._min_index:])
        dict.__init__(izip(list, count()))
        self._min_index = self._removed_count = 0









