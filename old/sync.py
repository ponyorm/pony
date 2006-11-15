# -*- coding: cp1251 -*-

from threading import Lock, currentThread
from Queue import Queue, Empty as QueueEmpty
from time import time, sleep

__all__ = ('in_thread', 'Activity', 'Result', 'ThreadPool',
           'NotReady', 'NOT_READY')

###############################################################################

class UniQueue(Queue):
    def _init(self, maxsize):
        Queue.__init__(self, maxsize)
        self.set = set()
    def _put(self, item):
        # Will block if qsize == maxsize even if the item already in queue!
        if item not in self.set:
            self.set.add(item)
            self.queue.append(item)
    def _get(self):
        result = self.queue.popleft()
        self.set.remove(result)
        return result    
    def __contains__(self, x):
        # Because of multithreading semantics, the result is not reliable.
        return x in self.set

###############################################################################

class NotReady(Exception):
    pass

NOT_READY = object()

class DeferredResult(object):
    def __init__(self, value=NOT_READY):
        self.waiting_threads = set()
        self.value = value

    def is_ready(self):
        return self.value is not NOT_READY

    def get_nowait(self):
        if self.value is NOT_READY: raise NotReady
        return self.value

    def get_value(self, timeout=None):
        if timeout is None:
            return self._get()
        else:
            # copied from threading.Condition:
            endtime = time() + timeout
            delay = 0.0005 # 500 us -> initial delay of 1 ms
            while True:
                if self.value is not NOT_READY: return self.value
                remaining = endtime - time()
                if remaining <= 0: raise NotReady('Timeout is exceeded')
                delay = min(delay * 2, remaining, .05)
                sleep(delay)

    _global_lock = Lock()

    def _get(self):
        if self.value is not NOT_READY: return self.value
        t = currentThread()
        DeferredResult._global_lock.acquire()
        try:
            if self.value is not NOT_READY: return self.value
            self.waiting_threads.add(t)
            try:
                thread_lock = t.__result_lock
            except AttributeError:                
                thread_lock = t.__result_lock = Lock()
                thread_lock.acquire()
        finally:
            DeferredResult._global_lock.release()
        thread_lock.acquire()
        return self.value

    def _set(self, value):
        DeferredResult._global_lock.acquire()
        try:
            self.value = value
            for t in self.waiting_threads:
                t.__result_lock.release()
            self.waiting_threads.clear()
        finally:
            DeferredResult._global_lock.release()

def in_thread(thread_name):
    def decorator(func):
        def wrapper(self, *args, **keyargs):
            return self._send_func(func, args, keyargs, thread_name)
        wrapper.__doc__ = func.__doc__
        return wrapper
    return decorator

class Activity(object):
    def __init__(self):
        self._lock1 = Lock()
        self._lock2 = Lock()
        self._func_queues = {}

    _global_lock = Lock()        
        
    def _send_func(self, func, args, keyargs, thread_name):
        t = currentThread()
        Activity._global_lock.acquire()
        try:
            func_queue = self._func_queues.get(thread_name)
            if func_queue is None:
                func_queue = self._func_queues[thread_name] = Queue()
            func_queue.put((func, args, keyargs))
            try:
                obj_queue = t.__activity_queue
            except AttributeError:
                obj_queue = t.__activity_queue = UniQueue()
            obj_queue.put(self)
        finally:
            Activity._global_lock.release()
        if t.getName() == thread_name: self.execute()

    def execute(self):
        t = currentThread()
        thread_name = t.getName()
        func_queue = self._func_queues.get[thread_name]
        if func_queue is None: return
        try:
            try:
                self._lock.acquire()
                while True:
                    func, args, keyargs = func_queue.get_nowait()
                    self.func(*args, **keyargs)
            finally:
                self._lock.release()
        except QueueEmpty:
            pass

class ThreadPool(object):
    pass




