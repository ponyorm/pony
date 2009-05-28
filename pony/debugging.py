
import threading

import pony

if pony.MODE.startswith('GAE-'):
    
    def debug_app(app, environ):
        return app(environ)

else:

    class Local(threading.local):
        def __init__(self):
            self.lock = threading.Lock()
            self.lock.acquire()

    local = Local()

    def debug_app(app, environ):
        result_holder = []
        queue.put((local.lock, app, environ, result_holder))
        local.lock.acquire()
        return result_holder[0]

    from Queue import Queue
    queue = Queue()

    class DebugThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self, name="DebugThread")
            self.setDaemon(True)
        def run(self):
            while True:
                x = queue.get()
                if x is None: break
                lock, app, environ, result_holder = x
                status, headers, result = app(environ)
                result_holder.append((status, headers, result))
                lock.release()

    @pony.on_shutdown
    def do_shutdown():
        queue.put(None)
        debug_thread.join()

    debug_thread = DebugThread()
    debug_thread.start()