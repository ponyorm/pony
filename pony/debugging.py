
import re, threading

import pony

if pony.MODE.startswith('GAE-'):
    
    def debug_app(app, environ):
        return app(environ)

else:
    import bdb
    class Local(threading.local):
        def __init__(self):
            self.lock = threading.Lock()
            self.lock.acquire()

    local = Local()

    debug_re = re.compile('(^|&)debug=')

    def debug_app(app, environ):
        query = environ.get('QUERY_STRING', '')
        if not debug_re.search(query): return app(environ)
       
        result_holder = []
        queue.put((local.lock, app, environ, result_holder))
        local.lock.acquire()
        return result_holder[0]

    from Queue import Queue
    queue = Queue()

    run_app = compile('x = app(environ)', '<?>', 'exec')

    def f(app, environ, result_holder) :
        status, headers, result = app(environ)
        headers.append(('X-Debug', 'True'))
        result_holder.append((status, headers, result))

    class DebugThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self, name="DebugThread")
            self.setDaemon(True)
        def run(self):
            while True:
                x = queue.get()
                if x is None: break
                lock, app, environ, result_holder = x
                debugger = bdb.Bdb()
                d = dict(app=app, environ=environ, result_holder=result_holder)
                debugger.run(run_app, d)
                status, headers, result = d['x']
                headers.append(('X-Debug', 'True'))
                result_holder.append((status, headers, result))
                lock.release()

    @pony.on_shutdown
    def do_shutdown():
        queue.put(None)
        debug_thread.join()

    debug_thread = DebugThread()
    debug_thread.start()