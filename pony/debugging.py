
import re, threading

import pony
# from pony import httputils

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

    debug_re = re.compile('(^|&)debug(=[^&]+)?')

    def debug_app(app, environ):
        query = environ.get('QUERY_STRING', '')
        if not debug_re.search(query): return app(environ)
       
        result_holder = []
        queue.put((local.lock, app, environ, result_holder))
        local.lock.acquire()
        return result_holder[0]

    from Queue import Queue
    queue = Queue()

    class Debugger(bdb.Bdb):
        def user_call(self, frame, args):
            name = frame.f_code.co_name or "<unknown>"
            print>>pony.real_stderr, "call", name, args
            self.set_step()
        def user_line(self, frame):
            name = frame.f_code.co_name or "<unknown>"
            filename = self.canonic(frame.f_code.co_filename)
            print>>pony.real_stderr, "stop at", filename, frame.f_lineno, "in", name
            self.set_step()
        def user_return(self, frame, value):
            name = frame.f_code.co_name or "<unknown>"
            print>>pony.real_stderr, "return from", name, value
            self.set_step()
        def user_exception(self, frame, exception):
            name = frame.f_code.co_name or "<unknown>"
            print>>pony.real_stderr, "exception in", name, exception
            self.set_step()

    class DebugThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self, name="DebugThread")
            self.setDaemon(True)
        def run(self):
            while True:
                x = queue.get()
                if x is None: break
                lock, app, environ, result_holder = x
                # url = httputils.reconstruct_script_url(environ)
                # url = debug_re.sub(url, '&')

                debugger = Debugger()
                # debugger.url = url
                
                d = dict(app=app, environ=environ, result_holder=result_holder)
                status, headers, result = debugger.runcall(app, environ)
                headers.append(('X-Debug', 'True'))
                result_holder.append((status, headers, result))
                lock.release()

    @pony.on_shutdown
    def do_shutdown():
        queue.put(None)
        debug_thread.join()

    debug_thread = DebugThread()
    debug_thread.start()