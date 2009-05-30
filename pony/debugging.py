
import re, threading

import pony
from pony import httputils

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

    debug_re = re.compile(r'((?<=\?|&)|^)debug(=[^&]*)?&?')

    def debug_app(app, environ):
        query = environ.get('QUERY_STRING', '')
        if not debug_re.search(query): return app(environ)
       
        result_holder = []
        queue.put((local.lock, app, environ, result_holder))
        local.lock.acquire()
        return result_holder[0]

    from Queue import Queue
    queue = Queue()

    last = None

    class DebugThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self, name="DebugThread")
            self.setDaemon(True)
        def run(self):
            # print>>pony.real_stdout, 111
            global last
            last = queue.get()
            # print>>pony.real_stdout, 222

            while last is not None:
                lock, app, environ, result_holder = last
                url = httputils.reconstruct_url(environ)
                url = debug_re.sub('', url)
                if url.endswith('&'): url = url[:-1]
                debugger = Debugger(url)
                # print>>pony.real_stdout, 333

                result = debugger.runcall(app, environ)
                # print>>pony.real_stdout, 999
                if result is not None:
                    status, headers, content = result
                    lock, app, environ, result_holder = last
                    headers.append(('X-Debug', 'Result'))
                    result_holder.append((status, headers, content))
                    lock.release()
                    # print>>pony.real_stdout, 'aaa'
                    last = queue.get()
                    # print>>pony.real_stdout, 'bbb'

    class Debugger(bdb.Bdb):
        def __init__(self, url):
            self.url = url
            bdb.Bdb.__init__(self)
        def process_queue(self, response_text):
            # print>>pony.real_stdout, 444
            global last
            if last is None: self.set_quit(); return
            # print>>pony.real_stdout, 555
            lock, app, environ, result_holder = last
            url = httputils.reconstruct_url(environ)
            url = debug_re.sub('', url)
            if url.endswith('&'): url = url[:-1]
            if url != self.url: self.set_quit(); return
            # print>>pony.real_stdout, 666
            # if response_text.startswith('call '): self.set_continue() else:
            self.set_step()
            headers = [('Content-Type', 'text/plain'), ('X-Debug', 'Step')]
            result_holder.append(('200 OK', headers, response_text))
            lock.release()
            # print>>pony.real_stdout, 777
            last = queue.get()
            # print>>pony.real_stdout, 888
        def user_call(self, frame, args):
            self.process_queue('call ' + (frame.f_code.co_name or "<unknown>"))
        def user_line(self, frame):
            name = frame.f_code.co_name or "<unknown>"
            filename = self.canonic(frame.f_code.co_filename)
            self.process_queue('stop at %s %s in %s' % (filename, frame.f_lineno, name))
        def user_return(self, frame, value):
            name = frame.f_code.co_name or "<unknown>"
            self.process_queue('return from ' + name)
        def user_exception(self, frame, exception):
            name = frame.f_code.co_name or "<unknown>"
            self.process_queue('exception in %s %s' % (name, exception))

    @pony.on_shutdown
    def do_shutdown():
        queue.put(None)
        debug_thread.join()

    debug_thread = DebugThread()
    debug_thread.start()