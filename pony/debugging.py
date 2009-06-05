
import re, threading, cStringIO

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

    debug_re = re.compile(r'(?:(?<=\?|&)|^)debug(?:=([^&]*))?&?')

    def debug_app(app, environ):
        query = environ.get('QUERY_STRING', '')
        if not debug_re.search(query): return app(environ)

        env = dict((key, value) for key, value in environ.iteritems()
                                if isinstance(key, basestring) and isinstance(value, basestring))
        env['wsgi.version'] = environ['wsgi.version']
        env['wsgi.url_scheme'] = environ['wsgi.url_scheme']
        env['wsgi.multithread'] = environ['wsgi.multithread']
        env['wsgi.multiprocess'] = environ['wsgi.multiprocess']
        env['wsgi.run_once'] = environ['wsgi.run_once']
        content_length = int(environ.get('CONTENT_LENGTH', '0'))
        input_data = environ['wsgi.input'].read(content_length)
        env['wsgi.input'] = cStringIO.StringIO(input_data)
        env['wsgi.errors'] = cStringIO.StringIO()
        file_wrapper = environ.get('wsgi.file_wrapper')
        if file_wrapper is not None: env['wsgi.file_wrapper'] = file_wrapper

        url = httputils.reconstruct_url(environ)
        command = debug_re.search(url).group(1)
        result_holder = []
        queue.put((local.lock, app, env, result_holder, url, command))
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
                lock, app, environ, result_holder, url, command = last                
                url = debug_re.sub('', url)
                if url.endswith('&'): url = url[:-1]
                debugger = Debugger(url)
                # print>>pony.real_stdout, 333

                result = debugger.runcall(app, environ)
                # print>>pony.real_stdout, 999
                if result is not None:
                    status, headers, content = result
                    lock, app, environ, result_holder, url, command = last
                    headers.append(('X-Debug', 'Result'))
                    result_holder.append((status, headers, content))
                    lock.release()
                    # print>>pony.real_stdout, 'aaa'
                    last = queue.get()
                    # print>>pony.real_stdout, 'bbb'

# set_step     step   "Stop after one line of code"
# set_next     next   "Stop on the next line in or below the given frame"     frame should be specified as a paramater
# set_return   return "Stop when returning from the given frame"              frame should be specified as a paramater
# set_continue cont   "Don't stop except at breakpoints or when finished"

    class Debugger(bdb.Bdb):
        def __init__(self, url):
            self.url = url
            bdb.Bdb.__init__(self)
        def process_queue(self, response_text, frame):
            # print>>pony.real_stdout, 444
            global last
            if last is None: self.set_quit(); return
            # print>>pony.real_stdout, 555
            lock, app, environ, result_holder, url, command = last
            url = debug_re.sub('', url)
            if url.endswith('&'): url = url[:-1]
            if url != self.url: self.set_quit(); return
            module = frame.f_globals.get('__name__') or '?'
            if module == 'pony' or module.startswith('pony.'): self.set_step; return
            headers = [('Content-Type', 'text/html'), ('X-Debug', 'Step')]
            if url.endswith('?'):
                debug_url = url
            else:
                debug_url = '%s&' % url
            debug_dashboard = """ <br> 
            <a href="%sdebug=step">step</a> <a href="%sdebug=next">next</a>
            <a href="%sdebug=return">return</a> <a href="%sdebug=cont">cont</a>
            """ % (debug_url, debug_url, debug_url, debug_url)
            result_holder.append(('200 OK', headers, response_text + debug_dashboard))
            lock.release()
            # print>>pony.real_stdout, 777
            last = queue.get()
            # print>>pony.real_stdout, 888
            lock, app, environ, result_holder, url, command = last
            if command == 'step':
                self.set_step()
            elif command == 'next':
                self.set_next(frame)
            elif command == 'return':
                self.set_return(frame)
            elif command == 'cont':
                self.set_continue()
            else: self.set_step()
        def user_call(self, frame, args):
            self.process_queue('call ' + (frame.f_code.co_name or "<unknown>"), frame)
        def user_line(self, frame):
            name = frame.f_code.co_name or "<unknown>"
            filename = self.canonic(frame.f_code.co_filename)
            self.process_queue('stop at %s %s in %s' % (filename, frame.f_lineno, name), frame)
        def user_return(self, frame, value):
            name = frame.f_code.co_name or "<unknown>"
            self.process_queue('return from ' + name, frame)
        def user_exception(self, frame, exception):
            name = frame.f_code.co_name or "<unknown>"
            self.process_queue('exception in %s %s' % (name, exception), frame)

    @pony.on_shutdown
    def do_shutdown():
        queue.put(None)
        debug_thread.join()

    debug_thread = DebugThread()
    debug_thread.start()