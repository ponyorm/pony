
import re, sys, threading, cStringIO, weakref

import pony
from pony import options, httputils
from pony.utils import simple_decorator

if pony.MODE.startswith('GAE-'):
    
    def debugging_middleware_decorator(func):
        return func

    @simple_decorator    
    def debugging_pony_middleware(app, environ):
        return app(environ)

else:

    import bdb

    @simple_decorator
    def debugging_middleware_decorator(func, *args, **keyargs):
        if options.DEBUG:
            web = sys.modules.get('pony.web')
            if web is not None:
                debugger = web.local.request.environ.get('debugger')
                if debugger is not None: debugger().set_trace()
        return func(*args, **keyargs)

    class Local(threading.local):
        def __init__(self):
            self.lock = threading.Lock()
            self.lock.acquire()

    local = Local()

    debug_re = re.compile(r'(?:(?<=\?|&)|^)debug(?:=([^&]*))?&?')

    @simple_decorator
    def debugging_pony_middleware(app, environ):
        if not options.DEBUG: return app(environ)
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
            global last
            last = queue.get()
            while last is not None:
                lock, app, environ, result_holder, url, command = last                
                url = debug_re.sub('', url)
                if url.endswith('&'): url = url[:-1]
                debugger = Debugger(url)
                environ['debugger'] = weakref.ref(debugger)
                result = debugger.runcall(app, environ)
                if result is not None:
                    status, headers, content = result
                    lock, app, environ, result_holder, url, command = last
                    headers.append(('X-Debug', 'Result'))
                    result_holder.append((status, headers, content))
                    lock.release()
                    last = queue.get()

    class Debugger(bdb.Bdb):
        def __init__(self, url):
            self.url = url
            bdb.Bdb.__init__(self)
            self.__state = 0
            self.__top_user_frame = None
        def process_queue(self, response_text, frame):
            if self.__state == 0:
                self.__state = 1
                self.set_continue()
                return
            global last
            if last is None: self.set_quit(); return
            lock, app, environ, result_holder, url, command = last
            url = debug_re.sub('', url)
            if url.endswith('&'): url = url[:-1]
            if url != self.url: self.set_quit(); return
            if self.__state == 1:
                module = frame.f_globals.get('__name__') or '?'
                if module == 'pony' or module.startswith('pony.'): self.set_step(); return
                self.__top_user_frame = frame
                self.__state = 2
            headers = [('Content-Type', 'text/html'), ('X-Debug', 'Step')]
            if not url.endswith('?'): url += '&'
            debug_dashboard = """ <br> 
            <a href="%sdebug=step">step</a> <a href="%sdebug=next">next</a>
            <a href="%sdebug=return">return</a> <a href="%sdebug=cont">cont</a>
            """ % (url, url, url, url)
            result_holder.append(('200 OK', headers, response_text + debug_dashboard))
            lock.release()
            last = queue.get()
            lock, app, environ, result_holder, url, command = last
            environ['debugger'] = weakref.ref(self)
            if command == 'step': self.set_step()
            elif command == 'next': self.set_next(frame)
            elif command == 'return': self.set_return(frame)
            elif command == 'cont': self.set_continue()
            else: self.set_step()
        def user_line(self, frame):
            name = frame.f_code.co_name or "<unknown>"
            filename = self.canonic(frame.f_code.co_filename)
            self.process_queue('stop at %s %s in %s' % (filename, frame.f_lineno, name), frame)
        def user_return(self, frame, value):
            if frame is self.__top_user_frame:
                self.__top_user_frame = None
                self.set_continue()
        # def user_exception(self, frame, exception):
        #   name = frame.f_code.co_name or "<unknown>"
        #   self.process_queue('exception in %s %s' % (name, exception), frame)

    @pony.on_shutdown
    def do_shutdown():
        queue.put(None)
        debug_thread.join()

    debug_thread = DebugThread()
    debug_thread.start()
