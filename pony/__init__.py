import sys, time, threading, random
from os.path import dirname
from itertools import count

uid = str(random.randint(1, 1000000))

def detect_mode():
    try: import google.appengine
    except ImportError: pass
    else:
        try: import dev_appserver
        except ImportError: return 'GAE-SERVER'
        return 'GAE-LOCAL'

    try: mod_wsgi = sys.modules['mod_wsgi']
    except KeyError: pass
    else: return 'MOD_WSGI'

    if 'flup.server.fcgi' in sys.modules: return 'FCGI-FLUP'

    try: sys.modules['__main__'].__file__
    except AttributeError:  return 'INTERACTIVE'
    return 'CHERRYPY'

MODE = detect_mode()

MAIN_FILE = None
if MODE in ('CHERRYPY', 'GAE-LOCAL', 'GAE-SERVER', 'FCGI-FLUP'):
    MAIN_FILE = sys.modules['__main__'].__file__
elif MODE == 'MOD_WSGI':
    for module_name, module in sys.modules.items():
        if module_name.startswith('_mod_wsgi_'):
            MAIN_FILE = module.__file__
            break

if MAIN_FILE is not None: MAIN_DIR = dirname(MAIN_FILE)
else: MAIN_DIR = None

PONY_DIR = dirname(__file__)

################################################################################

# pony.MODE must be defined before this import
from pony.utils import decorator, tostring, localbase

try: real_stdout
except NameError:
    assert sys.stdout.__class__.__name__ != 'PonyStdout'
    real_stdout = sys.stdout
    real_stderr = sys.stderr

class Local(localbase):
    def __init__(self):
        self.output_streams = [ real_stdout ]
        self.error_streams = [ real_stderr ]
        self.error_stream_nested = 0  # nested calls counter to prevent infinite recursion with flup.fcgi

local = Local()

class PonyStdout(object):
    def __getattribute__(self, name):
        return getattr(local.output_streams[-1], name)
    def _get_softspace(self):
        return local.output_streams[-1].softspace
    def _set_softspace(self, value):
        local.output_streams[-1].softspace = value
    softspace = property(_get_softspace, _set_softspace)
pony_stdout = PonyStdout()
sys.stdout = pony_stdout

class PonyStderr(object):
    def __getattribute__(self, name):
        nested = local.error_stream_nested
        if nested: return getattr(real_stderr, name)
        try:
            local.error_stream_nested = nested + 1
            return getattr(local.error_streams[-1], name)
        finally: local.error_stream_nested = nested
    def _get_softspace(self):
        return local.error_streams[-1].softspace
    def _set_softspace(self, value):
        local.error_streams[-1].softspace = value
    softspace = property(_get_softspace, _set_softspace)
pony_stderr = PonyStderr()
sys.stderr = pony_stderr

class ListStream(list):
    softspace = 0
    write = list.append
    writelines = list.extend
    @staticmethod
    def flush(): pass

@decorator
def grab_stdout(f):
    def new_function(*args, **keyargs):
        output_stream = ListStream()
        local.output_streams.append(output_stream)
        # The next line required for PythonWin interactive window
        # (PythonWin resets stdout all the time)
        sys.stdout = pony_stdout
        try: result = f(*args, **keyargs)
        finally:
            top_stream = local.output_streams.pop() 
            assert top_stream is output_stream
        if result is None: return output_stream
        return (tostring(result),)
    return new_function

################################################################################

shutdown = False

shutdown_list = []

def on_shutdown(func):
    if func not in shutdown_list: shutdown_list.append(func)
    return func

exception_in_main = None  # sets to exception instance by use_autoreload()

def exitfunc():
    mainloop()
    _shutdown()
    if sys.platform == 'win32' and MODE == 'CHERRYPY' and exception_in_main:
        # If a script is started in Windows by double-clicking 
        # and a problem occurs, then the following code will
        # prevent the console window from closing immediately.
        # This only works if use_autoreload() has been called
        print '\nPress Enter to exit...'
        raw_input()
    prev_func()

if MODE in ('INTERACTIVE', 'GAE-SERVER', 'GAE-LOCAL'): pass
elif hasattr(threading, '_shutdown'):
    prev_func = threading._shutdown
    threading._shutdown = exitfunc
else:
    prev_func = sys.exitfunc
    sys.exitfunc = exitfunc

mainloop_counter = count()

_do_mainloop = False  # sets to True by pony.web.start_http_server()

def mainloop():
    if not _do_mainloop: return
    if MODE not in ('CHERRYPY', 'FCGI-FLUP'): return
    if mainloop_counter.next(): return
    try:
        while True:
            if shutdown: break
            time.sleep(1)
    except:
        try: log_exc = logging.log_exc
        except NameError: pass
        else: log_exc()

shutdown_counter = count()

def _shutdown():
    global shutdown
    shutdown = True
    if shutdown_counter.next(): return
    for func in reversed(shutdown_list): func()
