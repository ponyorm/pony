import sys, time, threading, random
from os.path import dirname
from itertools import count

from pony.utils import decorator

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

    try: sys.modules['__main__'].__file__
    except AttributeError:  return 'INTERACTIVE'
    return 'CHERRYPY'

MODE = detect_mode()

MAIN_FILE = None
if MODE in ('CHERRYPY', 'GAE-LOCAL', 'GAE-SERVER'):
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

try: real_stdout
except NameError:
    assert sys.stdout.__class__.__name__ != 'PonyStdout'
    real_stdout = sys.stdout
    real_stderr = sys.stderr

class Local(threading.local):
    def __init__(self):
        self.stdout_writers = [ real_stdout.write ]
        self.stderr_writers = [ real_stderr.write ]

local = Local()

class PonyStdout(object):
    try: from pony._templating import write_to_stdout as write
    except ImportError:
        def write(s): local.stdout_writers[-1](s)
    write = staticmethod(write)
pony_stdout = PonyStdout()
try:
    pony_stdout.flush = real_stdout.flush
    pony_stdout.seek = real_stdout.seek
    pony_stdout.readline = real_stdout.readline
except AttributeError: pass
sys.stdout = pony_stdout

class PonyStderr(object):
    try: from pony._templating import write_to_stderr as write
    except ImportError:
        def write(s): local.stderr_writers[-1](s)
    write = staticmethod(write)
pony_stderr = PonyStderr()
try:
    pony_stderr.flush = real_stderr.flush
    pony_stderr.seek = real_stderr.seek
    pony_stderr.readline = real_stderr.readline
except AttributeError: pass
sys.stderr = pony_stderr

@decorator
def grab_stdout(f):
    def new_function(*args, **keyargs):
        data = []
        local.stdout_writers.append(data.append)
        # The next line required for PythonWin interactive window
        # (PythonWin resets stdout all the time)
        sys.stdout = pony_stdout
        try: result = f(*args, **keyargs)
        finally:
            if local.stdout_writers.pop() != data.append: raise AssertionError
        if result is None: return data
        if not isinstance(result, basestring):
            if hasattr(result, '__unicode__'): result = unicode(result)
            else: result = str(result)
        return (result,)
    return new_function

################################################################################

shutdown = False

shutdown_list = []

def on_shutdown(func):
    if func not in shutdown_list: shutdown_list.append(func)
    return func

def exitfunc():
    mainloop()
    _shutdown()
    prev_func()

if MODE in ('INTERACTIVE', 'GAE-SERVER', 'GAE-LOCAL'): pass
elif hasattr(threading, '_shutdown'):
    prev_func = threading._shutdown
    threading._shutdown = exitfunc
else:
    prev_func = sys.exitfunc
    sys.exitfunc = exitfunc

mainloop_counter = count()

_do_mainloop = False

def mainloop():
    if not _do_mainloop or MODE != 'CHERRYPY' or mainloop_counter.next(): return
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
