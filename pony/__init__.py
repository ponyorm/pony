import sys, time, threading, random
from os.path import dirname
from itertools import count

__version__ = '0.5-beta'

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
        try: log_exc = logging2.log_exc
        except NameError: pass
        else: log_exc()

shutdown_counter = count()

def _shutdown():
    global shutdown
    shutdown = True
    if shutdown_counter.next(): return
    for func in reversed(shutdown_list): func()
