from __future__ import absolute_import, print_function

import os, sys
from os.path import dirname

__version__ = '0.7.11'

def detect_mode():
    try: import google.appengine
    except ImportError: pass
    else:
        if os.environ.get('SERVER_SOFTWARE', '').startswith('Development'):
            return 'GAE-LOCAL'
        return 'GAE-SERVER'

    try: from mod_wsgi import version
    except: pass
    else: return 'MOD_WSGI'

    main = sys.modules['__main__']

    if not hasattr(main, '__file__'): # console
        return 'INTERACTIVE'

    if getattr(main, 'INTERACTIVE_MODE_AVAILABLE', False): # pycharm console
        return 'INTERACTIVE'

    if 'flup.server.fcgi' in sys.modules: return 'FCGI-FLUP'
    if 'uwsgi' in sys.modules: return 'UWSGI'
    if 'flask' in sys.modules: return 'FLASK'
    if 'cherrypy' in sys.modules: return 'CHERRYPY'
    if 'bottle' in sys.modules: return 'BOTTLE'
    return 'UNKNOWN'

MODE = detect_mode()

MAIN_FILE = None
if MODE == 'MOD_WSGI':
    for module_name, module in sys.modules.items():
        if module_name.startswith('_mod_wsgi_'):
            MAIN_FILE = module.__file__
            break
elif MODE != 'INTERACTIVE':
    MAIN_FILE = sys.modules['__main__'].__file__

if MAIN_FILE is not None: MAIN_DIR = dirname(MAIN_FILE)
else: MAIN_DIR = None

PONY_DIR = dirname(__file__)
