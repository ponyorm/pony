from __future__ import absolute_import, print_function

import sys
from os.path import dirname

__version__ = '0.8-dev'

def detect_mode():
    try: import google.appengine
    except ImportError: pass
    else:
        try: import dev_appserver
        except ImportError: return 'GAE-SERVER'
        return 'GAE-LOCAL'

    try: from mod_wsgi import version
    except: pass
    else: return 'MOD_WSGI'

    try:
        sys.modules['__main__'].__file__
    except AttributeError:
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
