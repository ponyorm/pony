from __future__ import absolute_import, print_function

import sys
from os.path import dirname

__version__ = '0.7'

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

    if 'uwsgi' in sys.modules: return 'UWSGI'

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
