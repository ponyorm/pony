import itertools, linecache, sys, time, os, imp, traceback

from os.path import abspath, basename, dirname, exists, splitext

import pony
from pony.utils import get_mtime, shortened_filename
from pony.logging import log, log_exc, ERROR, DEBUG

USE_AUTORELOAD = True

mtimes = {}
clear_funcs = []
reloading = False

def on_reload(func):
    if func not in clear_funcs: clear_funcs.append(func)
    return func

def load_main():
    name, ext = splitext(basename(pony.MAIN_FILE))
    file, filename, description = imp.find_module(name, [ pony.MAIN_DIR ])
    try: imp.load_module('__main__', file, filename, description)
    finally:
        if file: file.close()

def reload(modules, changed_module, filename):
    global reloading
    reloading = True
    log(type='RELOAD:begin', prefix='RELOADING: ', text=shortened_filename(filename), severity=ERROR,
        module=changed_module.__name__, modules=dict((m.__name__, m.__file__) for m in modules))
    erroneous = set()
    try:
        try:
            for clear_func in clear_funcs: clear_func()
            mtimes.clear()
            linecache.checkcache()
            for m in modules: sys.modules.pop(m.__name__, None)
            load_main()
        except Exception:
            erroneous.add(m.__file__)
            log_exc()
            raise
    finally:
        log(type='RELOAD:end', severity=DEBUG, text=erroneous and 'Reloaded with errors' or 'Reloaded successfully',
            success=not erroneous, erroneous=erroneous)
        reloading = False

def use_autoreload():
    if pony.MODE != 'CHERRYPY' or pony.mainloop_counter.next(): return
    load_main()
    error = False
    while True:
        if pony.shutdown: sys.exit()
        if not error:
            modules = [ m for name, m in sys.modules.items()
                        if getattr(m, 'USE_AUTORELOAD', False)
                           and (name.startswith('pony.examples.')
                                or not name.startswith('pony.')) ]
        try:
            for m in modules:
                filename = abspath(m.__file__)
                if filename.endswith(".pyc") or filename.endswith(".pyo"):
                    filename = filename[:-1]
                if not exists(filename): continue
                mtime = get_mtime(filename)
                if mtimes.setdefault(filename, mtime) != mtime:
                    try: reload(modules, m, filename)
                    except Exception:
                        error = True
                        traceback.print_exc()
                    else: error = False
                    break
            time.sleep(1)
        except:
            log_exc()
            sys.exit()
