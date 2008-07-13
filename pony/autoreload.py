import itertools, linecache, sys, time, os, imp, traceback

from os.path import abspath, basename, dirname, exists, splitext

import pony
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

def shortened_module_name(filename):
    if pony.MAIN_DIR is None: return filename
    maindir = pony.MAIN_DIR + os.sep
    if filename.startswith(maindir): return filename[len(maindir):]
    return filename

def reload(modules, changed_module, filename):
    global reloading
    reloading = True
    success = True
    module_name = shortened_module_name(filename)
    if pony.logging.verbose: print>>sys.stderr, 'RELOADING: %s' % module_name
    log(type='RELOAD:begin', prefix='RELOADING: ', text=module_name, severity=ERROR,
        module=changed_module.__name__, modules=dict((m.__name__, m.__file__) for m in modules))
    try:
        try:
            for clear_func in clear_funcs: clear_func()
            mtimes.clear()
            linecache.checkcache()
            for m in modules: sys.modules.pop(m.__name__, None)
            load_main()
        except Exception:
            success = False
            log_exc()
            raise
    finally:
        log(type='RELOAD:end', severity=DEBUG,
            text=success and 'Reloaded successfully' or 'Reloaded with errors', success=success)
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
                stat = os.stat(filename)
                mtime = stat.st_mtime
                if sys.platform == "win32": mtime -= stat.st_ctime
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
