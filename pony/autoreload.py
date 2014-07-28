# coding: cp1251

from __future__ import absolute_import, print_function

import linecache, sys, time, os, imp, traceback
from os.path import abspath, basename, dirname, exists, splitext

import pony
from pony.utils import get_mtime, shortened_filename
from pony.logging2 import log, log_exc, ERROR, DEBUG

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
    success = True
    log(type='RELOAD:begin', prefix='RELOADING: ', text=shortened_filename(filename), severity=ERROR,
        module=changed_module.__name__, modules=dict((m.__name__, m.__file__) for m in modules))
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
        log(type='RELOAD:end', severity=DEBUG, success=success,
            text='Reloaded successfully' if success else 'Reloaded with errors')
        reloading = False

reloading_exception = None

def check_files():
    global reloading_exception
    if not reloading_exception:
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
                    # Запоминаем traceback так что мы можем отобразить его
                    # на веб-странице позднее, когда поступит какой-либо HTTP запрос
                    reloading_exception = sys.exc_info()
                else: reloading_exception = None
                break
    except:
        log_exc()
        sys.exit()

def use_autoreload():
    if pony.MODE not in ('CHERRYPY', 'FCGI-FLUP') or next(pony.mainloop_counter): return
    try: load_main()
    except Exception as e:
        pony.exception_in_main = e
        raise
    if not pony._do_mainloop: sys.exit()
    while True:
        if pony.shutdown: sys.exit()
        check_files()
        time.sleep(1)
