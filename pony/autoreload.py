import itertools, linecache, sys, time, os, imp, traceback

from os.path import abspath, basename, dirname, exists, splitext

from pony.logging import log, log_exc

mainfile = getattr(sys.modules['__main__'], '__file__', '')
maindir = abspath(dirname(mainfile)) + os.sep
counter = itertools.count()
mtimes = {}
clear_funcs = []
reloading = False

def load_main():
    name = splitext(basename(mainfile))[0]
    file, filename, description = imp.find_module(name, [ maindir ])
    try: imp.load_module('__main__', file, filename, description)
    finally:
        if file: file.close()

def shortened_module_name(filename):
    try: main = sys.modules['__main__'].__file__
    except AttributeError: return filename
    dir = os.path.dirname(main) + os.sep
    if filename.startswith(dir): return filename[len(dir):]
    return filename

def reload(modules, changed_module, filename):
    global reloading
    reloading = True
    success = True
    print 'RELOADING: %s' % shortened_module_name(filename)
    log('RELOAD:begin', text='Changed: %s' % changed_module.__name__,
        modules=dict((m.__name__, m.__file__) for m in modules))
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
            traceback.print_exc()
            raise
    finally:
        log('RELOAD:end', success=success,
            text=success and 'Reloaded successfully' or 'Reloaded with errors')
        reloading = False

def use_autoreload():
    if counter.next() or not mainfile: return
    load_main()
    error = False
    while True:
        if not error:
            modules = [ m for m in sys.modules.values()
                        if getattr(m, '__file__', '').startswith(maindir) ]
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
                except Exception: error = True
                else: error = False
                break
        time.sleep(1)
