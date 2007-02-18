import re, os.path, inspect, traceback
import sys

from pony.utils import decorator_with_params

re_param = re.compile("""
        [$]
        (?: (\d+)              # param number (group 1)
        |   ([A-Za-z_]\w*)  # param identifier (group 2)
        )$
    |   (                      # path component (group 3)
            (?:[$][$] | [^$])+
        )$                     # end of string
    """, re.VERBOSE)

@decorator_with_params
def http(path=None, ext=None):
    def new_decorator(old_func):
        if path is None:
            real_path = old_func.__name__
        else:
            real_path = path
        register_http_handler(old_func, real_path, ext)
        return old_func
    return new_decorator

def register_http_handler(func, path, ext):
    return HttpInfo(func, path, ext)

class HttpInfo(object):
    registry = ({}, [])
    def __init__(self, func, urlpath, ext=None):
        self.func = func
        self.urlpath = urlpath
        self.ext = []
        path, urlext = os.path.splitext(urlpath)
        if urlext: self.ext.append(urlext)
        if isinstance(ext, basestring): self.ext.append(ext)
        elif ext is not None: self.ext.extend(ext)
        if not self.ext: self.ext.append('')
        if not hasattr(func, 'argspec'):
            func.argspec = self.getargspec(func)
            func.dummy_func = self.create_dummy_func(func)
            func.default_dict = self.get_default_dict(func)
        self.parsed_path = self.parse_path(path)
        self.adjust_path(self.parsed_path, func)
        self.register(self.parsed_path)
        func.__dict__.setdefault('http', []).insert(0, self)
    @staticmethod
    def getargspec(func):
        original_func = getattr(func, 'original_func', func)
        names,argsname,keyargsname,defaults = inspect.getargspec(original_func)
        if argsname: raise TypeError(
            'HTTP handler function cannot have *%s argument' % argsname)
        if defaults is None: new_defaults = []
        else: new_defaults = list(defaults)
        for i, value in enumerate(new_defaults):
            if value is not None: new_defaults[i] = unicode(value)
        return names, argsname, keyargsname, new_defaults
    @staticmethod
    def create_dummy_func(func):
        spec = inspect.formatargspec(*func.argspec)[1:-1]
        source = "lambda %s: __locals__()" % spec
        return eval(source, dict(__locals__=locals))
    @staticmethod
    def get_default_dict(func):
        names, argsname, keyargsname, defaults = func.argspec
        defaults = list(defaults)
        for i, value in enumerate(defaults):
            if value is not None and not isinstance(value, basestring):
                defaults[i] = unicode(value)
        names_with_defaults = list(names[-len(defaults):])
        return dict(zip(names_with_defaults, defaults))
    @staticmethod
    def parse_path(path):
        result = []
        args, keyargs = set(), set()
        components = path.split('/')
        if not components[0]: components = components[1:]
        for component in components:
            match = re_param.match(component)
            if not match: raise ValueError(
                'Invalid path component: '
                + (not component and '<empty string>' or component))
            i = match.lastindex
            if i == 1:
                param = int(match.group(1)) - 1
                if param < 0 or param in args:
                    raise ValueError('Invalid path parameters: %s' % path)
                args.add(param)
                result.append((True, param))
            elif i == 2:
                param = match.group(2)
                if param in keyargs:
                    raise ValueError('Invalid path parameters: %s' % path)
                keyargs.add(param)
                result.append((True, param))
            elif i == 3:
                result.append((False, match.group(3).replace('$$', '$')))
            else: assert False
        if args and max(args) > len(args):
            raise ValueError('Invalid path parameters: %s' % path)
        return result
    @staticmethod
    def adjust_path(parsed_path, func):
        names, argsname, keyargsname, defaults = func.argspec
        param_names = set()
        for i, (is_param, x) in enumerate(parsed_path):
            if not is_param: continue
            if isinstance(x, int):
                try: param_name = names[x]
                except IndexError:
                    if argsname is None:
                        raise TypeError('Invalid path parameter index: %s' % x)
                    parsed_path[i] = (True, x - len(names))
                else:
                    parsed_path[i] = (True, param_name)
                    param_names.add(param_name)
            elif isinstance(x, basestring):
                if x not in names and keyargsname is None:
                    raise TypeError('Invalid parameter name: %s' % x)
                param_names.add(x)
            else: assert False
        param_names.update(func.default_dict)
        for arg in names:
            if arg not in param_names: raise TypeError(
                'There are no specified value for argument %s' % arg)
    def register(self, parsed_path):
        dict, list = self.registry
        for is_param, x in parsed_path:
            if is_param: dict, list = dict.setdefault(None, ({}, []))
            else: dict, list = dict.setdefault(x, ({}, []))
        list.append(self)

class PathError(Exception): pass

def url(func, *args, **keyargs):
    http_list = getattr(func, 'http')
    if http_list is None:
        raise ValueError('Cannot create url for this object :%s' % func)
    for info in http_list:
        try:
            path = build_path(info.parsed_path, func, args, keyargs)
        except PathError: pass
        else: break
    else:
        raise PathError('Suitable url path for %s() not found' % func.__name__)
    return '/%s%s' % (path, (info.ext + [''])[0])

def build_path(parsed_path, func, args, keyargs):
    try:
        keyparams = func.dummy_func(*args, **keyargs).copy()
    except TypeError, e:
        raise TypeError(e.args[0].replace('<lambda>', func.__name__))
    indexparams = keyparams.get(func.argspec[1], ())
    keyparams.update(keyparams.get(func.argspec[2], {}))
    if func.argspec[1] is not None: del keyparams[func.argspec[1]]
    if func.argspec[2] is not None: del keyparams[func.argspec[2]]
    result = []
    used = set()
    for is_param, x in parsed_path:
        if not is_param:
            result.append(x)
            continue
        elif isinstance(x, basestring):
            try: value = keyparams[x]
            except KeyError: assert False, 'Parameter not found: %s' % x
            used.add(x)
        elif isinstance(x, int):
            value = indexparams[x]
            used.add(x)
        else: assert False
        if value is None: raise PathError('Value for parameter %s is None' % x)
        if not isinstance(value, unicode): value = unicode(value)
        result.append(value)
    not_none_params = set(key for (key, value) in keyparams.iteritems()
                              if value is not None)
    not_none_indexes = set(i for i, value in enumerate(indexparams)
                             if value is not None)
    if used.issuperset(not_none_params) and used.issuperset(not_none_indexes):
        return u'/'.join(result)
    raise PathError('Not all parameters were used in path construction')

def get_http_handlers(urlpath):
    path, ext = os.path.splitext(urlpath)
    components = path.split('/')
    if not components[0]: components = components[1:]
    result = []
    triples = [ HttpInfo.registry + ({},) ]
    for i, component in enumerate(components):
        new_triples = []
        for dict, list, params in triples:
            pair = dict.get(component)
            if pair:
                new_params = params.copy()
                new_triples.append(pair + (new_params,))
            pair = dict.get(None)
            if pair:
                new_params = params.copy()
                new_params[i] = component
                new_triples.append(pair + (new_params,))
        triples = new_triples
    result = []
    for dict, list, params in triples:
        for info in list:
            if ext in info.ext:
                args, keyargs = {}, {}
                for i, value in params.items():
                    is_param, key = info.parsed_path[i]
                    assert is_param
                    if isinstance(key, int): args[key] = value
                    else: keyargs[key] = value
                args = [ args[i] for i in range(len(args)) ]
                result.append((info, args, keyargs))
    return result

def wsgi_test(environ, start_response):
    from StringIO import StringIO
    stdout = StringIO()
    print >>stdout, "Hello world!"
    print >>stdout
    h = environ.items(); h.sort()
    for k,v in h:
        print >>stdout, k,'=',`v`
    start_response("200 OK", [ ('Content-Type', 'text/plain') ])
    return [ stdout.getvalue() ]

def wsgi_app(environ, start_response):
    urlpath = environ['PATH_INFO']
    handlers = get_http_handlers(urlpath)
    if not handlers:
        start_response("200 OK", [ ('Content-Type', 'text/plain') ])
        return [ "Requested page not found!" ]
    info, args, keyargs = handlers[0]
    try:
        result = info.func(*args, **keyargs)
        if isinstance(result, unicode): result = result.encode('utf8')
    except:
        start_response("200 OK", [ ('Content-Type', 'text/plain') ])
        return [ traceback.format_exc() ]
    start_response("200 OK", [ ('Content-Type','text/html') ])
    return [ result ]

def parse_address(address):
    if isinstance(address, basestring):
        if ':' in address:
            host, port = address.split(':')
            return host, int(port)
        else:
            return address, 80
    assert len(address) == 2
    return tuple(address)

def start_http_server(address):
    host, port = parse_address(address)
    from pony.thirdparty.cherrypy.wsgiserver import CherryPyWSGIServer
    wsgi_apps = [("", wsgi_app), ("/test/", wsgi_test)]
    server = CherryPyWSGIServer((host, port), wsgi_apps, server_name=host)
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()

run_http_server = start_http_server
    