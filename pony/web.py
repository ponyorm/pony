import re, os.path, inspect, traceback
import sys

from pony.utils import decorator_with_params

re_param = re.compile("""
        [$]
        (?: (\d+)              # param number (group 1)
        |   ([A-Za-z_]\w*)     # param identifier (group 2)
        |   (\*)               # param list (group 3)
        )$
    |   (                      # path component (group 4)
            (?:[$][$] | [^$])*
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
        components = path.split('/')
        if not components[0]: components = components[1:]
        for component in components:
            match = re_param.match(component)
            if not match:
                raise ValueError('Invalid path component: %r' % component)
            i = match.lastindex
            if i == 1:
                param = int(match.group(i)) - 1
                result.append((True, param))
            elif i == 2:
                param = match.group(i)
                result.append((True, param))
            elif i == 3:
                result.append((True, '*'))
            elif i == 4:
                result.append((False, match.group(i).replace('$$', '$')))
            else: assert False
        return result
    @staticmethod
    def adjust_path(parsed_path, func):
        names, argsname, keyargsname, defaults = func.argspec
        names = list(names)
        args, keyargs = set(), set()
        param_names = set()
        for i, (is_param, x) in enumerate(parsed_path):
            if not is_param: continue
            if isinstance(x, int):
                if x < 0 or x >= len(names) and argsname is None:
                    raise TypeError('Invalid parameter index: %d' % (x+1))
                if x in args:
                    raise TypeError('Parameter index %d already in use' % (x+1))
                args.add(x)
            elif isinstance(x, basestring):
                if x == '*':
                    pass
                else:
                    try: j = names.index(x)
                    except ValueError:
                        if keyargsname is None or x in keyargs:
                            raise TypeError('Invalid parameter name: %s' % x)
                        keyargs.add(x)
                    else:
                        if j in args: raise TypeError(
                            'Parameter name %s already in use' % x)
                        args.add(j)
                        parsed_path[i] = (True, j)
            else: assert False
        for i, name in enumerate(names[:len(names)-len(defaults)]):
            if i not in args:
                raise TypeError('Undefined path parameter: %s' % name)
        if args:
            for i in range(len(names), max(args)):
                if i not in args:
                    raise TypeError('Undefined path parameter: %d' % (i+1))
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
    indexparams = map(keyparams.pop, func.argspec[0])
    indexparams.extend(keyparams.pop(func.argspec[1], ()))
    keyparams.update(keyparams.pop(func.argspec[2], {}))
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
                argspec = info.func.argspec
                names, defaults = argspec[0], argspec[3]
                arglist = [ None ] * len(names)
                arglist[-len(defaults):] = defaults
                for i, value in sorted(args.items()):
                    try: arglist[i] = value
                    except IndexError:
                        assert i == len(arglist)
                        arglist.append(value)
                result.append((info, arglist, keyargs))
    return result

class HttpException(Exception): pass
class Http404(HttpException):
    status = '404 Not Found'
    headers = [ ('Content-Type', 'text/plain') ]

def invoke(urlpath):
    handlers = get_http_handlers(urlpath)
    if not handlers: raise Http404, 'Page not found'
    info, args, keyargs = handlers[0]
    return info.func(*args, **keyargs)

def wsgi_test(environ, start_response):
    from StringIO import StringIO
    stdout = StringIO()
    print >>stdout, 'Hello world!'
    print >>stdout
    h = environ.items(); h.sort()
    for k,v in h:
        print >>stdout, k,'=',`v`
    start_response('200 OK', [ ('Content-Type', 'text/plain') ])
    return [ stdout.getvalue() ]

def wsgi_app(environ, start_response):
    urlpath = environ['PATH_INFO']
    try:
        result = invoke(urlpath)
    except HttpException, e:
        start_response(e.status, e.headers)
        return [ e.args[0] ]
    except:
        start_response('200 OK', [ ('Content-Type', 'text/plain') ])
        return [ traceback.format_exc() ]
    else:
        if isinstance(result, unicode): result = result.encode('utf8')
        start_response('200 OK', [ ('Content-Type', 'text/plain') ])
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
    wsgi_apps = [('', wsgi_app), ('/test/', wsgi_test)]
    server = CherryPyWSGIServer((host, port), wsgi_apps, server_name=host)
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()

run_http_server = start_http_server
    