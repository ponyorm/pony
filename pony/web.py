import re, threading, os.path, inspect, cgi, urllib, sys, cStringIO, cgitb

from pony.utils import decorator_with_params
from pony.templating import Html

re_component = re.compile("""
        [$]
        (?: (\d+)              # param number (group 1)
        |   ([A-Za-z_]\w*)     # param identifier (group 2)
        )$
    |   (                      # path component (group 3)
            (?:[$][$] | [^$])*
        )$                     # end of string
    """, re.VERBOSE)

@decorator_with_params
def http(url=None, ext=None, **params):
    params = dict([ (name.replace('_', '-').title(), value)
                    for name, value in params.items() ])
    def new_decorator(old_func):
        real_url = url is None and old_func.__name__ or url
        register_http_handler(old_func, real_url, ext, params)
        return old_func
    return new_decorator

def register_http_handler(func, url, ext, params):
    return HttpInfo(func, url, ext, params)

class HttpInfo(object):
    registry = ({}, [])
    def __init__(self, func, url, ext, params):
        self.func = func
        self.url = url
        self.ext = []
        self.params = params
        if '?' in url: path, query = url.split('?', 1)
        else: path, query = url, None
        path, _ext = os.path.splitext(path)
        if _ext: self.ext.append(_ext)
        if isinstance(ext, basestring): self.ext.append(ext)
        elif ext is not None: self.ext.extend(ext)
        if not self.ext: self.ext.append('')
        if not hasattr(func, 'argspec'):
            func.argspec = self.getargspec(func)
            func.dummy_func = self.create_dummy_func(func)
        self.args = set()
        self.keyargs = set()
        self.parsed_path = self.parse_path(path)
        self.parsed_query = self.parse_query(query)
        self.check()
        self.register()
        func.__dict__.setdefault('http', []).insert(0, self)
    @staticmethod
    def getargspec(func):
        original_func = getattr(func, 'original_func', func)
        names,argsname,keyargsname,defaults = inspect.getargspec(original_func)
        names = list(names)
        if defaults is None: new_defaults = []
        else: new_defaults = list(defaults)
        for i, value in enumerate(new_defaults):
            if value is not None and not isinstance(value, unicode):
                new_defaults[i] = unicode(value)
        return names, argsname, keyargsname, new_defaults
    @staticmethod
    def create_dummy_func(func):
        spec = inspect.formatargspec(*func.argspec)[1:-1]
        source = "lambda %s: __locals__()" % spec
        return eval(source, dict(__locals__=locals))
    def parse_path(self, path):
        components = path.split('/')
        if not components[0]: components = components[1:]
        return map(self.parse_component, components)
    def parse_query(self, query):
        if query is None: return []
        params = query.split('&')
        result = []
        for param in params:
            if '=' in param: name, value = param.split('=', 1)
            else: name, value = param, ''
            is_param, x = self.parse_component(value)
            result.append((name, is_param, x))
        return result
    def parse_component(self, component):
        match = re_component.match(component)
        if not match: raise ValueError('Invalid url component: %r' % component)
        i = match.lastindex
        if i == 1: return True, self.adjust(int(match.group(i)) - 1)
        elif i == 2: return True, self.adjust(match.group(i))
        elif i == 3: return False, match.group(i).replace('$$', '$')
        else: assert False
    def adjust(self, x):
        names, argsname, keyargsname, defaults = self.func.argspec
        args, keyargs = self.args, self.keyargs
        if isinstance(x, int):
            if x < 0 or x >= len(names) and argsname is None:
                raise TypeError('Invalid parameter index: %d' % (x+1))
            if x in args:
                raise TypeError('Parameter index %d already in use' % (x+1))
            args.add(x)
            return x
        elif isinstance(x, basestring):
            try: i = names.index(x)
            except ValueError:
                if keyargsname is None or x in keyargs:
                    raise TypeError('Invalid parameter name: %s' % x)
                keyargs.add(x)
                return x
            else:
                if i in args: raise TypeError(
                    'Parameter name %s already in use' % x)
                args.add(i)
                return i
        assert False
    def check(self):
        names, argsname, keyargsname, defaults = self.func.argspec
        args, keyargs = self.args, self.keyargs
        for i, name in enumerate(names[:len(names)-len(defaults)]):
            if i not in args:
                raise TypeError('Undefined path parameter: %s' % name)
        if args:
            for i in range(len(names), max(args)):
                if i not in args:
                    raise TypeError('Undefined path parameter: %d' % (i+1))
    def register(self):
        dict, list = self.registry
        for is_param, x in self.parsed_path:
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
            url = build_url(info, func, args, keyargs)
        except PathError: pass
        else: break
    else:
        raise PathError('Suitable url path for %s() not found' % func.__name__)
    return url

def build_url(info, func, args, keyargs):
    try: keyparams = func.dummy_func(*args, **keyargs).copy()
    except TypeError, e:
        raise TypeError(e.args[0].replace('<lambda>', func.__name__))
    names, argsname, keyargsname, defaults = func.argspec
    indexparams = map(keyparams.pop, names)
    indexparams.extend(keyparams.pop(argsname, ()))
    for i, value in enumerate(indexparams):
        if value is not None and not isinstance(value, unicode):
            indexparams[i] = unicode(value)
    keyparams.update(keyparams.pop(keyargsname, {}))
    for key, value in keyparams.items():
        if value is not None and not isinstance(value, unicode):
            keyparams[key] = unicode(value)
    path = []
    used_indexparams = set()
    used_keyparams = set()
    offset = len(names) - len(defaults)

    def build_param(x):
        if isinstance(x, int):
            value = indexparams[x]
            used_indexparams.add(x)
            is_default = offset <= x < len(names) and defaults[x - offset] == value
            return is_default, value
        if isinstance(x, basestring):
            try: value = keyparams[x]
            except KeyError: assert False, 'Parameter not found: %s' % x
            used_keyparams.add(x)
            return False, value
        assert False

    for is_param, x in info.parsed_path:
        if not is_param: path.append(x)
        else:
            is_default, value = build_param(x)
            if value is None: raise PathError('Value for parameter %s is None' % x)
            path.append(value)
    path = u'/'.join(path)

    query = []
    for name, is_param, x in info.parsed_query:
        if not is_param: query.append((name, x))
        else:
            is_default, value = build_param(x)
            if not is_default:
                if value is None: raise PathError('Value for parameter %s is None' % x)
                query.append((name, value))
    query = urllib.urlencode(query)

    errmsg = 'Not all parameters were used during path construction'
    if len(used_keyparams) != len(keyparams):
        raise PathError(errmsg)
    if len(used_indexparams) != len(indexparams):
        for i, value in enumerate(indexparams):
            if (i not in used_indexparams
                and value != defaults[i-offset]):
                    raise PathError(errmsg)

    ext = (info.ext + [''])[0]
    if not query: return u'/%s%s' % (path, ext)
    else: return u'/%s%s?%s' % (path, ext, query)

link_template = Html('<a href="%s">%s</a>')

def link(*args, **keyargs):
    description = None
    if isinstance(args[0], basestring):
        description = args[0]
        func = args[1]
        args = args[2:]
    else:
        func = args[0]
        args = args[1:]
        if func.__doc__ is None: description = func.__name__
        else: description = Html(func.__doc__.split('\n', 1)[0])
    href = url(func, *args, **keyargs)
    return link_template % (href, description)

def parse_query(query):
    params = cgi.parse_qs(query)
    for name, value in params.items():
        params[name] = value[0]
    return params

def get_http_handlers(url):
    if '?' in url:
        path, query = url.split('?', 1)
        params = parse_query(query)
    else: path, params = url, {}
    path, ext = os.path.splitext(path)
    components = path.split('/')
    if not components[0]: components = components[1:]
    result = []
    pairs = [ HttpInfo.registry ]
    for i, component in enumerate(components):
        new_pairs = []
        for dict, list in pairs:
            pair = dict.get(component)
            if pair: new_pairs.append(pair)
            pair = dict.get(None)
            if pair: new_pairs.append(pair)
        pairs = new_pairs
    result = []
    not_found = object()
    for dict, list in pairs:
        for info in list:
            if ext not in info.ext: continue
            args, keyargs = {}, {}
            for i, (is_param, x) in enumerate(info.parsed_path):
                if not is_param: continue
                value = components[i]
                if isinstance(x, int): args[x] = value
                elif isinstance(x, basestring): keyargs[x] = value
                else: assert False
            names, _, _, defaults = info.func.argspec
            offset = len(names) - len(defaults)
            for name, is_param, x in info.parsed_query:
                value = params.get(name, not_found)
                if not is_param:
                    if value != x: break
                elif isinstance(x, int):
                    if value is not_found:
                        if offset <= x < len(names): continue
                        else: break
                    else: args[x] = value
                elif isinstance(x, basestring):
                    if value is not_found: break
                    keyargs[x] = value
                else: assert False
            else:
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
    headers = {'Content-Type': 'text/plain'}

class HttpRequest(object):
    def __init__(self, environ):
        self.environ = environ

class HttpResponse(object):
    def __init__(self):
        self.headers = {}

class Local(threading.local):
    def __init__(self):
        self.request = HttpRequest({})
        self.response = HttpResponse()

local = Local()        

def invoke(url):
    response = local.response = HttpResponse()
    handlers = get_http_handlers(url)
    if not handlers:
        raise Http404, 'Page not found'
    info, args, keyargs = handlers[0]
    result = info.func(*args, **keyargs)
    response.headers.update(info.params)
    return result

def format_exc():
    exc_type, exc_value, traceback = sys.exc_info()
    traceback = traceback.tb_next.tb_next
    try:
        io = cStringIO.StringIO()
        hook = cgitb.Hook(file=io)
        hook.handle((exc_type, exc_value, traceback))
        return io.getvalue()
    finally:
        del traceback
    
def wsgi_app(environ, start_response):
    local.request = HttpRequest(environ)
    url = environ['PATH_INFO']
    query = environ['QUERY_STRING']
    if query: url = '%s?%s' % (url, query)
    try:
        result = invoke(url)
    except HttpException, e:
        start_response(e.status, e.headers.items())
        return [ e.args[0] ]
    except:
        # start_response('200 OK', [ ('Content-Type', 'text/plain') ])
        # return [ traceback.format_exc() ]
        start_response('200 OK', [ ('Content-Type', 'text/html') ])
        return [ format_exc() ]
    else:
        response = local.response
        charset = response.headers.pop('Charset', 'UTF-8')
        type = response.headers.pop('Type', 'text/plain')
        if isinstance(result, Html): type = 'text/html'
        if isinstance(result, unicode): result = result.encode(charset)
        response.headers['Content-Type'] = '%s; charset=%s' % (type, charset)
        # print '---'
        # for name, value in response.headers.items():
        #     print '%s: %s' % (name, value)
        start_response('200 OK', response.headers.items())
        return [ result ]

def wsgi_test(environ, start_response):
    from cStringIO import StringIO
    stdout = StringIO()
    print >>stdout, 'Hello world!'
    print >>stdout
    h = environ.items(); h.sort()
    for k,v in h:
        print >>stdout, k,'=',`v`
    start_response('200 OK', [ ('Content-Type', 'text/plain') ])
    return [ stdout.getvalue() ]

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
    