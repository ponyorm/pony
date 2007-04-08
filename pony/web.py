import re, threading, os.path, inspect, sys, cStringIO
import cgi, cgitb, urllib, Cookie

from pony.thirdparty.cherrypy.wsgiserver import CherryPyWSGIServer

from pony import auth
from pony.utils import decorator_with_params
from pony.templating import Html
from pony.logging import log, log_exc

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
    path, query, ext_list = split_url(url, ext, check=True)
    return HttpInfo(func, path, query, ext_list, params)

http_registry_lock = threading.Lock()
http_registry = ({}, [])

def split_url(url, ext=None, check=False):
    if isinstance(url, unicode): url = url.encode('utf8')
    elif isinstance(url, str):
        if check:
            try: url.decode('ascii')
            except UnicodeDecodeError: raise ValueError(
                'Url string contains non-ascii symbols. '
                'Such urls must be in unicode.')
    else: raise ValueError('Url parameter must be str or unicode')
    if '?' in url: path, query = url.split('?', 1)
    else: path, query = url, None
    path, _ext = os.path.splitext(path)
    ext_list = []
    if isinstance(ext, basestring): ext_list.append(ext)
    elif ext is not None: ext_list.extend(ext)
    if _ext and _ext not in ext_list: ext_list.insert(0, _ext)
    if not ext_list: ext_list.append('')
    return path, query, ext_list

class HttpInfo(object):
    def __init__(self, func, path, query, ext_list, params):
        self.func = func
        self.path = path
        self.query = query
        self.ext_list = ext_list
        self.params = params
        if not hasattr(func, 'argspec'):
            func.argspec = self.getargspec(func)
            func.dummy_func = self.create_dummy_func(func)
        self.args = set()
        self.keyargs = set()
        self.parsed_path = self.parse_path()
        self.parsed_query = self.parse_query()
        self.check()
        self.register()
    @staticmethod
    def getargspec(func):
        original_func = getattr(func, 'original_func', func)
        names,argsname,keyargsname,defaults = inspect.getargspec(original_func)
        names = list(names)
        if defaults is None: new_defaults = []
        else: new_defaults = list(defaults)
        try:
            for i, value in enumerate(new_defaults):
                if value is not None:
                    new_defaults[i] = unicode(value).encode('utf8')
        except UnicodeDecodeError:
            raise ValueError('Default value contains non-ascii symbols. '
                             'Such default values must be in unicode.')
        return names, argsname, keyargsname, new_defaults
    @staticmethod
    def create_dummy_func(func):
        spec = inspect.formatargspec(*func.argspec)[1:-1]
        source = "lambda %s: __locals__()" % spec
        return eval(source, dict(__locals__=locals))
    def parse_path(self):
        components = self.path.split('/')
        if not components[0]: components = components[1:]
        return map(self.parse_component, map(urllib.unquote, components))
    def parse_query(self):
        if self.query is None: return []
        params = cgi.parse_qsl(self.query, strict_parsing=True,
                                           keep_blank_values=True)
        result = []
        for name, value in params:
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
        http_registry_lock.acquire()
        try:
            for info, _, _ in get_http_handlers(
                              self.path, self.query, self.ext_list):
                _http_remove(info, self.ext_list)

            dict, list = http_registry
            for is_param, x in self.parsed_path:
                if is_param: dict, list = dict.setdefault(None, ({}, []))
                else: dict, list = dict.setdefault(x, ({}, []))
            self.list = list
            self.func.__dict__.setdefault('http', []).insert(0, self)
            list.append(self)
        finally: http_registry_lock.release()
            
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
    keyparams.update(keyparams.pop(keyargsname, {}))
    try:
        for i, value in enumerate(indexparams):
            if value is not None: indexparams[i] = unicode(value).encode('utf8')
        for key, value in keyparams.items():
            if value is not None: keyparams[key] = unicode(value).encode('utf8')
    except UnicodeDecodeError:
        raise ValueError('Url parameter value contains non-ascii symbols. '
                         'Such values must be in unicode.')
    path = []
    used_indexparams = set()
    used_keyparams = set()
    offset = len(names) - len(defaults)

    def build_param(x):
        if isinstance(x, int):
            value = indexparams[x]
            used_indexparams.add(x)
            is_default = (offset <= x < len(names)
                          and defaults[x - offset] == value)
            return is_default, value
        elif isinstance(x, basestring):
            try: value = keyparams[x]
            except KeyError: assert False, 'Parameter not found: %s' % x
            used_keyparams.add(x)
            return False, value
        else: assert False

    for is_param, x in info.parsed_path:
        if not is_param: component = x
        else:
            is_default, component = build_param(x)
            if component is None:
                raise PathError('Value for parameter %s is None' % x)
        path.append(urllib.quote(component, safe=':@&=+$,'))
    path = '/'.join(path)

    query = []
    for name, is_param, x in info.parsed_query:
        if not is_param: query.append((name, x))
        else:
            is_default, value = build_param(x)
            if not is_default:
                if value is None:
                    raise PathError('Value for parameter %s is None' % x)
                query.append((name, value))
    quote_plus = urllib.quote_plus
    query = "&".join(("%s=%s" % (quote_plus(name), quote_plus(value)))
                     for name, value in query)

    errmsg = 'Not all parameters were used during path construction'
    if len(used_keyparams) != len(keyparams):
        raise PathError(errmsg)
    if len(used_indexparams) != len(indexparams):
        for i, value in enumerate(indexparams):
            if (i not in used_indexparams
                and value != defaults[i-offset]):
                    raise PathError(errmsg)

    ext = info.ext_list[0]
    if not query: return '/%s%s' % (path, ext)
    else: return '/%s%s?%s' % (path, ext, query)

link_template = Html(u'<a href="%s">%s</a>')

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

def get_http_handlers(path, query, ext_list):
    components = map(urllib.unquote, path.split('/'))
    if not components[0]: components = components[1:]
    if query is None: params = {}
    else: params = dict(reversed(cgi.parse_qsl(query)))

    # http_registry_lock.release()
    # try:
    variants = [ http_registry ]
    for i, component in enumerate(components):
        new_variants = []
        for d, list in variants:
            variant = d.get(component)
            if variant: new_variants.append(variant)
            variant = d.get(None)
            if variant: new_variants.append(variant)
        variants = new_variants
    # finally: http_registry_lock.release()

    result = []
    not_found = object()
    for _, list in variants:
        for info in list:
            for ext in ext_list:
                if ext in info.ext_list: break
            else: continue
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

def invoke(url):
    path, query, ext_list = split_url(url)
    local.response = HttpResponse()
    handlers = get_http_handlers(path, query, ext_list)
    if not handlers:
        raise Http404, 'Page not found'
    info, args, keyargs = handlers[0]
    for i, value in enumerate(args):
        if value is not None: args[i] = value.decode('utf8')
    for key, value in keyargs.items():
        if value is not None: keyargs[key] = value.decode('utf8')
    result = info.func(*args, **keyargs)
    local.response.headers.update(info.params)
    return result
http.invoke = invoke

def _http_remove(info, ext_list):
    for ext in ext_list:
        try: info.ext_list.remove(ext)
        except ValueError: pass
    if not info.ext_list:
        info.list.remove(info)
        info.func.http.remove(info)
            
def http_remove(x, ext=None):
    if isinstance(x, basestring):
        path, query, ext_list = split_url(x, ext, check=True)
        http_registry_lock.acquire()
        try:
            for info, _, _ in get_http_handlers(path, query, ext_list):
                _http_remove(info, ext_list)
        finally: http_registry_lock.release()
    elif hasattr(x, 'http'):
        if ext is None: ext_list = []
        elif isinstance(x, basestring): ext_list = [ ext ]
        else: ext_list = list(ext)
        http_registry_lock.acquire()
        try: _http_remove(x, ext_list)
        finally: http_registry_lock.release()
    else: raise ValueError('This object is not bound to url: %r' % x)

http.remove = http_remove

def _http_clear(dict, list):
    for info in list: info.func.http.remove(info)
    list[:] = []
    for dict2, list2 in dict.itervalues(): _http_clear(dict2, list2)
    dict.clear()

def http_clear():
    http_registry_lock.acquire()
    try: _http_clear(*http_registry)
    finally: http_registry_lock.release()

http.clear = http_clear

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

def format_exc():
    exc_type, exc_value, traceback = sys.exc_info()
    if traceback.tb_next: traceback = traceback.tb_next
    if traceback.tb_next: traceback = traceback.tb_next
    try:
        io = cStringIO.StringIO()
        hook = cgitb.Hook(file=io)
        hook.handle((exc_type, exc_value, traceback))
        return io.getvalue()
    finally:
        del traceback

def reconstruct_url(environ):
    url = environ['wsgi.url_scheme']+'://'
    if environ.get('HTTP_HOST'): url += environ['HTTP_HOST']
    else:
        url += environ['SERVER_NAME']
        if environ['wsgi.url_scheme'] == 'https':
            if environ['SERVER_PORT'] != '443':
                url += ':' + environ['SERVER_PORT']
        elif environ['SERVER_PORT'] != '80':
            url += ':' + environ['SERVER_PORT']

    url += urllib.quote(environ.get('SCRIPT_NAME',''))
    url += urllib.quote(environ.get('PATH_INFO',''))
    if environ.get('QUERY_STRING'):
        url += '?' + environ['QUERY_STRING']
    return url

def log_request(environ):
    headers=dict((key, value) for key, value in environ.items()
                              if isinstance(key, basestring)
                              and isinstance(value, basestring))
    log(type='HTTP:%s' % environ.get('REQUEST_METHOD', '?'),
        text=reconstruct_url(environ),
        headers=headers)

def determine_user(environ):
    data = None
    if 'HTTP_COOKIE' in environ:
        c = Cookie.SimpleCookie()
        c.load(environ['HTTP_COOKIE'])
        morsel = c.get('pony')
        if morsel: data = morsel.value
    auth.load(data, environ)

http_only_incompatible_browsers = [ 'WebTV', 'MSIE 5.0; Mac' ]

ONE_MONTH = 60*60*24*31

def create_cookies(environ):
    data, domain, path = auth.save(environ)
    if data is None: return []
    c = Cookie.SimpleCookie()
    c['pony'] = data
    morsel = c['pony']
    morsel['path'] = path or '/'
    if domain: morsel['domain'] = domain
    morsel['max-age'] = ONE_MONTH
    morsel['expires'] = ONE_MONTH
    # if secure: morsel['secure'] = True
    cookie_data = morsel.OutputString()
    user_agent = environ.get('HTTP_USER_AGENT', '')
    for browser in http_only_incompatible_browsers:
        if browser in user_agent: break
    else: cookie_data += ' HttpOnly'
    return [ ('Set-Cookie', cookie_data) ]

def wsgi_app(environ, wsgi_start_response):
    def start_response(status, headers):
        headers = headers.items()
        headers.extend(create_cookies(environ))
        log(type='HTTP:response', text=status, headers=headers)
        wsgi_start_response(status, headers)

    local.request = HttpRequest(environ)
    determine_user(environ)
    url = environ['PATH_INFO']
    query = environ['QUERY_STRING']
    if query: url = '%s?%s' % (url, query)
    try:
        log_request(environ)
        result = invoke(url)
    except HttpException, e:
        start_response(e.status, e.headers)
        return [ e.args[0] ]
    except:
        log_exc()
        start_response('200 OK', {'Content-Type': 'text/html'})
        return [ format_exc() ]
    else:
        response = local.response
        charset = response.headers.pop('Charset', 'UTF-8')
        type = response.headers.pop('Type', 'text/plain')
        if isinstance(result, Html): type = 'text/html'
        if isinstance(result, unicode): result = result.encode(charset)
        elif not isinstance(result, str):
            try: result = str(result)
            except UnicodeEncodeError:
                result = unicode(result, charset, 'replace')
        response.headers['Content-Type'] = '%s; charset=%s' % (type, charset)
        start_response('200 OK', response.headers)
        return [ result ]

def wsgi_test(environ, start_response):
    from cStringIO import StringIO
    stdout = StringIO()
    h = environ.items(); h.sort()
    for k,v in h:
        print >>stdout, k,'=',`v`
    start_response('200 OK', [ ('Content-Type', 'text/plain') ])
    return [ stdout.getvalue() ]

wsgi_apps = [('', wsgi_app), ('/test/', wsgi_test)]

def parse_address(address):
    if isinstance(address, basestring):
        if ':' in address:
            host, port = address.split(':')
            return host, int(port)
        else:
            return address, 80
    assert len(address) == 2
    return tuple(address)

server_threads = {}

class ServerStartException(Exception): pass
class ServerStopException(Exception): pass

class ServerThread(threading.Thread):
    def __init__(self, host, port, wsgi_app, verbose):
        server = server_threads.setdefault((host, port), self)
        if server != self: raise ServerStartException(
            'HTTP server already started: %s:%s' % (host, port))
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.server = CherryPyWSGIServer(
            (host, port), wsgi_apps, server_name=host)
        self.verbose = verbose
        self.setDaemon(False)
    def run(self):
        msg = 'Starting HTTP server at %s:%s' % (self.host, self.port)
        log('HTTP:start', msg)
        if self.verbose: print msg
        self.server.start()
        msg = 'HTTP server at %s:%s stopped successfully' \
              % (self.host, self.port)
        log('HTTP:start', msg)
        if self.verbose: print msg
        server_threads.pop((self.host, self.port), None)

def start_http_server(address='localhost:8080', verbose=True):
    host, port = parse_address(address)
    server_thread = ServerThread(host, port, wsgi_app, verbose=verbose)
    server_thread.start()

def stop_http_server(address=None):
    if address is None:
        for server_thread in server_threads.values():
            server_thread.server.stop()
    else:
        host, port = parse_address(address)
        server_thread = server_threads.get((host, port))
        if server_thread is None: raise ServerStopException(
            'Cannot stop HTTP server at %s:%s '
            'because it is not started:' % (host, port))
        server_thread.server.stop()
    