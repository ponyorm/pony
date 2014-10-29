from __future__ import absolute_import, print_function
from pony.py23compat import izip

import re, threading, os.path, sys, cgi, urllib, Cookie, cPickle, time

from cStringIO import StringIO
from itertools import count
from operator import attrgetter
from bdb import BdbQuit

import pony

from pony import routing, autoreload, auth, httputils, options, middleware
from pony.utils import decorator, decorator_with_params, tostring, localbase
from pony.templating import html, Html, StrHtml
from pony.postprocessing import postprocess
from pony.logging2 import log, log_exc, DEBUG, INFO, WARNING
from pony.debugging import format_exc

class HttpException(Exception):
    content = ''
    headers = {}

class Http4xxException(HttpException):
    pass

class Http400BadRequest(Http4xxException):
    status = '400 Bad Request'
    def __init__(exc, content='Bad Request'):
        Exception.__init__(exc, 'Bad Request')
        exc.content = content

welcome_template_filename = os.path.join(pony.PONY_DIR, 'welcome.html')
notfound_template_filename = os.path.join(pony.PONY_DIR, 'notfound.html')

class Http404NotFound(Http4xxException):
    status = '404 Not Found'
    def __init__(exc, msg='Page not found', content=None):
        Exception.__init__(exc, msg)
        if content: pass
        elif not routing.user_routes:
              content = html(filename=welcome_template_filename)
        else: content = html(filename=notfound_template_filename)
        exc.content = content or msg

class Http405MethodNotAllowed(Http4xxException):
    status = '405 Method Not Allowed'
    headers = {'Allow' : 'GET, HEAD'}
    def __init__(exc, msg='Method not allowed', content=None):
        Exception.__init__(exc, msg)
        exc.content = content or msg

class HttpRedirect(HttpException):
    status_dict = {'301' : '301 Moved Permanently',
                   '302' : '302 Found',
                   '303' : '303 See Other',
                   '305' : '305 Use Proxy',
                   '307' : '307 Temporary Redirect'}
    def __init__(exc, location=None, status='302 Found'):
        if location and not isinstance(location, basestring):
            raise TypeError('Redirect location must be string. Got: %r' % location)
        Exception.__init__(exc, location)
        exc.location = location or local.request.full_url
        status = str(status)
        exc.status = exc.status_dict.get(status, status)
        exc.headers = {'Location' : location}

class HttpRequest(object):
    def __init__(request, environ):
        request.environ = environ
        request.method = environ.get('REQUEST_METHOD', 'GET')
        request.cookies = Cookie.SimpleCookie()
        if 'HTTP_COOKIE' in environ: request.cookies.load(environ['HTTP_COOKIE'])
        if environ:
            http_host = environ.get('HTTP_HOST')
            if http_host:
                if ':' in http_host: host, port = http_host.split(':')
                else: host, port = http_host, 80
            else:
                host = environ['SERVER_NAME']
                port = environ['SERVER_PORT']
            request.host, request.port = host, int(port)

            request.url = urllib.quote(environ['PATH_INFO'])
            query = environ['QUERY_STRING']
            if query: request.url += '?' + query
            request.script_url = httputils.reconstruct_script_url(environ)
            request.full_url = request.script_url + request.url
        else:
            request.script_url = ''
            request.url = '/'
            request.full_url = 'http://localhost/'
            request.host = 'localhost'
            request.port = 80
        request.languages = request._get_languages()
        request.params = {}
        input_stream = environ.get('wsgi.input') or StringIO()
        request.fields = cgi.FieldStorage(fp=input_stream, environ=environ, keep_blank_values=True)
        request.form_processed = None
        request.submitted_form = request.fields.getfirst('_f')
    def _get_languages(request):
        languages = httputils.parse_accept_language(request.environ.get('HTTP_ACCEPT_LANGUAGE'))
        try: languages.insert(0, auth.local.session['lang'])
        except KeyError: pass
        result = []
        for lang in languages:
            try: result.remove(lang)
            except ValueError: pass
            result.append(lang)
            while '-' in lang:
                lang = lang.rsplit('-', 1)[0]
                try: result.remove(lang)
                except ValueError: pass
                result.append(lang)
        return result

class HttpResponse(object):
    def __init__(response):
        response.status = '200 OK'
        response.headers = {}
        response.cookies = Cookie.SimpleCookie()
        response.postprocessing = True
        response.base_stylesheets = []
        response.component_stylesheets = []
        response.scripts = []
        response.id_counter = ('id_%d' % i for i in count())
    def add_base_stylesheets(response, links):
        stylesheets = response.base_stylesheets
        for link in links:
            if not isinstance(link, (basestring, tuple)): raise TypeError('Reference to CSS stylesheet must be string or tuple. Got: %r' % link)
            if link not in stylesheets: stylesheets.append(link)
    def add_component_stylesheets(response, links):
        stylesheets = response.component_stylesheets
        for link in links:
            if not isinstance(link, (basestring, tuple)): raise TypeError('Reference to CSS stylesheet must be string or tuple. Got: %r' % link)
            if link not in stylesheets: stylesheets.append(link)
    def add_scripts(response, links):
        scripts = response.scripts
        for link in links:
            if not isinstance(link, basestring): raise TypeError('Reference to script must be string. Got: %r' % link)
            if link not in scripts: scripts.append(link)

def url(func, *args, **kwargs):
    routes = getattr(func, 'routes')
    if routes is None: raise ValueError('Cannot create url for this object :%s' % func)
    try: keyparams = func.dummy_func(*args, **kwargs).copy()
    except TypeError as e: raise TypeError(e.args[0].replace('<lambda>', func.__name__, 1))
    names, argsname, keyargsname, defaults, converters = func.argspec
    indexparams = [ keyparams.pop(name) for name in names ]
    indexparams.extend(keyparams.pop(argsname, ()))
    keyparams.update(keyparams.pop(keyargsname, {}))
    try:
        for i, value in enumerate(indexparams):
            if value is not None and value is not routing.__nodefault__:
                indexparams[i] = unicode(value).encode('utf8')
        for key, value in keyparams.items():
            if value is not None: keyparams[key] = unicode(value).encode('utf8')
    except UnicodeDecodeError: raise ValueError(
        'Url parameter value contains non-ascii symbols. Such values must be in unicode.')
    request = local.request
    host, port = request.host, request.port
    script_name = local.request.environ.get('SCRIPT_NAME', '')
    key = func, tuple(indexparams), tuple(sorted(keyparams.items())), host, port, script_name
    try: return routing.url_cache[key]
    except KeyError: pass
    first, second = [], []
    for route in routes:
        if not route.redirect: first.append(route)
        else: second.append(route)
    for route in first + second:
        try: url = routing.build_url(route, keyparams, indexparams, host, port, script_name)
        except routing.PathError: pass
        else: break
    else: raise routing.PathError('Suitable url path for %s() not found' % func.__name__)
    if len(routing.url_cache) > 4000: routing.url_cache.clear()
    routing.url_cache[key] = url
    return url
make_url = url

class Local(localbase):
    def __init__(local):
        local.request = HttpRequest({})
        local.response = HttpResponse()
        local.no_cookies = False

local = Local()

def get_cookie(name, default=None):
    morsel = local.request.cookies.get(name)
    if morsel is None: return default
    return morsel.value

def set_cookie(name, value, expires=None, max_age=None, path=None, domain=None,
               secure=False, http_only=False, comment=None, version=None):
    httputils.set_cookie(local.response.cookies,
                        name, value, expires, max_age, path, domain, secure, http_only, comment, version)

@decorator
def no_cookies(func, *args, **kwargs):
    local.no_cookies = True
    return func(*args, **kwargs)

path_re = re.compile(r"^[-_.!~*'()A-Za-z0-9]+$")

pony_static_dir = os.path.join(os.path.dirname(__file__), 'static')

@no_cookies
def get_static_file(path, dir=None, max_age=10):
    if not path: raise Http404NotFound
    if dir is None: dir = options.STATIC_DIR
    if dir is None:
        if pony.MAIN_DIR is None: raise Http404NotFound
        dir = os.path.join(pony.MAIN_DIR, 'static')
    for component in path:
        if not path_re.match(component): raise Http404NotFound
    fname = os.path.join(dir, *path)
    if not os.path.isfile(fname):
        if path == [ 'favicon.ico' ]: return get_static_file(path, pony_static_dir, 30*60)
        raise Http404NotFound
    method = local.request.method
    if method not in ('GET', 'HEAD'): raise Http405MethodNotAllowed
    ext = os.path.splitext(path[-1])[1]
    headers = local.response.headers
    headers['Content-Type'] = httputils.guess_type(ext)
    if max_age <= 60: headers['Expires'] = '0'
    else: headers['Expires'] = Cookie._getdate(max_age)
    headers['Cache-Control'] = 'max-age=%d' % max_age
    headers['Content-Length'] = str(os.path.getsize(fname))
    if method == 'HEAD': return ''
    return file(fname, 'rb')

@decorator
def normalize_result_decorator(func, *args, **kwargs):
    content, headers = normalize_result(func(*args, **kwargs), local.response.headers)
    local.response.headers = headers
    return content

def normalize_result(result, headers):
    if hasattr(result, 'read'): content = result  # file-like object
    else: content = tostring(result)
    headers = dict([ (name.replace('_', '-').title(), str(value))
                     for name, value in headers.items() ])
    media_type = headers.pop('Type', None)
    charset = headers.pop('Charset', None)
    content_type = headers.get('Content-Type')
    if content_type:
        media_type, type_params = cgi.parse_header(content_type)
        charset = type_params.get('charset', 'iso-8859-1')
    else:
        if media_type is None: media_type = getattr(result, 'media_type', None)
        if media_type is None:
            if isinstance(content, (Html, StrHtml)): media_type = 'text/html'
            else: media_type = 'text/plain'
        if charset is None: charset = getattr(result, 'charset', 'UTF-8')
        content_type = '%s; charset=%s' % (media_type, charset)
        headers['Content-Type'] = content_type
    if hasattr(content, 'read') \
       or media_type != 'text/html' \
       or isinstance(content, (Html, StrHtml)): pass
    elif isinstance(content, unicode): content = Html(content)
    elif isinstance(content, str): content = StrHtml(content)
    else: assert False  # pragma: no cover
    return content, headers

def invoke(url):
    if isinstance(url, str):
        try: url.decode('utf8')
        except UnicodeDecodeError: raise Http400BadRequest
    request = local.request
    response = local.response
    path, qlist = httputils.split_url(url)
    if path[:1] == ['static'] and len(path) > 1:
        return get_static_file(path[1:])
    if path[:2] == ['pony', 'static'] and len(path) > 2:
        return get_static_file(path[2:], pony_static_dir, 30*60)
    qdict = dict(qlist)
    routes = routing.get_routes(path, qdict, request.method, request.host, request.port)
    if routes: pass
    elif request.method in ('GET', 'HEAD'):
        i = url.find('?')
        if i == -1: p, q = url, ''
        else: p, q = url[:i], url[i:]
        if p.endswith('/'): url2 = p[:-1] + q
        else: url2 = p + '/' + q
        path2, qlist = httputils.split_url(url2)
        routes = routing.get_routes(path2, qdict, request.method, request.host, request.port)
        if not routes: return get_static_file(path)
        script_name = request.environ.get('SCRIPT_NAME', '')
        url2 = script_name + url2 or '/'
        if url2 != script_name + url: raise HttpRedirect(url2)
    else:
        routes = routing.get_routes(path, qdict, 'GET', request.host, request.port)
        if routes: raise Http405MethodNotAllowed
        raise Http404NotFound

    route, args, kwargs = routes[0]

    if route.redirect:
        for alternative in route.func.routes:
            if not alternative.redirect:
                new_url = make_url(route.func, *args, **kwargs)
                status = '301 Moved Permanently'
                if isinstance(route.redirect, basestring): status = route.redirect
                elif isinstance(route.redirect, (int, long)) and 300 <= route.redirect < 400:
                    status = str(route.redirect)
                raise HttpRedirect(new_url, status)
    response.headers.update(route.headers)

    names, argsname, keyargsname, defaults, converters = route.func.argspec
    params = request.params
    params.update(izip(names, args))
    params.update(kwargs)

    middlewared_func = middleware.decorator_wrap(normalize_result_decorator(route.func))
    result = middlewared_func(*args, **kwargs)

    headers = response.headers
    headers.setdefault('Expires', '0')
    max_age = headers.pop('Max-Age', '2')
    cache_control = headers.get('Cache-Control')
    if not cache_control: headers['Cache-Control'] = 'max-age=%s' % max_age
    headers.setdefault('Vary', 'Cookie')

    return result

def log_request(request):
    environ = request.environ
    headers=dict((key, value) for key, value in environ.items()
                              if isinstance(key, basestring)
                              and isinstance(value, basestring))
    method = environ.get('REQUEST_METHOD', 'GET')
    request_type = 'HTTP:%s' % method
    user = auth.local.user
    if user is not None and not isinstance(user, (int, long, basestring)):
        user = unicode(user)
    log(type=request_type, prefix=method+' ', text=request.full_url, severity=INFO,
        headers=headers, user=user, session=auth.local.session.__dict__)

BLOCK_SIZE = 65536

STD_ERROR_HEADERS = {'Content-Type': 'text/html; charset=UTF-8'}
INTERNAL_SERVER_ERROR = '500 Internal Server Error', STD_ERROR_HEADERS

def app(environ):
    request = local.request = HttpRequest(environ)
    log_request(request)
    response = local.response = HttpResponse()
    local.no_cookies = False
    auth.load(environ, request.cookies)
    auth.verify_ticket(request.fields.getfirst('_t'))
    postprocessing = True
    no_exception = False
    if autoreload.reloading_exception and not request.url.startswith('/pony/static/'):
        status, headers = INTERNAL_SERVER_ERROR
        result = format_exc(autoreload.reloading_exception)
    elif request.method not in ('HEAD', 'GET', 'POST', 'PUT', 'DELETE'):
        status = '501 Not Implemented'
        headers = {'Content-Type' : 'text/plain'}
        result = 'Unknown HTTP method: %s' % request.method
    else:
        try:
            try:
                if auth.local.ticket_payload is not None:
                    form = cPickle.loads(auth.local.ticket_payload)
                    form._handle_request_()
                    form = None
                result = invoke(request.url)
            finally:
                if auth.local.ticket and not request.form_processed and request.form_processed is not None:
                    auth.unexpire_ticket()
        except HttpException as e:
            status, headers, result = e.status, e.headers, e.content
            result, headers = normalize_result(result, headers)
        except BdbQuit: raise
        except:
            log_exc()
            status, headers = INTERNAL_SERVER_ERROR
            result, headers = normalize_result(format_exc(), headers)
        else:
            no_exception = True
            status = response.status
            headers = response.headers
            postprocessing = response.postprocessing

    content_type = headers.get('Content-Type', 'text/plain')
    media_type, type_params = cgi.parse_header(content_type)
    charset = type_params.get('charset', 'iso-8859-1')
    if isinstance(result, basestring):
        if media_type == 'text/html' and postprocessing:
            if no_exception:
                  result = postprocess(result, response.base_stylesheets, response.component_stylesheets, response.scripts)
            else: result = postprocess(result, [], [], [])
        if isinstance(result, unicode):
            if media_type == 'text/html' or 'xml' in media_type :
                  result = result.encode(charset, 'xmlcharrefreplace')
            else: result = result.encode(charset, 'replace')
        headers['Content-Length'] = str(len(result))

    headers = headers.items()
    for header, value in headers:
        assert isinstance(header, str)
        assert isinstance(value, str)
    if not local.no_cookies and not status.startswith('5'):
        auth.save(response.cookies)
        headers += httputils.serialize_cookies(environ, response.cookies)
    log(type='HTTP:response', prefix='Response: ', text=status, severity=DEBUG, headers=headers)
    if request.method == 'HEAD' and 'Content-Length' in headers: result = ''
    return status, headers, result

def inner_application(environ, start_response):
    middlewared_app = middleware.pony_wrap(app)
    status, headers, result = middlewared_app(environ)
    start_response(status, headers)
    # result must be str or file-like object:
    if not hasattr(result, 'read'): return [ result ]
    elif 'wsgi.file_wrapper' in environ: return environ['wsgi.file_wrapper'](result, BLOCK_SIZE)
    else: return iter(lambda: result.read(BLOCK_SIZE), '')  # return [ result.read() ]

def application(environ, start_response):
    middlewared_application = middleware.wsgi_wrap(inner_application)
    return middlewared_application(environ, start_response)

def main():
    from pony.thirdparty.wsgiref.handlers import CGIHandler
    CGIHandler().run(application)

server_threads = {}

class ServerException(Exception): pass
class   ServerStartException(ServerException): pass
class     ServerAlreadyStarted(ServerStartException): pass
class   ServerStopException(ServerException): pass
class     ServerNotStarted(ServerStopException): pass

class ServerThread(threading.Thread):
    def __init__(thread, host, port, application):
        server_thread = server_threads.setdefault((host, port), thread)
        if server_thread != thread: raise ServerAlreadyStarted('HTTP server already started: %s:%s' % (host, port))
        threading.Thread.__init__(thread)
        thread.host = host
        thread.port = port
        from pony.thirdparty.cherrypy.wsgiserver import CherryPyWSGIServer
        thread.server = CherryPyWSGIServer((host, port), application, server_name=host)
        thread.setDaemon(True)
    def run(thread):
        message = 'Starting HTTP server at %s:%s' % (thread.host, thread.port)
        log(type='HTTP:start', text=message, severity=WARNING, host=thread.host, port=thread.port, uid=pony.uid)
        thread.server.start()
        message = 'HTTP server at %s:%s stopped successfully' % (thread.host, thread.port)
        log(type='HTTP:stop', text=message, severity=WARNING, host=thread.host, port=thread.port)
        server_threads.pop((thread.host, thread.port), None)

def start_http_server(address='localhost:8080'):
    if pony.MODE.startswith('GAE-'): main(); return
    elif pony.MODE not in ('INTERACTIVE', 'CHERRYPY'): return
    pony._do_mainloop = True
    host, port = httputils.parse_address(address)
    try: server_thread = ServerThread(host, port, application)
    except ServerAlreadyStarted:
        if not autoreload.reloading: raise
    else: server_thread.start()

    if host != 'localhost': return
    url = 'http://localhost:%d/pony/shutdown?uid=%s' % (port, pony.uid)
    import urllib
    for i in range(6):
        time.sleep(.2)
        try: response_string = urllib.urlopen(url).read()
        except: continue
        if not response_string.startswith('+'): break

def stop_http_server(address=None):
    if pony.MODE not in ('INTERACTIVE', 'CHERRYPY'): return
    if address is None:
        for server_thread in server_threads.values():
            server_thread.server.stop()
            server_thread.join()
    else:
        host, port = httputils.parse_address(address)
        server_thread = server_threads.get((host, port))
        if server_thread is None: raise ServerNotStarted(
            'Cannot stop HTTP server at %s:%s because it is not started:' % (host, port))
        server_thread.server.stop()
        server_thread.join()

class Http(object):
    NO_REDIRECT = True

    invoke = staticmethod(invoke)
    remove = staticmethod(routing.remove)
    clear = staticmethod(routing.clear)
    start = staticmethod(start_http_server)
    stop = staticmethod(stop_http_server)

    @staticmethod
    @decorator_with_params
    def __call__(func, url=None, host=None, port=None, redirect=False, **headers):
        routing.Route(func, url, None, host, port, redirect, headers)
        return func

    @staticmethod
    @decorator_with_params
    def HEAD(func, url=None, host=None, port=None, redirect=False, **headers):
        routing.Route(func, url, 'HEAD', host, port, redirect, headers)
        return func

    @staticmethod
    @decorator_with_params
    def GET(func, url=None, host=None, port=None, redirect=False, **headers):
        routing.Route(func, url, 'GET', host, port, redirect, headers)
        return func

    @staticmethod
    @decorator_with_params
    def POST(func, url=None, host=None, port=None, redirect=False, **headers):
        routing.Route(func, url, 'POST', host, port, redirect, headers)
        return func

    @staticmethod
    @decorator_with_params
    def PUT(func, url=None, host=None, port=None, redirect=False, **headers):
        routing.Route(func, url, 'PUT', host, port, redirect, headers)
        return func

    @staticmethod
    @decorator_with_params
    def DELETE(func, url=None, host=None, port=None, redirect=False, **headers):
        routing.Route(func, url, 'DELETE', host, port, redirect, headers)
        return func

    @property
    def request(self): return local.request

    @property
    def response(self): return local.response

    @property
    def session(self): return auth.local.session

    def get_user(self):
        return auth.get_user()
    def set_user(self, user, longlife_session=False, remember_ip=False):
        auth.set_user(user, longlife_session, remember_ip)
    user = property(get_user, set_user)

    def get_lang(self):
        return auth.local.session.lang
    def set_lang(self, lang):
        if lang:
            if not isinstance(lang, basestring):
                raise TypeError('http.lang must be string. Got: %s' % lang)
            auth.local.session.lang = lang[:]  # = utils.plainstr(lang)
        else: auth.local.session.pop('lang', None)
        local.request.languages = local.request._get_languages()
    lang = property(get_lang, set_lang)

    def __getitem__(self, key):
        if isinstance(key, basestring):
            result = local.request.fields.getfirst(key)
            if result is None: return None
            try: return result.decode('utf8')
            except UnicodeDecodeError: raise Http400BadRequest
        elif hasattr(key, '__iter__'): # sequence of field names
            return tuple(self[fieldname] for fieldname in key)
        else: raise KeyError(key)

    class _Params(object):
        def __getattr__(self, attr): return local.request.params.get(attr)
        def __setattr__(self, attr, value): local.request.params[attr] = value
    _params = _Params()
    params = property(attrgetter('_params'))

    class _Cookies(object):
        __getattr__ = staticmethod(get_cookie)
        __setattr__ = set = staticmethod(set_cookie)
    _cookies = _Cookies()
    cookies = property(attrgetter('_cookies'))

http = Http()
http.Exception = HttpException
http.BadRequest = Http400BadRequest
http.NotFound = Http404NotFound
http.Redirect = HttpRedirect

@http('/pony/shutdown?uid=$uid')
def http_shutdown(uid=None):
    if uid == pony.uid: return pony.uid

    environ = local.request.environ
    if environ.get('REMOTE_ADDR') != '127.0.0.1': return pony.uid

    if pony.MODE == 'INTERACTIVE':
        stop_http_server()
        return '+' + pony.uid

    if pony.MODE != 'CHERRYPY': return pony.uid
    if not (environ.get('HTTP_HOST', '') + ':').startswith('localhost:'): return pony.uid
    if environ.get('SERVER_NAME') != 'localhost': return pony.uid
    pony.shutdown = True
    return '+' + pony.uid

@pony.on_shutdown
def do_shutdown():
    try: stop_http_server()
    except ServerNotStarted: pass
