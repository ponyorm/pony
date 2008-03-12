import re, threading, os.path, inspect, sys
import cgi, cgitb, urllib, Cookie, mimetypes, cPickle, time

from cStringIO import StringIO
from itertools import imap, izip, count
from operator import itemgetter, attrgetter

from pony.thirdparty.cherrypy.wsgiserver import CherryPyWSGIServer

import pony
from pony import autoreload, auth, utils, xslt
from pony.autoreload import on_reload
from pony.utils import decorator_with_params
from pony.templating import Html, real_stdout
from pony.logging import log, log_exc
from pony.xslt import xslt_function

class _Http(object):
    def __call__(self, *args, **keyargs):
        return _http(*args, **keyargs)
    def invoke(self, url):
        return invoke(url)
    def remove(self, x, host=None, port=None):
        return http_remove(x, host, port)
    def clear(self):
        return http_clear()

    def start(self, address='localhost:8080', verbose=True):
        start_http_server(address, verbose)
    def stop(self, address=None):
        stop_http_server(address)

    def get_request(self):
        return local.request
    request = property(get_request)

    def get_response(self):
        return local.response
    response = property(get_response)

    def get_session(self):
        return auth.local.session
    session = property(get_session)

    def get_conversation(self):
        return auth.local.conversation
    conversation = property(get_conversation)

    def get_user(self):
        return auth.get_user()
    def set_user(self, user, remember_ip=False, path='/', domain=None):
        auth.set_user(user, remember_ip, path, domain)
    user = property(get_user, set_user)

    def get_param(self, name):
        return local.request.params.get(name)
    class _Params(object):
        def __getattr__(self, attr):
            return local.request.params.get(attr)
        def __setattr__(self, attr, value):
            local.request.params[attr] = value
    _params = _Params()
    params = property(attrgetter('_params'))

    class _Cookies(object):
        def set(self, name, value, expires=None, max_age=None,
                path=None, domain=None, secure=False, http_only=False, comment=None, version=None):
            set_cookie(name, value, expires, max_age,
                       path, domain, secure, http_only, comment, version)
        def __getattr__(self, attr):
            return get_cookie(attr)
        __setattr__ = set
    _cookies = _Cookies()
    set_cookie = _cookies.set
    cookies = property(attrgetter('_cookies'))

http = _Http()

get_request = http.get_request
get_response = http.get_response
get_param = http.get_param

@decorator_with_params
def _http(url=None, host=None, port=None, redirect=False, **http_headers):
    http_headers = dict([ (name.replace('_', '-').title(), value)
                          for name, value in http_headers.items() ])
    def new_decorator(old_func):
        real_url = url is None and old_func.__name__ or url
        HttpInfo(old_func, real_url, host, port, redirect, http_headers)
        return old_func
    return new_decorator

http_registry_lock = threading.RLock()
http_registry = ({}, [], [])
http_system_handlers = []

def split_url(url, strict_parsing=False):
    if isinstance(url, unicode): url = url.encode('utf8')
    elif isinstance(url, str):
        if strict_parsing:
            try: url.decode('ascii')
            except UnicodeDecodeError: raise ValueError(
                'Url string contains non-ascii symbols. Such urls must be in unicode.')
    else: raise ValueError('Url parameter must be str or unicode')
    if '?' in url:
        p, q = url.split('?', 1)
        qlist = []
        qnames = set()
        for name, value in cgi.parse_qsl(q, strict_parsing=strict_parsing, keep_blank_values=True):
            if name not in qnames:
                qlist.append((name, value))
                qnames.add(name)
            elif strict_parsing: raise ValueError('Duplicate url parameter: %s' % name)
    else: p, qlist = url, []
    components = p.split('/')
    if not components[0]: components = components[1:]
    path = map(urllib.unquote, components)
    return path, qlist

class NodefaultType(object):
    def __repr__(self): return '__nodefault__'
    
__nodefault__ = NodefaultType()

class HttpInfo(object):
    def __init__(self, func, url, host, port, redirect, http_headers):
        url_cache.clear()
        self.func = func
        if not hasattr(func, 'argspec'):
            func.argspec = self.getargspec(func)
            func.dummy_func = self.create_dummy_func(func)
        self.url = url
        if host is not None:
            if not isinstance(host, basestring): raise TypeError('Host must be string')
            if ':' in host:
                if port is not None: raise TypeError('Duplicate port specification')
                host, port = host.split(':')
        self.host, self.port = host, port and int(port) or None
        self.path, self.qlist = split_url(url, strict_parsing=True)
        self.redirect = redirect
        module = func.__module__
        self.system = module.startswith('pony.') and not module.startswith('pony.examples.')
        self.http_headers = http_headers
        self.args = set()
        self.keyargs = set()
        self.parsed_path = []
        self.star = False
        for component in self.path:
            if self.star:
                raise TypeError("'*' must be last element in url path")
            elif component != '*':
                self.parsed_path.append(self.parse_component(component))
            else: self.star = True
        self.parsed_query = []
        for name, value in self.qlist:
            if value == '*':
                raise TypeError("'*' does not allowed in query part of url")
            is_param, x = self.parse_component(value)
            self.parsed_query.append((name, is_param, x))
        self.check()
        if self.system: http_system_handlers.append(self)
        self.register()
    @staticmethod
    def getargspec(func):
        original_func = getattr(func, 'original_func', func)
        names, argsname, keyargsname, defaults = inspect.getargspec(original_func)
        defaults = defaults and list(defaults) or []
        diff = len(names) - len(defaults)
        converters = {}
        try:
            for i, value in enumerate(defaults):
                if value is None: continue
                elif isinstance(value, basestring):
                    defaults[i] = unicode(value).encode('utf8')
                elif callable(value):
                    converters[diff+i] = value
                    defaults[i] = __nodefault__
                else: converters[diff+i] = value.__class__
        except UnicodeDecodeError: raise ValueError(
            'Default value contains non-ascii symbols. Such default values must be in unicode.')
        return names, argsname, keyargsname, defaults, converters
    @staticmethod
    def create_dummy_func(func):
        spec = inspect.formatargspec(*func.argspec[:-1])[1:-1]
        source = "lambda %s: __locals__()" % spec
        return eval(source, dict(__locals__=locals, __nodefault__=__nodefault__))
    component_re = re.compile(r"""
            [$]
            (?: (\d+)              # param number (group 1)
            |   ([A-Za-z_]\w*)     # param identifier (group 2)
            )
        |   (                      # path component (group 3)
                (?:[$][$] | [^$])+
            )
        """, re.VERBOSE)
    def parse_component(self, component):
        items = list(self.split_component(component))
        if not items: return False, ''
        if len(items) == 1: return items[0]
        pattern = []
        regexp = []
        for i, item in enumerate(items):
            if item[0]:
                pattern.append('/')
                try: nextchar = items[i+1][1][0]
                except IndexError: regexp.append('(.*)$')
                else: regexp.append('([^%s]*)' % nextchar.replace('\\', '\\\\'))
            else:
                s = item[1]
                pattern.append(s)
                for char in s:
                    regexp.append('[%s]' % char.replace('\\', '\\\\'))
        pattern = ''.join(pattern)
        regexp = ''.join(regexp)
        return True, [ pattern, re.compile(regexp) ] + items
    def split_component(self, component):
        pos = 0
        is_param = False
        for match in self.component_re.finditer(component):
            if match.start() != pos:
                raise ValueError('Invalid url component: %r' % component)
            i = match.lastindex
            if 1 <= i <= 2:
                if is_param: raise ValueError('Invalid url component: %r' % component)
                is_param = True
                if i == 1: yield is_param, self.adjust(int(match.group(i)) - 1)
                elif i == 2: yield is_param, self.adjust(match.group(i))
            elif i == 3:
                is_param = False
                yield is_param, match.group(i).replace('$$', '$')
            else: assert False
            pos = match.end()
        if pos != len(component):
            raise ValueError('Invalid url component: %r' % component)
    def adjust(self, x):
        names, argsname, keyargsname, defaults, converters = self.func.argspec
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
                    raise TypeError('Unknown parameter name: %s' % x)
                keyargs.add(x)
                return x
            else:
                if i in args: raise TypeError('Parameter name %s already in use' % x)
                args.add(i)
                return i
        assert False
    def check(self):
        names, argsname, keyargsname, defaults, converters = self.func.argspec
        if self.star and not argsname: raise TypeError(
            "Function %s does not accept arbitrary argument list" % self.func.__name__)
        args, keyargs = self.args, self.keyargs
        diff = len(names) - len(defaults)
        for i, name in enumerate(names[:diff]):
            if i not in args: raise TypeError('Undefined path parameter: %s' % name)
        for i, name, default in izip(xrange(diff, diff+len(defaults)), names[diff:], defaults):
            if default is __nodefault__ and i not in args:
                raise TypeError('Undefined path parameter: %s' % name)
        if args:
            for i in range(len(names), max(args)):
                if i not in args:
                    raise TypeError('Undefined path parameter: %d' % (i+1))
    def register(self):
        def get_url_map(info):
            result = {}
            for i, (is_param, x) in enumerate(info.parsed_path):
                if is_param: result[i] = isinstance(x, list) and x[0] or '/'
                else: result[i] = ''
            for name, is_param, x in info.parsed_query:
                if is_param: result[name] = isinstance(x, list) and x[0] or '/'
                else: result[name] = ''
            if info.star: result['*'] = len(info.parsed_path)
            if info.host: result[('host',)] = info.host
            if info.port: result[('port',)] = info.port
            return result
        url_map = get_url_map(self)
        qdict = dict(self.qlist)
        http_registry_lock.acquire()
        try:
            for info, _, _ in get_http_handlers(self.path, qdict, self.host, self.port):
                if url_map == get_url_map(info):
                    log(type='Warning:URL',
                        text='Route already in use (old handler was removed): %s' % info.url)
                    _http_remove(info)
            d, list1, list2 = http_registry
            for is_param, x in self.parsed_path:
                if is_param: d, list1, list2 = d.setdefault(None, ({}, [], []))
                else: d, list1, list2 = d.setdefault(x, ({}, [], []))
            if not self.star: self.list = list1
            else: self.list = list2
            self.func.__dict__.setdefault('http', []).insert(0, self)
            self.list.insert(0, self)
        finally: http_registry_lock.release()
            
class PathError(Exception): pass

url_cache = {}

def url(func, *args, **keyargs):
    http_list = getattr(func, 'http')
    if http_list is None:
        raise ValueError('Cannot create url for this object :%s' % func)
    try: keyparams = func.dummy_func(*args, **keyargs).copy()
    except TypeError, e:
        raise TypeError(e.args[0].replace('<lambda>', func.__name__, 1))
    names, argsname, keyargsname, defaults, converters = func.argspec
    indexparams = map(keyparams.pop, names)
    indexparams.extend(keyparams.pop(argsname, ()))
    keyparams.update(keyparams.pop(keyargsname, {}))
    try:
        for i, value in enumerate(indexparams):
            if value is not None and value is not __nodefault__:
                indexparams[i] = unicode(value).encode('utf8')
        for key, value in keyparams.items():
            if value is not None: keyparams[key] = unicode(value).encode('utf8')
    except UnicodeDecodeError: raise ValueError(
        'Url parameter value contains non-ascii symbols. Such values must be in unicode.')
    request = local.request
    host, port = request.host, request.port
    key = func, tuple(indexparams), tuple(sorted(keyparams.items())), host, port
    try: return url_cache[key]
    except KeyError: pass
    first, second = [], []
    for info in http_list:
        if not info.redirect: first.append(info)
        else: second.append(info)
    for info in first + second:
        try: url = build_url(info, keyparams, indexparams, host, port)
        except PathError: pass
        else: break
    else: raise PathError('Suitable url path for %s() not found' % func.__name__)
    if len(url_cache) > 4000: url_cache.clear()
    url_cache[key] = url
    return url
make_url = url

def build_url(info, keyparams, indexparams, host, port):
    names, argsname, keyargsname, defaults, converters = info.func.argspec
    path = []
    used_indexparams = set()
    used_keyparams = set()
    diff = len(names) - len(defaults)
    def build_param(x):
        if isinstance(x, int):
            value = indexparams[x]
            used_indexparams.add(x)
            is_default = False
            if diff <= x < len(names):
                if value is __nodefault__: raise PathError('Value for paremeter %r does not set' % names[x])
                default = defaults[x-diff]
                if value is None and default is None or value == unicode(default).encode('utf8'):
                    is_default = True
            return is_default, value
        elif isinstance(x, basestring):
            try: value = keyparams[x]
            except KeyError: assert False, 'Parameter not found: %r' % x
            used_keyparams.add(x)
            return False, value
        elif isinstance(x, list):
            result = []
            is_default = True
            for is_param, y in x[2:]:
                if not is_param: result.append(y)
                else:
                    is_default_2, component = build_param(y)
                    is_default = is_default and is_default_2
                    if component is None: raise PathError('Value for parameter %r is None' % y)
                    result.append(component)
            return is_default, ''.join(result)
        else: assert False

    for is_param, x in info.parsed_path:
        if not is_param: component = x
        else:
            is_default, component = build_param(x)
            if component is None: raise PathError('Value for parameter %r is None' % x)
        path.append(urllib.quote(component, safe=':@&=+$,'))
    if info.star:
        for i in range(len(info.args), len(indexparams)):
            path.append(urllib.quote(indexparams[i], safe=':@&=+$,'))
            used_indexparams.add(i)
    p = '/'.join(path)

    qlist = []
    for name, is_param, x in info.parsed_query:
        if not is_param: qlist.append((name, x))
        else:
            is_default, value = build_param(x)
            if not is_default:
                if value is None: raise PathError('Value for parameter %r is None' % x)
                qlist.append((name, value))
    quote_plus = urllib.quote_plus
    q = "&".join(("%s=%s" % (quote_plus(name), quote_plus(value))) for name, value in qlist)

    errmsg = 'Not all parameters were used during path construction'
    if len(used_keyparams) != len(keyparams): raise PathError(errmsg)
    if len(used_indexparams) != len(indexparams):
        for i, value in enumerate(indexparams):
            if i not in used_indexparams and value != defaults[i-diff]: raise PathError(errmsg)

    script_name = local.request.environ.get('SCRIPT_NAME', '')
    url = q and '?'.join((p, q)) or p
    result = '/'.join((script_name, url))
    if info.host is None or info.host == host:
        if info.port is None or info.port == port: return result
    host = info.host or host
    port = info.port or 80
    if port == 80: return 'http://%s%s' % (host, result)
    return 'http://%s:%d%s' % (host, port, result)

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

img_template = Html(u'<img src="%s" title="%s" alt="%s">')

def img(*args, **keyargs):
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
    return img_template % (href, description, description)

if not mimetypes.inited: # Copied from SimpleHTTPServer
    mimetypes.init() # try to read system mime.types
extensions_map = mimetypes.types_map.copy()
extensions_map.update({
    '': 'application/octet-stream', # Default
    '.py': 'text/plain',
    '.c': 'text/plain',
    '.h': 'text/plain',
    })

def guess_type(ext):
    result = extensions_map.get(ext)
    if result is not None: return result
    result = extensions_map.get(ext.lower())
    if result is not None: return result
    return 'application/octet-stream'

def get_static_dir_name():
    if pony.MAIN_DIR is None: return None
    return os.path.join(pony.MAIN_DIR, 'static')

static_dir = get_static_dir_name()

path_re = re.compile(r"^[-_.!~*'()A-Za-z0-9]+$")

def get_static_file(path):
    if not path: raise Http404
    if static_dir is None: raise Http404
    for component in path:
        if not path_re.match(component): raise Http404
    fname = os.path.join(static_dir, *path)
    if not os.path.isfile(fname):
        if path == [ 'favicon.ico' ]: return get_pony_static_file(path)
        raise Http404
    ext = os.path.splitext(path[-1])[1]
    headers = local.response.headers
    headers['Content-Type'] = guess_type(ext)
    headers['Expires'] = '0'
    headers['Cache-Control'] = 'max-age=10'
    return file(fname, 'rb')

pony_static_dir = os.path.join(os.path.dirname(__file__), 'static')

def get_pony_static_file(path):
    if not path: raise Http404
    for component in path:
        if not path_re.match(component): raise Http404
    fname = os.path.join(pony_static_dir, *path)
    if not os.path.isfile(fname): raise Http404
    ext = os.path.splitext(path[-1])[1]
    headers = local.response.headers
    headers['Content-Type'] = guess_type(ext)
    max_age = 30 * 60
    headers['Expires'] = Cookie._getdate(max_age)
    headers['Cache-Control'] = 'max-age=%d' % max_age
    return file(fname, 'rb')

def get_http_handlers(path, qdict, host, port):
    # http_registry_lock.acquire()
    # try:
    variants = [ http_registry ]
    infos = []
    for i, component in enumerate(path):
        new_variants = []
        for d, list1, list2 in variants:
            variant = d.get(component)
            if variant: new_variants.append(variant)
            # if component:
            variant = d.get(None)
            if variant: new_variants.append(variant)
            infos.extend(list2)
        variants = new_variants
    for d, list1, list2 in variants: infos.extend(list1)
    # finally: http_registry_lock.release()

    result = []
    not_found = object()
    for info in infos:
        args, keyargs = {}, {}
        priority = 0
        if info.host is not None:
            if info.host != host: continue
            priority += 10000
        if info.port is not None:
            if info.port != port: continue
            priority += 100
        for i, (is_param, x) in enumerate(info.parsed_path):
            if not is_param:
                priority += 1
                continue
            value = path[i].decode('utf8')
            if isinstance(x, int): args[x] = value
            elif isinstance(x, basestring): keyargs[x] = value
            elif isinstance(x, list):
                match = x[1].match(value)
                if not match: break
                params = [ y for is_param, y in x[2:] if is_param ]
                groups = match.groups()
                n = len(x) - len(params)
                if not x[-1][0]: n += 1
                priority += n
                assert len(params) == len(groups)
                for param, value in zip(params, groups):
                    if isinstance(param, int): args[param] = value
                    elif isinstance(param, basestring): keyargs[param] = value
                    else: assert False
            else: assert False
        else:
            names, _, _, defaults, converters = info.func.argspec
            diff = len(names) - len(defaults)
            non_used_query_params = set(qdict)
            for name, is_param, x in info.parsed_query:
                non_used_query_params.discard(name)
                value = qdict.get(name, not_found)
                if value is not not_found: value = value.decode('utf8')
                if not is_param:
                    if value != x: break
                    priority += 1
                elif isinstance(x, int):
                    if value is not_found:
                        if diff <= x < len(names): continue
                        else: break
                    else: args[x] = value
                elif isinstance(x, basestring):
                    if value is not_found: break
                    keyargs[x] = value
                elif isinstance(x, list):
                    if value is not_found:
                        for is_param, y in x[2:]:
                            if not is_param: continue
                            if isinstance(y, int) and diff <= y < len(names): continue
                            break
                        else: continue
                        break
                    match = x[1].match(value)
                    if not match: break
                    params = [ y for is_param, y in x[2:] if is_param ]
                    groups = match.groups()
                    n = len(x) - len(params) - 2
                    if not x[-1][0]: n += 1
                    priority += n
                    assert len(params) == len(groups)
                    for param, value in zip(params, groups):
                        if isinstance(param, int): args[param] = value
                        elif isinstance(param, basestring):
                            keyargs[param] = value
                        else: assert False
                else: assert False
            else:
                arglist = [ None ] * len(names)
                arglist[diff:] = defaults
                for i, value in sorted(args.items()):
                    converter = converters.get(i)
                    if converter is not None:
                        try: value = converter(value)
                        except: break
                    try: arglist[i] = value
                    except IndexError:
                        assert i == len(arglist)
                        arglist.append(value)
                else:
                    if __nodefault__ in arglist[diff:]: continue
                    if len(info.parsed_path) != len(path):
                        assert info.star
                        arglist.extend(path[len(info.parsed_path):])
                    result.append((info, arglist, keyargs, priority, len(non_used_query_params)))
    if result:
        x = max(map(itemgetter(3), result))
        result = [ tup for tup in result if tup[3] == x ]
        x = min(map(itemgetter(4), result))
        result = [ tup[:3] for tup in result if tup[4] == x ]
    return result

def invoke(url):
    if isinstance(url, str):
        try: url.decode('utf8')
        except UnicodeDecodeError: raise Http400BadRequest
    request = local.request
    response = local.response = HttpResponse()
    path, qlist = split_url(url)
    if path[:1] == ['static'] and len(path) > 1:
        return get_static_file(path[1:])
    if path[:2] == ['pony', 'static'] and len(path) > 2:
        return get_pony_static_file(path[2:])
    qdict = dict(qlist)
    handlers = get_http_handlers(path, qdict, request.host, request.port)
    if not handlers:
        i = url.find('?')
        if i == -1: p, q = url, ''
        else: p, q = url[:i], url[i:]
        if p.endswith('/'): url2 = p[:-1] + q
        else: url2 = p + '/' + q
        path2, qlist = split_url(url2)
        handlers = get_http_handlers(path2, qdict, request.host, request.port)
        if not handlers: return get_static_file(path)
        script_name = request.environ.get('SCRIPT_NAME', '')
        url2 = script_name + url2 or '/'
        if url2 != script_name + url: raise HttpRedirect(url2)
    info, args, keyargs = handlers[0]

    if info.redirect:
        for alternative in info.func.http:
            if not alternative.redirect:
                new_url = make_url(info.func, *args, **keyargs)
                status = '301 Moved Permanently'
                if isinstance(info.redirect, basestring): status = info.redirect
                elif isinstance(info.redirect, (int, long)) and 300 <= info.redirect < 400:
                    status = str(info.redirect)
                raise HttpRedirect(new_url, status)
    response.headers.update(info.http_headers)

    names, argsname, keyargsname, defaults, converters = info.func.argspec
    params = request.params
    params.update(zip(names, args))
    params.update(keyargs)

    result = info.func(*args, **keyargs)

    headers = dict([ (name.replace('_', '-').title(), value)
                     for name, value in response.headers.items() ])
    response.headers = headers

    media_type = headers.pop('Type', 'text/plain')
    charset = headers.pop('Charset', 'UTF-8')
    content_type = headers.get('Content-Type')
    if content_type:
        media_type, type_params = cgi.parse_header(content_type)
        charset = type_params.get('charset', 'iso-8859-1')
    else:
        if isinstance(result, Html): media_type = 'text/html'
        content_type = '%s; charset=%s' % (media_type, charset)
        headers['Content-Type'] = content_type

    response.conversation_data = auth.save_conversation()
    if media_type == 'text/html' and xslt.is_supported:
        result = xslt.transform(result, charset)
    else:
        if hasattr(result, '__unicode__'): result = unicode(result)
        if isinstance(result, unicode):
            if media_type == 'text/html' or 'xml' in media_type :
                  result = result.encode(charset, 'xmlcharrefreplace')
            else: result = result.encode(charset, 'replace')
        elif not isinstance(result, str):
            try: result = etree.tostring(result, charset)
            except: result = str(result)

    headers.setdefault('Expires', '0')
    max_age = headers.pop('Max-Age', '2')
    cache_control = headers.get('Cache-Control')
    if not cache_control: headers['Cache-Control'] = 'max-age=%s' % max_age
    headers.setdefault('Vary', 'Cookie')
    return result

def _http_remove(info):
    url_cache.clear()
    info.list.remove(info)
    info.func.http.remove(info)
            
def http_remove(x, host=None, port=None):
    if isinstance(x, basestring):
        path, qlist = split_url(x, strict_parsing=True)
        qdict = dict(qlist)
        http_registry_lock.acquire()
        try:
            for info, _, _ in get_http_handlers(path, qdict, host, port): _http_remove(info)
        finally: http_registry_lock.release()
    elif hasattr(x, 'http'):
        assert host is None and port is None
        http_registry_lock.acquire()
        try:
            for info in list(x.http): _http_remove(info)
        finally: http_registry_lock.release()
    else: raise ValueError('This object is not bound to url: %r' % x)

def _http_clear(dict, list1, list2):
    url_cache.clear()
    for info in list1: info.func.http.remove(info)
    list1[:] = []
    for info in list2: info.func.http.remove(info)
    list2[:] = []
    for inner_dict, list1, list2 in dict.itervalues():
        _http_clear(inner_dict, list1, list2)
    dict.clear()

@on_reload
def http_clear():
    http_registry_lock.acquire()
    try:
        _http_clear(*http_registry)
        for handler in http_system_handlers: handler.register()
    finally: http_registry_lock.release()

################################################################################

class HttpException(Exception):
    content = ''
http.Exception = HttpException

class Http400BadRequest(HttpException):
    status = '400 Bad Request'
    headers = {'Content-Type': 'text/plain'}
    def __init__(self, content='Bad Request'):
        Exception.__init__(self, 'Bad Request')
        self.content = content
http.BadRequest = Http400BadRequest
        
class Http404NotFound(HttpException):
    status = '404 Not Found'
    headers = {'Content-Type': 'text/plain'}
    def __init__(self, content='Page not found'):
        Exception.__init__(self, 'Page not found')
        self.content = content
Http404 = Http404NotFound
http.NotFound = Http404NotFound

class HttpRedirect(HttpException):
    status_dict = {'301' : '301 Moved Permanently',
                   '302' : '302 Found',
                   '303' : '303 See Other',
                   '305' : '305 Use Proxy',
                   '307' : '307 Temporary Redirect'}
    def __init__(self, location, status='302 Found'):
        Exception.__init__(self, location)
        self.location = location
        status = str(status)
        self.status = self.status_dict.get(status, status)
        self.headers = {'Location': location}
http.Redirect = HttpRedirect

################################################################################

def reconstruct_script_url(environ):
    url_scheme  = environ['wsgi.url_scheme']
    host        = environ.get('HTTP_HOST')
    server_name = environ['SERVER_NAME']
    server_port = environ['SERVER_PORT']
    script_name = environ.get('SCRIPT_NAME','')
    path_info   = environ.get('PATH_INFO','')
    query       = environ.get('QUERY_STRING')
    
    url = url_scheme + '://'
    if host: url += host
    else:
        url += server_name
        if (url_scheme == 'https' and server_port == '443') \
        or (url_scheme == 'http' and server_port == '80'): pass
        else: url += ':' + server_port

    url += urllib.quote(script_name)
    return url

class HttpRequest(object):
    def __init__(self, environ):
        self.environ = environ
        self.cookies = Cookie.SimpleCookie()
        self.method = environ.get('REQUEST_METHOD', 'GET')
        if environ:
            http_host = environ.get('HTTP_HOST')
            if http_host:
                if ':' in http_host: host, port = http_host.split(':')
                else: host, port = http_host, 80
            else:
                host = environ['SERVER_NAME']
                post = environ['SERVER_PORT']
            self.host, self.port = host, int(port)

            self.url = urllib.quote(environ['PATH_INFO'])
            query = environ['QUERY_STRING']
            if query: self.url += '?' + query
            self.script_url = reconstruct_script_url(environ)
            self.full_url = self.script_url + self.url

            if 'HTTP_COOKIE' in environ: self.cookies.load(environ['HTTP_COOKIE'])
            morsel = self.cookies.get('pony')
            session_data = morsel and morsel.value or None
            auth.load(session_data, environ)
        else:
            self.script_url = ''
            self.url = '/'
            self.full_url = 'http://localhost/'
            self.host = 'localhost'
            self.port = 80
        self._base_url = None
        self.conversation = {}
        input_stream = environ.get('wsgi.input') or StringIO()
        self.params = {}
        self.fields = cgi.FieldStorage(fp=input_stream, environ=environ, keep_blank_values=True)
        self.form_processed = None
        self.submitted_form = self.fields.getfirst('_f')
        self.ticket, self.payload = auth.verify_ticket(self.fields.getfirst('_t'))
        self.conversation_data = self.fields.getfirst('_c')
        auth.load_conversation(self.conversation_data)
        self.id_counter = imap('id_%d'.__mod__, count())

class HttpResponse(object):
    def __init__(self):
        self.headers = {}
        self.cookies = Cookie.SimpleCookie()
        self._http_only_cookies = set()
        self.conversation_data = ''

class Local(threading.local):
    def __init__(self):
        self.request = HttpRequest({})
        self.response = HttpResponse()

local = Local()        

def get_cookie(name, default=None):
    morsel = local.request.cookies.get(name)
    if morsel is None: return default
    return morsel.value

def set_cookie(name, value, expires=None, max_age=None, path=None, domain=None,
               secure=False, http_only=False, comment=None, version=None):
    response = local.response
    cookies = response.cookies
    if value is None:
        cookies.pop(name, None)
        response._http_only_cookies.discard(name)
    else:
        cookies[name] = value
        morsel = cookies[name]
        if expires is not None: morsel['expires'] = expires
        if max_age is not None: morsel['max-age'] = max_age
        if path is not None: morsel['path'] = path
        if domain is not None: morsel['domain'] = domain
        if comment is not None: morsel['comment'] = comment
        if version is not None: morsel['version'] = version
        if secure: morsel['secure'] = True
        if http_only: response._http_only_cookies.add(name)
        else: response._http_only_cookies.discard(name)

def format_exc():
    exc_type, exc_value, traceback = sys.exc_info()
    if traceback.tb_next: traceback = traceback.tb_next
    if traceback.tb_next: traceback = traceback.tb_next
    try:
        io = StringIO()
        hook = cgitb.Hook(file=io)
        hook.handle((exc_type, exc_value, traceback))
        return io.getvalue()
    finally:
        del traceback

def log_request(request):
    environ = request.environ
    headers=dict((key, value) for key, value in environ.items()
                              if isinstance(key, basestring)
                              and isinstance(value, basestring))
    request_type = 'HTTP:%s' % environ.get('REQUEST_METHOD', 'GET')
    user = auth.local.user
    if user is not None and not isinstance(user, (int, long, basestring)):
        user = unicode(user)
    log(type=request_type, text=request.full_url, headers=headers,
        user=user, session=auth.local.session)

http_only_incompatible_browsers = [ 'WebTV', 'MSIE 5.0; Mac',
    'Firefox/2.0.0.0', 'Firefox/2.0.0.1', 'Firefox/2.0.0.2', 'Firefox/2.0.0.3', 'Firefox/2.0.0.4', ]

ONE_MONTH = 60*60*24*31

def create_cookies(environ):
    data, domain, path = auth.save(environ)
    if data is not None:
        set_cookie('pony', data, ONE_MONTH, ONE_MONTH, path or '/', domain,
                   http_only=True)
    user_agent = environ.get('HTTP_USER_AGENT', '')
    support_http_only = True
    for browser in http_only_incompatible_browsers:
        if browser in user_agent:
            support_http_only = False
            break
    response = local.response
    result = []
    for name, morsel in response.cookies.items():
        cookie = morsel.OutputString()
        if support_http_only and name in response._http_only_cookies:
            cookie += ' HttpOnly'
        result.append(('Set-Cookie', cookie))
    return result

BLOCK_SIZE = 65536

@xslt_function
def xslt_set_base_url(url):
    local.request._base_url = url

@xslt_function
def xslt_conversation():
    return local.response._new_conversation_data

@xslt_function
def xslt_url(url):
    request = local.request
    script_url = request.script_url
    base_url = request._base_url
    if base_url is not None and not base_url.startswith(script_url): return url
    if url.startswith(script_url): pass
    elif url.startswith('http://'): return external_url('http', url[7:])
    elif url.startswith('https://'): return external_url('https', url[8:])

    conversation_data = local.response.conversation_data
    if not conversation_data: return url
    if '?' in url: return '%s&_c=%s' % (url, conversation_data)
    return '%s?_c=%s' % (url, conversation_data)
        
def external_url(protocol, s):
    request = local.request
    try: i = s.index('/')
    except ValueError: pass
    else:
        # Phishing prevention
        try: j = s.index('@', 0, i)
        except ValueError: pass
        else: s = s[j+1:]
    return '%s://%s' % (protocol, s)
    # return '%s/pony/redirect/%s/%s' % (request.environ.get('SCRIPT_NAME',''), protocol, s)

##@http('/pony/redirect/*')
##def external_redirect(*args):
##    url = local.request.url 
##    assert url.startswith('/pony/redirect/')
##    url = url[len('/pony/redirect/'):]
##    protocol, url = url.split('/', 1)
##    if protocol not in ('http', 'https'): raise http.NotFound
##    url = '%s://%s' % (protocol, url)
##    local.response.headers['Refresh'] = '0; url=' + url
##    return '<html></html>'
##
##@http('/pony/blocked')
##def blocked_url():
##    raise http.NotFound

def application(environ, wsgi_start_response):
    def start_response(status, headers):
        headers = [ (name, str(value)) for name, value in headers.items() ]
        headers.extend(create_cookies(environ))
        log(type='HTTP:response', text=status, headers=headers)
        wsgi_start_response(status, headers)

    # This next line is required, because hase possible side-effect
    # (initialization of new dummy request in frest thread)
    local.request # It must be done before creation of non-dummy request

    request = local.request = HttpRequest(environ)
    log_request(request)
    try:
        try:
            if request.payload is not None:
                form = cPickle.loads(request.payload)
                form._handle_request_()
                form = None
            result = invoke(request.url)
        finally:
            if request.ticket and not request.form_processed and request.form_processed is not None:
                auth.unexpire_ticket(request.ticket)
    except HttpException, e:
        start_response(e.status, e.headers)
        return [ e.content ]
    except:
        log_exc()
        start_response('500 Internal Server Error',
                       {'Content-Type': 'text/html'})
        return [ format_exc() ]
    else:
        response = local.response
        start_response('200 OK', response.headers)
        if not hasattr(result, 'read'): return [ result ]
        # result is a file:
        # return [ result.read() ]
        return iter(lambda: result.read(BLOCK_SIZE), '')
        

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

class ServerException(Exception): pass
class   ServerStartException(ServerException): pass
class     ServerAlreadyStarted(ServerStartException): pass
class   ServerStopException(ServerException): pass
class     ServerNotStarted(ServerStopException): pass

class ServerThread(threading.Thread):
    def __init__(self, host, port, application, verbose):
        server = server_threads.setdefault((host, port), self)
        if server != self: raise ServerAlreadyStarted(
            'HTTP server already started: %s:%s' % (host, port))
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.server = CherryPyWSGIServer(
            (host, port), [('', application)], server_name=host)
        self.verbose = verbose
        self.setDaemon(True)
    def run(self):
        msg = 'Starting HTTP server at %s:%s' % (self.host, self.port)
        log('HTTP:start', msg + (', uid=%s' % pony.uid))
        if self.verbose: print>>sys.stderr, msg
        self.server.start()
        msg = 'HTTP server at %s:%s stopped successfully' \
              % (self.host, self.port)
        log('HTTP:stop', msg)
        if self.verbose: print>>sys.stderr, msg
        server_threads.pop((self.host, self.port), None)

def start_http_server(address='localhost:8080', verbose=True):
    if pony.RUNNED_AS == 'MOD_WSGI': return
    pony._do_mainloop = True
    host, port = parse_address(address)
    try:
        server_thread = ServerThread(host, port, application, verbose=verbose)
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
    if pony.RUNNED_AS == 'MOD_WSGI': return
    if address is None:
        for server_thread in server_threads.values():
            server_thread.server.stop()
            server_thread.join()
    else:
        host, port = parse_address(address)
        server_thread = server_threads.get((host, port))
        if server_thread is None:
            raise ServerNotStarted('Cannot stop HTTP server at %s:%s '
                                   'because it is not started:' % (host, port))
        server_thread.server.stop()
        server_thread.join()

@http('/pony/shutdown?uid=$uid')
def http_shutdown(uid=None):
    if uid == pony.uid: return pony.uid

    environ = local.request.environ
    if environ.get('REMOTE_ADDR') != '127.0.0.1': return pony.uid

    if pony.RUNNED_AS == 'INTERACTIVE':
        stop_http_server()
        return '+' + pony.uid

    if pony.RUNNED_AS != 'NATIVE': return pony.uid
    if not (environ.get('HTTP_HOST', '') + ':').startswith('localhost:'): return pony.uid
    if environ.get('SERVER_NAME') != 'localhost': return pony.uid
    pony.shutdown = True
    return '+' + pony.uid

@pony.on_shutdown
def do_shutdown():
    try: stop_http_server()
    except ServerNotStarted: pass
    