import re, threading, os.path, inspect, sys, cStringIO, itertools
import cgi, cgitb, urllib, Cookie, mimetypes

from operator import itemgetter, attrgetter

from pony.thirdparty.cherrypy.wsgiserver import CherryPyWSGIServer

import pony
from pony import autoreload, auth, utils, xslt
from pony.autoreload import on_reload
from pony.utils import decorator_with_params
from pony.templating import Html, real_stdout
from pony.logging import log, log_exc

class _Http(object):
    def __call__(self, *args, **keyargs):
        return _http(*args, **keyargs)
    def invoke(self, url):
        return invoke(url)
    def remove(self, x):
        return http_remove(x)
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
    class _Session(object):
        def __getattr__(self, attr):
            return auth.local.session.get(attr)
        def __setattr__(self, attr, value):
            if value is None: auth.local.session.pop(attr, None)
            else: auth.local.session[attr] = value
    _session = _Session()
    session = property(attrgetter('_session'))

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
def _http(url=None, redirect=False, system=False, **http_headers):
    http_headers = dict([ (name.replace('_', '-').title(), value)
                          for name, value in http_headers.items() ])
    def new_decorator(old_func):
        real_url = url is None and old_func.__name__ or url
        HttpInfo(old_func, real_url, redirect, system, http_headers)
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
                'Url string contains non-ascii symbols. '
                'Such urls must be in unicode.')
    else: raise ValueError('Url parameter must be str or unicode')
    if '?' in url:
        p, q = url.split('?', 1)
        qlist = []
        qnames = set()
        for name, value in cgi.parse_qsl(q, strict_parsing=strict_parsing,
                                            keep_blank_values=True):
            if name not in qnames:
                qlist.append((name, value))
                qnames.add(name)
            elif strict_parsing:
                raise ValueError('Duplicate url parameter: %s' % name)
    else: p, qlist = url, []
    p, ext = os.path.splitext(p)
    components = p.split('/')
    if not components[0]: components = components[1:]
    path = map(urllib.unquote, components)
    return path, ext, qlist

class HttpInfo(object):
    def __init__(self, func, url, redirect, system, http_headers):
        self.func = func
        if not hasattr(func, 'argspec'):
            func.argspec = self.getargspec(func)
            func.dummy_func = self.create_dummy_func(func)
        self.url = url
        self.path, self.ext, self.qlist = split_url(url, strict_parsing=True)
        self.redirect = redirect
        self.system = system
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
        if system: http_system_handlers.append(self)
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
                try: nextchar = items[i+1][1]
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
                if i in args: raise TypeError('Parameter name %s already in use' % x)
                args.add(i)
                return i
        assert False
    def check(self):
        names, argsname, keyargsname, defaults = self.func.argspec
        if self.star and not argsname: raise TypeError(
            "Function %s does not accept arbitrary argument list" % self.func.__name__)
        args, keyargs = self.args, self.keyargs
        for i, name in enumerate(names[:len(names)-len(defaults)]):
            if i not in args:
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
            return result
        url_map = get_url_map(self)
        qdict = dict(self.qlist)
        http_registry_lock.acquire()
        try:
            for info, _, _ in get_http_handlers(self.path, self.ext, qdict):
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
    key = func, tuple(indexparams), tuple(sorted(keyparams.items()))
    try: return url_cache[key]
    except KeyError: pass
    first, second = [], []
    for info in http_list:
        if not info.redirect: first.append(info)
        else: second.append(info)
    for info in first + second:
        try:
            url = build_url(info, keyparams, indexparams)
        except PathError: pass
        else: break
    else:
        raise PathError('Suitable url path for %s() not found' % func.__name__)
    if len(url_cache) > 1000: url_cache.clear()
    url_cache[key] = url
    return url
make_url = url

def build_url(info, keyparams, indexparams):
    names, argsname, keyargsname, defaults = info.func.argspec
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
        elif isinstance(x, list):
            result = []
            is_default = True
            for is_param, y in x[2:]:
                if not is_param: result.append(y)
                else:
                    is_default_2, component = build_param(y)
                    is_default = is_default and is_default_2
                    if component is None:
                        raise PathError('Value for parameter %s is None' % y)
                    result.append(component)
            return is_default, ''.join(result)
        else: assert False

    for is_param, x in info.parsed_path:
        if not is_param: component = x
        else:
            is_default, component = build_param(x)
            if component is None:
                raise PathError('Value for parameter %s is None' % x)
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
                if value is None:
                    raise PathError('Value for parameter %s is None' % x)
                qlist.append((name, value))
    quote_plus = urllib.quote_plus
    q = "&".join(("%s=%s" % (quote_plus(name), quote_plus(value)))
                 for name, value in qlist)
    if q: q = '?' + q

    errmsg = 'Not all parameters were used during path construction'
    if len(used_keyparams) != len(keyparams):
        raise PathError(errmsg)
    if len(used_indexparams) != len(indexparams):
        for i, value in enumerate(indexparams):
            if (i not in used_indexparams
                and value != defaults[i-offset]):
                    raise PathError(errmsg)

    url = ''.join((p, info.ext, q))
    script_name = local.request.environ.get('SCRIPT_NAME', '')
    return '/'.join((script_name, url))

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

img_template = Html(u'<img src="%s" alt="%s">')

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
    return img_template % (href, description)

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

def get_static_file(path, ext):
    if static_dir is None: raise Http404
    for component in path:
        if not path_re.match(component): raise Http404
    if ext and not path_re.match(ext): raise Http404
    fname = os.path.join(static_dir, *path) + ext
    if not os.path.isfile(fname):
        if path == ['favicon'] and ext == '.ico':
            return get_pony_static_file(path, ext)
        raise Http404
    headers = local.response.headers
    headers['Content-Type'] = guess_type(ext)
    headers['Expires'] = '0'
    headers['Cache-Control'] = 'max-age=10'
    return file(fname, 'rb')

pony_static_dir = os.path.join(os.path.dirname(__file__), 'static')

def get_pony_static_file(path, ext):
    for component in path:
        if not path_re.match(component): raise Http404
    if ext and not path_re.match(ext): raise Http404
    fname = os.path.join(pony_static_dir, *path) + ext
    if not os.path.isfile(fname): raise Http404
    headers = local.response.headers
    headers['Content-Type'] = guess_type(ext)
    max_age = 30 * 60
    headers['Expires'] = Cookie._getdate(max_age)
    headers['Cache-Control'] = 'max-age=%d' % max_age
    return file(fname, 'rb')

def get_http_handlers(path, ext, qdict):
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
        if ext != info.ext: continue
        args, keyargs = {}, {}
        priority = 0
        for i, (is_param, x) in enumerate(info.parsed_path):
            if not is_param:
                priority += 1
                continue
            value = path[i]
            if isinstance(x, int): args[x] = value
            elif isinstance(x, basestring): keyargs[x] = value
            elif isinstance(x, list):
                match = x[1].match(value)
                if not match: break
                params = [ y for is_param, y in x[2:] if is_param ]
                groups = match.groups()
                priority += len(params)
                assert len(params) == len(groups)
                for param, value in zip(params, groups):
                    if isinstance(param, int): args[param] = value
                    elif isinstance(param, basestring): keyargs[param] = value
                    else: assert False
            else: assert False
        else:
            names, _, _, defaults = info.func.argspec
            offset = len(names) - len(defaults)
            non_used_query_params = set(qdict)
            for name, is_param, x in info.parsed_query:
                non_used_query_params.discard(name)
                value = qdict.get(name, not_found)
                if not is_param:
                    if value != x: break
                    priority += 1
                elif isinstance(x, int):
                    if value is not_found:
                        if offset <= x < len(names): continue
                        else: break
                    else: args[x] = value
                elif isinstance(x, basestring):
                    if value is not_found: break
                    keyargs[x] = value
                elif isinstance(x, list):
                    match = x[1].match(value)
                    if not match: break
                    params = [ y for is_param, y in x[2:] if is_param ]
                    groups = match.groups()
                    priority += len(params)
                    assert len(params) == len(groups)
                    for param, value in zip(params, groups):
                        if isinstance(param, int): args[param] = value
                        elif isinstance(param, basestring):
                            keyargs[param] = value
                        else: assert False
                else: assert False
            else:
                arglist = [ None ] * len(names)
                arglist[-len(defaults):] = defaults
                for i, value in sorted(args.items()):
                    try: arglist[i] = value
                    except IndexError:
                        assert i == len(arglist)
                        arglist.append(value)
                if len(info.parsed_path) != len(path):
                    assert info.star
                    arglist.extend(path[len(info.parsed_path):])
                result.append((info, arglist, keyargs, priority,
                               len(non_used_query_params)))
    if result:
        x = max(map(itemgetter(3), result))
        result = [ tup for tup in result if tup[3] == x ]
        x = min(map(itemgetter(4), result))
        result = [ tup[:3] for tup in result if tup[4] == x ]
    return result

def invoke(url):
    local.response = HttpResponse()
    path, ext, qlist = split_url(url)
    if path[:1] == ['static'] and len(path) > 1:
        return get_static_file(path[1:], ext)
    if path[:2] == ['pony', 'static'] and len(path) > 2:
        return get_pony_static_file(path[2:], ext)
    qdict = dict(qlist)
    handlers = get_http_handlers(path, ext, qdict)
    if not handlers:
        i = url.find('?')
        if i == -1: p, q = url, ''
        else: p, q = url[:i], url[i:]
        if p.endswith('/'): url2 = p[:-1] + q
        else: url2 = p + '/' + q
        path2, ext2, qlist = split_url(url2)
        handlers = get_http_handlers(path2, ext2, qdict)
        if not handlers: return get_static_file(path, ext)
        script_name = local.request.environ.get('SCRIPT_NAME', '')
        if not url2: url2 = script_name or '/'
        else: url2 = script_name + url2
        if url2 != script_name + url: raise HttpRedirect(url2)
    info, args, keyargs = handlers[0]
    try:
        for i, value in enumerate(args):
            if value is not None: args[i] = value.decode('utf8')
        for key, value in keyargs.items():
            if value is not None: keyargs[key] = value.decode('utf8')
    except UnicodeDecodeError: raise Http400BadRequest
    if info.redirect:
        for alternative in info.func.http:
            if not alternative.redirect:
                new_url = make_url(info.func, *args, **keyargs)
                status = '301 Moved Permanently'
                if isinstance(info.redirect, basestring): status = info.redirect
                elif isinstance(info.redirect, (int, long)) \
                     and 300 <= info.redirect < 400: status = str(info.redirect)
                raise HttpRedirect(new_url, status)
    local.response.headers.update(info.http_headers)

    names, argsname, keyargsname, defaults = info.func.argspec
    params = local.request.params
    params.update(zip(names, args))
    params.update(keyargs)

    result = info.func(*args, **keyargs)

    headers = dict([ (name.replace('_', '-').title(), value)
                     for name, value in local.response.headers.items() ])
    local.response.headers = headers

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
    info.list.remove(info)
    info.func.http.remove(info)
            
def http_remove(x):
    if isinstance(x, basestring):
        path, ext, qlist = split_url(x, strict_parsing=True)
        qdict = dict(qlist)
        http_registry_lock.acquire()
        try:
            for info, _, _ in get_http_handlers(path, ext, qdict):
                _http_remove(info)
        finally: http_registry_lock.release()
    elif hasattr(x, 'http'):
        http_registry_lock.acquire()
        try: _http_remove(x)
        finally: http_registry_lock.release()
    else: raise ValueError('This object is not bound to url: %r' % x)

def _http_clear(dict, list1, list2):
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

class Http400BadRequest(HttpException):
    status = '400 Bad Request'
    headers = {'Content-Type': 'text/plain'}
    def __init__(self, content='Bad Request'):
        Exception.__init__(self, 'Bad Request')
        self.content = content

class Http404(HttpException):
    status = '404 Not Found'
    headers = {'Content-Type': 'text/plain'}
    def __init__(self, content='Page not found'):
        Exception.__init__(self, 'Page not found')
        self.content = content

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

################################################################################

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

class HttpRequest(object):
    def __init__(self, environ):
        self.environ = environ
        self.cookies = Cookie.SimpleCookie()
        self.method = environ.get('REQUEST_METHOD', 'GET')
        if environ:
            self.full_url = reconstruct_url(environ)
            if 'HTTP_COOKIE' in environ:
                self.cookies.load(environ['HTTP_COOKIE'])
            morsel = self.cookies.get('pony')
            session_data = morsel and morsel.value or None
            auth.load(session_data, environ)
        else:
            self.full_url = None
        input_stream = environ.get('wsgi.input') or cStringIO.StringIO()
        self.params = {}
        self.fields = cgi.FieldStorage(
            fp=input_stream, environ=environ, keep_blank_values=True)
        self.submitted_form = self.fields.getfirst('_f')
        self.ticket_is_valid = auth.verify_ticket(self.fields.getfirst('_t'))
        self.id_counter = itertools.imap('id_%d'.__mod__, itertools.count())

class HttpResponse(object):
    def __init__(self):
        self.headers = {}
        self.cookies = Cookie.SimpleCookie()
        self._http_only_cookies = set()

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
        io = cStringIO.StringIO()
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

def application(environ, wsgi_start_response):
    def start_response(status, headers):
        headers = [ (name, str(value)) for name, value in headers.items() ]
        headers.extend(create_cookies(environ))
        log(type='HTTP:response', text=status, headers=headers)
        wsgi_start_response(status, headers)

    local.request = request = HttpRequest(environ)
    log_request(request)
    url = environ['PATH_INFO']
    query = environ['QUERY_STRING']
    if query: url = '%s?%s' % (url, query)
    try:
        result = invoke(url)
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
        if hasattr(result, 'read'): # result is file
            # return [ result.read() ]
            return iter(lambda: result.read(BLOCK_SIZE), '')
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
        log('HTTP:start', msg)
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

@pony.on_shutdown
def do_shutdown():
    try: stop_http_server()
    except ServerNotStarted: pass
    