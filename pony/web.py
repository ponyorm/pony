import re, threading, os.path, inspect, sys, warnings, cgi, urllib, Cookie, cPickle, time

from cStringIO import StringIO
from itertools import imap, izip, count
from operator import itemgetter, attrgetter

import pony
from pony import autoreload, auth, httputils, options
from pony.autoreload import on_reload
from pony.utils import decorator_with_params
from pony.templating import Html, StrHtml, plainstr
from pony.logging import log, log_exc, DEBUG, INFO, WARNING
from pony.db import with_transaction, RowNotFound
from pony.htmltb import format_exc

try: from pony.thirdparty import etree
except ImportError: etree = None

class NodefaultType(object):
    def __repr__(self): return '__nodefault__'
    
__nodefault__ = NodefaultType()

http_registry_lock = threading.RLock()
http_registry = ({}, [], [])
http_system_handlers = []

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
        self.path, self.qlist = httputils.split_url(url, strict_parsing=True)
        self.redirect = redirect
        module = func.__module__
        self.system = module.startswith('pony.') and not module.startswith('pony.examples.')
        self.http_headers = http_headers
        self.args = set()
        self.keyargs = set()
        self.parsed_path = []
        self.star = False
        for component in self.path:
            if self.star: raise TypeError("'$*' must be last element in url path")
            elif component != '$*': self.parsed_path.append(self.parse_component(component))
            else: self.star = True
        self.parsed_query = []
        for name, value in self.qlist:
            if value == '$*': raise TypeError("'$*' does not allowed in query part of url")
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
            if x < 0 or x >= len(names) and argsname is None: raise TypeError('Invalid parameter index: %d' % (x+1))
            if x in args: raise TypeError('Parameter index %d already in use' % (x+1))
            args.add(x)
            return x
        elif isinstance(x, basestring):
            try: i = names.index(x)
            except ValueError:
                if keyargsname is None or x in keyargs: raise TypeError('Unknown parameter name: %s' % x)
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
                if i not in args: raise TypeError('Undefined path parameter: %d' % (i+1))
    def register(self):
        def get_url_map(info):
            result = {}
            for i, (is_param, x) in enumerate(info.parsed_path):
                if is_param: result[i] = isinstance(x, list) and x[0] or '/'
                else: result[i] = ''
            for name, is_param, x in info.parsed_query:
                if is_param: result[name] = isinstance(x, list) and x[0] or '/'
                else: result[name] = ''
            if info.star: result['$*'] = len(info.parsed_path)
            if info.host: result[('host',)] = info.host
            if info.port: result[('port',)] = info.port
            return result
        url_map = get_url_map(self)
        qdict = dict(self.qlist)
        http_registry_lock.acquire()
        try:
            for info, _, _ in get_http_handlers(self.path, qdict, self.host, self.port):
                if url_map == get_url_map(info):
                    warnings.warn('Route already in use (old handler was removed): %s' % info.url)
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
    if http_list is None: raise ValueError('Cannot create url for this object :%s' % func)
    try: keyparams = func.dummy_func(*args, **keyargs).copy()
    except TypeError, e: raise TypeError(e.args[0].replace('<lambda>', func.__name__, 1))
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

def http_remove(x, host=None, port=None):
    if isinstance(x, basestring):
        path, qlist = httputils.split_url(x, strict_parsing=True)
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

def _http_remove(info):
    url_cache.clear()
    info.list.remove(info)
    info.func.http.remove(info)
            
@on_reload
def http_clear():
    http_registry_lock.acquire()
    try:
        _http_clear(*http_registry)
        for handler in http_system_handlers: handler.register()
    finally: http_registry_lock.release()

def _http_clear(dict, list1, list2):
    url_cache.clear()
    for info in list1: info.func.http.remove(info)
    list1[:] = []
    for info in list2: info.func.http.remove(info)
    list2[:] = []
    for inner_dict, list1, list2 in dict.itervalues():
        _http_clear(inner_dict, list1, list2)
    dict.clear()

class HttpRequest(object):
    def __init__(self, environ):
        self.environ = environ
        self.method = environ.get('REQUEST_METHOD', 'GET')
        self.cookies = Cookie.SimpleCookie()
        if 'HTTP_COOKIE' in environ: self.cookies.load(environ['HTTP_COOKIE'])
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
            self.script_url = httputils.reconstruct_script_url(environ)
            self.full_url = self.script_url + self.url
        else:
            self.script_url = ''
            self.url = '/'
            self.full_url = 'http://localhost/'
            self.host = 'localhost'
            self.port = 80
        self._base_url = None
        self.languages = self._get_languages()
        self.params = {}
        input_stream = environ.get('wsgi.input') or StringIO()
        self.fields = cgi.FieldStorage(fp=input_stream, environ=environ, keep_blank_values=True)
        self.form_processed = None
        self.submitted_form = self.fields.getfirst('_f')
    def _get_languages(self):
        languages = httputils.parse_accept_language(self.environ.get('HTTP_ACCEPT_LANGUAGE'))
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

element_re = re.compile(r'\s*(?:<!--.*?--\s*>\s*)*(</?\s*([!A-Za-z-]\w*)\b[^>]*>)', re.DOTALL)

header_tags = set("!doctype html head title base script style meta link object".split())

css_re = re.compile('<link\b[^>]\btype\s*=\s*([\'"])text/css\1')

class _UsePlaceholders(Exception): pass

class HttpResponse(object):
    def __init__(self):
        self.status = '200 OK'
        self.headers = {}
        self.cookies = Cookie.SimpleCookie()
        self.postprocessing = True
        self.base_stylesheets = []
        self.component_stylesheets = []
        self.scripts = []
        self.next_id = imap('id_%d'.__mod__, count()).next
    def add_base_stylesheets(self, links):
        stylesheets = self.base_stylesheets
        for link in links:
            if not isinstance(link, (basestring, tuple)): raise TypeError('Reference to CSS stylesheet must be string or tuple. Got: %r' % link)
            if link not in stylesheets: stylesheets.append(link)
    def add_component_stylesheets(self, links):
        stylesheets = self.component_stylesheets
        for link in links:
            if not isinstance(link, (basestring, tuple)): raise TypeError('Reference to CSS stylesheet must be string or tuple. Got: %r' % link)
            if link not in stylesheets: stylesheets.append(link)
    def add_scripts(self, links):
        scripts = self.scripts
        for link in links:
            if not isinstance(link, basestring): raise TypeError('Reference to script must be string. Got: %r' % link)
            if link not in scripts: scripts.append(link)
    def postprocess(self, html):
        if not self.postprocessing: return html
        elif html.__class__ == str: html = StrHtml(html)
        elif html.__class__ == unicode: html = Html(html)
        stylesheets = self.base_stylesheets
        if not stylesheets: stylesheets = options.STD_STYLESHEETS
        base_css = css_links(stylesheets)
        if base_css: base_css += StrHtml('\n')
        component_css = css_links(self.component_stylesheets)
        if component_css: component_css += StrHtml('\n')
        scripts = script_links(self.scripts)
        if scripts: scripts += StrHtml('\n')

        doctype = ''
        try:        
            match = element_re.search(html)
            if match is None or match.group(2).lower() not in header_tags:
                doctype = StrHtml(options.STD_DOCTYPE)
                head = ''
                body = html
            else:
                first_element = match.group(2).lower()

                for match in element_re.finditer(html):
                    element = match.group(2).lower()
                    if element not in header_tags: break
                    last_match = match
                bound = last_match.end(1)
                head = html.__class__(html[:bound])
                body = html.__class__(html[bound:])

                if first_element in ('!doctype', 'html'): raise _UsePlaceholders
                doctype = StrHtml(options.STD_DOCTYPE)

            match = element_re.search(head)
            if match is None or match.group(2).lower() != 'head':
                if css_re.search(head) is not None: base_css = ''
                head = StrHtml('<head>\n%s%s%s%s</head>' % (base_css, head, component_css, scripts))
            else: raise _UsePlaceholders
        except _UsePlaceholders:
            head = head.replace(options.BASE_STYLESHEETS_PLACEHOLDER, base_css, 1)
            head = head.replace(options.COMPONENT_STYLESHEETS_PLACEHOLDER, component_css, 1)
            head = head.replace(options.SCRIPTS_PLACEHOLDER, scripts, 1)
            head = html.__class__(head)

        match = element_re.search(body)
        if match is None or match.group(2).lower() != 'body':
            if 'blueprint' in base_css: body = StrHtml('<div class="container">\n%s\n</div>\n') % body
            body = StrHtml('<body>\n%s</body>') % body

        if doctype: return StrHtml('\n').join([doctype, head, body])
        else: return StrHtml('\n').join([head, body])

def css_link(link):
    if isinstance(link, basestring): link = (link,)
    elif len(link) > 3: raise TypeError('too many parameters for CSS reference')
    href, media, cond = (link + (None, None))[:3]
    result = '<link rel="stylesheet" href="%s" type="text/css"%s>' % (href, media and ' media="%s"' % media or '')
    if cond: result = '<!--[%s]>%s<![endif]-->' % (cond, result)
    return StrHtml(result)

def css_links(links):
    return StrHtml('\n').join(css_link(link) for link in links)

def script_link(link):
    return StrHtml('<script type="text/javascript" src="%s"></script>') % link

def script_links(links):
    return StrHtml('\n').join(script_link(link) for link in links)

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
    httputils.set_cookie(local.response.cookies,
                        name, value, expires, max_age, path, domain, secure, http_only, comment, version)

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
    headers['Content-Type'] = httputils.guess_type(ext)
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
    headers['Content-Type'] = httputils.guess_type(ext)
    max_age = 30 * 60
    headers['Expires'] = Cookie._getdate(max_age)
    headers['Cache-Control'] = 'max-age=%d' % max_age
    return file(fname, 'rb')

def http_invoke(url):
    if isinstance(url, str):
        try: url.decode('utf8')
        except UnicodeDecodeError: raise Http400BadRequest
    request = local.request
    response = local.response = HttpResponse()
    path, qlist = httputils.split_url(url)
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
        path2, qlist = httputils.split_url(url2)
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

    try: result = with_transaction(info.func, *args, **keyargs)
    except RowNotFound: raise Http404NotFound

    headers = dict([ (name.replace('_', '-').title(), value)
                     for name, value in response.headers.items() ])
    response.headers = headers

    media_type = headers.pop('Type', None)
    charset = headers.pop('Charset', None)
    content_type = headers.get('Content-Type')
    if content_type:
        media_type, type_params = cgi.parse_header(content_type)
        charset = type_params.get('charset', 'iso-8859-1')
    else:
        if media_type is not None: pass
        elif isinstance(result, (Html, StrHtml)): media_type = 'text/html'
        else: media_type = getattr(result, 'media_type', 'text/plain')
        charset = charset or getattr(result, 'charset', 'UTF-8')
        content_type = '%s; charset=%s' % (media_type, charset)
        headers['Content-Type'] = content_type

    if media_type == 'text/html': result = response.postprocess(result)
    if hasattr(result, '__unicode__'): result = unicode(result)
    if isinstance(result, unicode):
        if media_type == 'text/html' or 'xml' in media_type :
              result = result.encode(charset, 'xmlcharrefreplace')
        else: result = result.encode(charset, 'replace')
    elif not isinstance(result, str):
        if etree is None: result = str(result)
        else:
            try: result = etree.tostring(result, charset)
            except: result = str(result)

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
        headers=headers, user=user, session=auth.local.session)

BLOCK_SIZE = 65536

def application(environ, start_response):
    sys.stdout = pony.pony_stdout
    sys.stderr = pony.pony_stderr
    error_stream = environ['wsgi.errors']
    wsgi_errors_is_stderr = error_stream is sys.stderr
    if not wsgi_errors_is_stderr: pony.local.error_streams.append(error_stream)
    pony.local.output_streams.append(error_stream)

    request = local.request = HttpRequest(environ)
    auth.load(environ, request.cookies)
    auth.verify_ticket(request.fields.getfirst('_t'))
    try:
        log_request(request)
        if autoreload.reloading_exception and not request.url.startswith('/pony/static/'):
            status = '500 Internal Server Error'
            headers = {'Content-Type': 'text/html'}
            result = local.response.postprocess(format_exc(autoreload.reloading_exception))
        else:
            try:
                try:
                    if auth.local.ticket_payload is not None:
                        form = cPickle.loads(auth.local.ticket_payload)
                        form._handle_request_()
                        form = None
                    result = http_invoke(request.url)
                finally:
                    if auth.local.ticket and not request.form_processed and request.form_processed is not None:
                        auth.unexpire_ticket()
            except HttpException, e:
                status, headers, result = e.status, e.headers, e.content
            except:
                log_exc()
                status = '500 Internal Server Error'
                headers = {'Content-Type': 'text/html'}
                result = local.response.postprocess(format_exc())
            else:
                status = local.response.status
                headers = local.response.headers

        headers = [ (name, str(value)) for name, value in headers.items() ]
        if not status.startswith('5'):
            auth.save(local.response.cookies)
            headers += httputils.serialize_cookies(environ, local.response.cookies)
        if not hasattr(result, 'read'): content = [ result ]
        else: content = iter(lambda: result.read(BLOCK_SIZE), '')  # content = [ result.read() ]

        log(type='HTTP:response', prefix='Response: ', text=status, severity=DEBUG, headers=headers)
        start_response(status, headers)
        return content
    finally:
        top_output_stream = pony.local.output_streams.pop()
        assert top_output_stream is error_stream
        if not wsgi_errors_is_stderr:
            top_error_stream = pony.local.error_streams.pop()
            assert top_error_stream is error_stream

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
    def __init__(self, host, port, application):
        server = server_threads.setdefault((host, port), self)
        if server != self: raise ServerAlreadyStarted('HTTP server already started: %s:%s' % (host, port))
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        from pony.thirdparty.cherrypy.wsgiserver import CherryPyWSGIServer
        self.server = CherryPyWSGIServer((host, port), application, server_name=host)
        self.setDaemon(True)
    def run(self):
        message = 'Starting HTTP server at %s:%s' % (self.host, self.port)
        log(type='HTTP:start', text=message, severity=WARNING, host=self.host, port=self.port, uid=pony.uid)
        self.server.start()
        message = 'HTTP server at %s:%s stopped successfully' % (self.host, self.port)
        log(type='HTTP:stop', text=message, severity=WARNING, host=self.host, port=self.port)
        server_threads.pop((self.host, self.port), None)

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

@decorator_with_params
def http(old_func, url=None, host=None, port=None, redirect=False, **http_headers):
    real_url = url is None and old_func.__name__ or url
    http_headers = dict([ (name.replace('_', '-').title(), value)
                          for name, value in http_headers.items() ])
    HttpInfo(old_func, real_url, host, port, redirect, http_headers)
    return old_func
register_http_handler = http

class _Http(object):
    __call__ = staticmethod(register_http_handler)
    invoke = staticmethod(http_invoke)
    remove = staticmethod(http_remove)
    clear = staticmethod(http_clear)
    start = staticmethod(start_http_server)
    stop = staticmethod(stop_http_server)

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
        return auth.local.session.get('lang')
    def set_lang(self, lang):
        if lang and not isinstance(lang, basestring):
            raise TypeError('http.lang must be string. Got: %s' % lang)
        lang = plainstr(lang)
        if not lang: auth.local.session.pop('lang', None)
        else: auth.local.session['lang'] = lang
        local.request.languages = local.request._get_languages()
    lang = property(get_lang, set_lang)

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

http = _Http()

class HttpException(Exception):
    content = ''
http.Exception = HttpException

class Http400BadRequest(HttpException):
    status = '400 Bad Request'
    headers = {'Content-Type' : 'text/plain'}
    def __init__(self, content='Bad Request'):
        Exception.__init__(self, 'Bad Request')
        self.content = content
http.BadRequest = Http400BadRequest
        
class Http404NotFound(HttpException):
    status = '404 Not Found'
    headers = {'Content-Type' : 'text/plain'}
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
    def __init__(self, location=None, status='302 Found'):
        if location and not isinstance(location, basestring):
            raise TypeError('Redirect location must be string. Got: %r' % location)
        Exception.__init__(self, location)
        self.location = location or local.request.full_url
        status = str(status)
        self.status = self.status_dict.get(status, status)
        self.headers = {'Location' : location}
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

@decorator_with_params
def component(old_func, css=None, js=None):
    def new_func(*args, **keyargs):
        response = local.response
        if css is not None:
            if isinstance(css, (basestring, tuple)):
                  response.add_component_stylesheets([ css ])
            else: response.add_component_stylesheets(css)
        if js is not None:
            if isinstance(js, basestring):
                  response.add_scripts([ js ])
            else: response.add_scripts(js)
        return old_func(*args, **keyargs)
    return new_func
