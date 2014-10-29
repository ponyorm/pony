from __future__ import absolute_import, print_function
from pony.py23compat import izip, itervalues

import re, threading, inspect, warnings, urllib
from operator import itemgetter

import pony
from pony.httputils import split_url
from pony.autoreload import on_reload

class NodefaultType(object):
    def __repr__(self): return '__nodefault__'

__nodefault__ = NodefaultType()

registry_lock = threading.RLock()
registry = ({}, [], [])
system_routes = []
user_routes = []

url_cache = {}

class Route(object):
    def __init__(route, func, url, method, host, port, redirect, headers):
        url_cache.clear()
        route.func = func
        module = func.__module__
        route.system = module.startswith('pony.') and not module.startswith('pony.examples.')
        argspec = getattr(func, 'argspec', None)
        if argspec is None:
            argspec = func.argspec = route.getargspec(func)
            func.dummy_func = route.create_dummy_func(func)
        if url is not None: route.url = url
        elif argspec[1] is not None: raise TypeError('Not supported: *%s' % argspec[1])
        elif argspec[2] is not None: raise TypeError('Not supported: **%s' % argspec[2])
        else:
            url = func.__name__
            if argspec[0]: url = '?'.join((url, '&'.join('=$'.join((argname, argname)) for argname in argspec[0])))
            route.url = url
        if method is not None and method not in ('HEAD', 'GET', 'POST', 'PUT', 'DELETE'):
            raise TypeError('Invalid HTTP method: %r' % method)
        route.method = method
        if host is not None:
            if not isinstance(host, basestring): raise TypeError('Host must be string')
            if ':' in host:
                if port is not None: raise TypeError('Duplicate port specification')
                host, port = host.split(':')
        route.host = host
        route.port = int(port) if port else None
        route.path, route.qlist = split_url(url, strict_parsing=True)
        route.redirect = redirect
        route.headers = dict([ (name.replace('_', '-').title(), value) for name, value in headers.items() ])
        route.args = set()
        route.kwargs = set()
        route.parsed_path = []
        route.star = False
        for component in route.path:
            if route.star: raise TypeError("'$*' must be last element in url path")
            elif component != '$*': route.parsed_path.append(route.parse_component(component))
            else: route.star = True
        route.parsed_query = []
        for name, value in route.qlist:
            if value == '$*': raise TypeError("'$*' does not allowed in query part of url")
            is_param, x = route.parse_component(value)
            route.parsed_query.append((name, is_param, x))
        route.check()
        route.register()
    @staticmethod
    def getargspec(func):
        original_func = getattr(func, 'original_func', func)
        names, argsname, keyargsname, defaults = inspect.getargspec(original_func)
        defaults = list(defaults) if defaults else []
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
    def parse_component(route, component):
        items = list(route.split_component(component))
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
    def split_component(route, component):
        pos = 0
        is_param = False
        for match in route.component_re.finditer(component):
            if match.start() != pos:
                raise ValueError('Invalid url component: %r' % component)
            i = match.lastindex
            if 1 <= i <= 2:
                if is_param: raise ValueError('Invalid url component: %r' % component)
                is_param = True
                if i == 1: yield is_param, route.adjust(int(match.group(i)) - 1)
                elif i == 2: yield is_param, route.adjust(match.group(i))
            elif i == 3:
                is_param = False
                yield is_param, match.group(i).replace('$$', '$')
            else: assert False  # pragma: no cover
            pos = match.end()
        if pos != len(component):
            raise ValueError('Invalid url component: %r' % component)
    def adjust(route, x):
        names, argsname, keyargsname, defaults, converters = route.func.argspec
        args, kwargs = route.args, route.kwargs
        if isinstance(x, int):
            if x < 0 or x >= len(names) and argsname is None: raise TypeError('Invalid parameter index: %d' % (x+1))
            if x in args: raise TypeError('Parameter index %d already in use' % (x+1))
            args.add(x)
            return x
        elif isinstance(x, basestring):
            try: i = names.index(x)
            except ValueError:
                if keyargsname is None or x in kwargs: raise TypeError('Unknown parameter name: %s' % x)
                kwargs.add(x)
                return x
            else:
                if i in args: raise TypeError('Parameter name %s already in use' % x)
                args.add(i)
                return i
        assert False  # pragma: no cover
    def check(route):
        names, argsname, keyargsname, defaults, converters = route.func.argspec
        if route.star and not argsname: raise TypeError(
            "Function %s does not accept arbitrary argument list" % route.func.__name__)
        args, kwargs = route.args, route.kwargs
        diff = len(names) - len(defaults)
        for i, name in enumerate(names[:diff]):
            if i not in args: raise TypeError('Undefined path parameter: %s' % name)
        for i, name, default in izip(xrange(diff, diff+len(defaults)), names[diff:], defaults):
            if default is __nodefault__ and i not in args:
                raise TypeError('Undefined path parameter: %s' % name)
        if args:
            for i in range(len(names), max(args)):
                if i not in args: raise TypeError('Undefined path parameter: %d' % (i+1))
    def _get_url_map(route):
        result = {}
        for i, (is_param, x) in enumerate(route.parsed_path):
            if is_param: result[i] = x[0] if isinstance(x, list) else '/'
            else: result[i] = ''
        for name, is_param, x in route.parsed_query:
            if is_param: result[name] = x[0] if isinstance(x, list) else '/'
            else: result[name] = ''
        if route.star: result['$*'] = len(route.parsed_path)
        if route.host: result[('host',)] = route.host
        if route.port: result[('port',)] = route.port
        return result
    def register(route):
        url_map = route._get_url_map()
        qdict = dict(route.qlist)
        with registry_lock:
            for route, _, _ in get_routes(route.path, qdict, route.method, route.host, route.port):
                if url_map != route._get_url_map() or route.method != route.method: continue
                if pony.MODE != 'INTERACTIVE':
                    warnings.warn('Url path already in use (old route was removed): %s' % route.url)
                _remove(route)
            d, list1, list2 = registry
            for is_param, x in route.parsed_path:
                if is_param: d, list1, list2 = d.setdefault(None, ({}, [], []))
                else: d, list1, list2 = d.setdefault(x, ({}, [], []))
            if not route.star: route.list = list1
            else: route.list = list2
            route.func.__dict__.setdefault('routes', []).insert(0, route)
            route.list.insert(0, route)
            if route.system and route not in system_routes: system_routes.append(route)
            else: user_routes.append(route)

def get_routes(path, qdict, method, host, port):
    # registry_lock.acquire()
    # try:
    variants = [ registry ]
    routes = []
    for i, component in enumerate(path):
        new_variants = []
        for d, list1, list2 in variants:
            variant = d.get(component)
            if variant: new_variants.append(variant)
            # if component:
            variant = d.get(None)
            if variant: new_variants.append(variant)
            routes.extend(list2)
        variants = new_variants
    for d, list1, list2 in variants: routes.extend(list1)
    # finally: registry_lock.release()

    result = []
    not_found = object()
    for route in routes:
        args, kwargs = {}, {}
        priority = 0
        if route.host is not None:
            if route.host != host: continue
            priority += 8000
        if route.port is not None:
            if route.port != port: continue
            priority += 4000
        if method == route.method:
            if method is not None: priority += 2000
        elif route.method is None and method in ('HEAD', 'GET', 'POST'): pass
        elif route.method == 'GET' and method == 'HEAD': priority += 1000
        else: continue

        for i, (is_param, x) in enumerate(route.parsed_path):
            if not is_param:
                priority += 1
                continue
            value = path[i].decode('utf8')
            if isinstance(x, int): args[x] = value
            elif isinstance(x, basestring): kwargs[x] = value
            elif isinstance(x, list):
                match = x[1].match(value)
                if not match: break
                params = [ y for is_param, y in x[2:] if is_param ]
                groups = match.groups()
                n = len(x) - len(params)
                if not x[-1][0]: n += 1
                priority += n
                assert len(params) == len(groups)
                for param, value in izip(params, groups):
                    if isinstance(param, int): args[param] = value
                    elif isinstance(param, basestring): kwargs[param] = value
                    else: assert False  # pragma: no cover
            else: assert False  # pragma: no cover
        else:
            names, _, _, defaults, converters = route.func.argspec
            diff = len(names) - len(defaults)
            non_used_query_params = set(qdict)
            for name, is_param, x in route.parsed_query:
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
                    kwargs[x] = value
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
                    for param, value in izip(params, groups):
                        if isinstance(param, int): args[param] = value
                        elif isinstance(param, basestring):
                            kwargs[param] = value
                        else: assert False  # pragma: no cover
                else: assert False  # pragma: no cover
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
                    if len(route.parsed_path) != len(path):
                        assert route.star
                        arglist.extend(path[len(route.parsed_path):])
                    result.append((route, arglist, kwargs, priority, len(non_used_query_params)))
    if result:
        x = max(tup[3] for tup in result)
        result = [ tup for tup in result if tup[3] == x ]
        x = min(tup[4] for tup in result)
        result = [ tup[:3] for tup in result if tup[4] == x ]
    return result

class PathError(Exception): pass

def build_url(route, keyparams, indexparams, host, port, script_name):
    names, argsname, keyargsname, defaults, converters = route.func.argspec
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
            except KeyError: assert False, 'Parameter not found: %r' % x  # pragma: no cover
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
        else: assert False  # pragma: no cover

    for is_param, x in route.parsed_path:
        if not is_param: component = x
        else:
            is_default, component = build_param(x)
            if component is None: raise PathError('Value for parameter %r is None' % x)
        path.append(urllib.quote(component, safe=':@&=+$,'))
    if route.star:
        for i in range(len(route.args), len(indexparams)):
            path.append(urllib.quote(indexparams[i], safe=':@&=+$,'))
            used_indexparams.add(i)
    p = '/'.join(path)

    qlist = []
    for name, is_param, x in route.parsed_query:
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

    url = '?'.join((p, q)) if q else p
    result = '/'.join((script_name, url))
    if route.host is None or route.host == host:
        if route.port is None or route.port == port: return result
    host = route.host or host
    port = route.port or 80
    if port == 80: return 'http://%s%s' % (host, result)
    return 'http://%s:%d%s' % (host, port, result)

def remove(x, method=None, host=None, port=None):
    if isinstance(x, basestring):
        path, qlist = split_url(x, strict_parsing=True)
        qdict = dict(qlist)
        with registry_lock:
            for route, _, _ in get_routes(path, qdict, method, host, port): _remove(route)
    elif hasattr(x, 'routes'):
        assert host is None and port is None
        with registry_lock:
            for route in list(x.routes): _remove(route)
    else: raise ValueError('This object is not bound to url: %r' % x)

def _remove(route):
    url_cache.clear()
    route.list.remove(route)
    route.func.routes.remove(route)
    if route.system: system_routes.remove(routre.url)
    else: user_routes.remove(route)

@on_reload
def clear():
    with registry_lock:
        _clear(*registry)
        for route in system_routes: route.register()
        del(user_routes[:])

def _clear(dict, list1, list2):
    url_cache.clear()
    for route in list1: route.func.routes.remove(route)
    list1[:] = []
    for route in list2: route.func.routes.remove(route)
    list2[:] = []
    for inner_dict, list1, list2 in itervalues(dict):
        _clear(inner_dict, list1, list2)
    dict.clear()
