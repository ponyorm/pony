import re, os.path
import inspect

from pony.utils import decorator_with_params

re_path = re.compile("""
        [$]
        (?: (\d+)              # param number (group 1)
        |   ([A-Za-z_]\w*)  # param identifier (group 2)
        )$
    |   (                      # path component (group 3)
            (?:[$][$] | [^$])+
        )$                     # end of string
    """, re.VERBOSE)

def parse_path(path):
    result = []
    args, keyargs = set(), set()
    components = path.split('/')
    if not components[0]: components = components[1:]
    for component in components:
        match = re_path.match(component)
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
    return result, len(args), keyargs

http_handlers = ({}, [])

class HttpInfo(object):
    def __init__(self, func, path):
        self.func = func
        self.path = path
        root, ext = os.path.splitext(path)
        self.ext = [ ext ]
        self.parsed_path, self.arg_count, self.keyargs = parse_path(root)
        dict, list = http_handlers
        for is_param, x in self.parsed_path:
            if is_param: dict, list = dict.setdefault(None, ({}, []))
            else: dict, list = dict.setdefault(x, ({}, []))
        list.append(self)
        self.func.__dict__.setdefault('http', []).insert(0, self)

@decorator_with_params
def http(path=None):
    def new_decorator(old_func):
        names, argsname, keyargsname, defaults = inspect.getargspec(old_func)
        if argsname: raise TypeError(
            'HTTP handler function cannot have *%s argument' % argsname)
        if defaults is None: defaults = []
        else: defaults = list(defaults)
        for i, value in enumerate(defaults):
            if value is not None: defaults[i] = unicode(value)
        old_func.argspec = names, argsname, keyargsname, defaults
        HttpInfo(old_func, path is None and old_func.__name__ or path)
        return old_func
    return new_decorator

def get_http_handlers(url):
    root, ext = os.path.splitext(url)
    components = root.split('/')
    if not components[0]: components = components[1:]
    result = []
    triples = [ http_handlers + ({},) ]
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

def get_params(func, args, keyargs):
    args = list(args)
    for i, value in enumerate(args):
        if value is not None: args[i] = unicode(value)
    keyargs = keyargs.copy()
    for name, value in keyargs.items():
        if value is not None: keyargs[name] = unicode(value)
    names, argsname, keyargsname, defaults = func.argspec
    if len(args) > len(names) and not argsname:
        raise TypeError('%s() takes at most %d arguments (%d given)'
                        % (func.__name__, len(names), len(args)))
    diff = len(names) - len(defaults)
    param_list = []
    param_dict = {}
    for i, value in enumerate(args[:len(names)]):
        name = names[i]
        if name in keyargs:
            raise TypeError('%s() got multiple values for keyword argument %r'
                            % (func.__name__, name))
        assert len(param_list) == i
        param = [value, False, i >= diff and value == defaults[i-diff]]
        param_list.append(param)
        param_dict[name] = param
    for i in range(len(args), len(names)):
        name = names[i]
        try: value = keyargs.pop(name)
        except KeyError:
            if i >= diff: value = defaults[i-diff]
            else: raise TypeError(
                        '%s() takes %s %d non-keyword arguments (%d given)'
                        % (func.__name__, defaults and 'at least' or 'exactly',
                           diff, len(args)))
        assert len(param_list) == i
        param = [value, False, i >= diff and value == defaults[i-diff]]
        param_list.append(param)
        param_dict[name] = param
    assert len(param_list) == len(names)
    for i in range(len(names), len(args)):
        param_list.append([value, False, False])
    if keyargs:
        if not keyargsname:
            raise TypeError('%s() got an unexpected keyword argument %r'
                            % (func.__name__, iter(keyargs).next()))
        for name, value in keyargs.items():
            param_dict[name] = [value, False, False]
    return param_list, param_dict

def substitute_params(parsed_path, param_list, param_dict):
    for param in param_list: param[1] = param[2]
    for param in param_dict.values(): param[1] = param[2]
    result = []
    for is_param, x in parsed_path:
        if not is_param:
            result.append(x)
            continue
        elif isinstance(x, int):
            try: param = param_list[x]
            except IndexError: raise TypeError('Invalid parameter $%d' % x)
            if param[0] is None:
                raise TypeError('Invalid value for parameter $%d' % x)
        elif isinstance(x, basestring):
            try: param = param_dict[x]
            except KeyError: raise TypeError('Invalid parameter $%s' % x)
            if param[0] is None:
                raise TypeError('Invalid value for parameter $%s' % x)
        result.append(param[0])
        param[1] = True
    for name, (value, used, optional) in param_dict.items():
        if not used: raise TypeError('Parameter %r is not used' % name)
    for i, (value, used, optional) in enumerate(param_list):
        if not used: raise TypeError('Parameter %d is not used' % i)
    return u'/'.join(result)

def url(func, *args, **keyargs):
    http_list = getattr(func, 'http')
    if http_list is None:
        raise ValueError('Cannot create url for this object :%s' % func)
    param_list, param_dict = get_params(func, args, keyargs)
    if len(http_list) == 1:
        info = http_list[0]
        x = (substitute_params(info.parsed_path, param_list, param_dict)
                 + info.ext[0])
    else:
        for info in http_list:
            try:
                x = (substitute_params(info.parsed_path, param_list, param_dict)
                     + info.ext[0])
                break
            except TypeError, e:
                if e.args[0].startswith('s'): pass
        else: raise TypeError('Suitable url path for %s() not found'
                              % func.__name__)
    return '/' + x
