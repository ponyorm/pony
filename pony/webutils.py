import re, mimetypes

from urllib import quote, unquote
from cgi import parse_qsl

# from SimpleHTTPServer
if not mimetypes.inited: mimetypes.init() # try to read system mime.types
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

    url += quote(script_name)
    return url

q_re = re.compile('\s*q\s*=\s*([0-9.]+)\s*')

def parse_accept_language(s):
    if not s: return []
    languages = {}
    for lang in s.lower().split(','):
        lang = lang.strip()
        if not lang: continue
        if ';' not in lang: languages[lang.strip()] = 1
        else:
            lang, params = lang.split(';', 1)
            q = 1
            for params in params.split(';'):
                match = q_re.match(params)
                if match is not None:
                    try: q = float(match.group(1))
                    except: pass
            lang = lang.strip()
            if lang: languages[lang] = max(q, languages.get(lang))
    languages = sorted((q, lang) for lang, q in languages.iteritems())
    languages.reverse()
    return [ lang for q, lang in languages ]

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
        for name, value in parse_qsl(q, strict_parsing=strict_parsing, keep_blank_values=True):
            if name not in qnames:
                qlist.append((name, value))
                qnames.add(name)
            elif strict_parsing: raise ValueError('Duplicate url parameter: %s' % name)
    else: p, qlist = url, []
    components = p.split('/')
    if not components[0]: components = components[1:]
    path = map(unquote, components)
    return path, qlist

def parse_address(address):
    if isinstance(address, basestring):
        if ':' in address:
            host, port = address.split(':')
            return host, int(port)
        else:
            return address, 80
    assert len(address) == 2
    return tuple(address)
