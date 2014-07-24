from __future__ import absolute_import, print_function, division
from pony.py23compat import iteritems

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
    '.asc': 'text/plain',
    '.diff': 'text/plain',
    '.csv': 'text/comma-separated-values',
    '.rss': 'application/rss+xml',
    '.text': 'text/plain',
    '.wbmp': 'image/vnd.wap.wbmp',
    '.dwg': 'image/x-dwg',
    '.ico': 'image/x-icon',
    '.bz2': 'application/x-bzip2',
    '.gz': 'application/x-gzip'
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

def reconstruct_url(environ):
    url = reconstruct_script_url(environ)
    url += quote(environ['PATH_INFO'])
    query = environ['QUERY_STRING']
    if query: url += '?' + query
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
    languages = sorted((q, lang) for lang, q in iteritems(languages))
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
    path = [ unquote(component) for component in components ]
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

def set_cookie(cookies, name, value, expires=None, max_age=None, path=None, domain=None,
               secure=False, http_only=False, comment=None, version=None):
    if value is None: cookies.pop(name, None); return
    cookies[name] = value
    morsel = cookies[name]
    if expires is not None: morsel['expires'] = expires
    if max_age is not None: morsel['max-age'] = max_age
    if path is not None: morsel['path'] = path
    if domain is not None: morsel['domain'] = domain
    if comment is not None: morsel['comment'] = comment
    if version is not None: morsel['version'] = version
    if secure: morsel['secure'] = True
    morsel.http_only = http_only

http_only_incompatible_browsers = re.compile(r'''
    WebTV
    | MSIE[ ]5[.]0;[ ]Mac
    | Firefox/[12][.]
''', re.VERBOSE)

def serialize_cookies(environ, cookies):
    user_agent = environ.get('HTTP_USER_AGENT', '')
    support_http_only = http_only_incompatible_browsers.search(user_agent) is None
    result = []
    for name, morsel in cookies.items():
        cookie = morsel.OutputString().rstrip()
        if support_http_only and getattr(morsel, 'http_only', False):
            if not cookie.endswith(';'): cookie += '; HttpOnly'
            else: cookie += ' HttpOnly'
        result.append(('Set-Cookie', cookie))
    return result

def http_put(url, type, data):
    # Source: http://stackoverflow.com/questions/111945/is-there-anyway-to-do-http-put-in-python
    import urllib2
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request(url, data)
    request.add_header('Content-Type', type)
    request.get_method = lambda: 'PUT'
    return opener.open(request)
