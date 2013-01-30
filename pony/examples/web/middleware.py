from pony.main import *
from pony import middleware
from pony.templating import Html, StrHtml

use_autoreload()

http.start()

@http('/')
def index():
    return html('''
    <h1>Hello, world!!!</h1>
    <p><a href="/test/100/200">test page</a></p>
    <p>@([ getattr(x, '__name__', '?') for x in middleware.decorators ])
    <p>@([ getattr(x, '__name__', '?') for x in middleware.pony_middleware ])
    ''')

@http('/TEST/$a/$b')
def test(a, b):
    return html('''
    <h1>Test:</h1>
    <h2>a = @a</h2>
    <h2>b = @b</h2>
    ''')

def uppercase_decorator(func):
    def new_func(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(result, (Html, StrHtml)): result = Html(result.upper())
        http.response.headers['X-Bar'] = 'Yes'
        return result
    return new_func

middleware.decorators.append(uppercase_decorator)

def pony_middleware(app):
    def new_app(environ):
        status, headers, content = app(environ)
        if not isinstance(content, basestring):
            return status, headers, content
        header_dict = dict(headers)
        if not header_dict.get('Content-Type', '').startswith('text/html'):
            return status, headers, content

        content = content.replace('<H1>', '<H1><font color="red">')
        content = content.replace('</H1>', '</font></H1>')

        headers = [ (name, value) for name, value in headers if name != 'Content-Length' ]
        headers.append(('Content-Length', str(len(content))))
        headers.append(('X-Foo', 'Yes'))
        return status, headers, content
    return new_app

middleware.pony_middleware.append(pony_middleware)
