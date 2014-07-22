from __future__ import absolute_import, print_function

from pony.autoreload import on_reload
from pony.compression import compression_middleware
from pony.debugging import debugging_middleware, debugging_decorator
from pony.orm.core import db_decorator

wsgi_middleware = []
pony_middleware = []
decorators = []

def wsgi_wrap(application):
    for m in reversed(wsgi_middleware): application = m(application)
    return application

def pony_wrap(app):
    for m in reversed(pony_middleware): app = m(app)
    return app

def decorator_wrap(func):
    for d in reversed(decorators): func = d(func)
    return func

@on_reload
def init():
    wsgi_middleware[:] = []
    pony_middleware[:] = [ compression_middleware, debugging_middleware ]
    decorators[:] = [ db_decorator, debugging_decorator ]

init()
