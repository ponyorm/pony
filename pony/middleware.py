
from pony import db, debugging
from pony.utils import simple_decorator
from pony.autoreload import on_reload

wsgi_middleware_list = []
pony_middleware_list = []
decorator_list = []

def wsgi_wrap(application):
    for m in wsgi_middleware_list: application = m(application)
    return application

def pony_wrap(app):
    for m in pony_middleware_list: app = m(app)
    return app

def decorator_wrap(func):
    for d in decorator_list: func = d(func)
    return func

@on_reload
def init():
    wsgi_middleware_list[:] = []
    pony_middleware_list[:] = [ debugging.debug_middleware ]
    decorator_list[:] = [ debugging.middleware_decorator, db.middleware_decorator ]

init()
