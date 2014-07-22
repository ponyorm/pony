from __future__ import absolute_import, print_function, division

from bottle import HTTPResponse, HTTPError
from pony.orm.core import db_session

def is_allowed_exception(e):
    return isinstance(e, HTTPResponse) and not isinstance(e, HTTPError)

class PonyPlugin(object):
    name = 'pony'
    api  = 2
    def apply(self, callback, route):
        return db_session(allowed_exceptions=is_allowed_exception)(callback)
