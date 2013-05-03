from bottle import HTTPResponse, HTTPError
from pony.orm.core import with_transaction

def is_allowed_exception(e):
    return isinstance(e, HTTPResponse) and not isinstance(e, HTTPError)

class PonyPlugin(object):
    name = 'pony'
    api  = 2
    def apply(self, callback, route):
        return with_transaction(allowed_exceptions=is_allowed_exception)(callback)
