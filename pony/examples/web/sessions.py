from pony.main import *

@http('/')
def index():
    user = get_user()
    return html()

@http('/login?user=$user')
def login(user=None):
    if user: set_user(user)
    return html()

@http('/logout')
def logout():
    user = get_user()
    set_user(None)
    return html()

start_http_server('localhost:8080')
