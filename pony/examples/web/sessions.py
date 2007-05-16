from pony.main import *

import random

use_autoreload()

@http('/')
def index():
    user = get_user()
    session = get_session()
    if 'x' in session: session['x'] += 1
    return html()

@http('/login?user=$user')
def login(user=None):
    if user: set_user(user)
    session = get_session()
    session['x'] = 0
    session['y'] = random.random()
    return html()

@http('/logout')
def logout():
    user = get_user()
    set_user(None)
    return html()

start_http_server('localhost:8080')
show_gui()