from pony.main import *

import random

use_autoreload()

@http('/')
def index():
    if 'x' in http.session: http.session['x'] += 1
    return html()

@http('/login?user=$user')
def login(user=None):
    if user: http.user = user
    http.session['x'] = 0
    http.session['y'] = random.random()
    return html()

@http('/logout')
def logout():
    prev_user = http.user
    http.user = None
    return html()

start_http_server('localhost:8080')
show_gui()