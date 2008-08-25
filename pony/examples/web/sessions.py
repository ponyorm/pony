from pony.main import *

import random

use_autoreload()

@http('/')
def index():
    conv = http.conversation['z'] = http.conversation.get('z', 0) + 1
    if 'x' in http.session: http.session['x'] += 1
    return html()

@http('/login?user=$user')
def login(user=None):
    if user: http.set_user(user, longlife_session=True, remember_ip=True)
    http.session['x'] = 0
    http.session['y'] = random.random()
    return html()

@http('/logout')
def logout():
    prev_user = http.user
    http.user = None
    return html()

http.start()
# show_gui()