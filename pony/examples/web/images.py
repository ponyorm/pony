from pony.main import *

use_autoreload()

@http('/')
def index():
    return html('''
        <h1>Hello, world!</h1>
        <img src="/images/1.jpg">
    ''')

http.start()