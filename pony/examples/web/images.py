from pony.main import *

use_autoreload()

@http('/')
@printhtml
def index():
    print '<h1>Hello, world!</h1>'
    print '<img src="/images/1.jpg">'

start_http_server()