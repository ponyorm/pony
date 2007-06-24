from pony.main import *

use_autoreload()

@http('/')
def index():
    return html()

start_http_server()