from pony.main import *

use_autoreload()

@http('/')
@printhtml
def index():
    f = Form()
    f.first_name = Text(required=True)
    f.last_name = Text()
    if f.is_valid:
        print '<h1>Hello, %s!</h1>' % f.first_name.value
    else:
        print '<h1>Please fill the form:</h1>'
        print f

start_http_server()
