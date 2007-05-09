from pony.main import *

use_autoreload()

@http('/')
def index():
    f = Form()
    f.first_name = Text(required=True)
    f.last_name = Text()
    if f.is_submitted and not f.error:
        return html('<h1>Hello, $f.first_name.value!</h1>')
    else:
        return html('''
            <style>
                .required { color: red }
                .error { color: red }
            </style>
            <h1>Please fill the form:</h1>
            $f.html
            ''')

start_http_server()
