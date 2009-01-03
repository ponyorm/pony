from pony.main import *

use_autoreload()

@http('/')
def index():
    f1 = Form(method='GET', action='/action/')
    f1.text = Text()
    f1.btn = Submit('Submit GET request')

    f2 = Form(method='POST', action='/action/')
    f2.text = Text()
    f2.btn = Submit('Submit POST request')
    
    return html('''
    <h2>GET form:</h2>
    $f1
    <br><br><br>
    <h2>POST form:</h2>
    $f2
    ''')

@http('/action/')
def default_handler():
    return html('''
    <h1>Default handler (will never invoke)</h1>
    <p>Value: <strong>$http['text']</strong>
    ''')

@http.GET('/action/')
def get_handler():
    return html('''
    <h1>Handler for GET method</h1>
    <p>Value: <strong>$http['text']</strong>
    ''')

@http.POST('/action/')
def post_handler():
    return html('''
    <h1>Handler for POST method</h1>
    <p>Value: <strong>$http['text']</strong>
    ''')

http.start()
