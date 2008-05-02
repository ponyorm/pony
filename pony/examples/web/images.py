from pony.main import *

use_autoreload()

@webpage('/')
def index():
    print '<h1>Hello, world!</h1>'
    print '<img src="/images/1.jpg">'

http.start()