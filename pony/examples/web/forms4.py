from pony.main import *

use_autoreload()

class MyForm(Form):
    def __init__(self):
        self.first_name = Text(required=True)
        self.last_name = Text()
        self.age = Text(type=int)
    def validate(self):
        age = self.age.value
        if age is None: pass
        elif age < 10: self.age.error_text = "Must be 10 at least"
        elif age > 120: self.age.error_text = "Must not be greater then 120"
    def on_submit(self):
        print self.first_name.value

@http('/')
@printhtml
def index():
    f = MyForm()
    print f
    
start_http_server()
show_gui()
