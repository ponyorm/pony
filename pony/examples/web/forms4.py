from __future__ import print_function

from pony.main import *

use_autoreload()

class MyForm(Form):
    def __init__(self):
        Form.__init__(self, name='MyForm', method='POST')
        self.first_name = Text(required=True)
        self.last_name = Text()
        self.age = Text(type=int)
        self.btn = Submit(value='Submit')
    def validate(self):
        age = self.age.value
        if age is None: pass
        elif age < 10: self.age.error_text = "Must be 10 at least"
        elif age > 120: self.age.error_text = "Must not be greater then 120"
    def on_submit(self):
        http.session['fname'] = self.first_name.value
        print(self.first_name.value)

@http('/')
def index():
    fname = http.session.__dict__.pop('fname', '')
    if fname: return html('<h1>@fname</h1>')
    f = MyForm()
    return f
    
http.start()
show_gui()
