from pony.main import *

use_autoreload()

class MyForm(Form):
    def __init__(self, first_name, last_name):
        self.first_name = Text(value=first_name, required=True)
        self.last_name = Text(value=last_name)
        self.age = Text()
    def validate(self):
        if self.age.value:
            try: int(self.age.value)
            except ValueError:
                self.age.error_text = 'Must be number!'
    def on_submit(self):
        print self.first_name.value, self.last_name.value, self.age.value

@http('/')
@printhtml
def index():
    f = MyForm('John', 'Smith')
    print f
    
start_http_server()
