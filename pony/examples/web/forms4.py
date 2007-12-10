from pony.main import *

use_autoreload()

class MyForm(Form):
    first_name = Text(required=True)
    last_name = Text()
    age = Text()
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
    f = MyForm()
    print f
    
start_http_server()
