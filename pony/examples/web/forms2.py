from datetime import date

from pony.main import *

use_autoreload()

@http('/')
def index():
    f = Form()
    f.first_name = Text(required=True)
    f.last_name = Text()
    f.age = Text(type='positive')
    f.birth_date = DatePicker()

##    f.birth_date = Composite(required=True)
##    f.birth_date.year = Text(size=4)
##    f.birth_date.month = Text(size=4)
##    f.birth_date.day = Text(size=2)
##
##    if f.birth_date.is_valid:
##        year, month, day = f.birth_date.value
##        try: birth_date = datetime(int(year), int(month), int(day))
##        except ValueError: f.birth_date.error_text = 'Incorrect date'

    f.btn = Submit('Send')
    return html('''
        @if (f.is_valid) {
            <h1>Hello, @f.first_name.value!</h1>
            <h2>Birth date: @f.birth_date.value</h2>
        } @else {
            <h1>Please fill the form:</h1>
            @f
        }
    ''')

http.start()
