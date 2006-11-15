
from pony.main import template, Field

def GET(name=None):
    if name is None:
        result = template('Please enter your <b>/name</b> after url')
    else:
        result = template('Hello, <b>&(name)</b>')
    return template('<html><body>&(result)</body></html>')

def GET():
    form = pony.form(action='get', layout='horizontal')
    form.user_name = Field(string)
    if form.user_name.has_value():
        return template("""
        <html><body>Hello, &(form.user_name.value)!</body></html>
        """)
    else:
        return template("""
        <html><body>Please enter your name: &(form)</body></html>
        """)

