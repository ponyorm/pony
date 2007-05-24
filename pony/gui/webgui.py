
from pony.templating import printhtml
from pony.web import http, get_request

@http('/pony/test', system=True)
@printhtml
def test():
    print '<h1>Content of request headers</h1>'
    print '<table border="1">'
    print '<tr><th>Header</th><th>Value</th></tr>'
    for key, value in sorted(get_request().environ.items()):
        if value == '': value='&nbsp;'
        print '<tr><td>%s</td><td>%s</td></tr>' % (key, value)
    print '</table>'
