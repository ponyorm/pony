from pony.main import *

@http
@html
def index1(self):
    print '<html><head>'
    print '<title>%s</title>' % self.title
    print '<link rel="stylesheet" type="text/css" href="%s">' % self.css
    print '</head><body>'
    print '<h1>%s</h1>' % self.greeting.upper()
    print '<div id="main">'
    print '<ul class="main-list">'
    for text in self.list_content:
        print '<li>%s</li>' % text
    print '</ul></div>'
    print '</body></html>'

@http
@html
def index2(self):
    with tag.html:
        with tag.head:
            print tag.title(self.title)
            print tag.link(rel="stylesheet", type="text/css", href=self.css)
        with tag.body:
            print tag.h1(self.greeting.upper())
            with tag.div(id="main"):
                with tag.ul(class_="main-list"):
                    for text in self.list_content: print tag.li(text)

@http
def index3(self):
    return html("""
    <html><head>
        <title>$self.title</title>
        <link rel="stylesheet" type="text/css" href="$self.css">
    </head><body>
        <h1>$self.greeting.upper()</h1>
        <div id="main">
            <ul class="main-list">
            $(for text in self.list_content){<li>$text</li>}
            </ul>
        </div>
    </body></html>
    """)

start_http_server('localhost:8080')
