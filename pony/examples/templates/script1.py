from pony.templating import html

def layout(title, content):
    return html("""
    <html>
    <head><title>$title</title></head>
    <body>$content</body>
    </html>
    """)

class A(object):
    def __init__(self, name):
        self.name = name
    def hello(self):
        return html()

a = A('John')
print a.hello()