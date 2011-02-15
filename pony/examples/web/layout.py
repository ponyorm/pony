from pony.templating import html

class Layout(object):
    def header(self, markup):
        self._header = markup
    def footer(self, markup):
        self._footer = markup
    def __str__(self):
        return html('''
            <div>
              <h1>@(self._header)</h1>
              <h2>@(self._footer)</h2>
            </div>
            ''')
    
print html("""

@Layout()
@+header{<strong>This is header</strong>}
@+footer{<em>This is footer</em>}

@Layout()
@+header{<strong>This is header</strong>}
@+footer{<em>This is footer</em>}

""")