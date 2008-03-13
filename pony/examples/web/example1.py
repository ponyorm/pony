# -*- coding: cp1251 -*-

from pony.main import *
from pony import utils

use_autoreload()

@http
def page1():
    return 'Hello, world!'

@http(type='text/html')
def page2():
    return """
              <h1>This is HTML page</h1>
              <br>
              <p>Click <a href="/">here</a> for return
           """

@http
def page3():
    "This page deliberately contains error"
    a = 'hello'
    b = 10
    c = 1/0
    return a
    
@http
@printhtml
def page4():
    "Using of <strong>@printhtml</strong> decorator"
    print '<html>'
    print '<head><title>Page 3</title></head>'
    print '<body>'
    print '<h1>Hello again!</h1>'
    print '<br>'
    print '<p>Demonstration of <strong>@printhtml</strong> decorator'
    print '<p>Click <a href="/">here</a> for return'
    print '</body>'
    print '</html>'

@printhtml
def header(title='Demonstration of Pony features'):
    print '<title>%s</title>' % title
    print '<h1>%s</h1>' % title
    print '<br>'

@printhtml
def footer():
    print '<p>Click <a href="/">here</a> to return to main page'

@http('mypage.html')
@printhtml
def page5():
    "Page with custom URL"
    print header('Demonstration of custom URL')
    print '<p>This page have custom URL'
    print '<p>It also demonstrate how common parts of page'
    print '(such as header or footer) can be factored out'
    print footer()

@http('/myblog/archives/$year/posts.html')
@http('/myblog/archives/$year/$month/posts.html')
@http('/myblog/$lang/archives/$year/posts.html')
@http('/myblog/$lang/archives/$year/$month/posts.html')
@printhtml
def page6(year, month=None, lang='en'):
    "Parameters encoded in URL"
    print header('My Blog Archives')
    print '<p>Demonstration how parameters can be encoded in URL'
    print '<ul>'
    print '<li>Language: <strong>%s</strong>' % lang
    print '<li>Year: <strong>%s</strong>' % year
    print '<li>Month: <strong>%s</strong>' % (month or 'Not given')
    print '</ul>'
    print '<p><a href="%s">Go to year 2003</a></p>' % url(page6, '2003')
    print ('<p><a href="%s">Go to French 2005-11</a></p>'
           % url(page6, 2005, 11, 'fr'))
    print '<p>%s</p>' % link('Go to English 2004-10', page6, 2004, 10)
    print footer()

@http('/hello?first_name=$name')
@printhtml
def page7(name=None):
    "Parameters in query part of URL"
    print header('URL parameters')
    if name is None:
        print '<h2>What is your name?</h2>'
        print '<form>'
        print '<input type="text" name="first_name">'
        print '<input type="submit" value="Send!">'
        print '</form>'
        print '<br>'
        print '<p>You can try input something &quot;illegal&quot;'
        print 'instead of name, such as'
        print '<p><code><strong>&lt;script&gt;',
        print 'alert(&quot;You are hacked!&quot;);',
        print '&lt;/script&gt;</strong></code>'
        print '<p>You will see as <strong>Pony</strong> automatically prevent'
        print 'such XSS (Cross-Site Scripting) attacks '
        print '(those script will not be executed)</p>'
    else:
        print '<h2>Hello, %s!</h2>' % name
        print '<p><a href="%s">Try again</a></p>' % url(page7)
    print footer()

@http
def page8():
    "Using of html() function"
    return html()

@http('/') # This is root page
@printhtml
def index():
    print header('Simple Pony examples')
    print '<ul>'
    print '<li><h4><a href="%s">HelloWorld example</a></h4></li>' % url(page1)
    print '<li><h4>%s</h3></li>' % link('Simplest HTML page', page2)
    print '<li><h4>%s</h3></li>' % link(page3)
    print '<li><h4>%s</h3></li>' % link(page4)
    print '<li><h4>%s</h3></li>' % link(page5)
    print '<li><h4>%s</h3></li>' % link(page6, 2007, 10)
    print '<li><h4>%s</h3></li>' % link(page7)
    print '<li><h4>%s</h3></li>' % link(page8)
    print '</ul>'
    print '<br><br><p><a href="mailto:example@example.com">automatically obfuscated e-mail</a></p>'
    print '<p><a href="http://www.google.com@members.tripod.com/abc/def?x=1&y=2">External link</a></p>'
    print '<p><a href="ftp://aaa.bbb.com/xxx/yyy">FTP link</a></p>'
    print '''<p><a href="javascript:alert('Hello');">JavaScript url</a></p>'''

if __name__ == '__main__':
    start_http_server()
    # show_gui()
